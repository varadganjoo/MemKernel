"""Operator commands for local keys, pinned trust, synchronization and diagnostics."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from memkernel.kernel import MemKernel, default_db_path
from memkernel.private_files import protect_private_file
from memkernel.sync import SyncConfig, atomic_write, load_config


def _initialize(args) -> dict:
    path = args.config.expanduser().resolve()
    key_path = path.with_suffix(".key")
    key = Ed25519PrivateKey.generate()
    public = key.public_key().public_bytes_raw()
    policy = SyncConfig.model_validate(
        {
            "schema": "memkernel.sync-config/v1",
            "workspace_id": args.workspace,
            "peer_id": args.peer,
            "folder": str(args.folder.expanduser().resolve()),
            "private_key": str(key_path),
            "trusted_peers": {
                args.peer: {"public_key": public.hex(), "agents": args.agents}
            },
        }
    )
    if path.is_relative_to(policy.folder) or key_path.is_relative_to(policy.folder):
        raise ValueError(
            "trust policy and private key must stay outside the exchange folder"
        )
    if path.exists() or key_path.exists():
        raise ValueError(
            "configuration or signing key already exists; initialization never overwrites them"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    policy.folder.mkdir(parents=True, exist_ok=True)
    created = []
    try:
        for target, data in (
            (key_path, key.private_bytes_raw()),
            (path, policy.model_dump_json(by_alias=True, indent=2).encode()),
        ):
            fd = os.open(
                target,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
                0o600,
            )
            created.append(target)
            with os.fdopen(fd, "wb") as stream:
                protect_private_file(target)
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
    except BaseException:
        for target in created:
            target.unlink(missing_ok=True)
        raise
    return {
        "peer_id": args.peer,
        "agents": args.agents,
        "public_key": public.hex(),
        "fingerprint": hashlib.sha256(public).hexdigest(),
    }


async def _operate(args) -> tuple[object, int]:
    async with await MemKernel.open(args.db, auto_gc=False) as kernel:
        await kernel.enable_sync(args.config)
        if args.action == "run":
            result = await kernel.sync.run_once()
            return result, 1 if result["errors"] else 0
        if args.action == "status":
            return await kernel.sync.status(), 0
        return await kernel.sync.audit(args.limit), 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="memkernel sync", description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    init = sub.add_parser(
        "init", help="create a local signing identity, never overwrite"
    )
    init.add_argument("--config", type=Path, required=True)
    init.add_argument("--folder", type=Path, required=True)
    init.add_argument("--workspace", required=True)
    init.add_argument("--peer", required=True)
    init.add_argument("--agents", nargs="+", required=True)
    trust = sub.add_parser(
        "trust", help="pin an independently verified peer public key"
    )
    trust.add_argument("--config", type=Path, required=True)
    trust.add_argument("--peer", required=True)
    trust.add_argument("--public-key", required=True)
    trust.add_argument("--agents", nargs="+", required=True)
    trust.add_argument(
        "--replace",
        action="store_true",
        help="explicitly replace an existing pinned identity",
    )
    revoke = sub.add_parser(
        "revoke",
        help="remove a peer from the local allowlist; restart servers afterwards",
    )
    revoke.add_argument("--config", type=Path, required=True)
    revoke.add_argument("--peer", required=True)
    for name in ("run", "status", "audit"):
        command = sub.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--db", type=Path, default=default_db_path())
        if name == "audit":
            command.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv)
    try:
        if args.action == "init":
            result, code = _initialize(args), 0
        elif args.action in {"trust", "revoke"}:
            config = load_config(args.config)
            value = config.model_dump(mode="json", by_alias=True)
            if args.action == "trust":
                if args.peer in config.trusted_peers and not args.replace:
                    raise ValueError(
                        "peer is already pinned; use --replace after verifying its new key"
                    )
                value["trusted_peers"][args.peer] = {
                    "public_key": args.public_key,
                    "agents": args.agents,
                }
            else:
                if args.peer not in value["trusted_peers"]:
                    raise ValueError("peer is not pinned")
                del value["trusted_peers"][args.peer]
            checked = SyncConfig.model_validate(value)
            atomic_write(
                args.config.resolve(),
                checked.model_dump_json(by_alias=True, indent=2).encode(),
                private=True,
            )
            result, code = (
                {"peer_id": args.peer, "action": args.action, "restart_servers": True},
                0,
            )
        else:
            result, code = asyncio.run(_operate(args))
        print(json.dumps(result, indent=2))
        return code
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
