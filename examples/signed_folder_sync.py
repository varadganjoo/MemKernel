"""Two local databases, signed notices, tamper rejection and replay protection.

Run: python examples/signed_folder_sync.py
Uses temporary synthetic keys and files; no network, model or hosted service.
"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from memkernel import MemKernel
from memkernel.private_files import protect_private_file


async def main() -> None:
    with TemporaryDirectory(prefix="memkernel-signed-") as directory:
        root = Path(directory)
        folder = root / "exchange"
        folder.mkdir()
        keys = {name: Ed25519PrivateKey.generate() for name in ("alice", "bob")}
        policies = {
            name: {
                "public_key": key.public_key().public_bytes_raw().hex(),
                "agents": [name],
            }
            for name, key in keys.items()
        }
        for name, key in keys.items():
            home = root / name
            home.mkdir()
            private = home / "sync.key"
            with private.open("xb") as stream:
                protect_private_file(private)
                stream.write(key.private_bytes_raw())
            config = home / "sync.json"
            config.write_text(
                json.dumps(
                    {
                        "schema": "memkernel.sync-config/v1",
                        "workspace_id": "example",
                        "peer_id": name,
                        "folder": str(folder),
                        "private_key": str(private),
                        "trusted_peers": policies,
                    }
                ),
                encoding="utf-8",
            )
            protect_private_file(config)
        async with (
            await MemKernel.open(
                root / "alice" / "memory.db", tokenizer="heuristic"
            ) as alice,
            await MemKernel.open(
                root / "bob" / "memory.db", tokenizer="heuristic"
            ) as bob,
        ):
            await alice.enable_sync(root / "alice" / "sync.json")
            await bob.enable_sync(root / "bob" / "sync.json")
            notice = await alice.notices.post(
                agent_id="alice", resource="service:auth", message="Changing login"
            )
            await alice.sync.run_once()
            assert (await bob.sync.run_once())["changed"] == 1
            assert (await bob.notices.list())[0]["trusted_source"] is True
            print(
                "PASS: pinned peer signature and author verified across independent databases"
            )

            path = folder / "alice.json"
            original = path.read_bytes()
            document = json.loads(original)
            document["notices"][0]["message"] = "Injected edit"
            path.write_text(json.dumps(document), encoding="utf-8")
            assert (await bob.sync.run_once())["errors"]["alice"] == "invalid_signature"
            assert (await bob.notices.list())[0]["message"] == "Changing login"
            print("PASS: a tampered transport cannot modify stored notices")

            path.write_bytes(original)
            await bob.sync.run_once()
            assert (await bob.sync.status())["peers"]["alice"]["last_error"] is None
            await alice.notices.clear(notice.notice_id, agent_id="alice")
            await alice.sync.run_once()
            await bob.sync.run_once()
            path.write_bytes(original)
            assert (await bob.sync.run_once())["errors"]["alice"] == "replayed_sequence"
            assert await bob.notices.list() == []
            print(
                "PASS: clears propagate and an old signed snapshot cannot reopen a notice"
            )


if __name__ == "__main__":
    asyncio.run(main())
