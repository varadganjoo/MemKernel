"""Signed, workspace-bound snapshots over an operator-managed folder.

SQLite stays local. The folder is a transport, not a source of trust. OS
permissions protect the local policy, signing key, database and audit history.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import stat
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from memkernel.core.events import canonical_json, utcnow, validate_agent_id
from memkernel.core.notices import CoordinationNotice, _reject_constant, _unique_object
from memkernel.private_files import assert_private_permissions, protect_private_file

if TYPE_CHECKING:
    from memkernel import MemKernel

MAX_BUNDLE_BYTES = 8 * 1024 * 1024
MAX_SYNC_NOTICES = 10_000
PEER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"


class PeerPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    public_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    agents: list[str] = Field(min_length=1, max_length=64)

    @field_validator("agents")
    @classmethod
    def valid_agents(cls, values: list[str]) -> list[str]:
        return [validate_agent_id(value) for value in values]


class SyncConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["memkernel.sync-config/v1"] = Field(alias="schema")
    workspace_id: str = Field(
        strict=True,
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    peer_id: str = Field(pattern=PEER_PATTERN)
    folder: Path
    private_key: Path
    trusted_peers: dict[str, PeerPolicy] = Field(min_length=1, max_length=128)
    interval_seconds: int = Field(default=10, strict=True, ge=1, le=3600)
    stale_after_seconds: int = Field(default=300, strict=True, ge=1, le=86400)

    @model_validator(mode="after")
    def trust_policy(self) -> SyncConfig:
        import re

        if self.peer_id not in self.trusted_peers:
            raise ValueError("local peer must be explicitly pinned in trusted_peers")
        agents, keys = set(), set()
        for peer, policy in self.trusted_peers.items():
            if not re.fullmatch(PEER_PATTERN, peer):
                raise ValueError("invalid trusted peer_id")
            if agents.intersection(policy.agents) or len(set(policy.agents)) != len(
                policy.agents
            ):
                raise ValueError("each agent identity must belong to exactly one peer")
            if policy.public_key in keys:
                raise ValueError("each peer must have a distinct signing key")
            agents.update(policy.agents)
            keys.add(policy.public_key)
        return self


class _Bundle(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["memkernel.signed-notices/v1"] = Field(alias="schema")
    workspace_id: str
    peer_id: str = Field(pattern=PEER_PATTERN)
    sequence: int = Field(strict=True, ge=1, le=2**63 - 1)
    sent_at: AwareDatetime
    notices: list[CoordinationNotice] = Field(max_length=MAX_SYNC_NOTICES)
    signature: str = Field(pattern=r"^[0-9a-f]{128}$")

    @field_validator("sent_at", mode="before")
    @classmethod
    def valid_time(cls, value):
        if not isinstance(value, str):
            raise ValueError("sent_at must be a timezone-aware ISO timestamp")  # noqa: TRY004 - Pydantic validators require ValueError.
        return datetime.fromisoformat(value)

    @model_validator(mode="after")
    def unique_ids(self) -> _Bundle:
        if len({n.notice_id for n in self.notices}) != len(self.notices):
            raise ValueError("duplicate notice_id")
        return self


class SyncRejected(ValueError):
    """A safe machine-readable reason; never contains imported content or keys."""


def _read_file(path: Path, maximum: int, *, private: bool = False) -> bytes:
    # ASVS 2.2.1: bound before parsing, reject links and special files.
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or path.is_symlink():
        raise SyncRejected("unsafe_file")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_BINARY", 0)
    )
    with os.fdopen(os.open(path, flags), "rb") as stream:
        if private:
            assert_private_permissions(path, fd=stream.fileno())
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise SyncRejected("unsafe_file")
        if os.fstat(stream.fileno()).st_size > maximum:
            raise SyncRejected("oversized_file")
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise SyncRejected("oversized_file")
    return data


def _parse(data: bytes):
    # ASVS 1.5.2/1.5.3: one strict UTF-8 JSON parser, no executable objects.
    try:
        return json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise SyncRejected("invalid_document") from exc


def atomic_write(path: Path, data: bytes, *, private: bool = False) -> None:
    """Write and flush a complete private file, then replace within one directory."""
    fd, temporary = tempfile.mkstemp(
        prefix=f".{path.stem}-", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "wb") as stream:
            if private:
                protect_private_file(Path(temporary))
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            directory = os.open(
                path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def load_config(path: str | Path) -> SyncConfig:
    path = Path(path).expanduser().resolve()
    try:
        config = SyncConfig.model_validate(_parse(_read_file(path, 64 * 1024, private=True)))
    except OSError as exc:
        raise ValueError("cannot read local sync policy") from exc
    paths = {
        name: (path.parent / getattr(config, name)).resolve()
        for name in ("folder", "private_key")
    }
    config = config.model_copy(update=paths)
    if config.private_key.is_relative_to(config.folder) or path.is_relative_to(
        config.folder
    ):
        raise ValueError(
            "signing key and trust policy must stay outside the exchange folder"
        )
    return config


class FolderSync:
    def __init__(self, kernel: MemKernel, config_path: str | Path) -> None:
        if kernel.store.read_only:
            raise ValueError("read-only sessions cannot enable synchronization")
        self.kernel = kernel
        self.config = load_config(config_path)
        self.clock = utcnow
        if Path(kernel.store.path).resolve().is_relative_to(self.config.folder):
            raise ValueError("SQLite database must stay outside the exchange folder")
        if not self.config.folder.is_dir():
            raise ValueError("exchange folder does not exist; initialize it explicitly")
        try:
            self._key = Ed25519PrivateKey.from_private_bytes(
                _read_file(self.config.private_key, 32, private=True)
            )
        except OSError as exc:
            raise ValueError("cannot read local signing key") from exc
        pinned = self.config.trusted_peers[self.config.peer_id].public_key
        if self._key.public_key().public_bytes_raw().hex() != pinned:
            raise ValueError("signing key does not match the pinned local public key")
        self._lock = asyncio.Lock()

    def _verify(self, peer: str) -> tuple[_Bundle, str]:
        try:
            raw = _parse(
                _read_file(self.config.folder / f"{peer}.json", MAX_BUNDLE_BYTES)
            )
            bundle = _Bundle.model_validate(raw)
        except FileNotFoundError as exc:
            raise SyncRejected("missing_file") from exc
        except ValidationError as exc:
            raise SyncRejected("invalid_document") from exc
        if bundle.peer_id != peer:
            raise SyncRejected("wrong_peer")
        if bundle.workspace_id != self.config.workspace_id:
            raise SyncRejected("wrong_workspace")
        payload = canonical_json(
            {k: v for k, v in raw.items() if k != "signature"}
        ).encode("utf-8")
        policy = self.config.trusted_peers[peer]
        try:
            # ASVS 11.2.1: vetted Ed25519, exact pinned public key, no algorithm negotiation.
            Ed25519PublicKey.from_public_bytes(bytes.fromhex(policy.public_key)).verify(
                bytes.fromhex(bundle.signature), payload
            )
        except InvalidSignature as exc:
            raise SyncRejected("invalid_signature") from exc
        if (bundle.sent_at - self.clock()).total_seconds() > 300:
            raise SyncRejected("future_timestamp")
        if any(n.author not in policy.agents for n in bundle.notices):
            raise SyncRejected("unauthorized_author")
        return bundle, hashlib.sha256(payload).hexdigest()

    async def _audit(
        self,
        db,
        peer: str,
        direction: str,
        outcome: str,
        reason: str = "",
        sequence=None,
        changed=0,
    ):
        # ASVS 16.1.1: metadata only; imported text and key material never enter logs.
        await db.execute(
            "INSERT INTO sync_audit (timestamp, peer_id, direction, outcome, reason, sequence, changed) VALUES (?,?,?,?,?,?,?)",
            (
                self.clock().isoformat(),
                peer,
                direction,
                outcome,
                reason,
                sequence,
                changed,
            ),
        )

    async def _publish(self) -> None:
        peer = self.config.peer_id
        # Keep the file replacement under BEGIN IMMEDIATE to serialize local processes.
        async with self.kernel.store.transaction(write=True) as db:
            async with db.execute(
                "SELECT sequence FROM sync_state WHERE peer_id = ?", (peer,)
            ) as cur:
                row = await cur.fetchone()
            sequence = row[0] if row else 0
            try:
                previous, _ = self._verify(peer)
                sequence = max(sequence, previous.sequence)
            except SyncRejected as exc:
                if str(exc) not in {
                    "missing_file",
                    "invalid_document",
                    "invalid_signature",
                    "future_timestamp",
                }:
                    raise
            async with db.execute(
                "SELECT n.document, n.cleared_at FROM coordination_notices n "
                "JOIN notice_sources s ON s.notice_id = n.notice_id WHERE s.peer_id = ? ORDER BY n.notice_id",
                (peer,),
            ) as cur:
                rows = await cur.fetchall()
            notices = []
            for body, cleared in rows:
                notice = CoordinationNotice.model_validate(
                    {**json.loads(body), "cleared_at": cleared}
                )
                if notice.author in self.config.trusted_peers[peer].agents:
                    notices.append(notice.model_dump(mode="json"))
            payload = {
                "schema": "memkernel.signed-notices/v1",
                "workspace_id": self.config.workspace_id,
                "peer_id": peer,
                "sequence": sequence + 1,
                "sent_at": self.clock().astimezone(UTC).isoformat(),
                "notices": notices,
            }
            signed = {
                **payload,
                "signature": self._key.sign(canonical_json(payload).encode()).hex(),
            }
            _Bundle.model_validate(signed)
            data = canonical_json(signed).encode("utf-8")
            if len(data) > MAX_BUNDLE_BYTES:
                raise SyncRejected("oversized_file")
            atomic_write(self.config.folder / f"{peer}.json", data)
            now = self.clock().isoformat()
            digest = hashlib.sha256(canonical_json(payload).encode()).hexdigest()
            await db.execute(
                "INSERT OR REPLACE INTO sync_state VALUES (?, ?, ?, ?, ?)",
                (peer, sequence + 1, digest, now, now),
            )
            await self._audit(db, peer, "out", "published", sequence=sequence + 1)

    async def _receive(self, peer: str) -> int:
        bundle, digest = self._verify(peer)
        async with self.kernel.store.transaction(write=True) as db:
            async with db.execute(
                "SELECT sequence, digest FROM sync_state WHERE peer_id = ?", (peer,)
            ) as cur:
                row = await cur.fetchone()
            if row:
                if bundle.sequence < row[0]:
                    raise SyncRejected("replayed_sequence")
                if bundle.sequence == row[0]:
                    if digest != row[1]:
                        raise SyncRejected("sequence_conflict")
                    async with db.execute(
                        "SELECT outcome FROM sync_audit WHERE peer_id = ? ORDER BY id DESC LIMIT 1",
                        (peer,),
                    ) as cur:
                        last = await cur.fetchone()
                    if last and last[0] == "rejected":
                        await self._audit(
                            db,
                            peer,
                            "in",
                            "accepted",
                            "unchanged",
                            sequence=bundle.sequence,
                        )
                    return 0  # Re-reading the same file does not refresh peer liveness.
            try:
                changed = await self.kernel.notices._merge(
                    db, bundle.notices, peer_id=peer
                )
            except ValueError as exc:
                raise SyncRejected("notice_conflict") from exc
            await db.execute(
                "INSERT OR REPLACE INTO sync_state VALUES (?, ?, ?, ?, ?)",
                (
                    peer,
                    bundle.sequence,
                    digest,
                    bundle.sent_at.isoformat(),
                    self.clock().isoformat(),
                ),
            )
            await self._audit(
                db, peer, "in", "accepted", sequence=bundle.sequence, changed=changed
            )
            return changed

    async def run_once(self) -> dict:
        """Publish local authors' records and import every explicitly pinned peer."""
        async with self._lock:
            errors, changed, published = {}, 0, False
            for peer in self.config.trusted_peers:
                try:
                    if peer == self.config.peer_id:
                        await self._publish()
                        published = True
                    else:
                        changed += await self._receive(peer)
                except (ValueError, OSError) as exc:
                    reason = (
                        str(exc)
                        if isinstance(exc, SyncRejected)
                        else "io_error"
                        if isinstance(exc, OSError)
                        else "invalid_document"
                    )
                    errors[peer] = reason
                    async with self.kernel.store.transaction(write=True) as db:
                        await self._audit(
                            db,
                            peer,
                            "out" if peer == self.config.peer_id else "in",
                            "rejected",
                            reason,
                        )
            return {"published": published, "changed": changed, "errors": errors}

    async def status(self) -> dict:
        peers = {}
        async with self.kernel.store.transaction(write=False) as db:
            for peer in self.config.trusted_peers:
                if peer == self.config.peer_id:
                    continue
                async with db.execute(
                    "SELECT sequence, sent_at, received_at FROM sync_state WHERE peer_id = ?",
                    (peer,),
                ) as cur:
                    row = await cur.fetchone()
                async with db.execute(
                    "SELECT outcome, reason FROM sync_audit WHERE peer_id = ? ORDER BY id DESC LIMIT 1",
                    (peer,),
                ) as cur:
                    last = await cur.fetchone()
                age = (
                    max(
                        0.0,
                        (self.clock() - datetime.fromisoformat(row[1])).total_seconds(),
                    )
                    if row
                    else None
                )
                peers[peer] = {
                    "status": "never_received"
                    if row is None
                    else "stale"
                    if age > self.config.stale_after_seconds
                    else "fresh",
                    "sequence": row[0] if row else None,
                    "sent_at": row[1] if row else None,
                    "received_at": row[2] if row else None,
                    "age_seconds": age,
                    "last_error": last[1] if last and last[0] == "rejected" else None,
                }
        return {
            "enabled": True,
            "workspace_id": self.config.workspace_id,
            "peer_id": self.config.peer_id,
            "peers": peers,
            "distributed_locks_enforced": False,
        }

    async def audit(self, limit: int = 100) -> list[dict]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("audit limit must be between 1 and 1000")
        async with self.kernel.store.transaction(write=False) as db:  # noqa: SIM117 - keep query and transaction lifetimes explicit.
            async with db.execute(
                "SELECT id, timestamp, peer_id, direction, outcome, reason, sequence, changed FROM sync_audit ORDER BY id DESC LIMIT ?",
                (limit,),
            ) as cur:
                return [
                    dict(
                        zip(
                            (
                                "id",
                                "timestamp",
                                "peer_id",
                                "direction",
                                "outcome",
                                "reason",
                                "sequence",
                                "changed",
                            ),
                            row,
                        )
                    )
                    for row in await cur.fetchall()
                ]

    async def run_daemon(self) -> None:
        while True:
            await asyncio.sleep(self.config.interval_seconds)
            try:
                await self.run_once()
            except (sqlite3.Error, RuntimeError):
                logging.getLogger(__name__).error(
                    "sync storage failure; retrying on the next poll"
                )
