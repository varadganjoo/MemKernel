"""Async append-only event store over SQLite (WAL mode) with copy-on-write
branch lineage.

Concurrency model
-----------------
* **In-process:** one ``aiosqlite`` connection per store; every transaction
  is serialized by an ``asyncio.Lock`` so no coroutine can observe another's
  uncommitted writes.
* **Cross-process:** several MemKernel processes (e.g. one MCP server per
  IDE) may share one database file. Writers use ``BEGIN IMMEDIATE`` so the
  read-modify-write of a branch head is atomic across processes; SQLite's
  ``busy_timeout`` queues contending writers. WAL lets readers proceed
  concurrently with the single writer.

Immutability
------------
``UPDATE``/``DELETE`` on ``events`` are rejected by triggers. Rollback and GC
are expressed as *new* events; history is never rewritten. Each event carries
``hash = SHA256(prev_hash || canonical(event))`` along its lineage.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Callable

import aiosqlite

from memkernel.core.branch import diff_lineages, fold_live, plan_rollback, verify_lineage
from memkernel.core.events import (
    CONTROL_TYPES,
    GENESIS_HASH,
    EventEnvelope,
    EventType,
    canonical_json,
    new_event,
    utcnow,
    validate_agent_id,
    validate_branch_id,
)
from memkernel.memory.state_view import SNAPSHOT_SCHEMA, WorkspaceState, split_context

SCHEMA_VERSION = 1


def _read_only_authorizer(
    action: int, arg1: str | None, arg2: str | None, _database: str | None, _trigger: str | None
) -> int:
    # mode=ro applies to the main database only. An attached connection to
    # the same file could otherwise reopen it writable after query_only=OFF.
    if action in {sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH}:
        return sqlite3.SQLITE_DENY
    if action == sqlite3.SQLITE_PRAGMA and arg1.lower() == "query_only" and arg2 is not None:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


_SCHEMA = """
CREATE TABLE IF NOT EXISTS branches (
    branch_id         TEXT PRIMARY KEY,
    parent_branch_id  TEXT REFERENCES branches(branch_id),
    fork_event_id     INTEGER,
    fork_sequence     INTEGER NOT NULL DEFAULT 0,
    head_event_id     INTEGER,
    head_sequence     INTEGER NOT NULL DEFAULT 0,
    head_hash         TEXT NOT NULL,
    snapshot_event_id INTEGER,
    created_at        TEXT NOT NULL,
    created_by        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id        INTEGER PRIMARY KEY,
    branch_id       TEXT NOT NULL REFERENCES branches(branch_id),
    sequence        INTEGER NOT NULL,
    timestamp       TEXT NOT NULL,
    agent_id        TEXT NOT NULL,
    parent_event_id INTEGER,
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,
    metadata        TEXT NOT NULL,
    hash            TEXT NOT NULL,
    UNIQUE (branch_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_events_branch ON events(branch_id, event_id);
CREATE INDEX IF NOT EXISTS idx_events_agent ON events(agent_id);

CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'memkernel: events are append-only'); END;

CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'memkernel: events are append-only'); END;

CREATE TABLE IF NOT EXISTS leases (
    resource_key    TEXT PRIMARY KEY,
    holder_agent_id TEXT,
    fencing_token   INTEGER NOT NULL,
    ttl_seconds     REAL NOT NULL,
    acquired_at     REAL NOT NULL,
    expires_at      REAL NOT NULL
);

-- Optional workspace-wide coordination, outside rollback and snapshot lineage.
-- Existing v1 readers ignore this table; the event schema stays unchanged.
CREATE TABLE IF NOT EXISTS coordination_notices (
    notice_id       TEXT PRIMARY KEY,
    document        TEXT NOT NULL,
    cleared_at      TEXT
);

CREATE TABLE IF NOT EXISTS notice_sources (
    notice_id TEXT PRIMARY KEY REFERENCES coordination_notices(notice_id),
    peer_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_state (
    peer_id TEXT PRIMARY KEY,
    sequence INTEGER NOT NULL,
    digest TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    workspace_id TEXT NOT NULL,
    peer_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_audit (
    id INTEGER PRIMARY KEY,
    timestamp TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    outcome TEXT NOT NULL,
    reason TEXT NOT NULL,
    sequence INTEGER,
    changed INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sync_audit_peer ON sync_audit(peer_id, id DESC);
"""

_EVENT_COLUMNS = (
    "event_id, branch_id, sequence, timestamp, agent_id, parent_event_id, "
    "event_type, payload, metadata, hash"
)


class StoreError(RuntimeError):
    """Base class for store failures."""


class BranchNotFoundError(StoreError):
    pass


class BranchExistsError(StoreError):
    pass


class EventNotFoundError(StoreError):
    pass


class StaleHeadError(StoreError):
    """Optimistic-concurrency precondition failed: the branch head moved."""

    def __init__(self, branch_id: str, expected: int | None, actual: int | None) -> None:
        super().__init__(
            f"branch {branch_id!r} head is {actual}, expected {expected}; "
            "re-read the branch and retry"
        )
        self.branch_id = branch_id
        self.expected = expected
        self.actual = actual


@dataclass(frozen=True)
class BranchInfo:
    branch_id: str
    parent_branch_id: str | None
    fork_event_id: int | None
    fork_sequence: int
    head_event_id: int | None
    head_sequence: int
    head_hash: str
    snapshot_event_id: int | None
    created_at: str
    created_by: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class ContextSlice:
    """``[latest snapshot] + [delta of raw events]`` for one branch."""

    branch_id: str
    head_event_id: int | None
    snapshot: EventEnvelope | None
    delta: list[EventEnvelope]
    fast_path: bool


def _row_to_event(row: Any) -> EventEnvelope:
    return EventEnvelope.model_construct(
        event_id=row[0],
        branch_id=row[1],
        sequence=row[2],
        timestamp=datetime.fromisoformat(row[3]),
        agent_id=row[4],
        parent_event_id=row[5],
        event_type=EventType(row[6]),
        payload=json.loads(row[7]),
        metadata=json.loads(row[8]),
        hash=row[9],
    )


def _row_to_branch(row: Any) -> BranchInfo:
    return BranchInfo(*row)


class AsyncEventStore:
    """Append-only, branchable event log. Use as an async context manager."""

    def __init__(
        self,
        path: str | Path,
        *,
        synchronous: str = "NORMAL",
        busy_timeout_ms: int = 15_000,
        clock: Callable[[], datetime] = utcnow,
        read_only: bool = False,
    ) -> None:
        if synchronous.upper() not in {"OFF", "NORMAL", "FULL", "EXTRA"}:
            raise ValueError(f"invalid synchronous mode {synchronous!r}")
        self.path = str(path)
        self._synchronous = synchronous.upper()
        self._busy_timeout_ms = int(busy_timeout_ms)
        self._clock = clock
        self.read_only = read_only
        self._db: aiosqlite.Connection | None = None
        self._lock: asyncio.Lock | None = None
        self.journal_mode: str | None = None

    # --------------------------------------------------------------- lifecycle

    async def open(self) -> "AsyncEventStore":
        if self._db is not None:
            return self
        if self.read_only:
            # Construct the URI ourselves: caller-supplied URI parameters must
            # never turn a read-only open into a writable/create connection.
            if self.path == ":memory:" or self.path.startswith("file:"):
                raise StoreError("read-only mode requires an existing local database file path")
            connect_path = Path(self.path).expanduser().resolve().as_uri() + "?mode=ro"
        else:
            connect_path = self.path
        if not self.read_only and self.path != ":memory:" and not self.path.startswith("file:"):
            Path(self.path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        try:
            db = await aiosqlite.connect(connect_path, isolation_level=None, uri=self.read_only)
        except sqlite3.OperationalError as exc:
            if self.read_only:
                raise StoreError("cannot open read-only database; initialize it with a writable session first") from exc
            raise
        try:
            await db.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
            if self.read_only:
                await db.execute("PRAGMA query_only = ON")
                await db.set_authorizer(_read_only_authorizer)
            deadline = asyncio.get_running_loop().time() + self._busy_timeout_ms / 1000
            while True:
                try:
                    async with db.execute("PRAGMA journal_mode" if self.read_only else "PRAGMA journal_mode = WAL") as cur:
                        row = await cur.fetchone()
                    break
                except sqlite3.OperationalError as exc:
                    # WAL initialization can report BUSY without invoking SQLite's busy handler.
                    remaining = deadline - asyncio.get_running_loop().time()
                    if self.read_only or getattr(exc, "sqlite_errorcode", 0) & 0xFF != sqlite3.SQLITE_BUSY or remaining <= 0:
                        raise
                    await asyncio.sleep(min(0.01, remaining))
            self.journal_mode = str(row[0]).lower() if row else None
            await db.execute(f"PRAGMA synchronous = {self._synchronous}")
            await db.execute("PRAGMA foreign_keys = ON")
            async with db.execute("PRAGMA user_version") as cur:
                row = await cur.fetchone()
            version = int(row[0]) if row else 0
            if self.read_only:
                if version != SCHEMA_VERSION:
                    raise StoreError(
                        f"read-only database has schema v{version}; expected v{SCHEMA_VERSION}. "
                        "Initialize or upgrade it with a writable session first."
                    )
                self._db = db
                return self
            if version > SCHEMA_VERSION:
                raise StoreError(
                    f"database {self.path!r} has schema v{version}; this MemKernel supports "
                    f"v{SCHEMA_VERSION}. Upgrade memkernel."
                )
            await db.execute("BEGIN IMMEDIATE")
            try:
                for statement in _split_sql(_SCHEMA):
                    await db.execute(statement)
                await db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                await db.execute("COMMIT")
            except BaseException:
                await db.execute("ROLLBACK")
                raise
        except BaseException:
            await db.close()
            raise
        self._db = db
        return self

    async def close(self) -> None:
        if self._db is not None:
            db, self._db = self._db, None
            await db.close()

    async def __aenter__(self) -> "AsyncEventStore":
        return await self.open()

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @asynccontextmanager
    async def transaction(self, *, write: bool) -> AsyncIterator[aiosqlite.Connection]:
        """Serialized transaction. ``write=True`` takes SQLite's RESERVED lock
        up front (``BEGIN IMMEDIATE``) so read-modify-write sequences are
        atomic across processes."""
        if write and self.read_only:
            raise StoreError("database is read-only; writes are disabled")
        if self._db is None:
            raise StoreError("store is not open; use 'async with AsyncEventStore(...)' or await open()")
        assert self._lock is not None
        async with self._lock:
            db = self._db
            await db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            try:
                yield db
            except BaseException:
                if db.in_transaction:
                    await db.execute("ROLLBACK")
                raise
            else:
                await db.execute("COMMIT")

    # ---------------------------------------------------------------- branches

    async def create_branch(self, branch_id: str, *, agent_id: str = "memkernel") -> BranchInfo:
        """Create an empty root branch. Raises :class:`BranchExistsError`."""
        validate_branch_id(branch_id)
        validate_agent_id(agent_id)
        async with self.transaction(write=True) as db:
            if await self._maybe_branch(db, branch_id) is not None:
                raise BranchExistsError(f"branch {branch_id!r} already exists")
            await db.execute(
                "INSERT INTO branches (branch_id, head_hash, created_at, created_by) VALUES (?, ?, ?, ?)",
                (branch_id, GENESIS_HASH, self._now_iso(), agent_id),
            )
            return await self._branch(db, branch_id)

    async def ensure_branch(self, branch_id: str, *, agent_id: str = "memkernel") -> BranchInfo:
        """Idempotently create a root branch."""
        validate_branch_id(branch_id)
        async with self.transaction(write=True) as db:
            await db.execute(
                "INSERT OR IGNORE INTO branches (branch_id, head_hash, created_at, created_by) "
                "VALUES (?, ?, ?, ?)",
                (branch_id, GENESIS_HASH, self._now_iso(), validate_agent_id(agent_id)),
            )
            return await self._branch(db, branch_id)

    async def get_branch(self, branch_id: str) -> BranchInfo:
        async with self.transaction(write=False) as db:
            return await self._branch(db, branch_id)

    async def list_branches(self) -> list[BranchInfo]:
        async with self.transaction(write=False) as db:
            rows = await db.execute_fetchall(f"SELECT {_BRANCH_COLUMNS} FROM branches ORDER BY created_at, branch_id")
            return [_row_to_branch(r) for r in rows]

    # ------------------------------------------------------------------ append

    async def append_event(self, envelope: EventEnvelope) -> int:
        """Append an event and return its ``event_id``.

        ``event_id`` is globally monotonic; ``sequence`` increments by exactly
        one per event along the branch lineage. If ``envelope.parent_event_id``
        is set it must equal the current head (else :class:`StaleHeadError`).
        """
        persisted = await self.append(envelope)
        return persisted.event_id  # type: ignore[return-value]

    async def append(self, envelope: EventEnvelope) -> EventEnvelope:
        """Like :meth:`append_event` but returns the persisted envelope."""
        async with self.transaction(write=True) as db:
            return await self.append_in_transaction(db, envelope)

    async def append_in_transaction(
        self, db: aiosqlite.Connection, envelope: EventEnvelope, *, _control: bool = False
    ) -> EventEnvelope:
        """Append inside a caller-held ``transaction(write=True)``."""
        if envelope.event_id is not None or envelope.hash is not None:
            raise ValueError("envelope is already persisted (event_id/hash set)")
        if envelope.event_type in CONTROL_TYPES and not _control:
            raise ValueError(
                f"{envelope.event_type.value} events are kernel-issued; "
                "use fork_branch() or rollback()"
            )
        branch = await self._branch(db, envelope.branch_id)
        if envelope.parent_event_id is not None and envelope.parent_event_id != branch.head_event_id:
            raise StaleHeadError(branch.branch_id, envelope.parent_event_id, branch.head_event_id)
        if envelope.event_type is EventType.SEMANTIC_SNAPSHOT:
            await self._validate_snapshot(db, envelope)

        async with db.execute("SELECT COALESCE(MAX(event_id), 0) + 1 FROM events") as cur:
            row = await cur.fetchone()
        event_id = int(row[0])  # type: ignore[index]
        persisted = envelope.model_copy(
            update={
                "event_id": event_id,
                "sequence": branch.head_sequence + 1,
                "parent_event_id": branch.head_event_id,
                "timestamp": self._clock(),
            }
        )
        persisted = persisted.model_copy(update={"hash": persisted.compute_hash(branch.head_hash)})
        await db.execute(
            f"INSERT INTO events ({_EVENT_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                persisted.event_id,
                persisted.branch_id,
                persisted.sequence,
                persisted.timestamp_iso,
                persisted.agent_id,
                persisted.parent_event_id,
                persisted.event_type.value,
                canonical_json(persisted.payload),
                canonical_json(persisted.metadata),
                persisted.hash,
            ),
        )
        is_snapshot = persisted.event_type is EventType.SEMANTIC_SNAPSHOT
        await db.execute(
            "UPDATE branches SET head_event_id = ?, head_sequence = ?, head_hash = ?, "
            "snapshot_event_id = CASE WHEN ? THEN ? ELSE snapshot_event_id END WHERE branch_id = ?",
            (
                persisted.event_id,
                persisted.sequence,
                persisted.hash,
                is_snapshot,
                persisted.event_id,
                persisted.branch_id,
            ),
        )
        return persisted

    async def _validate_snapshot(self, db: aiosqlite.Connection, envelope: EventEnvelope) -> None:
        p = envelope.payload
        if p.get("schema") != SNAPSHOT_SCHEMA:
            raise ValueError(f"snapshot payload must declare schema {SNAPSHOT_SCHEMA!r}")
        covered_id, covered_seq = p.get("covers_through_event_id"), p.get("covers_through_sequence")
        if not isinstance(covered_id, int) or not isinstance(covered_seq, int):
            raise ValueError("snapshot must set integer covers_through_event_id/covers_through_sequence")
        live = fold_live(await self.raw_lineage_in_transaction(db, envelope.branch_id))
        if not any(e.event_id == covered_id and e.sequence == covered_seq for e in live):
            raise ValueError(
                f"snapshot covers event {covered_id} (seq {covered_seq}), which is not live on "
                f"branch {envelope.branch_id!r}"
            )
        WorkspaceState.from_snapshot(envelope)  # schema-validates every section

    # -------------------------------------------------------------------- read

    async def get_event(self, event_id: int) -> EventEnvelope:
        async with self.transaction(write=False) as db:
            return await self._event(db, event_id)

    async def read_stream(self, branch_id: str, from_event_id: int = 0) -> list[EventEnvelope]:
        """Raw lineage of ``branch_id`` (inherited + own events, including
        reverted ones) with ``event_id >= from_event_id``, in log order."""
        async with self.transaction(write=False) as db:
            return await self.raw_lineage_in_transaction(db, branch_id, from_event_id=from_event_id)

    async def read_live(self, branch_id: str) -> list[EventEnvelope]:
        """Live timeline of ``branch_id`` (rollbacks applied)."""
        return fold_live(await self.read_stream(branch_id))

    async def context_events(self, branch_id: str) -> ContextSlice:
        """``[latest snapshot] + [delta]`` for ``branch_id``.

        Fast path: when the branch's snapshot pointer is set and no rollback
        occurred after the snapshot's coverage point, only the suffix after
        ``covers_through_event_id`` is read — an index range scan, O(delta).
        (Along one lineage, sequence and event_id increase together.)
        Otherwise the full lineage is folded (O(lineage)).
        """
        async with self.transaction(write=False) as db:
            branch = await self._branch(db, branch_id)
            if branch.snapshot_event_id is not None:
                snapshot = await self._event(db, branch.snapshot_event_id)
                covered_id = int(snapshot.payload["covers_through_event_id"])
                suffix = await self.raw_lineage_in_transaction(db, branch_id, from_event_id=covered_id + 1)
                if all(e.event_type is not EventType.ROLLBACK_TRIGGERED for e in suffix):
                    delta = [e for e in suffix if e.event_type is not EventType.SEMANTIC_SNAPSHOT]
                    return ContextSlice(branch_id, branch.head_event_id, snapshot, delta, True)
            live = fold_live(await self.raw_lineage_in_transaction(db, branch_id))
            snapshot, delta = split_context(live)
            return ContextSlice(branch_id, branch.head_event_id, snapshot, delta, False)

    async def raw_lineage_in_transaction(
        self,
        db: aiosqlite.Connection,
        branch_id: str,
        *,
        from_event_id: int = 0,
    ) -> list[EventEnvelope]:
        chain = await db.execute_fetchall(
            """
            WITH RECURSIVE chain(branch_id, parent_branch_id, fork_event_id, depth) AS (
                SELECT branch_id, parent_branch_id, fork_event_id, 0 FROM branches WHERE branch_id = ?
                UNION ALL
                SELECT b.branch_id, b.parent_branch_id, b.fork_event_id, c.depth + 1
                FROM branches b JOIN chain c ON b.branch_id = c.parent_branch_id
            )
            SELECT branch_id, parent_branch_id, fork_event_id FROM chain ORDER BY depth
            """,
            (branch_id,),
        )
        if not chain:
            raise BranchNotFoundError(f"branch {branch_id!r} does not exist")
        clauses: list[str] = []
        params: list[Any] = []
        cap: int | None = None  # inclusive upper bound inherited from descendants
        for seg_branch, parent, fork_event_id in chain:
            if cap is None:
                clauses.append("(branch_id = ?)")
                params.append(seg_branch)
            else:
                clauses.append("(branch_id = ? AND event_id <= ?)")
                params.extend((seg_branch, cap))
            if parent is None or fork_event_id is None:
                break
            cap = fork_event_id if cap is None else min(cap, fork_event_id)
        rows = await db.execute_fetchall(
            f"SELECT {_EVENT_COLUMNS} FROM events WHERE ({' OR '.join(clauses)}) "
            "AND event_id >= ? ORDER BY event_id",
            (*params, from_event_id),
        )
        return [_row_to_event(r) for r in rows]

    # ------------------------------------------------------- fork / rollback

    async def fork_branch(
        self,
        source_branch: str,
        fork_point_event_id: int | None,
        new_branch_id: str,
        *,
        agent_id: str = "memkernel",
        reason: str = "",
    ) -> EventEnvelope:
        """Create ``new_branch_id`` whose lineage is ``source_branch``'s raw
        lineage up to and including ``fork_point_event_id`` (``None`` = head).

        No event rows are copied. The new branch's first own event is a
        ``BRANCH_FORKED`` audit record, which is returned.
        """
        validate_branch_id(new_branch_id)
        validate_agent_id(agent_id)
        async with self.transaction(write=True) as db:
            if await self._maybe_branch(db, new_branch_id) is not None:
                raise BranchExistsError(f"branch {new_branch_id!r} already exists")
            source = await self._branch(db, source_branch)
            if fork_point_event_id is None or fork_point_event_id == source.head_event_id:
                # Fork at head, O(1): the child's view equals the source's current view.
                point = await self._event(db, source.head_event_id) if source.head_event_id else None
                snapshot_id = source.snapshot_event_id
            else:
                raw = await self.raw_lineage_in_transaction(db, source_branch)
                index = next((i for i, e in enumerate(raw) if e.event_id == fork_point_event_id), None)
                if index is None:
                    raise ValueError(
                        f"event {fork_point_event_id} is not in the lineage of branch {source_branch!r}"
                    )
                point = raw[index]
                snapshot, _ = split_context(fold_live(raw[: index + 1]))
                snapshot_id = snapshot.event_id if snapshot else None
            await db.execute(
                "INSERT INTO branches (branch_id, parent_branch_id, fork_event_id, fork_sequence, "
                "head_event_id, head_sequence, head_hash, snapshot_event_id, created_at, created_by) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    new_branch_id,
                    source_branch,
                    point.event_id if point else None,
                    point.sequence if point else 0,
                    point.event_id if point else None,
                    point.sequence if point else 0,
                    point.hash if point else GENESIS_HASH,
                    snapshot_id,
                    self._now_iso(),
                    agent_id,
                ),
            )
            return await self.append_in_transaction(
                db,
                new_event(
                    EventType.BRANCH_FORKED,
                    branch_id=new_branch_id,
                    agent_id=agent_id,
                    payload={
                        "source_branch": source_branch,
                        "fork_point_event_id": point.event_id if point else None,
                        "fork_point_sequence": point.sequence if point else 0,
                        "reason": reason,
                    },
                ),
                _control=True,
            )

    async def rollback(
        self, branch_id: str, target_event_id: int, *, agent_id: str, reason: str = ""
    ) -> EventEnvelope:
        """Revert the live timeline of ``branch_id`` to ``target_event_id``.

        Appends a ``ROLLBACK_TRIGGERED`` compensating event; reverted events
        stay in the log for audit and remain forkable.
        """
        validate_agent_id(agent_id)
        async with self.transaction(write=True) as db:
            live = fold_live(await self.raw_lineage_in_transaction(db, branch_id))
            target, reverted = plan_rollback(live, target_event_id)
            persisted = await self.append_in_transaction(
                db,
                new_event(
                    EventType.ROLLBACK_TRIGGERED,
                    branch_id=branch_id,
                    agent_id=agent_id,
                    payload={
                        "target_event_id": target.event_id,
                        "target_sequence": target.sequence,
                        "reverted_event_ids": reverted,
                        "reason": reason,
                    },
                ),
                _control=True,
            )
            kept = live[: live.index(target) + 1] + [persisted]
            snapshot, _ = split_context(kept)
            await db.execute(
                "UPDATE branches SET snapshot_event_id = ? WHERE branch_id = ?",
                (snapshot.event_id if snapshot else None, branch_id),
            )
            return persisted

    # ---------------------------------------------------------- diff / audit

    async def get_branch_diff(self, branch_a: str, branch_b: str) -> dict[str, Any]:
        """Divergence points and conflicting decisions between two branches."""
        async with self.transaction(write=False) as db:
            raw_a = await self.raw_lineage_in_transaction(db, branch_a)
            raw_b = await self.raw_lineage_in_transaction(db, branch_b)
        return diff_lineages(branch_a, raw_a, branch_b, raw_b)

    async def verify_branch(self, branch_id: str) -> list[str]:
        """Verify hash chain, sequencing and head pointers. Empty list = intact."""
        async with self.transaction(write=False) as db:
            branch = await self._branch(db, branch_id)
            raw = await self.raw_lineage_in_transaction(db, branch_id)
        problems = verify_lineage(raw)
        last = raw[-1] if raw else None
        if (last.event_id if last else None) != branch.head_event_id:
            problems.append(f"branch head {branch.head_event_id} != last lineage event {last and last.event_id}")
        if (last.hash if last else GENESIS_HASH) != branch.head_hash:
            problems.append("branch head_hash does not match the last lineage event")
        return problems

    # ---------------------------------------------------------------- helpers

    def _now_iso(self) -> str:
        return self._clock().isoformat(timespec="microseconds")

    async def _maybe_branch(self, db: aiosqlite.Connection, branch_id: str) -> BranchInfo | None:
        async with db.execute(f"SELECT {_BRANCH_COLUMNS} FROM branches WHERE branch_id = ?", (branch_id,)) as cur:
            row = await cur.fetchone()
        return _row_to_branch(row) if row else None

    async def _branch(self, db: aiosqlite.Connection, branch_id: str) -> BranchInfo:
        branch = await self._maybe_branch(db, branch_id)
        if branch is None:
            raise BranchNotFoundError(f"branch {branch_id!r} does not exist")
        return branch

    async def _event(self, db: aiosqlite.Connection, event_id: int) -> EventEnvelope:
        async with db.execute(f"SELECT {_EVENT_COLUMNS} FROM events WHERE event_id = ?", (event_id,)) as cur:
            row = await cur.fetchone()
        if row is None:
            raise EventNotFoundError(f"event {event_id} does not exist")
        return _row_to_event(row)


_BRANCH_COLUMNS = (
    "branch_id, parent_branch_id, fork_event_id, fork_sequence, head_event_id, "
    "head_sequence, head_hash, snapshot_event_id, created_at, created_by"
)


def _split_sql(script: str) -> list[str]:
    """Split the schema script into statements (trigger bodies contain ';')."""
    statements, buffer = [], []
    for line in script.strip().splitlines():
        buffer.append(line)
        joined = "\n".join(buffer).strip()
        if joined.endswith(";") and (not joined.upper().startswith("CREATE TRIGGER") or joined.upper().endswith("END;")):
            statements.append(joined)
            buffer = []
    if "\n".join(buffer).strip():
        raise StoreError("unterminated statement in schema script")
    return statements
