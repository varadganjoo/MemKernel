"""Lease-based resource locking with fencing tokens.

A lease is a time-bounded exclusive claim on a resource key (typically a
normalized file path). Every grant carries a **fencing token** that is
strictly greater than every token previously issued for that resource, so a
paused or partitioned agent whose lease expired cannot clobber the work of
the next holder: writes are validated against the *current* token inside the
same transaction (see :meth:`LockManager.check_fence_in_transaction` and
``MemKernel.fenced_append``).

Leases live in the same SQLite database as the event log and use the store's
``BEGIN IMMEDIATE`` transactions, so check-and-grant is atomic across
processes sharing the file.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Callable

import aiosqlite

from memkernel.core.events import validate_agent_id
from memkernel.core.store import AsyncEventStore

MAX_RESOURCE_KEY_LEN = 1024
MAX_TTL_SECONDS = 24 * 3600


@dataclass(frozen=True)
class LeaseInfo:
    resource_key: str
    holder_agent_id: str | None
    fencing_token: int
    ttl_seconds: float
    acquired_at: float
    expires_at: float

    def is_active(self, now: float) -> bool:
        return self.holder_agent_id is not None and self.expires_at > now

    def to_dict(self, now: float) -> dict[str, Any]:
        return {
            "resource_key": self.resource_key,
            "holder_agent_id": self.holder_agent_id if self.is_active(now) else None,
            "fencing_token": self.fencing_token,
            "ttl_seconds": self.ttl_seconds,
            "expires_at": self.expires_at,
            "expires_in_seconds": round(max(0.0, self.expires_at - now), 3) if self.is_active(now) else 0.0,
            "active": self.is_active(now),
        }


def validate_resource_key(resource_key: str) -> str:
    if not isinstance(resource_key, str) or not resource_key.strip():
        raise ValueError("resource_key must be a non-empty string")
    if len(resource_key) > MAX_RESOURCE_KEY_LEN:
        raise ValueError(f"resource_key longer than {MAX_RESOURCE_KEY_LEN} characters")
    return resource_key


def validate_ttl(ttl_seconds: float) -> float:
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, (int, float)):
        raise ValueError("ttl_seconds must be a number")
    if not math.isfinite(ttl_seconds) or not 0 < ttl_seconds <= MAX_TTL_SECONDS:
        raise ValueError(f"ttl_seconds must be in (0, {MAX_TTL_SECONDS}], got {ttl_seconds!r}")
    return float(ttl_seconds)


class LockManager:
    """Lease manager bound to an open :class:`AsyncEventStore`.

    ``clock`` returns wall-clock seconds; it must be comparable across every
    process sharing the database (the default, ``time.time``, is). Inject a
    fake clock in tests to exercise expiry deterministically.
    """

    def __init__(self, store: AsyncEventStore, *, clock: Callable[[], float] = time.time) -> None:
        self.store = store
        self.clock = clock

    # ------------------------------------------------------------- public API

    async def acquire_lock(self, resource_key: str, agent_id: str, ttl_seconds: float = 30) -> int | None:
        """Grant a lease and return its fencing token, or ``None`` if an
        unexpired lease exists (including one held by ``agent_id`` itself —
        leases are not re-entrant; use :meth:`heartbeat_lock` to extend)."""
        async with self.store.transaction(write=True) as db:
            return await self.acquire_in_transaction(db, resource_key, agent_id, ttl_seconds)

    async def release_lock(self, resource_key: str, agent_id: str, fencing_token: int) -> bool:
        """Release a lease. Succeeds only for the holder presenting the
        current token (an expired-but-untaken lease can still be released)."""
        async with self.store.transaction(write=True) as db:
            return await self.release_in_transaction(db, resource_key, agent_id, fencing_token)

    async def heartbeat_lock(self, resource_key: str, fencing_token: int) -> bool:
        """Extend an *unexpired* lease by its original TTL. Returns ``False``
        if the token is stale or the lease already lapsed (lease lost)."""
        validate_resource_key(resource_key)
        async with self.store.transaction(write=True) as db:
            lease = await self.get_in_transaction(db, resource_key)
            now = self.clock()
            if lease is None or lease.fencing_token != fencing_token or not lease.is_active(now):
                return False
            await db.execute(
                "UPDATE leases SET expires_at = ? WHERE resource_key = ? AND fencing_token = ?",
                (now + lease.ttl_seconds, resource_key, fencing_token),
            )
            return True

    async def get_lease(self, resource_key: str) -> LeaseInfo | None:
        validate_resource_key(resource_key)
        async with self.store.transaction(write=False) as db:
            return await self.get_in_transaction(db, resource_key)

    async def list_leases(self, *, active_only: bool = True) -> list[LeaseInfo]:
        async with self.store.transaction(write=False) as db:
            rows = await db.execute_fetchall(f"SELECT {_COLUMNS} FROM leases ORDER BY resource_key")
        now = self.clock()
        leases = [LeaseInfo(*r) for r in rows]
        return [lease for lease in leases if lease.is_active(now)] if active_only else leases

    async def validate_fencing_token(self, resource_key: str, fencing_token: int, agent_id: str | None = None) -> bool:
        """True iff ``fencing_token`` is the current, unexpired grant (and,
        when given, held by ``agent_id``)."""
        async with self.store.transaction(write=False) as db:
            return await self.check_fence_in_transaction(db, resource_key, fencing_token, agent_id)

    # ------------------------------------------------ transaction primitives

    async def acquire_in_transaction(
        self, db: aiosqlite.Connection, resource_key: str, agent_id: str, ttl_seconds: float
    ) -> int | None:
        validate_resource_key(resource_key)
        validate_agent_id(agent_id)
        ttl = validate_ttl(ttl_seconds)
        now = self.clock()
        lease = await self.get_in_transaction(db, resource_key)
        if lease is not None and lease.is_active(now):
            return None
        token = (lease.fencing_token + 1) if lease is not None else 1
        await db.execute(
            "INSERT INTO leases (resource_key, holder_agent_id, fencing_token, ttl_seconds, acquired_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(resource_key) DO UPDATE SET holder_agent_id = excluded.holder_agent_id, "
            "fencing_token = excluded.fencing_token, ttl_seconds = excluded.ttl_seconds, "
            "acquired_at = excluded.acquired_at, expires_at = excluded.expires_at",
            (resource_key, agent_id, token, ttl, now, now + ttl),
        )
        return token

    async def release_in_transaction(
        self, db: aiosqlite.Connection, resource_key: str, agent_id: str, fencing_token: int
    ) -> bool:
        validate_resource_key(resource_key)
        validate_agent_id(agent_id)
        lease = await self.get_in_transaction(db, resource_key)
        if lease is None or lease.holder_agent_id != agent_id or lease.fencing_token != fencing_token:
            return False
        # Keep the row: its fencing_token is the high-water mark for the next grant.
        await db.execute(
            "UPDATE leases SET holder_agent_id = NULL, expires_at = ? WHERE resource_key = ?",
            (self.clock(), resource_key),
        )
        return True

    async def check_fence_in_transaction(
        self, db: aiosqlite.Connection, resource_key: str, fencing_token: int, agent_id: str | None = None
    ) -> bool:
        validate_resource_key(resource_key)
        lease = await self.get_in_transaction(db, resource_key)
        return (
            lease is not None
            and lease.is_active(self.clock())
            and lease.fencing_token == fencing_token
            and (agent_id is None or lease.holder_agent_id == agent_id)
        )

    async def get_in_transaction(self, db: aiosqlite.Connection, resource_key: str) -> LeaseInfo | None:
        async with db.execute(f"SELECT {_COLUMNS} FROM leases WHERE resource_key = ?", (resource_key,)) as cur:
            row = await cur.fetchone()
        return LeaseInfo(*row) if row else None


_COLUMNS = "resource_key, holder_agent_id, fencing_token, ttl_seconds, acquired_at, expires_at"
