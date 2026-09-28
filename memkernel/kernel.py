"""``MemKernel``: the facade composing the event store, lease manager and GC.

This is the single entry point used by the MCP server and the client
adapters. Multi-step operations that must be atomic (grant a lease *and*
record the claim, validate a fencing token *and* append) run inside one
store transaction.
"""

from __future__ import annotations

import logging
import os
import posixpath
import time
from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel

from memkernel.core.events import EventEnvelope, EventType, new_event, validate_branch_id
from memkernel.core.locks import LockManager
from memkernel.core.store import AsyncEventStore, StoreError
from memkernel.memory.gc import GCConfig, GCResult, MemoryGCEngine, Summarizer
from memkernel.memory.state_view import materialize, render_context, render_event
from memkernel.memory.token_counter import TokenCounter

log = logging.getLogger(__name__)

DEFAULT_BRANCH = "main"


class FencingTokenError(StoreError):
    """A write presented a fencing token that is stale, expired or foreign."""


class LeaseGrant(BaseModel):
    acquired: bool
    resource: str
    fencing_token: int | None = None
    ttl_seconds: float
    expires_at: float | None = None
    holder_agent_id: str | None = None
    expires_in_seconds: float | None = None
    claim_event_id: int | None = None


def default_db_path(cwd: str | Path | None = None) -> Path:
    """Where a server should keep its database when none is configured.

    Precedence: ``MEMKERNEL_DB``; else the nearest ancestor of ``cwd``
    (inclusive) containing ``.memkernel/`` or ``.git``; else ``cwd``. Agents
    launch MCP servers from the project directory, so one global client
    config gives every project its own shared memory. Nearest wins, so a
    stray ``~/.memkernel`` never captures a git project below it.
    """
    env = os.environ.get("MEMKERNEL_DB")
    if env:
        return Path(env).expanduser()
    start = Path(cwd or os.getcwd()).resolve()
    for directory in (start, *start.parents):
        if (directory / ".memkernel").is_dir() or (directory / ".git").exists():
            return directory / ".memkernel" / "memkernel.db"
    return start / ".memkernel" / "memkernel.db"


def normalize_resource(path: str) -> str:
    """Canonical lease key for a file path: forward slashes, no ``.``/``..``
    segments or trailing slash, so ``./src\\a.py`` and ``src/a.py`` collide."""
    if not isinstance(path, str) or not path.strip():
        raise ValueError("file path must be a non-empty string")
    normalized = posixpath.normpath(path.strip().replace("\\", "/"))
    if normalized in {".", "/"}:
        raise ValueError(f"{path!r} does not name a file or directory")
    return normalized


class MemKernel:
    def __init__(
        self,
        store: AsyncEventStore,
        *,
        locks: LockManager | None = None,
        gc: MemoryGCEngine | None = None,
        auto_gc: bool = True,
    ) -> None:
        self.store = store
        self.locks = locks or LockManager(store)
        self.gc_engine = gc or MemoryGCEngine(store)
        self.auto_gc = auto_gc

    @classmethod
    async def open(
        cls,
        path: str | Path,
        *,
        gc_config: GCConfig | None = None,
        summarizer: Summarizer | None = None,
        tokenizer: str | None = None,
        lease_clock: Callable[[], float] = time.time,
        auto_gc: bool = True,
        synchronous: str = "NORMAL",
    ) -> "MemKernel":
        store = await AsyncEventStore(path, synchronous=synchronous).open()
        try:
            await store.ensure_branch(DEFAULT_BRANCH)
        except BaseException:
            await store.close()
            raise
        gc = MemoryGCEngine(
            store, counter=TokenCounter(tokenizer), summarizer=summarizer, config=gc_config
        )
        return cls(store, locks=LockManager(store, clock=lease_clock), gc=gc, auto_gc=auto_gc)

    async def close(self) -> None:
        await self.store.close()

    async def __aenter__(self) -> "MemKernel":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # ---------------------------------------------------------------- writes

    async def record(
        self,
        event_type: EventType,
        *,
        branch_id: str,
        agent_id: str,
        payload: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        expected_head: int | None = None,
    ) -> EventEnvelope:
        """Append an event; ``expected_head`` enables optimistic concurrency."""
        persisted = await self.store.append(
            new_event(
                event_type,
                branch_id=branch_id,
                agent_id=agent_id,
                payload=payload,
                metadata=metadata,
                parent_event_id=expected_head,
            )
        )
        await self._auto_gc(branch_id)
        return persisted

    async def init_task(
        self, *, branch_id: str, agent_id: str, directive: str, constraints: list[str] | None = None
    ) -> EventEnvelope:
        if not directive.strip():
            raise ValueError("directive must be non-empty")
        return await self.record(
            EventType.AGENT_TASK_INIT,
            branch_id=branch_id,
            agent_id=agent_id,
            payload={"directive": directive, "constraints": list(constraints or [])},
        )

    async def append_decision(
        self, *, branch_id: str, agent_id: str, key: str, value: str, rationale: str
    ) -> EventEnvelope:
        if not key.strip():
            raise ValueError("decision_key must be non-empty")
        return await self.record(
            EventType.ARCHITECTURAL_DECISION,
            branch_id=branch_id,
            agent_id=agent_id,
            payload={"decision_key": key.strip(), "decision_value": value, "rationale": rationale},
        )

    async def log_execution(
        self,
        *,
        branch_id: str,
        agent_id: str,
        command: str,
        exit_code: int,
        output: str = "",
        facts: dict[str, Any] | None = None,
    ) -> EventEnvelope:
        payload: dict[str, Any] = {"command": command, "exit_code": int(exit_code), "output": output}
        if facts:
            payload["facts"] = dict(facts)
        return await self.record(
            EventType.TOOL_EXECUTION_LOG, branch_id=branch_id, agent_id=agent_id, payload=payload
        )

    async def note(self, *, branch_id: str, agent_id: str, note: str) -> EventEnvelope:
        return await self.record(
            EventType.AGENT_SCRATCHPAD, branch_id=branch_id, agent_id=agent_id, payload={"note": note}
        )

    # ---------------------------------------------------------------- leases

    async def acquire_lease(
        self, *, branch_id: str, agent_id: str, resource: str, ttl_seconds: float = 30
    ) -> LeaseGrant:
        """Grant a lease and record ``FILE_CLAIM_ACQUIRED`` atomically."""
        key = normalize_resource(resource)
        async with self.store.transaction(write=True) as db:
            token = await self.locks.acquire_in_transaction(db, key, agent_id, ttl_seconds)
            if token is None:
                lease = await self.locks.get_in_transaction(db, key)
                now = self.locks.clock()
                return LeaseGrant(
                    acquired=False,
                    resource=key,
                    ttl_seconds=float(ttl_seconds),
                    holder_agent_id=lease.holder_agent_id if lease else None,
                    expires_at=lease.expires_at if lease else None,
                    expires_in_seconds=round(lease.expires_at - now, 3) if lease else None,
                )
            lease = await self.locks.get_in_transaction(db, key)
            assert lease is not None
            claim = await self.store.append_in_transaction(
                db,
                new_event(
                    EventType.FILE_CLAIM_ACQUIRED,
                    branch_id=branch_id,
                    agent_id=agent_id,
                    payload={
                        "resource": key,
                        "fencing_token": token,
                        "ttl_seconds": lease.ttl_seconds,
                        "expires_at": lease.expires_at,
                    },
                ),
            )
        return LeaseGrant(
            acquired=True,
            resource=key,
            fencing_token=token,
            ttl_seconds=lease.ttl_seconds,
            expires_at=lease.expires_at,
            holder_agent_id=agent_id,
            expires_in_seconds=lease.ttl_seconds,
            claim_event_id=claim.event_id,
        )

    async def release_lease(self, *, branch_id: str, agent_id: str, resource: str, fencing_token: int) -> bool:
        """Release a lease and record ``FILE_CLAIM_RELEASED`` atomically."""
        key = normalize_resource(resource)
        async with self.store.transaction(write=True) as db:
            released = await self.locks.release_in_transaction(db, key, agent_id, fencing_token)
            if released:
                await self.store.append_in_transaction(
                    db,
                    new_event(
                        EventType.FILE_CLAIM_RELEASED,
                        branch_id=branch_id,
                        agent_id=agent_id,
                        payload={"resource": key, "fencing_token": fencing_token},
                    ),
                )
        return released

    async def heartbeat_lease(self, *, resource: str, fencing_token: int) -> bool:
        return await self.locks.heartbeat_lock(normalize_resource(resource), fencing_token)

    async def fenced_append(self, envelope: EventEnvelope, *, resource: str, fencing_token: int) -> EventEnvelope:
        """Append ``envelope`` only if ``fencing_token`` is the current,
        unexpired lease on ``resource`` held by ``envelope.agent_id``. The
        check and the append share one transaction."""
        key = normalize_resource(resource)
        async with self.store.transaction(write=True) as db:
            if not await self.locks.check_fence_in_transaction(db, key, fencing_token, envelope.agent_id):
                raise FencingTokenError(
                    f"fencing token {fencing_token} for {key!r} is not the active lease of "
                    f"{envelope.agent_id!r}; re-acquire the lease before writing"
                )
            persisted = await self.store.append_in_transaction(db, envelope)
        await self._auto_gc(envelope.branch_id)
        return persisted

    # ----------------------------------------------------------------- reads

    async def query_state(self, branch_id: str = DEFAULT_BRANCH, *, include_ephemeral: bool = False) -> dict[str, Any]:
        """Materialized branch state: ``[latest snapshot] + [delta]`` folded."""
        context = await self.store.context_events(branch_id)
        state = materialize(context.delta, seed=context.snapshot)
        now = self.locks.clock()
        recent = [e for e in context.delta if include_ephemeral or not e.is_ephemeral]
        return {
            "branch_id": branch_id,
            "head_event_id": context.head_event_id,
            "snapshot_event_id": context.snapshot.event_id if context.snapshot else None,
            "context_tokens": self.gc_engine.measure_slice(context),
            "tokenizer": self.gc_engine.counter.backend_name,
            "state": state.sections(),
            "active_leases": [lease.to_dict(now) for lease in await self.locks.list_leases()],
            "recent_events": [render_event(e) for e in recent],
        }

    async def context(self, branch_id: str = DEFAULT_BRANCH) -> str:
        """The exact text an agent should load as memory for ``branch_id``."""
        context = await self.store.context_events(branch_id)
        return render_context(context.snapshot, context.delta)

    async def history(self, branch_id: str = DEFAULT_BRANCH, *, limit: int = 50) -> list[dict[str, Any]]:
        """Most recent live events (newest last), for choosing rollback targets."""
        live = await self.store.read_live(branch_id)
        return [
            {
                "event_id": e.event_id,
                "sequence": e.sequence,
                "agent_id": e.agent_id,
                "event_type": e.event_type.value,
                "timestamp": e.timestamp_iso,
                "summary": render_event(e)[:240],
            }
            for e in live[-max(1, limit) :]
        ]

    # --------------------------------------------------------------- branching

    async def fork(
        self,
        *,
        source_branch: str,
        new_branch: str,
        agent_id: str,
        at_event_id: int | None = None,
        reason: str = "",
    ) -> EventEnvelope:
        return await self.store.fork_branch(
            source_branch, at_event_id, validate_branch_id(new_branch), agent_id=agent_id, reason=reason
        )

    async def rollback(self, *, branch_id: str, target_event_id: int, agent_id: str, reason: str = "") -> EventEnvelope:
        return await self.store.rollback(branch_id, target_event_id, agent_id=agent_id, reason=reason)

    async def diff(self, branch_a: str, branch_b: str) -> dict[str, Any]:
        return await self.store.get_branch_diff(branch_a, branch_b)

    async def verify(self, branch_id: str = DEFAULT_BRANCH) -> list[str]:
        return await self.store.verify_branch(branch_id)

    # ---------------------------------------------------------------------- gc

    async def gc(self, branch_id: str = DEFAULT_BRANCH, *, force: bool = False) -> GCResult:
        return await self.gc_engine.collect(branch_id, force=force)

    async def _auto_gc(self, branch_id: str) -> None:
        if not self.auto_gc:
            return
        try:
            await self.gc_engine.maybe_collect(branch_id)
        except Exception:
            # GC is an optimization; the append already committed.
            log.exception("automatic GC failed for branch %r", branch_id)
