"""Lease contention, expiry, heartbeats and fencing-token enforcement."""

from __future__ import annotations

import asyncio

import pytest

from memkernel import AsyncEventStore, BranchNotFoundError, EventType, FencingTokenError, LockManager, MemKernel
from memkernel.clients.adapter import LeaseUnavailableError, MemKernelClient
from memkernel.core.events import new_event
from memkernel.kernel import normalize_resource
from tests.conftest import FakeClock


async def test_concurrent_agents_one_winner(store: AsyncEventStore, clock: FakeClock) -> None:
    locks = LockManager(store, clock=clock)
    results = await asyncio.gather(
        *(locks.acquire_lock("src/auth.py", agent, ttl_seconds=30) for agent in ("cursor-ide", "claude-code-cli", "codex-cli"))
    )
    winners = [r for r in results if r is not None]
    assert winners == [1]
    lease = await locks.get_lease("src/auth.py")
    assert lease is not None and lease.holder_agent_id in {"cursor-ide", "claude-code-cli", "codex-cli"}


async def test_contention_across_connections(db_path: str) -> None:
    """Two processes (separate connections) racing for the same file."""
    async with AsyncEventStore(db_path) as a, AsyncEventStore(db_path) as b:
        la, lb = LockManager(a), LockManager(b)
        for round_ in range(10):
            key = f"module_{round_}.py"
            ra, rb = await asyncio.gather(la.acquire_lock(key, "cursor-ide"), lb.acquire_lock(key, "codex-cli"))
            assert (ra is None) != (rb is None), (ra, rb)


async def test_lease_expiry_and_fencing(store: AsyncEventStore, clock: FakeClock) -> None:
    locks = LockManager(store, clock=clock)
    t1 = await locks.acquire_lock("src/db.py", "agent-a", ttl_seconds=30)
    assert t1 == 1
    assert await locks.acquire_lock("src/db.py", "agent-b", ttl_seconds=30) is None
    assert await locks.acquire_lock("src/db.py", "agent-a", ttl_seconds=30) is None  # not re-entrant

    clock.advance(29.9)
    assert await locks.acquire_lock("src/db.py", "agent-b") is None
    clock.advance(0.2)  # lease lapsed
    assert await locks.heartbeat_lock("src/db.py", t1, agent_id="agent-a") is False
    t2 = await locks.acquire_lock("src/db.py", "agent-b", ttl_seconds=30)
    assert t2 == 2 and t2 > t1

    # The paused former holder is fenced off.
    assert await locks.validate_fencing_token("src/db.py", t1) is False
    assert await locks.release_lock("src/db.py", "agent-a", t1) is False
    assert await locks.validate_fencing_token("src/db.py", t2, "agent-b") is True
    assert await locks.release_lock("src/db.py", "agent-b", t2) is True
    assert await locks.release_lock("src/db.py", "agent-b", t2) is False  # already released
    assert await locks.acquire_lock("src/db.py", "agent-a") == 3  # tokens never reused


async def test_heartbeat_extends_lease(store: AsyncEventStore, clock: FakeClock) -> None:
    locks = LockManager(store, clock=clock)
    token = await locks.acquire_lock("a.py", "agent-a", ttl_seconds=10)
    assert token is not None
    for _ in range(5):
        clock.advance(8)
        assert await locks.heartbeat_lock("a.py", token, agent_id="agent-a") is True
    assert await locks.acquire_lock("a.py", "agent-b") is None
    assert await locks.heartbeat_lock("a.py", token + 1, agent_id="agent-a") is False
    clock.advance(10.01)
    assert await locks.acquire_lock("a.py", "agent-b") == token + 1


async def test_heartbeat_requires_holder_and_preserves_expiry(kernel: MemKernel, clock: FakeClock) -> None:
    grant = await kernel.acquire_lease(branch_id="main", agent_id="owner", resource="src/a.py", ttl_seconds=10)
    token = grant.fencing_token
    clock.advance(5)
    assert not await kernel.heartbeat_lease(resource="./src\\a.py", fencing_token=token, agent_id="other")
    lease = await kernel.locks.get_lease("src/a.py")
    assert lease.expires_at == grant.expires_at
    assert await kernel.heartbeat_lease(resource="./src\\a.py", fencing_token=token, agent_id="owner")
    assert (await kernel.locks.get_lease("src/a.py")).expires_at == clock() + 10
    assert await kernel.release_lease(branch_id="main", agent_id="owner", resource="src/a.py", fencing_token=token)
    assert not await kernel.heartbeat_lease(resource="src/a.py", fencing_token=token, agent_id="owner")
    assert not await kernel.heartbeat_lease(resource="missing.py", fencing_token=token, agent_id="owner")


@pytest.mark.parametrize("token", [True, False, 1.0, "1", 0, -1])
async def test_fence_tokens_cannot_be_coerced(kernel: MemKernel, token: object) -> None:
    await kernel.acquire_lease(branch_id="main", agent_id="owner", resource="a.py")
    with pytest.raises(ValueError, match="positive integer"):
        await kernel.heartbeat_lease(resource="a.py", fencing_token=token, agent_id="owner")
    with pytest.raises(ValueError, match="positive integer"):
        await kernel.release_lease(branch_id="main", agent_id="owner", resource="a.py", fencing_token=token)
    with pytest.raises(ValueError, match="positive integer"):
        await kernel.fenced_append(
            new_event(EventType.AGENT_SCRATCHPAD, branch_id="main", agent_id="owner", payload={"note": "bad"}),
            resource="a.py", fencing_token=token,
        )
    assert (await kernel.locks.get_lease("a.py")).holder_agent_id == "owner"


async def test_real_time_expiry(store: AsyncEventStore) -> None:
    locks = LockManager(store)
    assert await locks.acquire_lock("slow.py", "agent-a", ttl_seconds=0.2) == 1
    assert await locks.acquire_lock("slow.py", "agent-b", ttl_seconds=0.2) is None
    await asyncio.sleep(0.35)
    assert await locks.acquire_lock("slow.py", "agent-b", ttl_seconds=0.2) == 2
    assert [l.holder_agent_id for l in await locks.list_leases()] == ["agent-b"]


async def test_invalid_arguments(store: AsyncEventStore) -> None:
    locks = LockManager(store)
    for ttl in (0, -1, float("inf"), float("nan"), 10**9):
        with pytest.raises(ValueError):
            await locks.acquire_lock("x.py", "agent", ttl_seconds=ttl)
    with pytest.raises(ValueError):
        await locks.acquire_lock("", "agent")
    with pytest.raises(ValueError):
        await locks.acquire_lock("x.py", "bad agent")


def test_normalize_resource() -> None:
    assert normalize_resource("./src\\auth//login.py") == "src/auth/login.py"
    assert normalize_resource("src/a/../b.py") == "src/b.py"
    for bad in ("", "  ", ".", "/"):
        with pytest.raises(ValueError):
            normalize_resource(bad)


async def test_kernel_records_claims_atomically(kernel: MemKernel, clock: FakeClock) -> None:
    grant = await kernel.acquire_lease(branch_id="main", agent_id="cursor-ide", resource="./src/x.py", ttl_seconds=20)
    assert grant.acquired and grant.resource == "src/x.py" and grant.fencing_token == 1
    denied = await kernel.acquire_lease(branch_id="main", agent_id="codex-cli", resource="src/x.py")
    assert not denied.acquired and denied.holder_agent_id == "cursor-ide"
    assert denied.expires_in_seconds == pytest.approx(20)

    stream = await kernel.store.read_stream("main")
    assert [e.event_type for e in stream] == [EventType.FILE_CLAIM_ACQUIRED]  # denial wrote nothing
    state = (await kernel.query_state("main"))["state"]
    assert state["active_claims"]["src/x.py"]["agent_id"] == "cursor-ide"

    assert await kernel.release_lease(branch_id="main", agent_id="cursor-ide", resource="src/x.py", fencing_token=1)
    state = (await kernel.query_state("main"))["state"]
    assert state["active_claims"] == {}

    # A claim on a missing branch rolls back the lease grant too.
    with pytest.raises(BranchNotFoundError):
        await kernel.acquire_lease(branch_id="nope", agent_id="cursor-ide", resource="src/y.py")
    assert await kernel.locks.get_lease("src/y.py") is None


async def test_fenced_append_rejects_stale_tokens(kernel: MemKernel, clock: FakeClock) -> None:
    a = await kernel.acquire_lease(branch_id="main", agent_id="agent-a", resource="core.py", ttl_seconds=5)
    write = lambda agent: new_event(  # noqa: E731
        EventType.TOOL_EXECUTION_LOG, branch_id="main", agent_id=agent, payload={"command": "edit core.py"}
    )
    ok = await kernel.fenced_append(write("agent-a"), resource="core.py", fencing_token=a.fencing_token)  # type: ignore[arg-type]
    assert ok.event_id is not None

    clock.advance(6)  # agent-a stalls past its TTL; agent-b takes over
    b = await kernel.acquire_lease(branch_id="main", agent_id="agent-b", resource="core.py")
    assert b.fencing_token == 2
    with pytest.raises(FencingTokenError):
        await kernel.fenced_append(write("agent-a"), resource="core.py", fencing_token=1)
    with pytest.raises(FencingTokenError):  # right token, wrong agent
        await kernel.fenced_append(write("agent-a"), resource="core.py", fencing_token=2)
    await kernel.fenced_append(write("agent-b"), resource="core.py", fencing_token=2)


async def test_client_lease_context_waits_heartbeats_and_releases(db_path: str) -> None:
    kernel = await MemKernel.open(db_path, tokenizer="heuristic", auto_gc=False)
    try:
        a = MemKernelClient(kernel, "cursor-ide")
        b = MemKernelClient(kernel, "codex-cli")
        order: list[str] = []

        async def holder() -> None:
            async with a.lease("shared.py", ttl_seconds=0.3) as handle:
                order.append("a-start")
                await asyncio.sleep(0.5)  # longer than TTL: heartbeats must keep it alive
                assert not handle.lost
                order.append("a-end")

        async def waiter() -> None:
            await asyncio.sleep(0.05)
            with pytest.raises(LeaseUnavailableError):
                async with b.lease("shared.py", ttl_seconds=1):
                    pass
            async with b.lease("shared.py", ttl_seconds=1, wait_seconds=3, poll_interval=0.05) as handle:
                order.append("b-start")
                assert handle.fencing_token == 2

        await asyncio.gather(holder(), waiter())
        assert order == ["a-start", "a-end", "b-start"]
        assert await kernel.locks.list_leases() == []
    finally:
        await kernel.close()
