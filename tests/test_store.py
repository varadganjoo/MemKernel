"""WAL persistence, monotonicity, immutability and lineage traversal."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from memkernel import AsyncEventStore, BranchNotFoundError, EventType, StaleHeadError
from memkernel.core.events import GENESIS_HASH, MAX_EVENT_BYTES, EventEnvelope, new_event
from tests.conftest import decision, ev


async def test_wal_mode_enabled(store: AsyncEventStore) -> None:
    assert store.journal_mode == "wal"


async def test_append_assigns_monotonic_ids_sequences_and_chain(store: AsyncEventStore) -> None:
    ids = [await store.append_event(ev(note=f"n{i}")) for i in range(5)]
    assert ids == sorted(ids) and len(set(ids)) == 5

    stream = await store.read_stream("main")
    assert [e.event_id for e in stream] == ids
    assert [e.sequence for e in stream] == [1, 2, 3, 4, 5]
    assert stream[0].parent_event_id is None
    assert [e.parent_event_id for e in stream[1:]] == ids[:-1]
    assert all(e.timestamp.tzinfo is not None for e in stream)
    assert stream[0].hash == stream[0].compute_hash(GENESIS_HASH)
    assert await store.verify_branch("main") == []

    branch = await store.get_branch("main")
    assert (branch.head_event_id, branch.head_sequence, branch.head_hash) == (ids[-1], 5, stream[-1].hash)


async def test_read_stream_from_event_id_is_inclusive(store: AsyncEventStore) -> None:
    ids = [await store.append_event(ev(note=str(i))) for i in range(4)]
    tail = await store.read_stream("main", from_event_id=ids[2])
    assert [e.event_id for e in tail] == ids[2:]


async def test_append_returns_new_envelope_without_mutating_input(store: AsyncEventStore) -> None:
    original = ev(note="x")
    persisted = await store.append(original)
    assert original.event_id is None and original.hash is None
    assert persisted.event_id is not None and persisted.payload == {"note": "x"}
    with pytest.raises(ValueError, match="already persisted"):
        await store.append(persisted)


async def test_persistence_across_reopen(db_path: str) -> None:
    async with AsyncEventStore(db_path) as s:
        await s.ensure_branch("main")
        for i in range(3):
            await s.append(decision(f"k{i}", f"v{i}"))
        before = await s.read_stream("main")
    async with AsyncEventStore(db_path) as s:
        after = await s.read_stream("main")
        assert [e.to_dict() for e in after] == [e.to_dict() for e in before]
        assert await s.verify_branch("main") == []
        next_id = await s.append_event(ev(note="after reopen"))
        assert next_id == before[-1].event_id + 1  # type: ignore[operator]


async def test_events_table_is_append_only(store: AsyncEventStore) -> None:
    await store.append(ev(note="immutable"))
    async with store.transaction(write=False) as db:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            await db.execute("UPDATE events SET payload = '{}'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            await db.execute("DELETE FROM events")


async def test_hash_chain_detects_tampering(store: AsyncEventStore) -> None:
    for i in range(3):
        await store.append(decision("db", f"v{i}"))
    target = (await store.read_stream("main"))[1]
    async with store.transaction(write=True) as db:  # simulate an attacker with raw file access
        await db.execute("DROP TRIGGER events_no_update")
        await db.execute(
            "UPDATE events SET payload = ? WHERE event_id = ?",
            ('{"decision_key":"db","decision_value":"evil","rationale":""}', target.event_id),
        )
    problems = await store.verify_branch("main")
    assert any(f"event {target.event_id}: hash mismatch" in p for p in problems)


async def test_optimistic_concurrency_with_parent_event_id(store: AsyncEventStore) -> None:
    head = await store.append_event(ev(note="a"))
    ok = await store.append(new_event(EventType.AGENT_SCRATCHPAD, branch_id="main", agent_id="x",
                                      payload={"note": "b"}, parent_event_id=head))
    assert ok.parent_event_id == head
    with pytest.raises(StaleHeadError) as info:
        await store.append(new_event(EventType.AGENT_SCRATCHPAD, branch_id="main", agent_id="x",
                                     payload={"note": "c"}, parent_event_id=head))
    assert info.value.actual == ok.event_id


async def test_rejects_invalid_events(store: AsyncEventStore) -> None:
    with pytest.raises(BranchNotFoundError):
        await store.append(ev(branch="nope"))
    with pytest.raises(ValueError, match="kernel-issued"):
        await store.append(ev(EventType.ROLLBACK_TRIGGERED, target_event_id=1))
    with pytest.raises(ValueError, match="kernel-issued"):
        await store.append(ev(EventType.BRANCH_FORKED))
    with pytest.raises(ValueError, match="schema"):
        await store.append(ev(EventType.SEMANTIC_SNAPSHOT, covers_through_event_id=1))
    with pytest.raises(ValueError):
        ev(blob="x" * (MAX_EVENT_BYTES + 1))
    with pytest.raises(ValueError):
        EventEnvelope(event_type=EventType.AGENT_SCRATCHPAD, branch_id="main", agent_id="x",
                      payload={"obj": object()})
    with pytest.raises(ValueError):
        ev(branch="bad branch name")
    with pytest.raises(ValueError):
        ev(agent="agent with spaces")


async def test_forged_snapshot_is_rejected(store: AsyncEventStore) -> None:
    await store.append(ev(note="a"))
    with pytest.raises(ValueError, match="not live"):
        await store.append(
            ev(EventType.SEMANTIC_SNAPSHOT, schema="memkernel.snapshot/v1",
               covers_through_event_id=999, covers_through_sequence=1)
        )


async def test_concurrent_appends_are_serialized(store: AsyncEventStore) -> None:
    await asyncio.gather(*(store.append(ev(note=str(i))) for i in range(50)))
    stream = await store.read_stream("main")
    assert [e.sequence for e in stream] == list(range(1, 51))
    assert await store.verify_branch("main") == []


async def test_two_connections_share_one_file(db_path: str) -> None:
    """Simulates two MCP server processes appending to one database."""
    async with AsyncEventStore(db_path) as a, AsyncEventStore(db_path) as b:
        await a.ensure_branch("main")
        await b.ensure_branch("main")
        await asyncio.gather(
            *(a.append(ev(agent="cursor-ide", note=str(i))) for i in range(25)),
            *(b.append(ev(agent="claude-code-cli", note=str(i))) for i in range(25)),
        )
        stream = await a.read_stream("main")
        assert [e.sequence for e in stream] == list(range(1, 51))
        assert {e.agent_id for e in stream} == {"cursor-ide", "claude-code-cli"}
        assert await b.verify_branch("main") == []


async def test_lineage_traversal_across_nested_forks(store: AsyncEventStore) -> None:
    g = [await store.append_event(ev(note=f"g{i}")) for i in range(3)]
    await store.fork_branch("main", g[2], "parent")
    p_own = await store.append_event(ev(branch="parent", note="p0"))
    # Fork the child from an event the parent *inherited* from main.
    await store.fork_branch("parent", g[1], "child")
    c_own = await store.append_event(ev(branch="child", note="c0"))
    await store.append_event(ev(note="g3"))  # later main events are invisible to both

    child = await store.read_stream("child")
    assert [e.event_id for e in child][:2] == g[:2]
    assert c_own in [e.event_id for e in child]
    assert p_own not in [e.event_id for e in child]
    assert [e.sequence for e in child] == list(range(1, len(child) + 1))

    parent = await store.read_stream("parent")
    assert [e.event_id for e in parent][:3] == g
    for branch in ("main", "parent", "child"):
        assert await store.verify_branch(branch) == [], branch
