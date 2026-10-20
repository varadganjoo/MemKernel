"""Portable advisory notices, independent of branch history and local leases."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from memkernel import MemKernel
from memkernel.core.store import StoreError


async def test_two_databases_exchange_clear_and_stale_replay(kernel, tmp_path):
    async with await MemKernel.open(tmp_path / "other.db", tokenizer="heuristic") as other:
        notice = await kernel.notices.post(
            agent_id="alice", resource="service:auth", message="Changing login, please coordinate first"
        )
        original = await kernel.notices.export_json()
        assert await other.notices.import_json(original) == 1
        assert await other.notices.import_json(original) == 0
        visible = (await other.query_state())["coordination_notices"]
        assert visible[0]["notice_id"] == notice.notice_id
        assert visible[0]["status"] == "active"
        assert visible[0]["author"] == "alice"
        assert "Changing login" in await other.context()
        # An imported advisory never occupies a local lease.
        assert (await other.acquire_lease(branch_id="main", agent_id="bob", resource="auth.py")).acquired
        await kernel.notices.clear(notice.notice_id, agent_id="alice")
        assert await other.notices.import_json(await kernel.notices.export_json()) == 1
        assert await other.notices.import_json(original) == 0
        assert await other.notices.list() == []
        assert (await other.notices.list(include_cleared=True))[0]["status"] == "cleared"
        assert await other.notices.export_json() == await kernel.notices.export_json()


async def test_expiry_is_stale_not_cleared(kernel):
    now = datetime(2026, 10, 20, tzinfo=UTC)
    kernel.notices.clock = lambda: now
    notice = await kernel.notices.post(agent_id="alice", resource="service:auth", message="Working", ttl_seconds=10)
    assert (await kernel.notices.list())[0]["status"] == "active"
    now += timedelta(seconds=10)
    stale = (await kernel.notices.list())[0]
    assert stale["status"] == "stale" and stale["cleared_at"] is None
    assert "stale" in await kernel.context()
    state = await kernel.query_state()
    assert state["context_tokens"] == kernel.gc_engine.counter.count(await kernel.context())
    assert state["coordination_notice_tokens"] > 0
    await kernel.notices.clear(notice.notice_id, agent_id="alice")
    assert await kernel.notices.list() == []


async def test_branch_rollback_and_gc_do_not_resurrect_notices(kernel):
    first = await kernel.note(branch_id="main", agent_id="alice", note="before")
    notice = await kernel.notices.post(agent_id="alice", resource="service:auth", message="Working")
    await kernel.fork(source_branch="main", new_branch="investigation", agent_id="bob")
    assert (await kernel.query_state("investigation"))["coordination_notices"][0]["notice_id"] == notice.notice_id
    await kernel.notices.clear(notice.notice_id, agent_id="alice")
    await kernel.note(branch_id="main", agent_id="alice", note="after")
    await kernel.gc("main", force=True)
    await kernel.rollback(branch_id="main", target_event_id=first.event_id, agent_id="alice")
    for branch in ("main", "investigation"):
        assert (await kernel.query_state(branch))["coordination_notices"] == []
        assert not await kernel.verify(branch)


async def test_clear_is_idempotent_and_requires_matching_self_asserted_author(kernel):
    notice = await kernel.notices.post(agent_id="alice", resource="service:auth", message="Working")
    with pytest.raises(ValueError, match="author"):
        await kernel.notices.clear(notice.notice_id, agent_id="bob")
    cleared = await kernel.notices.clear(notice.notice_id, agent_id="alice")
    assert await kernel.notices.clear(notice.notice_id, agent_id="alice") == cleared
    with pytest.raises(ValueError, match="not found"):
        await kernel.notices.clear("00000000-0000-4000-8000-000000000000", agent_id="alice")


async def test_clear_arriving_before_active_record_stays_cleared(kernel, tmp_path):
    notice = await kernel.notices.post(agent_id="alice", resource="service:auth", message="Working")
    active = await kernel.notices.export_json()
    await kernel.notices.clear(notice.notice_id, agent_id="alice")
    async with await MemKernel.open(tmp_path / "fresh.db", tokenizer="heuristic") as other:
        assert await other.notices.import_json(await kernel.notices.export_json()) == 1
        assert await other.notices.import_json(active) == 0
        assert await other.notices.list() == []


async def test_concurrent_clear_merge_converges_without_clock_ordering(kernel, tmp_path):
    notice = await kernel.notices.post(agent_id="alice", resource="service:auth", message="Working")
    async with await MemKernel.open(tmp_path / "other.db", tokenizer="heuristic") as other:
        await other.notices.import_json(await kernel.notices.export_json())
        future = datetime.now(UTC) + timedelta(hours=2)
        kernel.notices.clock = lambda: future
        other.notices.clock = lambda: future + timedelta(hours=1)
        await kernel.notices.clear(notice.notice_id, agent_id="alice")
        await other.notices.clear(notice.notice_id, agent_id="alice")
        left, right = await kernel.notices.export_json(), await other.notices.export_json()
        await kernel.notices.import_json(right)
        await other.notices.import_json(left)
        assert await kernel.notices.export_json() == await other.notices.export_json()


@pytest.mark.parametrize("changes", [
    {"message": ""}, {"message": "   "}, {"message": "a" * 4097},
    {"resource": "C:/work/auth.py"}, {"resource": "auth"}, {"agent_id": "has spaces"},
    {"ttl_seconds": 0}, {"ttl_seconds": -1}, {"ttl_seconds": 604801},
    {"ttl_seconds": True}, {"ttl_seconds": 1.5}, {"message": 123},
])
async def test_post_validation_has_no_side_effects(kernel, changes):
    args = {"agent_id": "alice", "resource": "service:auth", "message": "Working", **changes}
    with pytest.raises(ValueError):
        await kernel.notices.post(**args)
    assert await kernel.notices.list(include_cleared=True) == []


@pytest.mark.parametrize("corrupt", [
    lambda doc: doc.update(schema="memkernel.notices/v2"),
    lambda doc: doc.update(extra="unexpected"),
    lambda doc: doc["notices"][0].update(created_at="2026-10-20T00:00:00"),
    lambda doc: doc["notices"][0].update(expires_at="2020-01-01T00:00:00Z"),
    lambda doc: doc["notices"][0].update(cleared_at="2020-01-01T00:00:00Z"),
    lambda doc: doc["notices"][0].update(author=False),
    lambda doc: doc["notices"][0].update(notice_id="not-a-uuid"),
    lambda doc: doc["notices"][0].update(created_at=1),
    lambda doc: doc["notices"][0].update(created_at="1"),
    lambda doc: doc["notices"][0].update(message="\ud800"),
    lambda doc: doc["notices"][0].update(fencing_token=1),
    lambda doc: doc["notices"].append(doc["notices"][0]),
])
async def test_invalid_import_is_atomic(kernel, tmp_path, corrupt):
    await kernel.notices.post(agent_id="alice", resource="service:auth", message="Working")
    doc = json.loads(await kernel.notices.export_json())
    corrupt(doc)
    async with await MemKernel.open(tmp_path / "other.db", tokenizer="heuristic") as other:
        with pytest.raises(ValueError):
            await other.notices.import_json(json.dumps(doc))
        assert await other.notices.list(include_cleared=True) == []


async def test_conflict_rolls_back_earlier_valid_import_in_same_batch(kernel, tmp_path):
    original = await kernel.notices.post(agent_id="alice", resource="service:auth", message="Working")
    async with await MemKernel.open(tmp_path / "other.db", tokenizer="heuristic") as other:
        await other.notices.import_json(await kernel.notices.export_json())
        await kernel.notices.post(agent_id="bob", resource="service:billing", message="Also working")
        doc = json.loads(await kernel.notices.export_json())
        conflicting = next(n for n in doc["notices"] if n["notice_id"] == original.notice_id)
        conflicting["message"] = "Changed immutable body"
        doc["notices"].sort(key=lambda n: n["notice_id"] == original.notice_id)
        before = await other.notices.export_json()
        with pytest.raises(ValueError, match="conflict"):
            await other.notices.import_json(json.dumps(doc))
        assert await other.notices.export_json() == before


@pytest.mark.parametrize("raw", [
    '{"schema":"memkernel.notices/v1","notices":[],"notices":[]}',
    '{"schema":"memkernel.notices/v1","notices":NaN}',
    '[]', '{', '[' * 2000 + ']' * 2000, ' ' * (1024 * 1024 + 1),
], ids=["duplicate-key", "nan", "array", "truncated", "too-deep", "too-large"])
async def test_bad_json_is_a_readable_error(kernel, raw):
    with pytest.raises(ValueError):
        await kernel.notices.import_json(raw)


async def test_export_selection_does_not_include_other_memory(kernel):
    one = await kernel.notices.post(agent_id="alice", resource="service:auth", message="Share this")
    await kernel.notices.post(agent_id="alice", resource="service:private", message="Do not share")
    await kernel.note(branch_id="main", agent_id="alice", note="Secret scratchpad")
    await kernel.acquire_lease(branch_id="main", agent_id="alice", resource="private.py")
    exported = await kernel.notices.export_json(notice_ids=[one.notice_id])
    assert "Share this" in exported
    for excluded in ("Do not share", "Secret scratchpad", "private.py", "fencing_token", "event_id"):
        assert excluded not in exported
    assert len(await kernel.notices.list(resource="service:auth")) == 1
    assert json.loads(await kernel.notices.export_json(notice_ids=[]))["notices"] == []
    with pytest.raises(ValueError, match="not found"):
        await kernel.notices.export_json(notice_ids=["00000000-0000-4000-8000-000000000000"])


async def test_import_limits_on_count_and_utf8_bytes(kernel):
    from uuid import uuid4

    notice = await kernel.notices.post(agent_id="alice", resource="service:auth", message="Working")
    doc = json.loads(await kernel.notices.export_json())
    doc["notices"] = [{**doc["notices"][0], "notice_id": str(uuid4())} for _ in range(1001)]
    before = await kernel.notices.export_json()
    with pytest.raises(ValueError):
        await kernel.notices.import_json(json.dumps(doc))
    # Under 1 Mi characters, but over 1 MiB when encoded.
    with pytest.raises(ValueError, match="1 MiB"):
        await kernel.notices.import_json('"' + "\u00e9" * 600000 + '"')
    assert await kernel.notices.export_json() == before
    assert (await kernel.notices.list())[0]["notice_id"] == notice.notice_id


async def test_equivalent_timezone_offsets_are_not_conflicts_and_absence_does_not_clear(kernel):
    kernel.notices.clock = lambda: datetime(2026, 10, 20, tzinfo=UTC)
    await kernel.notices.post(agent_id="alice", resource="service:auth", message="Working")
    doc = json.loads(await kernel.notices.export_json())
    doc["notices"][0]["created_at"] = "2026-10-19T18:00:00-06:00"
    assert await kernel.notices.import_json(json.dumps(doc)) == 0
    assert await kernel.notices.import_json('{"schema":"memkernel.notices/v1","notices":[]}') == 0
    assert len(await kernel.notices.list()) == 1


async def test_same_database_concurrent_imports_are_idempotent(kernel, db_path, tmp_path):
    async with await MemKernel.open(tmp_path / "source.db", tokenizer="heuristic") as source:
        await source.notices.post(agent_id="alice", resource="service:auth", message="Working")
        bundle = await source.notices.export_json()
    async with await MemKernel.open(db_path, tokenizer="heuristic") as peer:
        counts = await asyncio.gather(*[
            (kernel if i % 2 else peer).notices.import_json(bundle) for i in range(20)
        ])
        assert sum(counts) == 1
        assert len(await kernel.notices.list()) == 1


async def test_persistence_read_only_and_legacy_database(tmp_path):
    path = tmp_path / "existing.db"
    async with await MemKernel.open(path, tokenizer="heuristic") as kernel:
        await kernel.note(branch_id="main", agent_id="alice", note="keep me")
    # Simulate the exact pre-extension database: schema v1, no notice table.
    with sqlite3.connect(path) as db:
        db.execute("DROP TABLE coordination_notices")
    async with await MemKernel.open(path, tokenizer="heuristic", read_only=True) as reader:
        assert (await reader.query_state())["coordination_notices"] == []
        assert json.loads(await reader.notices.export_json())["notices"] == []
    async with await MemKernel.open(path, tokenizer="heuristic") as upgraded:
        assert len(await upgraded.store.read_stream("main")) == 1
        await upgraded.notices.post(agent_id="alice", resource="service:auth", message="Persisted")
    async with await MemKernel.open(path, tokenizer="heuristic", read_only=True) as reader:
        notice = (await reader.notices.list())[0]
        assert notice["message"] == "Persisted"
        with pytest.raises(StoreError, match="read-only"):
            await reader.notices.post(agent_id="alice", resource="service:auth", message="Denied")
        with pytest.raises(StoreError, match="read-only"):
            await reader.notices.clear(notice["notice_id"], agent_id="alice")
        with pytest.raises(StoreError, match="read-only"):
            await reader.notices.import_json(await reader.notices.export_json())
