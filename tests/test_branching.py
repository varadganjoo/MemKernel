"""Deterministic branch / fork / rollback / diff semantics."""

from __future__ import annotations

import pytest

from memkernel import AsyncEventStore, BranchExistsError, BranchNotFoundError, EventType
from memkernel.core.branch import LineageError, fold_live, merge_base
from memkernel.memory.state_view import materialize, materialize_live
from tests.conftest import decision, ev


async def _ids(store: AsyncEventStore, branch: str, *, live: bool = False) -> list[int]:
    events = await (store.read_live(branch) if live else store.read_stream(branch))
    return [e.event_id for e in events]  # type: ignore[misc]


async def test_fork_at_n_isolates_diverging_writes(store: AsyncEventStore) -> None:
    base = [await store.append_event(ev(note=f"base{i}")) for i in range(5)]
    n = base[2]
    fork_event = await store.fork_branch("main", n, "feature", agent_id="codex-cli")
    assert fork_event.event_type is EventType.BRANCH_FORKED
    assert fork_event.payload["fork_point_event_id"] == n
    assert fork_event.sequence == 4  # continues from the fork point's sequence (3)

    main_new = await store.append_event(decision("db", "postgres"))
    feat_new = await store.append_event(decision("db", "sqlite", branch="feature"))

    main_ids = await _ids(store, "main")
    feat_ids = await _ids(store, "feature")
    assert main_ids == base + [main_new]
    assert feat_ids == base[:3] + [fork_event.event_id, feat_new]
    assert feat_new not in main_ids and main_new not in feat_ids
    assert base[3] not in feat_ids and base[4] not in feat_ids

    feature = await store.get_branch("feature")
    assert (feature.parent_branch_id, feature.fork_event_id, feature.fork_sequence) == ("main", n, 3)
    assert [e.sequence for e in await store.read_stream("feature")] == [1, 2, 3, 4, 5]
    assert await store.verify_branch("feature") == []
    assert await store.verify_branch("main") == []

    # Copy-on-write: the fork added exactly one physical row (its audit event).
    async with store.transaction(write=False) as db:
        rows = await db.execute_fetchall("SELECT COUNT(*) FROM events WHERE branch_id = 'feature'")
    assert rows[0][0] == 2


async def test_fork_defaults_to_head_and_validates(store: AsyncEventStore) -> None:
    head = await store.append_event(ev(note="x"))
    fork = await store.fork_branch("main", None, "at-head")
    assert fork.payload["fork_point_event_id"] == head
    with pytest.raises(BranchExistsError):
        await store.fork_branch("main", None, "at-head")
    with pytest.raises(BranchNotFoundError):
        await store.fork_branch("missing", None, "x")
    await store.fork_branch("main", None, "other")
    other_only = await store.append_event(ev(branch="other", note="o"))
    with pytest.raises(ValueError, match="not in the lineage"):
        await store.fork_branch("main", other_only, "bad")
    with pytest.raises(ValueError):
        await store.fork_branch("main", None, "bad name!")


async def test_fork_of_empty_branch(store: AsyncEventStore) -> None:
    await store.create_branch("empty")
    fork = await store.fork_branch("empty", None, "empty-child")
    assert fork.payload["fork_point_event_id"] is None
    assert fork.sequence == 1 and fork.parent_event_id is None
    assert await _ids(store, "empty-child") == [fork.event_id]
    assert await store.verify_branch("empty-child") == []


async def test_rollback_truncates_live_timeline_but_keeps_audit(store: AsyncEventStore) -> None:
    good = await store.append_event(decision("auth", "oauth2-pkce"))
    bad1 = await store.append_event(decision("auth", "hallucinated-basic-auth"))
    bad2 = await store.append_event(ev(EventType.TOOL_EXECUTION_LOG, command="pytest", exit_code=1, output="boom"))

    rb = await store.rollback("main", good, agent_id="claude-code-cli", reason="tests failed")
    assert rb.payload["reverted_event_ids"] == [bad1, bad2]
    assert rb.payload["target_sequence"] == 1

    assert await _ids(store, "main", live=True) == [good, rb.event_id]
    assert await _ids(store, "main") == [good, bad1, bad2, rb.event_id]  # history intact
    state = materialize_live(await store.read_live("main"))
    assert state.decisions["auth"].value == "oauth2-pkce"
    assert state.execution_blockers == {}
    assert await store.verify_branch("main") == []

    with pytest.raises(ValueError, match="not on the live timeline"):
        await store.rollback("main", bad1, agent_id="x")


async def test_nested_rollbacks_and_resurrection_by_fork(store: AsyncEventStore) -> None:
    a = await store.append_event(ev(note="a"))
    b = await store.append_event(ev(note="b"))
    await store.rollback("main", a, agent_id="x")
    c = await store.append_event(ev(note="c"))
    rb2 = await store.rollback("main", a, agent_id="x")
    assert await _ids(store, "main", live=True) == [a, rb2.event_id]
    assert c not in await _ids(store, "main", live=True)

    # A reverted event cannot be a rollback target, but it can be forked from.
    await store.fork_branch("main", b, "postmortem")
    assert await _ids(store, "postmortem", live=True) == [a, b, (await store.get_branch("postmortem")).head_event_id]


async def test_diff_reports_merge_base_and_conflicts(store: AsyncEventStore) -> None:
    await store.append(decision("db", "postgres"))
    base = await store.append_event(decision("cache", "redis"))
    await store.fork_branch("main", base, "alt")
    await store.append(decision("db", "sqlite", branch="alt"))
    await store.append(decision("queue", "sqs", branch="alt"))
    await store.append(decision("ci", "github-actions"))

    diff = await store.get_branch_diff("main", "alt")
    assert diff["merge_base_event_id"] == base
    assert [c["key"] for c in diff["conflicting_decisions"]] == ["db"]
    conflict = diff["conflicting_decisions"][0]
    assert conflict["main"]["value"] == "postgres" and conflict["alt"]["value"] == "sqlite"
    assert set(diff["decisions_only_in_a"]) == {"ci"}
    assert set(diff["decisions_only_in_b"]) == {"queue"}
    assert len(diff["ahead_a"]) == 1 and len(diff["ahead_b"]) == 3  # fork audit + 2 decisions
    assert diff["identical_state"] is False

    same = await store.get_branch_diff("main", "main")
    assert same["identical_state"] is True and same["conflicting_decisions"] == []


async def test_replay_is_deterministic(tmp_path) -> None:
    """The same operation script yields the same live timeline and state."""

    async def script(path: str) -> tuple[list[tuple], dict]:
        async with AsyncEventStore(path) as s:
            await s.ensure_branch("main")
            a = await s.append_event(decision("x", "1"))
            await s.append_event(decision("x", "2"))
            await s.fork_branch("main", a, "f")
            await s.append_event(decision("y", "f", branch="f"))
            await s.rollback("main", a, agent_id="r")
            await s.append_event(decision("x", "3"))
            live = await s.read_live("main")
            shape = [(e.event_id, e.sequence, e.event_type.value, e.payload.get("decision_value")) for e in live]
            return shape, materialize_live(live).core_sections()

    first = await script(str(tmp_path / "a.db"))
    second = await script(str(tmp_path / "b.db"))
    assert first == second


def test_fold_live_is_pure_and_rejects_corrupt_lineage() -> None:
    from memkernel.core.events import EventEnvelope

    def mk(i: int, t: EventType = EventType.AGENT_SCRATCHPAD, **p: object) -> EventEnvelope:
        return EventEnvelope.model_construct(
            event_id=i, sequence=i, branch_id="m", agent_id="a", event_type=t, payload=p, metadata={}
        )

    raw = [mk(1), mk(2), mk(3, EventType.ROLLBACK_TRIGGERED, target_event_id=1), mk(4)]
    assert [e.event_id for e in fold_live(raw)] == [1, 3, 4]
    assert fold_live(raw) == fold_live(list(raw))
    with pytest.raises(LineageError):
        fold_live([mk(1), mk(2, EventType.ROLLBACK_TRIGGERED, target_event_id=99)])
    assert merge_base(raw, raw[:2]) is raw[1]
    assert materialize([]).core_sections() == materialize_live([]).core_sections()
