"""Token-budget GC, semantic consolidation and snapshot soundness."""

from __future__ import annotations

import asyncio
import os

import pytest

from memkernel import AsyncEventStore, EventType, GCConfig, LLMSummarizer, MemKernel, MemoryGCEngine, TokenCounter
from memkernel.core.branch import fold_live
from memkernel.memory.state_view import SNAPSHOT_SCHEMA, WorkspaceState, materialize, render_context, split_context
from tests.conftest import decision, ev

NOISY_OUTPUT = "\n".join(
    f"tests/test_api.py::test_endpoint_{i} PASSED [{i}%]  (0.0{i}s) warnings: DeprecationWarning in urllib3"
    for i in range(25)
)


def tool_log(i: int, *, command: str | None = None, exit_code: int = 0, facts: dict | None = None):
    payload = {"command": command or f"pytest -k case_{i}", "exit_code": exit_code, "output": NOISY_OUTPUT}
    if facts:
        payload["facts"] = facts
    return ev(EventType.TOOL_EXECUTION_LOG, agent="claude-code-cli", **payload)


def engine(store: AsyncEventStore, threshold: int = 4000, keep: int = 4, **kw) -> MemoryGCEngine:
    return MemoryGCEngine(
        store,
        counter=TokenCounter("heuristic"),
        # A huge tail budget makes retention purely count-based for these tests.
        config=GCConfig(max_token_threshold=threshold, retain_recent_events=keep, retain_token_budget=10**9),
        **kw,
    )


async def seed_noisy_history(store: AsyncEventStore) -> None:
    await store.append(ev(EventType.AGENT_TASK_INIT, agent="cursor-ide", directive="Add OAuth login",
                          constraints=["no new deps"]))
    await store.append(decision("auth.method", "OAuth2 PKCE", agent="cursor-ide"))
    for i in range(20):
        facts = {"python_version": "3.13" if i < 10 else "3.14", "db_migrated": i >= 5}
        exit_code = 1 if i in (3, 17) else 0
        command = "pytest tests/test_auth.py" if i in (3, 17, 12) else None
        await store.append(tool_log(i, command=command, exit_code=exit_code, facts=facts))
        if i == 3:  # the failure is fixed immediately...
            await store.append(tool_log(i, command="pytest tests/test_auth.py", exit_code=0))
    await store.append(decision("auth.method", "OAuth2 PKCE + refresh rotation", agent="codex-cli"))
    await store.append(ev(note="token refresh races with logout; investigate"))


async def test_twenty_noisy_logs_compact_by_more_than_half(store: AsyncEventStore) -> None:
    for i in range(20):
        await store.append(tool_log(i, facts={"tests_passing": i}))
    gc = engine(store, threshold=1000)
    before = await gc.measure("main")
    assert before > 1000

    result = await gc.maybe_collect("main")
    assert result.triggered and result.reason == "compacted"
    assert result.compacted_event_count == 16 and result.retained_event_count == 4
    assert result.tokens_before == before
    assert result.tokens_after == await gc.measure("main")
    assert result.reduction_ratio > 0.5
    assert result.tokens_after < 0.5 * result.tokens_before

    snapshot = await store.get_event(result.snapshot_event_id)  # type: ignore[arg-type]
    assert snapshot.event_type is EventType.SEMANTIC_SNAPSHOT
    assert snapshot.payload["schema"] == SNAPSHOT_SCHEMA
    assert snapshot.payload["compacted_event_count"] == 16
    assert snapshot.payload["active_workspace_facts"]["tests_passing"]["value"] == 15
    WorkspaceState.from_snapshot(snapshot)  # valid snapshot schema
    assert (await store.get_branch("main")).snapshot_event_id == snapshot.event_id
    assert await store.verify_branch("main") == []

    context = await store.context_events("main")
    assert context.fast_path and context.snapshot == snapshot
    assert [e.payload["facts"]["tests_passing"] for e in context.delta] == [16, 17, 18, 19]
    # Nothing was deleted.
    assert len(await store.read_stream("main")) == 21


async def test_snapshot_consolidates_facts_assumptions_blockers(store: AsyncEventStore) -> None:
    await seed_noisy_history(store)
    await store.append(tool_log(99, command="npm run build", exit_code=2))
    result = await engine(store, threshold=500, keep=0).collect("main")
    assert result.triggered

    snap = (await store.context_events("main")).snapshot
    assert snap is not None
    p = snap.payload
    assert p["tasks"]["cursor-ide"]["constraints"] == ["no new deps"]
    assert p["decisions"]["auth.method"]["value"] == "OAuth2 PKCE + refresh rotation"
    assert p["decisions"]["auth.method"]["agent_id"] == "codex-cli"
    assert p["active_workspace_facts"]["python_version"]["value"] == "3.14"
    deprecated = {(d["kind"], d["key"], d["value"]) for d in p["deprecated_assumptions"]}
    assert ("decision", "auth.method", "OAuth2 PKCE") in deprecated
    assert ("fact", "python_version", "3.13") in deprecated
    assert ("fact", "db_migrated", False) in deprecated
    # pytest failed at i=17 and never recovered; npm build failed.
    assert set(p["execution_blockers"]) == {"pytest tests/test_auth.py", "npm run build"}
    assert p["execution_blockers"]["pytest tests/test_auth.py"]["occurrences"] == 1
    assert p["execution_digest"]["pytest tests/test_auth.py"]["runs"] == 4
    assert any("token refresh races" in i for i in p["insights"])


async def test_gc_is_sound_state_is_preserved(store: AsyncEventStore) -> None:
    """materialize(snapshot + delta) == materialize(full history) across repeated GCs."""
    gc = engine(store, threshold=300, keep=3)
    await seed_noisy_history(store)
    full_before = materialize(fold_live(await store.read_stream("main"))).core_sections()
    first = await gc.collect("main")
    assert first.triggered
    for i in range(12):
        await store.append(tool_log(100 + i, facts={"iteration": i}))
    await store.append(decision("cache", "redis"))
    second = await gc.collect("main")
    assert second.triggered and second.snapshot_event_id != first.snapshot_event_id
    snap = await store.get_event(second.snapshot_event_id)  # type: ignore[arg-type]
    assert snap.payload["base_snapshot_event_id"] == first.snapshot_event_id

    context = await store.context_events("main")
    via_snapshot = materialize(context.delta, seed=context.snapshot).core_sections()
    live = fold_live(await store.read_stream("main"))
    full = materialize(live).core_sections()  # snapshots skipped: pure replay of history
    assert via_snapshot == full
    assert full_before["decisions"]["auth.method"] == full["decisions"]["auth.method"]


async def test_fast_and_slow_paths_agree_after_rollback(store: AsyncEventStore) -> None:
    gc = engine(store, threshold=300, keep=2)
    for i in range(10):
        await store.append(tool_log(i, facts={"step": i}))
    first = await gc.collect("main")
    assert first.triggered
    after_first = await store.append_event(decision("k", "v1"))
    for i in range(10, 20):
        await store.append(tool_log(i, facts={"step": i}))
    second = await gc.collect("main")
    assert second.triggered

    # Roll back to before the second snapshot: pointer must fall back to the first.
    await store.rollback("main", after_first, agent_id="claude-code-cli")
    branch = await store.get_branch("main")
    assert branch.snapshot_event_id == first.snapshot_event_id
    context = await store.context_events("main")
    assert not context.fast_path  # a rollback sits after the snapshot's coverage point
    snapshot, delta = split_context(fold_live(await store.read_stream("main")))
    assert context.snapshot == snapshot and context.delta == delta
    state = materialize(context.delta, seed=context.snapshot)
    assert state.active_workspace_facts["step"].value == 9
    assert state.decisions["k"].value == "v1"

    # Rolling back past every snapshot clears the pointer entirely.
    first_event = (await store.read_stream("main"))[0]
    await store.rollback("main", first_event.event_id, agent_id="x")  # type: ignore[arg-type]
    assert (await store.get_branch("main")).snapshot_event_id is None


async def test_noop_paths(store: AsyncEventStore) -> None:
    gc = engine(store, threshold=10_000, keep=4)
    await store.append(tool_log(0))
    assert (await gc.maybe_collect("main")).reason == "below_threshold"
    assert (await gc.collect("main", force=True)).reason == "nothing_to_compact"
    small = engine(store, threshold=1, keep=0)
    await store.create_branch("tiny")
    await store.append(ev(branch="tiny", note="hi"))
    result = await small.collect("tiny")
    assert result.reason == "no_gain" and not result.triggered
    assert (await store.get_branch("tiny")).snapshot_event_id is None


async def test_concurrent_gc_writes_one_snapshot(db_path: str) -> None:
    async with AsyncEventStore(db_path) as a, AsyncEventStore(db_path) as b:
        await a.ensure_branch("main")
        for i in range(20):
            await a.append(tool_log(i))
        results = await asyncio.gather(engine(a, threshold=1000).collect("main"), engine(b, threshold=1000).collect("main"))
        assert sum(r.triggered for r in results) == 1
        snapshots = [e for e in await a.read_stream("main") if e.event_type is EventType.SEMANTIC_SNAPSHOT]
        assert len(snapshots) == 1


async def test_llm_summarizer_adapter(store: AsyncEventStore) -> None:
    prompts: list[str] = []

    async def fake_llm(prompt: str) -> str:
        prompts.append(prompt)
        return 'Sure! {"insights": ["Root cause: flaky \\u0007 fixture", "Next: pin urllib3"]} done'

    for i in range(12):
        await store.append(tool_log(i))
    result = await engine(store, threshold=500, summarizer=LLMSummarizer(fake_llm)).collect("main")
    assert result.triggered and result.summarizer == "llm"
    snap = await store.get_event(result.snapshot_event_id)  # type: ignore[arg-type]
    assert snap.payload["insights"] == ["Root cause: flaky fixture", "Next: pin urllib3"]
    assert "untrusted data" in prompts[0] and "EVENTS:" in prompts[0]
    assert snap.payload["active_workspace_facts"] == {}  # the LLM never writes facts

    async def broken_llm(prompt: str) -> str:
        return "I refuse to answer in JSON"

    await store.append(ev(note="remember: staging uses a self-signed cert"))
    for i in range(12):
        await store.append(tool_log(50 + i))
    fallback = await engine(store, threshold=500, keep=0, summarizer=LLMSummarizer(broken_llm)).collect("main")
    assert fallback.triggered
    snap2 = await store.get_event(fallback.snapshot_event_id)  # type: ignore[arg-type]
    assert "Next: pin urllib3" in snap2.payload["insights"]  # carried forward
    assert any("self-signed cert" in i for i in snap2.payload["insights"])  # heuristic fallback


async def test_kernel_auto_gc_and_daemon(db_path: str) -> None:
    kernel = await MemKernel.open(db_path, tokenizer="heuristic", gc_config=GCConfig(max_token_threshold=800))
    try:
        for i in range(15):
            await kernel.log_execution(branch_id="main", agent_id="codex-cli", command=f"make {i}",
                                       exit_code=0, output=NOISY_OUTPUT)
        state = await kernel.query_state("main")
        assert state["snapshot_event_id"] is not None
        raw = [e for e in await kernel.store.read_live("main") if e.event_type is not EventType.SEMANTIC_SNAPSHOT]
        raw_tokens = kernel.gc_engine.counter.count(render_context(None, raw))
        assert state["context_tokens"] < 0.5 * raw_tokens

        kernel.auto_gc = False
        await kernel.fork(source_branch="main", new_branch="side", agent_id="codex-cli")
        for i in range(15):
            await kernel.log_execution(branch_id="side", agent_id="codex-cli", command=f"make side {i}",
                                       exit_code=0, output=NOISY_OUTPUT)
        before = await kernel.gc_engine.measure("side")
        stop = asyncio.Event()
        daemon = asyncio.create_task(kernel.gc_engine.run_daemon(interval_seconds=0.05, stop=stop))
        await asyncio.sleep(0.5)
        stop.set()
        await asyncio.wait_for(daemon, 5)
        assert await kernel.gc_engine.measure("side") < before
    finally:
        await kernel.close()


@pytest.mark.skipif(
    not (os.environ.get("MEMKERNEL_LIVE_LLM_TESTS") == "1" and os.environ.get("OPENAI_API_KEY")
         and os.environ.get("OPENAI_LLM")),
    reason="set MEMKERNEL_LIVE_LLM_TESTS=1, OPENAI_API_KEY and OPENAI_LLM to run against a real model",
)
async def test_live_openai_summarizer(store: AsyncEventStore) -> None:
    import httpx

    model = os.environ["OPENAI_LLM"]

    async def complete(prompt: str) -> str:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}"},
                json={"model": model, "messages": [{"role": "user", "content": prompt}]},
            )
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]

    await seed_noisy_history(store)
    summarizer = LLMSummarizer(complete, fallback=None)
    state = materialize(fold_live(await store.read_stream("main")))
    insights = LLMSummarizer.parse(await complete(summarizer.build_prompt(state, await store.read_live("main"))))
    assert insights and all(isinstance(i, str) for i in insights)
    result = await engine(store, threshold=500, summarizer=summarizer).collect("main")
    assert result.triggered and result.summarizer == "llm"


def test_blocker_description_prefers_diagnostic_lines() -> None:
    from memkernel.memory.state_view import blocker_description

    pytest_out = "collected 3\nE   AssertionError: boom\nFAILED t.py::x - AssertionError: boom\n=== 1 failed ==="
    assert blocker_description(pytest_out, 1) == "FAILED t.py::x - AssertionError: boom"
    assert blocker_description("Traceback (most recent call last):\n  ...\nKeyError: 'k'", 1) == "KeyError: 'k'"
    assert blocker_description("npm ERR! code 1\nnpm ERR! errno 2\ndone", 2) == "npm ERR! errno 2"
    assert blocker_description("", 3) == "exit code 3"
    assert blocker_description("just output", 4) == "just output"


async def test_low_water_mark_prevents_gc_thrash(store: AsyncEventStore) -> None:
    gc = MemoryGCEngine(store, counter=TokenCounter("heuristic"),
                        config=GCConfig(max_token_threshold=3000, retain_recent_events=4))
    assert gc.config.tail_budget == 1500
    snapshots = 0
    for i in range(40):
        await store.append(tool_log(i))
        result = await gc.maybe_collect("main")
        snapshots += result.triggered
        if result.triggered:
            context = await store.context_events("main")
            tail = gc.counter.count(render_context(None, context.delta))
            assert tail <= 1500
    # Each compaction frees at least half the budget, so GC runs far less than once per append.
    assert 0 < snapshots <= 40 // 2
    assert await gc.measure("main") <= 3000 + gc.counter.count(render_context(None, [tool_log(0)]))


async def test_fork_at_head_inherits_snapshot_pointer(store: AsyncEventStore) -> None:
    for i in range(12):
        await store.append(tool_log(i))
    assert (await engine(store, threshold=500).collect("main")).triggered
    main = await store.get_branch("main")
    fork = await store.fork_branch("main", None, "probe")
    child = await store.get_branch("probe")
    assert child.snapshot_event_id == main.snapshot_event_id is not None
    assert child.head_hash == fork.hash and fork.parent_event_id == main.head_event_id
    fast = await store.context_events("probe")
    assert fast.fast_path
    slow_snapshot, slow_delta = split_context(fold_live(await store.read_stream("probe")))
    assert fast.snapshot == slow_snapshot and fast.delta == slow_delta
    assert await store.verify_branch("probe") == []
