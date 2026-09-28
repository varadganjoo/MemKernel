"""Reproducible MemKernel benchmarks (numbers reported in docs/whitepaper.md).

    python benchmarks/run_benchmarks.py [--quick]

1. Token savings: three deterministic 50-turn workflows, comparing the naive
   "load the whole history" context with MemKernel's snapshot + delta.
2. Recovery latency: time to rebuild agent context from the log (cold open,
   fast path, full replay), and to fork / roll back, at several log sizes.
3. Append throughput for a single writer.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import platform
import random
import statistics
import sys
import tempfile
import time
from typing import Awaitable, Callable

import aiosqlite

from memkernel import AsyncEventStore, EventType, GCConfig, MemKernel
from memkernel.core.branch import fold_live
from memkernel.core.events import new_event
from memkernel.memory.state_view import materialize, render_context

LINT_RULES = ["E501 line too long", "F401 unused import", "W291 trailing whitespace", "B008 call in default arg"]


def pytest_output(rng: random.Random, n_tests: int, failures: int) -> str:
    lines = ["============================= test session starts ==============================",
             f"platform linux -- Python 3.14.0, pytest-9.1.1", f"collected {n_tests} items", ""]
    for i in range(n_tests):
        status = "FAILED" if i < failures else "PASSED"
        lines.append(f"tests/test_module_{i % 7}.py::test_case_{i} {status} [{int(100 * (i + 1) / n_tests)}%]")
    for i in range(failures):
        lines += [f"____ test_case_{i} ____", f"E   AssertionError: expected {rng.randint(1, 99)} got {rng.randint(100, 999)}"]
    lines.append(f"==== {failures} failed, {n_tests - failures} passed in {rng.uniform(1, 9):.2f}s ====")
    return "\n".join(lines)


def lint_output(rng: random.Random, n: int) -> str:
    return "\n".join(
        f"src/pkg/module_{rng.randint(0, 30)}.py:{rng.randint(1, 900)}:{rng.randint(1, 80)}: {rng.choice(LINT_RULES)}"
        for _ in range(n)
    )


async def workflow(kernel: MemKernel, name: str, seed: int, turns: int = 50) -> None:
    """Deterministic synthetic agent session of ``turns`` turns."""
    rng = random.Random(seed)
    agents = ["cursor-ide", "claude-code-cli", "codex-cli"] if name != "single-agent-test-fix" else ["claude-code-cli"]
    branch = "main"
    await kernel.init_task(branch_id=branch, agent_id=agents[0], directive=f"workflow {name}",
                           constraints=["keep public API stable"])
    failing = 5
    for turn in range(turns):
        agent = agents[turn % len(agents)]
        if name == "lint-heavy-exploration" and turn % 2 == 0:
            await kernel.log_execution(branch_id=branch, agent_id=agent, command="ruff check src",
                                       exit_code=1 if turn < 40 else 0,
                                       output=lint_output(rng, rng.randint(40, 90)),
                                       facts={"lint_findings": max(0, 60 - turn)})
        else:
            failing = max(0, failing - (1 if rng.random() < 0.3 else 0))
            await kernel.log_execution(branch_id=branch, agent_id=agent, command="pytest -q",
                                       exit_code=1 if failing else 0,
                                       output=pytest_output(rng, rng.randint(15, 45), failing),
                                       facts={"failing_tests": failing, "turn": turn})
        if turn % 3 == 1:
            await kernel.note(branch_id=branch, agent_id=agent,
                              note=f"turn {turn}: suspect fixture ordering in module_{rng.randint(0, 6)}")
        if turn % 10 == 5:
            await kernel.append_decision(branch_id=branch, agent_id=agent, key=f"design.choice_{turn // 20}",
                                         value=f"option-{rng.randint(1, 4)}", rationale="benchmarks favoured it")
        if name == "multi-agent-refactor" and turn % 4 == 0:
            path = f"src/pkg/module_{rng.randint(0, 5)}.py"
            grant = await kernel.acquire_lease(branch_id=branch, agent_id=agent, resource=path, ttl_seconds=60)
            if grant.acquired:
                await kernel.release_lease(branch_id=branch, agent_id=agent, resource=path,
                                           fencing_token=grant.fencing_token)  # type: ignore[arg-type]


async def token_savings(tmp: str) -> list[dict]:
    rows = []
    for seed, name in enumerate(("single-agent-test-fix", "multi-agent-refactor", "lint-heavy-exploration")):
        kernel = await MemKernel.open(os.path.join(tmp, f"tokens-{name}.db"),
                                      gc_config=GCConfig(max_token_threshold=4000, retain_recent_events=4))
        try:
            await workflow(kernel, name, seed)
            live = await kernel.store.read_live("main")
            history = [e for e in live if e.event_type is not EventType.SEMANTIC_SNAPSHOT]
            counter = kernel.gc_engine.counter
            raw_tokens = counter.count(render_context(None, history))
            context = await kernel.store.context_events("main")
            mk_tokens = kernel.gc_engine.measure_slice(context)
            sound = (materialize(context.delta, seed=context.snapshot).core_sections()
                     == materialize(history).core_sections())
            rows.append({
                "workflow": name, "events": len(history), "raw_tokens": raw_tokens, "memkernel_tokens": mk_tokens,
                "reduction": 1 - mk_tokens / raw_tokens,
                "snapshots": sum(e.event_type is EventType.SEMANTIC_SNAPSHOT for e in live),
                "state_equivalent": sound, "tokenizer": counter.backend_name,
            })
        finally:
            await kernel.close()
    return rows


async def timed(fn: Callable[[], Awaitable[object]], repeat: int) -> float:
    samples = []
    for _ in range(repeat):
        start = time.perf_counter()
        await fn()
        samples.append((time.perf_counter() - start) * 1000)
    return statistics.median(samples)


async def recovery_latency(tmp: str, sizes: list[int], repeat: int) -> tuple[list[dict], float]:
    rows, throughput = [], 0.0
    for size in sizes:
        path = os.path.join(tmp, f"latency-{size}.db")
        async with AsyncEventStore(path) as store:
            await store.ensure_branch("main")
            start = time.perf_counter()
            # Bulk load in one transaction (the per-event commit cost is measured separately below).
            async with store.transaction(write=True) as db:
                for i in range(size):
                    await store.append_in_transaction(db, new_event(
                        EventType.TOOL_EXECUTION_LOG if i % 5 else EventType.ARCHITECTURAL_DECISION,
                        branch_id="main", agent_id="bench",
                        payload={"command": f"cmd {i % 50}", "exit_code": 0, "output": "x" * 200,
                                 "decision_key": f"k{i % 40}", "decision_value": str(i), "rationale": "r"}))
            load_s = time.perf_counter() - start
        kernel = await MemKernel.open(path, gc_config=GCConfig(max_token_threshold=4000), auto_gc=False)
        try:
            await kernel.gc("main", force=True)
            head = (await kernel.store.get_branch("main")).head_event_id
            cold = await timed(lambda: _cold_open(path), max(3, repeat // 3))
            fast = await timed(lambda: kernel.store.context_events("main"), repeat)
            full = await timed(lambda: _full_replay(kernel), repeat)
            fork_n = iter(range(10**6))
            fork = await timed(lambda: kernel.store.fork_branch("main", head, f"f{next(fork_n)}"), repeat)
            rb_n = iter(range(10**6))

            async def rollback_op() -> None:
                name = f"rb{next(rb_n)}"
                await kernel.store.fork_branch("main", head, name)
                await kernel.store.rollback(name, head, agent_id="bench")  # type: ignore[arg-type]

            rollback = await timed(rollback_op, repeat)
            rows.append({"events": size, "bulk_load_events_per_s": size / load_s, "cold_open_ms": cold,
                         "context_fast_path_ms": fast, "full_replay_ms": full,
                         "fork_ms": fork, "fork_plus_rollback_ms": rollback})
        finally:
            await kernel.close()
    async with AsyncEventStore(os.path.join(tmp, "throughput.db")) as store:
        await store.ensure_branch("main")
        n = 500
        start = time.perf_counter()
        for i in range(n):
            await store.append(new_event(EventType.AGENT_SCRATCHPAD, branch_id="main", agent_id="bench",
                                         payload={"note": f"n{i}"}))
        throughput = n / (time.perf_counter() - start)
    return rows, throughput


async def _cold_open(path: str) -> None:
    kernel = await MemKernel.open(path, auto_gc=False, tokenizer="heuristic")
    try:
        context = await kernel.store.context_events("main")
        materialize(context.delta, seed=context.snapshot)
    finally:
        await kernel.close()


async def _full_replay(kernel: MemKernel) -> None:
    materialize(fold_live(await kernel.store.read_stream("main")))


def describe_os() -> str:
    """Human-readable OS name. ``platform.release()`` alone is unreliable:
    older Pythons report every Windows 11 machine as "10"."""
    system = platform.system()
    if system == "Windows":
        build = int(platform.version().rsplit(".", 1)[-1])
        return f"Windows {'11' if build >= 22000 else '10'} (build {build})"
    if system == "Linux":
        try:
            distro = platform.freedesktop_os_release().get("PRETTY_NAME", "Linux")
        except OSError:
            distro = "Linux"
        return f"{distro} (kernel {platform.release()})"
    if system == "Darwin":
        return f"macOS {platform.mac_ver()[0]}"
    return f"{system} {platform.release()}"


async def main(quick: bool) -> None:
    sizes = [1_000, 5_000] if quick else [1_000, 10_000, 50_000]
    repeat = 5 if quick else 15
    with tempfile.TemporaryDirectory() as tmp:
        savings = await token_savings(tmp)
        latency, throughput = await recovery_latency(tmp, sizes, repeat)
    print(f"Environment: Python {platform.python_version()} on {describe_os()}, {os.cpu_count()} logical CPUs, "
          f"SQLite {aiosqlite.sqlite_version}, synchronous=NORMAL, WAL\n")
    print("## Token savings (50-turn workflows, GC threshold 4000, retain 4)\n")
    print("| Workflow | Events | Raw history tokens | MemKernel context tokens | Reduction | Snapshots | State equivalent |")
    print("|---|---:|---:|---:|---:|---:|:---:|")
    for r in savings:
        print(f"| {r['workflow']} | {r['events']} | {r['raw_tokens']:,} | {r['memkernel_tokens']:,} | "
              f"{r['reduction']:.1%} | {r['snapshots']} | {'yes' if r['state_equivalent'] else 'NO'} |")
    print(f"\nTokenizer: {savings[0]['tokenizer']}\n")
    print(f"## Recovery latency (median of {repeat} runs, ms)\n")
    print("| Log size (events) | Cold open + context | Context (fast path) | Full replay | Fork | Fork + rollback |")
    print("|---:|---:|---:|---:|---:|---:|")
    for r in latency:
        print(f"| {r['events']:,} | {r['cold_open_ms']:.1f} | {r['context_fast_path_ms']:.2f} | "
              f"{r['full_replay_ms']:.1f} | {r['fork_ms']:.1f} | {r['fork_plus_rollback_ms']:.1f} |")
    print(f"\nSingle-writer durable append throughput (one transaction per event): {throughput:,.0f} events/s")
    print("Bulk load (one transaction): " + ", ".join(
        f"{r['events']:,} events at {r['bulk_load_events_per_s']:,.0f}/s" for r in latency))
    if not all(r["state_equivalent"] for r in savings):
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--quick", action="store_true", help="smaller log sizes and fewer repetitions")
    asyncio.run(main(parser.parse_args().quick))
