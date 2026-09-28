"""Simulation: an agent branches, hallucinates, fails its tests and rolls back.

Uses the in-process client adapter (no MCP subprocess):

    python examples/rollback_on_hallucination.py [--db path/to/memkernel.db]

The script asserts its invariants and exits non-zero if any fails.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import tempfile

from memkernel import MemKernel
from memkernel.clients.adapter import MemKernelClient


def show(title: str, state: dict) -> None:
    s = state["state"]
    print(f"\n--- {title} [branch={state['branch_id']} head=#{state['head_event_id']}] ---")
    for key, d in s["decisions"].items():
        print(f"  decision {key} = {d['value']}")
    for key, f in sorted(s["active_workspace_facts"].items()):
        print(f"  fact     {key} = {f['value']}")
    for key, b in s["execution_blockers"].items():
        print(f"  BLOCKER  {key}: {b['description']}")


async def main(db_path: str) -> None:
    kernel = await MemKernel.open(db_path)
    try:
        agent = MemKernelClient(kernel, "claude-code-cli")

        # Baseline on main: a verified, working state.
        await agent.init_task("Add JWT verification to the API gateway", ["only stdlib + PyJWT"])
        await agent.decide("jwt.library", "PyJWT 2.x", "already a dependency; maintained")
        await agent.log_execution("pytest tests/test_gateway.py", 0, "31 passed in 1.12s",
                                  facts={"tests_passing": 31, "pyjwt_version": "2.10.1"})
        baseline = await agent.query_state()
        show("baseline (main)", baseline)

        # Speculate on a branch. Remember where it starts.
        fork = await agent.fork_investigation("jwt-refactor")
        checkpoint = fork.event_id
        print(f"\nforked main -> {agent.branch!r}; checkpoint = event #{checkpoint}")

        # The agent hallucinates an API that does not exist...
        await agent.note("PyJWT has jwt.verify_strict() which also checks key rotation")
        await agent.decide("jwt.library", "PyJWT verify_strict()", "hallucinated: stricter verification")
        await agent.log_execution(
            "pytest tests/test_gateway.py", 1,
            "E   AttributeError: module 'jwt' has no attribute 'verify_strict'\n"
            "FAILED tests/test_gateway.py::test_rotated_key - AttributeError: module 'jwt' has no attribute "
            "'verify_strict'\n=== 1 failed, 30 passed in 1.31s ===",
            facts={"tests_passing": 30},
        )
        broken = await agent.query_state()
        show("after hallucination (jwt-refactor)", broken)
        assert "pytest tests/test_gateway.py" in broken["state"]["execution_blockers"]

        # Main is untouched by the speculative branch.
        main_state = await agent.query_state(branch="main")
        assert main_state["state"]["decisions"] == baseline["state"]["decisions"]
        diff = await agent.diff("main")
        print(f"\ndiff jwt-refactor..main: conflicting decisions = "
              f"{[c['key'] for c in diff['conflicting_decisions']]}")
        assert [c["key"] for c in diff["conflicting_decisions"]] == ["jwt.library"]

        # Tests failed -> roll the branch back to its checkpoint.
        rollback = await agent.rollback(checkpoint, reason="tests failed: hallucinated jwt.verify_strict")
        print(f"\nrollback event #{rollback.event_id} reverted {rollback.payload['reverted_event_ids']}")
        restored = await agent.query_state()
        show("after rollback (jwt-refactor)", restored)
        for section in ("decisions", "active_workspace_facts", "execution_blockers", "tasks"):
            assert restored["state"][section] == baseline["state"][section], section

        # The hallucination is still auditable in the raw log...
        raw = await kernel.store.read_stream("jwt-refactor")
        reverted = set(rollback.payload["reverted_event_ids"])
        assert reverted and reverted <= {e.event_id for e in raw}
        # ...and the retry proceeds from the clean checkpoint.
        await agent.decide("jwt.library", "PyJWT 2.x with PyJWKClient",
                           "real API: fetch rotating keys from the JWKS endpoint")
        await agent.log_execution("pytest tests/test_gateway.py", 0, "32 passed in 1.18s",
                                  facts={"tests_passing": 32})
        fixed = await agent.query_state()
        show("after correct retry (jwt-refactor)", fixed)
        assert fixed["state"]["execution_blockers"] == {}
        deprecated_values = [d["value"] for d in fixed["state"]["deprecated_assumptions"]]
        assert "PyJWT verify_strict()" not in deprecated_values  # reverted, not merely superseded

        for branch in ("main", "jwt-refactor"):
            assert await kernel.verify(branch) == [], branch
        print("\nhash chains verified on main and jwt-refactor; all invariants hold")
    finally:
        await kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", help="database path (default: fresh temporary file)")
    args = parser.parse_args()
    path = args.db or os.path.join(tempfile.mkdtemp(prefix="memkernel-demo-"), "memkernel.db")
    try:
        asyncio.run(main(path))
    except AssertionError as exc:
        print(f"INVARIANT VIOLATED: {exc!r}", file=sys.stderr)
        sys.exit(1)
