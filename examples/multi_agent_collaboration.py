"""Simulation: Cursor, Claude Code and Codex coordinate on one repository.

Each agent is a *real* MemKernel MCP server process spoken to over stdio —
exactly how the IDEs connect — and all three share one SQLite database.

    python examples/multi_agent_collaboration.py [--db path/to/memkernel.db]

The script asserts its invariants and exits non-zero if any fails.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import tempfile
import time
from typing import Any

from memkernel.clients.adapter import McpStdioClient

AGENTS = ("cursor-ide", "claude-code-cli", "codex-cli")
T0 = time.perf_counter()

PYTEST_FAIL = """\
============================= test session starts ==============================
collected 14 items
tests/test_login.py ......F.......                                        [100%]
=================================== FAILURES ===================================
____________________________ test_refresh_rotation _____________________________
    def test_refresh_rotation(client):
>       assert client.refresh(token).status_code == 200
E       AssertionError: assert 401 == 200
FAILED tests/test_login.py::test_refresh_rotation - AssertionError: assert 401 == 200
========================= 1 failed, 13 passed in 2.41s =========================
"""

PYTEST_PASS = """\
============================= test session starts ==============================
collected 14 items
tests/test_login.py ..............                                        [100%]
============================== 14 passed in 2.37s ==============================
"""


def say(agent: str, message: str) -> None:
    print(f"[{time.perf_counter() - T0:6.2f}s] {agent:<16} {message}")


async def claim(agent: McpStdioClient, preferred: list[str]) -> tuple[str, int]:
    """Lease the first free file from ``preferred`` and keep holding it."""
    for path in preferred:
        grant = await agent.call("memkernel_acquire_lease", file_path=path, timeout_seconds=30)
        if grant["acquired"]:
            say(agent.agent_id, f"leased {path} (fencing token {grant['fencing_token']})")
            return path, grant["fencing_token"]
        say(agent.agent_id, f"{path} is held by {grant['holder_agent_id']}; moving on")
    raise RuntimeError(f"{agent.agent_id}: no free file among {preferred}")


async def work(agent: McpStdioClient, lease: tuple[str, int], command: str, exit_code: int,
               output: str, facts: dict[str, Any]) -> None:
    """Run (simulate) a command on the leased file, log it, release the lease."""
    path, token = lease
    await agent.call("memkernel_log_execution", command=command, exit_code=exit_code, output=output, facts=facts)
    assert await agent.call("memkernel_release_lease", file_path=path, fencing_token=token) is True
    say(agent.agent_id, f"`{command}` -> exit {exit_code}; released {path}")


async def main(db_path: str) -> None:
    say("kernel", f"shared database: {db_path}")
    async with McpStdioClient(agent_id=AGENTS[0], db_path=db_path) as cursor, \
            McpStdioClient(agent_id=AGENTS[1], db_path=db_path) as claude, \
            McpStdioClient(agent_id=AGENTS[2], db_path=db_path) as codex:
        agents = (cursor, claude, codex)
        say("kernel", f"3 MCP server processes up; {len(await cursor.list_tools())} tools each")

        # 1. Cursor frames the task and the architecture.
        await cursor.call("memkernel_init_task", directive="Add OAuth login to the web app",
                          constraints=["no new runtime dependencies", "keep /api/v1 stable"])
        await cursor.call("memkernel_append_decision", decision_key="auth.method",
                          decision_value="OAuth2 Authorization Code + PKCE",
                          rationale="public SPA client cannot keep a secret")
        say("cursor-ide", "recorded decision auth.method = OAuth2 Authorization Code + PKCE")

        # 2. All three race for the same hot file; exactly one lease is granted.
        grants = await asyncio.gather(*(a.call("memkernel_acquire_lease", file_path="src/auth/login.py")
                                        for a in agents))
        winners = [a.agent_id for a, g in zip(agents, grants) if g["acquired"]]
        assert len(winners) == 1, grants
        holder = next(a for a in agents if a.agent_id == winners[0])
        say("kernel", f"contention on src/auth/login.py -> granted to {winners[0]} only")
        token = next(g for g in grants if g["acquired"])["fencing_token"]
        assert await holder.call("memkernel_release_lease", file_path="src/auth/login.py", fencing_token=token)

        # 3. Parallel work on disjoint files, coordinated purely through leases.
        #    Claims are held until every agent has one, so the split is deterministic.
        leases = await asyncio.gather(
            claim(cursor, ["src/auth/login.py", "src/auth/routes.py"]),
            claim(claude, ["src/auth/login.py", "tests/test_login.py"]),
            claim(codex, ["src/auth/login.py", "src/auth/tokens.py"]),
        )
        assert len({path for path, _ in leases}) == 3, leases
        await asyncio.gather(
            work(cursor, leases[0], "npm run build", 0, "webpack compiled successfully",
                 {"node_version": "24.9.0", "frontend_build": "green"}),
            work(claude, leases[1], "pytest tests/test_login.py", 1, PYTEST_FAIL,
                 {"python_version": "3.14.0", "test_count": 14}),
            work(codex, leases[2], "ruff check src/auth", 0, "All checks passed!", {"lint": "clean"}),
        )

        # 4. Cursor sees Claude Code's failing test as a shared blocker.
        state = (await cursor.call("memkernel_query_state"))["state"]
        assert "pytest tests/test_login.py" in state["execution_blockers"]
        blocker = state["execution_blockers"]["pytest tests/test_login.py"]
        say("cursor-ide", f"sees blocker from {blocker['agent_id']}: {blocker['description']}")

        # 5. Codex revises the design and fixes the test; the old choice becomes a deprecated assumption.
        await codex.call("memkernel_append_decision", decision_key="auth.method",
                         decision_value="OAuth2 Authorization Code + PKCE with refresh-token rotation",
                         rationale="refresh tokens must be single-use to pass test_refresh_rotation")
        await work(codex, await claim(codex, ["src/auth/tokens.py"]), "pytest tests/test_login.py", 0,
                   "14 passed in 2.37s", {"test_count": 14, "refresh_rotation": "implemented"})

        # 6. More noisy iterations, then compaction.
        for i in range(12):
            await claude.call("memkernel_log_execution", command=f"pytest -x -k auth_{i}", exit_code=0,
                              output=PYTEST_PASS)
        gc = await claude.call("memkernel_trigger_gc", target_branch="main")
        assert gc["triggered"], gc
        say("claude-code-cli", f"GC: {gc['tokens_before']} -> {gc['tokens_after']} tokens "
                               f"({gc['reduction_ratio']:.0%} saved, {gc['compacted_event_count']} events compacted)")

        # 7. Every agent now reads an identical consolidated state.
        views = await asyncio.gather(*(a.call("memkernel_query_state") for a in agents))
        states = [v["state"] for v in views]
        assert states[0] == states[1] == states[2]
        final = states[0]
        assert final["execution_blockers"] == {}
        assert final["decisions"]["auth.method"]["agent_id"] == "codex-cli"
        deprecated = [d for d in final["deprecated_assumptions"] if d["key"] == "auth.method"]
        assert deprecated and deprecated[0]["value"] == "OAuth2 Authorization Code + PKCE"
        assert final["active_claims"] == {}
        assert len(final["tasks"]) == 1

        print("\n=== consolidated memory (identical for all three agents) ===")
        for key, record in final["decisions"].items():
            print(f"decision  {key} = {record['value']}   (by {record['agent_id']})")
        for key, fact in sorted(final["active_workspace_facts"].items()):
            print(f"fact      {key} = {fact['value']}")
        for item in final["deprecated_assumptions"]:
            print(f"deprecated {item['kind']} {item['key']}: {item['value']!r} -> {item['replaced_by']!r}")
        print(f"blockers  {list(final['execution_blockers']) or 'none'}")
        print(f"context   {views[0]['context_tokens']} tokens (snapshot #{views[0]['snapshot_event_id']} + delta)")
    say("kernel", "all invariants hold; servers shut down cleanly")


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
