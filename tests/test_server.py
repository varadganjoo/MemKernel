"""MCP server tools (in-process) and an end-to-end stdio integration test."""

from __future__ import annotations

import json
from typing import AsyncIterator

import pytest

from memkernel.clients.adapter import McpStdioClient, McpToolError
from memkernel.server import mcp_server as srv


@pytest.fixture
async def session(db_path: str) -> AsyncIterator[srv.ServerSession]:
    s = await srv.configure(db_path=db_path, agent_id="claude-code-cli", gc_threshold=4000)
    try:
        yield s
    finally:
        await srv.SESSION.close()


async def test_tool_flow(session: srv.ServerSession) -> None:
    first = await srv.memkernel_append_decision("auth.method", "OAuth2 PKCE", "public SPA client")
    assert first["event_id"] == 1 and first["superseded_value"] is None
    second = await srv.memkernel_append_decision("auth.method", "OAuth2 PKCE + DPoP", "token binding")
    assert second["superseded_value"] == "OAuth2 PKCE"

    await srv.memkernel_init_task("Implement login", ["keep API stable"])
    await srv.memkernel_log_execution("pytest", 1, "1 failed", {"python_version": "3.14.0"})
    await srv.memkernel_note("fixture leaks a DB connection")

    state = json.loads(await srv.memkernel_query_state())
    assert state["agent_id"] == "claude-code-cli" and state["active_branch"] == "main"
    assert state["state"]["decisions"]["auth.method"]["value"] == "OAuth2 PKCE + DPoP"
    assert "pytest" in state["state"]["execution_blockers"]
    assert not any("TOOL_EXECUTION_LOG" in e for e in state["recent_events"])
    verbose = json.loads(await srv.memkernel_query_state("main", include_ephemeral=True))
    assert any("TOOL_EXECUTION_LOG" in e for e in verbose["recent_events"])

    lease = await srv.memkernel_acquire_lease("src/login.py", 45)
    assert lease["acquired"] and lease["ttl_seconds"] == 45
    assert await srv.memkernel_heartbeat_lease("src/login.py", lease["fencing_token"]) is True
    assert await srv.memkernel_release_lease("src/login.py", lease["fencing_token"]) is True
    assert await srv.memkernel_release_lease("src/login.py", lease["fencing_token"]) is False

    message = await srv.memkernel_fork_investigation("try-sessions")
    assert "try-sessions" in message and session.branch == "try-sessions"
    bad = await srv.memkernel_append_decision("auth.method", "server sessions", "experiment")
    diff = await srv.memkernel_diff_branches("main", "try-sessions")
    assert [c["key"] for c in diff["conflicting_decisions"]] == ["auth.method"]

    history = (await srv.memkernel_history(10))["events"]
    fork_event = next(e for e in history if e["event_type"] == "BRANCH_FORKED")
    result = await srv.memkernel_rollback(fork_event["event_id"])
    assert f"[{bad['event_id']}]" in result
    rolled = json.loads(await srv.memkernel_query_state("try-sessions"))
    assert rolled["state"]["decisions"]["auth.method"]["value"] == "OAuth2 PKCE + DPoP"

    assert "main" in await srv.memkernel_checkout("main")
    branches = await srv.memkernel_list_branches()
    assert {b["branch_id"] for b in branches["branches"]} == {"main", "try-sessions"}

    gc = await srv.memkernel_trigger_gc("main")
    assert gc["branch_id"] == "main" and gc["reason"] in {"compacted", "nothing_to_compact", "no_gain"}


async def test_tool_errors_are_readable(session: srv.ServerSession) -> None:
    with pytest.raises(srv.ToolError, match="not on the live timeline"):
        await srv.memkernel_rollback(12345)
    with pytest.raises(srv.ToolError, match="does not exist"):
        await srv.memkernel_checkout("nope")
    with pytest.raises(srv.ToolError, match="ttl_seconds"):
        await srv.memkernel_acquire_lease("a.py", 0)
    with pytest.raises(srv.ToolError, match="invalid branch_id"):
        await srv.memkernel_fork_investigation("has spaces")
    with pytest.raises(srv.ToolError, match="decision_key"):
        await srv.memkernel_append_decision("  ", "v", "r")


async def test_stdio_end_to_end_two_processes(db_path: str) -> None:
    """Boots two real MCP server processes on one database and coordinates."""
    async with McpStdioClient(agent_id="cursor-ide", db_path=db_path) as cursor, McpStdioClient(
        agent_id="codex-cli", db_path=db_path
    ) as codex:
        tools = await cursor.list_tools()
        for required in (
            "memkernel_append_decision",
            "memkernel_query_state",
            "memkernel_acquire_lease",
            "memkernel_release_lease",
            "memkernel_fork_investigation",
            "memkernel_rollback",
            "memkernel_trigger_gc",
        ):
            assert required in tools

        await cursor.call("memkernel_append_decision", decision_key="db", decision_value="postgres", rationale="ACID")
        grant = await cursor.call("memkernel_acquire_lease", file_path="src/models.py", timeout_seconds=30)
        assert grant["acquired"] is True
        denied = await codex.call("memkernel_acquire_lease", file_path="./src/models.py")
        assert denied["acquired"] is False and denied["holder_agent_id"] == "cursor-ide"

        state = await codex.call("memkernel_query_state")
        assert state["state"]["decisions"]["db"]["value"] == "postgres"
        assert state["state"]["active_claims"]["src/models.py"]["agent_id"] == "cursor-ide"

        # Tokens are visible shared state, not credentials. Another identity
        # must not be able to prolong the holder's claim with a known token.
        assert await codex.call("memkernel_heartbeat_lease", file_path="src/models.py",
                                fencing_token=grant["fencing_token"]) is False
        assert await cursor.call("memkernel_heartbeat_lease", file_path="src/models.py",
                                 fencing_token=grant["fencing_token"]) is True

        assert await cursor.call("memkernel_release_lease", file_path="src/models.py",
                                 fencing_token=grant["fencing_token"]) is True
        regrant = await codex.call("memkernel_acquire_lease", file_path="src/models.py")
        assert regrant["acquired"] is True and regrant["fencing_token"] == grant["fencing_token"] + 1

        with pytest.raises(McpToolError, match="not on the live timeline"):
            await codex.call("memkernel_rollback", target_event_id=999)
