"""Local safety boundaries, including real MCP subprocesses and SQLite enforcement."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from memkernel import AsyncEventStore, MemKernel
from memkernel.clients.adapter import McpStdioClient, McpToolError
from memkernel.core.store import StoreError
from memkernel.server import mcp_server as srv
from tests.conftest import FakeClock


MUTATIONS = {
    "memkernel_append_decision": {"decision_key": "db", "decision_value": "bad", "rationale": "test"},
    "memkernel_fenced_append_decision": {
        "file_path": "a.py", "fencing_token": 1, "decision_key": "db", "decision_value": "bad", "rationale": "test",
    },
    "memkernel_acquire_lease": {"file_path": "a.py"},
    "memkernel_release_lease": {"file_path": "a.py", "fencing_token": 1},
    "memkernel_heartbeat_lease": {"file_path": "a.py", "fencing_token": 1},
    "memkernel_fork_investigation": {"fork_branch_name": "bad"},
    "memkernel_rollback": {"target_event_id": 1},
    "memkernel_trigger_gc": {},
    "memkernel_init_task": {"directive": "bad"},
    "memkernel_log_execution": {"command": "bad", "exit_code": 1},
    "memkernel_note": {"note": "bad"},
}


async def test_read_only_stdio_blocks_every_mutator_and_keeps_reads(db_path: str) -> None:
    async with await MemKernel.open(db_path, tokenizer="heuristic", auto_gc=False) as kernel:
        await kernel.append_decision(branch_id="main", agent_id="owner", key="db", value="sqlite", rationale="local")
        await kernel.acquire_lease(branch_id="main", agent_id="owner", resource="a.py", ttl_seconds=300)
        await kernel.fork(source_branch="main", new_branch="existing", agent_id="owner")
        before_events = await kernel.store.read_stream("main")
        before_branches = await kernel.store.list_branches()
        before_leases = await kernel.locks.list_leases(active_only=False)
        async with McpStdioClient(agent_id="observer", db_path=db_path, env={"MEMKERNEL_READ_ONLY": "true"}) as client:
            tools = await client.list_tools()
            assert set(tools) == set(MUTATIONS) | srv.READ_ONLY_TOOLS
            state = await client.call("memkernel_query_state")
            assert state["safety"]["read_only"] is True
            assert state["state"]["decisions"]["db"]["value"] == "sqlite"
            for tool, arguments in MUTATIONS.items():
                with pytest.raises(McpToolError, match="read-only"):
                    await client.call(tool, **arguments)
            assert (await client.call("memkernel_history"))["events"]
            assert (await client.call("memkernel_list_branches"))["branches"]
            assert "existing" in await client.call("memkernel_checkout", branch_id="existing")
            assert await client.call("memkernel_diff_branches", branch_a="main", branch_b="existing")
        assert await kernel.store.read_stream("main") == before_events
        assert await kernel.store.list_branches() == before_branches
        assert await kernel.locks.list_leases(active_only=False) == before_leases


async def test_read_only_storage_rejects_writes_even_in_read_transactions(tmp_path: Path) -> None:
    # URI delimiters in a normal filename must be escaped, not interpreted as options.
    path = tmp_path / "space # % database.db"
    async with await MemKernel.open(path, tokenizer="heuristic"):
        pass
    async with AsyncEventStore(path, read_only=True) as store:
        with pytest.raises(StoreError, match="read-only"):
            await store.create_branch("bad")
        async with store.transaction(write=False) as db:
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                await db.execute("DELETE FROM leases")
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                await db.execute("PRAGMA query_only = OFF")
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                await db.execute("DELETE FROM leases")
            # SQLite applies mode=ro to main only. Reattaching the same file
            # writable must be rejected before any SQL can mutate that alias.
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                await db.execute("ATTACH DATABASE ? AS writable", (str(path),))
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                await db.execute("ATTACH DATABASE ? AS outside", (str(tmp_path / "outside.db"),))
            assert not (tmp_path / "outside.db").exists()
        assert [b.branch_id for b in await store.list_branches()] == ["main"]


async def test_read_only_does_not_create_or_initialize_database(tmp_path: Path) -> None:
    path = tmp_path / "missing" / "memory.db"
    with pytest.raises(StoreError, match="read-only"):
        await MemKernel.open(path, read_only=True, tokenizer="heuristic")
    assert not path.parent.exists()
    empty = tmp_path / "uninitialized.db"
    empty.touch()
    with pytest.raises(StoreError, match="schema v0"):
        await MemKernel.open(empty, read_only=True, tokenizer="heuristic")
    assert empty.stat().st_size == 0
    for unsafe in (":memory:", path.as_uri() + "?mode=rwc"):
        with pytest.raises(StoreError, match="existing local database"):
            await AsyncEventStore(unsafe, read_only=True).open()


def test_read_only_configuration_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    for value in ("true", "1", "YES", "on"):
        monkeypatch.setenv("MEMKERNEL_READ_ONLY", value)
        assert srv._read_only_from_env()
    monkeypatch.setenv("MEMKERNEL_READ_ONLY", "tru")
    with pytest.raises(ValueError, match="MEMKERNEL_READ_ONLY"):
        srv._read_only_from_env()
    with pytest.raises(ValueError, match="background GC"):
        srv.ServerSession(db_path="unused.db", agent_id="observer", read_only=True, gc_interval=1)


async def test_read_only_guard_runs_before_initialization(tmp_path: Path) -> None:
    path = tmp_path / "missing.db"
    await srv.configure(db_path=str(path), agent_id="observer", read_only=True)
    try:
        with pytest.raises(srv.ToolError, match="read-only"):
            await srv.memkernel_note("must not initialize")
        assert not path.exists() and srv.SESSION._kernel is None and srv.SESSION._daemon is None
    finally:
        await srv.SESSION.close()


async def test_fenced_decision_rejects_foreign_expired_and_stale_leases(db_path: str) -> None:
    clock = FakeClock()
    kernel = await MemKernel.open(db_path, tokenizer="heuristic", lease_clock=clock, auto_gc=False)
    session = await srv.configure(db_path=db_path, agent_id="owner")
    session._kernel = kernel
    try:
        grant = await srv.memkernel_acquire_lease("src/a.py", 10)
        token = grant["fencing_token"]
        args = {"file_path": "./src\\a.py", "fencing_token": token,
                "decision_key": "db", "decision_value": "sqlite", "rationale": "local"}
        result = await srv.memkernel_fenced_append_decision(**args)
        event = (await kernel.store.read_stream("main"))[-1]
        assert event.agent_id == "owner" and event.branch_id == "main"
        assert event.metadata["lease"] == {"resource": "src/a.py", "fencing_token": token}
        assert event.event_id == result["event_id"]
        before = await kernel.store.read_stream("main")
        session.agent_id = "other"
        with pytest.raises(srv.ToolError, match="not the active lease"):
            await srv.memkernel_fenced_append_decision(**args)
        session.agent_id = "owner"
        clock.advance(11)
        with pytest.raises(srv.ToolError, match="not the active lease"):
            await srv.memkernel_fenced_append_decision(**args)
        assert await kernel.store.read_stream("main") == before
        session.agent_id = "other"
        next_grant = await srv.memkernel_acquire_lease("src/a.py", 10)
        session.agent_id = "owner"
        with pytest.raises(srv.ToolError, match="not the active lease"):
            await srv.memkernel_fenced_append_decision(**args)
        session.agent_id = "other"
        args["fencing_token"] = next_grant["fencing_token"]
        await srv.memkernel_fenced_append_decision(**args)
        assert not await kernel.verify()
    finally:
        await srv.SESSION.close()


async def test_fenced_decision_stdio_uses_server_identity(db_path: str) -> None:
    async with McpStdioClient(agent_id="owner", db_path=db_path) as owner, McpStdioClient(
        agent_id="other", db_path=db_path
    ) as other:
        grant = await owner.call("memkernel_acquire_lease", file_path="a.py")
        arguments = {"file_path": "a.py", "fencing_token": grant["fencing_token"],
                     "decision_key": "db", "decision_value": "sqlite", "rationale": "local"}
        with pytest.raises(McpToolError, match="not the active lease"):
            await other.call("memkernel_fenced_append_decision", **arguments)
        for token in (True, "1", 1.0):
            with pytest.raises(McpToolError):
                await owner.call("memkernel_fenced_append_decision", **{**arguments, "fencing_token": token})
        await owner.call("memkernel_fenced_append_decision", **arguments)
        state = await other.call("memkernel_query_state")
        assert state["state"]["decisions"]["db"]["agent_id"] == "owner"
