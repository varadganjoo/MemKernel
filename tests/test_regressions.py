"""Failure cases found during the production-readiness review."""

import asyncio
import json
import sqlite3
import tomllib
from pathlib import Path

import pytest

from memkernel import AsyncEventStore, EventType, MemKernel
from memkernel import cli
from memkernel.clients.adapter import MemKernelClient
from memkernel.clients.adapter import McpStdioClient
from memkernel.core.events import MAX_EVENT_BYTES
from memkernel.memory.state_view import SNAPSHOT_SCHEMA, materialize
from memkernel.memory.token_counter import heuristic_count
from memkernel.server import mcp_server as srv
from tests.conftest import decision, ev


@pytest.mark.parametrize("change", ["erase", "replace", "provenance"])
async def test_snapshot_must_preserve_covered_state(store, change):
    covered = await store.append(decision("storage", "sqlite"))
    state = materialize(await store.read_live("main")).sections()
    if change == "erase":
        state["decisions"].clear()
    elif change == "replace":
        state["decisions"]["storage"]["value"] = "foreign"
    else:
        state["decisions"]["storage"]["agent_id"] = "someone-else"
    with pytest.raises(ValueError, match="preserve"):
        await store.append(ev(
            EventType.SEMANTIC_SNAPSHOT, schema=SNAPSHOT_SCHEMA,
            covers_through_event_id=covered.event_id,
            covers_through_sequence=covered.sequence, **state,
        ))
    assert (await store.get_branch("main")).head_event_id == covered.event_id
    assert not await store.verify_branch("main")


@pytest.mark.parametrize("original,replacement", [(1, True), (1, 1.0), ({"enabled": 0}, {"enabled": False})],
                         ids=["boolean", "float", "nested-boolean"])
async def test_snapshot_preserves_json_fact_types(store, original, replacement):
    covered = await store.append(ev(EventType.TOOL_EXECUTION_LOG, command="probe", exit_code=0,
                                    facts={"policy": original}))
    state = materialize(await store.read_live("main")).sections()
    state["active_workspace_facts"]["policy"]["value"] = replacement
    with pytest.raises(ValueError, match="preserve"):
        await store.append(ev(
            EventType.SEMANTIC_SNAPSHOT, schema=SNAPSHOT_SCHEMA,
            covers_through_event_id=covered.event_id,
            covers_through_sequence=covered.sequence, **state,
        ))
    assert (await store.get_branch("main")).head_event_id == covered.event_id


async def test_snapshot_accepts_exact_state_and_summarizer_insights(store):
    covered = await store.append(decision("storage", "sqlite"))
    state = materialize(await store.read_live("main")).sections()
    state["insights"] = ["A summarizer may add an advisory insight."]
    snapshot = await store.append(ev(
        EventType.SEMANTIC_SNAPSHOT, schema=SNAPSHOT_SCHEMA,
        covers_through_event_id=covered.event_id,
        covers_through_sequence=covered.sequence, **state,
    ))
    assert (await store.context_events("main")).snapshot == snapshot


async def test_failed_commit_rolls_back_and_connection_remains_usable(store):
    with pytest.raises(sqlite3.IntegrityError):
        async with store.transaction(write=True) as db:
            await db.execute("PRAGMA defer_foreign_keys = ON")
            await db.execute(
                "INSERT INTO branches (branch_id, parent_branch_id, head_hash, created_at, created_by) "
                "SELECT 'orphan', 'missing', head_hash, created_at, created_by FROM branches WHERE branch_id = 'main'"
            )
    assert [b.branch_id for b in await store.list_branches()] == ["main"]
    await store.append(ev(note="connection recovered"))


async def test_cancelled_begin_does_not_leave_transaction_open(store, monkeypatch):
    execute = store._db.execute

    async def cancelled_begin(sql, *args, **kwargs):
        result = await execute(sql, *args, **kwargs)
        if sql == "BEGIN IMMEDIATE":
            asyncio.current_task().cancel()
            await asyncio.sleep(0)
        return result

    monkeypatch.setattr(store._db, "execute", cancelled_begin)

    async def write():
        async with store.transaction(write=True):
            pytest.fail("cancelled transaction must not enter its body")

    task = asyncio.create_task(write())
    with pytest.raises(asyncio.CancelledError):
        await task
    monkeypatch.setattr(store._db, "execute", execute)
    assert [b.branch_id for b in await store.list_branches()] == ["main"]


@pytest.mark.parametrize("field", ["payload", "metadata"])
async def test_mutated_envelope_cannot_bypass_size_limit(store, field):
    envelope = ev(note="valid initially")
    getattr(envelope, field)["oversized"] = "x" * (MAX_EVENT_BYTES + 1)
    with pytest.raises(ValueError, match="exceeds"):
        await store.append(envelope)
    assert await store.read_stream("main") == []


@pytest.mark.parametrize("number", [float("nan"), float("inf"), -float("inf")])
def test_events_reject_non_finite_json_numbers(number):
    with pytest.raises(ValueError, match="JSON"):
        ev(value=number)


@pytest.mark.parametrize("text", ["界" * 100, "🙂" * 100, "qzjx" * 100, " \n\t" * 100],
                         ids=["cjk", "emoji", "source-like", "whitespace"])
def test_offline_token_budget_uses_a_byte_level_upper_bound(text):
    assert heuristic_count(text) == len(text.encode("utf-8"))


async def test_failed_kernel_initialization_closes_store(db_path, monkeypatch):
    stores = []
    original_open = AsyncEventStore.open

    async def track_open(store):
        stores.append(store)
        return await original_open(store)

    monkeypatch.setattr(AsyncEventStore, "open", track_open)
    try:
        with pytest.raises(ValueError, match="unknown tokenizer"):
            await MemKernel.open(db_path, tokenizer="misspelled")
        assert len(stores) == 1 and stores[0]._db is None
    finally:
        for store in stores:
            await store.close()


async def test_heartbeat_error_marks_lease_lost_and_releases(kernel, monkeypatch):
    attempted = asyncio.Event()

    async def unavailable(**_kwargs):
        attempted.set()
        raise OSError("storage unavailable")

    monkeypatch.setattr(kernel, "heartbeat_lease", unavailable)
    client = MemKernelClient(kernel, "owner")
    async with client.lease("shared.py", ttl_seconds=0.03) as handle:
        await asyncio.wait_for(attempted.wait(), 1)
        await asyncio.sleep(0)
        assert handle.lost
    assert await kernel.locks.list_leases() == []


async def test_heartbeat_error_does_not_mask_work_error(kernel, monkeypatch):
    attempted = asyncio.Event()

    async def unavailable(**_kwargs):
        attempted.set()
        raise OSError("storage unavailable")

    monkeypatch.setattr(kernel, "heartbeat_lease", unavailable)
    with pytest.raises(RuntimeError, match="work failed"):
        async with MemKernelClient(kernel, "owner").lease("shared.py", ttl_seconds=0.03):
            await asyncio.wait_for(attempted.wait(), 1)
            raise RuntimeError("work failed")
    assert await kernel.locks.list_leases() == []


async def test_store_expands_home_for_writable_and_read_only_opens(tmp_path, monkeypatch):
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    async with AsyncEventStore("~/nested/memory.db") as store:
        await store.ensure_branch("main")
        await store.append(decision("storage", "sqlite"))
    assert (tmp_path / "nested" / "memory.db").exists()
    async with AsyncEventStore("~/nested/memory.db", read_only=True) as reader:
        assert len(await reader.read_live("main")) == 1


async def test_mcp_defaults_follow_active_branch(db_path):
    await srv.configure(db_path=db_path, agent_id="reviewer")
    try:
        await srv.memkernel_append_decision("storage", "sqlite", "main")
        await srv.memkernel_fork_investigation("investigation")
        await srv.memkernel_append_decision("storage", "other", "experiment")
        state = json.loads(await srv.memkernel_query_state())
        assert state["branch_id"] == state["active_branch"] == "investigation"
        assert state["state"]["decisions"]["storage"]["value"] == "other"
        assert (await srv.memkernel_trigger_gc())["branch_id"] == "investigation"
        main = json.loads(await srv.memkernel_query_state("main"))
        assert main["state"]["decisions"]["storage"]["value"] == "sqlite"
    finally:
        await srv.SESSION.close()


def test_attach_pins_relative_database_to_invocation_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    project = tmp_path / "project"
    home = tmp_path / "home"
    cli.attach(["claude-code", "codex"], project=project, home=home, db=Path("memory.db"))
    claude = json.loads((project / ".mcp.json").read_text())["mcpServers"]["memkernel"]
    assert claude["env"]["MEMKERNEL_DB"] == str(tmp_path / "memory.db")


def test_attach_rejects_invalid_identity_before_writing(tmp_path):
    with pytest.raises(ValueError, match="agent_id"):
        cli.attach(["claude-code"], project=tmp_path, home=tmp_path, agent_id="invalid identity")
    assert not (tmp_path / ".memkernel").exists()
    assert not (tmp_path / ".mcp.json").exists()


@pytest.mark.parametrize("client", ["claude-code", "codex"])
def test_reattach_preserves_read_only_and_sync_policy(tmp_path, client):
    project, home = tmp_path / "project", tmp_path / "home"
    project.mkdir()
    if client == "claude-code":
        path = project / ".mcp.json"
        path.write_text(json.dumps({"mcpServers": {"memkernel": {
            "command": "old-python", "args": ["-m", cli.SERVER_MODULE, "--read-only"],
            "env": {"MEMKERNEL_READ_ONLY": "true", "MEMKERNEL_SYNC_CONFIG": "policy.json"},
        }}}))
    else:
        path = home / ".codex" / "config.toml"
        path.parent.mkdir(parents=True)
        path.write_text(
            '[mcp_servers.memkernel]\ncommand="old-python"\n'
            'args=["-m","memkernel.server.mcp_server","--read-only"]\n'
            'default_tools_approval_mode="prompt"\n'
            '[mcp_servers.memkernel.env]\nMEMKERNEL_READ_ONLY="true"\n'
            'MEMKERNEL_SYNC_CONFIG="policy.json"\n'
        )
    cli.attach([client], project=project, home=home)
    if client == "claude-code":
        entry = json.loads(path.read_text())["mcpServers"]["memkernel"]
    else:
        entry = tomllib.loads(path.read_text())["mcp_servers"]["memkernel"]
        assert entry["default_tools_approval_mode"] == "prompt"
    assert entry["env"]["MEMKERNEL_READ_ONLY"] == "true"
    assert entry["env"]["MEMKERNEL_SYNC_CONFIG"] == "policy.json"
    assert entry["args"][-1] == "--read-only"
    before = path.read_bytes()
    cli.attach([client], project=project, home=home)
    assert path.read_bytes() == before


def test_toml_roundtrip_preserves_unicode_and_refuses_custom_settings(tmp_path):
    path = tmp_path / "config.toml"
    entry = {"command": "python", "args": ["-m", cli.SERVER_MODULE],
             "env": {"MEMKERNEL_DB": str(tmp_path / "memory-🙂.db")}}
    _, text = cli.merge_toml_config(path, entry)
    assert tomllib.loads(text)["mcp_servers"]["memkernel"]["env"] == entry["env"]
    original = '[mcp_servers.memkernel]\ncommand="old"\nargs=[]\nenabled=false\n'
    path.write_text(original)
    with pytest.raises(cli.AttachError, match="custom memkernel settings"):
        cli.merge_toml_config(path, entry)
    assert path.read_text() == original


def test_atomic_config_failure_keeps_original_and_removes_temporary(tmp_path, monkeypatch):
    import os

    path = tmp_path / "config.json"
    path.write_text("original")

    def fail_replace(*_args):
        raise OSError("disk unavailable")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="disk unavailable"):
        cli._atomic_write(path, "new")
    assert path.read_text() == "original"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["config.json"]


async def test_stdio_sessions_recall_their_active_branch_independently(db_path):
    async with McpStdioClient(agent_id="reviewer", db_path=db_path) as reviewer, McpStdioClient(
        agent_id="observer", db_path=db_path
    ) as observer:
        await reviewer.call("memkernel_append_decision", decision_key="storage",
                            decision_value="sqlite", rationale="main")
        await reviewer.call("memkernel_fork_investigation", fork_branch_name="investigation")
        await reviewer.call("memkernel_append_decision", decision_key="storage",
                            decision_value="other", rationale="experiment")
        state = await reviewer.call("memkernel_query_state")
        assert state["branch_id"] == "investigation"
        assert state["state"]["decisions"]["storage"]["value"] == "other"
        assert (await observer.call("memkernel_query_state"))["state"]["decisions"]["storage"]["value"] == "sqlite"
        assert (await reviewer.call("memkernel_trigger_gc"))["branch_id"] == "investigation"
