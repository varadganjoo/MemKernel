"""Selective recall must use the same current, branch-aware state as full recall."""

from __future__ import annotations

import pytest

from memkernel import MemKernel
from memkernel.clients.adapter import MemKernelClient, McpStdioClient, McpToolError
from memkernel.core.store import BranchNotFoundError


async def test_lookup_current_decision_and_typed_facts(kernel: MemKernel) -> None:
    client = MemKernelClient(kernel, "author")
    first = await client.decide("auth.method", "sessions", "initial choice")
    latest = await client.decide("auth.method", "PKCE", "public client")
    facts = await client.log_execution("probe", 0, facts={"auth.method": False, "nullable": None,
                                                       " 引用' ": [1, True], "": 0, "  ": None})
    before = await kernel.store.read_stream("main")

    decision = await client.lookup_memory("auth.method")
    assert decision == {
        "branch_id": "main", "head_event_id": facts.event_id,
        "kind": "decision", "key": "auth.method",
        "record": {"key": "auth.method", "value": "PKCE", "rationale": "public client",
                   "agent_id": "author", "event_id": latest.event_id, "supersedes_event_id": first.event_id},
    }
    fact = await client.lookup_memory("auth.method", kind="fact")
    assert fact["record"] == {"value": False, "agent_id": "author", "event_id": facts.event_id}
    assert (await client.lookup_memory("nullable", kind="fact"))["record"]["value"] is None
    assert (await client.lookup_memory(" 引用' ", kind="fact"))["record"]["value"] == [1, True]
    assert (await client.lookup_memory("", kind="fact"))["record"]["value"] == 0
    assert (await client.lookup_memory("  ", kind="fact"))["record"]["value"] is None
    assert (await client.lookup_memory("missing", kind="fact"))["record"] is None
    assert (await client.lookup_memory("AUTH.METHOD"))["record"] is None
    assert await kernel.store.read_stream("main") == before


async def test_lookup_survives_compaction_fork_and_rollback(kernel: MemKernel) -> None:
    client = MemKernelClient(kernel, "owner")
    original = await client.decide("db", "sqlite", "offline")
    for _ in range(8):
        await client.log_execution("probe", 0, "diagnostic output " * 100, {"ready": True})
    assert (await client.gc(force=True)).triggered
    assert (await client.lookup_memory("ready", kind="fact"))["record"]["value"] is True
    expected = (await client.lookup_memory("db"))["record"]
    assert expected["event_id"] == original.event_id
    await client.fork_investigation("experiment")
    assert (await client.lookup_memory("db"))["record"] == expected
    await client.decide("db", "postgres", "try another backend")
    assert (await client.lookup_memory("db"))["record"]["value"] == "postgres"
    assert (await client.lookup_memory("db", branch="main"))["record"] == expected
    await client.rollback(original.event_id)
    assert (await client.lookup_memory("db"))["record"] == expected
    assert (await client.lookup_memory("ready", kind="fact"))["record"] is None
    assert await kernel.verify("main") == await kernel.verify("experiment") == []


@pytest.mark.parametrize("key,kind", [(42, "fact"), (False, "decision"), (None, "fact"), ("db", "unknown")])
async def test_lookup_rejects_invalid_selectors(kernel: MemKernel, key: object, kind: str) -> None:
    with pytest.raises(ValueError):
        await kernel.lookup_memory(key, kind=kind)


async def test_lookup_missing_branch_is_not_missing_memory(kernel: MemKernel) -> None:
    with pytest.raises(BranchNotFoundError):
        await kernel.lookup_memory("db", branch_id="missing")


async def test_lookup_stdio_read_only_and_active_branch(db_path: str) -> None:
    async with await MemKernel.open(db_path, tokenizer="heuristic", auto_gc=False) as kernel:
        main = MemKernelClient(kernel, "owner")
        event = await main.decide("db", "sqlite", "offline")
        await main.fork_investigation("experiment")
        await main.decide("db", "postgres", "experiment only")
        before = {b: await kernel.store.read_stream(b) for b in ("main", "experiment")}
        async with McpStdioClient(agent_id="observer", db_path=db_path, branch="experiment",
                                  env={"MEMKERNEL_READ_ONLY": "true", "MEMKERNEL_TOKENIZER": "heuristic"}) as client:
            assert "memkernel_lookup_memory" in await client.list_tools()
            result = await client.call("memkernel_lookup_memory", key="db")
            assert result["branch_id"] == "experiment" and result["record"]["value"] == "postgres"
            result = await client.call("memkernel_lookup_memory", key="db", branch_id="main")
            assert result["record"]["event_id"] == event.event_id and result["record"]["value"] == "sqlite"
            await client.call("memkernel_checkout", branch_id="main")
            assert (await client.call("memkernel_lookup_memory", key="db"))["branch_id"] == "main"
            assert (await client.call("memkernel_lookup_memory", key="missing"))["record"] is None
            for arguments in ({"key": None}, {"key": "db", "kind": "unknown"}, {"key": "db", "branch_id": "absent"}):
                with pytest.raises(McpToolError):
                    await client.call("memkernel_lookup_memory", **arguments)
        assert {b: await kernel.store.read_stream(b) for b in before} == before
