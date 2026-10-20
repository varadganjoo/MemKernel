"""Notice tools through the same stdio path used by IDE clients."""

import asyncio
import json

import pytest

from memkernel import MemKernel
from memkernel.clients.adapter import McpStdioClient, McpToolError


async def test_independent_stdio_servers_exchange_notices(tmp_path):
    async with McpStdioClient(agent_id="alice", db_path=str(tmp_path / "alice.db")) as alice, McpStdioClient(
        agent_id="bob", db_path=str(tmp_path / "bob.db")
    ) as bob:
        tools = await alice.list_tools()
        assert {"memkernel_post_notice", "memkernel_list_notices", "memkernel_clear_notice",
                "memkernel_export_notices", "memkernel_import_notices"} <= set(tools)
        notice = await alice.call("memkernel_post_notice", resource="service:auth", message="Changing login")
        assert notice["author"] == "alice"
        exported = await alice.call("memkernel_export_notices", notice_ids=[notice["notice_id"]])
        assert (await bob.call("memkernel_query_state"))["coordination_notices"] == []
        assert await bob.call("memkernel_import_notices", document=json.dumps(exported)) == {"changed": 1}
        assert await bob.call("memkernel_import_notices", document=json.dumps(exported)) == {"changed": 0}
        state = await bob.call("memkernel_query_state")
        assert state["coordination_notices"][0]["message"] == "Changing login"
        assert state["safety"]["distributed_locks_enforced"] is False
        with pytest.raises(McpToolError, match="author"):
            await bob.call("memkernel_clear_notice", notice_id=notice["notice_id"])
        with pytest.raises(McpToolError):
            await alice.call("memkernel_post_notice", resource="service:auth", message="bad", ttl_seconds=True)
        with pytest.raises(McpToolError):
            await bob.call("memkernel_import_notices", document="broken JSON")
        await alice.call("memkernel_clear_notice", notice_id=notice["notice_id"])
        cleared = await alice.call("memkernel_export_notices")
        await bob.call("memkernel_import_notices", document=json.dumps(cleared))
        await bob.call("memkernel_import_notices", document=json.dumps(exported))
        assert await bob.call("memkernel_list_notices") == {"notices": []}
        assert (await bob.call("memkernel_list_notices", include_cleared=True))["notices"][0]["status"] == "cleared"


async def test_shared_database_import_race_and_clear_against_delayed_export(tmp_path):
    async with await MemKernel.open(tmp_path / "source.db", tokenizer="heuristic") as source:
        notice = await source.notices.post(agent_id="alice", resource="service:auth", message="Working")
        bundle = await source.notices.export_json()
    shared = str(tmp_path / "shared.db")
    async with McpStdioClient(agent_id="alice", db_path=shared) as alice, McpStdioClient(
        agent_id="bob", db_path=shared
    ) as bob:
        results = await asyncio.gather(
            alice.call("memkernel_import_notices", document=bundle),
            bob.call("memkernel_import_notices", document=bundle),
        )
        assert sum(result["changed"] for result in results) == 1
        await asyncio.gather(
            alice.call("memkernel_clear_notice", notice_id=notice.notice_id),
            bob.call("memkernel_import_notices", document=bundle),
        )
        assert await alice.call("memkernel_list_notices") == {"notices": []}
        assert await bob.call("memkernel_list_notices") == {"notices": []}
