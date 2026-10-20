import pytest


async def test_context_budget_reports_omissions_without_hiding_them(kernel):
    for i in range(30):
        await kernel.notices.post(
            agent_id="alice", resource="service:auth", message=f"Working {i}"
        )
    state = await kernel.query_state(notice_token_budget=500)
    assert 0 < len(state["coordination_notices"]) < 30
    assert state["notice_page"]["omitted"] > 0
    assert state["notice_page"]["total"] == 30
    assert state["coordination_notice_tokens"] <= 500
    context = await kernel.context(notice_token_budget=500)
    assert "omitted" in context and "not permission" in context
    assert state["context_tokens"] == kernel.gc_engine.counter.count(context)


async def test_scope_filter_and_pagination_can_retrieve_every_notice(kernel):
    for resource in ("service:auth", "service:billing"):
        for i in range(8):
            await kernel.notices.post(
                agent_id="alice", resource=resource, message=f"Working {i}"
            )
    ids, offset = set(), 0
    while True:
        state = await kernel.query_state(
            notice_resource="service:auth",
            notice_token_budget=500,
            notice_offset=offset,
        )
        for notice in state["coordination_notices"]:
            assert notice["resource"] == "service:auth"
            assert notice["notice_id"] not in ids
            ids.add(notice["notice_id"])
        next_offset = state["notice_page"]["next_offset"]
        if next_offset is None:
            break
        assert next_offset > offset
        offset = next_offset
    assert len(ids) == 8


@pytest.mark.parametrize(
    "kwargs",
    [
        {"notice_token_budget": True},
        {"notice_token_budget": -1},
        {"notice_offset": -1},
        {"notice_token_budget": 1000000},
    ],
)
async def test_context_selection_validates_limits(kernel, kwargs):
    with pytest.raises(ValueError):
        await kernel.query_state(**kwargs)


async def test_notice_that_does_not_fit_can_be_loaded_with_larger_budget(kernel):
    await kernel.notices.post(
        agent_id="alice", resource="service:auth", message="long " * 700
    )
    state = await kernel.query_state(notice_token_budget=128)
    assert state["coordination_notices"] == []
    assert state["notice_page"]["omitted"] == 1
    assert state["notice_page"]["requires_larger_budget"] is True
    assert (
        len(
            (await kernel.query_state(notice_token_budget=4000))["coordination_notices"]
        )
        == 1
    )
