from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator

import pytest

from memkernel import AsyncEventStore, EventType, GCConfig, MemKernel
from memkernel.core.events import EventEnvelope, new_event


class FakeClock:
    """Deterministic wall clock for lease tests."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "memkernel.db")


@pytest.fixture
async def store(db_path: str) -> AsyncIterator[AsyncEventStore]:
    async with AsyncEventStore(db_path) as s:
        await s.ensure_branch("main")
        yield s


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
async def kernel(db_path: str, clock: FakeClock) -> AsyncIterator[MemKernel]:
    k = await MemKernel.open(
        db_path,
        gc_config=GCConfig(max_token_threshold=4000),
        tokenizer="heuristic",
        lease_clock=clock,
        auto_gc=False,
    )
    try:
        yield k
    finally:
        await k.close()


def ev(
    event_type: EventType = EventType.AGENT_SCRATCHPAD,
    *,
    branch: str = "main",
    agent: str = "test-agent",
    **payload: object,
) -> EventEnvelope:
    return new_event(event_type, branch_id=branch, agent_id=agent, payload=dict(payload))


def decision(key: str, value: str, *, branch: str = "main", agent: str = "test-agent") -> EventEnvelope:
    return ev(
        EventType.ARCHITECTURAL_DECISION,
        branch=branch,
        agent=agent,
        decision_key=key,
        decision_value=value,
        rationale=f"because {value}",
    )
