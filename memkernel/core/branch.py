"""Git-like lineage algebra: live-timeline folding, rollback planning, diff
and hash-chain verification.

Everything here is a pure function over lists of envelopes. The store runs
these functions inside its transactions; keeping them pure makes the branch
semantics testable without a database and identical on every replica.

Definitions
-----------
* **Raw lineage** ``L(b)``: every event reachable from branch ``b`` in log
  order — the parent's raw lineage up to the fork point, then ``b``'s own
  events. Forks copy no rows (copy-on-write lineage).
* **Live timeline** ``live(L)``: left fold of ``L`` where a
  ``ROLLBACK_TRIGGERED`` event targeting ``t`` truncates the live list to the
  prefix ending at ``t`` and then appends itself (so rollbacks stay auditable).
"""

from __future__ import annotations

from typing import Any

from memkernel.core.events import GENESIS_HASH, EventEnvelope, EventType
from memkernel.memory.state_view import materialize_live


class LineageError(RuntimeError):
    """The stored lineage violates a structural invariant."""


def fold_live(raw: list[EventEnvelope]) -> list[EventEnvelope]:
    """Compute the live timeline of a raw lineage."""
    live: list[EventEnvelope] = []
    position: dict[int, int] = {}
    for event in raw:
        if event.event_type is EventType.ROLLBACK_TRIGGERED:
            target = event.payload.get("target_event_id")
            pos = position.get(target) if isinstance(target, int) else None
            if pos is None:
                raise LineageError(
                    f"rollback event {event.event_id} targets {target!r}, "
                    "which is not live at that point in the lineage"
                )
            for dropped in live[pos + 1 :]:
                del position[dropped.event_id]  # type: ignore[arg-type]
            del live[pos + 1 :]
        position[event.event_id] = len(live)  # type: ignore[index]
        live.append(event)
    return live


def plan_rollback(live: list[EventEnvelope], target_event_id: int) -> tuple[EventEnvelope, list[int]]:
    """Validate a rollback target and return ``(target, reverted_event_ids)``.

    Only live events are valid targets: an event that is already reverted
    cannot be restored by rolling "forward" to it — fork from it instead.
    """
    for index, event in enumerate(live):
        if event.event_id == target_event_id:
            return event, [e.event_id for e in live[index + 1 :]]  # type: ignore[misc]
    raise ValueError(
        f"event {target_event_id} is not on the live timeline of this branch; "
        "to recover a reverted or foreign event, fork a new branch from it"
    )


def merge_base(raw_a: list[EventEnvelope], raw_b: list[EventEnvelope]) -> EventEnvelope | None:
    """Last event shared by two raw lineages (their longest common prefix)."""
    base = None
    for a, b in zip(raw_a, raw_b):
        if a.event_id != b.event_id:
            break
        base = a
    return base


def _summary(e: EventEnvelope) -> dict[str, Any]:
    return {
        "event_id": e.event_id,
        "sequence": e.sequence,
        "agent_id": e.agent_id,
        "event_type": e.event_type.value,
    }


def diff_lineages(
    branch_a: str, raw_a: list[EventEnvelope], branch_b: str, raw_b: list[EventEnvelope]
) -> dict[str, Any]:
    """Divergence report between two branches.

    Returns the merge base, the live events each side has beyond it, and a
    key-by-key comparison of materialized decisions and workspace facts.
    """
    base = merge_base(raw_a, raw_b)
    base_seq = (base.sequence or 0) if base is not None else 0
    live_a, live_b = fold_live(raw_a), fold_live(raw_b)
    state_a, state_b = materialize_live(live_a), materialize_live(live_b)

    def compare(left: dict[str, Any], right: dict[str, Any], field: str) -> dict[str, Any]:
        conflicts, only_left, only_right = [], {}, {}
        for key in sorted(left.keys() | right.keys()):
            if key not in right:
                only_left[key] = left[key]
            elif key not in left:
                only_right[key] = right[key]
            elif left[key][field] != right[key][field]:
                conflicts.append({"key": key, branch_a: left[key], branch_b: right[key]})
        return {"conflicts": conflicts, "only_in_a": only_left, "only_in_b": only_right}

    decisions = compare(state_a.sections()["decisions"], state_b.sections()["decisions"], "value")
    facts = compare(
        state_a.sections()["active_workspace_facts"],
        state_b.sections()["active_workspace_facts"],
        "value",
    )
    ahead_a = [_summary(e) for e in live_a if (e.sequence or 0) > base_seq]
    ahead_b = [_summary(e) for e in live_b if (e.sequence or 0) > base_seq]
    rollbacks = {
        branch: [
            {"event_id": e.event_id, "target_event_id": e.payload.get("target_event_id")}
            for e in raw
            if e.event_type is EventType.ROLLBACK_TRIGGERED and (e.sequence or 0) > base_seq
        ]
        for branch, raw in ((branch_a, raw_a), (branch_b, raw_b))
    }
    return {
        "branch_a": branch_a,
        "branch_b": branch_b,
        "merge_base_event_id": base.event_id if base is not None else None,
        "merge_base_sequence": base_seq,
        "ahead_a": ahead_a,
        "ahead_b": ahead_b,
        "rollbacks_since_base": rollbacks,
        "conflicting_decisions": decisions["conflicts"],
        "decisions_only_in_a": decisions["only_in_a"],
        "decisions_only_in_b": decisions["only_in_b"],
        "conflicting_facts": facts["conflicts"],
        "identical_state": state_a.core_sections() == state_b.core_sections(),
    }


def verify_lineage(raw: list[EventEnvelope]) -> list[str]:
    """Check sequence contiguity, parent pointers and the SHA-256 hash chain.

    Returns a list of human-readable violations (empty means intact).
    """
    problems: list[str] = []
    prev_hash = GENESIS_HASH
    prev_id: int | None = None
    for index, event in enumerate(raw, start=1):
        if event.sequence != index:
            problems.append(f"event {event.event_id}: sequence {event.sequence} != expected {index}")
        if event.parent_event_id != prev_id:
            problems.append(
                f"event {event.event_id}: parent_event_id {event.parent_event_id} != predecessor {prev_id}"
            )
        expected = event.compute_hash(prev_hash)
        if event.hash != expected:
            problems.append(f"event {event.event_id}: hash mismatch (content or chain tampered)")
        prev_hash = event.hash or ""
        prev_id = event.event_id
    return problems
