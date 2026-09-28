"""Consolidated entity-state materializer.

``WorkspaceState`` is the left fold of a branch's live events::

    S_t = f(S_0, E_1 .. E_t)

The fold is pure and deterministic. A ``SEMANTIC_SNAPSHOT`` payload is an
exact serialization of the fold at its ``covers_through_sequence``, so
``materialize(delta, seed=snapshot)`` equals ``materialize(full_history)`` —
the soundness property the GC relies on (verified in ``tests/test_gc.py``).
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterable

from pydantic import BaseModel, ConfigDict, Field

from memkernel.core.events import EventEnvelope, EventType, canonical_json

SNAPSHOT_SCHEMA = "memkernel.snapshot/v1"

#: Sections of ``WorkspaceState`` that are persisted verbatim in snapshots.
SECTION_KEYS: tuple[str, ...] = (
    "tasks",
    "decisions",
    "active_workspace_facts",
    "deprecated_assumptions",
    "execution_blockers",
    "active_claims",
    "execution_digest",
    "insights",
)

# ponytail: fixed caps keep snapshots bounded; make them GCConfig fields if
# a deployment needs longer audit trails inside the context window.
MAX_DEPRECATED = 50
MAX_DIGEST = 20
MAX_BLOCKERS = 50
MAX_INSIGHTS = 12
MAX_OUTPUT_CHARS = 4000
MAX_BLOCKER_CHARS = 300


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TaskRecord(_Model):
    agent_id: str
    directive: str
    constraints: list[str] = Field(default_factory=list)
    event_id: int


class DecisionRecord(_Model):
    key: str
    value: str
    rationale: str
    agent_id: str
    event_id: int
    supersedes_event_id: int | None = None


class FactRecord(_Model):
    value: Any
    agent_id: str
    event_id: int


class DeprecatedAssumption(_Model):
    kind: str  # "decision" | "fact"
    key: str
    value: Any
    replaced_by: Any
    deprecated_by_event_id: int
    agent_id: str


class Blocker(_Model):
    key: str
    description: str
    exit_code: int
    agent_id: str
    first_seen_event_id: int
    last_seen_event_id: int
    occurrences: int = 1


class ClaimRecord(_Model):
    resource: str
    agent_id: str
    fencing_token: int
    event_id: int


class DigestEntry(_Model):
    command: str
    runs: int = 0
    failed_runs: int = 0  # historical count; last_exit_code is the current status
    last_exit_code: int
    last_agent_id: str
    last_event_id: int


class WorkspaceState(_Model):
    tasks: dict[str, TaskRecord] = Field(default_factory=dict)
    decisions: dict[str, DecisionRecord] = Field(default_factory=dict)
    active_workspace_facts: dict[str, FactRecord] = Field(default_factory=dict)
    deprecated_assumptions: list[DeprecatedAssumption] = Field(default_factory=list)
    execution_blockers: dict[str, Blocker] = Field(default_factory=dict)
    active_claims: dict[str, ClaimRecord] = Field(default_factory=dict)
    execution_digest: dict[str, DigestEntry] = Field(default_factory=dict)
    insights: list[str] = Field(default_factory=list)

    def sections(self) -> dict[str, Any]:
        """JSON-ready dict of all persisted sections."""
        return self.model_dump(mode="json")

    def core_sections(self) -> dict[str, Any]:
        """Sections that a full-history fold reproduces exactly (excludes
        ``insights``, which only a summarizer can produce)."""
        data = self.sections()
        data.pop("insights")
        return data

    @classmethod
    def from_snapshot(cls, snapshot: EventEnvelope) -> "WorkspaceState":
        if snapshot.event_type is not EventType.SEMANTIC_SNAPSHOT:
            raise ValueError(f"event {snapshot.event_id} is not a SEMANTIC_SNAPSHOT")
        return cls.model_validate({k: snapshot.payload.get(k, _empty(k)) for k in SECTION_KEYS})

    # ------------------------------------------------------------------ fold

    def apply(self, event: EventEnvelope) -> None:
        """Apply one live event in place. Unknown payload keys are ignored."""
        handler = _HANDLERS.get(event.event_type)
        if handler is not None:
            handler(self, event)

    def _deprecate(self, item: DeprecatedAssumption) -> None:
        self.deprecated_assumptions.append(item)
        if len(self.deprecated_assumptions) > MAX_DEPRECATED:
            del self.deprecated_assumptions[: len(self.deprecated_assumptions) - MAX_DEPRECATED]


def _empty(key: str) -> Any:
    return [] if key in ("deprecated_assumptions", "insights") else {}


def _str(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, sort_keys=True)


def _apply_task(state: WorkspaceState, e: EventEnvelope) -> None:
    constraints = e.payload.get("constraints") or []
    if not isinstance(constraints, list):
        constraints = [constraints]
    state.tasks[e.agent_id] = TaskRecord(
        agent_id=e.agent_id,
        directive=_str(e.payload.get("directive", "")),
        constraints=[_str(c) for c in constraints],
        event_id=e.event_id or 0,
    )


def _apply_decision(state: WorkspaceState, e: EventEnvelope) -> None:
    key = _str(e.payload.get("decision_key", ""))
    if not key:
        return
    value = _str(e.payload.get("decision_value", ""))
    previous = state.decisions.get(key)
    if previous is not None and previous.value != value:
        state._deprecate(
            DeprecatedAssumption(
                kind="decision",
                key=key,
                value=previous.value,
                replaced_by=value,
                deprecated_by_event_id=e.event_id or 0,
                agent_id=e.agent_id,
            )
        )
    state.decisions[key] = DecisionRecord(
        key=key,
        value=value,
        rationale=_str(e.payload.get("rationale", "")),
        agent_id=e.agent_id,
        event_id=e.event_id or 0,
        supersedes_event_id=previous.event_id if previous is not None else None,
    )


_FAILURE_LINE = re.compile(r"^(FAILED|ERROR)\b")
_EXCEPTION_LINE = re.compile(r"\b\w*(Error|Exception)\b:")
_SIGNAL_LINE = re.compile(r"error|\berr\b|fail|exception|traceback|fatal|panic", re.IGNORECASE)


def blocker_description(output: str, exit_code: int) -> str:
    """Most diagnostic line of a failed command's output: a test-runner
    ``FAILED``/``ERROR`` line, else an exception line, else the last line
    mentioning an error, else the last line."""
    lines = [line.strip().strip("=").strip() for line in output.splitlines()]
    lines = [line for line in lines if line]
    for pattern, pick_first in ((_FAILURE_LINE, True), (_EXCEPTION_LINE, True), (_SIGNAL_LINE, False)):
        matches = [line for line in lines if pattern.search(line)]
        if matches:
            return (matches[0] if pick_first else matches[-1])[:MAX_BLOCKER_CHARS]
    return (lines[-1] if lines else f"exit code {exit_code}")[:MAX_BLOCKER_CHARS]


def _apply_tool_log(state: WorkspaceState, e: EventEnvelope) -> None:
    command = _str(e.payload.get("command", "")).strip()
    raw_exit = e.payload.get("exit_code", 0)
    exit_code = raw_exit if isinstance(raw_exit, int) and not isinstance(raw_exit, bool) else 0
    event_id = e.event_id or 0

    facts = e.payload.get("facts") or {}
    if isinstance(facts, dict):
        for key in sorted(facts):
            value = facts[key]
            previous = state.active_workspace_facts.get(key)
            if previous is not None and previous.value != value:
                state._deprecate(
                    DeprecatedAssumption(
                        kind="fact",
                        key=key,
                        value=previous.value,
                        replaced_by=value,
                        deprecated_by_event_id=event_id,
                        agent_id=e.agent_id,
                    )
                )
            state.active_workspace_facts[key] = FactRecord(
                value=value, agent_id=e.agent_id, event_id=event_id
            )

    if not command:
        return

    entry = state.execution_digest.get(command)
    if entry is None:
        entry = DigestEntry(
            command=command, last_exit_code=exit_code, last_agent_id=e.agent_id, last_event_id=event_id
        )
        state.execution_digest[command] = entry
    entry.runs += 1
    entry.failed_runs += 1 if exit_code != 0 else 0
    entry.last_exit_code = exit_code
    entry.last_agent_id = e.agent_id
    entry.last_event_id = event_id
    if len(state.execution_digest) > MAX_DIGEST:
        # Evict by recency, never by dict order: snapshots persist with sorted keys.
        stalest = min(state.execution_digest.values(), key=lambda d: d.last_event_id)
        del state.execution_digest[stalest.command]

    if exit_code == 0:
        state.execution_blockers.pop(command, None)
        return
    description = blocker_description(_str(e.payload.get("output", "")), exit_code)
    existing = state.execution_blockers.get(command)
    state.execution_blockers[command] = Blocker(
        key=command,
        description=description,
        exit_code=exit_code,
        agent_id=e.agent_id,
        first_seen_event_id=existing.first_seen_event_id if existing else event_id,
        last_seen_event_id=event_id,
        occurrences=(existing.occurrences + 1) if existing else 1,
    )
    if len(state.execution_blockers) > MAX_BLOCKERS:
        stalest = min(state.execution_blockers.values(), key=lambda b: b.last_seen_event_id)
        del state.execution_blockers[stalest.key]


def _apply_claim(state: WorkspaceState, e: EventEnvelope) -> None:
    resource = _str(e.payload.get("resource", ""))
    token = e.payload.get("fencing_token")
    if not resource or not isinstance(token, int):
        return
    state.active_claims[resource] = ClaimRecord(
        resource=resource, agent_id=e.agent_id, fencing_token=token, event_id=e.event_id or 0
    )


def _apply_release(state: WorkspaceState, e: EventEnvelope) -> None:
    resource = _str(e.payload.get("resource", ""))
    current = state.active_claims.get(resource)
    if current is not None and current.fencing_token == e.payload.get("fencing_token"):
        del state.active_claims[resource]


_HANDLERS = {
    EventType.AGENT_TASK_INIT: _apply_task,
    EventType.ARCHITECTURAL_DECISION: _apply_decision,
    EventType.TOOL_EXECUTION_LOG: _apply_tool_log,
    EventType.FILE_CLAIM_ACQUIRED: _apply_claim,
    EventType.FILE_CLAIM_RELEASED: _apply_release,
}


# ------------------------------------------------------------ context split


def split_context(live: list[EventEnvelope]) -> tuple[EventEnvelope | None, list[EventEnvelope]]:
    """Split a live timeline into ``(latest snapshot, delta)``.

    The delta is every live, non-snapshot event after the snapshot's
    ``covers_through_sequence`` (events the snapshot has not absorbed, which
    may precede the snapshot's own position in the log).
    """
    snapshot = None
    for event in reversed(live):
        if event.event_type is EventType.SEMANTIC_SNAPSHOT:
            snapshot = event
            break
    covered = int(snapshot.payload["covers_through_sequence"]) if snapshot else 0
    delta = [
        e
        for e in live
        if (e.sequence or 0) > covered and e.event_type is not EventType.SEMANTIC_SNAPSHOT
    ]
    return snapshot, delta


def materialize(events: Iterable[EventEnvelope], seed: EventEnvelope | None = None) -> WorkspaceState:
    """Fold ``events`` (live, in order, snapshots skipped) onto ``seed``."""
    state = WorkspaceState.from_snapshot(seed) if seed is not None else WorkspaceState()
    for event in events:
        if event.event_type is not EventType.SEMANTIC_SNAPSHOT:
            state.apply(event)
    return state


def materialize_live(live: list[EventEnvelope]) -> WorkspaceState:
    """Materialize a live timeline through its latest snapshot."""
    snapshot, delta = split_context(live)
    return materialize(delta, seed=snapshot)


# ---------------------------------------------------------------- rendering


def render_event(event: EventEnvelope) -> str:
    """Compact, LLM-readable one-block rendering of an event."""
    head = f"[#{event.event_id} seq={event.sequence} {event.agent_id} {event.event_type.value}]"
    p = event.payload
    t = event.event_type
    if t is EventType.TOOL_EXECUTION_LOG:
        output = _str(p.get("output", ""))
        if len(output) > MAX_OUTPUT_CHARS:
            output = "...[truncated]\n" + output[-MAX_OUTPUT_CHARS:]
        body = f"$ {_str(p.get('command', ''))} (exit {p.get('exit_code', 0)})\n{output}"
        if p.get("facts"):
            body += f"\nfacts: {canonical_json(p['facts'])}"
        return f"{head} {body}"
    if t is EventType.ARCHITECTURAL_DECISION:
        return (
            f"{head} {_str(p.get('decision_key', ''))} = {_str(p.get('decision_value', ''))}"
            f" | rationale: {_str(p.get('rationale', ''))}"
        )
    if t is EventType.AGENT_SCRATCHPAD:
        return f"{head} {_str(p.get('note', ''))}"
    if t is EventType.SEMANTIC_SNAPSHOT:
        coverage = f"covers events through #{p.get('covers_through_event_id')}"
        return "\n".join([f"{head} {coverage}", render_snapshot_sections(p)])
    return f"{head} {canonical_json(p)}"


def _j(value: Any) -> str:
    return value if isinstance(value, str) else canonical_json(value)


def render_snapshot_sections(p: dict[str, Any]) -> str:
    """Line-oriented rendering of snapshot sections for agent context.

    Far denser than the JSON payload (which remains the source of truth), and
    every durable item keeps its provenance (``agent #event_id``).
    """
    lines: list[str] = []
    for agent, t in sorted((p.get("tasks") or {}).items()):
        constraints = f" | constraints: {'; '.join(t['constraints'])}" if t.get("constraints") else ""
        lines.append(f"TASK {agent}: {t['directive']}{constraints}")
    for key, d in sorted((p.get("decisions") or {}).items()):
        lines.append(f"DECISION {key} = {d['value']} | {d['rationale']} ({d['agent_id']} #{d['event_id']})")
    for key, f in sorted((p.get("active_workspace_facts") or {}).items()):
        lines.append(f"FACT {key} = {_j(f['value'])} ({f['agent_id']} #{f['event_id']})")
    for d in p.get("deprecated_assumptions") or []:
        lines.append(
            f"DEPRECATED {d['kind']} {d['key']}: {_j(d['value'])} -> {_j(d['replaced_by'])} "
            f"(#{d['deprecated_by_event_id']})"
        )
    for key, b in sorted((p.get("execution_blockers") or {}).items()):
        lines.append(
            f"BLOCKER {key}: {b['description']} (exit {b['exit_code']}, x{b['occurrences']}, "
            f"{b['agent_id']} #{b['last_seen_event_id']})"
        )
    for resource, c in sorted((p.get("active_claims") or {}).items()):
        lines.append(f"CLAIM {resource} by {c['agent_id']} token {c['fencing_token']}")
    for command, d in sorted((p.get("execution_digest") or {}).items()):
        lines.append(f"RAN {command} last exit {d['last_exit_code']} ({d['runs']} runs, {d['failed_runs']} failed)")
    for insight in p.get("insights") or []:
        lines.append(f"INSIGHT {insight}")
    return "\n".join(lines)


def render_context(snapshot: EventEnvelope | None, delta: list[EventEnvelope]) -> str:
    parts = [render_event(snapshot)] if snapshot is not None else []
    parts.extend(render_event(e) for e in delta)
    return "\n".join(parts)
