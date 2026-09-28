"""Immutable event taxonomy and envelope schema.

Every state change in MemKernel is an :class:`EventEnvelope` appended to a
branch's log. Envelopes are frozen pydantic models; the store assigns
``event_id``, ``sequence`` and ``hash`` at append time and returns a new
envelope rather than mutating the caller's copy.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

#: Hard cap on the serialized size of an event's ``payload`` and, separately,
#: its ``metadata``. Protects the log and every agent's context from flooding.
MAX_EVENT_BYTES = 256 * 1024

BRANCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")
AGENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$")

GENESIS_HASH = "0" * 64


class EventType(str, Enum):
    """Canonical event taxonomy."""

    AGENT_TASK_INIT = "AGENT_TASK_INIT"
    """Initial task directive and constraints for an agent."""
    ARCHITECTURAL_DECISION = "ARCHITECTURAL_DECISION"
    """Canonical, durable design choice keyed by ``decision_key``."""
    FILE_CLAIM_ACQUIRED = "FILE_CLAIM_ACQUIRED"
    """A lease on a resource was granted (carries the fencing token)."""
    FILE_CLAIM_RELEASED = "FILE_CLAIM_RELEASED"
    """A lease on a resource was released."""
    TOOL_EXECUTION_LOG = "TOOL_EXECUTION_LOG"
    """Ephemeral diagnostic / terminal output. Eligible for compaction."""
    AGENT_SCRATCHPAD = "AGENT_SCRATCHPAD"
    """Ephemeral intermediate reasoning turn. Eligible for compaction."""
    SEMANTIC_SNAPSHOT = "SEMANTIC_SNAPSHOT"
    """GC checkpoint holding distilled facts, decisions and blockers."""
    BRANCH_FORKED = "BRANCH_FORKED"
    """Audit record written as the first event of a forked branch."""
    ROLLBACK_TRIGGERED = "ROLLBACK_TRIGGERED"
    """Compensating event: the live timeline is truncated to its target."""


#: Event types whose content is transient and may be compacted by GC.
EPHEMERAL_TYPES: frozenset[EventType] = frozenset(
    {EventType.TOOL_EXECUTION_LOG, EventType.AGENT_SCRATCHPAD}
)

#: Event types that only describe timeline structure, not workspace state.
CONTROL_TYPES: frozenset[EventType] = frozenset(
    {EventType.BRANCH_FORKED, EventType.ROLLBACK_TRIGGERED}
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def canonical_json(value: Any) -> str:
    """Deterministic JSON encoding used for storage and hashing."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def validate_branch_id(branch_id: str) -> str:
    if not isinstance(branch_id, str) or not BRANCH_ID_PATTERN.fullmatch(branch_id):
        raise ValueError(
            f"invalid branch_id {branch_id!r}: expected 1-128 chars of [A-Za-z0-9._/-], "
            "starting with a letter or digit"
        )
    return branch_id


def validate_agent_id(agent_id: str) -> str:
    if not isinstance(agent_id, str) or not AGENT_ID_PATTERN.fullmatch(agent_id):
        raise ValueError(
            f"invalid agent_id {agent_id!r}: expected 1-128 chars of [A-Za-z0-9._:@-], "
            "starting with a letter or digit"
        )
    return agent_id


class EventEnvelope(BaseModel):
    """Immutable event record.

    Fields assigned by the store (``event_id``, ``sequence``, ``hash``) are
    ``None`` on envelopes that have not been persisted yet.

    ``parent_event_id`` is the lineage predecessor (the branch head at append
    time). When a caller supplies it, the store treats it as an optimistic
    concurrency precondition: the append fails with
    :class:`~memkernel.core.store.StaleHeadError` if the head has moved.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", use_enum_values=False)

    event_id: int | None = Field(default=None, ge=1)
    sequence: int | None = Field(default=None, ge=1)
    timestamp: datetime = Field(default_factory=utcnow)
    branch_id: str
    agent_id: str
    parent_event_id: int | None = Field(default=None, ge=1)
    event_type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    hash: str | None = None

    @field_validator("branch_id")
    @classmethod
    def _check_branch(cls, v: str) -> str:
        return validate_branch_id(v)

    @field_validator("agent_id")
    @classmethod
    def _check_agent(cls, v: str) -> str:
        return validate_agent_id(v)

    @field_validator("timestamp")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware (UTC)")
        return v.astimezone(timezone.utc)

    @field_validator("payload", "metadata")
    @classmethod
    def _json_safe(cls, v: dict[str, Any]) -> dict[str, Any]:
        try:
            encoded = canonical_json(v)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"payload/metadata must be JSON-serializable: {exc}") from exc
        if len(encoded.encode("utf-8")) > MAX_EVENT_BYTES:
            raise ValueError(
                f"payload/metadata exceeds {MAX_EVENT_BYTES} bytes; store large artifacts "
                "outside MemKernel and reference them by path or digest"
            )
        # Round-trip so the in-memory value is exactly what gets persisted
        # (tuples become lists, etc.), keeping hashes reproducible.
        return json.loads(encoded)

    @property
    def timestamp_iso(self) -> str:
        return self.timestamp.isoformat(timespec="microseconds")

    @property
    def is_ephemeral(self) -> bool:
        return self.event_type in EPHEMERAL_TYPES

    def hash_material(self, prev_hash: str) -> str:
        """Canonical byte string covered by this event's hash-chain link."""
        return canonical_json(
            {
                "prev": prev_hash,
                "event_id": self.event_id,
                "sequence": self.sequence,
                "timestamp": self.timestamp_iso,
                "branch_id": self.branch_id,
                "agent_id": self.agent_id,
                "parent_event_id": self.parent_event_id,
                "event_type": self.event_type.value,
                "payload": self.payload,
                "metadata": self.metadata,
            }
        )

    def compute_hash(self, prev_hash: str) -> str:
        return hashlib.sha256(self.hash_material(prev_hash).encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "sequence": self.sequence,
            "timestamp": self.timestamp_iso,
            "branch_id": self.branch_id,
            "agent_id": self.agent_id,
            "parent_event_id": self.parent_event_id,
            "event_type": self.event_type.value,
            "payload": self.payload,
            "metadata": self.metadata,
            "hash": self.hash,
        }


def new_event(
    event_type: EventType,
    *,
    branch_id: str,
    agent_id: str,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    parent_event_id: int | None = None,
) -> EventEnvelope:
    """Convenience constructor for an unpersisted envelope."""
    return EventEnvelope(
        event_type=event_type,
        branch_id=branch_id,
        agent_id=agent_id,
        payload=payload or {},
        metadata=metadata or {},
        parent_event_id=parent_event_id,
    )
