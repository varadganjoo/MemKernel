"""Workspace-wide advisory notices and explicit, offline JSON exchange.

Notice bodies are immutable. Clearing is monotonic: an old export cannot
reopen a notice. The earliest clear timestamp wins concurrent clears, solely
to make replicas converge, not to establish a trusted ordering of clocks.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, NoReturn
from uuid import uuid4

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from memkernel.core.events import canonical_json, utcnow, validate_agent_id
from memkernel.core.store import AsyncEventStore

NOTICE_SCHEMA = "memkernel.notices/v1"
MAX_EXCHANGE_BYTES = 1024 * 1024
MAX_EXCHANGE_NOTICES = 1000


class CoordinationNotice(BaseModel):
    """Self-asserted, advisory information. This is never a lease or credential."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    notice_id: str = Field(
        pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )
    author: str = Field(strict=True)
    resource: str = Field(
        strict=True,
        pattern=r"^[a-z][a-z0-9_.-]{0,31}:[A-Za-z0-9][A-Za-z0-9._/-]{0,191}$",
    )
    message: str = Field(strict=True, min_length=1, max_length=4096)
    created_at: AwareDatetime
    expires_at: AwareDatetime
    cleared_at: AwareDatetime | None = None

    @field_validator("author")
    @classmethod
    def valid_author(cls, value: str) -> str:
        return validate_agent_id(value)

    @field_validator("message")
    @classmethod
    def valid_message(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("message must be non-empty")
        try:
            value.encode("utf-8")
        except UnicodeError as exc:
            raise ValueError("message must be valid UTF-8 text") from exc
        return value

    @field_validator("created_at", "expires_at", "cleared_at", mode="before")
    @classmethod
    def datetime_input(cls, value: object) -> object:
        if value is not None and not isinstance(value, (str, datetime)):
            raise ValueError(
                "timestamps must be timezone-aware ISO strings or datetimes"
            )
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return value

    @field_validator("created_at", "expires_at", "cleared_at")
    @classmethod
    def utc(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(UTC) if value is not None else None

    @model_validator(mode="after")
    def lifecycle(self) -> CoordinationNotice:
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be after created_at")
        if self.cleared_at is not None and self.cleared_at < self.created_at:
            raise ValueError("cleared_at must not precede created_at")
        return self


class _Exchange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["memkernel.notices/v1"] = Field(alias="schema")
    notices: list[CoordinationNotice] = Field(max_length=MAX_EXCHANGE_NOTICES)

    @model_validator(mode="after")
    def unique_ids(self) -> _Exchange:
        ids = [n.notice_id for n in self.notices]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate notice_id in exchange")
        return self


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> NoReturn:
    raise ValueError(f"invalid JSON constant: {value}")


def _body(notice: CoordinationNotice) -> str:
    return canonical_json(notice.model_dump(mode="json", exclude={"cleared_at"}))


def _decode(row: tuple[str, str | None]) -> CoordinationNotice:
    return CoordinationNotice.model_validate(
        {**json.loads(row[0]), "cleared_at": row[1]}
    )


class NoticeBoard:
    """A local board shared by every branch, using the store's transactions.

    Exchange only with trusted collaborators: authors and clear records are
    self-asserted, and import does not authenticate the sender. Absence from
    an export never deletes or clears a local notice.
    """

    def __init__(
        self, store: AsyncEventStore, *, clock: Callable[[], datetime] = utcnow
    ) -> None:
        self.store = store
        self.clock = clock
        self.local_authors: frozenset[str] | None = None
        self.local_peer: str | None = None
        self.trusted_peers: frozenset[str] | None = None

    def _check_local_author(self, agent_id: str) -> None:
        if self.local_authors is not None and agent_id not in self.local_authors:
            raise ValueError(
                "agent_id is not an authorized local author in the sync policy"
            )

    async def post(
        self, *, agent_id: str, resource: str, message: str, ttl_seconds: int = 3600
    ) -> CoordinationNotice:
        """Post an advisory with a logical scope such as ``service:auth``."""
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 604800:
            raise ValueError("ttl_seconds must be an integer from 1 to 604800")
        self._check_local_author(agent_id)
        now = self.clock()
        notice = CoordinationNotice(
            notice_id=str(uuid4()),
            author=agent_id,
            resource=resource,
            message=message,
            created_at=now,
            expires_at=now + timedelta(seconds=ttl_seconds),
        )
        async with self.store.transaction(write=True) as db:
            await db.execute(
                "INSERT INTO coordination_notices (notice_id, document) VALUES (?, ?)",
                (notice.notice_id, _body(notice)),
            )
            if self.local_peer:
                await db.execute(
                    "INSERT INTO notice_sources VALUES (?, ?)",
                    (notice.notice_id, self.local_peer),
                )
        return notice

    async def clear(self, notice_id: str, *, agent_id: str) -> CoordinationNotice:
        """Clear your notice. Matching the author is a courtesy, not authentication."""
        validate_agent_id(agent_id)
        self._check_local_author(agent_id)
        async with self.store.transaction(write=True) as db:
            async with db.execute(
                "SELECT document, cleared_at FROM coordination_notices WHERE notice_id = ?",
                (notice_id,),
            ) as cur:
                row = await cur.fetchone()
            if row is None:
                raise ValueError(f"notice {notice_id!r} not found")
            notice = _decode(row)
            if notice.author != agent_id:
                raise ValueError(
                    "only the notice author can clear it; identities are self-asserted"
                )
            if notice.cleared_at is None:
                # A remote clock can be ahead; clearing must still be possible.
                notice = notice.model_copy(
                    update={"cleared_at": max(self.clock(), notice.created_at)}
                )
                await db.execute(
                    "UPDATE coordination_notices SET cleared_at = ? WHERE notice_id = ?",
                    (notice.cleared_at.isoformat(), notice_id),
                )
            return notice

    async def _read(self, *, include_cleared: bool) -> list[CoordinationNotice]:
        async with self.store.transaction(write=False) as db:
            if self.store.read_only:
                async with db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'coordination_notices'"
                ) as cur:
                    if await cur.fetchone() is None:
                        return []  # A pre-notices database needs no write to remain readable.
            sql = "SELECT document, cleared_at FROM coordination_notices"
            if not include_cleared:
                sql += " WHERE cleared_at IS NULL"
            async with db.execute(sql + " ORDER BY notice_id") as cur:
                return [_decode(row) for row in await cur.fetchall()]

    async def list(
        self, *, resource: str | None = None, include_cleared: bool = False
    ) -> list[dict[str, Any]]:
        """Return active and stale notices; expiry never means work is finished."""
        now = self.clock()
        notices = [
            {
                **n.model_dump(mode="json"),
                "status": (
                    "cleared"
                    if n.cleared_at
                    else "stale"
                    if n.expires_at <= now
                    else "active"
                ),
            }
            for n in await self._read(include_cleared=include_cleared)
            if resource is None or n.resource == resource
        ]
        if notices:
            async with self.store.transaction(write=False) as db:
                async with db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='notice_sources'"
                ) as cur:
                    exists = await cur.fetchone()
                if exists:
                    async with db.execute(
                        "SELECT notice_id, peer_id FROM notice_sources"
                    ) as cur:
                        sources = dict(await cur.fetchall())
                else:
                    sources = {}
            for notice in notices:
                source = sources.get(notice["notice_id"])
                notice["source_peer"] = source
                notice["trusted_source"] = (
                    source in self.trusted_peers
                    if self.trusted_peers is not None
                    else None
                )
        return notices

    async def export_json(self, *, notice_ids: list[str] | None = None) -> str:
        """Export notices only, including clears. Select IDs to limit disclosure."""
        notices = await self._read(include_cleared=True)
        if notice_ids is not None:
            requested = set(notice_ids)
            missing = requested - {n.notice_id for n in notices}
            if missing:
                raise ValueError(f"notice IDs not found: {sorted(missing)}")
            notices = [n for n in notices if n.notice_id in requested]
        exchange = _Exchange(schema=NOTICE_SCHEMA, notices=notices)
        result = canonical_json(exchange.model_dump(mode="json", by_alias=True))
        if len(result.encode("utf-8")) > MAX_EXCHANGE_BYTES:
            raise ValueError("notice exchange exceeds 1 MiB; export fewer notice_ids")
        return result

    async def import_json(self, document: str) -> int:
        """Validate and merge a complete batch atomically. Return changed records.

        Re-imports are harmless. Conflicting immutable bodies reject the whole
        batch. A clear wins over active records, regardless of arrival order.
        """
        if self.local_authors is not None:
            raise ValueError(
                "unsigned import is disabled; use configured signed folder sync"
            )
        if not isinstance(document, str) or len(document) > MAX_EXCHANGE_BYTES:
            raise ValueError("notice exchange must be JSON text of at most 1 MiB")
        try:
            if len(document.encode("utf-8")) > MAX_EXCHANGE_BYTES:
                raise ValueError("notice exchange exceeds 1 MiB")
            parsed = json.loads(
                document,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
            )
        except (UnicodeError, RecursionError) as exc:
            raise ValueError(
                "notice exchange is not valid UTF-8 JSON or is nested too deeply"
            ) from exc
        exchange = _Exchange.model_validate(parsed)
        async with self.store.transaction(write=True) as db:
            return await self._merge(db, exchange.notices)

    async def _merge(
        self, db, notices: list[CoordinationNotice], *, peer_id: str | None = None
    ) -> int:
        """Caller owns the transaction; signed sync also commits its cursor here."""
        changed = 0
        for notice in notices:
            if peer_id:
                async with db.execute(
                    "SELECT peer_id FROM notice_sources WHERE notice_id = ?",
                    (notice.notice_id,),
                ) as cur:
                    source = await cur.fetchone()
                if source and source[0] != peer_id:
                    raise ValueError("notice has a conflicting source peer")
            body = _body(notice)
            async with db.execute(
                "SELECT document, cleared_at FROM coordination_notices WHERE notice_id = ?",
                (notice.notice_id,),
            ) as cur:
                row = await cur.fetchone()
            cleared = notice.cleared_at.isoformat() if notice.cleared_at else None
            if row is None:
                await db.execute(
                    "INSERT INTO coordination_notices (notice_id, document, cleared_at) VALUES (?, ?, ?)",
                    (notice.notice_id, body, cleared),
                )
                changed += 1
            else:
                existing = _decode(row)
                if _body(existing) != body:
                    raise ValueError(
                        f"notice {notice.notice_id!r} has a conflicting immutable body"
                    )
                if notice.cleared_at is not None and (
                    existing.cleared_at is None
                    or notice.cleared_at < existing.cleared_at
                ):
                    await db.execute(
                        "UPDATE coordination_notices SET cleared_at = ? WHERE notice_id = ?",
                        (cleared, notice.notice_id),
                    )
                    changed += 1
            if peer_id:
                await db.execute(
                    "INSERT OR IGNORE INTO notice_sources VALUES (?, ?)",
                    (notice.notice_id, peer_id),
                )
        return changed
