"""Client adapters for agent harnesses.

* :class:`MemKernelClient` — in-process, agent-scoped handle on a
  :class:`~memkernel.kernel.MemKernel` (lowest latency; for Python harnesses).
* :class:`McpStdioClient` — speaks MCP to a ``memkernel-server`` subprocess,
  exactly like Cursor, Claude Code or Codex do. Many such clients (one per
  process) can share a single database file.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from memkernel.core.events import EventEnvelope, EventType, new_event, validate_agent_id, validate_branch_id
from memkernel.kernel import DEFAULT_BRANCH, LeaseGrant, MemKernel
from memkernel.memory.gc import GCResult


class LeaseUnavailableError(RuntimeError):
    def __init__(self, grant: LeaseGrant) -> None:
        super().__init__(
            f"lease on {grant.resource!r} is held by {grant.holder_agent_id!r} "
            f"for another {grant.expires_in_seconds}s"
        )
        self.grant = grant


@dataclass
class LeaseHandle:
    """Yielded by :meth:`MemKernelClient.lease`. ``lost`` becomes true if a
    heartbeat fails; stop writing and re-acquire when it does."""

    grant: LeaseGrant
    _lost: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def fencing_token(self) -> int:
        assert self.grant.fencing_token is not None
        return self.grant.fencing_token

    @property
    def lost(self) -> bool:
        return self._lost.is_set()


class MemKernelClient:
    """Agent-scoped view of a kernel: fixed ``agent_id``, switchable branch."""

    def __init__(self, kernel: MemKernel, agent_id: str, branch: str = DEFAULT_BRANCH) -> None:
        self.kernel = kernel
        self.agent_id = validate_agent_id(agent_id)
        self.branch = validate_branch_id(branch)

    async def checkout(self, branch: str) -> None:
        await self.kernel.store.get_branch(branch)  # raises if missing
        self.branch = branch

    # ---------------------------------------------------------------- writes

    async def init_task(self, directive: str, constraints: list[str] | None = None) -> EventEnvelope:
        return await self.kernel.init_task(
            branch_id=self.branch, agent_id=self.agent_id, directive=directive, constraints=constraints
        )

    async def decide(self, key: str, value: str, rationale: str) -> EventEnvelope:
        return await self.kernel.append_decision(
            branch_id=self.branch, agent_id=self.agent_id, key=key, value=value, rationale=rationale
        )

    async def log_execution(
        self, command: str, exit_code: int, output: str = "", facts: dict[str, Any] | None = None
    ) -> EventEnvelope:
        return await self.kernel.log_execution(
            branch_id=self.branch,
            agent_id=self.agent_id,
            command=command,
            exit_code=exit_code,
            output=output,
            facts=facts,
        )

    async def note(self, text: str) -> EventEnvelope:
        return await self.kernel.note(branch_id=self.branch, agent_id=self.agent_id, note=text)

    async def fenced_write(
        self, event_type: EventType, payload: dict[str, Any], *, resource: str, fencing_token: int
    ) -> EventEnvelope:
        """Append only while holding the current lease on ``resource``."""
        return await self.kernel.fenced_append(
            new_event(event_type, branch_id=self.branch, agent_id=self.agent_id, payload=payload),
            resource=resource,
            fencing_token=fencing_token,
        )

    # ----------------------------------------------------------------- reads

    async def query_state(self, *, include_ephemeral: bool = False, branch: str | None = None) -> dict[str, Any]:
        return await self.kernel.query_state(branch or self.branch, include_ephemeral=include_ephemeral)

    async def context(self, branch: str | None = None) -> str:
        return await self.kernel.context(branch or self.branch)

    async def history(self, limit: int = 30) -> list[dict[str, Any]]:
        return await self.kernel.history(self.branch, limit=limit)

    # --------------------------------------------------------------- timeline

    async def fork_investigation(self, name: str, *, at_event_id: int | None = None) -> EventEnvelope:
        """Fork the current branch and switch to the fork."""
        event = await self.kernel.fork(
            source_branch=self.branch,
            new_branch=name,
            agent_id=self.agent_id,
            at_event_id=at_event_id,
            reason="investigation",
        )
        self.branch = name
        return event

    async def rollback(self, target_event_id: int, reason: str = "") -> EventEnvelope:
        return await self.kernel.rollback(
            branch_id=self.branch, target_event_id=target_event_id, agent_id=self.agent_id, reason=reason
        )

    async def diff(self, other_branch: str) -> dict[str, Any]:
        return await self.kernel.diff(self.branch, other_branch)

    async def gc(self, *, force: bool = True) -> GCResult:
        return await self.kernel.gc(self.branch, force=force)

    # ----------------------------------------------------------------- leases

    async def acquire(self, resource: str, ttl_seconds: float = 30) -> LeaseGrant:
        return await self.kernel.acquire_lease(
            branch_id=self.branch, agent_id=self.agent_id, resource=resource, ttl_seconds=ttl_seconds
        )

    async def release(self, resource: str, fencing_token: int) -> bool:
        return await self.kernel.release_lease(
            branch_id=self.branch, agent_id=self.agent_id, resource=resource, fencing_token=fencing_token
        )

    @asynccontextmanager
    async def lease(
        self,
        resource: str,
        *,
        ttl_seconds: float = 30,
        wait_seconds: float = 0,
        poll_interval: float = 0.25,
        heartbeat: bool = True,
    ) -> AsyncIterator[LeaseHandle]:
        """Hold a lease for the duration of the block, heartbeating every
        ``ttl_seconds / 3`` and releasing on exit. Waits up to
        ``wait_seconds`` for a contended lease, then raises
        :class:`LeaseUnavailableError`."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, wait_seconds)
        while True:
            grant = await self.acquire(resource, ttl_seconds)
            if grant.acquired:
                break
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise LeaseUnavailableError(grant)
            await asyncio.sleep(max(0.01, min(poll_interval, remaining)))

        handle = LeaseHandle(grant)

        async def beat() -> None:
            while True:
                await asyncio.sleep(ttl_seconds / 3)
                if not await self.kernel.heartbeat_lease(resource=grant.resource, fencing_token=handle.fencing_token):
                    handle._lost.set()
                    return

        task = asyncio.create_task(beat()) if heartbeat else None
        try:
            yield handle
        finally:
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await self.release(grant.resource, handle.fencing_token)


# ------------------------------------------------------------ MCP over stdio


class McpToolError(RuntimeError):
    def __init__(self, tool: str, message: str) -> None:
        super().__init__(f"{tool}: {message}")
        self.tool = tool


class McpStdioClient:
    """MCP client that spawns ``python -m memkernel.server.mcp_server``.

    Usage::

        async with McpStdioClient(agent_id="cursor-ide", db_path="mem.db") as c:
            await c.call("memkernel_append_decision", decision_key="db",
                         decision_value="postgres", rationale="...")
    """

    def __init__(
        self,
        *,
        agent_id: str,
        db_path: str,
        branch: str = DEFAULT_BRANCH,
        gc_threshold: int | None = None,
        command: str | None = None,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
    ) -> None:
        self.agent_id = validate_agent_id(agent_id)
        server_env = {
            "MEMKERNEL_DB": os.path.abspath(db_path),
            "MEMKERNEL_AGENT_ID": self.agent_id,
            "MEMKERNEL_BRANCH": validate_branch_id(branch),
        }
        if gc_threshold is not None:
            server_env["MEMKERNEL_GC_THRESHOLD"] = str(int(gc_threshold))
        for passthrough in ("PYTHONPATH", "MEMKERNEL_TOKENIZER", "MEMKERNEL_LOG_LEVEL", "TIKTOKEN_CACHE_DIR"):
            if passthrough in os.environ:
                server_env[passthrough] = os.environ[passthrough]
        server_env.update(env or {})
        self._command = command or sys.executable
        self._args = args if args is not None else ["-m", "memkernel.server.mcp_server"]
        self._env = server_env
        self._cwd = cwd
        self._stack: AsyncExitStack | None = None
        self._session: Any = None

    async def __aenter__(self) -> "McpStdioClient":
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        params = StdioServerParameters(command=self._command, args=self._args, env=self._env, cwd=self._cwd)
        stack = AsyncExitStack()
        try:
            # Server logs go to our stderr, unless it is not a real file
            # (pytest capture, Jupyter, some IDE consoles): then discard them.
            errlog: Any = sys.stderr
            try:
                errlog.fileno()
            except (AttributeError, OSError, ValueError):
                errlog = stack.enter_context(open(os.devnull, "w", encoding="utf-8"))
            read, write = await stack.enter_async_context(stdio_client(params, errlog=errlog))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        except BaseException:
            await stack.aclose()
            raise
        self._stack, self._session = stack, session
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._stack is not None:
            stack, self._stack, self._session = self._stack, None, None
            await stack.aclose()

    async def list_tools(self) -> list[str]:
        result = await self._require_session().list_tools()
        return sorted(tool.name for tool in result.tools)

    async def call(self, tool: str, **arguments: Any) -> Any:
        """Call a tool; returns decoded JSON when the result is JSON, else text.
        Raises :class:`McpToolError` when the server reports a tool error."""
        result = await self._require_session().call_tool(tool, arguments)
        text = "\n".join(c.text for c in result.content if getattr(c, "type", None) == "text")
        is_error = getattr(result, "is_error", None)
        if is_error is None:  # mcp 1.x field name
            is_error = getattr(result, "isError", False)
        if is_error:
            raise McpToolError(tool, text)
        try:
            return json.loads(text)
        except ValueError:
            return text

    def _require_session(self) -> Any:
        if self._session is None:
            raise RuntimeError("McpStdioClient is not connected; use 'async with'")
        return self._session
