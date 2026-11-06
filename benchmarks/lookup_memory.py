"""Compare full recall with exact-key recall on 20 known-key questions.

Run from an installed checkout: python benchmarks/lookup_memory.py
Uses disposable synthetic data and real read-only MCP over stdio. Measures the
entire serialized CallToolResult (text + structured content where the SDK sends
both), excluding JSON-RPC framing, tool schemas, and conversation history.
"""

from __future__ import annotations

import asyncio
import json
import platform
import statistics
import tempfile
import time
from importlib.metadata import version
from pathlib import Path

from memkernel import EventType, GCConfig, MemKernel
from memkernel.clients.adapter import McpStdioClient
from memkernel.core.events import new_event
from memkernel.memory.token_counter import TokenCounter


async def main() -> None:
    counter = TokenCounter()
    report = {"python": platform.python_version(), "mcp": version("mcp"),
              "tokenizer": counter.backend_name, "questions": 20, "decision_keys": 100,
              "revisions_per_key": 2, "phases": {}}
    with tempfile.TemporaryDirectory() as folder:
        path = str(Path(folder) / "memory.db")
        async with await MemKernel.open(path, tokenizer="heuristic", auto_gc=False,
                                       gc_config=GCConfig(retain_recent_events=0)) as kernel:
            async with kernel.store.transaction(write=True) as db:
                for revision in range(2):
                    for index in range(100):
                        await kernel.store.append_in_transaction(db, new_event(
                            EventType.ARCHITECTURAL_DECISION, branch_id="main", agent_id="owner",
                            payload={"decision_key": f"component.{index}.storage",
                                     "decision_value": "sqlite" if revision else "postgres",
                                     "rationale": "Use local storage for offline agents."},
                        ))
            async with McpStdioClient(agent_id="observer", db_path=path,
                                      env={"MEMKERNEL_READ_ONLY": "true", "MEMKERNEL_TOKENIZER": "heuristic"}) as client:
                for phase in ("raw", "compacted"):
                    if phase == "compacted":
                        gc = await kernel.gc(force=True)
                        assert gc.triggered
                        report["gc"] = gc.model_dump()
                    samples = {tool: [] for tool in ("memkernel_query_state", "memkernel_lookup_memory")}
                    for index in range(20):
                        key = f"component.{index}.storage"
                        # Alternate order to reduce a consistent first-reader advantage.
                        order = list(samples) if index % 2 == 0 else list(reversed(samples))
                        for tool in order:
                            started = time.perf_counter()
                            result = await client._require_session().call_tool(tool, {"key": key} if tool.endswith("lookup_memory") else {})
                            elapsed = (time.perf_counter() - started) * 1000
                            assert not getattr(result, "isError", getattr(result, "is_error", False))
                            text = "\n".join(c.text for c in result.content if c.type == "text")
                            decoded = json.loads(text)
                            record = decoded["record"] if tool.endswith("lookup_memory") else decoded["state"]["decisions"][key]
                            assert record["value"] == "sqlite" and record["event_id"] == 101 + index
                            assert record["agent_id"] == "owner" and record["supersedes_event_id"] == 1 + index
                            serialized = json.dumps(result.model_dump(mode="json", exclude_none=True), ensure_ascii=False)
                            samples[tool].append({"bytes": len(serialized.encode("utf-8")),
                                                  "tokens": counter.count(serialized),
                                                  "text_tokens": counter.count(text), "ms": elapsed})
                    report["phases"][phase] = {
                        tool: {"correct": len(rows), "mean_result_bytes": statistics.mean(r["bytes"] for r in rows),
                               "mean_result_tokens": statistics.mean(r["tokens"] for r in rows),
                               "mean_text_content_tokens": statistics.mean(r["text_tokens"] for r in rows),
                               "median_ms": round(statistics.median(r["ms"] for r in rows), 3)}
                        for tool, rows in samples.items()
                    }
            assert await kernel.verify() == []
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
