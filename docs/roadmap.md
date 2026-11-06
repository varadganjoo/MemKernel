# MemKernel capability roadmap

These are proposals from the November 2026 code review and actual MCP use, not
features delivered in 0.3.1. Keep the local event log and Python API small. Validate
each step with real tasks before expanding its scope.

| Order | Improvement | Smallest useful version | Acceptance measure |
|---|---|---|---|
| 1 | Task-relevant recall | One compact recall tool accepting a task, active branch and output budget; rank existing decisions, blockers and facts with SQLite full-text search first. Return sources, omissions and freshness. | Compare response tokens and correct retrieval against `query_state` on a versioned task set; measure the actual serialized response. No silent loss of relevant blockers. |
| 2 | Easy desktop installation | A portable desktop plugin plus a Claude Desktop extension; require an explicit workspace/database selector and a read-only option. Reuse the existing server. | Fresh-machine install, first recall, restart recall and uninstall on Windows/macOS/Linux; no config loss or accidental sharing between projects. |
| 3 | Explain and correct memory | Expose why a fact is present, its originating events, whether it was tested, and which newer decision superseded it. Add explicit correction events. | A user can inspect and correct a bad memory without hand-editing SQLite; replay and compaction preserve the correction. |
| 4 | Reliable backup and recovery | A CLI using SQLite's online backup API, integrity checks, restore into a new file and a rehearsed rollback guide. | Restore a busy database with active readers and verify every branch, notice, policy binding and hash chain. Never copy only the main WAL database file. |
| 5 | Memory lifecycle and privacy | Separate task memory, durable project decisions and personal preferences. Add expiry/review status first; design explicit export and irreversible erasure separately. | No cross-project leakage; expired information is clearly marked. Erasure tests cover raw history, snapshots, notices, backups and sync tombstones. Rollback must never be labelled deletion. |
| 6 | Measured agent outcomes | A small checked-in benchmark of multi-session tasks, distractors, wrong memories, replay and interruptions. Include on-device hardware profiles. | Track useful-recall rate, irrelevant-memory rate, stale-fact rate, task success, tokens, startup latency, CPU, RAM and DB growth. Publish baseline and candidate results, including regressions. |
| 7 | Safer remote access when demanded | A separately designed authenticated gateway with per-workspace permissions and read-only scopes; keep the core local. | Tenant/owner isolation, negative authorization tests, bounded outputs, origin protection and deletion semantics before enabling remote writes. |

## Claude, ChatGPT, Codex and local models

Claude Code, Cursor, Codex and other stdio clients can use the existing server.
Claude Desktop has a documented local extension installation path; an `.mcpb`
package could remove manual setup. See [Anthropic's local-server guide](https://support.claude.com/en/articles/10949351-getting-started-with-local-mcp-servers-on-claude-desktop).

OpenAI documents local desktop plugin packaging and a separate custom MCP
connection route. Treat desktop-local, browser/hosted ChatGPT, and API access as
different test targets. A working Codex connection is not evidence for all three.
See [plugin packaging](https://developers.openai.com/plugins/build/plugins) and
[custom MCP connections](https://developers.openai.com/api/docs/guides/custom-mcp-server).

For local model harnesses, start with the in-process `MemKernelClient` and current
stdio server. Keep embeddings optional if full-text retrieval proves insufficient.
Memory retrieval should work offline without a hosted embedding service.

## What to try first

The November 6 weekly development patch adds exact-key lookup as a first,
independently useful part of selective recall. A synthetic experiment found that
compaction alone still returned every durable decision for a one-key question.
See the [discovery and measurements](reviews/weekly-2026-11-06.md). This is an
unreleased development change; task ranking, unknown-key discovery, budgets,
freshness evaluation and relevant-blocker selection remain open.

Build the compact recall prototype and run the same 20 representative tasks with
and without it. Require correct source retrieval and smaller responses, then use
the results to decide whether embeddings or a more elaborate ranking layer are
necessary. Ship desktop packaging only after workspace selection is explicit.

Defer a hosted memory service, distributed consensus, automatic full-history sync,
a vector database dependency and a broad dashboard. They introduce operational or
privacy costs before the current local workflow demonstrates their need. The
largest risk is making bad or stale memory easier to retrieve with false confidence.
