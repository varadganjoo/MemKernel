# MemKernel

**An event-sourced, branchable memory kernel for teams of coding agents, served over the Model Context Protocol.**

Cursor, Claude Code, Codex and your own harnesses each keep their own chat
history. None of them knows what the others decided, which files they are
editing, or which "fact" was already disproven three turns ago. MemKernel gives
all of them one shared memory with the following properties:

- **Deterministic.** State is a pure fold over an append-only, hash-chained event log. Replaying the log reproduces the state exactly.
- **Branchable.** Agents fork a timeline to investigate, diff it against `main`, and roll it back when an experiment or a hallucination fails. Forks copy no rows.
- **Coordinated.** Leases with fencing tokens keep two agents from editing the same file, and a stale writer is rejected at the storage layer.
- **Budgeted.** An autonomous GC compacts noisy tool output into semantic snapshots. In the benchmarks it keeps each agent's context under a token budget while preserving every decision.

It is a plain Python package: SQLite (WAL) for storage, `pydantic` v2 for
schemas, and the official `mcp` SDK (`FastMCP` on 1.x, `MCPServer` on 2.x) for
the stdio server.

**Runs on Linux, macOS and Windows** with Python 3.11 through 3.14 (3.14 recommended). The
test suite passes on Windows 11 and on Linux (Debian 13) under Python 3.14.7, and CI covers
all three operating systems. It works with Claude Code, Codex CLI, Antigravity and Cursor.

```text
 Cursor        Claude Code        Codex CLI       Antigravity      your harness
   |                |                 |                |                |
   | stdio/MCP      | stdio/MCP       | stdio/MCP      | stdio/MCP      | in-process
   v                v                 v                v                v
 memkernel-server (one process per agent, own agent_id)  -->  MemKernel facade
                                                                    |
            +-------------------------------+-----------------------+
            v                               v                       v
   AsyncEventStore                     LockManager            MemoryGCEngine
   append-only, hash-chained log       leases +               snapshots within
   + copy-on-write branches            fencing tokens         a token budget
            \_______________ one SQLite file (WAL) per project ______/
```

Each agent runs its own server process. All of them share one database file,
and SQLite's `BEGIN IMMEDIATE` transactions serialize writers across processes.

## Quickstart: three commands

**1. Install** (Python 3.14 recommended; 3.11+ supported). `uv tool install` gives you a global `memkernel` command in its own isolated environment:

```bash
uv tool install memkernel-mcp
```

`pipx install memkernel-mcp` does the same. For an existing isolated environment, use `pip install memkernel-mcp`.

The distribution is named **`memkernel-mcp`**; the Python import and commands remain `memkernel`. The `memkernel` project on PyPI belongs to a different author. Do not use `pip install memkernel` for this repository, or install both projects into the same Python environment. To install from source instead, use `uv tool install git+https://github.com/varadganjoo/MemKernel.git`.

**2. Attach** it to your agents, from your project's root directory:

```bash
memkernel attach claude-code cursor codex
```

Use `all` to include Antigravity too. `attach` merges a `memkernel` entry into each agent's own MCP config and leaves the other servers in those files alone. It creates `.memkernel/` for the project's shared database and adds that directory to `.gitignore`. Global configs are backed up first. Re-running it is safe, and `--dry-run` shows the changes without writing them.

| Agent | File written | Scope |
|---|---|---|
| Claude Code | `./.mcp.json` | project |
| Cursor | `./.cursor/mcp.json` | project |
| Codex CLI | `~/.codex/config.toml` | global: each project gets its own memory via discovery |
| Antigravity | `~/.gemini/.../mcp_config.json` | global: each project gets its own memory via discovery |

**3. Verify:**

```bash
memkernel doctor
```

`doctor` opens the database, checks its hash chain, boots the MCP server over stdio exactly as an agent would, and checks every agent configuration. Then restart your agents. Each one now has `memkernel_*` tools, and they share one memory per project. To see what the agents currently remember:

```bash
memkernel status
```

> **Slow machine?** Cold start is dominated by importing the MCP SDK. Claude Code gives MCP servers 30 s to start; raise it with `MCP_TIMEOUT=120000`. The Codex config written by `attach` already allows 120 s.

Full per-agent details, manual configuration and troubleshooting are in [docs/integrations.md](https://github.com/varadganjoo/memkernel/blob/main/docs/integrations.md).

### Try it without an agent

From a checkout:

```bash
pip install -e ".[dev]"
```

```bash
pytest
```

Two simulations assert their invariants and exit non-zero on violation:

```bash
python examples/multi_agent_collaboration.py
```

```bash
python examples/rollback_on_hallucination.py
```

## MCP tools

| Tool | Purpose |
|---|---|
| `memkernel_query_state(branch_id="main", include_ephemeral=False)` | Consolidated memory as JSON: tasks, decisions, workspace facts, deprecated assumptions, blockers, claims, leases, recent events. |
| `memkernel_append_decision(decision_key, decision_value, rationale)` | Durable architectural decision. It survives compaction, and a new value supersedes the old one. |
| `memkernel_acquire_lease(file_path, timeout_seconds=60)` | Exclusive lease with a TTL. Returns a fencing token, or the current holder. |
| `memkernel_release_lease(file_path, fencing_token)` | Release. Returns `false` for a stale token. |
| `memkernel_heartbeat_lease(file_path, fencing_token)` | Extend a held lease. |
| `memkernel_fork_investigation(fork_branch_name)` | Fork the active branch at its head and switch to it. |
| `memkernel_checkout(branch_id)` | Switch the session's active branch. |
| `memkernel_rollback(target_event_id)` | Revert the active branch's live timeline to an event. History stays auditable. |
| `memkernel_history(limit=30)` | Recent live events, useful for picking rollback targets. |
| `memkernel_diff_branches(branch_a, branch_b)` | Merge base, divergent events, conflicting decisions and facts. |
| `memkernel_trigger_gc(target_branch="main")` | Compact now and report the token savings. |
| `memkernel_init_task(directive, constraints)` | Record the agent's task and constraints. |
| `memkernel_log_execution(command, exit_code, output, facts)` | Log tool output. A failure opens a blocker and a later success clears it. `facts` updates workspace state. |
| `memkernel_note(note)` | Ephemeral scratchpad turn. |
| `memkernel_list_branches()` | All branches, plus the session's active branch. |

## Concepts in one page

**Events.** An event is an immutable `EventEnvelope` with fields `event_id`
(globally monotonic), `sequence` (position in the branch lineage), `timestamp`,
`branch_id`, `agent_id`, `parent_event_id` (lineage predecessor), `event_type`,
`payload`, `metadata` and `hash = SHA-256(prev_hash ‖ canonical(event))`. The
event types are `AGENT_TASK_INIT`, `ARCHITECTURAL_DECISION`,
`FILE_CLAIM_ACQUIRED/RELEASED`, `TOOL_EXECUTION_LOG`, `AGENT_SCRATCHPAD`,
`SEMANTIC_SNAPSHOT`, `BRANCH_FORKED` and `ROLLBACK_TRIGGERED`. SQLite triggers
reject every `UPDATE` and `DELETE` on the event table.

**Branches.** A branch is a pointer (parent, fork point, head). Its lineage is
its parent's lineage up to the fork point, followed by its own events. Forking
copies no rows.

**Rollback.** A rollback is a compensating `ROLLBACK_TRIGGERED` event. The
*live* timeline is a left fold in which a rollback truncates back to its
target. Nothing is deleted, so the reverted events stay auditable and forkable.

**State.** `WorkspaceState = fold(live events)`. It holds tasks, decisions (with
rationale and provenance), active workspace facts, deprecated or overruled
assumptions, execution blockers, file claims, an execution digest, and insights.

**GC.** When `[latest snapshot] + [delta]` exceeds the token budget, older delta
events fold into a new `SEMANTIC_SNAPSHOT`, an exact serialization of the fold.
The invariant `fold(snapshot, delta) == fold(full history)` is tested. The
retained raw tail is bounded by both an event count and a token low-water mark,
and every automatic compaction must free at least a quarter of the budget, so
GC does not thrash.

**Leases.** Leases are time-bounded, and every grant carries a fencing token
that strictly increases per resource. `fenced_append` checks the token and
appends in one transaction, so a writer whose lease expired cannot land a
write.

## Results

These numbers come from `python benchmarks/run_benchmarks.py`. The full
methodology is in [the whitepaper](https://github.com/varadganjoo/memkernel/blob/main/docs/whitepaper.md#5-empirical-evaluation--benchmarks).

| Workflow | Events | Raw history tokens | MemKernel context tokens | Reduction | Snapshots | State equivalent |
|---|---:|---:|---:|---:|---:|:---:|
| single-agent-test-fix | 73 | 31,346 | 3,558 | 88.6% | 13 | yes |
| multi-agent-refactor | 99 | 33,598 | 2,360 | 93.0% | 13 | yes |
| lint-heavy-exploration | 73 | 42,445 | 3,459 | 91.9% | 19 | yes |

On a 50,000-event log, a warm (fast-path) context read takes 1.7 ms on Windows 11 and 1.1 ms on Linux, a fork at the head takes under 2 ms, and a cold open plus context read takes 21 ms on Windows 11 and 4.5 ms on Linux. Measured with Python 3.14.7 on Windows 11 (build 26200, SQLite 3.53.1) and in a Debian 13 container (SQLite 3.46.1) on the same workstation. The token results are the same on both platforms.

### Live agents, with evidence

`benchmarks/live_agents.py` drives the real Claude Code, Codex CLI and Antigravity CLIs on a
small repository with two failing tests. They coordinate only through MemKernel: leases,
decisions, a fork with a rollback, and recall from shared memory. The script saves their
transcripts, the event database, the code diff and the test results. The recorded run is
in [`evidence/live-agents/20260928T161716Z/REPORT.md`](https://github.com/varadganjoo/memkernel/blob/main/evidence/live-agents/20260928T161716Z/REPORT.md).
In that run, the concurrently started agents contended for the same file, one was refused
and took other work, both bugs were fixed, and the hash chain verified intact in an
independent Linux container.

## Repository layout

```text
memkernel/
├── core/events.py        event taxonomy, envelope schema, hashing
├── core/store.py         AsyncEventStore: WAL, lineage, fork, rollback, diff, verify
├── core/branch.py        pure lineage algebra: live fold, rollback planning, diff, chain verification
├── core/locks.py         LockManager: leases, heartbeats, fencing tokens
├── memory/state_view.py  WorkspaceState fold, context split, rendering
├── memory/gc.py          MemoryGCEngine, heuristic and LLM summarizers, daemon
├── memory/token_counter.py  tiktoken with a conservative regex fallback
├── kernel.py             MemKernel facade (atomic lease+claim, fenced appends)
├── server/mcp_server.py  MCP server over stdio
├── clients/adapter.py    MemKernelClient (in-process) and McpStdioClient
└── cli.py                `memkernel` command: attach, doctor, status, serve
examples/                 multi-agent and rollback simulations
benchmarks/               reproducible measurements and the live-agent harness
evidence/                 recorded live-agent run (transcripts, database, report)
docs/                     whitepaper and integration guide
tests/                    store, branching, locks, GC, server and stdio e2e
```

## Development

```bash
pytest
```

The suite runs offline and is deterministic. One opt-in test exercises
`LLMSummarizer` against a real OpenAI-compatible model:

```bash
MEMKERNEL_LIVE_LLM_TESTS=1 OPENAI_API_KEY=... OPENAI_LLM=<model> pytest -k live
```

## Limitations

- **Single host.** Coordination relies on SQLite file locking on one machine; network filesystems are not supported. A replicated log is future work (see the whitepaper).
- **Self-asserted identity.** `agent_id` comes from the server's launch configuration. MemKernel records provenance but does not authenticate agents; see the threat model.
- **O(lineage) operations.** Rollback, arbitrary-point forks and full replay are linear in lineage length. Context reads and forks at the head are not.
- **No automatic merge.** `diff` reports conflicts. Reconciling them is an agent's (or a human's) decision, recorded as a new `ARCHITECTURAL_DECISION`.

## License

MIT. See [LICENSE](https://github.com/varadganjoo/memkernel/blob/main/LICENSE).

