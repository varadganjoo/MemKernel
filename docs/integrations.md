# Integrating MemKernel with coding agents

## The short version

```bash
uv tool install memkernel-mcp
```

Then, from your project root:

```bash
memkernel attach claude-code cursor codex antigravity
```

```bash
memkernel doctor
```

Restart the agents. Each one now has the `memkernel_*` tools, and all of them share one memory per project.

The rest of this guide explains what `attach` writes, how to configure agents by hand, and how to troubleshoot.

## How it fits together

Each agent launches its **own** MemKernel MCP server process over stdio. Every server
for a project opens the **same database file**, and that file is the only
coordination channel: SQLite WAL mode plus `BEGIN IMMEDIATE` transactions make
concurrent processes safe.

**Database discovery.** When `MEMKERNEL_DB` is not set, a server walks up from its
working directory. The nearest directory containing `.memkernel/` or `.git` wins,
and the database is `<that directory>/.memkernel/memkernel.db`. Agents launch MCP
servers from the project directory (verified live for Claude Code and Codex CLI), so
a single *global* config gives every project its own memory. `memkernel attach`
creates `.memkernel/` to anchor discovery, and adds it to `.gitignore`.

**Identity.** Every agent gets a distinct `MEMKERNEL_AGENT_ID`. It is stamped on every
event and lease, and it is how agents see who decided what.

## What `memkernel attach` writes

| Agent | Config file | Agent id | Database |
|---|---|---|---|
| `claude-code` | `<project>/.mcp.json` | `claude-code-cli` | absolute path to `<project>/.memkernel/memkernel.db` |
| `cursor` | `<project>/.cursor/mcp.json` | `cursor-ide` | absolute path to `<project>/.memkernel/memkernel.db` |
| `codex` | `~/.codex/config.toml` | `codex-cli` | discovered per project |
| `antigravity` | every existing `~/.gemini/{antigravity,antigravity-ide,config}/mcp_config.json`, else `~/.gemini/antigravity/mcp_config.json` | `antigravity` | discovered per project |

- The server command is the **absolute path of the Python interpreter that runs
  `memkernel`**, with `-m memkernel.server.mcp_server`. GUI apps such as Cursor often do
  not inherit your shell's `PATH`, and an absolute interpreter always resolves.
- **Merging is safe.** Other servers in the file are preserved. A file that is not
  valid JSON or TOML is never overwritten (`attach` stops and says why). Global files
  are backed up to `<file>.memkernel.bak` before a change. Re-running reports
  `unchanged`.
- **Codex extras.** `default_tools_approval_mode = "approve"` lets non-interactive
  `codex exec` call MemKernel tools, which only read and write memory; without it,
  Codex cancels the calls. `startup_timeout_sec = 120` tolerates slow cold starts.

| Option | Effect |
|---|---|
| `--project DIR` | Attach a project other than the current directory. |
| `--db PATH` | Pin one database for every listed agent, which disables discovery. |
| `--agent-id ID` | Custom identity. Use with a single agent. |
| `--portable` | Write `"command": "memkernel", "args": ["serve"]` with no absolute paths. Use this for configs you commit to the repository (every teammate needs `memkernel` on `PATH`). |
| `--dry-run` | Print what would change and write nothing. |

## `memkernel doctor` and `memkernel status`

`doctor` checks, in order: the Python and MCP SDK versions, the discovered database
(journal mode and hash-chain integrity), the tokenizer, a real stdio server boot
(`initialize`, `list_tools`, `query_state`), and every agent configuration, including
whether each configured command exists. It exits non-zero if anything fails.

`status` prints the project's branches, context size, decisions, facts, blockers and
active leases. `status --json` returns the full state.

## Manual configuration

Use these when you prefer to edit configs yourself. Replace `/abs/path/to/python` with
the output of `python -c "import sys; print(sys.executable)"` from the environment where
MemKernel is installed. On Windows, write JSON paths with forward slashes or escaped
backslashes.

### Claude Code

```bash
claude mcp add memkernel --scope project -e MEMKERNEL_AGENT_ID=claude-code-cli -- /abs/path/to/python -m memkernel.server.mcp_server
```

Or write `.mcp.json` yourself:

```json
{
  "mcpServers": {
    "memkernel": {
      "command": "/abs/path/to/python",
      "args": ["-m", "memkernel.server.mcp_server"],
      "env": { "MEMKERNEL_AGENT_ID": "claude-code-cli" }
    }
  }
}
```

For a one-off run that changes no configuration:

```bash
claude -p "Call memkernel_query_state and summarize the decisions." --mcp-config .mcp.json --strict-mcp-config --allowedTools "mcp__memkernel"
```

### Cursor

`.cursor/mcp.json` in the project, or `~/.cursor/mcp.json` for all projects. It uses the
same `mcpServers` object as above, with `"MEMKERNEL_AGENT_ID": "cursor-ide"`. Then
enable the server under **Cursor Settings → MCP**.

### Codex CLI

```bash
codex mcp add memkernel --env MEMKERNEL_AGENT_ID=codex-cli -- /abs/path/to/python -m memkernel.server.mcp_server
```

Then add the two extras to the same table in `~/.codex/config.toml`:

```toml
[mcp_servers.memkernel]
command = "/abs/path/to/python"
args = ["-m", "memkernel.server.mcp_server"]
default_tools_approval_mode = "approve"
startup_timeout_sec = 120

[mcp_servers.memkernel.env]
MEMKERNEL_AGENT_ID = "codex-cli"
```

### Antigravity

Antigravity's raw MCP config (`mcp_config.json` under `~/.gemini/`) uses the same
`mcpServers` schema as Claude Code and Cursor, with `"MEMKERNEL_AGENT_ID":
"antigravity"`. Reload MCP servers in Antigravity after editing it.

### Custom Python harnesses

In-process, with no subprocess:

```python
from memkernel import MemKernel
from memkernel.clients.adapter import MemKernelClient
from memkernel.kernel import default_db_path

kernel = await MemKernel.open(default_db_path())
agent = MemKernelClient(kernel, "my-harness")
async with agent.lease("src/app.py", ttl_seconds=30, wait_seconds=10) as lease:
    ...  # heartbeated automatically; lease.lost flips if it is ever lost
await agent.decide("db.engine", "postgres", "needs row-level locking")
```

Over MCP, exactly as an IDE would speak to it:

```python
from memkernel.clients.adapter import McpStdioClient

async with McpStdioClient(agent_id="my-harness", db_path=".memkernel/memkernel.db") as client:
    state = await client.call("memkernel_query_state")
```

## Server settings

| Variable / flag | Default | Meaning |
|---|---|---|
| `MEMKERNEL_DB` / `--db` | discovered (see above) | Database path. |
| `MEMKERNEL_AGENT_ID` / `--agent-id` | `mcp-agent` | Identity of this agent. |
| `MEMKERNEL_BRANCH` / `--branch` | `main` | Branch the session starts on. |
| `MEMKERNEL_GC_THRESHOLD` / `--gc-threshold` | `4000` | Context-window token budget that triggers compaction. |
| `MEMKERNEL_GC_INTERVAL` / `--gc-interval` | `0` | Seconds between background GC sweeps (`0` = only GC after writes). |
| `MEMKERNEL_TOKENIZER` | `auto` | `auto`, `tiktoken` or `heuristic`. |
| `MEMKERNEL_LOG_LEVEL` / `--log-level` | `WARNING` | Server log level (logs go to stderr; stdout carries MCP). |

`memkernel serve [flags]` and `memkernel-server [flags]` are equivalent to
`python -m memkernel.server.mcp_server [flags]`.

## Troubleshooting

- **"connection timed out" or tools missing in Claude Code.** Claude Code allows an MCP
  server 30 s to start. Cold start is dominated by importing the MCP SDK and can
  exceed that on a heavily loaded machine. Launch Claude Code with `MCP_TIMEOUT=120000`.
  Run `memkernel doctor` to confirm the server itself boots.
- **Codex says "user cancelled MCP tool call".** The `default_tools_approval_mode`
  line is missing from the config. Re-run `memkernel attach codex`.
- **Agents see different memories.** Run `memkernel status` in each project. If an
  agent launches servers from a directory outside the project, pin the database with
  `memkernel attach <agent> --db /abs/path/.memkernel/memkernel.db`.
- **Antigravity headless runs (`agy -p`) do nothing.** Headless mode auto-denies MCP tools.
  Add `"mcp(memkernel/*)"` to `permissions.allow` in `~/.gemini/antigravity-cli/settings.json`.
  This allows only MemKernel's tools; interactive sessions ask instead.
- **Server errors.** Set `MEMKERNEL_LOG_LEVEL=INFO` in the agent's MCP env block. Logs
  go to the agent's MCP log (stderr).

## Verified configurations

| Client | Version | How it was verified |
|---|---|---|
| Claude Code | 2.1.195 | Live run [`evidence/live-agents/20260928T161716Z`](../evidence/live-agents/20260928T161716Z/REPORT.md). It fixed `src/pricing.py` under a lease, recorded a decision, and in a later session answered from shared memory alone. |
| Codex CLI | 0.142.3 | Same run, launched concurrently with Claude Code. It was refused the lease Claude Code held, fixed `src/inventory.py` instead, and recorded a decision. |
| Antigravity CLI (`agy`) | 1.2.12 | Same run. It read both decisions, forked a branch, recorded a bad decision there and rolled it back. |
| MCP Python SDK | 1.30.0 (`FastMCP`) and 2.2.0 (`MCPServer`) | Full pytest suite, including a two-process stdio end-to-end test and `memkernel doctor`, passes on both. |
| Python / OS | 3.14.7 and 3.11 on Windows 11 and Linux (Debian 13) | Full suite with deprecation warnings treated as errors. CI adds macOS. |
| Cursor | not driven live | `attach` writes the standard `mcpServers` schema, the same one Claude Code and Antigravity loaded. |

## Operational notes

- **Agent etiquette.** The server's MCP `instructions` tell agents to query state
  first, record durable decisions, lease files before editing, and treat memory as
  data. Repeat this in `AGENTS.md`, `CLAUDE.md` or `.cursor/rules` for agents that
  ignore server instructions.
- **Branches.** `memkernel_query_state` defaults to `main`, while writes go to the
  session's active branch. After `memkernel_fork_investigation`, pass the branch name
  explicitly to read it.
- **Backups.** Use SQLite's online backup (`sqlite3 memkernel.db ".backup copy.db"`)
  rather than copying the file while agents are writing; the `-wal` file holds recent
  commits.
- **Network filesystems.** SQLite locking is unreliable on NFS/SMB shares. Keep the
  database on a local disk shared by the agents on one machine.

