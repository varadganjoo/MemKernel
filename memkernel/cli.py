"""``memkernel`` command line: attach agents, check health, inspect memory.

    memkernel attach claude-code        # or cursor | codex | antigravity | all
    memkernel doctor                    # end-to-end health check
    memkernel status                    # what the agents currently remember
    memkernel serve                     # run the MCP server (what agents launch)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import tomllib
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any, Callable

from memkernel.kernel import DEFAULT_BRANCH, MemKernel, default_db_path

SERVER_NAME = "memkernel"
SERVER_MODULE = "memkernel.server.mcp_server"
CODEX_STARTUP_TIMEOUT_SEC = 120


# ------------------------------------------------------------------ attach


@dataclass(frozen=True)
class Client:
    name: str
    agent_id: str
    project_scoped: bool
    config_paths: Callable[[Path, Path], list[Path]]  # (project, home) -> files to write
    fmt: str  # "json" | "toml"
    hint: str


def _antigravity_paths(project: Path, home: Path) -> list[Path]:
    # Antigravity builds have used several locations; update every one that exists.
    candidates = [
        home / ".gemini" / "antigravity" / "mcp_config.json",
        home / ".gemini" / "antigravity-ide" / "mcp_config.json",
        home / ".gemini" / "config" / "mcp_config.json",
    ]
    existing = [p for p in candidates if p.exists()]
    return existing or candidates[:1]


CLIENTS: dict[str, Client] = {
    "claude-code": Client(
        "claude-code", "claude-code-cli", True, lambda p, h: [p / ".mcp.json"], "json",
        "Restart Claude Code in this project and approve the 'memkernel' server when prompted (/mcp lists it). "
        "On a slow machine, raise its 30 s server start limit: set MCP_TIMEOUT=120000 before launching claude.",
    ),
    "cursor": Client(
        "cursor", "cursor-ide", True, lambda p, h: [p / ".cursor" / "mcp.json"], "json",
        "Open Cursor Settings -> MCP and enable 'memkernel'.",
    ),
    "codex": Client(
        "codex", "codex-cli", False, lambda p, h: [h / ".codex" / "config.toml"], "toml",
        "Start a new Codex session; `codex mcp list` shows 'memkernel'.",
    ),
    "antigravity": Client(
        "antigravity", "antigravity", False, _antigravity_paths, "json",
        "Reload MCP servers in Antigravity (Manage MCP Servers -> Refresh).",
    ),
}


class AttachError(RuntimeError):
    pass


def server_launch(*, portable: bool) -> tuple[str, list[str]]:
    """Command that starts the server. Absolute interpreter by default: GUI
    apps often do not inherit the shell PATH. ``portable`` uses the
    ``memkernel`` entry point on PATH (for configs committed to a repo)."""
    if portable:
        return "memkernel", ["serve"]
    return sys.executable, ["-m", SERVER_MODULE]


def build_entry(client: Client, *, project: Path, db: Path | None, portable: bool, agent_id: str | None) -> dict[str, Any]:
    command, args = server_launch(portable=portable)
    env = {"MEMKERNEL_AGENT_ID": agent_id or client.agent_id}
    if db is not None:
        env["MEMKERNEL_DB"] = str(db)
    elif client.project_scoped and not portable:
        env["MEMKERNEL_DB"] = str(project / ".memkernel" / "memkernel.db")
    return {"command": command, "args": args, "env": env}


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".memkernel-tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def merge_json_config(path: Path, entry: dict[str, Any]) -> tuple[str, str]:
    """Return ``(action, new_text)``; other servers in the file are preserved."""
    data: dict[str, Any] = {}
    if path.exists():
        raw = path.read_text(encoding="utf-8")
        if raw.strip():
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise AttachError(f"{path} is not valid JSON ({exc}); fix or remove it, then re-run") from exc
        if not isinstance(data, dict) or not isinstance(data.get("mcpServers", {}), dict):
            raise AttachError(f"{path}: expected an object with an 'mcpServers' object")
    servers = data.setdefault("mcpServers", {})
    action = "unchanged" if servers.get(SERVER_NAME) == entry else ("updated" if SERVER_NAME in servers else "added")
    servers[SERVER_NAME] = entry
    return action, json.dumps(data, indent=2) + "\n"


_OUR_TABLE = re.compile(r'^\s*\[\s*mcp_servers\s*\.\s*(?:memkernel|"memkernel")\s*(?:\.[^\]]*)?\]\s*(?:#.*)?$')
_ANY_TABLE = re.compile(r"^\s*\[")


def _toml_str(value: str) -> str:
    return json.dumps(value)  # JSON string escapes are valid TOML basic strings


def merge_toml_config(path: Path, entry: dict[str, Any]) -> tuple[str, str]:
    """Replace (or append) the ``[mcp_servers.memkernel]`` tables in a Codex
    ``config.toml``. The result is re-parsed and verified before returning."""
    original = path.read_text(encoding="utf-8") if path.exists() else ""
    try:
        existing = tomllib.loads(original).get("mcp_servers", {}).get(SERVER_NAME)
    except tomllib.TOMLDecodeError as exc:
        raise AttachError(f"{path} is not valid TOML ({exc}); fix it, then re-run") from exc
    expected = dict(entry, default_tools_approval_mode="approve", startup_timeout_sec=CODEX_STARTUP_TIMEOUT_SEC)
    if existing == expected:
        return "unchanged", original

    kept, skipping = [], False
    for line in original.splitlines():
        if _ANY_TABLE.match(line):
            skipping = bool(_OUR_TABLE.match(line))
        if not skipping:
            kept.append(line)
    block = [
        f"[mcp_servers.{SERVER_NAME}]",
        f"command = {_toml_str(entry['command'])}",
        "args = [" + ", ".join(_toml_str(a) for a in entry["args"]) + "]",
        "# Lets non-interactive `codex exec` call MemKernel tools (memory writes only).",
        'default_tools_approval_mode = "approve"',
        "# The MCP SDK import dominates cold start; allow for slow or heavily loaded machines.",
        f"startup_timeout_sec = {CODEX_STARTUP_TIMEOUT_SEC}",
        "",
        f"[mcp_servers.{SERVER_NAME}.env]",
        *(f"{k} = {_toml_str(v)}" for k, v in entry["env"].items()),
    ]
    head = "\n".join(kept).rstrip()
    text = (head + "\n\n" if head else "") + "\n".join(block) + "\n"
    try:
        parsed = tomllib.loads(text).get("mcp_servers", {}).get(SERVER_NAME)
    except tomllib.TOMLDecodeError as exc:
        parsed, detail = None, str(exc)
    else:
        detail = "entry did not round-trip"
    if parsed != expected:
        raise AttachError(
            f"could not safely update {path} ({detail}); 'memkernel' may be defined with dotted keys. "
            "Remove it from the file and re-run, or use `codex mcp add` (see docs/integrations.md)."
        )
    return ("updated" if existing is not None else "added"), text


def _ensure_gitignored(project: Path, dry_run: bool) -> str | None:
    if not (project / ".git").exists():
        return None
    gitignore = project / ".gitignore"
    lines = gitignore.read_text(encoding="utf-8").splitlines() if gitignore.exists() else []
    if any(line.strip().rstrip("/") in {".memkernel", "/.memkernel"} for line in lines):
        return None
    if not dry_run:
        prefix = "" if not lines or lines[-1] == "" else "\n"
        with gitignore.open("a", encoding="utf-8") as fh:
            fh.write(f"{prefix}# MemKernel shared agent memory (local database)\n.memkernel/\n")
    return str(gitignore)


def attach(
    clients: list[str],
    *,
    project: Path,
    home: Path,
    db: Path | None = None,
    portable: bool = False,
    agent_id: str | None = None,
    dry_run: bool = False,
) -> list[dict[str, str]]:
    """Register the MemKernel server with each client. Returns a report."""
    project = project.resolve()
    report: list[dict[str, str]] = []
    if not dry_run:
        (project / ".memkernel").mkdir(parents=True, exist_ok=True)  # anchors DB discovery
    ignored = _ensure_gitignored(project, dry_run)
    if ignored:
        note = "added .memkernel/ to .gitignore" + (" (dry run)" if dry_run else "")
        report.append({"client": "git", "path": ignored, "action": note})
    for name in clients:
        client = CLIENTS[name]
        entry = build_entry(client, project=project, db=db, portable=portable, agent_id=agent_id)
        for path in client.config_paths(project, home):
            merge = merge_toml_config if client.fmt == "toml" else merge_json_config
            action, text = merge(path, entry)
            if action != "unchanged" and not dry_run:
                if path.exists() and not client.project_scoped:
                    backup = path.with_name(path.name + ".memkernel.bak")
                    backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
                _atomic_write(path, text)
            report.append({"client": name, "path": str(path), "action": action + (" (dry run)" if dry_run else ""),
                           "hint": client.hint})
    return report


# ------------------------------------------------------------------ doctor


async def _doctor(project: Path, home: Path) -> bool:
    ok = True

    def line(status: str, label: str, detail: str = "") -> None:
        print(f"  [{status}] {label}" + (f": {detail}" if detail else ""))

    try:
        from mcp.server.fastmcp import FastMCP  # noqa: F401
        flavor = "FastMCP (mcp 1.x)"
    except ModuleNotFoundError:
        flavor = "MCPServer (mcp 2.x)"
    print("MemKernel doctor")
    line("ok", "python", sys.version.split()[0] + f" ({sys.executable})")
    line("ok", "memkernel", _version())
    line("ok", "mcp sdk", f"{metadata.version('mcp')} via {flavor}")

    db = default_db_path(project)
    try:
        kernel = await MemKernel.open(db, auto_gc=False)
        try:
            branches = await kernel.store.list_branches()
            problems = await kernel.verify(DEFAULT_BRANCH)
            line("ok", "database", f"{db} (journal={kernel.store.journal_mode}, {len(branches)} branch(es))")
            line("ok" if not problems else "FAIL", "hash chain (main)", "intact" if not problems else "; ".join(problems[:3]))
            line("ok", "tokenizer", kernel.gc_engine.counter.backend_name)
            ok &= not problems
        finally:
            await kernel.close()
    except Exception as exc:
        line("FAIL", "database", f"{db}: {exc}")
        return False

    from memkernel.clients.adapter import McpStdioClient

    try:
        async with McpStdioClient(agent_id="memkernel-doctor", db_path=str(db), cwd=str(project)) as client:
            tools = await asyncio.wait_for(client.list_tools(), 60)
            state = await client.call("memkernel_query_state")
        line("ok", "mcp server (stdio)", f"{len(tools)} tools; query_state head={state['head_event_id']}")
    except Exception as exc:
        line("FAIL", "mcp server (stdio)", repr(exc))
        ok = False

    print("Agent configurations:")
    for name, client in CLIENTS.items():
        for path in client.config_paths(project, home):
            entry = _read_entry(path, client.fmt)
            if entry is None:
                line("--", name, f"not attached ({path}); run `memkernel attach {name}`")
                continue
            command = entry.get("command", "")
            resolvable = Path(command).exists() or bool(_which(command))
            line("ok" if resolvable else "FAIL", name, f"{path} -> {command}" + ("" if resolvable else " (command not found)"))
            ok &= resolvable
    return ok


def _which(command: str) -> str | None:
    import shutil

    return shutil.which(command)


def _read_entry(path: Path, fmt: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8")
        data = tomllib.loads(text).get("mcp_servers", {}) if fmt == "toml" else json.loads(text).get("mcpServers", {})
    except Exception:
        return None
    entry = data.get(SERVER_NAME)
    return entry if isinstance(entry, dict) else None


def _version() -> str:
    try:
        return metadata.version("memkernel-mcp")
    except metadata.PackageNotFoundError:
        from memkernel import __version__

        return __version__


# ------------------------------------------------------------------ status


async def _status(project: Path, branch: str, as_json: bool) -> None:
    db = default_db_path(project)
    if not db.exists():
        print(f"No MemKernel database at {db}. Attach an agent (`memkernel attach ...`) and use it first.")
        return
    kernel = await MemKernel.open(db, auto_gc=False)
    try:
        state = await kernel.query_state(branch)
        branches = await kernel.store.list_branches()
    finally:
        await kernel.close()
    if as_json:
        print(json.dumps({"db": str(db), "branches": [b.to_dict() for b in branches], "state": state}, indent=2))
        return
    s = state["state"]
    print(f"database  {db}")
    print("branches  " + ", ".join(f"{b.branch_id}(head #{b.head_event_id})" for b in branches))
    print(f"branch    {branch}: head #{state['head_event_id']}, context {state['context_tokens']} tokens, "
          f"snapshot #{state['snapshot_event_id']}")
    for title, items in (
        ("tasks", [f"{a}: {t['directive']}" for a, t in s["tasks"].items()]),
        ("decisions", [f"{k} = {d['value']}  ({d['agent_id']})" for k, d in s["decisions"].items()]),
        ("facts", [f"{k} = {f['value']}" for k, f in s["active_workspace_facts"].items()]),
        ("blockers", [f"{k}: {b['description']}" for k, b in s["execution_blockers"].items()]),
        ("leases", [f"{l['resource_key']} held by {l['holder_agent_id']} ({l['expires_in_seconds']}s left)"
                    for l in state["active_leases"]]),
    ):
        print(f"{title:<9} " + ("\n          ".join(items) if items else "-"))


# -------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="memkernel", description="Shared, branchable memory for coding agents.")
    parser.add_argument("--version", action="version", version=f"memkernel {_version()}")
    sub = parser.add_subparsers(dest="command", required=True)

    p_attach = sub.add_parser("attach", help="register MemKernel with an agent's MCP configuration")
    p_attach.add_argument("clients", nargs="+", choices=[*CLIENTS, "all"])
    p_attach.add_argument("--project", type=Path, default=Path.cwd(), help="project root (default: current directory)")
    p_attach.add_argument("--db", type=Path, help="pin a database path instead of per-project discovery")
    p_attach.add_argument("--agent-id", help="override the agent identity written to the config")
    p_attach.add_argument("--portable", action="store_true",
                          help="launch via `memkernel serve` from PATH with no absolute paths (for committed configs)")
    p_attach.add_argument("--dry-run", action="store_true", help="show what would change without writing")

    p_doctor = sub.add_parser("doctor", help="check installation, database, server and agent configs")
    p_doctor.add_argument("--project", type=Path, default=Path.cwd())

    p_status = sub.add_parser("status", help="show the memory agents share in this project")
    p_status.add_argument("--project", type=Path, default=Path.cwd())
    p_status.add_argument("--branch", default=DEFAULT_BRANCH)
    p_status.add_argument("--json", action="store_true")

    sub.add_parser("serve", help="run the MCP server over stdio (flags: see `memkernel serve --help`)", add_help=False)

    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["serve"]:
        from memkernel.server.mcp_server import main as serve

        serve(argv[1:])
        return 0
    args = parser.parse_args(argv)
    home = Path.home()

    if args.command == "attach":
        names = list(CLIENTS) if "all" in args.clients else list(dict.fromkeys(args.clients))
        if args.agent_id and len(names) > 1:
            parser.error("--agent-id applies to a single client")
        try:
            report = attach(names, project=args.project, home=home, db=args.db, portable=args.portable,
                            agent_id=args.agent_id, dry_run=args.dry_run)
        except AttachError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        for item in report:
            print(f"{item['client']:<12} {item['action']:<10} {item['path']}")
        print("\nNext steps:")
        for hint in dict.fromkeys(item["hint"] for item in report if "hint" in item):
            print(f"  - {hint}")
        print("  - Run `memkernel doctor` to verify everything end to end.")
        return 0
    if args.command == "doctor":
        return 0 if asyncio.run(_doctor(args.project.resolve(), home)) else 1
    if args.command == "status":
        asyncio.run(_status(args.project.resolve(), args.branch, args.json))
        return 0
    parser.error(f"unknown command {args.command!r}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
