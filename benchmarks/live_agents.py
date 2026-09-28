"""Live multi-agent evaluation with real, unmodified agent CLIs.

    python benchmarks/live_agents.py --out evidence/live-agents [--antigravity] [--docker-verify]

Scenario (a small repository with two genuinely failing tests):

1. Claude Code and Codex CLI start **concurrently**. Each is told to fix the
   failing tests, starting with ``src/pricing.py``, and to lease files before
   editing. Only one of them can hold that lease, so the other has to pick
   other work. They coordinate only through MemKernel.
2. Antigravity (optional, ``--antigravity``) reviews the shared memory, runs the
   tests, then forks an investigation branch, records a bad decision there,
   and rolls it back.
3. A fresh Claude Code session answers questions about the work from shared
   memory alone.

Everything is saved under ``--out/<run-id>/``: agent transcripts (tool calls
included), CLI versions, the MemKernel database and an event export, hash-chain
verification (host, plus an independent Linux container with
``--docker-verify``), the code diff the agents produced, and test results before
and after. ``REPORT.md`` is generated from those files, never written by hand.

Requirements: ``claude``, ``codex`` and (optionally) ``agy`` on PATH and logged
in. Set ``MEMKERNEL_EVAL_CODEX_MODEL`` to pass ``-m`` to Codex when your
configured default model is not available to your account. Agents run with their own accounts, and this script never reads, copies or
prints credentials. ``--antigravity`` temporarily registers MemKernel in
Antigravity's global MCP config (there is no per-run flag for it). The original
files are restored byte-for-byte afterwards.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import glob
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from memkernel import cli  # noqa: E402
from memkernel.core.events import EventType  # noqa: E402
from memkernel.core.store import AsyncEventStore  # noqa: E402
from memkernel.core.branch import fold_live  # noqa: E402
from memkernel.memory.state_view import materialize_live  # noqa: E402

GC_THRESHOLD = 1500  # low enough that real agent sessions exercise compaction

# --------------------------------------------------------------- sample repo

SAMPLE_FILES = {
    "src/__init__.py": "",
    "src/pricing.py": '''\
        from decimal import Decimal


        def line_total(unit_price: str, qty: int) -> Decimal:
            """Price of ``qty`` units, rounded to whole cents (half up, as on invoices)."""
            return round(Decimal(unit_price) * qty, 2)
        ''',
    "src/inventory.py": '''\
        def reserve(stock: dict[str, int], sku: str, qty: int) -> dict[str, int]:
            """Reserve ``qty`` units of ``sku``; raise ValueError if stock is insufficient."""
            updated = dict(stock)
            updated[sku] = updated.get(sku, 0) - qty
            return updated
        ''',
    "tests/__init__.py": "",
    "tests/test_pricing.py": '''\
        import unittest
        from decimal import Decimal

        from src.pricing import line_total


        class PricingTest(unittest.TestCase):
            def test_rounds_half_up_to_cents(self):
                self.assertEqual(line_total("0.125", 1), Decimal("0.13"))

            def test_multiplies_quantity(self):
                self.assertEqual(line_total("1.10", 3), Decimal("3.30"))
        ''',
    "tests/test_inventory.py": '''\
        import unittest

        from src.inventory import reserve


        class InventoryTest(unittest.TestCase):
            def test_reserves_available_stock(self):
                self.assertEqual(reserve({"sku-1": 5}, "sku-1", 2), {"sku-1": 3})

            def test_refuses_to_oversell(self):
                with self.assertRaises(ValueError):
                    reserve({"sku-1": 1}, "sku-1", 2)
        ''',
}

TEST_CMD = [sys.executable, "-m", "unittest", "-v"]
# Per-command exception for drives without ownership metadata (FAT/exFAT); never edits global git config.
GIT = ["git", "-c", "safe.directory=*"]

RULES = """\
You are one of several AI coding agents working on this repository at the same time.
You share memory with the other agents through the `memkernel` MCP tools. Rules:
1. Start by calling memkernel_query_state.
2. Before editing any file, call memkernel_acquire_lease on it (timeout_seconds 600). If the
   result has acquired=false, another agent owns that file: do NOT edit it; choose other work.
   Hold a lease until your change passes the tests, then release it with memkernel_release_lease.
3. After every test run, call memkernel_log_execution with the command, the exit code and the
   last lines of output.
4. Record each design choice with memkernel_append_decision.
Run the tests with: python -m unittest -v
"""

PROMPTS = {
    "claude-fix": RULES + """
Your task: make the failing tests pass. Begin with src/pricing.py (tests/test_pricing.py).
Call memkernel_init_task first with a one-line directive. Money must be rounded to cents with
ROUND_HALF_UP; record that as decision_key "money.rounding". If the lease on src/pricing.py is
denied, fix src/inventory.py instead and record your policy as decision_key
"inventory.oversell_policy". Finish with one line saying which file you fixed.
""",
    "codex-fix": RULES + """
Your task: make the failing tests pass. Begin with src/pricing.py. Call memkernel_init_task
first with a one-line directive. If the lease on src/pricing.py is denied, fix src/inventory.py
instead and record your policy as decision_key "inventory.oversell_policy" (if you fix pricing,
record "money.rounding"). Finish with one line saying which file you fixed.
""",
    "antigravity-review": """\
You share memory with other AI coding agents through the `memkernel` MCP tools. Use them.
1. Call memkernel_query_state and list every decision with the agent_id that recorded it.
2. Record a workspace fact: call memkernel_log_execution with command "review", exit_code 0,
   output "reviewed shared memory", and facts {"reviewed_by": "antigravity"}.
3. Experiment safely: call memkernel_fork_investigation with fork_branch_name
   "experiment-float-money"; then memkernel_append_decision with decision_key "money.rounding",
   decision_value "binary float", rationale "experiment"; then memkernel_history; then
   memkernel_rollback with target_event_id set to the event_id of the BRANCH_FORKED event.
4. Call memkernel_query_state with branch_id "experiment-float-money" and report the value of
   money.rounding. Do not edit files or run shell commands; use only the memkernel tools.
""",
    "claude-recall": """\
New session. Do NOT read or edit any files. Using only the memkernel MCP tools
(memkernel_query_state, memkernel_list_branches, memkernel_history), answer:
(a) which agent recorded which decisions, (b) whether any execution blockers remain,
(c) which branches exist and what the last test run's exit code was.
Answer in at most six lines.
""",
}

# ------------------------------------------------------------------ helpers


def write_sample_repo(ws: Path) -> None:
    for rel, content in SAMPLE_FILES.items():
        path = ws / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(content), encoding="utf-8")
    subprocess.run([*GIT, "init", "-q"], cwd=ws, check=True)
    subprocess.run([*GIT, "add", "-A"], cwd=ws, check=True)
    subprocess.run(
        [*GIT, "-c", "user.name=memkernel-eval", "-c", "user.email=eval@memkernel.invalid",
         "commit", "-q", "-m", "sample project with two failing tests"],
        cwd=ws, check=True,
    )


def run_tests(ws: Path) -> tuple[int, str]:
    proc = subprocess.run(TEST_CMD, cwd=ws, capture_output=True, text=True)
    return proc.returncode, proc.stdout + proc.stderr


def version(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=60).stdout.strip().splitlines()[0]
    except Exception as exc:  # noqa: BLE001 - recorded in the manifest
        return f"unavailable ({exc})"


def codex_command() -> tuple[list[str], dict[str, str]]:
    """On Windows the npm shim is a .cmd file that mangles quoted -c values, so
    invoke the bundled native binary with the environment the shim sets."""
    if os.name != "nt":
        return ["codex"], {}
    npm = shutil.which("npm")
    if npm is None:
        return ["codex"], {}
    root = subprocess.run([npm, "root", "-g"], capture_output=True, text=True).stdout.strip()
    vendor = glob.glob(os.path.join(root, "@openai", "codex", "node_modules", "@openai", "codex-win32-*", "vendor", "*"))
    if not vendor:
        return ["codex"], {}
    extra_path = os.path.join(vendor[0], "codex-path")
    env = {"CODEX_MANAGED_BY_NPM": "1", "PATH": extra_path + os.pathsep + os.environ.get("PATH", "")}
    return [os.path.join(vendor[0], "bin", "codex.exe")], env


_HOME_VARIANTS: list[str] = []


def sanitize(text: str) -> str:
    """Strip machine-identifying paths and anything shaped like a credential."""
    for variant in _HOME_VARIANTS:
        text = text.replace(variant, "~")
    # Any escaping depth: C:\Users\me, C:\\\\Users\\\\me, /c/Users/me ...
    user = re.escape(Path.home().name)
    text = re.sub(rf"(?:[A-Za-z]:|/[A-Za-z](?=[\\/]))?[\\/]+Users[\\/]+{user}(?![\w.-])", "~", text,
                  flags=re.IGNORECASE)
    text = re.sub(r"[\w.+-]+@[\w-]+\.[\w.]+", "<email>", text)
    text = re.sub(r"(sk-[A-Za-z0-9_-]{8,}|Bearer\s+[A-Za-z0-9._-]{8,})", "<redacted>", text)
    return text


def run_agent(name: str, cmd: list[str], *, cwd: Path, env: dict[str, str], out: Path,
              timeout: int) -> subprocess.Popen[bytes]:
    stdout = (out / f"{name}.log").open("wb")
    proc = subprocess.Popen(cmd, cwd=cwd, env={**os.environ, **env}, stdin=subprocess.DEVNULL,
                            stdout=stdout, stderr=subprocess.STDOUT)
    proc._memkernel_meta = {"name": name, "started": time.time(), "timeout": timeout, "file": stdout}  # type: ignore[attr-defined]
    return proc


def wait_agent(proc: subprocess.Popen[bytes]) -> dict[str, Any]:
    meta = proc._memkernel_meta  # type: ignore[attr-defined]
    try:
        code = proc.wait(timeout=meta["timeout"])
    except subprocess.TimeoutExpired:
        proc.kill()
        code = "timeout"
    meta["file"].close()
    return {"agent_run": meta["name"], "exit_code": code, "seconds": round(time.time() - meta["started"], 1)}


def agent_commands(ws: Path, py: str) -> dict[str, tuple[list[str], dict[str, str]]]:
    claude_cfg = ws / ".claude-mcp.json"
    claude_cfg.write_text(json.dumps({"mcpServers": {"memkernel": {
        "command": py, "args": ["-m", "memkernel.server.mcp_server"],
        "env": {"MEMKERNEL_AGENT_ID": "claude-code-cli", "MEMKERNEL_GC_THRESHOLD": str(GC_THRESHOLD)},
    }}}), encoding="utf-8")
    claude_common = [
        "claude", "--output-format", "stream-json", "--verbose",
        "--mcp-config", str(claude_cfg), "--strict-mcp-config",
    ]
    claude_env = {"MCP_TIMEOUT": "180000"}
    codex_bin, codex_env = codex_command()
    codex = codex_bin + [
        "exec", "--json", "--skip-git-repo-check", "--ephemeral", "-s", "workspace-write", "-C", str(ws),
        "-c", f"mcp_servers.memkernel.command={json.dumps(py)}",
        "-c", 'mcp_servers.memkernel.args=["-m","memkernel.server.mcp_server"]',
        "-c", f'mcp_servers.memkernel.env={{MEMKERNEL_AGENT_ID="codex-cli",MEMKERNEL_GC_THRESHOLD="{GC_THRESHOLD}"}}',
        "-c", 'mcp_servers.memkernel.default_tools_approval_mode="approve"',
        "-c", "mcp_servers.memkernel.startup_timeout_sec=180",
        "-c", 'approval_policy="never"',
    ]
    if os.environ.get("MEMKERNEL_EVAL_CODEX_MODEL"):
        codex += ["-m", os.environ["MEMKERNEL_EVAL_CODEX_MODEL"]]
    return {
        "claude-fix": (claude_common + [
            "-p", PROMPTS["claude-fix"], "--permission-mode", "acceptEdits",
            "--allowedTools", "mcp__memkernel", "Read", "Edit", "Glob", "Grep", "Bash(python -m unittest:*)",
        ], claude_env),
        "codex-fix": (codex + [PROMPTS["codex-fix"]], codex_env),
        "antigravity-review": (["agy", "-p", PROMPTS["antigravity-review"], "--print-timeout", "10m"], {}),
        "claude-recall": (claude_common + ["-p", PROMPTS["claude-recall"], "--allowedTools", "mcp__memkernel"], claude_env),
    }


# ----------------------------------------------------------------- evidence


async def export_db(db: Path, out: Path) -> dict[str, Any]:
    async with AsyncEventStore(db) as store:
        branches = await store.list_branches()
        report: dict[str, Any] = {"branches": {}}
        all_events: dict[int, dict[str, Any]] = {}
        for b in branches:
            raw = await store.read_stream(b.branch_id)
            live = fold_live(raw)
            problems = await store.verify_branch(b.branch_id)
            state = materialize_live(live)
            report["branches"][b.branch_id] = {
                "head_event_id": b.head_event_id,
                "raw_events": len(raw),
                "live_events": len(live),
                "hash_chain_problems": problems,
                "decisions": {k: {"value": d.value, "agent_id": d.agent_id, "event_id": d.event_id}
                              for k, d in state.decisions.items()},
                "execution_blockers": sorted(state.execution_blockers),
            }
            for e in raw:
                all_events[e.event_id] = e.to_dict()  # type: ignore[index]
    events = [all_events[k] for k in sorted(all_events)]
    (out / "events.json").write_text(json.dumps(events, indent=1), encoding="utf-8")
    report["events_total"] = len(events)
    report["events_by_agent"] = _count(e["agent_id"] for e in events)
    report["events_by_type"] = _count(e["event_type"] for e in events)
    report["leases_granted"] = [
        {"agent_id": e["agent_id"], "resource": e["payload"]["resource"], "fencing_token": e["payload"]["fencing_token"],
         "event_id": e["event_id"]}
        for e in events if e["event_type"] == EventType.FILE_CLAIM_ACQUIRED.value
    ]
    report["snapshots"] = [e["event_id"] for e in events if e["event_type"] == EventType.SEMANTIC_SNAPSHOT.value]
    report["rollbacks"] = [
        {"agent_id": e["agent_id"], "branch_id": e["branch_id"], "target_event_id": e["payload"]["target_event_id"],
         "reverted_event_ids": e["payload"]["reverted_event_ids"]}
        for e in events if e["event_type"] == EventType.ROLLBACK_TRIGGERED.value
    ]
    return report


def _count(items: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        counts[item] = counts.get(item, 0) + 1
    return dict(sorted(counts.items()))


def tool_calls(log: Path) -> list[dict[str, Any]]:
    """memkernel tool calls actually executed, parsed from a structured transcript
    (Claude Code ``stream-json`` or Codex ``--json``). Plain-text logs yield []."""
    calls: list[dict[str, Any]] = []
    pending: dict[str, dict[str, Any]] = {}
    for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        item = ev.get("item") or {}
        if ev.get("type") == "item.completed" and item.get("type") == "mcp_tool_call" and item.get("server") == "memkernel":
            text = "".join(c.get("text", "") for c in ((item.get("result") or {}).get("content") or []))
            calls.append({"tool": item["tool"], "result": text})
        message = ev.get("message")
        blocks = message.get("content") if isinstance(message, dict) else None
        for block in blocks if isinstance(blocks, list) else []:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "tool_use" and block.get("name", "").startswith("mcp__memkernel__"):
                pending[block["id"]] = {"tool": block["name"].removeprefix("mcp__memkernel__"), "result": ""}
            elif block.get("type") == "tool_result" and block.get("tool_use_id") in pending:
                content = block.get("content")
                text = content if isinstance(content, str) else "".join(c.get("text", "") for c in content or [])
                call = pending.pop(block["tool_use_id"])
                call["result"] = text
                calls.append(call)
    return calls


def transcript_summary(out: Path) -> dict[str, dict[str, Any]]:
    summary = {}
    for log in sorted(out.glob("*.log")):
        calls = tool_calls(log)
        denials = sum(1 for c in calls if c["tool"] == "memkernel_acquire_lease"
                      and re.search(r'"acquired"\s*:\s*false', c["result"]))
        summary[log.stem] = {"tool_calls": _count(c["tool"].removeprefix("memkernel_") for c in calls),
                             "lease_denials": denials, "structured": bool(calls)}
    return summary


def docker_verify(out: Path) -> dict[str, Any]:
    """Re-verify the evidence database with a MemKernel built from source in a
    clean Linux container. Works on a copy so the evidence is never modified."""
    script = (
        "import asyncio, json\n"
        "from memkernel.core.store import AsyncEventStore\n"
        "async def main():\n"
        "    async with AsyncEventStore('/tmp/copy.db') as s:\n"
        "        res = {b.branch_id: await s.verify_branch(b.branch_id) for b in await s.list_branches()}\n"
        "    print(json.dumps(res))\n"
        "asyncio.run(main())\n"
    )
    cmd = [
        "docker", "run", "--rm", "-e", "MK_SCRIPT",
        "--mount", f"type=bind,source={REPO},target=/src,readonly",
        "--mount", f"type=bind,source={out},target=/evidence,readonly",
        "python:3.14-slim", "bash", "-c",
        "mkdir /work && tar -C /src --exclude=.env --exclude='*.egg-info' --exclude=__pycache__ "
        "--exclude=evidence -cf - . | tar -C /work -xf - && "
        "pip install -q --root-user-action=ignore /work >/dev/null 2>&1 && "
        "cp /evidence/memkernel.db /tmp/copy.db && python -c \"$MK_SCRIPT\"",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, "MK_SCRIPT": script}, timeout=900)
    try:
        verdict = json.loads(proc.stdout.strip().splitlines()[-1])
    except Exception:  # noqa: BLE001 - keep the raw output as evidence
        verdict = {"error": (proc.stdout + proc.stderr)[-2000:]}
    return {"image": "python:3.14-slim", "result": verdict}


def backup_db(src: Path, dst: Path) -> None:
    with sqlite3.connect(src) as source, sqlite3.connect(dst) as target:
        source.backup(target)


# --------------------------------------------------------------------- main


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=REPO / "evidence" / "live-agents")
    parser.add_argument("--workdir", type=Path, help="scratch directory for the sample repo (default: temp)")
    parser.add_argument("--antigravity", action="store_true", help="include the Antigravity CLI round")
    parser.add_argument("--docker-verify", action="store_true", help="re-verify the evidence DB in a clean Linux container")
    parser.add_argument("--timeout", type=int, default=900, help="seconds per agent run")
    args = parser.parse_args()

    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = (args.out / run_id).resolve()
    out.mkdir(parents=True)
    import tempfile

    ws = (args.workdir or Path(tempfile.mkdtemp(prefix="memkernel-live-"))).resolve() / "shop"
    ws.mkdir(parents=True)
    home = Path.home()
    _HOME_VARIANTS.extend(sorted({str(home), str(home).replace("\\", "/"), str(home).replace("\\", "\\\\"),
                                  "/c/Users/" + home.name, "/C/Users/" + home.name}, key=len, reverse=True))

    write_sample_repo(ws)
    (ws / ".memkernel").mkdir()
    before_code, before_out = run_tests(ws)
    (out / "tests-before.txt").write_text(sanitize(before_out), encoding="utf-8")

    py = sys.executable
    commands = agent_commands(ws, py)
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "platform": sys.platform,
        "python": sys.version.split()[0],
        "tools": {
            "claude": version(["claude", "--version"]),
            "codex": version(codex_command()[0] + ["--version"]),
            "agy": version(["agy", "--version"]) if args.antigravity else "not run",
        },
        "gc_threshold": GC_THRESHOLD,
        "tests_before_exit_code": before_code,
        "prompts": PROMPTS,
        "runs": [],
    }

    # Round 1: Claude Code and Codex concurrently.
    procs = [run_agent(n, commands[n][0], cwd=ws, env=commands[n][1], out=out, timeout=args.timeout)
             for n in ("claude-fix", "codex-fix")]
    manifest["runs"] += [wait_agent(p) for p in procs]

    # Round 2: Antigravity (global config edited temporarily, restored in finally).
    if args.antigravity:
        # Headless agy auto-denies MCP calls; allow only MemKernel's tools, temporarily.
        settings = home / ".gemini" / "antigravity-cli" / "settings.json"
        candidates = [settings, *cli._antigravity_paths(ws, home)]
        saved = {p: p.read_bytes() for p in candidates if p.exists()}
        try:
            cli.attach(["antigravity"], project=ws, home=home)
            data = json.loads(settings.read_text(encoding="utf-8")) if settings.exists() else {}
            data.setdefault("permissions", {}).setdefault("allow", []).append("mcp(memkernel/*)")
            settings.write_text(json.dumps(data, indent=2), encoding="utf-8")
            for p in cli._antigravity_paths(ws, home):
                data = json.loads(p.read_text(encoding="utf-8"))
                data["mcpServers"]["memkernel"]["env"]["MEMKERNEL_GC_THRESHOLD"] = str(GC_THRESHOLD)
                p.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
            proc = run_agent("antigravity-review", commands["antigravity-review"][0], cwd=ws, env={}, out=out,
                             timeout=args.timeout)
            manifest["runs"].append(wait_agent(proc))
        finally:
            for p in {*candidates, *cli._antigravity_paths(ws, home)}:
                if p in saved:
                    p.write_bytes(saved[p])
                elif p.exists():
                    p.unlink()
                p.with_name(p.name + ".memkernel.bak").unlink(missing_ok=True)
            manifest["antigravity_config_restored"] = all(p.read_bytes() == b for p, b in saved.items())

    # Round 3: fresh Claude session, memory only.
    proc = run_agent("claude-recall", commands["claude-recall"][0], cwd=ws, env=commands["claude-recall"][1],
                     out=out, timeout=args.timeout)
    manifest["runs"].append(wait_agent(proc))

    # Evidence.
    after_code, after_out = run_tests(ws)
    (out / "tests-after.txt").write_text(sanitize(after_out), encoding="utf-8")
    diff = subprocess.run([*GIT, "diff", "--", "src", "tests"], cwd=ws, capture_output=True, text=True).stdout
    (out / "code.diff").write_text(diff, encoding="utf-8")
    backup_db(ws / ".memkernel" / "memkernel.db", out / "memkernel.db")
    db_report = asyncio.run(export_db(out / "memkernel.db", out))
    manifest.update({
        "tests_after_exit_code": after_code,
        "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "database": db_report,
        "transcripts": transcript_summary(out),
    })
    if args.docker_verify:
        manifest["docker_verification"] = docker_verify(out)
    for log in out.glob("*.log"):
        log.write_text(sanitize(log.read_text(encoding="utf-8", errors="replace")), encoding="utf-8")
    for artifact in ("events.json",):
        path = out / artifact
        path.write_text(sanitize(path.read_text(encoding="utf-8")), encoding="utf-8")
    manifest_text = sanitize(json.dumps(manifest, indent=2, default=str))
    (out / "manifest.json").write_text(manifest_text, encoding="utf-8")
    (out / "REPORT.md").write_text(render_report(json.loads(manifest_text)), encoding="utf-8")
    print(out / "REPORT.md")
    return 0


def render_report(m: dict[str, Any]) -> str:
    db = m["database"]
    lines = [
        f"# Live multi-agent evaluation — run {m['run_id']}",
        "",
        "Generated by `benchmarks/live_agents.py` from the artifacts in this directory. Nothing here is hand-written.",
        "",
        f"- Started {m['started_utc']}, finished {m['finished_utc']} (UTC); platform `{m['platform']}`, Python {m['python']}.",
        f"- Agents: Claude Code `{m['tools']['claude']}`, Codex CLI `{m['tools']['codex']}`, Antigravity CLI `{m['tools']['agy']}`.",
        f"- Tests before: exit code {m['tests_before_exit_code']} (`tests-before.txt`). "
        f"Tests after: exit code {m['tests_after_exit_code']} (`tests-after.txt`). Code changes: `code.diff`.",
        "",
        "## Agent runs",
        "",
        "| Run | Exit code | Seconds | memkernel tool calls executed (from transcript) | Lease denials |",
        "|---|---:|---:|---|---:|",
    ]
    for run in m["runs"]:
        name = run["agent_run"]
        t = m["transcripts"].get(name, {})
        calls = ", ".join(f"{k}×{v}" for k, v in t.get("tool_calls", {}).items())
        if not t.get("structured"):
            calls = "plain-text transcript; see events by agent below"
        lines.append(f"| `{name}` | {run['exit_code']} | {run['seconds']} | {calls} | {t.get('lease_denials', 0)} |")
    lines += ["", "## What the shared log recorded (`events.json`, `memkernel.db`)", "",
              f"- {db['events_total']} events. By agent: "
              + ", ".join(f"`{k}` {v}" for k, v in db["events_by_agent"].items()) + ".",
              "- By type: " + ", ".join(f"{k} {v}" for k, v in db["events_by_type"].items()) + ".",
              "- Leases granted: " + ("; ".join(
                  f"`{g['resource']}` to `{g['agent_id']}` (token {g['fencing_token']}, event #{g['event_id']})"
                  for g in db["leases_granted"]) or "none") + ".",
              "- Rollbacks: " + ("; ".join(
                  f"`{r['agent_id']}` on `{r['branch_id']}` to #{r['target_event_id']}, reverting {r['reverted_event_ids']}"
                  for r in db["rollbacks"]) or "none") + ".",
              f"- GC snapshots written during the run: {len(db['snapshots'])} (threshold {m['gc_threshold']} tokens).",
              "", "| Branch | Raw events | Live events | Hash chain | Decisions (value, recorded by) |",
              "|---|---:|---:|---|---|"]
    for name, b in db["branches"].items():
        chain = "intact" if not b["hash_chain_problems"] else f"{len(b['hash_chain_problems'])} problem(s)"
        decisions = "; ".join(f"`{k}` = {d['value']!r} ({d['agent_id']})" for k, d in b["decisions"].items()) or "-"
        lines.append(f"| `{name}` | {b['raw_events']} | {b['live_events']} | {chain} | {decisions} |")
    if "docker_verification" in m:
        v = m["docker_verification"]
        lines += ["", f"Independent verification in a clean `{v['image']}` container: `{json.dumps(v['result'])}` "
                      "(an empty list per branch means the hash chain is intact)."]
    if "antigravity_config_restored" in m:
        lines += ["", f"Antigravity global MCP config restored byte-for-byte: {m['antigravity_config_restored']}."]
    lines += ["", "Transcripts: " + ", ".join(f"`{r['agent_run']}.log`" for r in m["runs"]) + ".", ""]
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
