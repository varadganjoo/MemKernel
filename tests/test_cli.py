"""`memkernel attach / doctor / status` and database discovery."""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

import pytest

from memkernel import cli
from memkernel.kernel import default_db_path


@pytest.fixture
def project(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / ".git").mkdir(parents=True)
    (root / ".gitignore").write_text("node_modules/", encoding="utf-8")
    return root


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    h = tmp_path / "home"
    h.mkdir()
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: h))
    monkeypatch.delenv("MEMKERNEL_DB", raising=False)
    return h


def test_default_db_path_discovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MEMKERNEL_DB", raising=False)
    (tmp_path / ".memkernel").mkdir()  # e.g. a stray ~/.memkernel
    repo = tmp_path / "code" / "app"
    (repo / ".git").mkdir(parents=True)
    (repo / "src" / "pkg").mkdir(parents=True)
    assert default_db_path(repo / "src" / "pkg") == repo.resolve() / ".memkernel" / "memkernel.db"
    (repo / "src" / ".memkernel").mkdir()  # nested project memory wins when nearer
    assert default_db_path(repo / "src" / "pkg") == repo.resolve() / "src" / ".memkernel" / "memkernel.db"
    assert default_db_path(tmp_path / "code") == tmp_path.resolve() / ".memkernel" / "memkernel.db"
    monkeypatch.setenv("MEMKERNEL_DB", str(tmp_path / "pinned.db"))
    assert default_db_path(repo) == tmp_path / "pinned.db"


def test_attach_all_merges_and_is_idempotent(project: Path, home: Path) -> None:
    (project / ".mcp.json").write_text(json.dumps({"mcpServers": {"github": {"command": "gh-mcp"}}}), encoding="utf-8")
    codex = home / ".codex" / "config.toml"
    codex.parent.mkdir()
    codex.write_text(
        'model = "gpt-5.5"\n\n[mcp_servers.memkernel]\ncommand = "old-python"\nargs = []\n\n'
        '[mcp_servers.memkernel.env]\nMEMKERNEL_AGENT_ID = "stale"\n\n[mcp_servers.other]\ncommand = "other"\n',
        encoding="utf-8",
    )
    legacy_ag = home / ".gemini" / "antigravity-ide" / "mcp_config.json"
    legacy_ag.parent.mkdir(parents=True)
    legacy_ag.write_text(json.dumps({"mcpServers": {"tavily": {"command": "npx"}}}), encoding="utf-8")

    report = cli.attach(["claude-code", "cursor", "codex", "antigravity"], project=project, home=home)
    actions = {(r["client"], Path(r["path"]).name): r["action"] for r in report}
    assert actions[("claude-code", ".mcp.json")] == "added"
    assert actions[("codex", "config.toml")] == "updated"
    assert actions[("git", ".gitignore")].startswith("added")

    claude = json.loads((project / ".mcp.json").read_text(encoding="utf-8"))["mcpServers"]
    assert claude["github"] == {"command": "gh-mcp"}  # untouched
    assert claude["memkernel"]["command"] == sys.executable
    assert claude["memkernel"]["args"] == ["-m", "memkernel.server.mcp_server"]
    assert claude["memkernel"]["env"] == {
        "MEMKERNEL_AGENT_ID": "claude-code-cli",
        "MEMKERNEL_DB": str(project.resolve() / ".memkernel" / "memkernel.db"),
    }
    cursor = json.loads((project / ".cursor" / "mcp.json").read_text(encoding="utf-8"))["mcpServers"]["memkernel"]
    assert cursor["env"]["MEMKERNEL_AGENT_ID"] == "cursor-ide"

    parsed = tomllib.loads(codex.read_text(encoding="utf-8"))
    assert parsed["model"] == "gpt-5.5" and parsed["mcp_servers"]["other"] == {"command": "other"}
    mk = parsed["mcp_servers"]["memkernel"]
    assert mk["command"] == sys.executable and mk["default_tools_approval_mode"] == "approve"
    assert mk["startup_timeout_sec"] == cli.CODEX_STARTUP_TIMEOUT_SEC
    assert mk["env"] == {"MEMKERNEL_AGENT_ID": "codex-cli"}  # global config: DB is discovered per project
    assert (codex.parent / "config.toml.memkernel.bak").read_text(encoding="utf-8").count("old-python") == 1

    ag = json.loads(legacy_ag.read_text(encoding="utf-8"))["mcpServers"]
    assert set(ag) == {"tavily", "memkernel"}  # existing location updated, none invented
    assert not (home / ".gemini" / "antigravity" / "mcp_config.json").exists()

    assert (project / ".memkernel").is_dir()
    assert (project / ".gitignore").read_text(encoding="utf-8").endswith(".memkernel/\n")

    again = cli.attach(["claude-code", "cursor", "codex", "antigravity"], project=project, home=home)
    assert {r["action"] for r in again} == {"unchanged"}
    assert (project / ".gitignore").read_text(encoding="utf-8").count(".memkernel/") == 1


def test_attach_portable_and_dry_run(project: Path, home: Path) -> None:
    report = cli.attach(["cursor"], project=project, home=home, portable=True, dry_run=True)
    assert [r["action"].endswith("(dry run)") for r in report] == [True, True]
    assert ".memkernel" not in (project / ".gitignore").read_text(encoding="utf-8")
    assert not (project / ".cursor").exists() and not (project / ".memkernel").exists()
    cli.attach(["cursor"], project=project, home=home, portable=True)
    entry = json.loads((project / ".cursor" / "mcp.json").read_text(encoding="utf-8"))["mcpServers"]["memkernel"]
    assert entry == {"command": "memkernel", "args": ["serve"], "env": {"MEMKERNEL_AGENT_ID": "cursor-ide"}}
    ag = cli.attach(["antigravity"], project=project, home=home)
    assert Path(ag[-1]["path"]) == home / ".gemini" / "antigravity" / "mcp_config.json"


def test_attach_refuses_to_clobber_broken_configs(project: Path, home: Path) -> None:
    broken = project / ".mcp.json"
    broken.write_text("{ not json", encoding="utf-8")
    with pytest.raises(cli.AttachError, match="not valid JSON"):
        cli.attach(["claude-code"], project=project, home=home)
    assert broken.read_text(encoding="utf-8") == "{ not json"

    codex = home / ".codex" / "config.toml"
    codex.parent.mkdir()
    dotted = '[mcp_servers]\nmemkernel.command = "x"\n'
    codex.write_text(dotted, encoding="utf-8")
    with pytest.raises(cli.AttachError, match="dotted keys"):
        cli.attach(["codex"], project=project, home=home)
    assert codex.read_text(encoding="utf-8") == dotted


def test_cli_main_attach_and_status(project: Path, home: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["attach", "codex", "--project", str(project)]) == 0
    out = capsys.readouterr().out
    assert "codex" in out and "memkernel doctor" in out
    assert cli.main(["status", "--project", str(project)]) == 0
    assert "No MemKernel database" in capsys.readouterr().out


async def test_doctor_end_to_end(project: Path, home: Path, capsys: pytest.CaptureFixture[str]) -> None:
    cli.attach(["claude-code"], project=project, home=home)
    assert await cli._doctor(project.resolve(), home) is True
    out = capsys.readouterr().out
    assert "15 tools" in out and "[ok] claude-code" in out and "[FAIL]" not in out
    await cli._status(project.resolve(), "main", as_json=False)
    assert "branches  main" in capsys.readouterr().out
