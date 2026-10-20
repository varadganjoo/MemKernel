import json

from memkernel import cli


def test_cli_initialize_pin_run_inspect_and_revoke(tmp_path, capsys):
    folder, config, db = (
        tmp_path / "exchange",
        tmp_path / "local" / "sync.json",
        tmp_path / "local" / "mem.db",
    )
    assert (
        cli.main(
            [
                "sync",
                "init",
                "--config",
                str(config),
                "--folder",
                str(folder),
                "--workspace",
                "example",
                "--peer",
                "alice",
                "--agents",
                "codex-cli",
            ]
        )
        == 0
    )
    identity = json.loads(capsys.readouterr().out)
    assert len(identity["public_key"]) == 64 and len(identity["fingerprint"]) == 64
    assert "private" not in identity
    before = config.read_bytes()
    assert (
        cli.main(
            [
                "sync",
                "init",
                "--config",
                str(config),
                "--folder",
                str(folder),
                "--workspace",
                "example",
                "--peer",
                "alice",
                "--agents",
                "codex-cli",
            ]
        )
        == 1
    )
    assert config.read_bytes() == before
    common = ["--config", str(config), "--db", str(db)]
    assert cli.main(["sync", "run", *common]) == 0
    result = capsys.readouterr().out
    assert '"published": true' in result
    assert cli.main(["sync", "status", *common]) == 0
    assert '"workspace_id": "example"' in capsys.readouterr().out
    assert cli.main(["sync", "audit", *common]) == 0
    assert '"published"' in capsys.readouterr().out


def test_cli_rejects_private_material_inside_exchange(tmp_path, capsys):
    folder = tmp_path / "exchange"
    assert (
        cli.main(
            [
                "sync",
                "init",
                "--config",
                str(folder / "sync.json"),
                "--folder",
                str(folder),
                "--workspace",
                "example",
                "--peer",
                "alice",
                "--agents",
                "codex-cli",
            ]
        )
        == 1
    )
    assert not folder.exists()
    assert "outside" in capsys.readouterr().err


def test_cli_pin_requires_valid_unique_identity(tmp_path, capsys):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    config = tmp_path / "local" / "sync.json"
    assert (
        cli.main(
            [
                "sync",
                "init",
                "--config",
                str(config),
                "--folder",
                str(tmp_path / "exchange"),
                "--workspace",
                "example",
                "--peer",
                "alice",
                "--agents",
                "codex-cli",
            ]
        )
        == 0
    )
    capsys.readouterr()
    public = Ed25519PrivateKey.generate().public_key().public_bytes_raw().hex()
    args = [
        "sync",
        "trust",
        "--config",
        str(config),
        "--peer",
        "bob",
        "--public-key",
        public,
        "--agents",
        "cursor-ide",
    ]
    assert cli.main(args) == 0
    assert "bob" in json.loads(config.read_text())["trusted_peers"]
    assert (
        cli.main(
            [
                "sync",
                "run",
                "--config",
                str(config),
                "--db",
                str(tmp_path / "memory.db"),
            ]
        )
        == 1
    )
    assert "missing_file" in capsys.readouterr().out
    assert cli.main(["sync", "revoke", "--config", str(config), "--peer", "bob"]) == 0
    assert "bob" not in json.loads(config.read_text())["trusted_peers"]
