"""Signed folder exchange using actual keys, local files and independent SQLite databases."""

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from memkernel import MemKernel
from memkernel.core.events import canonical_json
from memkernel.private_files import protect_private_file


@pytest.fixture
async def peers(tmp_path):
    folder = tmp_path / "exchange"
    folder.mkdir()
    configs, kernels, keys = {}, {}, {}
    for peer in ("alice", "bob"):
        keys[peer] = Ed25519PrivateKey.generate()
    trusted = {
        peer: {
            "public_key": key.public_key().public_bytes_raw().hex(),
            "agents": [peer],
        }
        for peer, key in keys.items()
    }
    for peer, key in keys.items():
        home = tmp_path / peer
        home.mkdir()
        private = home / "signing.key"
        private.write_bytes(key.private_bytes_raw())
        private.chmod(0o600)
        protect_private_file(private)
        config = home / "sync.json"
        config.write_text(
            json.dumps(
                {
                    "schema": "memkernel.sync-config/v1",
                    "workspace_id": "sample",
                    "peer_id": peer,
                    "folder": str(folder),
                    "private_key": str(private),
                    "trusted_peers": trusted,
                    "stale_after_seconds": 30,
                }
            ),
            encoding="utf-8",
        )
        configs[peer] = config
        protect_private_file(config)
        kernels[peer] = await MemKernel.open(
            home / "memory.db", tokenizer="heuristic", auto_gc=False
        )
    try:
        yield kernels, configs, folder, keys
    finally:
        for kernel in kernels.values():
            await kernel.close()


async def setup_peers(peers):
    kernels, configs, folder, keys = peers
    for peer, kernel in kernels.items():
        await kernel.enable_sync(configs[peer])
    return kernels["alice"], kernels["bob"], folder, keys


async def test_concurrent_cold_database_opens_do_not_fail_during_wal_initialization(
    tmp_path,
):
    for i in range(100):
        results = await asyncio.gather(
            *[
                MemKernel.open(tmp_path / f"cold-{i}.db", tokenizer="heuristic")
                for _ in range(2)
            ],
            return_exceptions=True,
        )
        try:
            assert not [
                result for result in results if isinstance(result, BaseException)
            ]
        finally:
            for result in results:
                if isinstance(result, MemKernel):
                    await result.close()


async def test_signed_roundtrip_and_owner_clear(peers):
    alice, bob, folder, _ = await setup_peers(peers)
    notice = await alice.notices.post(
        agent_id="alice", resource="service:auth", message="Changing login"
    )
    assert (await alice.sync.run_once())["published"] is True
    assert (await bob.sync.run_once())["changed"] == 1
    assert (await bob.notices.list())[0]["notice_id"] == notice.notice_id
    with pytest.raises(ValueError, match="local author"):
        await bob.notices.clear(notice.notice_id, agent_id="alice")
    await alice.notices.clear(notice.notice_id, agent_id="alice")
    await alice.sync.run_once()
    await bob.sync.run_once()
    assert await bob.notices.list() == []
    assert {p.name for p in folder.iterdir()} == {"alice.json", "bob.json"}
    assert "Changing login" not in canonical_json(await bob.sync.audit())


async def test_delayed_partial_transport_between_separate_exchange_folders(
    peers, tmp_path
):
    alice, bob, folder, _ = await setup_peers(peers)
    remote = tmp_path / "bob-transport"
    remote.mkdir()
    config = json.loads(peers[1]["bob"].read_text())
    config["folder"] = str(remote)
    peers[1]["bob"].write_text(json.dumps(config))
    await bob.enable_sync(peers[1]["bob"])
    notice = await alice.notices.post(
        agent_id="alice", resource="service:auth", message="Working"
    )
    await alice.sync.run_once()
    old = (folder / "alice.json").read_bytes()
    (remote / "alice.json").write_bytes(old[: len(old) // 2])
    assert (await bob.sync.run_once())["errors"]["alice"] == "invalid_document"
    assert await bob.notices.list() == []
    (remote / "alice.json").write_bytes(old)
    assert (await bob.sync.run_once())["changed"] == 1
    now = datetime.now(UTC) + timedelta(seconds=31)
    bob.sync.clock = alice.sync.clock = lambda: now
    await bob.sync.run_once()
    assert (await bob.sync.status())["peers"]["alice"]["status"] == "stale"
    assert len(await bob.notices.list()) == 1
    await alice.notices.clear(notice.notice_id, agent_id="alice")
    await alice.sync.run_once()
    assert len(await bob.notices.list()) == 1  # A clear is unknown until delivered.
    (remote / "alice.json").write_bytes((folder / "alice.json").read_bytes())
    await bob.sync.run_once()
    assert await bob.notices.list() == []
    (remote / "alice.json").write_bytes(old)
    assert (await bob.sync.run_once())["errors"]["alice"] == "replayed_sequence"


async def test_online_backup_restores_and_catches_up_without_losing_identity(
    peers, tmp_path
):
    import sqlite3
    from contextlib import closing

    alice, bob, _, _ = await setup_peers(peers)
    await alice.notices.post(
        agent_id="alice", resource="service:auth", message="Before backup"
    )
    await alice.sync.run_once()
    await bob.sync.run_once()
    backup = tmp_path / "restored.db"
    with (
        closing(sqlite3.connect(bob.store.path)) as source,
        closing(sqlite3.connect(backup)) as destination,
    ):
        source.backup(destination)
    await alice.notices.post(
        agent_id="alice", resource="service:billing", message="After backup"
    )
    await alice.sync.run_once()
    await bob.close()
    async with await MemKernel.open(backup, tokenizer="heuristic") as restored:
        await restored.enable_sync(peers[1]["bob"])
        assert len(await restored.notices.list()) == 1
        assert (await restored.sync.run_once())["changed"] == 1
        assert len(await restored.notices.list()) == 2
        assert (await restored.sync.status())["peer_id"] == "bob"


async def test_recovering_valid_duplicate_clears_error_without_refreshing_liveness(
    peers,
):
    alice, bob, folder, _ = await setup_peers(peers)
    await alice.sync.run_once()
    await bob.sync.run_once()
    before = (await bob.sync.status())["peers"]["alice"]["received_at"]
    path = folder / "alice.json"
    original = path.read_bytes()
    path.write_text("{}")
    assert (await bob.sync.run_once())["errors"]
    path.write_bytes(original)
    await bob.sync.run_once()
    status = (await bob.sync.status())["peers"]["alice"]
    assert status["last_error"] is None and status["received_at"] == before


async def test_published_sequence_recovers_if_database_commit_was_lost(peers):
    alice, bob, folder, _ = await setup_peers(peers)
    await alice.sync.run_once()
    await bob.sync.run_once()
    async with alice.store.transaction(write=True) as db:
        await db.execute("DELETE FROM sync_state WHERE peer_id='alice'")
    await alice.sync.run_once()
    assert json.loads((folder / "alice.json").read_text())["sequence"] == 2
    assert (await bob.sync.run_once())["errors"] == {}


async def test_unsigned_legacy_notices_are_not_silently_signed_as_local(peers):
    kernels, _, _, _ = peers
    await kernels["alice"].notices.post(
        agent_id="alice", resource="service:auth", message="Unverified legacy"
    )
    alice, bob, folder, _ = await setup_peers(peers)
    await alice.sync.run_once()
    assert json.loads((folder / "alice.json").read_text())["notices"] == []
    await bob.sync.run_once()
    assert await bob.notices.list() == []
    assert (await alice.notices.list())[0]["trusted_source"] is False


async def test_wrong_signing_key_and_database_in_exchange_are_rejected(peers):
    kernels, configs, folder, keys = peers
    config = json.loads(configs["alice"].read_text())
    private = Path(config["private_key"])
    private.write_bytes(keys["bob"].private_bytes_raw())
    with pytest.raises(ValueError, match="pinned"):
        await kernels["alice"].enable_sync(configs["alice"])
    private.write_bytes(keys["alice"].private_bytes_raw())
    async with await MemKernel.open(folder / "bad.db", tokenizer="heuristic") as unsafe:
        with pytest.raises(ValueError, match="outside"):
            await unsafe.enable_sync(configs["alice"])


@pytest.mark.skipif(os.name != "nt", reason="Windows NTFS permission enforcement")
async def test_windows_world_readable_signing_key_is_rejected(peers):
    import win32security

    kernels, configs, _, _ = peers
    path = json.loads(configs["alice"].read_text())["private_key"]
    acl = win32security.GetFileSecurity(
        path, win32security.DACL_SECURITY_INFORMATION
    ).GetSecurityDescriptorDacl()
    if acl is None:
        acl = win32security.ACL()
    acl.AddAccessAllowedAce(
        win32security.ACL_REVISION,
        0x1F01FF,
        win32security.ConvertStringSidToSid("S-1-1-0"),
    )
    win32security.SetNamedSecurityInfo(
        path,
        win32security.SE_FILE_OBJECT,
        win32security.DACL_SECURITY_INFORMATION,
        None,
        None,
        acl,
        None,
    )
    with pytest.raises(ValueError, match="permissions"):
        await kernels["alice"].enable_sync(configs["alice"])


async def test_read_only_store_cannot_start_a_sync_writer(peers):
    alice, _, _, _ = await setup_peers(peers)
    async with await MemKernel.open(
        alice.store.path, tokenizer="heuristic", read_only=True
    ) as reader:
        with pytest.raises(ValueError, match="read-only"):
            await reader.enable_sync(peers[1]["alice"])


@pytest.mark.skipif(os.name == "nt", reason="POSIX special files and private-key modes")
async def test_special_files_and_world_readable_keys_fail_closed(peers):
    alice, bob, folder, _ = await setup_peers(peers)
    os.mkfifo(folder / "alice.json")
    assert (await bob.sync.run_once())["errors"]["alice"] == "unsafe_file"
    alice.sync.config.private_key.chmod(0o644)
    with pytest.raises(ValueError, match="permissions"):
        await alice.enable_sync(peers[1]["alice"])


async def test_symlinked_peer_bundle_is_not_followed(peers):
    alice, bob, folder, _ = await setup_peers(peers)
    await alice.sync.run_once()
    target = folder / "saved.json"
    (folder / "alice.json").rename(target)
    try:
        (folder / "alice.json").symlink_to(target)
    except OSError:
        pytest.skip("OS does not permit symlink creation")
    assert (await bob.sync.run_once())["errors"]["alice"] == "unsafe_file"


async def test_unsigned_import_and_foreign_author_fail_closed(peers):
    alice, _, _, _ = await setup_peers(peers)
    with pytest.raises(ValueError, match="signed"):
        await alice.notices.import_json(
            '{"schema":"memkernel.notices/v1","notices":[]}'
        )
    with pytest.raises(ValueError, match="local author"):
        await alice.notices.post(
            agent_id="bob", resource="service:auth", message="Impersonation"
        )


async def test_tampering_cannot_change_memory_or_cursor(peers):
    alice, bob, folder, _ = await setup_peers(peers)
    await alice.notices.post(
        agent_id="alice", resource="service:auth", message="Original"
    )
    await alice.sync.run_once()
    path = folder / "alice.json"
    valid = path.read_bytes()
    document = json.loads(valid)
    document["notices"][0]["message"] = "Tampered"
    path.write_text(json.dumps(document), encoding="utf-8")
    result = await bob.sync.run_once()
    assert result["errors"]["alice"] == "invalid_signature"
    assert await bob.notices.list() == []
    path.write_bytes(valid)
    assert (await bob.sync.run_once())["changed"] == 1


def write_signed(path, document, key):
    document.pop("signature", None)
    document["signature"] = key.sign(canonical_json(document).encode()).hex()
    path.write_text(canonical_json(document), encoding="utf-8")


@pytest.mark.parametrize(
    "mutation,error",
    [
        (lambda doc: doc.update(workspace_id="other-project"), "wrong_workspace"),
        (lambda doc: doc["notices"][0].update(author="bob"), "unauthorized_author"),
        (lambda doc: doc.update(sequence=True), "invalid_document"),
        (
            lambda doc: doc.update(
                sent_at=(datetime.now(UTC) + timedelta(days=1)).isoformat()
            ),
            "future_timestamp",
        ),
    ],
)
async def test_valid_signature_does_not_bypass_policy(peers, mutation, error):
    alice, bob, folder, keys = await setup_peers(peers)
    await alice.notices.post(
        agent_id="alice", resource="service:auth", message="Working"
    )
    await alice.sync.run_once()
    path = folder / "alice.json"
    document = json.loads(path.read_text())
    mutation(document)
    write_signed(path, document, keys["alice"])
    assert (await bob.sync.run_once())["errors"]["alice"] == error
    assert await bob.notices.list() == []


async def test_replay_and_equivocation_do_not_refresh_peer(peers):
    alice, bob, folder, keys = await setup_peers(peers)
    now = datetime.now(UTC)
    alice.sync.clock = bob.sync.clock = lambda: now
    await alice.sync.run_once()
    old = (folder / "alice.json").read_bytes()
    await bob.sync.run_once()
    before = (await bob.sync.status())["peers"]["alice"]["received_at"]
    now += timedelta(seconds=31)
    await bob.sync.run_once()
    status = (await bob.sync.status())["peers"]["alice"]
    assert status["received_at"] == before and status["status"] == "stale"
    await alice.sync.run_once()
    await bob.sync.run_once()
    (folder / "alice.json").write_bytes(old)
    assert (await bob.sync.run_once())["errors"]["alice"] == "replayed_sequence"
    doc = json.loads(old)
    doc["sequence"] += 1
    doc["sent_at"] = (now + timedelta(seconds=1)).isoformat()
    write_signed(folder / "alice.json", doc, keys["alice"])
    assert (await bob.sync.run_once())["errors"]["alice"] == "sequence_conflict"


async def test_conflict_rolls_back_notice_batch_and_replay_cursor(peers):
    alice, bob, folder, keys = await setup_peers(peers)
    original = await alice.notices.post(
        agent_id="alice", resource="service:auth", message="Original"
    )
    await alice.sync.run_once()
    await bob.sync.run_once()
    await alice.notices.post(
        agent_id="alice", resource="service:billing", message="New"
    )
    await alice.sync.run_once()
    path = folder / "alice.json"
    valid = path.read_bytes()
    doc = json.loads(valid)
    doc["notices"].sort(key=lambda n: n["notice_id"] == original.notice_id)
    doc["notices"][-1]["message"] = "Conflict"
    write_signed(path, doc, keys["alice"])
    assert (await bob.sync.run_once())["errors"]["alice"] == "notice_conflict"
    assert len(await bob.notices.list()) == 1
    path.write_bytes(valid)
    assert (await bob.sync.run_once())["changed"] == 1


async def test_missing_and_invalid_peer_files_are_visible_and_isolated(peers):
    _alice, bob, folder, _ = await setup_peers(peers)
    assert (await bob.sync.run_once())["errors"]["alice"] == "missing_file"
    (folder / "alice.json").write_text("{", encoding="utf-8")
    assert (await bob.sync.run_once())["errors"]["alice"] == "invalid_document"
    (folder / "alice.json").write_bytes(b"x" * (8 * 1024 * 1024 + 1))
    assert (await bob.sync.run_once())["errors"]["alice"] == "oversized_file"
    assert await bob.notices.list() == []
    assert (await bob.sync.audit())[0]["outcome"] in {
        "accepted",
        "rejected",
        "published",
    }


async def test_concurrent_publishers_never_leave_an_older_snapshot(peers):
    alice, bob, folder, _ = await setup_peers(peers)
    config = peers[1]["alice"]
    async with await MemKernel.open(alice.store.path, tokenizer="heuristic") as other:
        await other.enable_sync(config)
        results = await asyncio.gather(
            *[(alice if i % 2 else other).sync.run_once() for i in range(8)]
        )
        assert all(result["published"] for result in results)
        sequence = json.loads((folder / "alice.json").read_text())["sequence"]
        assert sequence == 8
    await bob.sync.run_once()
    assert (await bob.sync.status())["peers"]["alice"]["sequence"] == 8


@pytest.mark.parametrize(
    "change",
    [
        lambda cfg: cfg.update(peer_id="../escape"),
        lambda cfg: cfg.update(workspace_id=""),
        lambda cfg: cfg.update(interval_seconds=0),
        lambda cfg: cfg["trusted_peers"]["bob"].update(agents=["alice"]),
        lambda cfg: cfg["trusted_peers"].pop("alice"),
        lambda cfg: cfg.update(private_key=str(Path(cfg["folder"]) / "key")),
    ],
)
async def test_invalid_local_policy_fails_before_exchange(peers, change):
    kernels, configs, folder, _ = peers
    cfg = json.loads(configs["alice"].read_text())
    change(cfg)
    configs["alice"].write_text(json.dumps(cfg))
    with pytest.raises(ValueError):
        await kernels["alice"].enable_sync(configs["alice"])
    assert list(folder.iterdir()) == []


@pytest.mark.parametrize("peer_id", ["Alice", "con", "nul", "com1", "lpt9"])
async def test_peer_names_are_portable_and_cannot_alias_windows_devices(peers, peer_id):
    kernels, configs, folder, _ = peers
    config = json.loads(configs["alice"].read_text())
    config["trusted_peers"][peer_id] = config["trusted_peers"].pop("alice")
    config["peer_id"] = peer_id
    configs["alice"].write_text(json.dumps(config))
    with pytest.raises(ValueError):
        await kernels["alice"].enable_sync(configs["alice"])
    assert list(folder.iterdir()) == []


async def test_revocation_requires_no_shared_folder_changes(peers):
    alice, bob, folder, _ = await setup_peers(peers)
    await alice.notices.post(
        agent_id="alice", resource="service:auth", message="Working"
    )
    await alice.sync.run_once()
    await bob.sync.run_once()
    cfg = json.loads(peers[1]["bob"].read_text())
    cfg["trusted_peers"].pop("alice")
    peers[1]["bob"].write_text(json.dumps(cfg))
    await bob.enable_sync(peers[1]["bob"])
    result = await bob.sync.run_once()
    assert "alice" not in result["errors"]
    assert "alice" not in (await bob.sync.status())["peers"]
    assert (folder / "alice.json").exists()
    assert (await bob.notices.list())[0]["trusted_source"] is False


async def test_atomic_publish_failure_preserves_previous_file(peers, monkeypatch):
    alice, _, folder, _ = await setup_peers(peers)
    await alice.sync.run_once()
    before = (folder / "alice.json").read_bytes()
    real_replace = os.replace

    def failing_replace(src, dst):
        if Path(dst) == folder / "alice.json":
            raise OSError("simulated interrupted replace")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", failing_replace)
    result = await alice.sync.run_once()
    assert result["published"] is False
    assert (folder / "alice.json").read_bytes() == before
    assert not list(folder.glob("*.tmp"))


async def test_managed_database_cannot_be_reopened_to_bypass_signed_import_policy(
    peers,
):
    alice, _, _, _ = await setup_peers(peers)
    async with await MemKernel.open(
        alice.store.path, tokenizer="heuristic"
    ) as unconfigured:
        with pytest.raises(ValueError, match="signed"):
            await unconfigured.notices.import_json(
                '{"schema":"memkernel.notices/v1","notices":[]}'
            )
        with pytest.raises(ValueError, match="local author"):
            await unconfigured.notices.post(
                agent_id="alice", resource="service:auth", message="Bypass"
            )


async def test_database_identity_cannot_change_workspaces_or_local_peers(peers):
    alice, _, _, _ = await setup_peers(peers)
    cfg = json.loads(peers[1]["alice"].read_text())
    cfg["workspace_id"] = "another-project"
    peers[1]["alice"].write_text(json.dumps(cfg))
    with pytest.raises(ValueError, match="bound"):
        await alice.enable_sync(peers[1]["alice"])


async def test_stdio_sync_and_context_are_usable_by_independent_agents(peers):
    from memkernel.clients.adapter import McpStdioClient, McpToolError

    kernels, configs, _, _ = peers
    async with (
        McpStdioClient(
            agent_id="alice",
            db_path=kernels["alice"].store.path,
            env={"MEMKERNEL_SYNC_CONFIG": str(configs["alice"])},
        ) as alice,
        McpStdioClient(
            agent_id="bob",
            db_path=kernels["bob"].store.path,
            env={"MEMKERNEL_SYNC_CONFIG": str(configs["bob"])},
        ) as bob,
    ):
        notice = await alice.call(
            "memkernel_post_notice", resource="service:auth", message="Changing login"
        )
        assert (await alice.call("memkernel_sync_now"))["published"]
        assert (await bob.call("memkernel_sync_now"))["errors"] == {}
        state = await bob.call(
            "memkernel_query_state",
            notice_resource="service:auth",
            notice_token_budget=500,
        )
        assert state["coordination_notices"][0]["notice_id"] == notice["notice_id"]
        assert state["sync"]["enabled"] is True
        with pytest.raises(McpToolError, match="signed"):
            await bob.call(
                "memkernel_import_notices",
                document='{"schema":"memkernel.notices/v1","notices":[]}',
            )
        assert (await bob.call("memkernel_sync_status"))["peers"]["alice"][
            "status"
        ] == "fresh"
