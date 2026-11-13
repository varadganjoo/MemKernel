"""Backups must include committed WAL state and never destroy an existing file."""

from __future__ import annotations

import asyncio
import shutil
import sqlite3
import threading
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest

from memkernel import MemKernel, cli
from memkernel.core.events import EventType, new_event
from memkernel.private_files import assert_private_permissions


async def test_backup_captures_wal_and_all_tables(kernel: MemKernel, tmp_path: Path) -> None:
    from memkernel.backup import backup_database

    with closing(sqlite3.connect(kernel.store.path)) as source:
        source.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
    first = await kernel.append_decision(branch_id="main", agent_id="owner", key="db", value="sqlite", rationale="offline")
    for index in range(20):
        await kernel.log_execution(branch_id="main", agent_id="owner", command="probe", exit_code=0,
                                   output="diagnostic " * 100, facts={"index": index})
    assert (await kernel.gc(force=True)).triggered
    await kernel.fork(source_branch="main", new_branch="experiment", agent_id="owner")
    await kernel.append_decision(branch_id="experiment", agent_id="owner", key="db", value="postgres", rationale="try")
    await kernel.rollback(branch_id="experiment", target_event_id=first.event_id, agent_id="owner")
    await kernel.notices.post(agent_id="owner", resource="service:auth", message="working")
    await kernel.acquire_lease(branch_id="main", agent_id="owner", resource="auth.py", ttl_seconds=60)
    async with kernel.store.transaction(write=True) as db:
        await db.execute("INSERT INTO sync_state(peer_id, sequence, digest, sent_at, received_at) VALUES (?, ?, ?, ?, ?)",
                         ("peer", 1, "test-digest", "2026-11-13T18:00:00Z", "2026-11-13T18:00:00Z"))

    plain = tmp_path / "plain-copy.db"
    shutil.copyfile(kernel.store.path, plain)
    with closing(sqlite3.connect(plain)) as copied:
        assert copied.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert copied.execute("SELECT count(*) FROM events").fetchone()[0] == 0

    backup = tmp_path / "backup # %.db"
    result = await backup_database(Path(kernel.store.path), backup)
    assert result["branches"] == 2
    assert_private_permissions(backup)
    with closing(sqlite3.connect(kernel.store.path)) as source, closing(sqlite3.connect(backup)) as copied:
        for (table,) in source.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            assert copied.execute(f'SELECT * FROM "{table}"').fetchall() == source.execute(f'SELECT * FROM "{table}"').fetchall()
    async with await MemKernel.open(backup, read_only=True, tokenizer="heuristic") as restored:
        for branch in ("main", "experiment"):
            assert await restored.verify(branch) == []
            assert (await restored.query_state(branch))["state"] == (await kernel.query_state(branch))["state"]
    assert not any(Path(str(backup) + suffix).exists() for suffix in ("-wal", "-shm", "-journal"))
    await kernel.note(branch_id="main", agent_id="owner", note="source remains writable")


@pytest.mark.parametrize("existing", ["file", "directory", "sidecar", "source"])
async def test_backup_refuses_existing_targets(kernel: MemKernel, tmp_path: Path, existing: str) -> None:
    from memkernel.backup import backup_database

    target = tmp_path / "backup.db"
    if existing == "file":
        target.write_bytes(b"keep me")
    elif existing == "directory":
        target.mkdir()
    elif existing == "sidecar":
        Path(str(target) + "-wal").write_bytes(b"keep me")
    else:
        target = Path(kernel.store.path)
    with pytest.raises((ValueError, FileExistsError)):
        await backup_database(Path(kernel.store.path), target)
    if existing == "file":
        assert target.read_bytes() == b"keep me"
    if existing == "sidecar":
        assert Path(str(target) + "-wal").read_bytes() == b"keep me"


async def test_backup_missing_source_does_not_create_files(tmp_path: Path) -> None:
    from memkernel.backup import backup_database

    source, target = tmp_path / "missing.db", tmp_path / "backup.db"
    with pytest.raises((OSError, sqlite3.Error, ValueError)):
        await backup_database(source, target)
    assert list(tmp_path.iterdir()) == []


async def test_backup_rejects_invalid_hash_without_publishing(kernel: MemKernel, tmp_path: Path) -> None:
    from memkernel.backup import backup_database

    await kernel.note(branch_id="main", agent_id="owner", note="original")
    async with kernel.store.transaction(write=True) as db:
        await db.execute("DROP TRIGGER events_no_update")
        await db.execute("UPDATE events SET hash = ?", ("0" * 64,))
    target = tmp_path / "backup.db"
    with pytest.raises(ValueError, match="hash|integrity"):
        await backup_database(Path(kernel.store.path), target)
    assert not target.exists()
    assert not list(tmp_path.glob(".memkernel-backup-*"))


def test_backup_cli_selects_database_and_reports_errors(tmp_path: Path, capsys, monkeypatch) -> None:
    source, target = tmp_path / "memory # %.db", tmp_path / "backup.db"

    async def initialize():
        async with await MemKernel.open(source, tokenizer="heuristic"):
            pass

    asyncio.run(initialize())
    monkeypatch.setenv("MEMKERNEL_DB", str(source))
    assert cli.main(["backup", str(target)]) == 0
    assert target.exists() and "backup" in capsys.readouterr().out.lower()
    assert cli.main(["backup", str(target), "--db", str(source)]) == 1
    assert "error:" in capsys.readouterr().err


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
async def test_backup_cannot_target_source_sidecars(kernel: MemKernel, suffix: str) -> None:
    from memkernel.backup import backup_database

    with pytest.raises(ValueError, match="source database sidecar"):
        await backup_database(Path(kernel.store.path), Path(kernel.store.path + suffix))
    assert await kernel.verify() == []


async def test_backup_publication_race_keeps_other_creators_file(kernel: MemKernel, tmp_path: Path, monkeypatch) -> None:
    from memkernel import backup

    link = backup.os.link
    target = tmp_path / "backup.db"

    def racing_link(source, destination):
        target.write_bytes(b"another process created this")
        link(source, destination)

    monkeypatch.setattr(backup.os, "link", racing_link)
    with pytest.raises(FileExistsError):
        await backup.backup_database(Path(kernel.store.path), target)
    assert target.read_bytes() == b"another process created this"
    assert not list(tmp_path.glob(".memkernel-backup-*"))


@pytest.mark.parametrize("cancel_count", [1, 2])
async def test_backup_cancel_waits_for_worker_and_cleans_partial_file(kernel: MemKernel, tmp_path: Path, monkeypatch,
                                                                   cancel_count: int) -> None:
    from memkernel import backup

    started, finish = threading.Event(), threading.Event()

    def slow_copy(_source, temporary):
        with temporary.open("wb") as stream:
            stream.write(b"partial private memory")
            stream.flush()
            started.set()
            assert finish.wait(5)

    monkeypatch.setattr(backup, "_copy_database", slow_copy)
    target = tmp_path / "backup.db"
    task = asyncio.create_task(backup.backup_database(Path(kernel.store.path), target))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        for _ in range(cancel_count):
            task.cancel()
            await asyncio.sleep(0)
        assert not task.done() and not target.exists()
    finally:
        finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not list(tmp_path.glob(".memkernel-backup-*")) and not target.exists()


async def test_backup_permission_failure_writes_no_memory(kernel: MemKernel, tmp_path: Path, monkeypatch) -> None:
    from memkernel import backup

    def refuse_permissions(_path):
        raise ValueError("cannot establish private permissions")

    monkeypatch.setattr(backup, "protect_private_file", refuse_permissions)
    target = tmp_path / "backup.db"
    with pytest.raises(ValueError, match="private permissions"):
        await backup.backup_database(Path(kernel.store.path), target)
    assert not list(tmp_path.glob(".memkernel-backup-*")) and not target.exists()


async def test_backup_refuses_dangling_destination_symlink(kernel: MemKernel, tmp_path: Path) -> None:
    from memkernel.backup import backup_database

    target, missing = tmp_path / "backup.db", tmp_path / "missing.db"
    try:
        target.symlink_to(missing)
    except OSError:
        pytest.skip("host does not permit symlink creation")
    with pytest.raises(FileExistsError):
        await backup_database(Path(kernel.store.path), target)
    assert target.is_symlink() and not missing.exists()


async def test_backup_reads_committed_snapshot_during_active_writer(kernel: MemKernel, tmp_path: Path) -> None:
    from memkernel.backup import backup_database

    committed = await kernel.note(branch_id="main", agent_id="owner", note="committed")
    target = tmp_path / "backup.db"
    async with kernel.store.transaction(write=True) as db:
        await kernel.store.append_in_transaction(db, new_event(
            EventType.AGENT_SCRATCHPAD, branch_id="main", agent_id="owner", payload={"note": "uncommitted"}))
        await asyncio.wait_for(backup_database(Path(kernel.store.path), target), timeout=5)
        async with await MemKernel.open(target, read_only=True, tokenizer="heuristic") as restored:
            assert [e.event_id for e in await restored.store.read_stream("main")] == [committed.event_id]
            assert await restored.verify() == []
    assert len(await kernel.store.read_stream("main")) == 2


async def test_backup_repeated_cancel_during_verification_closes_database(kernel: MemKernel, tmp_path: Path, monkeypatch) -> None:
    from memkernel import backup

    started, finish = asyncio.Event(), asyncio.Event()

    async def slow_verify(store, _branch):
        with closing(sqlite3.connect(store.path)) as reader:
            reader.execute("SELECT count(*) FROM events").fetchone()
            started.set()
            await finish.wait()
        return []

    monkeypatch.setattr(backup.AsyncEventStore, "verify_branch", slow_verify)
    target = tmp_path / "backup.db"
    task = asyncio.create_task(backup.backup_database(Path(kernel.store.path), target))
    try:
        await asyncio.wait_for(started.wait(), 5)
        for _ in range(3):
            task.cancel()
            await asyncio.sleep(0)
        assert not task.done() and not target.exists()
    finally:
        finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not list(tmp_path.glob(".memkernel-backup-*")) and not target.exists()


async def test_backup_copy_deadline_cleans_output(kernel: MemKernel, tmp_path: Path, monkeypatch) -> None:
    from memkernel import backup

    ticks = iter([0, 31])
    monkeypatch.setattr(backup, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    target = tmp_path / "backup.db"
    with pytest.raises(TimeoutError, match="30 seconds"):
        await backup.backup_database(Path(kernel.store.path), target)
    assert not list(tmp_path.glob(".memkernel-backup-*")) and not target.exists()


@pytest.mark.parametrize("damage", ["schema", "foreign_key"])
async def test_backup_invalid_database_cleans_output(kernel: MemKernel, tmp_path: Path, damage: str) -> None:
    from memkernel.backup import backup_database

    with closing(sqlite3.connect(kernel.store.path)) as source:
        if damage == "schema":
            source.execute("PRAGMA user_version=999")
        else:
            source.execute("INSERT INTO notice_sources (notice_id, peer_id) VALUES (?, ?)", ("missing-notice", "peer"))
            source.commit()
    target = tmp_path / "backup.db"
    with pytest.raises(ValueError, match="schema|foreign-key"):
        await backup_database(Path(kernel.store.path), target)
    assert not list(tmp_path.glob(".memkernel-backup-*")) and not target.exists()
