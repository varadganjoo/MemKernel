"""Operator-only online backup into a new, private local database file."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path

from memkernel.core.store import SCHEMA_VERSION, AsyncEventStore
from memkernel.private_files import protect_private_file

_SIDECARS = ("-wal", "-shm", "-journal")


def _copy_database(source: Path, temporary: Path) -> None:
    deadline = time.monotonic() + 30

    def progress(_status: int, _remaining: int, _total: int) -> None:
        if time.monotonic() > deadline:
            raise TimeoutError("backup exceeded 30 seconds; retry when database activity is lower")

    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as reader:
        if reader.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
            raise ValueError("backup requires an initialized database with the supported schema")
        with closing(sqlite3.connect(temporary)) as writer:
            reader.backup(writer, pages=128, progress=progress, sleep=0.05)
            if writer.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise ValueError("backup failed SQLite integrity check")
            if writer.execute("PRAGMA foreign_key_check").fetchall():
                raise ValueError("backup failed foreign-key integrity check")
            writer.execute("PRAGMA journal_mode=DELETE").fetchall()


async def _prepare_backup(source: Path, temporary: Path) -> int:
    await asyncio.to_thread(_copy_database, source, temporary)
    async with AsyncEventStore(temporary, read_only=True) as store:
        branches = await store.list_branches()
        for branch in branches:
            problems = await store.verify_branch(branch.branch_id)
            if problems:
                raise ValueError(f"backup failed hash integrity for branch {branch.branch_id!r}: {problems[0]}")
    return len(branches)


async def backup_database(source: Path, destination: Path) -> dict:
    """Copy committed SQLite state, verify all branch hashes, never replace a file.

    Includes all local history and coordination/sync state, not external keys or
    policy files. Protect the destination directory; the backup is not encrypted.
    """
    source = source.expanduser().resolve(strict=True)
    destination = destination.expanduser().absolute()
    if destination.resolve() in {Path(str(source) + suffix) for suffix in _SIDECARS}:
        raise ValueError("backup destination cannot be a source database sidecar")
    if any(path.exists() or path.is_symlink() for path in
           (destination, *(Path(str(destination) + suffix) for suffix in _SIDECARS))):
        raise FileExistsError("backup destination or sidecar already exists; choose a new file")
    fd, name = tempfile.mkstemp(prefix=".memkernel-backup-", suffix=".db", dir=destination.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb"):
            protect_private_file(temporary)
        prepare = asyncio.create_task(_prepare_backup(source, temporary))
        try:
            branches = await asyncio.shield(prepare)
        except asyncio.CancelledError:
            # Every SQLite handle must close before cleanup can remove its file.
            drain = asyncio.gather(prepare, return_exceptions=True)
            while not drain.done():
                try:
                    await asyncio.shield(drain)
                except asyncio.CancelledError:
                    pass
            raise
        with temporary.open("r+b") as stream:
            os.fsync(stream.fileno())
        os.link(temporary, destination)  # Atomic publication without overwriting a racing creator.
        if os.name != "nt":
            directory = os.open(destination.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        return {"source": str(source), "destination": str(destination), "branches": branches}
    finally:
        temporary.unlink(missing_ok=True)
