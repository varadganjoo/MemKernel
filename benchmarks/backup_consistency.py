"""Reproduce a healthy but incomplete plain copy versus online backup, offline.

Run from an installed checkout: python benchmarks/backup_consistency.py
All databases and copies contain synthetic data and are deleted after each run.
"""

from __future__ import annotations

import asyncio
import json
import platform
import shutil
import sqlite3
import sys
import tempfile
from contextlib import closing
from pathlib import Path

from memkernel import MemKernel


def inspect(path: Path) -> dict:
    with closing(sqlite3.connect(path)) as db:
        return {"integrity": db.execute("PRAGMA integrity_check").fetchone()[0],
                "events": db.execute("SELECT count(*) FROM events").fetchone()[0]}


async def main() -> None:
    runs = []
    for _ in range(5):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source, plain, online = (root / name for name in ("memory.db", "plain.db", "online.db"))
            async with await MemKernel.open(source, tokenizer="heuristic", auto_gc=False) as kernel:
                with closing(sqlite3.connect(source)) as connection:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
                for index in range(20):
                    await kernel.append_decision(branch_id="main", agent_id="owner", key=f"decision.{index}",
                                                 value="sqlite", rationale="offline")
                shutil.copyfile(source, plain)
                process = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", "memkernel.cli", "backup", str(online), "--db", str(source),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                _stdout, stderr = await process.communicate()
                assert process.returncode == 0, stderr.decode(errors="replace")
                async with await MemKernel.open(online, read_only=True, tokenizer="heuristic") as restored:
                    assert await restored.verify() == []
                    expected = (await kernel.query_state())["state"]
                    assert (await restored.query_state())["state"] == expected
                run = {"source_events": len(await kernel.store.read_stream("main")),
                       "plain_copy": inspect(plain), "online_backup": inspect(online)}
                assert run["plain_copy"] == {"integrity": "ok", "events": 0}
                assert run["online_backup"] == {"integrity": "ok", "events": 20}
                runs.append(run)
    print(json.dumps({"python": platform.python_version(), "platform": platform.system(),
                      "sqlite": sqlite3.sqlite_version, "mode": "WAL; agent connection held open",
                      "runs": runs, "plain_retained_events": sum(r["plain_copy"]["events"] for r in runs),
                      "online_retained_events": sum(r["online_backup"]["events"] for r in runs),
                      "expected_events": sum(r["source_events"] for r in runs)}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
