# Back up local memory

The unreleased development version adds an operator-only command:

```bash
memkernel backup /protected/existing-directory/memory-2026-11-13.db
# Pin a source explicitly instead of project discovery / MEMKERNEL_DB:
memkernel backup /protected/existing-directory/copy.db --db /project/.memkernel/memkernel.db
```

Choose a new filename inside an existing directory you control. On Windows, use
an owner-controlled NTFS directory, for example under your user profile. The
command applies private file permissions before writing memory. The filesystem
must support private permissions and hard links; unsupported destinations fail.
An existing destination, symlink or destination SQLite sidecar is never replaced.
A source database sidecar cannot be used as the destination.

Agents may remain connected. The command opens the source read-only and uses
SQLite's online backup API to include committed WAL data. It checks SQLite
integrity, foreign keys and every branch's event hash chain before publishing a
standalone file. Backup copying stops after a 30-second deadline; if a busy or
large source exceeds it, retry when activity is lower. Verification time is
additional. Cancellation waits for preparation to close its SQLite handles.
Failed or cancelled work does not publish a partial final file.
Use the command's exit status; interrupted runs may leave private temporary files.

Do not copy only `memkernel.db` while agents are running. A plain copy can pass
SQLite's integrity check while missing committed records still in `memkernel.db-wal`.
Do not live-sync SQLite files between devices. See the
[discovery and reproducible experiment](reviews/weekly-2026-11-13.md) and
[SQLite's online backup documentation](https://sqlite.org/backup.html).

## What the copy contains

The backup contains **all local database contents**: every branch, raw event
history (including rolled-back events), snapshots, notices and clear tombstones,
leases, sync identity, sequence/replay state and audit records. It is not selective
sharing and is not encrypted. Protect it like the source; do not put it in a public
repository, shared folder or notice-exchange directory. Filesystem copies are not
an erasure mechanism.

External signing keys, trust-policy files, client configurations and code files
are not in the database and are not backed up by this command. Preserve those
separately under your existing private recovery procedure. A restored database
does not restore trust configuration or rotate keys. Never run a backup alongside
the original as an independently syncing peer with the same identity; design and
review identity recovery separately. Older backups can have old sync counters.

The backup represents a consistent committed state during copying, not every
write made after that point. Hash checks detect internal inconsistency, not the
truth of stored assertions, authenticity against a trusted external checkpoint,
or records omitted by an earlier bad copy. Keep independent recovery copies.

## Rehearse recovery without overwriting data

Inspect a backup as a separate, read-only database, without enabling sync:

```python
from memkernel import MemKernel

async with await MemKernel.open("/protected/copy.db", read_only=True, tokenizer="heuristic") as memory:
    for branch in await memory.store.list_branches():
        assert await memory.verify(branch.branch_id) == []
    state = await memory.query_state("main")
```

The acceptance tests reopen a backup separately and compare every table and
materialized branch state with the source, including rollback and compaction.
This patch supplies no overwrite/restore command. Before choosing a backup for
production recovery, stop original writers, preserve the current database and
its sidecars, review sync identity/counter handling, and get the required recovery
approval. Existing backups are never modified by `memkernel backup`.
