# Changelog

## Unreleased

- Add `memkernel_lookup_memory` and Python `lookup_memory` for exact-key recall of
  one current decision or fact with its author and source event. Reads follow
  active branches, compaction and rollback, and work in read-only sessions.
  Startup and coordination checks still require `query_state`/notice tools.

## 0.3.1

- Reject snapshots that erase or change covered state or its provenance.
- Revalidate mutable event dictionaries at persistence; reject new events containing
  NaN or infinite numbers. Existing event serialization and hash chains are unchanged.
- Recover the connection after a failed commit or cancellation during transaction startup.
- Close the database when kernel initialization fails, including tokenizer configuration.
- Mark a lease lost when its background heartbeat fails; keep cleanup and work errors intact.
- Make MCP recall and explicit compaction default to the session's active branch.
  Pass `branch_id="main"` or `target_branch="main"` to select main explicitly.
- Use a conservative UTF-8 byte bound when tokenization is offline. This may compact
  sooner and return fewer notices; exact tiktoken counting is unchanged.
- Preserve read-only flags, sync policy, and approval preferences when reattaching.
  Refuse unsupported custom configurations instead of discarding settings.
- Resolve attached relative database paths once, validate identities before writes,
  support home-directory database paths, and preserve Unicode in TOML configuration.
- Reuse flushed, unique temporary files for atomic configuration writes and backups.

No database migration is required. Restart MCP servers after upgrading.

## 0.3.0

Signed coordination notices over an operator-managed folder, peer trust configuration,
provenance, freshness diagnostics, and bounded notice context. See the
[release notes](https://github.com/varadganjoo/MemKernel/releases/tag/v0.3.0).
