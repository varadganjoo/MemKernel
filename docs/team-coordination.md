# Deploying coordination in a controlled team

MemKernel 0.3 adds signed notice exchange to local branchable agent memory. This
guide is for an organization operating its own endpoints and file transport.
It does not establish organizational compliance, SSO, FIPS validation or a
distributed lock. Begin with synthetic data and a limited pilot.

## Architecture and trust

Each participant has a **local** SQLite database and an Ed25519 signing key.
An operator pins peer public keys and the agent IDs each peer may publish.
The exchange folder contains one signed JSON snapshot per peer. Use an existing
organization-approved folder synchronization mechanism; MemKernel does not
provide or pay for that transport. Never put a live SQLite database, local
policy, private key or backup in this folder.

Signatures bind the workspace, peer, sequence, timestamp and complete notice
records. A peer can publish only its allowlisted authors. Agent identities must
be distinct across peers. Each database is bound to one workspace/local peer.
Once bound, reopening without a policy does not re-enable unsigned imports or
notice posting. Local processes with the signing key or direct database access
are trusted; stdio agent IDs are not separately authenticated OS principals.

The implementation uses [PyCA's Ed25519 API](https://cryptography.io/en/latest/hazmat/primitives/asymmetric/ed25519/).
It does not implement cryptographic primitives. Signed-format v1 fixes the
algorithm; an incompatible future algorithm would require a new format version.
Signatures authenticate a peer, not the truth or safety of its message. Notice
text is untrusted agent input and must never become an instruction to execute.

**Confidentiality is the operator's responsibility.** JSON is signed, not
encrypted. Restrict folder readers and use transport/storage encryption according
to your organization's data classification. Do not store secrets in notices.
Protect the local database, WAL/SHM files, policy, key and backups with OS access
controls and disk encryption. Initialization protects keys and policy files
before writing them: mode 0600 on POSIX, or a protected NTFS DACL allowing the
current account, SYSTEM and local Administrators on Windows. Startup validates
permissions and ownership on the opened file handles and refuses unrestricted
or untrusted access. Manually provisioned keys and policies must meet the same
rules. The containing directories must also prevent untrusted replacement;
the operator must verify those directories and filesystem support. A library
cannot isolate two agents that share unrestricted access to the same OS account.

## Set up two participants

Install in an isolated environment, then initialize a policy **outside** the
exchange folder. These paths are examples; use absolute paths on your machine.

```text
memkernel sync init --config /local/alice/sync.json --folder /synced/project-notices --workspace project-alpha --peer alice --agents alice-codex alice-claude
memkernel sync init --config /local/bob/sync.json --folder /synced/project-notices --workspace project-alpha --peer bob --agents bob-cursor
```

On separate laptops, each participant initializes only their own identity and
uses their own local path to the common transport. `init` prints the public key,
its SHA-256 fingerprint and allowed agent IDs. It refuses to overwrite an
existing configuration or key. Verify fingerprints over an independent trusted
channel before pinning. The private key stays with its owner.

```text
memkernel sync trust --config /local/alice/sync.json --peer bob --public-key <bob-public-key> --agents bob-cursor
memkernel sync trust --config /local/bob/sync.json --peer alice --public-key <alice-public-key> --agents alice-codex alice-claude
```

Set these variables in **each client's MemKernel server entry**, using a local
database path and one of the agent IDs pinned for that participant:

```json
{
  "MEMKERNEL_DB": "/local/alice/memory.db",
  "MEMKERNEL_AGENT_ID": "alice-codex",
  "MEMKERNEL_SYNC_CONFIG": "/local/alice/sync.json"
}
```

Restart that client's MCP connection. Alternatively launch directly:

```text
memkernel serve --db /local/alice/memory.db --agent-id alice-codex --sync-config /local/alice/sync.json
```

Configuration is read at startup. Its `interval_seconds` defaults to 10 and
`stale_after_seconds` to 300. Adjust them in the local JSON policy and restart
all processes using it. The server syncs on startup, after post/clear operations
and during polling. Multiple local servers sharing a database serialize their
publishes through SQLite. Synchronization never shares leases or branch history.

The Python API is `await kernel.enable_sync(config_path)`, followed by
`await kernel.sync.run_once()`. A Python harness must schedule its own polling
and cancel it before closing the kernel. The MCP server manages that lifecycle.
Read-only servers never start synchronization; observe an already populated
local database without passing a sync policy.

## Agent workflow and context

At task start and before potentially overlapping work, call
`memkernel_sync_now`, then inspect `memkernel_sync_status` and
`memkernel_query_state`. Check **errors and freshness**. A successful local
publish does not prove delivery, acknowledgement, or agreement by a peer.
The library cannot guarantee that another laptop received the latest file.

Post a notice for a logical scope such as `service:auth`, use local file leases
where appropriate, and clear your notice on completion. Expiry means **stale**,
not finished. Missing or untrusted notices never imply permission to edit.
`source_peer` records provenance; `trusted_source` indicates whether that source
is in the current local policy. Revocation leaves historical warnings visible
with `trusted_source=false`. Do not silently treat them as completed work.

By default, query/context reserves at most 1,000 estimated tokens for notices.
`notice_page` reports the filtered total, omitted count, next offset and whether
a larger budget is required. Use `notice_resource="service:auth"`,
`notice_offset`, and `notice_token_budget` to inspect more. Budgets accept 128
through 32,768 tokens. `memkernel_list_notices` additionally supports `offset`,
`limit` (1 through 100) and `max_tokens` (default 4,000). Pagination is a live
view; concurrent inserts/clears can move offsets, so recheck before acting.
`context_tokens` includes rendered notices; branch GC does not erase warnings.

Manual JSON export/import remains available for unmanaged databases. In signed
mode unsigned import is rejected. Legacy notices without provenance remain
visible but are **not automatically signed**. Review, clear and repost them with
an allowed local identity if they should be shared.

## Failures, audit and capacity

```text
memkernel sync run --config /local/alice/sync.json --db /local/alice/memory.db
memkernel sync status --config /local/alice/sync.json --db /local/alice/memory.db
memkernel sync audit --config /local/alice/sync.json --db /local/alice/memory.db --limit 100
```

`run` exits nonzero if a publish or peer import fails. Operations on different
peers are independent; a bad peer does not roll back other accepted peers. Each
peer's records, ownership metadata, audit event and replay cursor commit in one
transaction. Atomic file replacement prevents readers seeing half a publish.
If replacement succeeds but the SQLite commit is lost, the next publisher also
checks its own signed file to recover the sequence floor.

Malformed, oversized, symlinked or special files are rejected before use.
Snapshots have an 8 MiB UTF-8 and 10,000-record limit per peer. Policies permit at
most 128 peers and 64 agent IDs per peer. These are safety bounds, not validated
capacity promises. Cleared records are retained as tombstones so late snapshots
cannot resurrect them. Do not remove tombstones or reset sequence counters to
save space. Monitor board and database size; plan a coordinated new-workspace
cutover before reaching the snapshot limit. No automatic destructive retention
is performed. Keep the actively unresolved board small and clear finished work.

Common status/error codes:

| Signal | Operator action |
| --- | --- |
| `never_received`, `missing_file` | Confirm the peer has started, published and delivered a file. |
| `stale` | Contact the peer and check their process, clocks and transport. Re-reading the same file never refreshes liveness. |
| `invalid_signature`, `wrong_peer`, `wrong_workspace` | Stop relying on that transfer; verify the policy and file provenance independently. |
| `replayed_sequence`, `sequence_conflict` | Investigate transport rollback, duplicate local identities or a restored database. Do not reset the cursor. |
| `unauthorized_author`, `notice_conflict` | Inspect peer identity ownership or incompatible records; the complete peer batch was rejected. |
| `future_timestamp` | Correct clock synchronization; timestamps over five minutes ahead are rejected. |
| `unsafe_file`, `oversized_file`, `io_error` | Correct file type, capacity or filesystem access before retrying. |

Audit is local SQLite metadata: timestamp, peer, direction, outcome, safe reason
code, sequence and changed count. It excludes message bodies, signatures and
private keys. It is outside the branch hash chain and is not tamper-proof against
an OS/database administrator. Restrict readers, monitor rejected imports and
stale peers, and archive with the database according to your organization's
retention requirements. The library does not contact a SIEM or send alerts.

## Rotation, revocation and recovery

To revoke a peer locally, run `memkernel sync revoke --config <policy> --peer
<peer>`, then restart **every** local server using that policy. Coordinate
revocation on all other endpoints; there is no central revocation service.
Existing local records remain available for investigation.

For key rotation, stop the owner's writers, generate a new protected Ed25519
key, independently distribute its fingerprint, and use `sync trust --replace`
on every endpoint including the owner. Update the owner's `private_key` path,
preserve the database/sequence identity, and restart. Never share a private key
between participants or copy a database into a second active writer identity.
Retire the old key only after the cutover and recovery policy permit it.

Back up SQLite using its online backup API or stop every writer and copy the
complete checkpointed database. Copying a live `.db` alone can omit WAL data.
Back up policy and keys separately in protected organization-approved storage.
Test restoration to a new local path before switching the endpoint over.
After restoration, require current signed snapshots and fresh peer status
before resuming coordinated work. Restoring an old backup loses knowledge of
updates received after it; replay protection cannot recover a missing database
and simultaneously rolled-back transport without an independent checkpoint.

Owner responsibilities include endpoint access controls, transport encryption,
fingerprint verification, clocks, backups, incident response and retention.
Decide whether these boundaries satisfy your deployment before using sensitive
data. A signed advisory board provides neither guaranteed delivery nor mutual
exclusion between machines.
