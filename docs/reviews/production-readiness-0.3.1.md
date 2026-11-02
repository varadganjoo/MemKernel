# MemKernel 0.3.1 review and improvement scorecard

Review started November 1, 2026 (America/Denver). Baseline: GitHub main
`82f5f32` and the installed PyPI release `memkernel-mcp==0.3.0`.

MemKernel is a local memory library and stdio MCP server for cooperating agents.
This review assumes multiple processes on one host, one local SQLite database per
project, and optional signed notices exchanged between hosts. It does not assume
a public service, authenticated local agent identities, or distributed file locks.

## Measured improvements

| Measure | 0.3.0 | 0.3.1 | Meaning |
|---|---:|---:|---|
| Passing tests in the normal suite | 159 | 186 | 27 more cases, a 17.0% increase |
| Failures in the new regression suite | 26 / 27 | 0 / 27 | The same cases were run against both installed implementations |
| Passing controls in that regression suite | 1 | 27 | Includes real independent MCP sessions |
| Python minor versions exercised by CI | 3.11, 3.14 | 3.11, 3.12, 3.13, 3.14 | Two additional Linux matrix entries |
| CI jobs, including dependency and wheel jobs | 9 | 11 | Both MCP SDK generations remain covered |
| New runtime dependencies | — | 0 | Reuses the existing atomic writer and standard library |

Final local coverage is **91.1% of statements** and **83.0% of branches**; the
combined coverage measure is 89.4%. This is coverage of the Python library, not
proof that every possible failure is tested. Baseline coverage was not measured
in a comparable successful run, so no coverage improvement is claimed.

### Performance: preserved token savings, mixed timing results

Both installed wheels used Python 3.11.0, the same dependencies, `cl100k_base`,
the same benchmark script and synthetic data. Three runs per version alternated
order; each latency run used 15 repetitions. Values below are medians of run
medians. This workstation was not a controlled device-performance environment.

| Metric | 0.3.0 | 0.3.1 | Observed change |
|---|---:|---:|---|
| Warm context read, 5,000 events | 1.34 ms | 1.63 ms | 21.3% slower in this sample |
| Cold open and recall, 5,000 events | 22.92 ms | 19.28 ms | 15.9% faster in this sample |
| Durable single-writer appends | 659 / s | 634 / s | 3.7% lower throughput |
| Bulk load, 5,000 events | 1,081 / s | 1,079 / s | Approximately unchanged |
| Context reduction: single-agent workflow | 88.6% | 88.6% | Preserved |
| Context reduction: multi-agent workflow | 93.0% | 93.0% | Preserved |
| Context reduction: lint-heavy workflow | 91.8% | 91.8% | Preserved |
| State-equivalence checks | All pass | All pass | No loss of durable state |

The patch is a correctness and reliability improvement, not a claimed speedup.
Appends now revalidate inputs; snapshots additionally replay and compare covered
state. Those checks add work. Timing varied materially across runs, including
unchanged read paths, so these percentages are observations rather than causal
claims or device latency promises. Raw runs and the method are in
[performance-0.3.1.json](performance-0.3.1.json).

### Offline budgeting now fails conservatively

The previous fallback undercounted inputs that occur in real code and multilingual
memory. The new fallback uses UTF-8 bytes as an upper bound for byte-level BPE.

| Input | Old estimate | Actual cl100k tokens | New upper bound |
|---|---:|---:|---:|
| 100 CJK characters | 25 | 100 | 300 |
| 100 emoji | 100 | 200 | 400 |
| `qzjx` repeated 100 times | 100 | 400 | 400 |
| Space/newline/tab repeated 100 times | 0 | 101 | 300 |

The bound can compact sooner and show fewer notices. Exact tiktoken counting is
unchanged. Durable state can still exceed the policy budget; preservation takes
precedence over deletion. `context_tokens` counts rendered memory, not the full
JSON MCP response, tool schemas, or the whole model conversation.

## Verified defects fixed

These are confirmed failure cases, not speculative scanner alerts. Severity is
relative to the documented local/cooperative trust boundary.

| Priority | Failure and consequence | Smallest complete repair | Evidence |
|---|---|---|---|
| Must fix | A schema-valid snapshot could erase decisions, replace their values, or forge provenance while the hash chain still verified. | Compare snapshot core state against replay of the covered live prefix before appending. Summarizer insights remain separate. | Three negative cases plus a valid snapshot control in `test_regressions.py`; existing repeated-GC/rollback tests |
| Must fix | Frozen envelopes contain mutable dictionaries. Mutation after construction bypassed the payload/metadata byte cap. NaN and infinities also entered nominal JSON. | Revalidate and detach at the persistence boundary; reject new non-finite JSON values. Preserve the historical hash serializer. | Two oversized-input cases and three non-finite cases |
| Must fix | A failed commit, or cancellation after BEGIN, left a transaction open; later operations failed until restart. | Include BEGIN/body/COMMIT in one failure path and queue a rollback on error. | Real deferred foreign-key commit failure and deterministic startup cancellation |
| Must fix | Tokenizer initialization errors left an open database worker. | Keep all post-open initialization inside the existing close-on-failure block. | Tracked connection is closed after invalid tokenizer input |
| Must fix | Background heartbeat exceptions left `lost=False`, skipped release, and replaced the caller's work exception during cleanup. | Mark the handle lost, log a bounded message, and let normal cleanup release it. | Injected storage failures verify loss, release, and preserved work exception |
| Must fix | After forking, default MCP recall and compaction still selected main. An agent could act on the wrong memory. | Default omitted branch arguments to the session's active branch. Explicit main still works. | Direct tool case and two actual stdio sessions |
| Must fix | Offline fallback ignored whitespace and undercounted multilingual/source-like text. | Replace the regex estimate with the byte bound above. | Four input classes and existing budget/notice-pagination tests |
| Must fix | Reattaching discarded read-only flags, sync policy and Codex approval preferences. | Preserve supported settings; reject custom configurations that cannot be safely rewritten. Compare the entire parsed TOML document after the merge. | JSON/TOML preservation, repeat-attach idempotence, custom-setting rejection |
| Must fix | Relative attached DB paths depended on each client's working directory; writable `~` paths created a parent in one location and opened another. Invalid IDs were written before server startup rejected them. | Resolve attached DB paths once; expand store home paths; validate IDs before writing files. | Path, writable/read-only reopen and no-write-on-invalid-ID cases |
| Should fix | TOML serialization escaped emoji as invalid surrogate escapes. Config failures left predictable temporary files behind. | Preserve Unicode in TOML and reuse the existing unique-temp, flushed atomic writer for configs and backups. | Unicode round trip and injected replace failure preserving the original |

Relevant implementation paths: `memkernel/core/store.py`, `core/events.py`,
`kernel.py`, `clients/adapter.py`, `memory/token_counter.py`, `server/mcp_server.py`,
and `cli.py`. All public entry points and connected callers were searched before
changes. No event rows are migrated, deleted, or rewritten by this patch.

## Acceptance and release evidence

Local checks apply to the 0.3.1 source candidate and its installed wheel. Remote
CI and publication must be verified separately against their actual revisions.

| Gate | Status | Evidence and limits |
|---|---|---|
| Python 3.11 / MCP 2.3.0 | PASS | 186 passed, 3 skipped; real SQLite and MCP subprocesses |
| Python 3.14.10 / MCP 1.30.0 | PASS | 186 passed, 3 skipped |
| New regression suite against published 0.3.0 | Confirmed baseline failures | 26 failed, 1 passed; fails for the expected behaviors |
| Syntax/import checks | PASS | Ruff 0.17.3, isolated `E9,F`; broader style rewrites deliberately excluded |
| Examples | PASS | All four examples, including three-process collaboration and signed-folder exchange |
| Wheel and source archive | PASS | Build and `twine check --strict` |
| Clean installed wheel | PASS | Import confirmed from site-packages; 23 MCP tools; three independent sessions; recall after restart, contention, foreign heartbeat rejection, branch isolation and read-only recall |
| 0.3.0 upgrade | PASS | Two branches; 31 lineage event entries compared exactly, including inherited entries; state, notices and leases preserved; post-upgrade write and hash verification succeed |
| Dependency advisories | PASS at scan time | pip-audit 2.10.1, online advisory queries; 86 resolved packages in the MCP 2.x audit environment and 43 in MCP 1.x; no known advisories. The first includes development/audit tools. This does not audit the interpreter. |
| Static security review | PASS with triage | Bandit 1.9.4: 0 high, 7 medium and 3 low alerts. The SQL alerts interpolate constant column lists/generated placeholder clauses; inputs remain parameterized. Assert alerts cover internal invariants, not authorization. No confirmed exploitable alert remains. |
| Credential-pattern audit | PASS within pattern scope | IBM helper plus high-confidence key/PEM patterns over tracked files and Git patch history; no matches. No dedicated entropy scanner or credential validation was performed. |
| Read-only/isolation/signed-notice controls | PASS | Existing negative tests cover every mutator, SQLite aliases, foreign holders, tampering, replay, wrong workspaces, unauthorized authors and revocation. Local IDs remain self-asserted. |
| Cross-platform CI and PyPI publication | Separate release gate | See the PR, CI runs and GitHub release linked from the release notes; local results alone do not prove publication. |
| GUI accessibility, browser journeys, web auth | N/A | This patch adds no GUI or network server. CLI/MCP behavior is tested. |
| Hosted multi-user operation, mobile and prolonged device soak | Not verified | Outside the existing deployment model; not represented as supported by this release |

Three skips are the opt-in paid-model test and two POSIX-only filesystem cases on
Windows. Windows ownership/ACL cases ran on the normal NTFS temporary directory.
The workspace drive lacks the required ownership semantics; using it for those
fixtures caused failures and was not treated as a passing security check.

## What actual use taught us

The reviewer used the connected MemKernel to recall a previous branding decision,
record this task and save the review's scope decision. That is useful cross-session
recall of a specific source, rather than a guessed recollection. The installed
candidate was also exercised through real stdio clients, not only direct functions.
These sessions did not call a paid model or certify Claude/ChatGPT UI integration.

The biggest friction is deciding what to retrieve and receiving duplicate verbose
state. The agent must manage many low-level calls, remember its branch and identity,
and interpret budgets, notices and provenance itself. The next work should reduce
that burden while keeping the event log and trust boundary explicit.

See the [prioritized capability roadmap](../roadmap.md) for proposed work. None of
those unimplemented capabilities is included in this patch's improvement counts.

## Procedures and exclusions

Applied enterprise-app-dev routing/release evidence, Ponytail implementation and
audit/review, Superpowers systematic debugging and verification, OWASP code review,
MCP review, secret-pattern and dependency audits, and the IBM five-pillar workflow.
Regression cases were demonstrated failing before fixes and passing afterward.
Spellbook Python/API/testing guidance informed async cleanup and public-contract
checks. Minimalist review constrained the roadmap to immediate user friction.

Anthropic webapp-testing, frontend frameworks, REST authentication, cloud IaC and
commercial workflows were excluded because no corresponding surface was changed.
There is no independent external security certification, full pen test, dedicated
entropy scan, paid-model evaluation or performance guarantee. The current trust
boundary and retained-history privacy limits remain documented in SECURITY.md.
