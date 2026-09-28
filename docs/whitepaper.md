# MemKernel: An Event-Sourced Virtual Operating System Kernel for Multi-Agent State Synchronization and Deterministic Replayability

**Varad Ganjoo**

*Technical report, version 0.1.0 (September 2026). The reference implementation, tests and benchmark scripts are in this repository.*

---

## Abstract

Autonomous coding agents such as Cursor, Claude Code and Codex CLI increasingly work on the
same repository at the same time. Each keeps a private, append-only chat transcript as its
memory. This architecture fragments context: agents cannot see each other's decisions,
cannot retract a disproven assumption except by appending contradictory text, cannot
coordinate edits to shared files, and watch their context windows fill with diagnostic
noise. We call the resulting failure modes *multi-agent context fragmentation* and
*state corruption*. MemKernel is a memory kernel that treats agent memory as an operating
system resource. Every state change is an immutable, hash-chained event in a SQLite
write-ahead log. Workspace state is a deterministic left fold over the *live* timeline of
a branch. Branches share history copy-on-write, and rollbacks are compensating events, so
exploration is cheap and every timeline jump is auditable. A token-budgeted garbage
collector folds ephemeral tool output into semantic snapshots. We prove that these
snapshots preserve the materialized state exactly, and they carry anti-thrash guarantees.
Leases with monotonically increasing fencing tokens serialize edits to shared resources
across heterogeneous agent processes. The kernel is exposed as a Model Context Protocol
(MCP) server over stdio, so unmodified IDE agents interoperate through one database file.
On three deterministic 50-turn workflows, MemKernel reduces the context an agent must load
by 88.6–93.0% relative to raw history, and the reduced context is verified to yield
identical workspace state. On a 50,000-event log, a cold process recovers its context in about
21 ms on Windows 11 and 5 ms on Linux, and a warm read takes 1–2 ms on both. We validated
interoperability live, with Claude Code, Codex CLI and Antigravity coordinating through a
shared MemKernel instance.

---

## 1. Introduction

The unit of software automation is shifting from a single assistant to a *swarm*: an IDE
agent refactoring a module, a terminal agent running the test suite, a review agent reading
diffs, and bespoke harnesses in CI. These agents come from different vendors, run in
different processes and speak to different models. They share exactly one thing, the
repository. The knowledge that makes them effective is rarely in the repository: *why*
the team chose OAuth2 PKCE, *which* test is failing and why, *who* is editing
`src/auth/login.py` right now.

Today that knowledge lives in two places, and neither works for a swarm.

**Chat-history memory** appends every turn to a per-agent transcript. It is (i)
*siloed*: Cursor cannot read Claude Code's transcript. It is (ii) *monotone*: an
overruled assumption stays in the transcript and competes with its correction for the
model's attention. Long contexts are known to be used unevenly [11]. It is (iii)
*unbounded*: tool output dominates growth, and agents compensate with ad-hoc truncation
that silently drops decisions. It is (iv) *unbranchable*: a failed experiment cannot be
cleanly undone, only argued against in subsequent turns.

**Vector-only retrieval (RAG)** [9] fixes (i) and (iii) but not (ii) or (iv), and it
introduces new problems. Similarity is not validity: a superseded decision embeds just as
closely to the query as its replacement. Retrieval has no notion of *order* or
*causality*, so it cannot answer "what is the current value of X?". It also offers no
*coordination primitive*: nothing in a vector store stops two agents from rewriting the
same file.

Systems like MemGPT [10] and generative-agent memory streams [12] show that LLM agents
benefit from OS-style memory management within a *single* agent. MemKernel targets the
multi-agent, multi-vendor setting and borrows its abstractions from operating systems and
databases rather than from retrieval:

1. **An event-sourced log as the single source of truth** [3]. State is derived, never
   stored authoritatively. This makes it replayable, auditable and deterministic.
2. **Git-like branches with copy-on-write lineage and compensating rollbacks**, which make
   speculative work and recovery from hallucinations first-class, cheap operations.
3. **A token-budget garbage collector** that compacts ephemeral content into semantic
   snapshots, with a soundness theorem and a bound on compaction frequency.
4. **Leases with fencing tokens** [1, 2] enforced at the storage layer, for safe
   concurrent editing by heterogeneous agents.
5. **Protocol-level interoperability** through MCP [13], so existing IDE agents
   participate without modification.

**Contributions.** (a) A formal lineage model with live-timeline semantics for branch,
fork and rollback (§2). (b) A snapshot-based compaction algorithm, a proof that it
preserves materialized state, and a proof that its policy bounds snapshot frequency (§3).
(c) A cross-process concurrency design combining optimistic compare-and-append with
fenced leases (§4). (d) An open-source implementation with an empirical evaluation (§5)
and a threat model (§6).

---

## 2. Formal Architecture & Lineage Model

### 2.1 Events and the log

An event is a tuple

$$e = (\iota, \sigma, \tau, b, a, \pi, \kappa, P, M, h)$$

where $\iota \in \mathbb{N}^+$ is a globally unique, strictly increasing event id; $\sigma$
is the event's sequence number within its lineage (§2.3); $\tau$ is a UTC timestamp
assigned by the store; $b$ is the branch the event was appended to; $a$ is the agent
identity; $\pi$ is the lineage predecessor's id ($\bot$ for the first event); $\kappa$ is
the event type; $P$ and $M$ are JSON payload and metadata; and $h$ is the chain hash

$$h(e) = \mathrm{SHA256}\big(h(\text{pred}(e)) \,\|\, \mathrm{canon}(\iota, \sigma, \tau, b, a, \pi, \kappa, P, M)\big), \qquad h(\bot) = 0^{256}.$$

Here $\mathrm{canon}$ is deterministic JSON (sorted keys, no insignificant whitespace), in
the spirit of hash-linked timestamping [14]. The type set is

$$K = \{\textsf{TASK\_INIT}, \textsf{DECISION}, \textsf{CLAIM\_ACQ}, \textsf{CLAIM\_REL}, \textsf{TOOL\_LOG}, \textsf{SCRATCHPAD}, \textsf{SNAPSHOT}, \textsf{FORKED}, \textsf{ROLLBACK}\}.$$

$K_{\text{eph}} = \{\textsf{TOOL\_LOG}, \textsf{SCRATCHPAD}\}$ are *ephemeral* and
$K_{\text{ctl}} = \{\textsf{FORKED}, \textsf{ROLLBACK}\}$ are *control* events, which only
the kernel may issue. The physical log is append-only: database triggers abort every
`UPDATE` or `DELETE` on the event table.

### 2.2 State transitions

Let $\mathcal{S}$ be the space of workspace states. A state $S \in \mathcal{S}$ is a record
of tasks, decisions, active workspace facts, deprecated assumptions, execution blockers,
file claims, an execution digest and insights. Each event type defines a transition
$\delta_\kappa : \mathcal{S} \times E \to \mathcal{S}$, and $\delta$ is the identity for
control and snapshot events. The state after events $E_1, \dots, E_t$ is the left fold

$$S_t \;=\; f(S_0, E_1 \ldots E_t) \;=\; \delta(\cdots\delta(\delta(S_0, E_1), E_2)\cdots, E_t),$$

which we abbreviate $S_t = f(S_0, \sum_{i \le t} E_i)$, reading $\sum$ as ordered
composition rather than commutative summation. The transitions are:

- $\delta_{\textsf{DECISION}}$ sets `decisions[k] := (v, rationale, a, ι)`. If a
  different value was present, it appends `(decision, k, v_old → v)` to the deprecated
  assumptions. This is last-writer-wins *within a branch*, with the loss made explicit.
- $\delta_{\textsf{TOOL\_LOG}}$ merges reported `facts`, recording each changed value as a
  deprecated assumption. It updates the digest entry for the command. A nonzero exit
  code opens or refreshes a blocker keyed by the command, holding the command's most
  diagnostic output line. A zero exit clears that blocker.
- $\delta_{\textsf{CLAIM\_ACQ}}$ and $\delta_{\textsf{CLAIM\_REL}}$ maintain the claim table,
  matched by fencing token.
- $\delta_{\textsf{TASK\_INIT}}$ records the agent's directive and constraints.

Each $\delta$ is a pure function of $(S, e)$: it reads no clock, randomness or external
state. The deprecated list, the digest and the blocker table have fixed capacities. When a
capacity is exceeded, the entry with the smallest event id is evicted, which is a
deterministic rule. **Lemma 1 (Determinism).** For any event sequence, $f$ yields the same
state on every replica and every replay. *Proof.* $f$ is a composition of pure functions
over a totally ordered input. ∎

### 2.3 Branches and copy-on-write lineage

A branch is a triple $B = (\text{parent}, \varphi, \text{head})$, where $\varphi$ is the fork
point (an event id, or $\bot$). The *raw lineage* of a branch is

$$L(B) \;=\; L(\text{parent})\big|_{\iota \le \varphi} \;\Vert\; \mathrm{Own}(B), \qquad L(\text{root}) = \mathrm{Own}(\text{root}).$$

$\mathrm{Own}(B)$ is the set of events physically appended to $B$, and
$\cdot|_{\iota\le\varphi}$ restricts to ids at most $\varphi$. Because ids increase
globally and a branch's own events are appended after its creation, every own event has an
id greater than $\varphi$. Concatenation therefore coincides with ordering by $\iota$, and
the implementation evaluates $L(B)$ as a single indexed SQL query over the branch's
ancestor chain, whose upper bounds are the minimum fork points along the chain. The model deliberately mirrors Git's commit graph [6], with events in place of commits. **Forking
copies no rows**: it inserts one branch record and one `FORKED` audit event.

**Lemma 2 (Sequence = position).** Every event $e \in L(B)$ satisfies
$\sigma(e) = $ position of $e$ in $L(B)$. *Proof.* By induction on branch creation. A new
branch starts with $\text{head\_sequence} = \sigma(\varphi)$, and each append assigns
$\text{head\_sequence}+1$. The inherited prefix of the parent lineage keeps its positions
by the induction hypothesis. ∎

Consequently, $\sigma$ acts as a per-lineage logical clock in Lamport's sense [5], and the pair
$(\text{fork point}, \sigma)$ locates any event in any branch in $O(1)$.

### 2.4 Rollback and the live timeline

A rollback never deletes. It appends a compensating event $r$ with target $t$. The *live
timeline* is the fold

$$\mathrm{live}(\epsilon) = \epsilon, \qquad \mathrm{live}(L \Vert e) = \begin{cases} \mathrm{prefix}_t(\mathrm{live}(L)) \Vert r & e = r \text{ is a rollback to } t \\ \mathrm{live}(L) \Vert e & \text{otherwise,}\end{cases}$$

where $\mathrm{prefix}_t$ truncates after $t$. The store only accepts rollback targets that
are live at append time, so the fold is total. The state of a branch is
$S(B) = f(S_0, \mathrm{live}(L(B)))$.

**Proposition 1 (Exact restoration).** If $r$ rolls $B$ back to $t$, then immediately
after $r$ we have $S(B) = f(S_0, \mathrm{prefix}_t(\mathrm{live}(L)))$, which is precisely
the state observed when $t$ was the head. *Proof.* By the fold definition, together with
$\delta_{\textsf{ROLLBACK}} = \mathrm{id}$. ∎

Rollbacks compose. A later rollback to an earlier target also truncates earlier rollback
events, so rollbacks nest correctly. The rollbacks and their targets form a **rollback
tree** over the raw lineage. A reverted event is not live, so it cannot be a rollback
target, but it can be **resurrected by forking from it**. The child's lineage ends at that
event and does not contain the rollback that reverted it. We use this for post-mortems of
failed experiments.

### 2.5 Diff and merge base

For branches $A$ and $B$, the merge base is the last event of the longest common prefix of
$L(A)$ and $L(B)$. The divergence report lists the live events with sequence beyond the
merge base on each side, the rollbacks since the base, and a key-by-key comparison of
$S(A)$ and $S(B)$ covering decisions and facts. MemKernel intentionally performs no
automatic merge. A conflicting decision is a semantic disagreement, and its resolution is
itself a decision that should be recorded with a rationale.

### 2.6 Integrity

`verify(B)` recomputes the chain over $L(B)$ and checks sequence contiguity, predecessor
pointers and the stored head hash. Modifying any event invalidates its hash, and rewriting
that hash breaks its successor's link. Rewriting an entire suffix is detectable only
against an externally anchored head hash (§6).

---

## 3. Autonomous Semantic Memory Compaction

### 3.1 Context windows

Let $S^\star$ be the latest live snapshot of a branch. Its payload is a serialization of
$f$ at its *coverage point* $c$ (an event id and sequence). The **context window** is

$$C(B) = \mathrm{render}(S^\star) \,\Vert\, \mathrm{render}(\Delta), \qquad \Delta = \{\, e \in \mathrm{live}(L(B)) : \sigma(e) > \sigma(c),\ \kappa(e) \ne \textsf{SNAPSHOT} \,\}.$$

$C(B)$ is exactly what an agent loads as memory, and $|C(B)|_{\text{tok}}$ is its token
cost under a tokenizer (`cl100k_base`, with a conservative regex fallback). $\Delta$ may
contain events positioned *before* $S^\star$ in the log. Those are the raw tail the
collector chose to retain.

### 3.2 Soundness

**Theorem 1 (Snapshot soundness).** Let the live timeline be $E_1 \ldots E_n$ with
snapshots removed, and let $S^\star$ cover $E_1 \ldots E_c$. Write $\llbracket
S^\star\rrbracket$ for the state decoded from its payload. Then

$$f(\llbracket S^\star\rrbracket, E_{c+1} \ldots E_n) \;=\; f(S_0, E_1 \ldots E_n).$$

*Proof.* The collector constructs $S^\star$ as $f(\llbracket S^\star_{\text{prev}}\rrbracket, E_{c'+1}\ldots E_c)$,
where $S^\star_{\text{prev}}$ is the previous snapshot with coverage $c'$ (or $S_0$).
Decoding inverts encoding exactly, since the state is a validated JSON model:
$\llbracket\mathrm{enc}(S)\rrbracket = S$. The claim follows by induction on the number of
snapshots, using the left-fold identity $f(f(S, A), B) = f(S, A \Vert B)$. Two subtleties
are handled by construction. First, eviction orders are keyed on event ids, never on
dictionary order, so they survive the sorted-key encoding. Second, snapshot `insights`
are excluded from the equality because no fold of raw events can produce them (§3.5). ∎

The test suite checks Theorem 1 directly: after repeated compactions, rollbacks and forks,
the state materialized through snapshots equals a replay of the full history. §5 reports
the same check on every benchmark workload.

A rollback that truncates the live timeline before $S^\star$ removes $S^\star$ from the
live timeline. The latest *surviving* snapshot then becomes $S^\star$, and Theorem 1
applies to it unchanged. The store maintains a per-branch **snapshot pointer**. It is set
when a snapshot is appended, recomputed on rollback, and inherited or recomputed on fork.

### 3.3 Where the tokens go: entropy reduction

Tool output is voluminous and highly redundant, but decision-relevant information in it is
sparse: *did it pass, what failed, what changed in the environment*. The fold is lossless
on durable content (decisions with rationale and provenance, facts, tasks, claims) and
deliberately lossy on ephemeral content. It projects each command's output stream onto a
digest entry $(\text{runs}, \text{failures}, \text{last exit})$ and a blocker entry holding
the single most diagnostic line. The line is chosen by priority: a test-runner
`FAILED`/`ERROR` line, then an exception line, then the last error-bearing line, then the
last line. Superseded information is not dropped silently. It moves to the *deprecated
assumptions* section, so an agent can tell that `python_version = 3.13` was once believed
and later overruled. This addresses failure mode (ii) of chat histories. Snapshots are
rendered to agents as dense, line-oriented text (`DECISION k = v | rationale (agent #id)`),
not as JSON, while the JSON payload remains the source of truth.

### 3.4 Collection policy and the anti-thrash bound

The policy has three parameters: a high-water mark $T$ (default 4000 tokens), a tail
bound of $k$ events and $T_{\text{low}}$ tokens (defaults $4$ and $T/2$), and a minimum
gain $g$ (default $T/4$).

**Algorithm 1 (Collect).**

```
collect(B, force):
  (S*, Δ, head) ← context_events(B)          -- O(|Δ|) fast path, §3.6
  before ← tok(render(S*) ‖ render(Δ))
  if ¬force ∧ before ≤ T: return below_threshold
  tail ← longest suffix of Δ with |tail| ≤ k and tok(tail) ≤ T_low
  X ← Δ minus tail;  if X = ∅: return nothing_to_compact
  S ← f(⟦S*⟧, X);  S.insights ← summarizer(S, X)
  S*' ← SNAPSHOT(S, covers = last(X), base = S*)
  after ← tok(render(S*') ‖ render(tail))
  if after ≥ before ∨ (¬force ∧ before − after < g): return no_gain
  append S*' with precondition parent = head  -- compare-and-append, §4.3
  on StaleHead: retry (bounded)
```

**Proposition 2 (Bounded compaction frequency).** Over any execution, the number of
automatic compactions is at most $(|C_0| + A)/g$, where $A$ is the total token cost of
appended events and $|C_0|$ is the initial context size. *Proof.* The context grows only
through appends, which add their own tokens (a new snapshot's rendering is already counted
in *after*). Every automatic compaction shrinks the context by at least $g$, and the
context size is non-negative, so the total shrinkage is at most $|C_0| + A$. ∎

With $g = T/4$, the collector writes at most one snapshot per $T/4$ tokens of new
material. The low-water tail bound makes the gain condition easy to satisfy: after
compaction the raw tail costs at most $T/2$. Without these two rules, a tail sitting near
$T$, or durable state approaching $T$, makes naive threshold GC fire on *every* append.
We observed exactly that in an early version (§5.2).

### 3.5 Summarizers

The snapshot's `insights` section is filled by a pluggable summarizer. The default
heuristic summarizer is deterministic: it carries prior insights forward and keeps the
last few scratchpad notes, which would otherwise disappear on compaction. `LLMSummarizer`
adapts any async completion function and asks for `{"insights": [...]}`. The prompt
labels event content as untrusted data. The output is parsed defensively, stripped of
control characters and capped at 300 characters per item and 12 items in total. It is
*confined to `insights`*: an LLM can never write facts, decisions or blockers, which stay
a pure function of the log. On any failure (timeout, malformed JSON, refusal) the
collector falls back to the heuristic summarizer. Theorem 1 therefore holds with any
summarizer.

### 3.6 Reading in $O(|\Delta|)$

When the snapshot pointer is set and no rollback occurs after its coverage point, the
store reads only the lineage suffix with $\iota > \iota(c)$. Lemma 2 implies that $\sigma$
and $\iota$ are co-monotone along a lineage, so this read is an index range scan on
`(branch_id, event_id)`. A rollback in the suffix forces the full fold. The next
compaction whose coverage point lies past that rollback restores the fast path. Tests
check that the fast and slow paths agree after forks and rollbacks.

---

## 4. Concurrency & Multi-Agent Consistency

### 4.1 Process model

Each IDE agent launches its own MemKernel MCP server as a stdio subprocess, for example
Cursor as `cursor-ide`, Claude Code as `claude-code-cli` and Codex CLI as `codex-cli`.
The servers share nothing except the database file. Every mutation runs in a SQLite
`BEGIN IMMEDIATE` transaction, which acquires the database's single RESERVED lock up
front, so all writes, from all processes, are serializable and totally ordered. WAL mode [4]
lets readers proceed concurrently with the writer against a consistent snapshot. Inside a
process, one connection is guarded by an async mutex, so no coroutine can observe another
coroutine's uncommitted writes.

### 4.2 Leases and fencing tokens

A lease on a resource $\rho$ (a normalized file path: `./src\a.py` and `src/a.py` name the
same resource) is a record $(\text{holder}, \theta, \text{ttl}, \text{expires})$, following
Gray and Cheriton [1]. Acquisition succeeds iff no unexpired lease exists, and it issues
fencing token $\theta' = \theta + 1$. The row is never deleted, so the token counter is
monotone forever. A heartbeat on an unexpired lease resets its expiry to *now + TTL*. A release succeeds only
for the holder presenting the current token.

A lease alone cannot prevent the classic failure described by Kleppmann [2]: an agent
pauses (a long model call, a GC pause, a laptop lid) past its TTL, another agent acquires
the lease, and the first agent wakes up and writes. MemKernel closes this gap at the
storage layer. `fenced_append(e, ρ, θ)` checks, *inside the same transaction as the
append*, that $\theta$ is the current token for $\rho$, that the lease is unexpired, and
that the holder is $a(e)$.

**Proposition 3 (Fencing safety).** Let agent $X$ hold token $\theta$ and agent $Y$ later
receive $\theta' > \theta$ for $\rho$. After $Y$'s grant commits, no append fenced by
$\theta$ can commit. *Proof.* Grants and fenced appends are serialized by the database's
write lock. After $Y$'s grant, the stored token is $\theta' \neq \theta$, so the check
inside any later fenced transaction fails. ∎

Lease grants and the corresponding `CLAIM_ACQ` events are written in one transaction, as
are releases and `CLAIM_REL`. The audit log therefore never disagrees with the lease table
about who was granted what. Expiry is decided by wall-clock time, so all processes must
share a clock. This holds on one host, and it is why the design is scoped to a single
host (§7).

### 4.3 Optimistic concurrency: compare-and-append

An append may carry an expected predecessor $\pi$. The store rejects it with `StaleHead`
if the branch head has moved, which is a compare-and-swap on the head pointer. The garbage
collector relies on this. It reads the context, synthesizes a snapshot outside any lock,
and appends with $\pi = $ the head it observed. If two server processes compact the same
branch concurrently, exactly one snapshot commits. The loser re-reads, finds the budget
satisfied, and stops. The test suite exercises this race across two connections.

### 4.4 Conflict semantics across heterogeneous agents

MemKernel distinguishes three kinds of conflict.

1. **Resource conflicts** (two agents editing one file) are *prevented* by leases. The
   losing agent receives the holder's identity and the remaining TTL, and can choose
   other work. The multi-agent simulation (`examples/multi_agent_collaboration.py`) races
   three MCP server processes for one file and asserts that exactly one lease is granted.
2. **Decision conflicts on one branch** (Cursor records `auth = PKCE`, then Codex records
   `auth = PKCE + rotation`) are *serialized*. The last writer wins, and the loss is
   recorded as a deprecated assumption, so the conflict is always visible to every agent.
3. **Divergent branches** are *reported*, not merged: `diff` lists conflicting decisions
   and facts with both values and their provenance.

All MemKernel state lives in one serializable log, so every agent reading a branch sees
the same state at the same log position. Our multi-agent simulation asserts that the
three agents' `query_state` views are identical.

---

## 5. Empirical Evaluation & Benchmarks

### 5.1 Setup

All numbers come from `benchmarks/run_benchmarks.py`, a deterministic, seeded script.

**Environment.** One workstation with 22 logical CPUs, measured two ways:

- natively on Windows 11 (build 26200), with Python 3.14.7 and SQLite 3.53.1;
- on Linux, in a Debian 13 container (Python 3.14.7, SQLite 3.46.1, kernel 6.6) running
  in Docker's WSL 2 virtual machine on the same hardware.

Both use WAL mode with `synchronous=NORMAL`, and the tokenizer is tiktoken `cl100k_base`.

**Workloads.** There are three synthetic 50-turn agent sessions:

- `single-agent-test-fix`: one agent in a pytest fix loop.
- `multi-agent-refactor`: three agents interleaved, with lease acquisition and release.
- `lint-heavy-exploration`: long linter reports alternating with test runs.

Every session includes scratchpad notes every third turn and a design decision every
tenth. The *raw history* baseline is the token cost of rendering every event, which is
what a chat-history memory would have to load. The *MemKernel* column is
$|C(B)|_{\text{tok}}$ at the end of the session, with automatic GC at $T = 4000$.
*State equivalent* checks Theorem 1: the state materialized through snapshots equals a
replay of the full history.

### 5.2 Token savings

**Table 1.** Context an agent must load after a 50-turn session. The results are the same
on Windows 11 and Linux, with one exception: the multi-agent row differs by a single raw
history token (33,599 vs 33,598), because its lease events embed wall-clock expiry times.

| Workflow | Events | Raw history tokens | MemKernel context tokens | Reduction | Snapshots | State equivalent |
|---|---:|---:|---:|---:|---:|:---:|
| single-agent-test-fix | 73 | 31,346 | 3,558 | 88.6% | 13 | yes |
| multi-agent-refactor | 99 | 33,598 | 2,360 | 93.0% | 13 | yes |
| lint-heavy-exploration | 73 | 42,445 | 3,459 | 91.9% | 19 | yes |

The collector keeps the context near or below the budget regardless of history length,
and it never trades away state: every workload passes the equivalence check. Snapshot
counts respect Proposition 2 (for example, at most $4 \cdot 31{,}346/4000 \approx 31$ for
the first workload). An earlier version, which rendered snapshots as JSON and lacked the low-water tail and minimum gain, wrote
28, 21 and 47 snapshots on the same workloads, compacting on almost every append once
over budget, and reduced context by only 86.3%, 89.7% and 89.2%. That regression
motivated §3.4.

### 5.3 Replay and recovery latency

Recovery is what an agent (or a restarted server) pays to rebuild its memory. We load
logs of $N$ events (20% decisions, 80% tool logs), compact once, and measure the median
over repeated runs of five operations:

- *cold open + context*: process-level open, schema check, context read and materialization.
- *context fast path*: §3.6.
- *full replay*: read and fold the entire lineage. This is the verification path and the
  worst case after a rollback.
- *fork*: at the head, the common "investigate" case. It reuses the source's head and
  snapshot pointer, so it is $O(1)$. A fork at a historical event folds the lineage
  prefix instead.
- *fork + rollback*: a fork at the head followed by a rollback to the fork point. Rollback
  validates its target against the live timeline, so it folds the lineage.

**Table 2a.** Recovery and timeline operations on Windows 11, median of 15 runs, in milliseconds.

| Log size (events) | Cold open + context | Context (fast path) | Full replay | Fork | Fork + rollback |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 16.6 | 0.81 | 15.2 | 2.6 | 23.5 |
| 10,000 | 18.3 | 0.98 | 162.8 | 2.0 | 143.6 |
| 50,000 | 21.4 | 1.72 | 837.1 | 1.6 | 686.4 |

**Table 2b.** The same operations on Linux (Debian 13 container).

| Log size (events) | Cold open + context | Context (fast path) | Full replay | Fork | Fork + rollback |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 5.6 | 1.58 | 17.4 | 2.8 | 19.2 |
| 10,000 | 4.2 | 0.97 | 195.2 | 2.4 | 113.4 |
| 50,000 | 4.5 | 1.10 | 863.0 | 1.8 | 669.8 |

Single-writer durable append throughput, with one transaction per event, is about 1,000
events/s on both platforms (1,068 on Windows 11, 998 on Linux). Bulk loads in a single
transaction run at 860–1,730 events/s.

Cold open is faster on Linux, where process and file startup costs are lower. The operations
that dominate agent traffic (fast-path context reads and forks) cost 1–3 ms on both
platforms regardless of log size.

The fast path is what agents hit on every `query_state`, and it scales with the delta, not
with the log. Fork at the head is constant-time. Full replay is linear, at roughly 60,000 events per second, which is acceptable
for verification and post-rollback reads. Operations that fold the lineage are the
scalability frontier (§7).

### 5.4 Correctness and interoperability

The test suite covers WAL persistence across reopen, append-only enforcement,
hash-chain tamper detection, optimistic concurrency, lineage traversal across nested
forks, fork isolation, nested rollbacks, resurrection by fork, deterministic replay across
databases, lease contention within a process and across connections, expiry under a fake
clock and in real time, fenced appends against stale tokens, snapshot soundness under
repeated GC, fast/slow path agreement, the anti-thrash bound, concurrent GC, LLM output
confinement, and an MCP end-to-end test with two real server processes. It passes,
with deprecation warnings treated as errors, on Python 3.14.7 and on the supported floor
(Python 3.11), on Windows 11 and on Linux (Debian 13), and on both MCP Python SDK lines:
1.30 (`FastMCP`) and 2.2 (`MCPServer`). Continuous integration adds macOS. An opt-in test
runs `LLMSummarizer` against a live OpenAI-compatible model, and it passed.

### 5.5 Live evaluation with real agents

Everything in this subsection comes from one recorded run of `benchmarks/live_agents.py`:
the artifacts are in `evidence/live-agents/20260928T161716Z/`. The harness drives the
unmodified, logged-in agent CLIs: Claude Code 2.1.195, Codex CLI 0.142.3 (model `gpt-5.5`)
and the Antigravity CLI 1.2.12. The server ran on Python 3.14.7 on Windows 11. The run
directory holds:

- every agent's own transcript, including its tool calls and their results;
- the shared MemKernel database and a JSON export of all its events;
- the code diff the agents produced, and the test output before and after;
- a generated `REPORT.md`.

No agent was given a database path. Each launched its own MemKernel server from the
project directory, and discovery (§4.1) put all of them on one shared memory.

**Task.** The run starts from a small repository with two genuinely failing tests: money
rounded with banker's rounding in `src/pricing.py`, and overselling allowed in
`src/inventory.py`. Claude Code and Codex CLI were started at the same moment with the same
instruction: fix the failing tests starting with `src/pricing.py`, lease a file before
editing it, and do other work if the lease is refused.

**Coordination.**

- Claude Code acquired the lease on `src/pricing.py` (fencing token 1) and fixed it with
  `Decimal.quantize(..., ROUND_HALF_UP)`.
- Codex CLI read the shared state, asked for the same lease, and was refused because
  Claude Code held it (one denial, recorded in its own transcript).
- Codex moved to `src/inventory.py` (token 1) and fixed that instead.

Each agent recorded its design decision (`money.rounding`, `inventory.oversell_policy`) and
released its lease. The test suite went from exit code 1 to 0 (`tests-before.txt`,
`tests-after.txt`, `code.diff`). Neither agent edited a file the other held.

**Branching by an agent.** Antigravity then read both agents' decisions from shared
memory and recorded a workspace fact. It forked `experiment-float-money`, recorded a
deliberately bad `money.rounding = "binary float"` decision there (event 14), located the
fork event through `memkernel_history`, and rolled back to it. The rollback reverted
exactly `[14]`, and the branch's `money.rounding` returned to Claude Code's value. Of the
15 events in the log, 5 were written by `claude-code-cli`, 6 by `codex-cli` and 4 by
`antigravity`.

**Recall across sessions.** A fresh Claude Code session, told not to read any files,
answered from shared memory alone. It correctly identified which agent recorded which
decision, confirmed that no blockers remained, listed both branches with the Antigravity
fork and rollback, and reported that the last test run exited 0.

**Integrity.** The hash chains of both branches verified intact on the host. They were
verified again, independently, in a clean `python:3.14-slim` Linux container built from
source, which read a copy of the evidence database.

**A finding from the run.** The recall session flagged that the execution digest "still
shows failures=1 despite exit 0". The field counted historical failed runs, but a model
read it as the current status. We renamed it `failed_runs` and now render it after the
last exit code (`last exit 0 (3 runs, 1 failed)`). Real agents surface interface
ambiguities that unit tests do not.

**Scope.** This run demonstrates cross-vendor coordination, branching and recall on a
small task. It is one run, not a statistical study: the agents are nondeterministic, so
a re-run can interleave differently. If one agent releases a lease before the other
requests it, for example, no refusal occurs. The context sizes in
this run stayed well below the GC threshold, so the token-savings claims rest on the
synthetic workloads of §5.2, not on this run.

**Threats to validity.** The workloads are synthetic. Real sessions repeat commands more
often, which favors the digest and so favors MemKernel, but they may carry larger durable
state. Token counts depend on the tokenizer. Latency was measured on one workstation,
natively on Windows 11 and in a Linux container sharing the same hardware, with
`synchronous=NORMAL`; `FULL` durability lowers append throughput.

---

## 6. Threat Model & Security

**Assets.** The integrity of shared memory (decisions, facts, blockers), the correctness
of coordination (leases), the availability of the context budget, and the confidentiality
of whatever agents log.

**Adversaries.** Four kinds are considered:

- (A1) a buggy agent;
- (A2) a prompt-injected agent that follows instructions hidden in repository content,
  web pages or tool output [15];
- (A3) a compromised summarization model;
- (A4) a local process with write access to the database file.

Out of scope: an attacker with the user's OS privileges who can also replace the
MemKernel code.

**Trust boundaries.** Each MCP server process runs with its agent's identity, taken from
launch configuration rather than from tool arguments. The database file is the boundary
of the shared domain. Everything *inside* events is untrusted data.

| Threat | Mitigation in MemKernel | Residual risk |
|---|---|---|
| **Memory injection:** an agent (A2) writes instructions into a decision or log, hoping other agents obey them | Server instructions and the LLM prompt label memory as data. Every item carries provenance (`agent #event_id`). Nothing in MemKernel executes stored content. | Downstream models may still follow injected text. Consumers should weigh provenance, and high-stakes deployments can restrict which agents may write decisions. |
| **History rewriting** (A4) | Append-only triggers. Per-lineage SHA-256 chain. `verify` detects in-place edits and broken links. | An attacker who rewrites an entire suffix and recomputes hashes is detected only against an externally anchored head hash. Periodically publish `head_hash` to an append-only external store. |
| **Forged control state** | `FORKED`/`ROLLBACK` events can only be issued by kernel operations. Snapshots must declare the schema, cover a live event and validate against the state model. | A4 can bypass the kernel entirely (see previous row). |
| **LLM summarizer poisoning** (A3) | Output is confined to `insights`, sanitized, and capped in length and count. Facts, decisions and blockers remain a pure function of the log. Failure falls back to the heuristic summarizer. | Insights can still mislead. They are labelled as summarizer output in snapshot metadata. |
| **Context flooding / DoS** (A1, A2) | 256 KiB cap on each event's payload and on its metadata. Rendered tool output is truncated. Bounded snapshot sections. GC with an anti-thrash bound. Validated identifiers. | Many distinct fact or decision keys grow durable state. Operators can monitor `context_tokens`. |
| **Lost-update on files** (A1) | Leases with fencing tokens, checked atomically with fenced appends (Prop. 3). | Leases are advisory for the filesystem itself. An agent that edits without leasing is not stopped, only visible, because its claims are absent. |
| **Identity spoofing** | `agent_id` is fixed per server process from launch configuration and validated against a strict pattern. | Identity is self-asserted: any process that can open the file can claim any id. Use OS permissions and one database per trust domain. Signed events are future work. |
| **Secret leakage** | Nothing is sent off-host except through a summarizer the operator explicitly configures. `.gitignore` excludes the database. | Agents may log secrets in tool output, and MemKernel stores what it is given. Redact before logging. |

**Privilege separation.** Separation is at process granularity: one server per agent,
each with a fixed identity and branch. Agents cannot issue raw SQL, control events or
snapshots through MCP. They are limited to the typed tool surface. A deployment that
needs hard isolation between agents should give each trust domain its own database. With
the standard `mcpServers` configuration schema this is a one-line change.

---

## 7. Conclusion & Future Directions

MemKernel treats multi-agent memory as a kernel resource. It keeps an immutable,
verifiable log. State is a deterministic fold. Branches and compensating rollbacks make
speculation and recovery cheap. A token-budget collector is provably sound and bounded.
Fenced leases make concurrent editing safe. All of it is reachable from unmodified agents
through MCP. The evaluation shows large context savings with verified state equivalence,
millisecond-scale recovery, and live interoperability between agents from different
vendors.

We see several directions for future work.

- **Replication beyond one host.** Replace the SQLite write lock with a consensus log
  (for example Raft [8]) and the wall-clock lease with a lease service, so swarms can span
  machines.
- **Semantic merge.** Represent decision sets as CRDTs [7] with type-specific merge
  functions, and use the diff (§2.5) to propose merges that an agent confirms.
- **Signed events.** Per-agent keys make provenance unforgeable and turn the hash chain
  into a verifiable multi-writer log.
- **Incremental folds everywhere.** Cache folded states at fork points to make rollback
  and historical forks $O(\Delta)$ as well.
- **Secondary indexes.** Semantic (vector) and symbolic indexes *over* snapshots, with
  retrieval that respects supersession, combining RAG recall with MemKernel's validity.
- **Remote transports.** Streamable HTTP with per-agent authentication, which turns
  self-asserted identity into authenticated identity.

---

## References

[1] C. Gray and D. Cheriton. Leases: An Efficient Fault-Tolerant Mechanism for Distributed File Cache Consistency. *SOSP*, 1989.

[2] M. Kleppmann. How to do distributed locking. 2016; and *Designing Data-Intensive Applications*, O'Reilly, 2017.

[3] M. Fowler. Event Sourcing. martinfowler.com, 2005.

[4] SQLite Consortium. Write-Ahead Logging. sqlite.org/wal.html.

[5] L. Lamport. Time, Clocks, and the Ordering of Events in a Distributed System. *CACM* 21(7), 1978.

[6] S. Chacon and B. Straub. *Pro Git*, 2nd ed. Apress, 2014.

[7] M. Shapiro, N. Preguiça, C. Baquero and M. Zawirski. Conflict-free Replicated Data Types. *SSS*, 2011.

[8] D. Ongaro and J. Ousterhout. In Search of an Understandable Consensus Algorithm. *USENIX ATC*, 2014.

[9] P. Lewis et al. Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks. *NeurIPS*, 2020.

[10] C. Packer et al. MemGPT: Towards LLMs as Operating Systems. arXiv:2310.08560, 2023.

[11] N. F. Liu et al. Lost in the Middle: How Language Models Use Long Contexts. *TACL*, 2024.

[12] J. S. Park et al. Generative Agents: Interactive Simulacra of Human Behavior. *UIST*, 2023.

[13] Anthropic. Model Context Protocol Specification. modelcontextprotocol.io, 2024–2026.

[14] S. Haber and W. S. Stornetta. How to Time-Stamp a Digital Document. *Journal of Cryptology* 3(2), 1991.

[15] K. Greshake et al. Not What You've Signed Up For: Compromising Real-World LLM-Integrated Applications with Indirect Prompt Injection. *AISec*, 2023.
