# Coordinator Runtime

This document specifies the coordinator's concurrency architecture: the
long-lived tasks inside a coordinator process, the channels between them,
who owns mutable state, how a proposal travels from a caller to a committed
apply result, and what changes when leadership moves. It is the
implementation contract for the coordinator daemon (`coppice-coordinator`)
and the consensus seam (`coppice-consensus`).

It builds directly on decisions made elsewhere and does not restate them:
the Raft substrate and storage layer ([ADR 0002](../decisions/0002-openraft-with-custom-segment-storage.md)),
read-consistency classes ([ADR 0007](../decisions/0007-per-endpoint-read-consistency.md)),
event delivery ([ADR 0008](../decisions/0008-event-delivery-guarantees.md)),
fencing and reconciliation ([ADR 0009](../decisions/0009-fencing-and-reconciliation.md)),
terminal-job eviction ([ADR 0012](../decisions/0012-data-retention.md)),
and rebuild-by-learner-join ([ADR 0016](../decisions/0016-coordinator-rebuild-learner-join.md)).
The apply contract, the command catalog, and the `RejectionReason` taxonomy
this runtime drives live in [command-catalog.md](command-catalog.md); the
subsystem overview is [components.md](components.md); the HA model is
[high-availability.md](high-availability.md).

The governing design choice is **single-writer state ownership with
lock-free snapshot fan-out**: exactly one task mutates the state machine,
and everyone else reads immutable snapshots. Almost every hard property in
this document — no deadlock, no `&mut` held across an `.await`, serial
deterministic apply — falls out of that choice by construction rather than
by discipline.

## The consensus seam

The compiler-checked boundary lives in `coppice-consensus`. openraft types
never cross out of that crate; everything above it speaks the seam's own
vocabulary:

- **`Consensus`** — the async trait the rest of the coordinator programs
  against: `propose`, `read_index`, a status watch, membership operations,
  and `trigger_snapshot`. Per [ADR 0002](../decisions/0002-openraft-with-custom-segment-storage.md)
  this is a *thin adapter* over openraft, not an abstraction meant to swap
  Raft libraries.
- **`StateView` / `StateViews`** — an `Arc` snapshot of the state machine
  plus its applied index, published over a `tokio::sync::watch`. `StateViews`
  is the reader handle; `StateView` is one immutable point-in-time snapshot.
- **`ViewPublisher`** — the apply-side counterpart that publishes new views.
- **`EventBatch` / `EventTap` / `EventTapReceiver`** — non-blocking,
  apply-side event emission. The tap carries a dense sequence so a dropped
  batch is recoverable as a synthesized gap ([ADR 0008](../decisions/0008-event-delivery-guarantees.md)).
- **`ConsensusError`** — split into *retryable* (`NotLeader`, `Timeout`,
  `MembershipInProgress`, `LearnerNotCaughtUp`) and *fatal* (`Shutdown`,
  `Fatal`).

**A deterministic rejection is not an error.** A committed command that
apply refuses is a *successful* propose result:
`Applied { log_index, outcome: Result<state::Applied, RejectionReason> }`.
`ConsensusError` is reserved for the cases where the command's fate is
genuinely undetermined or the node cannot act (not leader, timed out, shutting
down). This distinction is load-bearing everywhere downstream: the API maps a
`RejectionReason` to a user error, the scheduler maps it to "recompute," and
ingestion ignores benign ones — none of them treats a rejection as a fault.

## Task inventory

Every long-lived task in a coordinator process. "Leader-only" tasks exist on
every replica but self-gate on the status watch (see [Leader
transitions](#leader-transitions)); the unmarked tasks run identically on
followers, which is what lets followers serve reads and event streams.

| Task | Crate | Leader-only? | Owns | Wakes on |
| --- | --- | --- | --- | --- |
| openraft core + replication + election | `coppice-consensus` (openraft) | no | the Raft log, election, membership | its own internal machinery |
| Apply task | `coppice-consensus` | no | the mutable `StateMachine` (by value) | apply requests, view demand, cadence tick |
| API server | `coppice-coordinator` (handlers in `coppice-api`) | no | client connections | HTTP requests |
| Event fanout | `coppice-coordinator` | no | the reconnection ring, subscriptions | event-tap batches, subscribe/unsubscribe |
| Agent gateway (session mgr + per-session tasks) | `coppice-coordinator` | accept: yes | agent sockets, session state | socket reads, router sends |
| Ingestion / normalizer | `coppice-coordinator` | yes | the ObservedSet diff, dedupe state | agent inbound reports |
| Dispatch loop | `coppice-coordinator` | yes | in-flight dispatch bookkeeping | the event stream |
| Scheduler driver | `coppice-coordinator` | yes | the current scheduling pass | its own loop + view updates |
| Housekeeping | `coppice-coordinator` | yes | the eviction/snapshot cadence | a 60 s tick |
| Snapshot builder | `coppice-consensus` | no | snapshot serialization/IO | snapshot requests |

1. **openraft core + replication + election.** A black box. Its internal
   channels are its own business. It calls our storage layer (the segment
   files of [ADR 0002](../decisions/0002-openraft-with-custom-segment-storage.md))
   and our `RaftStateMachine` adapter, which is the only thing that talks to
   the apply task.

2. **Apply task** (in `coppice-consensus`, on *every* replica — followers
   included, since their reads and event streams derive from applied state).
   It is the **sole owner of the mutable `StateMachine`, held by value with
   no lock.** Because nothing else can name the value, holding `&mut` across
   an `.await` is impossible by construction, not by review. The task
   receives `ApplyRequest`s from the openraft state-machine adapter over a
   **bounded `mpsc(64)`** where the adapter awaits each reply — so
   backpressure lands on openraft replication, which is the correct pressure
   point for a serial apply loop. The loop is a `select!` over:
   - **apply requests** — apply each command via `StateMachine::apply` (sync,
     deterministic, bounded per the apply contract), emit events through the
     `EventTap`, `maybe_publish` a view, and reply;
   - **view-demand wakeups** — a strong read is waiting on an index at or
     below the committed frontier;
   - **a cadence tick** — so a strong-read barrier resolves even when the log
     is idle.

   Snapshot requests are answered by publishing the current state and handing
   out the `Arc`; serialization happens in the snapshot-builder task, never
   on the apply task.

3. **API server** (every replica). Serves reads from views according to the
   [ADR 0007](../decisions/0007-per-endpoint-read-consistency.md) classes;
   forwards or redirects writes to the leader. The write path is
   `Consensus::propose`, mapping an `Applied.outcome` rejection to a
   user-facing error.

4. **Event fanout** (every replica). Consumes the `EventTapReceiver`, owns
   the [ADR 0008](../decisions/0008-event-delivery-guarantees.md)
   reconnection ring (bounded: 1 h / 1M events), and manages subscriptions.

5. **Agent gateway.** A session manager plus one task per agent session.
   **Sessions terminate on the leader only** — a follower refuses an agent
   connection with a leader hint. The justification is not arbitrary: every
   inbound report must be normalized and proposed by the leader anyway (the
   ingestion boundary in [command-catalog.md](command-catalog.md#the-agent-report-ingestion-boundary)),
   so a follower-hosted session would only add a forwarding hop with no
   consistency benefit. Revisit only if leader connection fan-in becomes a
   *measured* bottleneck. Sessions re-fence with the new
   `(leader_term, node_epoch)` on every leader change ([ADR 0009](../decisions/0009-fencing-and-reconciliation.md)).

6. **Ingestion / normalizer** (leader-only). The boundary of
   [command-catalog.md](command-catalog.md#the-agent-report-ingestion-boundary):
   fencing check, dedupe by `(AttemptId, attempt_state)`, timestamping, and
   the ObservedSet diff — then `propose` (`RecordAttempt*`, `ReconcileNode`,
   `RegisterNode`, `DeclareNodeLost`). It ignores benign apply rejections
   (`StaleAttemptState` and the like) rather than treating them as failures.

7. **Dispatch loop** (leader-only). Consumes the event stream. On an attempt
   reaching `Ready` it proposes `DispatchAttempt`, and **only after that
   applies** does it send `StartJob` — the commit-before-send ordering of
   [command-catalog.md](command-catalog.md#dispatchattempt). A crash in the
   window between commit and send reconciles as *lost*, never as an untracked
   container. On a `StopRequested` event it routes a `StopJob`. After any
   event gap it resyncs by scanning the latest view for `Ready` attempts and
   pending aborts; at-least-once delivery plus idempotent commands make the
   rescan safe.

8. **Scheduler driver** (leader-only). A loop of: take `views.latest()`, run
   the CPU-heavy `Scheduler::schedule` pass in `spawn_blocking` (the trait
   stays *sync* — it is pure CPU over an immutable view), propose
   `CommitPlacements`, await the outcome. **At most one proposal is in flight
   by construction.** A rejection (`InvalidBatch`, `AccrualLimitExceeded`) is
   a recompute, the normal path of [scheduling-model.md](../scheduling/scheduling-model.md#operating-model).
   `expected_version` comes from `StateView::version()` — see the [two
   coordinates](#the-two-coordinates-trap) distinction.

9. **Housekeeping** (leader-only, 60 s tick). Scans the view for terminal
   jobs past retention and **writes them to the SQL job-history store first**
   — an *external* network call, therefore outside apply, with retries — and
   only after that write is durable proposes `EvictTerminalJobs`. This is the
   [ADR 0012](../decisions/0012-data-retention.md) ordering: history-write
   durability is sequenced *before* the evict proposal, never concurrent with
   it. The same task triggers snapshots via `Consensus::trigger_snapshot`
   when applied-entries-since-snapshot crosses a threshold ([ADR 0002](../decisions/0002-openraft-with-custom-segment-storage.md)
   / [ADR 0017](../decisions/0017-log-manifest-truncation-and-purge.md):
   sealed segments are deletable only once a snapshot covers them). Duplicate
   history writes across a leader change are harmless (idempotent by job id);
   duplicate evict proposals are absorbed by apply (missing ids are skipped).

10. **Snapshot builder.** Serializes the `Arc`'d state handed out by the
    apply task and writes it through the storage layer, keeping CPU and IO
    off the apply path.

### Task and channel topology

```
  agents (RPC)                              clients (UI / CLI / HTTP)
      |                                                |
      v                                                v
+----------------------+                 +----------------------------+
|    Agent gateway     |                 |         API server         |
|   (leader accepts;   |                 |       (every replica)      |
|   one task/session)  |                 |  reads    <- StateViews    |
+----------------------+                 |  streams  <- event fanout  |
    |            ^                       +----------------------------+
    | inbound    | outbound per session               |
    | (mpsc 8192,| (mpsc 256, try_send;               | propose
    |  await)    |  full => disconnect)               |
    v            |                                    |
+--------------+ +-----------------+                  |
| Ingestion /  | | Session manager |                  |
| normalizer   | +-----------------+                  |
+--------------+    ^  command router                 |
    |               |  (mpsc 1024, await)             |
    | propose       |                                 |
    |        +------+--------+  +---------------+  +------------------+
    |        | Dispatch loop |  | Scheduler drv |  |   Housekeeping   |
    |        +---------------+  +---------------+  +------------------+
    |           | propose          | propose        | propose   \
    v           v                  v                v            v
 ~~~~~~~~ proposal admission: semaphore, 4096 in flight ~~~~~~  [SQL job-
                          |                                     history
                          v                                     store]
        +--------------------------------------+               (external;
        | openraft core (black box):           |                write is
        | election, replication, log +         |                durable
        | segment storage, membership          |                BEFORE the
        +--------------------------------------+                evict is
                          |                                     proposed)
                          | ApplyRequest (mpsc 64;
                          v  adapter awaits each reply)
        +--------------------------------------+
        |  APPLY TASK  (runs on every replica) |
        |  sole owner of mutable StateMachine: |
        |  apply -> emit events -> publish     |
        +--------------------------------------+
           |               |                |
           | view watch    | event tap      | snapshot Arc handoff
           | (overwrite)   | (mpsc 4096,    |
           v               v  try_send)     v
     [ StateViews ]  +--------------+  +------------------+
      read by: API,  | Event fanout |  | Snapshot builder |
      scheduler,     | ring 1h/1M   |  +------------------+
      dispatch,      +--------------+
      ingestion,        | per-subscriber queues
      housekeeping      | (mpsc 1024, try_send; full => gap)
                        v
        [ subscribers: clients via API; dispatch loop internally ]
```

Every solid `propose` edge funnels through the openraft core and comes back
out through the apply task; every read edge terminates at the `StateViews`
watch. The only edges *out* of the apply task are the view watch (overwrite),
the event tap (`try_send`), the snapshot `Arc` handoff, and the apply reply —
none of them can block apply. That is the whole safety argument, made precise
in [the deadlock-freedom invariant](#deadlock-freedom).

## State ownership and views

**Single writer, own-don't-share.** The apply task owns the `StateMachine` by
value. No mutex, no `RwLock`, no shared `&mut`. Every other task reads a
`StateView` — an `Arc<StateMachine>` plus its `applied_index` — over a
`tokio::sync::watch`, which is latest-wins by construction: a reader always
observes a complete, internally consistent snapshot and never a torn
mid-apply state.

### Publish policy

The apply task publishes a new view:

- **after an apply batch, when dirty and ≥ 100 ms have passed since the last
  publish** — this 100 ms is the bounded-read staleness ceiling; or
- **early, when a strong-read demand exceeds the published index.** Demand is
  registered by `StateViews::at_least(n)` through an `AtomicU64`
  max-demanded-index plus a `Notify`; an early publish is rate-limited to a
  **10 ms minimum spacing** so a burst of strong reads cannot turn the apply
  loop into a publish storm.

Idle wakeups — the demand `Notify` and the cadence tick, both arms of the
apply loop's `select!` — guarantee a strong-read barrier resolves even on a
cluster with an idle log, where no apply request would otherwise wake the
task.

### Clone-cost analysis

Publishing a view clones the state machine. That cost is the load-bearing
risk of this design, so it is worth the arithmetic.

The state targets **1M live jobs**. Jobs, attempts, allocations, and accrual
entries together come to roughly **3–4M `BTreeMap` entries**; at a few hundred
owned bytes per entry that is **on the order of 1 GB**, and a deep clone of
that runs **hundreds of milliseconds to about a second** — unacceptable at
cadence. The design is therefore honest about its operating envelope:

| Live jobs | State size | Deep clone | Verdict |
| --- | --- | --- | --- |
| ≤ 10k | ~10 MB | single-digit ms | fine — bring-up, small/medium clusters |
| ~100k | ~100 MB | tens of ms | marginal |
| 1M | ~1 GB | hundreds of ms – ~1 s | unacceptable at cadence |

Publishing is **instrumented from day one**, not retrofitted: a
`coordinator_view_clone_seconds` histogram plus measured apply-stall time.

**The escape-hatch trigger is a metric, not a hunch:** sustained
`p99(coordinator_view_clone_seconds) > 25 ms`, or clone time exceeding 10% of
the interval between publishes. The escape hatch itself is to make the ordered
maps inside `coppice-state` persistent / structurally shared (an `im`-style
`OrdMap`): O(1) clone, O(log n) per-operation overhead, preserving both the
deterministic ordered iteration apply relies on and the structural `Eq` the
determinism harness asserts. Under that representation views publish
per-batch and the whole cadence/spacing machinery degenerates to a no-op.
Swapping the state representation is a contested enough call to earn its own
ADR *when* the trigger fires; **this document only names the trigger**, it
does not pre-decide the swap.

### The two coordinates trap

Two monotonic counters travel with every view and they must never be
interconverted:

- **Raft applied log index** — counts *every* applied log entry, including
  membership changes and blank/no-op entries. It is the cursor for read
  barriers and for event sequence numbers ([ADR 0007](../decisions/0007-per-endpoint-read-consistency.md)
  / [ADR 0008](../decisions/0008-event-delivery-guarantees.md)).
- **`StateMachine.version`** — counts *applied commands* (accepted or
  rejected). It is the scheduler's `expected_version`
  ([command-catalog.md](command-catalog.md#version-and-expected_version)).

`StateView` exposes both. They advance at different rates (a membership entry
bumps the log index but not `version`), so treating one as the other silently
corrupts either a read barrier or a scheduling precondition. There is no
conversion function on purpose.

## Channel inventory

Every channel in the coordinator, and every one is bounded. "Policy when
full" is the crux: it is what makes the blocking graph acyclic.

| Channel | Producer → consumer | Type | Capacity | Policy when full |
| --- | --- | --- | --- | --- |
| apply requests | openraft sm-adapter → apply task | mpsc | 64 | **await** (backpressure into openraft replication) |
| proposal admission | proposers → openraft | semaphore | 4096 in-flight | **await permit** (backpressure to proposers) |
| event tap | apply task → fanout | mpsc | 4096 batches | **`try_send`; on full DROP the batch** — the receiver synthesizes a gap from the dense tap sequence ([ADR 0008](../decisions/0008-event-delivery-guarantees.md) drop+gap). Apply NEVER awaits fanout. |
| fanout ring | fanout-internal | ring | 1 h / 1M events | **evict oldest** — it is a reconnection buffer, not history |
| per-subscriber queue | fanout → client conn | mpsc | 1024 | **`try_send`; on full mark the subscriber gapped**, drop its backlog, deliver `Gap{earliest_available}`; client resyncs via query ([ADR 0008](../decisions/0008-event-delivery-guarantees.md)) |
| agent inbound | session tasks → ingestion | mpsc (shared) | 8192 | **await** ⇒ the session stops reading its socket ⇒ TCP backpressure to the agent. Never touches apply. |
| agent outbound | router → per-session | mpsc | 256 | **`try_send`; on full DISCONNECT the session** — commands are idempotent and reconciliation ([ADR 0009](../decisions/0009-fencing-and-reconciliation.md) ObservedSet) heals on reconnect |
| command router | dispatch/ingestion → session manager | mpsc | 1024 | **await** (producers are leader-only loops that tolerate backpressure) |
| view / status / shutdown watch | various | watch | latest-value | **overwrite** (lossy by design, latest wins) |

### Deadlock-freedom

**Invariant: the blocking-edge graph is acyclic, and no blocking edge points
*into* the apply task.**

Consider only the *await-on-full* edges (an `mpsc` send that awaits, a
semaphore permit acquisition). `try_send`, watch-overwrite, ring-evict, and
disconnect-on-full are non-blocking and cannot participate in a wait cycle.
The apply task's only outbound edges are the event tap (`try_send`), the view
watch (overwrite), the snapshot `Arc` handoff (non-blocking), and its reply to
the adapter. **Apply never awaits a full channel.** Therefore apply can always
make progress, and since every cycle would have to pass through some task's
blocking edge, no cycle can route through apply.

The one non-trivial cycle candidate is:

```
ingestion --propose--> openraft --apply req--> apply
   ^                                             |
   |                                             | event tap
   +----------- fanout <-- subscriber <----------+
```

It looks circular, but it is not a *blocking* cycle: the apply→fanout edge is
the event tap, which `try_send`s and **drops on full**. Apply never waits on
fanout, so back-pressure cannot propagate from a slow subscriber back into
apply and stall ingestion's proposals. The cycle is broken exactly at the tap.
The same argument covers the agent path: agent-inbound backpressure stops at
the *session socket* (TCP), never reaching apply.

## Proposal lifecycle

End to end, for any proposer:

1. **Build the command.** Ids and timestamps are minted proposer-side per the
   apply contract ([command-catalog.md](command-catalog.md#apply-is-a-pure-function-of-state-command)) —
   apply never generates either.
2. **Acquire an in-flight permit** from the proposal-admission semaphore
   (4096). This is the only backpressure a proposer feels from consensus.
3. **Leader check inside openraft `client_write`.** On a follower, openraft
   returns `ForwardToLeader`; the seam maps this to the retryable
   `NotLeader { leader_hint }`, and the API forwards or redirects per
   [components.md](components.md#external-api-layer).
4. **Replication → commit.** openraft appends, replicates to a quorum, and
   commits.
5. **Apply.** The adapter hands the committed entry to the apply task over the
   `mpsc(64)`; the apply task applies it, emits events, `maybe_publish`es a
   view, and replies.
6. **Resolve.** `client_write` resolves; the seam returns
   `Applied { log_index, outcome }`.
7. **Map the outcome.** The waiter interprets `outcome`: the API turns a
   `RejectionReason` into a user error, the scheduler turns it into a
   recompute, ingestion ignores benign rejections.

**Waiter cleanup on leader change.** openraft resolves *every* in-flight
`client_write` with `ForwardToLeader` the moment leadership is lost; the seam
maps that to the retryable `NotLeader`. Waiters therefore **never hang** — a
step-down cannot orphan a proposal.

**The unknown-outcome window.** A `Timeout` (or a step-down mid-proposal)
means the outcome is genuinely *unknown* — the command may still commit later.
Safe re-proposal rests entirely on the catalog's idempotency points: a
duplicate `SubmitJob` rejects as `DuplicateJob`, a re-observed attempt
transition rejects as `StaleAttemptState`, an evict skips already-missing ids.
Every re-proposal resolves to a deterministic rejection or no-op, so a retry
after an unknown outcome can never double-apply.

## Read paths

The three [ADR 0007](../decisions/0007-per-endpoint-read-consistency.md)
classes map onto the runtime as follows. Followers run the apply task, the
view publisher, and the fanout *precisely so* these paths work off-leader.

- **Strong.** `Consensus::read_index()` on the leader (the quorum/lease check)
  yields a committed index `n`; the server then awaits `views.at_least(n)` and
  serves from that view. Latency is bounded by the demand-publish spacing
  (10 ms) plus apply catch-up — never unbounded, because the demand `Notify`
  wakes the apply task immediately.
- **Bounded-stale.** A follower serves `views.latest()`; the response carries
  `applied_index` and `known_committed` (from the status watch), so staleness
  is *surfaced*, not hidden. A follower that cannot bound its lag —
  partitioned, or installing a snapshot — rejects with a redirect to the
  leader.
- **Eventual.** Served from the SQL job-history store, outside consensus
  entirely; a job evicted from replicated state ([ADR 0012](../decisions/0012-data-retention.md))
  remains queryable there.

## Leader transitions

**Leader-only tasks self-gate; nothing is killed.** Each leader-only task
waits on the status watch (`wait_for(role.is_leader())`), then runs its body
under a `select!` on the status change and the shutdown watch. There is **no
supervisor that kills tasks**: a task always stops at an `.await` point *it
chose*, never mid-invariant. That is what makes leadership handoff safe
without distributed locks.

**On gaining leadership:**

- the scheduler driver, housekeeping, dispatch, and ingestion loops start;
- the agent listener begins accepting sessions;
- every command a session sends carries the new `leader_term` (re-fence,
  [ADR 0009](../decisions/0009-fencing-and-reconciliation.md));
- **dispatch resyncs from the current view before trusting the event stream**
  — it scans for `Ready` attempts and pending aborts, because events emitted
  under the previous leader may predate this task's subscription.

**On losing leadership:**

- in-flight proposals fail retryable (`NotLeader`, per the lifecycle above),
  so no waiter hangs;
- the scheduler abandons its pass — its `CommitPlacements` would fail anyway;
- housekeeping may have completed a history write whose `EvictTerminalJobs`
  proposal now fails — harmless, because the next leader re-does both writes
  idempotently ([ADR 0012](../decisions/0012-data-retention.md));
- the session manager closes agent sessions; agents rediscover the leader and
  re-register. A deposed leader's already-sent `StartJob`/`StopJob` **fail
  closed at the agents** on the term check ([ADR 0009](../decisions/0009-fencing-and-reconciliation.md)).

**Step-down mid-proposal and the unknown-outcome window** are covered by
fencing plus idempotency: a command committed under the old term either
applied on every replica or on none, so state cannot diverge, and any
re-proposal by the new leader resolves deterministically (above).

**Shutdown order** (one flip, then drain in dependency order):

1. flip the shutdown watch;
2. the API and agent listeners stop accepting new work;
3. the leader-only loops drain and exit at their chosen await points;
4. fanout closes its subscribers;
5. openraft shuts down — the apply task drains its request queue and exits
   when the adapter drops the `mpsc(64)`;
6. the storage layer flushes and closes.

## Traps appendix

Each hazard and the by-construction reason it cannot bite:

- **No `&mut StateMachine` across an `.await`.** The apply task owns the state
  by value and nothing else can name it — ownership, not review discipline.
- **Event emission happens inside apply but never blocks it.** The event tap
  `try_send`s and drops on full, synthesizing a gap; apply never awaits
  fanout.
- **The history-store write is outside apply and sequenced before the evict.**
  The external network call is a proposer-side obligation of housekeeping, and
  `EvictTerminalJobs` is proposed only after the write is durable
  ([ADR 0012](../decisions/0012-data-retention.md)).
- **openraft types are confined to `coppice-consensus`.** The `Consensus`
  seam is the only surface; a rejection is a successful `Applied` result, not
  a `ConsensusError`.
- **Followers run apply, views, and fanout.** The topology is leader-only
  *only* where the table marks it; every read and event path works off-leader
  by design.
