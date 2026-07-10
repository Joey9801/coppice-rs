# Known Open Issues

This register tracks known correctness and scalability issues in the current
design or implementation. It is deliberately separate from
[open-decisions.md](open-decisions.md): an open decision records a question
whose answer has not been settled, while this document records a known defect,
contradiction, or missing invariant in work that is otherwise described as
settled.

Entries stay in this file after resolution. Mark the status resolved, link the
fix, and link any ADR that changes an accepted decision. Until then, the issues
below are release blockers for the affected capability; passing tests do not
waive them.

## Status at a glance

| ID | Severity | Area | Status | Release impact |
| --- | --- | --- | --- | --- |
| [KOI-1](#koi-1-terminal-job-eviction-can-destroy-the-only-history) | Critical | Retention / history | Open (retention timing resolved) | Do not enable terminal-job eviction with the stub history sink |
| [KOI-2](#koi-2-job-submission-is-not-idempotent-across-an-unknown-outcome) | High | Public API | Open | Do not claim safely retryable job submission |
| [KOI-3](#koi-3-event-cursors-depend-on-local-apply-batching) | High | Events / replication | Resolved (2026-07-10) | — |
| [KOI-4](#koi-4-unbounded-projected-ready-does-not-protect-accrual-progress) | High | Scheduling | Open | Strict-backfill starvation guarantee is not established |
| [KOI-5](#koi-5-view-and-snapshot-publication-do-not-fit-the-1m-job-target) | High | Scalability | Open | Do not advertise the 1M-job target as supported |

## KOI-1: Terminal-job eviction can destroy the only history

- **Severity:** Critical
- **Status:** Open — the retention-timing half is resolved (2026-07-10); the
  durable-history half is not
- **Affected capability:** terminal-job retention and historical queries
- **Related decisions:** [ADR 0012](../decisions/0012-data-retention.md),
  [ADR 0007](../decisions/0007-per-endpoint-read-consistency.md)

### Required invariant

A terminal job is eligible for eviction from replicated state only after both
of these facts are true:

1. the configured retention interval has elapsed since the job reached its
   terminal state; and
2. the full history record has been durably and idempotently written to the
   history store.

After eviction, the job must remain queryable through the eventual-consistency
history path promised by ADR 0012.

### Current violation

~~`JobRecord` stores `submitted_at_us` but no terminal-transition timestamp.~~
**Resolved 2026-07-10.** `JobRecord.terminal_at_us` is stamped by whichever
command resolves the job terminally (abort `requested_at_us`, outcome and
reconcile reports' `observed_at_us`, loss declaration's `declared_at_us`;
a requeue leaves it unset), and the housekeeping retention scan measures
exclusively from it ([state record](../../crates/coppice-state/src/lib.rs),
[housekeeping](../../crates/coppice-coordinator/src/tasks/housekeeping.rs)).
A job that queues or runs longer than the retention period — a deliberately
supported pattern for cheap low-priority work — now gets the full configured
interval after completion. Terminal records predating the field carry no
stamp and are exempt from eviction (a retention leak, never an early loss).

The production runtime still installs `StubHistoryStore`. That implementation
only logs a count and returns success, which lets housekeeping propose
`EvictTerminalJobs` even though no durable historical copy exists. The job,
attempts, and allocations can then disappear from replicated state with no
queryable replacement. This half remains open, and is an accepted limitation
until the SQL history sink lands: eviction still runs, and evicted jobs are
lost to history.

### Impact

- Irrecoverable loss of user-visible job and attempt history (until a real
  history store lands).
- Eventual/history reads cannot satisfy the API contract after eviction.
- ~~A long-running job is at greater risk than a short job because its age is
  measured from submission.~~ Resolved: the retention clock starts at the
  terminal transition.

### Resolution requirements

- ~~Add a replicated terminal timestamp, stamped by the command that resolves
  the job terminally. Define its behavior for immediate abort, normal outcome,
  reconciliation, and node loss.~~ Done — see the `Finalizing` resolution
  rules in [command-catalog.md](../architecture/command-catalog.md#finalizing-resolution).
- ~~Base eviction eligibility exclusively on that terminal timestamp.~~ Done —
  `due_for_eviction` in housekeeping.
- Implement a durable, idempotent history store containing the full job,
  attempt, outcome, usage-summary, and audit data required by ADR 0012.
- Fail closed: a missing, disabled, or failed history sink must not be treated
  as a successful durable write. Prefer disabling eviction explicitly over a
  success-returning stub.
- ~~Add tests proving that old submissions receive a full post-terminal
  retention interval~~ (done: `eviction_runs_a_full_retention_from_the_terminal_transition`,
  `terminal_timestamp_is_stamped_by_the_resolving_command`,
  `reconcile_and_node_loss_stamp_the_terminal_timestamp`) and that no eviction
  is proposed after a failed or stubbed history write (open).

### Closure criteria

This issue is resolved when the runtime cannot emit `EvictTerminalJobs` without
a durable history receipt and a correct post-terminal retention calculation,
and an integration test queries an evicted job from the history path.

## KOI-2: Job submission is not idempotent across an unknown outcome

- **Severity:** High
- **Status:** Open
- **Affected capability:** public job submission and client retries
- **Related design:** [proposal lifecycle](../architecture/coordinator-runtime.md#proposal-lifecycle),
  [command idempotency](../architecture/command-catalog.md#idempotency-under-replay)

### Required invariant

Retrying one logical submission after a timeout, connection loss, or leader
change must create at most one durable job. This must hold even when the first
request committed but its response was lost.

### Current violation

The proposal-lifecycle design argues that a duplicate `SubmitJob` is safe
because its `JobId` will already exist. That only protects re-proposal of the
same command. `SubmitJobRequest` carries no idempotency key or client-minted
job identity, while `CoordinatorControlPlane::submit_job` mints a new random
`JobId` for every invocation
([API schema](../../proto/coppice/api/v1/api.proto),
[API implementation](../../crates/coppice-coordinator/src/tasks/api_server.rs)).

A retry at the request boundary is therefore a different command with a fresh
identity. If the first request committed before its outcome became unknown,
both submissions can be accepted and both jobs can execute.

### Impact

- Duplicate workload execution and duplicate resource consumption.
- Duplicate quota charging that is correct for the two records but incorrect
  for the user's single intent.
- Clients cannot safely apply ordinary retry policies to transient API errors.

### Resolution requirements

- Add a stable idempotency identity to the public request. A client-generated
  submission ID or a scoped idempotency key are both viable; the scope and
  retention window must be explicit.
- Persist enough deduplication state in the replicated state machine so a new
  leader gives the same answer.
- Return the original `JobId` and commit index for a repeated completed
  request, and define the response for a key reused with a different payload.
- Add an integration test that drops the first successful response, retries
  through another coordinator, and observes exactly one job and one charge.

### Closure criteria

This issue is resolved when the public client can automatically retry an
unknown submission outcome without a possibility of creating a second job.

## KOI-3: Event cursors depend on local apply batching

- **Severity:** High
- **Status:** Resolved (2026-07-10)
- **Affected capability:** resumable event subscriptions and follower fanout
- **Related decision:** [ADR 0008](../decisions/0008-event-delivery-guarantees.md)

### Required invariant

Every accepted command's events are associated with that command's Raft log
index. For an identical committed log, all replicas must derive the same
ordered sequence independently of how OpenRaft groups entries into apply
calls. Cursor replay must neither skip committed events nor make a cursor mean
different things on different replicas.

### Resolution (2026-07-10)

~~The apply loop accumulated all events from one `ApplyRequest::Apply` and
emitted a single `EventBatch` tagged with the final entry's index, so apply
batch boundaries — a local runtime detail — leaked into the public stream.~~
The apply loop now emits **one `EventBatch` per accepted command, tagged with
that command's own log index**
([apply loop](../../crates/coppice-consensus/src/apply_loop.rs)). Because
apply is deterministic, which commands emit events (and which batches are
therefore skipped as empty) is a pure function of the committed log, so every
replica derives byte-for-byte identical batches and cursor positions
regardless of how OpenRaft grouped entries into apply requests.
`event_stream_is_invariant_under_apply_batching` pins this: the same command
sequence — including a rejected command mid-sequence — run under three
different artificial batchings yields the identical `(index, events)` stream.

~~Scoped fanout had a second incompleteness: job and node filters omitted
attempt- and allocation-scoped events because the fanout layer could not map
those events back to their owning job or node.~~ `AttemptStateChanged`,
`AllocationFunded`, and `StopRequested` now carry their owning job and node
ids as scope keys, stamped during apply while the association is
authoritative ([state events](../../crates/coppice-state/src/lib.rs)); the
fanout filters on them directly with no mutable lookups
([fanout](../../crates/coppice-coordinator/src/tasks/event_fanout.rs)).

### Closure criteria

Met: event derivation is invariant under apply batching
(`event_stream_is_invariant_under_apply_batching`) and scoped subscriptions
deliver the complete documented event set
(`job_filter_admits_attempt_and_allocation_events`,
`node_filter_admits_attempt_and_allocation_events`).

## KOI-4: Unbounded `projected_ready` does not protect accrual progress

- **Severity:** High
- **Status:** Open
- **Affected capability:** strict backfill and starvation protection
- **Related decision:** [ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md)

### Required invariant

Backfill may borrow capacity pledged to an accruing allocation only when the
borrower's enforced completion bound proves that the accrual is not delayed.
The mechanism must provide meaningful forward progress for the protected
top-K blocked jobs rather than merely record that they are blocked.

### Current contradiction

ADR 0014 permits lending only when
`now + max_runtime <= projected_ready(A)` and describes
`projected_ready(A)` as a guaranteed worst-case funding time. It concludes
that accruing allocations are never delayed by backfill.

The materialised scheduler design later declares
`projected_ready(A) = None`—meaning no finite guaranteed release events are
known—to satisfy the check automatically. The implementation follows that
rule with `None => true`
([scheduler design](../scheduling/scheduler-v1.md#strict-backfill-the-revoke-and-reseat-lend),
[scheduler implementation](../../crates/coppice-scheduler/src/engine.rs)).

This makes the guarantee vacuous for the allocations that most need
protection. A succession of bounded jobs can repeatedly revoke and reseat an
accrual behind new work when it has no finite `projected_ready`, including
borrowing capacity it had already funded.

### Impact

- A protected blocked job can continue to lose accrued capacity to backfill.
- The claim that backfill never delays accrual is false in the ordinary sense
  operators and users will infer.
- The top-K accrual mechanism does not by itself establish a starvation bound.

### Resolution requirements

- Make the safety semantics explicit in a superseding or amending ADR. At
  minimum, decide whether `None` forbids lending (`None => false`).
- If unbounded lending is retained for utilization, add a separately bounded
  mechanism—credit, maximum cumulative lend time, protected funded floor, or
  equivalent—that proves forward progress.
- State the starvation/progress property formally enough for a property test,
  including repeated scheduling passes and an adversarial stream of bounded
  backfill jobs.
- Add regression tests for an accrual with no finite projected release and for
  repeated backfill arrivals.

### Closure criteria

This issue is resolved when the design states a non-vacuous progress invariant
and scheduler tests demonstrate it under repeated backfill, not only for one
pass at a finite boundary.

## KOI-5: View and snapshot publication do not fit the 1M-job target

- **Severity:** High
- **Status:** Open
- **Affected capability:** target-scale operation, strong reads, scheduling,
  and snapshot recovery
- **Related design:** [clone-cost analysis](../architecture/coordinator-runtime.md#clone-cost-analysis),
  [ADR 0018](../decisions/0018-protobuf-records-in-parallel-containers.md)

### Required invariant

At the documented target of approximately one million queued/live jobs,
publishing read views and building snapshots must not stall the serial apply
path beyond its latency budget or require enough simultaneous full-state
copies to create a credible out-of-memory risk.

### Current violation

The coordinator-runtime design already estimates a deep clone of target-scale
state at hundreds of milliseconds to roughly one second and calls that
unacceptable. The implementation nevertheless deep-clones the full
`StateMachine` whenever a view is published
([view publication](../../crates/coppice-consensus/src/view.rs)). Strong-read
demand can request early publication, so this work occurs on the sole apply
task.

Snapshot capture makes another full clone in the apply task, then builds a
second record representation, per-shard encoded buffers, and finally a complete
container `Vec<u8>` before writing it
([snapshot capture](../../crates/coppice-consensus/src/apply_loop.rs),
[snapshot builder](../../crates/coppice-consensus/src/storage/sm.rs),
[snapshot codec](../../crates/coppice-consensus/src/storage/snapshot.rs)). At
target scale, several large copies can coexist.

The design promises a `coordinator_view_clone_seconds` histogram and an
apply-stall measurement to trigger the structural-sharing escape hatch; those
metrics are not implemented. The million-job tests are also ignored in the
normal test suite.

### Impact

- Long pauses in Raft application, follower catch-up, strong reads, scheduling,
  and event production.
- High transient memory use and possible process OOM during snapshots.
- The published scale target is not backed by a routinely exercised gate.

### Resolution requirements

- Instrument view publication and snapshot capture before claiming a supported
  operating envelope.
- Replace full deep clones with structural sharing, immutable/versioned state,
  or another representation whose snapshot acquisition is bounded and cheap.
- Stream locally built snapshot sections to the durable file rather than
  retaining all record copies, section buffers, and the complete container
  simultaneously.
- Establish explicit latency and peak-memory budgets at 10k, 100k, and 1M jobs.
- Run the 1M scheduler, state-consistency, and snapshot tests in a release-mode
  performance job with enforceable thresholds.

### Closure criteria

This issue is resolved when a repeatable target-scale test demonstrates bounded
apply stalls and peak memory for both view publication and snapshot creation,
and those bounds are enforced in CI or a required performance gate.
