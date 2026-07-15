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
| [KOI-2](#koi-2-job-submission-is-not-idempotent-across-an-unknown-outcome) | High | Public API | Resolved (2026-07-10, ADR 0026) | — |
| [KOI-3](#koi-3-event-cursors-depend-on-local-apply-batching) | High | Events / replication | Resolved (2026-07-10; drop-and-gap completeness 2026-07-12) | — |
| [KOI-4](#koi-4-unbounded-projected-ready-does-not-protect-accrual-progress) | High | Scheduling | Resolved (2026-07-10, ADR 0027) | — |
| [KOI-5](#koi-5-view-and-snapshot-publication-do-not-fit-the-1m-job-target) | High | Scalability | Open (clone cost, copies, instrumentation, and apply latency resolved) | Do not advertise the 1M-job target as supported until the release-mode performance gate runs in CI |
| [KOI-6](#koi-6-nothing-records-when-anything-happened-so-no-windowed-read-can-be-served) | High | Observability / API | Open (design settled 2026-07-15, ADR 0032) | Every time-ranged read stays unserved: no queue rates or history, no job timeline, no usage/utilization series, no events window |

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
  reconciliation, and node loss.~~ Done — see the attempt-`Terminal`
  resolution rules in
  [command-catalog.md](../architecture/command-catalog.md#resolution-on-attempt-terminal).
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
- **Status:** Resolved (2026-07-10) —
  [ADR 0026](../decisions/0026-client-minted-job-ids-idempotent-submission.md)
- **Affected capability:** public job submission and client retries
- **Related design:** [proposal lifecycle](../architecture/coordinator-runtime.md#proposal-lifecycle),
  [command idempotency](../architecture/command-catalog.md#idempotency-under-replay)

### Required invariant

Retrying one logical submission after a timeout, connection loss, or leader
change must create at most one durable job. This must hold even when the first
request committed but its response was lost.

### Violation (historical)

The proposal-lifecycle design argued that a duplicate `SubmitJob` is safe
because its `JobId` will already exist. That only protected re-proposal of the
same command. `SubmitJobRequest` carried no idempotency key or client-minted
job identity, while `CoordinatorControlPlane::submit_job` minted a new random
`JobId` for every invocation. A retry at the request boundary was therefore a
different command with a fresh identity: if the first request committed before
its outcome became unknown, both submissions could be accepted and both jobs
could execute.

### Impact (historical)

- Duplicate workload execution and duplicate resource consumption.
- Duplicate quota charging that is correct for the two records but incorrect
  for the user's single intent.
- Clients could not safely apply ordinary retry policies to transient API
  errors.

### Resolution

ADR 0026: the client mints the `JobId` (`SubmitJobRequest.job`, required) and
it is the submission's idempotency identity; a retry re-sends the identical
request. Resolution-requirement by resolution-requirement:

- ~~Add a stable idempotency identity to the public request; the scope and
  retention window must be explicit.~~ Done — the client-minted `JobId`. The
  window is the job's residence in replicated state: original commit until
  ADR 0012 eviction of the terminal record.
- ~~Persist enough deduplication state in the replicated state machine so a
  new leader gives the same answer.~~ Done with no new state: the replicated
  `jobs` map is the dedup table. Apply treats an identical resubmission as an
  accepted no-op (no events)
  ([apply](../../crates/coppice-state/src/apply.rs)).
- ~~Return the original `JobId` and commit index for a repeated completed
  request, and define the response for a key reused with a different
  payload.~~ Done — `SubmitJobResponse` echoes the id and carries `log_index`
  (the repeat's own apply index, a valid ADR 0007 cursor); a reused id with a
  different spec rejects deterministically as `SubmitSpecMismatch`
  ([API schema](../../proto/coppice/api/v1/api.proto),
  [API implementation](../../crates/coppice-coordinator/src/tasks/api_server.rs)).
- ~~Add an integration test that drops the first successful response, retries
  through another coordinator, and observes exactly one job~~ (done:
  `retried_submission_across_leader_change_creates_one_job` in
  [submit_retry.rs](../../crates/coppice-coordinator/tests/submit_retry.rs) —
  submit through the leader, discard the response, kill the leader, retry the
  identical request through the new leader; also covers the follower-redirect
  and spec-mismatch paths). No quota charge exists at submission (cost is
  charged at placement), so "one job" implies "one charge" here; the state
  tests additionally pin the no-op (`identical_resubmission_is_an_accepted_no_op`,
  `resubmission_stays_idempotent_after_abort_and_terminal_state` in
  [lifecycle.rs](../../crates/coppice-state/tests/lifecycle.rs)).

### Closure criteria

~~This issue is resolved when the public client can automatically retry an
unknown submission outcome without a possibility of creating a second job.~~
Met: within the documented dedup window, an identical retry can only resolve
to the original job on every replica and across leader changes.

## KOI-3: Event cursors depend on local apply batching

- **Severity:** High
- **Status:** Resolved (2026-07-10; drop-and-gap completeness 2026-07-12)
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

### Resolution (2026-07-12): drop-and-gap completeness

The drop-and-gap guarantee (ADR 0008) held only when a *later* event exposed
the discontinuity, so three edges could lose events with no gap notification —
and a lost `Ready` or `StopRequested` wedges dispatch until the next event that
never comes:

- **Trailing tap drop.** The tap detected an overflow only when a following
  batch revealed the `seq` jump. A batch dropped as the last event before an
  idle period was never surfaced. The sender now raises an out-of-band
  `DropSignal` (a monotonic emitted-count plus a `Notify`) and the receiver
  surfaces a *trailing* gap the moment the channel idles with drops outstanding
  ([events](../../crates/coppice-consensus/src/events.rs)).
- **Trailing per-subscriber overflow, and lost replay gaps.** A subscriber
  whose queue overflowed was marked gapped but only re-notified on the next
  batch; a cursor replay that filled the queue dropped its gap marker and then
  recorded the subscriber as *not* gapped. The fanout now carries the pending
  gap into the subscriber and a timer-driven `flush_gaps` re-delivers it once
  the queue drains
  ([fanout](../../crates/coppice-coordinator/src/tasks/event_fanout.rs)).
- **Silent replay across a discontinuity.** Tap gaps, restarts, and snapshot
  installs were not represented in the reconnection ring, so a later subscriber
  could replay straight across the hole. The ring now tracks a monotonic replay
  *floor* — raised by eviction, tap gaps, and snapshot installs, and seeded with
  the recovered applied index — and a cursor below it opens with a gap instead
  of a silent replay. A snapshot install forces the discontinuity into the
  stream via `EventTap::force_gap`
  ([apply loop](../../crates/coppice-consensus/src/apply_loop.rs)).

### Closure criteria

Met: event derivation is invariant under apply batching
(`event_stream_is_invariant_under_apply_batching`) and scoped subscriptions
deliver the complete documented event set
(`job_filter_admits_attempt_and_allocation_events`,
`node_filter_admits_attempt_and_allocation_events`). Drop-and-gap completeness
is pinned by `trailing_drop_surfaces_a_gap_without_a_later_batch`,
`force_gap_surfaces_on_an_idle_tap`, `replay_overflow_stays_gapped_until_flushed`,
`cursor_below_the_floor_opens_with_a_gap`, and `tap_gap_raises_the_ring_floor`.

## KOI-4: Unbounded `projected_ready` does not protect accrual progress

- **Severity:** High
- **Status:** Resolved (2026-07-10) —
  [ADR 0027](../decisions/0027-finite-projected-ready-accrual-protection.md)
- **Affected capability:** strict backfill and starvation protection
- **Related decision:** [ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md),
  amended by ADR 0027

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

~~The materialised scheduler design later declares
`projected_ready(A) = None`—meaning no finite guaranteed release events are
known—to satisfy the check automatically. The implementation follows that
rule with `None => true`.~~ **Resolved 2026-07-10.**
[ADR 0027](../decisions/0027-finite-projected-ready-accrual-protection.md)
amends ADR 0014: an indefinite `projected_ready` now *forbids* the lend
(`None => false`), accrual placement prefers nodes that yield a finite bound
(never an indefinite node while a finite one is eligible), and re-planning
moves an existing accrual to a node that meaningfully improves its bound —
mandatorily when indefinite becomes finite
([scheduler design](../scheduling/scheduler-v1.md#strict-backfill-the-revoke-and-reseat-lend),
[scheduler implementation](../../crates/coppice-scheduler/src/engine.rs)).

The previous behavior made the guarantee vacuous for the allocations that
most need protection: a succession of bounded jobs could repeatedly revoke
and reseat an accrual behind new work when it had no finite
`projected_ready`, including borrowing capacity it had already funded. Under
ADR 0027 such an accrual keeps everything it accrues: its node lends
nothing, so funding is monotone.

### Impact

(Historical, before the resolution.)

- A protected blocked job could continue to lose accrued capacity to
  backfill.
- The claim that backfill never delays accrual was false in the ordinary
  sense operators and users would infer.
- The top-K accrual mechanism did not by itself establish a starvation bound.

### Resolution requirements

- ~~Make the safety semantics explicit in a superseding or amending ADR. At
  minimum, decide whether `None` forbids lending (`None => false`).~~ Done —
  ADR 0027 decides `None => false`.
- ~~If unbounded lending is retained for utilization, add a separately bounded
  mechanism—credit, maximum cumulative lend time, protected funded floor, or
  equivalent—that proves forward progress.~~ Not applicable — unbounded
  lending is not retained; a lend-credit mechanism remains ADR 0027's
  documented escape hatch if the utilization cost is ever measured to matter.
- ~~State the starvation/progress property formally enough for a property
  test, including repeated scheduling passes and an adversarial stream of
  bounded backfill jobs.~~ Done — ADR 0027's P1–P3.
- ~~Add regression tests for an accrual with no finite projected release and
  for repeated backfill arrivals.~~ Done —
  `no_lend_when_the_accruals_bound_is_indefinite` and
  `an_indefinite_accrual_survives_an_adversarial_backfill_stream` in
  [the scheduler's behavioural tests](../../crates/coppice-scheduler/tests/engine.rs),
  plus finite-first placement and improvement-move coverage.

### Closure criteria

Met: ADR 0027 states the non-vacuous progress invariant, and the scheduler
tests demonstrate it under a repeated adversarial stream of bounded backfill
arrivals across passes — the accrual is never revoked by a lend, its funded
capacity is monotone, and it funds the instant its unbounded holder releases,
exactly as with no backfill stream.

## KOI-5: View and snapshot publication do not fit the 1M-job target

- **Severity:** High
- **Status:** Open — the clone-cost, copy-count, and instrumentation halves
  are resolved (2026-07-10,
  [ADR 0028](../decisions/0028-persistent-state-maps.md)), as is the
  serial-apply-latency half (2026-07-12); the enforced performance gate is not
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

~~The coordinator-runtime design already estimates a deep clone of
target-scale state at hundreds of milliseconds to roughly one second and
calls that unacceptable. The implementation nevertheless deep-clones the full
`StateMachine` whenever a view is published.~~ **Resolved 2026-07-10.**
ADR 0028 makes the job-scaled maps persistent (`imbl::OrdMap`), so the
publish-time clone is O(1) structural sharing at any job count
([view publication](../../crates/coppice-consensus/src/view.rs)); an ignored
release-mode test bounds the 1M-job clone.

~~Snapshot capture makes another full clone in the apply task, then builds a
second record representation, per-shard encoded buffers, and finally a
complete container `Vec<u8>` before writing it.~~ **Resolved 2026-07-10.**
Capture reuses the published view's clone
([snapshot capture](../../crates/coppice-consensus/src/apply_loop.rs)),
sections are converted and encoded lazily per shard straight from the
captured state ([snapshot builder](../../crates/coppice-consensus/src/storage/sm.rs)),
and both build and install stream the container to/from the durable file
([snapshot codec](../../crates/coppice-consensus/src/storage/snapshot.rs)).
Peak build memory is the live state plus a structurally shared capture plus
a bounded set of in-flight section buffers.

~~The design promises a `coordinator_view_clone_seconds` histogram and an
apply-stall measurement to trigger the structural-sharing escape hatch; those
metrics are not implemented.~~ **Resolved 2026-07-10.** The histogram,
apply-batch and snapshot-capture/build stall timings, and state-size gauges
exist behind the `describe_metrics()`/`gather_metrics()` module pattern; a
/metrics endpoint to export them is still future work. The million-job tests
remain ignored in the normal test suite, and no release-mode performance job
runs them — this half stays open.

~~Applying a proposal at target scale stalls serial apply on its own, before
any publication cost: `free_capacity` scanned every allocation, so a full
cycle (~500 placements, ~500 revocations against ~1M allocations) applied in
tens of seconds — the 1M throughput test measured only scheduling and never
caught it.~~ **Resolved 2026-07-12.**
`commit_placements` now builds a per-node free-capacity memo once and consults
it for every read in the batch (state-external, thrown away after the call),
making the apply O(N + batch·log N); the 1M cycle applies in ~0.1 s, and
`schedule_apply.rs` now times the apply under a 1 s budget.

### Impact

- ~~Long pauses in Raft application, follower catch-up, strong reads,
  scheduling, and event production.~~ Resolved: publish and capture are O(1)
  on the apply task; apply pays a modest O(log n) path-copy overhead
  (single-digit percent on the recovery-replay benchmark).
- ~~High transient memory use and possible process OOM during snapshots.~~
  Resolved: no full-state protobuf copy or whole-container buffer exists on
  either the build or install path.
- The published scale target is not backed by a routinely exercised gate.

### Resolution requirements

- ~~Instrument view publication and snapshot capture before claiming a
  supported operating envelope.~~ Done — `coordinator_view_clone_seconds`,
  apply-batch/snapshot-capture/snapshot-build histograms, state-size gauges.
- ~~Replace full deep clones with structural sharing, immutable/versioned
  state, or another representation whose snapshot acquisition is bounded and
  cheap.~~ Done — ADR 0028.
- ~~Stream locally built snapshot sections to the durable file rather than
  retaining all record copies, section buffers, and the complete container
  simultaneously.~~ Done — sections encode lazily from the captured state and
  stream to the spool file.
- Establish explicit latency and peak-memory budgets at 10k, 100k, and 1M jobs.
- Run the 1M scheduler, state-consistency, clone-cost, and snapshot tests in a
  release-mode performance job with enforceable thresholds.

### Closure criteria

This issue is resolved when a repeatable target-scale test demonstrates bounded
apply stalls and peak memory for both view publication and snapshot creation,
and those bounds are enforced in CI or a required performance gate.

## KOI-6: Nothing records *when* anything happened, so no windowed read can be served

- **Severity:** High
- **Status:** Open — the design is settled (2026-07-15,
  [ADR 0032](../decisions/0032-advisory-event-timestamps.md)); the
  implementation is not
- **Affected capability:** every time-ranged read on the public API — the
  overview's queue rates and history and its recent-events window, the job
  timeline, job usage and node utilization series, and the ADR 0008 event
  subscription. *Narrowed by ADR 0032:* measured usage series (`GetJobUsage`
  samples and node utilization's `used` half) are measurements, not
  transitions — no command carries them, so no event derivation can serve
  them; they wait on a separate off-consensus measurement-pipeline decision
  and are no longer in this issue's scope
- **Related decisions:** [ADR 0031](../decisions/0031-http-api-surface.md)
  (the route map that promises these reads),
  [ADR 0008](../decisions/0008-event-delivery-guarantees.md) (event delivery),
  [ADR 0012](../decisions/0012-data-retention.md) (history store)
- **Related issue:** [KOI-1](#koi-1-terminal-job-eviction-can-destroy-the-only-history)
  — the same missing history store; KOI-1 is about *losing* the current record,
  this issue is about never having recorded the *transitions* in the first place

### Required invariant

Every read model the API promises must be servable from something the
coordinator actually retains. Point-in-time reads (what is queued, what is
running, what is allocated) project from replicated state. Time-ranged reads
(what happened to this job and in what order; how fast the queue drained over
the last hour; what this node consumed over the last day) require an ordered,
retained record of transitions, each attributable to a wall-clock instant that
every replica agrees on. Where such a record does not exist, the API must say
so — an absent window, never a fabricated zero.

### Current violation

No such record exists anywhere in the system, for three compounding reasons:

1. **Events carry no time.** `coppice_state::Event` is the derived output of
   apply, and apply may not read a clock (the determinism contract in
   [coppice-state](../../crates/coppice-state/src/lib.rs)). So the one
   representation of "a thing happened" is unstamped.
2. **Nothing retains events.** The fanout ring is a *reconnection buffer, not
   history* (bounded 1 h / 1M events, evict-oldest — see the channel inventory
   in [coordinator-runtime.md](../architecture/coordinator-runtime.md)),
   replica-local, and seeded only from the index this process recovered at. The
   only other sink is `StubHistoryStore`, which logs a count (KOI-1).
3. **Replicated state records facts, not transitions.** It carries a handful of
   per-record timestamps (`submitted_at_us`, `terminal_at_us`,
   `started_at_us`) — enough for an age, nowhere near enough to reconstruct a
   sequence of state changes or to bucket them into a window.

The consequence is visible in the shipped surface. `GET /api/v1/overview`
([routes](../../crates/coppice-api/src/http/routes.rs),
[projection](../../crates/coppice-api/src/http/project.rs)) serves queue depth,
the by-phase tally, and the oldest queued age from replicated state, but:

- `drain_rate_per_minute` and `arrival_rate_per_minute` are `null` and
  `history` is `[]` — a rate is a windowed quantity and nothing retains the
  window;
- the response has **no `recent_events` field at all**, unlike the UI's
  `ClusterOverview` in `web/src/api/types.ts`. Serving `[]` would have meant
  inventing a `TimelineEvent` wire shape — and freezing it into the v1
  contract — before this issue decides where events and their timestamps come
  from ([dto](../../crates/coppice-api/src/http/dto.rs)).

`GetJobTimeline`, `GetJobUsage`, `GetNodeUtilization`, `GetNodeHistory`, and
`SubscribeEvents` remain `501 UNIMPLEMENTED` for the same root cause; the ADR
0031 table already classes them bounded/eventual, so the routing is not what
blocks them.

### Impact

- The UI's queue sparklines, queue chart, and events feed have no data source,
  and the per-job timeline page cannot be built at all.
- Operators cannot answer *"what happened to this job, in what order, and
  when"* after the fact — the first question of every incident. Compounded by
  KOI-1: eviction then destroys even the current-state record.
- Any implementation tempted to fill the gap with `0.0` rates or synthesized
  events would make a cluster with no observability indistinguishable from a
  healthy cluster with no activity. The endpoints must keep failing honestly
  until they can answer.

### Likely direction: advisory event timestamps

**Settled 2026-07-15 by
[ADR 0032](../decisions/0032-advisory-event-timestamps.md)**, which confirms
this direction and answers the open questions below: batch-level stamping via
an exhaustive `Command::stamped_at_us()` (per-event is the identical value,
since every command carries exactly one stamp); sub-items inherit the batch's
stamp under the documented "proposer-asserted time" semantics; skew is stored
raw with all ordering on `(index, ordinal)` and clamping left to consumers;
and the retention line is three tiers (fanout ring for reconnection and the
recent-events cache, a history event table for timelines, in-memory 30 s
buckets for windowed stats). The section below is preserved as the original
derivation:

**Every command already carries a proposer-stamped timestamp** —
`submitted_at_us`, `requested_at_us`, `observed_at_us`, `dispatched_at_us`,
`declared_at_us`, and so on
([command catalog](../../crates/coppice-state/src/command.rs)). Apply can
therefore stamp each `Event` it emits with the timestamp of the command that
produced it. That is *deterministic*: the timestamp rides in the replicated
log, not off the applying replica's clock, so every replica derives the same
value for the same log — no determinism contract is touched, and apply's
latency budget is unaffected (it is a copy, not a syscall).

The timestamp is **advisory only**:

- apply never reads it back, never branches on it, and never lets it influence
  a decision or a rejection — a skewed or hostile proposer clock can make a
  timeline look odd but cannot corrupt state;
- it is not the ordering key. The Raft log index remains the order (KOI-3), and
  events already arrive one batch per command tagged with that index;
- it exists to render a human-facing timeline and to bucket transitions into
  windows after the fact.

With events timestamped, the durable history sink KOI-1 already requires
becomes the natural home for a retained per-job transition timeline, and the
windowed stats (queue rates and history, job usage, node utilization) become
projections over that retained record rather than new replicated structures.
It also unblocks the two payloads that are stuck purely for want of an `at_us`:
the overview's `recent_events` window and the ADR 0008 SSE event body.

Questions the ADR still has to settle:

- **Placement.** Per-`Event` or per-`EventBatch`? Since KOI-3 made batches
  one-per-command, a batch-level stamp is equivalent and cheaper.
- **Nested items.** Which timestamp is authoritative for events derived from a
  sub-item of a command (`ReconcileNode.observed_at_us` covering the
  `LostAttempt` entries it carries).
- **Skew and monotonicity.** A laggy proposer can stamp log index N+1 earlier
  than N. Clamp to monotonic at the edge, or expose the raw value and let the
  reader see it? "Advisory" argues for exposing it, but a timeline that goes
  backwards is a support ticket.
- **Retention line.** What the fanout ring keeps (reconnection only), what the
  history store keeps (per-job timeline), and what a derived stats store keeps
  (pre-bucketed series) — and which of the three each API read is served from.

### Resolution requirements

- ~~Record the event-timestamp design in an ADR: advisory semantics,
  placement, nested-item rule, skew/monotonicity handling, and the retention
  line above.~~ Done —
  [ADR 0032](../decisions/0032-advisory-event-timestamps.md).
- Stamp events deterministically from the proposing command's timestamp, and
  pin it with a determinism test (an identical log yields identical
  `(index, at_us, events)` on every replica).
- Retain transitions durably and queryably — the KOI-1 history store is the
  natural home — with an explicit bounded window.
- Serve them from one representation: the overview's `recent_events`,
  `GetJobTimeline`, and the ADR 0008 subscription payload must not each invent
  their own event shape. Queue rates/history and the usage/utilization series
  are then projections over the retained record.
- Keep failing honestly in the meantime: an endpoint with no retained window
  answers `null`/absent, never `0`.

### Closure criteria

This issue is resolved when a job's full transition timeline is queryable after
the fact from a coordinator that never observed it live (a restarted process,
or a different replica), the overview serves non-null queue rates and a
recent-events window from retained data, and a determinism test pins identical
event timestamps across replicas for an identical committed log.
