# Command Catalog and Apply Contract

This document is the authoritative catalog of every command in the Raft log
and the precise contract under which `StateMachine::apply` executes them. The
protobuf schema (ADR
[0003](../decisions/0003-protobuf-serialization-and-cluster-version-gates.md))
is frozen from this catalog as `coppice.command.v1`
(`proto/coppice/command/v1/command.proto`, evolved per
[schema-style](schema-style.md)); the code-side anchor is
`crates/coppice-state/src/command.rs` and `apply.rs`.

Settled inputs, not re-decided here: the three state machines and abort
semantics ([ADR 0013](../decisions/0013-job-attempt-allocation-state-machines.md)),
accruing allocations and commit-order funding
([ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md)),
fencing and reconciliation
([ADR 0009](../decisions/0009-fencing-and-reconciliation.md)), commanded
eviction ([ADR 0012](../decisions/0012-data-retention.md)), cost-based soft
quotas ([ADR 0005](../decisions/0005-cost-based-soft-quotas.md)) with the
fixed-point arithmetic of
[ADR 0019](../decisions/0019-deterministic-quota-arithmetic.md).

## The apply contract

### Apply is a pure function of (state, command)

`apply(state, command) → (state′, result)` — nothing else. Recovery replays
committed commands from a snapshot, so any dependence on anything but the
two arguments is a divergence bug. Concretely:

- **No wall clock.** Every command that needs a time carries it as an
  explicit `*_at_us` field (Unix microseconds, `int64`), stamped by the
  proposer. Apply only ever reads timestamps out of the command.
- **No randomness, no id minting.** Every `JobId`, `AttemptId`,
  `AllocationId`, and `GroupId` is minted by the proposer and carried in the
  command. Apply never generates an identifier — this is why a retry
  resolution returns the job to `Queued` rather than creating the next
  attempt: the fresh `AttemptId` belongs to the next `CommitPlacements`.
- **No unordered iteration.** All state collections are `BTreeMap`; where
  the ADRs demand an order other than key order (funding), the order is an
  explicit part of state (the per-node accrual queue, keyed by commit
  sequence).
- **No panics on bad input, ever.** A command that cannot be applied is a
  rejection (below), not a crash. All arithmetic is saturating
  (`coppice_core::quota`, `Resources` helpers).

### Rejection semantics

A command that fails validation **was still committed to the log on every
replica** — Raft already accepted it; only application can refuse it. The
contract:

- A failed command applies as a **deterministic no-op**: no state field
  changes except `version`.
- The apply result is `Result<Applied, RejectionReason>`. `RejectionReason`
  is a closed taxonomy (see the table at the end); every replica computes
  the identical rejection for the identical command. The proposer observes
  the rejection **through the apply result on the leader** (the client
  response / scheduler feedback path), never by comparing state across
  replicas — state never diverges, so it cannot carry the signal.
- **`version` bumps on rejection too.** `version` counts applied log
  entries, accepted or not, so it is a stable coordinate for
  `expected_version` and for read-consistency cursors (ADR 0007/0008).
- Validation runs **before** any mutation. Apply for each command is
  organized as a read-only validation phase that either produces a
  rejection or a plan whose application is infallible. There is no partial
  application: a rejected command has zero effects, including inside
  batches.

Proposers are expected to pre-validate (the API rejects bad submissions
synchronously; the leader's ingestion layer drops stale agent reports), so
apply-time rejections are the backstop for races — two commands in flight
where the earlier one invalidates the later — and for proposer bugs. Both
are normal operation, not exceptional control flow.

### `version` and `expected_version`

`CommitPlacements` carries the `expected_version` its scheduler snapshot was
taken at, per the "assign these jobs to these nodes under this expected
state version" rule in
[high-availability.md](high-availability.md). Its semantics:

- **Semantic validation is authoritative, not version equality.** Every item
  in the batch is re-validated against *current* state (job still `Queued`,
  node schedulable, allocation still `Accruing`, ids fresh, capacity sane,
  accrual cap respected). If every item passes, the batch commits even if
  `version` has advanced past `expected_version`. Rationale: `version`
  bumps on *every* command, including `SubmitJob`s that cannot invalidate a
  placement; strict equality would starve the scheduler on any busy cluster.
  [scheduling-model.md](../scheduling/scheduling-model.md) says failed
  proposals are normal — it does not say all proposals fail. A rejection
  must mean "the world changed in a way that invalidates these decisions",
  and the per-item checks are exactly that predicate. `expected_version`
  remains in the payload as the audit/debugging record of what the
  scheduler saw.
- **All-or-nothing per batch, with per-item diagnostics.** If any item fails,
  the whole batch rejects with `InvalidBatch`, listing `(item index,
  reason)` for every failing item. A batch is one atomic re-plan: its
  placements may depend on capacity its revocations free, its quota charges
  land together, and the accrual-cap check is a property of the batch as a
  whole. Applying the valid subset would commit a cluster state the
  scheduler never computed and reasoned about; rejecting the whole batch
  costs one recomputation, which the scheduler's operating model already
  treats as routine.

### The agent-report ingestion boundary

**Raw agent messages are not commands.** The leader runs a normalization
layer between the agent protocol (`coppice_proto::agent`) and the log:

1. **Fencing check** (ADR 0009): reports carrying a stale `node_epoch` are
   demoted to reconciliation input; they never become attempt-progress
   commands.
2. **Dedupe** by `(AttemptId, attempt_state)`: the attempt machine is
   monotonic, so the normalizer proposes at most one command per attempt
   per state. Duplicates that slip through (leader change mid-stream) are
   caught deterministically at apply by the same monotonicity check and
   reject as `StaleAttemptState` — a benign rejection the proposer ignores.
3. **Timestamping**: the leader stamps `observed_at_us` on the command at
   proposal time. Apply never asks what time it is.
4. **Decision-making**: anything judgement-shaped is resolved *before* the
   log. The ObservedSet diff (adopt / stop / lost) is computed by the
   leader; the command carries the verdicts. Outcome classification
   (`PullFailed { user_error }`) is resolved by the normalizer from error
   detail. Lost-attempt runtimes are computed by the normalizer and carried
   in the command.

The commands in the *agent ingestion* section below are therefore already
deterministic facts ("this attempt was observed running at T"), and apply's
only job is to order them against everything else in the log and fold them
into state.

The "stop" verdict of the ObservedSet diff never appears in a command: a
running container with no replicated intent has, by definition, nothing in
state to mutate. The leader sends `StopJob` directly (the agent journals a
tombstone); replicated state is untouched.

### The funding algorithm

Funding is deterministic bookkeeping in the apply loop (ADR 0014). Exactly:

- **Order.** `StateMachine.next_allocation_seq` is a monotonic counter in
  replicated state; every allocation created by `CommitPlacements` takes the
  next value as its `seq`. `seq` is the tie to log order: allocation A was
  committed before B iff `A.seq < B.seq`. **Commit order means `seq` order,
  never `AllocationId` order** — ids are UUIDs and their ordering is
  meaningless and nondeterministic across histories.
- **The queue.** `accrual_queue: BTreeMap<(NodeId, seq), AllocationId>`
  holds exactly the allocations in state `Accruing`, keyed so that a range
  scan over one node yields its accruing allocations in commit order. The
  queue is part of replicated state and of state equality.
- **The trigger.** Whenever capacity on node N becomes free during apply —
  an allocation on N is `Released` (attempt terminal, abort, revocation,
  node re-registration growing capacity) — apply runs one **pledge pass**
  over N.
- **The pledge pass.** Compute N's free capacity (advertised capacity minus
  the `funded` vectors of all non-`Released` allocations on N, saturating).
  Walk N's accrual queue in ascending `seq`. For each allocation, pledge
  per-dimension `min(free_d, requested_d − funded_d)`, add it to
  `funded`, subtract it from `free`. Partial pledges accumulate
  monotonically; `funded` never decreases while the allocation lives. The
  head of the queue takes what it needs of each dimension before anything
  flows to the next — dimensions it doesn't need flow past it.
- **`Funded` flips** when `funded == requested` on every dimension. The
  allocation leaves the accrual queue and becomes stable: funded
  allocations are never revoked in v1.
- **The `Ready` barrier** is an AND over the placement group: when an
  allocation flips `Funded`, apply checks every *live* (non-terminal)
  attempt in the same `GroupId`; if each has all of its allocations
  `Funded`, all of the group's `Accruing` attempts flip to `Ready` in the
  same apply. In v1 every group is a singleton (`GroupId` = the job's id),
  so the barrier is per-attempt; the evaluation is written over the group
  from day one so gang scheduling adds members, not mechanism.
- Nodes that are drained (`schedulable = false`) keep funding their
  existing accruing allocations — drain blocks *new placements*, not the
  completion of committed holds. A *lost* node's allocations are all
  released with the attempts, so no pledge pass runs there.

### Resolution on attempt `Terminal`

The job carries the id of its one in-flight attempt via `Attempting(id)`
(ADR 0030); there is no job-level `Finalizing` rest state. Every attempt end
resolves the job in the same apply that lands the attempt's
`Terminal(outcome)`, and the resolution rules live **here, in apply** —
never in the agent (ADR 0013). On an attempt reaching `Terminal(outcome)`
the same apply resolves the job:

1. **Truth wins the race.** The recorded terminal state derives from the
   outcome that actually ended the attempt. `Aborted` job state is reached
   only when the attempt outcome is `Aborted` — a container that exited
   naturally while an abort was in flight keeps its real outcome
   (`Succeeded` for exit 0, `Failed` otherwise), with `abort_requested`
   still visible in history.
2. **Abort wins over retry.** If `abort_requested` is set, resolution never
   returns the job to `Queued`, whatever the outcome class. (The only
   non-`Aborted` terminal a pending abort can produce this way is the
   truth-wins case above, or `Aborted` job state when a revocation raced
   the abort — a combination the ordered log makes unreachable, specified
   for completeness.)
3. **`Revoked` requeues free.** Job returns to `Queued`; `retries_used` is
   not incremented. `Revoked` is pre-`Running` and `Platform`-class, so
   true-up's refund fraction (ADR 0029) is 1000 either way: the attempt
   never ran and true-up returns the full (decayed) charge, exactly as
   under ADR 0019.
4. **Retry policy** for everything else:
   - `Success` → job `Succeeded`.
   - `MaxRuntimeExceeded` → job `Failed`, never retried (deterministic
     recurrence; opt-in does not apply).
   - Other `UserError` outcomes → retried only if the job's retry policy
     opts in (`retry_user_errors`) and `retries_used < max_retries`;
     otherwise `Failed`.
   - `Platform` outcomes → retried while `retries_used < max_retries`;
     otherwise `Failed`.
   - A retry is: `retries_used += 1` (except `Revoked`), job → `Queued` —
     the state transition drops the attempt id. The next attempt is created
     by a future `CommitPlacements`.
5. **Quota true-up** (ADR 0019, refund fraction per ADR 0029) runs in the
   same apply: actual cost is computed from the rate and multiplier stored
   in the attempt's charge record (policy edits never retroactively
   reprice); an attempt that never reached `Running` has actual cost zero;
   the refund is `unused × f / 1000` where `f` is the fraction recorded on
   the charge record at placement, decayed from charge time to the
   command's timestamp; every ancestor entity is touched then settled. `f`
   is always 1000 (full refund) unless the attempt reached `Running`, the
   outcome's `OutcomeClass` is not `Platform`, and the job declared an
   enforced `max_runtime` — otherwise the recorded `refund_fraction_milli`
   applies.
6. **Allocations release** in the same apply, running the pledge pass on
   each affected node.
7. **Terminal timestamp.** A resolution that lands terminal stamps the
   job's `terminal_at_us` from the resolving command's proposer timestamp —
   an abort's `requested_at_us`, an outcome or reconcile report's
   `observed_at_us`, a loss declaration's `declared_at_us`. A requeue
   leaves it unset. The `EvictTerminalJobs` retention clock (ADR 0012)
   runs from this stamp, never from `submitted_at_us`: a job may
   legitimately queue longer than the retention interval before it runs.

The job stays `Attempting(id)` for the whole attempt lifetime, including the
window between `RecordAttemptExited` (exit observed) and
`RecordAttemptOutcome` (outcome recorded) — that window is the *attempt*
resting in `Finalizing`, not the job. The job itself changes state exactly
once, atomically, when the attempt reaches `Terminal`.

### Idempotency under replay

Replay safety follows from purity plus explicit idempotency points:

- Snapshot-then-replay reproduces state exactly because every apply is a
  function of (state, command) — the determinism harness
  (`crates/coppice-state/tests/`) asserts replica equivalence and
  mid-stream snapshot equivalence on generated command sequences.
- *Re-proposal* (the same logical fact proposed twice, e.g. across a leader
  change) is absorbed by monotonicity: duplicate attempt-progress commands
  reject as `StaleAttemptState`; duplicate `EvictTerminalJobs` entries skip
  already-evicted ids; a duplicate `SubmitJob` with the identical spec is an
  accepted no-op — the job id is client-minted and is the submission's
  idempotency identity (ADR 0026), so this same point also absorbs a
  *client retry* after an unknown outcome, not just command re-proposal.
  Every duplicate resolves to a *rejection or no-op that is itself
  deterministic*, so replicas agree on the non-effect.

### Events

Each accepted command returns the list of change events it produced
(`JobStateChanged`, `AttemptStateChanged`, `AllocationFunded`,
`StopRequested`, …). Events are **derived output, not state**: they feed the
ADR 0008 event fanout and the coordinator runtime (dispatch and stop
signals), are produced deterministically, and are never read back by apply.
`StopRequested` is how apply tells the runtime "this abort needs a `StopJob`
sent" without doing I/O itself.

---

## Command catalog

Field types below are the frozen vocabulary for the proto task: ids are
UUIDs (16 bytes), timestamps are `int64` Unix µs named `*_at_us`, resource
vectors are `Resources`, cost quantities are `uint64` µCU (`CostUnits`),
fixed-point weights/multipliers are `uint64` Q32.32, the decay factor is
`uint64` Q0.64 (ADR 0019). Every command is one arm of the versioned
envelope `Command { version, oneof body }` (ADR 0003); all v1 commands are
cluster-version 1.

Every **API-proposed** command additionally carries
`actor: Actor { principal: string, groups: string[], operator_cert: bool }`,
transcribed by the API layer from the verified token or operator
certificate ([ADR 0023](../decisions/0023-scoped-role-bindings.md)). Apply
re-checks the actor's authority against the replicated bindings and job
ownership in its read-only validation phase — a pure lookup, rejecting
`PermissionDenied` — so authorization races resolve in log order. The
actor-carrying commands are `SubmitJob`, `AbortJob`, `SetNodeSchedulable`,
`ConfigureQuotaEntity`, `UpdatePolicy`, `UpdateAuthorization`, and
`BumpClusterVersion`; internal proposers (scheduler, ingestion, node
lifecycle, housekeeping) carry no actor and their command types are not
reachable through the API.

### API-proposed

#### `SubmitJob`

| | |
| --- | --- |
| Proposer | API layer, after synchronous admission checks |
| Payload | `job: Job` (id — **client-minted**, the submission's idempotency identity per ADR 0026 —, image, `requests: Resources`, `priority: i32`, `max_runtime_us: optional uint64`, `quota_entity: QuotaEntityId`, `retry: RetryPolicy { max_retries: u32, retry_user_errors: bool }`), `multiplier: PriorityMultiplier` (Q32.32 — the API resolves the user's `priority` through the replicated multiplier table at proposal time; apply never sees the raw `i32` in arithmetic, per ADR 0019), `actor: Actor`, `submitted_at_us` |
| Validation | `abort_requested` unset. If `job.id` is already in state (including terminal jobs not yet evicted): identical client-supplied spec → **accepted no-op** (idempotent resubmission, no events — the retryer observes success and the original job; the no-op creates nothing, so it skips the authorization check and never changes `submitted_by`); different spec → `SubmitSpecMismatch`. Otherwise `quota_entity` exists and the actor holds `submitter` (or higher) over it (ADR 0023). |
| Apply effects | Insert the job record with `submitted_by = actor.principal`; walk `Submitted → Accepted → Queued` in this one apply (admission is synchronous in v1 — the intermediate states exist for observability and appear as distinct events). No quota charge: cost is charged at placement, not submission. |
| Rejections | `SubmitSpecMismatch`, `UnknownQuotaEntity`, `InvalidCommand` (pre-set abort flag), `PermissionDenied` |

#### `AbortJob`

| | |
| --- | --- |
| Proposer | API layer (the user command; "abort" is the vocabulary everywhere) |
| Payload | `job: JobId`, `reason: optional string`, `actor: Actor`, `requested_at_us` |
| Validation | Job exists and is non-terminal; actor is the job's `submitted_by` or holds `operator`/`admin` over the job's quota entity (ADR 0023) |
| Apply effects | Set `abort_requested` (first request wins; a second `AbortJob` is an accepted no-op preserving the original). Then by current state: **no live attempt** (`Submitted`/`Accepted`/`Queued`) → job `Aborted` immediately, `terminal_at_us` stamped from `requested_at_us`. **Attempt `Accruing`/`Ready`** → attempt `Terminal(Aborted)` with the full terminal path (allocations released + pledge pass, true-up with actual cost 0, job `Aborted`) — no agent interaction. **Attempt `Dispatching`/`Running`** → flag only, emit `StopRequested { node, allocation }`; the runtime sends `StopJob` (tombstone rule / SIGTERM–grace–SIGKILL per ADR 0013) and the outcome arrives later via `RecordAttemptOutcome`. **Attempt `Finalizing`** → flag only; resolution honors abort-wins-over-retry. |
| Rejections | `UnknownJob`, `JobTerminal`, `PermissionDenied` |

### Scheduler-proposed

#### `CommitPlacements`

| | |
| --- | --- |
| Proposer | Scheduler engine via the leader — one batch per scheduling pass |
| Payload | `expected_version: u64` (audit record of the snapshot version; see contract), `proposed_at_us` (the charge timestamp), `revocations: AllocationId[]`, `placements: Placement[]` where `Placement = { job: JobId, attempt: AttemptId, group: GroupId, allocations: AllocationSpec[] }`, `AllocationSpec = { id: AllocationId, node: NodeId, requested: Resources }`. The proto field is repeated; **v1 writers emit exactly one allocation per placement and set `group` = the job's id** (singleton groups); apply rejects other shapes until the gang-scheduling ADR. |
| Validation (all-or-nothing, per-item diagnostics) | Revocations: allocation exists and is `Accruing` (funded allocations are stable — revoking one is always a rejection). Placements: job exists and is `Queued`; attempt and allocation ids are fresh; node exists and is schedulable; `requested` fits within the node's total advertised capacity; exactly one allocation, `group` = job id. Batch-level: after simulating the batch (revocation frees → pledge pass → new placements in order), the number of distinct jobs holding accruing allocations must not exceed the replicated accrual cap K. |
| Apply effects | In order: **(1) Revocations** — each attempt `Terminal(Revoked)`, allocations `Released`, true-up (full decayed refund; the attempt never ran), job → `Queued` free of retry budget (or `Aborted` if an abort is pending), freed capacity pledged onward in commit order. **(2) Placements**, in payload order: assign the allocation the next `seq`; insert attempt + allocation; run the pledge from the node's current free capacity — fully covered → allocation `Funded`, attempt starts `Ready` (accrual skipped, the common case); partially or not covered → allocation `Accruing` in the accrual queue, attempt starts `Accruing`. Job → `Attempting(attempt)`. **(3) Quota charge**: a job with no *enforced* `max_runtime` first has its multiplier inflated, `m' = ⌊multiplier × unbounded_runtime_multiplier / 2³²⌋` (else `m' = multiplier`); the job's full cost `C = rate(requests, current weights) × ceil(max_runtime_s) × m'` (jobs with no `max_runtime` are charged the replicated `default_charge_runtime` at `m'`) is charged to every ancestor of its entity at `proposed_at_us`; the attempt records `(C, rate, m', proposed_at_us, refund_fraction_milli)` for true-up, where the recorded fraction is the replicated `refund_fraction_milli` for a bounded job or 1000 for an unbounded one (ADR 0029). |
| Rejections | `InvalidBatch[(index, reason)]` wrapping `UnknownAllocation`, `AllocationNotAccruing`, `UnknownJob`, `JobNotQueued`, `DuplicateAttempt`, `DuplicateAllocation`, `UnknownNode`, `NodeNotSchedulable`, `RequestExceedsNodeCapacity`, `UnsupportedPlacementShape`, `UnknownQuotaEntity`; batch-level `AccrualLimitExceeded` |

#### `DispatchAttempt`

| | |
| --- | --- |
| Proposer | Leader dispatch loop. Ordering is load-bearing: `Dispatching` is committed **before** `StartJob` is sent, because the replicated `Dispatching`/`Running` set is the "intended" side of the ObservedSet diff (ADR 0009) — a crash between commit and send is reconciled as *lost*, never as an untracked container. |
| Payload | `attempt: AttemptId`, `dispatched_at_us` |
| Validation | Attempt exists and is `Ready` |
| Apply effects | Attempt → `Dispatching`. Allocations stay `Funded` (they flip `Active` when the container is observed). |
| Rejections | `UnknownAttempt`, `StaleAttemptState` |

### Agent ingestion (leader-normalized; see the boundary contract)

#### `RecordAttemptStarted`

| | |
| --- | --- |
| Proposer | Ingestion, from an `AttemptStatus` report observing the container running |
| Payload | `attempt: AttemptId`, `observed_at_us` |
| Validation | Attempt exists and is `Dispatching` (the agent can only start what was dispatched; anything else is a stale or duplicate report) |
| Apply effects | Attempt → `Running`, `started_at_us` recorded (the anchor for "reached Running" in true-up); allocation → `Active`; job state is unchanged — it stays `Attempting(id)` (ADR 0030: agent-observed transitions no longer move the job). |
| Rejections | `UnknownAttempt`, `StaleAttemptState` |

#### `RecordAttemptExited`

| | |
| --- | --- |
| Proposer | Ingestion, when exit is observed but agent-side finalization (log flush, usage summary) is still running |
| Payload | `attempt: AttemptId`, `observed_at_us` |
| Validation | Attempt exists and is `Running` |
| Apply effects | Attempt → `Finalizing`; job state is unchanged — it stays `Attempting(id)` (there is no job-level `Finalizing`; see ADR 0030). Skipping this command (outcome arriving directly) is legal — the terminal edge exists from every non-terminal state. |
| Rejections | `UnknownAttempt`, `StaleAttemptState` |

#### `RecordAttemptOutcome`

| | |
| --- | --- |
| Proposer | Ingestion, from the terminal `AttemptStatus` report (natural exit, OOM, `max_runtime` kill, abort completion, pull/start failure) |
| Payload | `attempt: AttemptId`, `outcome: AttemptOutcome` (the full ADR 0013 taxonomy except `Revoked`, which only `CommitPlacements` may produce), `actual_runtime_us: uint64` (normalizer-computed), `observed_at_us` |
| Validation | Attempt exists and is non-terminal; outcome ≠ `Revoked` |
| Apply effects | Attempt → `Terminal(outcome)` (legal from any non-terminal state — early endings arrive without prior started/exited commands); allocations `Released` + pledge pass; quota true-up (actual cost 0 if the attempt never reached `Running`, else from recorded rate × `ceil(actual_runtime_s)` × recorded multiplier; refund is the recorded `refund_fraction_milli` of the unused portion, or the full unused portion whenever the attempt never reached `Running` or `outcome`'s `OutcomeClass` is `Platform` — ADR 0029); job resolution per the rules above (retry / terminal / abort-wins / truth-wins), moving `Attempting(id)` directly to a terminal state or back to `Queued`. |
| Rejections | `UnknownAttempt`, `StaleAttemptState`, `InvalidCommand` (outcome `Revoked`) |

#### `ReconcileNode`

| | |
| --- | --- |
| Proposer | Ingestion, from an ObservedSet report (agent restart registration, or the periodic heartbeat diff). The leader computes the diff; the command carries verdicts. |
| Payload | `node: NodeId`, `node_epoch: u64` (the epoch the set was observed under), `adopted: AttemptId[]`, `lost: LostAttempt[]` where `LostAttempt = { attempt: AttemptId, outcome: AttemptOutcome, actual_runtime_us: uint64 }` (normalizer picks the outcome — typically `AgentError`; `NodeLost` and `StartFailed` are legal), `observed_at_us` |
| Validation | Node exists; `node_epoch` equals the node's current epoch (a stale epoch means the whole set predates a re-registration and is worthless); every referenced attempt exists and lives on this node; lost outcomes ≠ `Revoked` |
| Apply effects | **Adopted** (intended and running): attempt confirmed `Running` — `Dispatching → Running` with allocation `Active` (job state is unchanged, per ADR 0030); already-`Running` or already-terminal entries are no-ops (stale info, benign). **Lost** (intended but absent): the full terminal path with the carried outcome, identical to `RecordAttemptOutcome` — retry policy applies. The *stop* verdict never reaches apply (see the ingestion boundary). |
| Rejections | `UnknownNode`, `StaleNodeEpoch`, `InvalidBatch` wrapping `UnknownAttempt` / `AttemptNotOnNode` / `InvalidCommand` |

### Node lifecycle

#### `RegisterNode`

| | |
| --- | --- |
| Proposer | Leader, on agent (re)registration |
| Payload | `node: Node` (id, `capacity: Resources`, labels), `registered_at_us` |
| Validation | None beyond shape — registration is always legal |
| Apply effects | New node: insert with `epoch = 1`, `schedulable = true`. Existing node: **bump `node_epoch`** (invalidating all commands issued under earlier epochs, per ADR 0009), update capacity and labels; the drain flag is **not** cleared — drain is desired state owned by the admin, and an agent restart must not undo it. Live allocations are untouched (the ObservedSet reconciliation that follows registration settles them). If capacity grew, run a pledge pass. |
| Rejections | — (structurally infallible) |

#### `DeclareNodeLost`

| | |
| --- | --- |
| Proposer | Leader health monitor, when a node misses the replicated heartbeat deadline |
| Payload | `node: NodeId`, `declared_at_us` |
| Validation | Node exists |
| Apply effects | Bump `node_epoch`; `schedulable = false`. Every non-`Released` allocation on the node, **in `seq` order**: attempt `Terminal(NodeLost)` via the full terminal path (release, true-up, job resolution — platform outcome, so retry policy applies). The node's accrual queue empties as a consequence; no pledge pass runs on a lost node. The node record remains (it may re-register). |
| Rejections | `UnknownNode` |

#### `SetNodeSchedulable`

| | |
| --- | --- |
| Proposer | Admin API (drain / undrain) |
| Payload | `node: NodeId`, `schedulable: bool`, `actor: Actor`, `updated_at_us` |
| Validation | Node exists; actor holds unscoped `operator` or `admin` (node operations are cluster verbs — ADR 0023) |
| Apply effects | Set the flag. Drain blocks new placements only: running work continues, and existing accruing allocations keep funding (revoking them is the scheduler's call, via `CommitPlacements`). |
| Rejections | `UnknownNode`, `PermissionDenied` |

### Housekeeping

#### `EvictTerminalJobs`

| | |
| --- | --- |
| Proposer | Leader housekeeping loop — **only after the job-history-store write for every listed job is durable** (ADR 0012). That ordering is a proposal-side obligation; the apply itself just deletes. Timestamps ride in the command; apply never consults a clock to decide "72 h have passed" — the proposer decided that, measuring from each job's `terminal_at_us` (never from submission; a terminal job without the stamp is exempt). |
| Payload | `jobs: JobId[]`, `evicted_at_us` |
| Validation | Every listed job that exists must be terminal. Missing ids are skipped silently — duplicate eviction proposals across leader changes must be idempotent. A *non-terminal* listed job is a proposer bug and rejects the whole command. |
| Apply effects | Remove each listed job, its attempts, and their (already `Released`) allocations from state. Quota usage is untouched — charges and true-ups have long since settled. |
| Rejections | `InvalidBatch` wrapping `JobNotTerminal` |

### Admin / policy

#### `ConfigureQuotaEntity`

| | |
| --- | --- |
| Proposer | Admin API / `coppice-cli policy` (bootstrap tree included — ADR 0020: the node config file never seeds policy) |
| Payload | `entity: QuotaEntityId`, `parent: optional QuotaEntityId`, `name: string`, `quota: CostUnits` (a *stock* in µCU; the CLI converts human rates, per ADR 0019), `actor: Actor`, `updated_at_us` |
| Validation | Parent (if any) exists and is not the entity itself; the new parent chain is acyclic and within the depth cap (32); actor holds `admin` whose scope covers both the entity's current position and its new parent (unscoped admin covers everything — ADR 0023) |
| Apply effects | Create (usage accumulator initialized zero at `updated_at_us`) or update (parent/name/quota replaced; **usage is preserved** — reconfiguring an entity is not an amnesty). No delete command in v1: entities with historical charges stay; removal is a future decision. |
| Rejections | `UnknownQuotaEntity` (parent), `QuotaEntityCycle`, `PermissionDenied` |

#### `UpdatePolicy`

| | |
| --- | --- |
| Proposer | Admin API / CLI. The CLI converts human-facing forms (half-life → Q0.64 λ, rates → stocks) so no transcendental math ever runs in a replica (ADR 0019/0020). |
| Payload | `policy: PolicyConfig` — full replacement: `cost_weights: CostWeights` (Q32.32 per dimension), `decay: DecayPolicy { tick_us, decay_per_tick }`, `penalty_exponent_milli: u32`, `priority_multipliers` (priority → `PriorityMultiplier`; on the wire, repeated key-sorted entries — proto maps are banned in replicated payloads, see [schema-style](schema-style.md)), `accrual_limit: u32` (K, default 4), `default_charge_runtime_s: u64`, `unbounded_runtime_multiplier: u64` (Q32.32, default 2.0 — ADR 0029), `refund_fraction_milli: u32` (default 750 — ADR 0029), `terminal_retention_us: i64` (72 h default), `abort_grace_us: i64` (30 s default), `groups_claim: string` (`"groups"` default — ADR 0023); plus `actor: Actor`, `updated_at_us` |
| Validation | `decay.validate()` (positive tick, λ within the iteration bound); `unbounded_runtime_multiplier ≥ 2³²` (≥ 1.0); `refund_fraction_milli ≤ 1000`; actor holds unscoped `admin`; a full-replacement payload is otherwise self-consistent by construction |
| Apply effects | Replace the replicated policy. In-flight charge records keep their recorded rate/multiplier/refund fraction (no retroactive repricing); decay re-times from each entity's next touch; quota-stock rescaling on half-life change is owned by the tooling that authored the command. |
| Rejections | `InvalidPolicy`, `PermissionDenied` |

A fresh cluster boots with an **empty** `priority_multipliers` table, so
every `SubmitJob` fails synchronous validation until the first
`UpdatePolicy` commits. That is deliberate, not a gap: multipliers are a
pricing decision the operator must make explicitly, and per ADR 0020 no
node config file may seed replicated policy. Production bootstrap therefore
seeds nothing; only `coppice dev` proposes a development table (priorities
`-2..=2`) and a well-known `"dev"` quota entity at startup, using these
same commands.

#### `UpdateAuthorization`

| | |
| --- | --- |
| Proposer | Admin API / `coppice-cli policy` — full replacement of the role-binding list, mirroring `UpdatePolicy` (concurrent edits resolve last-writer-wins in log order) |
| Payload | `bindings: Binding[]` where `Binding = { subject: oneof { group: string, principal: string }, role: Role (submitter \| operator \| admin), scope: optional QuotaEntityId }`; plus `actor: Actor`, `updated_at_us` |
| Validation | Actor holds unscoped `admin`; every role is from the closed set and every subject non-empty; every `scope` references an existing quota entity; the new list retains at least one unscoped `admin` binding (lockout prevention — operator certs make lockout recoverable, but an empty admin list is almost always an accident, per ADR 0023) |
| Apply effects | Replace the replicated bindings. Takes effect for every command ordered after this one — in-flight commands proposed under the old bindings re-validate against the new ones at their own log position. Operator certificates (`actor.operator_cert`) are an implicit unscoped `admin` outside this list and cannot be revoked through it. |
| Rejections | `PermissionDenied`, `UnknownQuotaEntity` (scope), `InvalidAuthorization`, `AuthorizationLockout` |

#### `BumpClusterVersion`

| | |
| --- | --- |
| Proposer | Admin, via the leader. The leader **refuses to propose** a bump past the minimum version supported by current voting members (ADR 0003) — a proposal-side gate, since apply cannot see binaries. Each bump documents its downgrade limit. |
| Payload | `to: u32`, `actor: Actor`, `bumped_at_us` |
| Validation | `to` strictly greater than the current `ClusterVersion`; actor holds unscoped `admin` |
| Apply effects | Set `ClusterVersion`. Version-gated command forms become writable; all commands in this catalog are version 1. |
| Rejections | `ClusterVersionNotMonotonic`, `PermissionDenied` |

---

## RejectionReason taxonomy

| Reason | Meaning |
| --- | --- |
| `UnknownJob` / `UnknownNode` / `UnknownAttempt` / `UnknownAllocation` / `UnknownQuotaEntity` | Referenced entity not in state |
| `DuplicateAttempt` / `DuplicateAllocation` | Proposer-minted id already exists |
| `SubmitSpecMismatch` | Client-minted job id reused with a different spec (an identical spec is an accepted no-op, ADR 0026) |
| `JobTerminal` | Job already reached a terminal state |
| `JobNotQueued` | Placement target isn't `Queued` |
| `JobNotTerminal` | Eviction listed a live job |
| `StaleAttemptState` | Monotonicity dedupe: the attempt already passed this transition |
| `AttemptNotOnNode` | Reconciliation verdict references an attempt on a different node |
| `AllocationNotAccruing` | Revocation of a funded/active/released allocation |
| `NodeNotSchedulable` | Placement onto a drained node |
| `StaleNodeEpoch` | ObservedSet predates a re-registration |
| `RequestExceedsNodeCapacity` | Request can never fit the node's total capacity |
| `AccrualLimitExceeded` | Batch would leave more than K jobs accruing |
| `UnsupportedPlacementShape` | Not one-allocation-singleton-group (v1 gate) |
| `QuotaEntityCycle` | Parent edit would create a cycle or exceed the depth cap |
| `InvalidPolicy` | Policy payload failed validation |
| `PermissionDenied` | Actor lacks the role/scope (or ownership) the command requires (ADR 0023) |
| `InvalidAuthorization` | Bindings payload failed validation (unknown role, empty subject) |
| `AuthorizationLockout` | Bindings replacement would leave no unscoped admin |
| `ClusterVersionNotMonotonic` | Bump not strictly increasing |
| `InvalidCommand` | Shape violation (e.g. outcome `Revoked` outside `CommitPlacements`) |
| `InvalidBatch[(index, reason)]` | All-or-nothing batch rejection with per-item diagnostics |

Rejections are terminal for the command, invisible in state (beyond the
`version` bump), and surfaced to the proposer through the leader's apply
result. Proposers classify them: the API maps user-facing ones to errors,
the scheduler treats batch rejections as "recompute", ingestion ignores
stale-report rejections.
