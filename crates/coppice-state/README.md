# coppice-state

The deterministic replicated state machine behind Raft. This crate owns the
authoritative control-plane state and the closed set of commands that mutate
it; consensus delivers the command log, this crate decides what each committed
entry does to state.

## Determinism is the contract

`StateMachine::apply` is the single entry point that mutates authoritative
state, invoked on every replica from the coordinator's Raft apply loop. Given
the same sequence of committed commands, every replica *must* reach byte-equal
state — so apply reads no wall clock, draws no randomness, makes no network
call, and never iterates an unordered map. Every timestamp rides in the
command (`*_at_us`, Unix µs), every id is minted by the proposer, and the
`priority: i32` is pre-resolved to a fixed-point multiplier before it reaches
apply. State is `BTreeMap` throughout and every record is `PartialEq`/`Eq`, so
the determinism harness can assert replica equivalence structurally.

Commands commit *decisions, not computations*. A command that fails validation
is not dropped: it was already appended to the log on every replica, so it
applies as a deterministic no-op that returns a [`RejectionReason`] and still
bumps `version`. Refusing a command must be exactly as reproducible as
applying it. See [high-availability](../../docs/architecture/high-availability.md)
and [ADR 0019](../../docs/decisions/0019-deterministic-quota-arithmetic.md) for
the fixed-point integer arithmetic (µCU, exact-composition decay, no floats)
that keeps quota accounting replica-identical.

Every apply is also **bounded work**: the quota-ancestor walk is depth-capped
(`QUOTA_TREE_DEPTH_CAP`), batch commands validate all-or-nothing, and no
command can turn apply into an unbounded loop.

## The command catalog

[`Command`](src/command.rs) is the versioned enum of every log mutation,
grouped by proposer:

- **API** — `SubmitJob`, `AbortJob`.
- **Scheduler** — `CommitPlacements` (one pass's atomic batch of placements
  and revocations), `DispatchAttempt`.
- **Agent ingestion** — `RecordAttemptStarted` / `RecordAttemptExited` /
  `RecordAttemptOutcome` and `ReconcileNode`, all fed from the leader's
  normalized observed facts, never raw agent reports.
- **Node lifecycle** — `RegisterNode`, `DeclareNodeLost`, `SetNodeSchedulable`.
- **Housekeeping** — `EvictTerminalJobs`.
- **Admin / policy** — `ConfigureQuotaEntity`, `UpdatePolicy`,
  `BumpClusterVersion`.

These domain types mirror the frozen `coppice.command.v1` protobuf schema field
for field; `coppice_proto::convert` maps between the two at the wire boundary.
The per-command payload, validation, apply effects, and rejection taxonomy are
specified in [command-catalog](../../docs/architecture/command-catalog.md); the
shape of desired vs. observed state is in
[state-model](../../docs/architecture/state-model.md).

## What apply encodes

The interesting logic lives in [`apply.rs`](src/apply.rs), each handler split
into a read-only validation phase that yields to an infallible effects phase:

- **The job/attempt/allocation lifecycles** and the single `terminate_attempt`
  funnel — release, funding cascade, quota true-up, and job resolution (retry
  policy, abort-wins-over-retry, truth-wins-the-race) in one apply — implement
  [ADR 0013](../../docs/decisions/0013-job-attempt-allocation-state-machines.md).
- **Accruing allocations and the pledge order.** Allocations start `Funded` or
  `Accruing` based on actual free capacity decided *here*, not by the proposer;
  freed capacity flows to a node's accrual queue in commit (`seq`) order via
  `pledge_node`, and the per-batch accrual limit (K) is enforced against a
  simulation of that same arithmetic. This is
  [ADR 0014](../../docs/decisions/0014-accruing-allocations-replace-reservations.md).
- **Quota charging** at placement and true-up at terminal resolution walk the
  entity tree of [ADR 0005](../../docs/decisions/0005-cost-based-soft-quotas.md),
  using the rate and multiplier recorded on the attempt so a later policy edit
  never reprices in-flight work.
- **Epoch fencing.** Node (re)registration and loss bump an epoch that
  invalidates stale coordinator→agent commands and gates `ReconcileNode`
  ([ADR 0009](../../docs/decisions/0009-fencing-and-reconciliation.md)).

## Change notifications

An accepted command returns [`Applied`] carrying an ordered list of
[`Event`]s (`JobStateChanged`, `AllocationFunded`, `StopRequested`,
`NodeEpochBumped`, …). These are **derived output for the event fanout and the
coordinator runtime — never read back by apply**. In particular apply performs
no I/O: a needed side effect such as sending a `StopJob` surfaces as a
`StopRequested` event that the runtime acts on. Delivery ordering and cursor
semantics for these events are
[ADR 0008](../../docs/decisions/0008-event-delivery-guarantees.md).

## Boundaries

- **Not consensus.** Log replication, snapshots, and leadership live in
  `coppice-consensus` / the coordinator; this crate is a pure function from
  `(state, command)` to `(state, events | rejection)`.
- **Not the scheduler.** All placement policy — scoring, K, backfill, which
  node an accrual targets — lives in `coppice-scheduler` and reaches state only
  as a proposed `CommitPlacements`. Apply just validates and funds
  deterministically.
- **Replicated policy only.** `PolicyConfig` holds exactly the knobs that would
  diverge scheduling or accounting if replicas disagreed, so none of it may sit
  in a node config file ([ADR 0020](../../docs/decisions/0020-node-config-vs-replicated-policy.md)).
- **v1 placement shape.** The `allocations` vec and `GroupId` are the
  gang-scheduling seam; v1 requires exactly one allocation and a singleton
  group, and apply deterministically rejects other shapes
  (`UnsupportedPlacementShape`) rather than ignoring the field — a committed
  multi-allocation placement must still resolve identically on every replica.
