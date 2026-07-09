# coppice-core

The shared domain model for Coppice: the vocabulary every other crate speaks —
identifiers, the resource model, the job/attempt/allocation lifecycle types, and
the deterministic quota arithmetic. It contains no I/O, no async, and no
scheduling policy; only types and pure functions that are safe to depend on from
anywhere in the workspace.

## What lives here

- **Identifiers** (`id`) — strongly-typed newtypes over `Uuid` (`JobId`,
  `NodeId`, `AllocationId`, `AttemptId`, `GroupId`, `QuotaEntityId`) so one
  entity's id can never be passed where another's is expected. No `Default`: a
  defaulted id is always a bug.
- **Resources** (`resource`) — the multi-dimensional `Resources` vector
  (milli-CPU, memory bytes, disk bytes) with the saturating and component-wise
  operations the state machine relies on (`fits_within`, `saturating_add/sub`,
  `component_min`). The extensible-dimension design is still an open item.
- **Jobs** (`job`) — the coarse, user-visible `Job` and its `JobState` machine,
  plus `RetryPolicy` and `AbortRequest`. Abort is a flag, not a state; every
  attempt end funnels through `Finalizing`.
- **Attempts** (`attempt`) — `Attempt`, its `AttemptState` machine, and
  `AttemptOutcome`/`OutcomeClass`. Retries mint a fresh attempt; all agent
  reports are attempt-scoped, which is what makes duplicate/stale reports safe
  to drop.
- **Allocations** (`allocation`) — `Allocation` and its `AllocationState`
  machine. An accruing allocation *is* Coppice's reservation mechanism; there is
  no standalone reservation object.
- **Nodes** (`node`) — the authoritative `Node` record (advertised capacity,
  labels, schedulability).
- **Quota arithmetic** (`quota`) — the single implementation of the cost-based
  soft-quota model's math (see below).

## State-machine invariants

Each of the three lifecycle machines exposes an `is_terminal` predicate and a
`may_transition_to` table that encodes the *only* legal edges; the replicated
state machine rejects anything else. The tables are unit-tested against the
transition specs in the docs. The three machines are coarse and independent by
design so that the attempt machine can evolve (accrual now, gang barriers later)
without disturbing the user-visible job states.

See [state-model](../../docs/architecture/state-model.md),
[job-lifecycle](../../docs/lifecycle/job-lifecycle.md), and
[ADR 0013](../../docs/decisions/0013-job-attempt-allocation-state-machines.md)
for the desired/observed/derived split and the full transition tables, and
[ADR 0014](../../docs/decisions/0014-accruing-allocations-replace-reservations.md)
for why accruing allocations replaced standalone reservations.

## Deterministic quota arithmetic

The `quota` module is the load-bearing part of this crate. It is called from two
places with different requirements: the Raft apply loop (charging and true-up),
where every operation must be **bit-deterministic** across replicas, platforms,
and binary versions; and the scheduler (penalty and effective-score), which is
derived state where `f64` is tolerable.

Accordingly, everything that touches replicated state — `CostUnits` (µCU),
`DecayPolicy`, `UsageState`, `CostWeights`, `PriorityMultiplier`, charging and
true-up — is pure integer arithmetic: saturating, `u128` intermediates, floor
(truncation) rounding, no floats anywhere. Only `penalty` and `over_quota_ratio`
use `f64`, and their results must never be written back into commands, state, or
snapshots.

Key properties:

- **Exact decay composition.** Usage decays lazily (`UsageState::touch` brings
  the accumulator forward to a command's timestamp before charging), so the
  number of decay hops between two points in time is data-dependent.
  `decay(decay(u, n1), n2) == decay(u, n1 + n2)` must therefore hold exactly —
  it does, because decay is *literally* the n-fold iteration of the per-tick
  step `u ← ⌊u·λ⌋`. A closed-form or squaring fast path would break it. The
  per-tick factor λ is Q0.64 fixed point derived from a half-life at
  policy-authoring time, so no transcendental function ever runs in the state
  machine.
- **No time-travel on clock skew.** Command timestamps come from different
  leaders and may regress; a regressed timestamp decays nothing and never rewinds
  `last_update_us`, so a decay interval can never be applied twice.
- **Charge / true-up symmetry.** Cost is `resource_rate × runtime_seconds ×
  priority_multiplier`; true-up at terminal resolution refunds the decayed unused
  portion or surcharges a grace overrun, using the rate captured in the
  `ChargeRecord` so a mid-flight policy edit never reprices an in-flight charge.
  An attempt that never ran (including `Revoked`) has actual cost zero and gets
  the full decayed charge back — that is what makes revocation requeue free
  without a special case.

Golden decay/cost vectors and a property test for the composition invariant guard
against cross-binary drift.

See [quotas-and-priorities](../../docs/scheduling/quotas-and-priorities.md),
[ADR 0005](../../docs/decisions/0005-cost-based-soft-quotas.md) for the
cost-based soft-quota model, and
[ADR 0019](../../docs/decisions/0019-deterministic-quota-arithmetic.md) for the
fixed-point representation and algorithms.

## Boundaries

- No I/O, async, serialization, or wire types — those live in the transport and
  coordinator crates. This crate only defines the domain vocabulary.
- No scheduling policy: priority→multiplier tables, effective-score ordering, K,
  and backfill live in `coppice-scheduler`. `quota` supplies the arithmetic
  primitives, not the placement decisions.
