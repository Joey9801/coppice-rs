# Scheduling Model

## The scheduler engine

The scheduler engine is responsible for turning queued jobs into placement
decisions.

It should not directly mutate authoritative state. Instead, it should operate on
a consistent snapshot or versioned view of the cluster state, compute a batch of
proposed assignments, accruing allocations, and revocations, and submit those
proposals back to the coordinator leader for validation and commitment.

The scheduler must handle:

- Multi-dimensional resource bin packing.
- Hard placement constraints.
- Soft placement preferences.
- Priority ordering.
- Quota and fairness.
- Affinity and anti-affinity.
- Image locality and cache pressure.
- Large "whale" jobs requiring significant fractions of nodes.
- Starvation avoidance.
- Backfilling smaller jobs without permanently blocking larger jobs.
- Dynamic node availability.
- Jobs with uncertain runtime.

The scheduler should be designed as an asynchronous subsystem. Scheduling can be
CPU-intensive and should not block Raft application, API handling, or agent
heartbeat processing.

## Operating model

A useful model is:

1. Maintain an authoritative queue of pending work.
2. Select candidate jobs according to priority, fairness, and quota policy.
3. Classify jobs into ordinary jobs, constrained jobs, and large jobs where
   useful.
4. Compute feasible placements against a snapshot of cluster state.
5. Commit accruing allocations for large jobs that cannot run immediately but
   must not be starved, so they accumulate capacity as it frees.
6. Backfill around accruing allocations when provably safe.
7. Submit a batch of proposed placements, accruals, and revocations for atomic
   validation.
8. Recompute when the proposal conflicts with newer committed state.

The scheduler should expect proposals to fail validation due to concurrent
changes, node loss, job abort, quota updates, or leader changes. Failed
proposals are normal and should trigger recomputation, not exceptional control
flow.

## Constraints

Scheduling should be treated as a policy-driven optimization process with
correctness constraints.

**Hard constraints** must never be violated. Examples include:

- Required resource capacity.
- Required node labels.
- Required CPU architecture.
- Required GPU type.
- Required isolation properties.
- Hard affinity or anti-affinity.
- Node drain or maintenance state.
- User, project, or queue restrictions.

**Soft constraints** influence scoring but may be violated when necessary.
Examples include:

- CPU brand preference.
- Image locality.
- Spreading or packing preferences.
- Preferred zones or racks.
- Preferred co-location.
- Cache warmth.
- Historical reliability.

## Resource dimensions and bin packing

The scheduler should support extensible resource dimensions. CPU, memory, and
disk should be first-class from the start, but the representation should allow
future scalar or structured resources such as GPUs, accelerators, licenses,
NUMA-local resources, or special devices.

Bin packing should be heuristic. Full optimal packing is not practical at the
target scale. The scheduler should use a combination of candidate pruning,
scoring, batching, and incremental recomputation.

## Large jobs, accrual, and backfilling

Large jobs require special care. A strict single-job-at-a-time admission loop
can allow a large unschedulable job to block throughput. Conversely, ignoring
the large job allows smaller jobs to continuously consume capacity and starve
it.

The model, decided in
[ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md), is
**accruing allocations** plus strict (EASY) backfill — there is no standalone
reservation object.

The trigger rule matters and is easy to get wrong: **an accruing allocation is
the license to backfill past a job, not a placement optimization.** "Whale" is
shorthand for "blocked at the head of the effective-score order while others
are backfilled past it" — not a size classification, and there is no
user-declared whale flag.

- The default state of a queued job is *unpinned*, even on a saturated
  cluster with a deep queue. It is seated by ordinary score-order placement
  the moment any node frees enough capacity, wherever that is; no guess about
  which node frees first is ever committed for it.
- An accruing allocation is created only when the scheduler wants to hand
  freed capacity to a job *behind* a blocked higher-score job and must prove
  the blocked job is not starved. The blocked job then gets allocations on
  specific nodes in the `Accruing` state; freed capacity on those nodes is
  pledged to accruing allocations in commit order, deterministically, until
  they are `Funded` and the attempt passes the `Ready` barrier (see
  [../lifecycle/job-lifecycle.md](../lifecycle/job-lifecycle.md)).
- At most K jobs hold accruing allocations at once (replicated config,
  default 4). K bounds simultaneous *guarantees*, not throughput: jobs beyond
  the top-K are seated normally whenever they fit, and quota decay keeps
  effective scores moving so the protected window rotates rather than being
  camped on.
- An accrual is a hold, not a destination commitment. The scheduler re-plans
  every pass; if the job's slot appears on a different node first it is
  seated there and the accrual is revoked.
- Accruing allocations are revocable by scheduler command for re-planning
  (attempt outcome `Revoked`, requeued free of retry budget) — the
  anti-deadlock and anti-leak story for overlapping half-funded whales.
- A job may **backfill** into pledged capacity only if it has an *enforced*
  `max_runtime` and `now + max_runtime ≤ projected_ready(A)` for every
  accruing allocation A it would touch, where `projected_ready` is the
  worst-case funding time computed from the enforced `max_runtime`s of the
  jobs currently holding A's remainder. Jobs without a `max_runtime` never
  touch pledged capacity, so accrual is never delayed by backfill.
- Advisory runtime estimates may inform heuristics but never the backfill
  safety check.

## Runtime estimates

Runtime estimates may come from several sources:

- User-provided maximum runtime.
- Historical runtime for similar jobs.
- Image, command, queue, project, or user history.
- Explicit user-provided estimate.
- Agent-side progress reports.
- Job self-reporting through a controlled progress or ETA channel.
- Conservative defaults when no better signal exists.

Runtime estimates are advisory unless tied to explicit policy such as maximum
runtime enforcement. Persist only the estimates and progress signals that affect
durable scheduling decisions, fairness, accrual projections, or user-visible
semantics.

## Related

- [scheduler-v1.md](scheduler-v1.md) — the implemented v1 algorithm: scoring,
  node filtering, best-fit packing, and the accrual/backfill mechanics
  described above, as built.
- [quotas-and-priorities.md](quotas-and-priorities.md) — the fairness and
  admission policy the scheduler enforces.
- [image-cache.md](image-cache.md) — how image locality feeds soft scoring.
