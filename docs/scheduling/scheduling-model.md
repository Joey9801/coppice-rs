# Scheduling Model

## The scheduler engine

The scheduler engine is responsible for turning queued jobs into placement
decisions.

It should not directly mutate authoritative state. Instead, it should operate on
a consistent snapshot or versioned view of the cluster state, compute a batch of
proposed assignments or reservations, and submit those proposals back to the
coordinator leader for validation and commitment.

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
5. Use reservations or earmarked future capacity for large jobs that cannot run
   immediately but must not be starved.
6. Backfill around reservations when safe.
7. Submit a batch of proposed placements and reservations for atomic validation.
8. Recompute when the proposal conflicts with newer committed state.

The scheduler should expect proposals to fail validation due to concurrent
changes, node loss, job cancellation, quota updates, or leader changes. Failed
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

## Large jobs, reservations, and backfilling

Large jobs require special care. A strict single-job-at-a-time admission loop
can allow a large unschedulable job to block throughput. Conversely, ignoring
the large job allows smaller jobs to continuously consume capacity and starve
it. The design should support reservations or earmarked future capacity so that
large jobs can make progress while safe backfilling continues around them.

The detailed reservation and backfilling model is an
[open design decision](../roadmap/open-decisions.md).

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
durable scheduling decisions, fairness, reservations, or user-visible semantics.

## Related

- [quotas-and-priorities.md](quotas-and-priorities.md) — the fairness and
  admission policy the scheduler enforces.
- [image-cache.md](image-cache.md) — how image locality feeds soft scoring.
