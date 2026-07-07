# 14. Accruing allocations replace standalone reservations

- **Status:** Accepted
- **Date:** 2026-07-07
- **Supersedes:** [ADR 0006](0006-reservations-and-strict-backfill.md)

## Context

ADR 0006 gave whale jobs replicated time-based reservation records with strict
EASY backfill. The state-machine refinement in
[ADR 0013](0013-job-attempt-allocation-state-machines.md) introduced partial
scheduling: an allocation can be committed on a node before the node has space
(`Accruing`), accumulating capacity as it frees. That mechanism *is* a
reservation, and keeping both would mean two overlapping constructs for the
scheduler, snapshots, and failover to keep consistent.

## Decision

There is **no standalone reservation object** in replicated state. Earmarked
future capacity is represented exactly as an attempt whose allocations are
`Accruing` on specific nodes:

- **Funding.** When capacity frees on a node, the apply loop pledges it to
  that node's accruing allocations in commit order, deterministically. When an
  allocation's full request is funded it becomes `Funded`; when all of an
  attempt's allocations are funded the attempt passes the `Ready` barrier and
  dispatches.
- **Concurrency cap.** At most K jobs hold accruing allocations at once
  (configurable, default 4) — the top blocked whales in effective-score order.
- **Re-planning.** Accruing allocations are revocable by scheduler command
  (attempt outcome `Revoked`, requeued free of retry budget). Global
  commit-order funding plus revocation is the anti-deadlock story for
  overlapping half-funded whales; funded allocations are never revoked in v1.
- **Strict backfill carries over unchanged in spirit.** Define
  `projected_ready(A)` for an accruing allocation A as the worst-case time at
  which A's unfunded remainder frees, computed from the *enforced*
  `max_runtime`s of the jobs currently holding that capacity — a guaranteed
  bound, not an estimate. Capacity already funded to A (or otherwise pledged)
  may be lent to a new job only if that job has an enforced `max_runtime` and
  `now + max_runtime ≤ projected_ready(A)`. Jobs without a `max_runtime`
  never touch pledged capacity. Accruing allocations are therefore never
  delayed by backfill.
- Advisory runtime estimates may inform scheduling heuristics but never the
  backfill safety check, exactly as before.

## Consequences

- One mechanism to snapshot, fence, account for in quota charging, and reason
  about on failover — and it is the same mechanism a future gang scheduler
  needs, since a gang is just several attempts accruing behind one barrier.
- "Why is this whale waiting?" has a concrete, inspectable answer: its
  allocations, their funded fractions, and `projected_ready` per node.
- Reservations not tied to concrete nodes ("20% of the cluster at 9am") are
  expressible only by choosing nodes early; if calendar-style capacity
  planning is ever needed it is a new decision, not a revival of ADR 0006.
- The scheduler must pick nodes for a whale earlier than a time-based
  reservation would require; commitment is softened by revocation being
  cheap and free for the job.
