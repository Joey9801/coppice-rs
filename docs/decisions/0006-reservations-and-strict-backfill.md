# 6. Reservations for whale jobs with strict bounded backfill

- **Status:** Superseded by [ADR 0014](0014-accruing-allocations-replace-reservations.md)
- **Date:** 2026-07-07
- **Resolves:** [OD-5](../roadmap/open-decisions.md#od-5-reservation-and-backfilling-model)

## Context

Large ("whale") jobs starve if small jobs continuously consume capacity, but a
strict head-of-queue admission loop lets one unschedulable whale block all
throughput. Reservations fix starvation; backfill restores throughput — if it
provably cannot delay the reservation. Backfilling on advisory runtime
estimates risks exactly that delay.

## Decision

**Reservations.** When the scheduler cannot place a whale job that has reached
the front of the effective-score order, it proposes a **reservation**: a
replicated record `{reservation_id, job_id, capacity (node set or resource
vector), estimated_start, expiry}` computed from the projected completion of
running bounded jobs. Only the top **K** blocked whales hold reservations at
once (configurable, default 4). The scheduler renews or re-plans reservations
as cluster state drifts; reservations are released by explicit command when
the job places, is cancelled, or the plan changes. Expiry is a safety net
against leaks, enforced by commanded cleanup, never by wall clock during
apply.

**Strict backfill.** A job may backfill into reserved capacity only if it has
an **enforced** `max_runtime` (the agent kills the container at the bound) and
`now + max_runtime ≤ estimated_start` of every reservation whose capacity it
would touch. Jobs without a `max_runtime` never backfill. Reservations are
therefore never delayed by backfilled work — this is classic EASY backfilling.

Runtime estimates other than `max_runtime` remain advisory: they may inform
`estimated_start` projections but never backfill admission.

## Consequences

- Whales make progress with a bounded, explainable wait ("reserved, starts
  ~14:30"), and clusters keep high utilization around them.
- The safety argument is simple and local: enforced bounds, no estimates in
  the safety path. The cost is unused capacity when jobs lack `max_runtime` —
  deliberately accepted, and mitigated by the cost model
  ([ADR 0005](0005-cost-based-soft-quotas.md)): declaring a tight
  `max_runtime` lowers a job's cost, so users are already incentivized to make
  work backfillable.
- No preemption machinery enters v1.
- Reservations are replicated state and must be accounted for in snapshots and
  in the scheduler's feasibility view.
