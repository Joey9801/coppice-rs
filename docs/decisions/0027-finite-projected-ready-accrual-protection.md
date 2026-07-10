# 27. Finite projected-ready bounds govern lending and accrual placement

- **Status:** Accepted
- **Date:** 2026-07-10
- **Amends:** [ADR 0014](0014-accruing-allocations-replace-reservations.md)
- **Resolves:** [KOI-4](../roadmap/known-open-issues.md#koi-4-unbounded-projected_ready-does-not-protect-accrual-progress)

## Context

ADR 0014's strict-backfill rule permits lending an accruing allocation's
pledged capacity to a new job only when `now + max_runtime ≤
projected_ready(A)`, calls `projected_ready(A)` a guaranteed worst-case
funding time, and concludes that accruing allocations are never delayed by
backfill. The v1 scheduler materialised the unbounded case —
`projected_ready(A) = None`, meaning the node's guaranteed release events run
out before covering A's remaining need — as *satisfying* the check
(`None => true`), reasoning that where no finite bound exists there is
nothing to violate.

KOI-4 records why that is vacuous. The accruals that most need protection are
exactly the ones waiting on capacity held by jobs with no enforced
`max_runtime`; for them, a succession of bounded jobs can revoke-and-reseat
the accrual behind new work indefinitely, each lend borrowing capacity the
accrual had already been funded. The promised starvation protection did not
exist for the unbounded case, and the top-K accrual mechanism established no
progress bound by itself.

Jobs in the target domain often *cannot* declare a useful `max_runtime`:
runtimes vary unpredictably from hours to over a week. Two alternatives were
considered and rejected:

- **Require or infer a default `max_runtime` for every job.** An enforced
  default either kills legitimate long jobs or, if set conservatively large,
  produces a bound so distant that the lend check stays effectively vacuous —
  while also over-reserving quota for the worst case. An advisory default
  never enters the guaranteed sweep (ADR 0014) and changes nothing.
- **Keep unbounded lending but bound it with a lend credit** (a cumulative
  borrowed-time allowance per accrual). This yields a real "delayed by at
  most B" property and remains the designated escape hatch if the utilization
  cost of this decision is ever measured to matter — but it adds a replicated
  counter and a policy knob for a problem not yet observed, so it is
  deferred, not adopted.

## Decision

Four rules replace "`None` counts as satisfied":

1. **No finite bound, no lend (`None => false`).** A strict-backfill lend on
   a node is legal only when *every* surviving accrual `A` on that node has a
   finite `projected_ready(A)` and the borrower's enforced completion bound
   satisfies `now + max_runtime ≤ projected_ready(A)`. An accrual with no
   finite bound therefore never has pledged capacity borrowed: every unit it
   accrues, it keeps.

2. **Accrual placement prefers finite bounds.** When the scheduler opens a
   new accruing allocation, it must not select a node on which the accrual's
   `projected_ready` would be indefinite while some eligible node would give
   it a finite one. Candidate bounds are computed with the ADR 0014 sweep, as
   if the new accrual were appended behind the node's existing accrual queue.
   Finite candidates are ranked by earliest `projected_ready`, ties by the
   largest immediately-pledged fraction, then `NodeId`; indefinite candidates
   keep the previous largest-pledge ranking among themselves.

3. **Re-planning moves accruals toward better bounds.** The existing
   reseat-elsewhere rule (move an accrual to a node where its full request
   fits right now) generalises: an immediate fit is simply
   `projected_ready = now`, the best possible bound. Each pass, an existing
   accrual is moved to another node when the move improves its projected
   start time meaningfully:

   - indefinite → finite is mandatory whenever a finite-bound node is
     eligible (an unbounded-to-bounded move is an infinite improvement);
   - finite → finite requires the new bound to be earlier by at least a
     configured threshold (`replan_min_improvement_us`, scheduler-local
     tuning per ADR 0020's config/policy split — proposals are re-validated
     at commit, so replicas need not agree on it).

   The anti-churn discipline of ADR 0014 is preserved: a revocation is
   emitted only for a strictly better reseat planned in the same batch, one
   move per job per pass, and the pass must remain a fixpoint — re-running it
   immediately on its own applied output proposes nothing.

4. **Fallback: protection without a bound.** When no eligible node yields a
   finite bound, the accrual is still opened (by the previous largest-pledge
   ranking). The blocked job keeps its top-K protection, and rule 1 makes its
   node lend-free, so its funding is monotone: whatever frees on the node is
   pledged to it and cannot be taken back by backfill. Its user-visible
   answer to "why is this job waiting?" is then not a manufactured time but
   the honest one: the specific unbounded holders it waits on, plus its
   protected status.

Together these give every accruing allocation a well-defined projected start
answer — a finite `projected_ready` wherever one is achievable, and a
protected, monotone accrual where none is.

**The progress property.** Stated for property tests, in the guarantee model
of ADR 0014 (enforced `max_runtime`s hold; a seated borrower's bound runs
from its seating):

- **(P1) Monotone progress when unbounded.** An accrual with no finite
  `projected_ready` never loses funded capacity to backfill; across any
  stream of bounded backfill arrivals and scheduling passes, its funded
  vector is non-decreasing except by its own re-planning moves (rule 3).
- **(P2) Lends never delay a bound.** A legal lend never moves any surviving
  accrual's `projected_ready` later; an accrual with a finite bound `P` is
  fully funded by `P` regardless of how many legal lends occur.
- **(P3) Placement honours finiteness.** No scheduling pass opens or leaves
  an accrual on a node with an indefinite bound when an eligible node offers
  a finite one (or a θ-better one, per rule 3).

## Consequences

- The starvation guarantee becomes real instead of vacuous, and ADR 0014's
  claim narrows to what is true: accrual funding is never delayed *relative
  to its guaranteed bound*, and where no bound exists, never delayed at all —
  because nothing is borrowed.
- **Utilization is deliberately traded away on lend-free nodes.** Pledged
  capacity accruing toward an unbounded-wait whale can no longer be lent even
  to a five-minute job. The exposure is bounded by K nodes (default 4). If
  operators measure this to matter, the remedy is the deferred lend-credit
  mechanism, not a return to `None => true`.
- Incentives stay aligned and sharpen: declaring an enforced `max_runtime`
  remains the only way to backfill, and running without one now also
  implicitly repels accruals from your nodes (rule 2) — no new user-facing
  flag or mandatory field is introduced.
- Re-planning does more work per pass: release events are collected for all
  nodes (the same single allocation scan as before), and candidate bounds
  cost one sweep per node scanned. Improvement scans are capped per pass by
  the accrual limit K, so a pathological over-cap backlog cannot wedge a
  pass.
- A pre-existing wrinkle is unchanged by this decision: the lend check treats
  the borrower as starting at seating time, while its guaranteed release
  event exists only once it is `Running`; dispatch latency erodes the bound
  exactly as it did under ADR 0014.
