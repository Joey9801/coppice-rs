# Scheduler v1

This is the implemented v1 scheduling algorithm — `HeuristicScheduler` in
`crates/coppice-scheduler`. It is a concrete instance of the operating
model in [scheduling-model.md](scheduling-model.md); read that first for
*why* the pieces exist (accruing allocations, the license-to-backfill
trigger, strict backfill) and [quotas-and-priorities.md](quotas-and-priorities.md)
for the quota/priority model the ranking formula draws on. This page is
*how* v1 builds them into one deterministic pass.

Each pass is a pure function of `(snapshot, now_us)`: no clock reads, no
randomness, no I/O. It builds a working model of the snapshot, decides a
batch of revocations and placements, and emits a `PlacementProposal` for
the coordinator to validate and commit through Raft. The pass simulates
apply's own effects order while deciding — revocations first, in payload
order, freed capacity pledged onward to surviving accruals; then
placements, in payload order, funded from what remains — so a proposal is
built to be accepted whole. Proposals still fail sometimes (the snapshot
went stale under a concurrent commit); that is normal and the driver just
recomputes.

## Candidate selection

Every job with `state == Queued` is a candidate. Each is scored with
[`effective_score`](../decisions/0021-effective-score-ranking.md)
(ADR 0021): the job's priority multiplier divided by the ancestor
quota-penalty product, plus an age term bounded by the decay half-life.
Penalty products are memoized per distinct leaf quota entity for the pass,
so scoring the whole candidate set costs one ancestor walk per entity
touched, not per job.

The pass only considers the top `max_candidates` candidates by the ADR
0021 total order (score descending, then FIFO by submission time, then
`JobId`) — this bounds per-cycle work so a deep backlog cannot wedge a
pass; a job outside the window simply waits for a later pass once the
scores ahead of it move (aging keeps them moving). Under this cap, scoring
itself stays cheap even with a queue in the hundreds of thousands.

## Node filters

For a candidate job, a node is eligible only if it passes every hard
filter:

- **Schedulable** — `node.schedulable == true`. Unschedulable nodes are
  skipped entirely; their capacity isn't even loaded into the pass's node
  model.
- **Labels** — `node_satisfies_labels(required, node.labels)`: every
  `(key, value)` in the job's required set must be present on the node
  with an equal value. **v1 seam:** the frozen `Job` proto carries no
  label selector yet, so `required_labels(job)` always returns the empty
  set today and this filter is a no-op in practice. The mechanism exists
  and is unit-tested against `node_satisfies_labels` directly so that
  wiring a real selector later is a proto change plus one function, not a
  scheduler redesign.
- **Resource fit** — the job's `requests` fit within the node's *total*
  capacity, per dimension. This is the coarse admissibility check; whether
  a node has *free* capacity right now is decided separately by the
  packing and backfill steps below.

## Best-fit packing

Among nodes with enough **free** capacity right now, the job is seated on
the node that minimizes dominant leftover fraction:

```
max over dimensions d of (free_after_d / capacity_d)
```

(a dimension with zero capacity contributes 0), ties broken by `NodeId`
ascending. This is deliberately *best-fit*, not first-fit or a global
optimum: at 1M-queued-job scale, the pass has a fixed time budget per
cycle, and best-fit is `O(#nodes)` per candidate with no lookahead. The
goal is decision quality per unit of scheduler-cycle time, not optimal
packing — a locally-good choice made every cycle, re-evaluated every
cycle, beats a globally-optimal choice that takes too long to compute
often enough to matter.

The node-choice key has a seam for future soft scoring:

```rust
fn cache_affinity_bonus(_job: &Job, _node: &Node) -> f64 { 0.0 }
```

folded into (subtracted from, i.e. preferring warm nodes) the fit key.
It is a fixed `0.0` in v1 — image-cache soft scoring (ADR 0010) slots in
here without changing the packing structure.

## The accrual trigger: license to backfill

A candidate that fits no node's *free* capacity is **blocked**, not
necessarily unplaceable. Per
[scheduling-model.md](scheduling-model.md#large-jobs-accrual-and-backfilling),
an accruing allocation is the license to backfill *past* that job, nothing
more — it is opened only when doing so is otherwise legal, i.e. when the
K guard allows it:

```
after > max(before, policy.accrual_limit)   → forbidden
```

mirroring apply's `check_accrual_limit` exactly (`before`/`after` count
distinct jobs holding accruing allocations in the pass's batch simulator).
If some node passes the hard filters on total capacity and labels, and
opening one more accrual would not push the accruing-job count over the
limit, the job is placed there with `expect_funded = false`.

Node choice prefers a **finite `projected_ready`** (ADR 0027): for each
eligible node the pass computes the bound the new accrual would get —
appended behind that node's surviving accrual queue and any accruals this
batch already placed there, swept against the node's guaranteed release
events — and never picks a node where that bound is indefinite while some
eligible node offers a finite one. Finite candidates are ranked by
earliest bound, ties by largest immediately-pledged fraction
(`sim_free.component_min(requests)`, normalized); indefinite candidates
fall back to the pledged-fraction ranking among themselves. Ties break by
`NodeId` throughout. When only indefinite nodes exist the accrual still
opens — the job keeps its top-K protection, and the strict-backfill rule
below makes such a node lend-free, so the accrual's funding is monotone.

If no license or no feasible node exists, the job is skipped for this
pass and stays `Queued`; jobs beyond the top-K protected window, or ones
that fit no node's total capacity at all, are simply passed over rather
than treated as an error.

## Strict backfill: the revoke-and-reseat lend

A job with an *enforced* `max_runtime_us = Some(r)` may jump the queue
into capacity already pledged to accruing allocations, if doing so cannot
delay any of them. The pass checks nodes where the job's `requests` fit
within `sim_free + Σ funded` of that node's surviving accruals (i.e. it
fits once pledged capacity is lent), and requires the strict rule to hold
for **every** surviving accrual `A` on the node — a conservative
touch-everything-on-the-node set, not just the accruals actually being
lent from:

```
now + r ≤ projected_ready(A)     for every surviving accrual A on the node
```

**`projected_ready(A) = None` (unbounded) forbids the lend**
([ADR 0027](../decisions/0027-finite-projected-ready-accrual-protection.md),
which reversed the original `None => true` reading of ADR 0014).
`projected_ready(A)` is the *guaranteed* worst-case time by which `A` gets
funded from currently-running, `max_runtime`-enforced allocations (see
below). When that sweep runs out of guaranteed events before covering
`A`'s remaining need, `A` is waiting on capacity nothing bounds — exactly
the accrual an adversarial stream of bounded jobs could otherwise
revoke-and-reseat forever, taking back capacity it had already been
funded. With `None` excluded, an accrual with no finite bound keeps every
unit it accrues: its node lends nothing, and its funding is monotone. The
utilization this forgoes on such nodes is deliberate and bounded by K
(ADR 0027's consequences).

Among nodes that pass, the pass picks the one minimizing borrowed capacity
(`Σ_d (requests − sim_free).saturating_sub(0) / capacity_d`, ties by
`NodeId`), then plans the lend as three steps in the batch, in order:

1. **Revoke every surviving accrual on that node, wholesale.** Partial
   revocation is not legal here: apply's funding order pledges capacity
   freed by a revocation to the node's *surviving* accruals first, so
   revoking only some would leak the freed capacity right back to the
   others instead of to the backfilling job.
2. **Seat the backfilling job** on the now-larger free pool.
3. **Reseat each revoked accrual**, same job, same node, same `requested`,
   immediately after the backfill placement in the batch, in their
   original `seq` order. They re-accrue with whatever capacity remains —
   almost always still `Accruing`, not `Funded`.

At most one lend is planned per node per pass, to keep the revoke/reseat
ordering untangled; the batch simulator still verifies the full sequence
mirrors apply's effects exactly before it is emitted.

### `projected_ready`: guaranteed bounds only

`projected_ready(A)` is computed by sweeping a node's **guaranteed**
release events in ascending time: an event exists only for an allocation
whose attempt is `Running` (`started_at_us = Some(s)`) *and* whose job has
an *enforced* `max_runtime_us = Some(r)`, releasing its `funded` capacity
at `s + r`. Any other live, non-accruing allocation on the node — not yet
started, or with no enforced `max_runtime` — contributes nothing; it has
no guaranteed bound, so it is treated as never releasing for the purpose
of this computation. Advisory runtime estimates never enter this sweep,
per ADR 0014. Events are walked in time order, freed capacity is pledged
component-wise to the node's accrual queue in `seq` order (mirroring
`pledge_node`), and `projected_ready(A)` is the event time at which `A`'s
remaining need reaches zero — or `None` if the events run out first.
Simultaneous events tie-break by allocation `seq`.

Events are collected for **every** node in the pass's model (one scan of
the allocation table, as before), because candidate bounds for opening or
moving an accrual sweep any eligible node, not only current accrual hosts
(ADR 0027). Accruals the batch itself places on a node are carried as
pending claims so later candidate sweeps on that node see the whole queue.

## Re-planning existing accruals

Before the seating loop, the pass looks at every existing accruing job
(distinct, in `seq` order) and re-plans it in two tiers:

1. **Reseat-elsewhere (immediate fit).** If its full `requested` now fits
   within some *other* schedulable, accrual-free node's free capacity
   (label-checked), revoke the accrual and reseat the job there with
   `expect_funded = true` — "if the job's slot appears on a different node
   first it is seated there and the accrual is revoked"
   ([scheduling-model.md](scheduling-model.md#large-jobs-accrual-and-backfilling)).
2. **Improvement move (ADR 0027).** Failing that, if some other
   schedulable node would give the accrual a finite `projected_ready`
   *meaningfully better* than its current bound — any finite bound when
   the current one is indefinite, or earlier by at least
   `replan_min_improvement_us` (scheduler config, default 300 s) between
   finite bounds — revoke and reseat it there, usually re-accruing. An
   immediate fit is just the degenerate case `projected_ready = now`, and
   a node whose bound would be indefinite is never a move target. The
   move's batch shape is a lend's revoke-then-reseat, so it is gated on
   the exact batch simulator the same way.

Both tiers obey the same anti-churn rule as everything else: a revocation
is only planned when it enables a concrete, strictly better reseat in the
same batch, never in place with no gain — the threshold keeps
finite-to-finite moves from oscillating, and the fixpoint rule below
still holds.

## Work bounds

- `max_candidates` (default **4096**) bounds scored-and-considered jobs per
  pass. Scoring is `O(#queued)` cheap float arithmetic with memoized
  penalties, so this remains fast well past the pass's own budget even at
  1M queued jobs; the cap exists to bound the *seating* work that follows,
  not the scoring.
- `max_placements_per_cycle` (default **512**) bounds placements emitted
  per pass; the seating loop stops once it is reached. Revocations are not
  counted against it — they are already bounded by the replicated accrual
  cap `K` (`policy.accrual_limit`, default 4).
- Node scans are `O(#nodes)` per candidate (best-fit and backfill node
  choice both scan the node set once). Candidate-bound scans (accrual
  opens and improvement moves) additionally sweep each scanned node's
  release events; opens are bounded by K per pass via the accrual guard,
  and improvement scans are explicitly capped at K per pass so an
  over-cap backlog cannot wedge the pass.

## Anti-churn and the fixpoint rule

Revocations are proposed **only** when they enable a concrete placement in
the same batch — a reseat-elsewhere (previous section) or a strict-backfill
lend. The pass never revokes and reseats a job in place for no gain.

This is what makes the pass a genuine fixpoint: a pass over a snapshot
where nothing is actionable returns an empty proposal
(`PlacementProposal::is_empty()`), and running the pass again immediately
after applying its own proposal must also yield an empty proposal. The
driver's backoff behavior depends on this — without it, churn (revoke,
reseat, revoke the same thing again next pass) would look like progress
and spin.

## Related

- [ADR 0021](../decisions/0021-effective-score-ranking.md) — the
  effective-score formula candidates are ranked by.
- [ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md) —
  why accruing allocations replace standalone reservations, and the
  strict-backfill rule this page implements.
- [ADR 0027](../decisions/0027-finite-projected-ready-accrual-protection.md) —
  why an indefinite `projected_ready` forbids lending, and the
  finite-first placement and improvement-move rules.
- [scheduling-model.md](scheduling-model.md) — the operating model and
  vocabulary (accrual, license to backfill, `projected_ready`, K) this
  page assumes.
- [quotas-and-priorities.md](quotas-and-priorities.md) — the quota and
  priority policy `effective_score` is built from.
