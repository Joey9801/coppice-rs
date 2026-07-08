# 21. Effective-score ranking

- **Status:** Accepted
- **Date:** 2026-07-08
- **Resolves:** [OD-13](../roadmap/open-decisions.md#od-13-base-score-and-the-exact-job-costing-formula)
- **Extends:** [ADR 0005](0005-cost-based-soft-quotas.md), [ADR 0019](0019-deterministic-quota-arithmetic.md)

## Context

ADR 0005 introduced `effective_score = base(job) / Π penalty(usage_a /
quota_a)` but left `base(job)` undefined. ADR 0019 fixed the replicated
bookkeeping — cost units, decay, penalty shape — without touching the
scoring numerator, and flagged the gap as
[OD-13](../roadmap/open-decisions.md#od-13-base-score-and-the-exact-job-costing-formula):
within one quota entity the ancestor-penalty product is a shared factor
that cancels, so relative order among a user's own queued jobs is decided
entirely by `base` and the FIFO tie-break. The expected "Friday evening
backlog" behaviour — a later high-priority submission outranks the same
user's earlier low-priority backlog — only holds if `base` is monotone in
priority, and ADR 0005 never said so.

OD-13 raised four specific considerations this decision must settle:

1. Is `base` monotone in the priority multiplier, with FIFO within a level?
2. Should `base` also reflect job size/cost, given that cheap-job bias
   already comes from packing and strict backfill (ADR 0014)?
3. Does the placement-time lump charge of a whale's full `max_runtime` cost
   need smoothing, or does true-up plus decay already cover it?
4. What scheduler data structure keeps rescoring cheap at 1M queued jobs?

## Decision

The formula, implemented once in `coppice_scheduler::score::effective_score`
(with property tests covering the properties below) as the single source of
truth:

```
effective_score(j, now) = m(j) / P(j, now)  +  w_age · age(j, now) / H
```

- **`m(j)` is `base(job)`, and nothing else.** The job's Q32.32 priority
  multiplier, `JobRecord.multiplier.0 as f64 / 2^32`, read as a real number.
  No size or cost term is folded in (OD-13 consideration 2): cheap-job bias
  already comes from best-fit packing and strict backfill (ADR 0014,
  [scheduler-v1.md](../scheduling/scheduler-v1.md)), and a size term inside
  `base` too would double-count it — penalizing large jobs once in
  placement order and again in rank. `base` answers "how urgent"; "how it
  fits" is entirely the packer's job.
- **`P(j, now)`** is ADR 0005's ancestor-penalty product, `Π over ancestors
  a of penalty(usage_a / quota_a)`, computed leaf → root over the job's
  quota-entity path (depth-capped at `QUOTA_TREE_DEPTH_CAP`, stopping at a
  missing parent — mirroring apply's `charge_ancestors` walk exactly), with
  each ancestor's usage first brought forward to `now` via
  `coppice_core::quota::DecayPolicy::decay_between`, the exact integer
  decay from ADR 0019 — never reimplemented here. `penalty` is ADR 0019's
  quadratic derived function, `x^p` past quota, `p =
  penalty_exponent_milli / 1000`.
- **The age term is additive, sitting outside the quotient, not folded into
  it.** `age(j, now) = max(0, now − submitted_at_us)`, clamped at zero so a
  submit stamped ahead of `now` by clock skew earns no age, mirroring ADR
  0019's decay clamp. `H` is the **age horizon**: the decay half-life
  implied by the replicated decay policy (`half_life_ticks = ln 0.5 / ln
  λ`, `λ = decay_per_tick / 2^64`, scaled by `tick_us`), clamped to at
  least one tick so a degenerate policy (λ → 0, instant decay) cannot
  divide by zero. `w_age` is a **scheduler-side knob, default 1.0,
  deliberately not replicated** — like the rest of `SchedulerConfig`
  (`coppice-scheduler/README.md`), it shapes a proposal that is
  re-validated against replicated state at commit, so replicas need not
  agree on it.
- **No lump-charge smoothing** (OD-13 consideration 3). A whale's placement
  charges its full `max_runtime` cost as one lump (ADR 0019), and nothing
  in scoring re-smooths that: the ancestor's decayed usage already reflects
  the charge from the moment it lands, and true-up settles the difference
  at resolution. Smoothing the *score* instead would either double-account
  the charge (once in `P` via decayed usage, once via an artificial score
  ramp) or under-penalize a whale mid-flight. True-up plus decay is
  sufficient; `P` stays the single source of "how over quota is this
  entity, right now."
- **Ties break FIFO by submission time, then `JobId`.** The total order on
  candidates is score descending (`f64::total_cmp`, which stays a total
  order even at the `+∞` an infinite penalty can produce), then
  `submitted_at_us` ascending, then `JobId` ascending — a fully
  deterministic order even among simultaneous submissions.
- **Scheduler data structure (OD-13 consideration 4).** A pass memoizes the
  penalty product per distinct leaf quota entity among the candidates
  (`BTreeMap<QuotaEntityId, f64>`), computed once per entity rather than
  once per job. Rescoring a pass is therefore `O(entities touched)`
  ancestor walks plus `O(candidates)` cheap arithmetic, not `O(candidates ×
  ancestor-depth)` — the per-entity data structure OD-13 asked for, without
  a persistent per-entity queue: the memo is rebuilt each pass from the
  snapshot, which is simpler and just as cheap at 1M queued jobs.

No `NaN` is reachable: `P ∈ [1, +∞]` (penalty is never below 1), `m` is
finite and ≥ 0, `m / +∞ = 0`, and `age` is finite — so `f64::total_cmp`
never has to arbitrate a `NaN`.

## Consequences

1. **Friday-evening monotonicity, with a deliberate aging crossover.** Two
   jobs sharing an entity (so identical `P`) with multipliers `m_hi >
   m_lo`: the higher-priority job outranks the lower-priority one exactly
   while its age deficit stays under the crossover, `Δage < (Δm / (P ·
   w_age)) · H`, where `Δage` is how much *more* age the lower-priority job
   has accumulated and `Δm = m_hi − m_lo`. Once `Δage` grows past that
   bound, aging overtakes priority and the backlog job ranks first again.
   Priority buys a **bounded head start measured in half-lives**, not a
   permanent lane — the same "no hard limits, only degraded priority"
   posture ADR 0005 established for quota, now applied to priority itself.
2. **Composition with ADR 0005's penalty.** The penalty product divides
   only the priority term; the age term is untouched by `P`. Two jobs
   under the *same* leaf entity share the same `P`, so it cancels exactly
   when comparing them — relative order among a user's own queued jobs is
   decided entirely by `m` and age, exactly the within-entity cancellation
   OD-13 called out. Jobs under *different* entities each carry their own
   `P`, so an over-quota entity's whole queue is pushed down uniformly,
   consistent with ADR 0005's per-entity degradation.
3. **Starvation resistance.** Because the age term is additive and sits
   outside the quotient, `effective_score → ∞` as `age → ∞` for *any*
   fixed finite `P` — and even a `P = ∞` entity (zero quota, nonzero usage,
   ADR 0005's worst case) still has its jobs' scores climb via the age term
   alone, toward parity with everyone else. Aging survives an infinite
   penalty, which is what makes "no hard limits" mean no starvation, not
   merely no outright rejection.
4. **Determinism and `f64` discipline (ADR 0019).** Every float in this
   formula is on ADR 0019's derived side of the line — computed fresh each
   pass, never serialized into a command, state, or snapshot. The
   evaluation shape is fixed per job: one quotient (`m / P`), one weighted
   quotient (`age / H`), one addition; `P` itself is a product walked in a
   fixed path order (leaf → root), never accumulated over an unordered
   collection. Candidate ordering uses `f64::total_cmp`, a genuine total
   order, so a pass is a deterministic function of the snapshot even though
   two replicas' floats could differ in the last ulp — tolerable for
   exactly the reason ADR 0019 gives: scores order a *proposal*, and the
   resulting placement is validated against replicated integer state at
   commit.

Monotonicity of `m(j)` in the user-facing priority level rests on the
replicated `priority_multipliers` table itself being monotone — nothing in
`effective_score` enforces this; it is an operator/tooling obligation the
formula inherits from ADR 0019, the same way well-formed decay policy is.

The formula lives in exactly one place, `coppice_scheduler::score`
(`effective_score`, `penalty_product`, `age_horizon_us`, `Rank::cmp`),
covered by property tests for each consequence above (monotonicity in
multiplier, monotonicity in age, penalty composition, the Friday-evening
crossover, and bit-identical determinism on identical inputs). The engine
([scheduler-v1.md](../scheduling/scheduler-v1.md)) calls it and never
reimplements it.
