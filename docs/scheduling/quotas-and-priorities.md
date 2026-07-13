# Quotas and Priorities

Quota and priority management is built into the scheduler rather than bolted
on later. The model was decided in
[ADR 0005](../decisions/0005-cost-based-soft-quotas.md): **cost-based soft
quotas over a generic entity tree, with no hard limits**. The exact
arithmetic — representations, decay algorithm, true-up, and penalty shape —
was decided in
[ADR 0019](../decisions/0019-deterministic-quota-arithmetic.md) and is
implemented once, as pure functions, in `coppice_core::quota`; the apply loop
and the scheduler both call that module.

## The quota-entity tree

Quota policy is expressed over a tree of **quota entities**. Levels carry no
built-in meaning — one deployment may use org → team → user, another just
user. Each entity has a parent, a soft quota, and its configuration is
replicated policy state. Every job is submitted under exactly one leaf entity
and charges every ancestor on its path.

A **soft quota** is replicated as a stock in µCU: the decayed-usage level
that counts as "at quota". Humans configure a rate ("100 core-hours per
day"); tooling converts it via `quota_stock = rate × half_life / ln 2`, the
level that charging at that rate forever converges to.

## Cost units

All cost and usage bookkeeping uses **`CostUnits`**: unsigned 64-bit
integers counting **micro-cost-units** (µCU, 1 CU = 10⁶ µCU), with
saturating arithmetic and `u128` intermediates. There are no floats anywhere
in replicated state, commands, or snapshots. The reference calibration is
1 CU ≈ 1 core-second, with other dimensions priced relative to it by policy
weights. ADR 0019 carries the overflow-horizon analysis (decay bounds
accumulated usage regardless of cluster age; a `u64` leaves ~500× headroom
for a 100k-core cluster at the default half-life).

## Job cost

Each job gets a single scalar cost, computed in fixed point at submission
and carried in committed commands:

```
rate (µCU/s) = Σ over dimensions ⌊quantity × weight / 2³²⌋
cost (µCU)   = ⌊rate × runtime_seconds × priority_multiplier / 2³²⌋
```

- Weights are **Q32.32** fixed point, µCU per resource-unit-second, one per
  dimension — replicated policy, so new dimensions (GPUs) are priced by
  adding entries.
- `runtime_seconds` is the declared `max_runtime`, rounded **up** to whole
  seconds. Declaring a tighter `max_runtime` lowers cost — and makes the job
  backfillable (see [scheduling-model](scheduling-model.md)).
- The user-chosen priority maps (via a policy table) to a **Q32.32
  multiplier** on cost: users burn budget faster to push one important job
  forward. Priority is not a free lane.
- A job placed with no *enforced* `max_runtime` has that multiplier itself
  inflated: `m' = ⌊m × unbounded_runtime_multiplier / 2³²⌋` (replicated
  policy, Q32.32, default 2.0), and `m'` — not `m` — is what's threaded
  through the charge above, the surcharge on overrun, and true-up. Declaring
  a bound is cheaper than going unbounded up to about 5× your expected
  runtime; see
  [ADR 0029](../decisions/0029-runtime-declaration-incentives.md) for the
  break-even and the reasoning.

## Decayed usage

Each entity holds a replicated `(accumulated_usage, last_update_timestamp)`
pair. Usage decays exponentially with a configurable half-life (default
24 h), computed lazily when a command touches the entity — and the decay
algorithm is exact, integer-only, and compositional (ADR 0019):

- Time is quantized to **ticks** on absolute boundaries
  (`tick_index = ⌊timestamp / tick_us⌋`, default tick 60 s).
- Replicated policy carries the per-tick retention factor λ as a Q0.64
  integer, derived from the half-life by config tooling — no `exp` runs in
  the state machine. The default (24 h half-life, 60 s tick) is
  `decay_per_tick = 18 437 866 829 417 916 986`.
- Decay over n ticks is the n-fold iteration of `u ← ⌊u · λ⌋`, which makes
  `decay(decay(u, n₁), n₂) = decay(u, n₁ + n₂)` hold **exactly** — replicas
  that bring an entity forward in different numbers of hops agree bit for
  bit. Usage below 1 µCU truncates to zero.
- Clock skew across leaders is clamped: a command timestamp at or before the
  stored one decays nothing and never rewinds `last_update_timestamp`.

## Charging and true-up

At placement commit, every ancestor is decayed to the command's tick and
charged the job's full cost; the attempt records `(amount, charge time)`. At
attempt resolution ([ADR 0013](../decisions/0013-job-attempt-allocation-state-machines.md)'s
`Finalizing` funnel), the actual cost is recomputed with the same weights,
multiplier, and ceil-seconds rounding over observed runtime, and the
difference is settled:

- **Refund**: the unused portion of the charge is only *partly* returned.
  Replicated policy `refund_fraction_milli` (parts-per-thousand, default
  750) is captured onto the charge record at placement time, so mid-flight
  policy edits never reprice an in-flight charge. True-up refunds
  `unused × f / 1000`, **decayed from charge time to resolution time**; the
  rest stays in the entity's decayed usage as a lasting record that the
  declared bound overshot the run. `f` is 1000 (full refund) — recovering
  ADR 0019's original behaviour exactly — whenever the attempt never
  reached `Running`, whenever the outcome's `OutcomeClass` is `Platform`
  (`Revoked`, `NodeLost`, `AgentError`, platform-side pull/start failures),
  or when the job declared no `max_runtime` (the unbounded multiplier above
  already prices that case; retaining against the synthetic
  `default_charge_runtime_s` charge would price it twice). Otherwise — the
  attempt ran and ended in `Success`, `UserError`, or `UserRequest` — `f` is
  the recorded fraction, and the gap between bound and actual is priced as
  the user's own declaration error.
- **Surcharge** (post-`max_runtime` kill grace only): the excess is charged
  fresh, at the recorded (possibly unbounded-inflated) multiplier.

**Requeue is still free by arithmetic**: a `Revoked` attempt never ran, so
its actual cost is zero, its `OutcomeClass` is `Platform`, and both guards
give `f = 1000` — the full (decayed) charge comes back, as ADR 0013
requires. Platform-attributed failures (`NodeLost`, `AgentError`, pull/start
failures) refund in full for the same reason: the platform, not the user,
chose the placement that didn't work out. Retries charge each placement
anew and true up each attempt separately. See
[ADR 0029](../decisions/0029-runtime-declaration-incentives.md) for the
full mechanism, the attribution rule, and the incentive properties it's
meant to establish.

## Effective priority

There is no quota-based admission rejection. Exceeding a soft quota never
blocks work; it lowers the effective priority of the owner's queued jobs, so
a quiet cluster is always fully usable. Queued jobs are ordered by:

```
effective_score = base(job) / Π over ancestors a of penalty(usage_a / quota_a)

penalty(x) = 1    for x ≤ 1
           = x^p  for x > 1        (p = penalty_exponent_milli / 1000, default 2)
```

The quadratic default is superlinear — sustained overuse cannot be linearly
bought back with priority multipliers — while staying human-checkable: an
entity at 3.1× its quota has its jobs deprioritized 9.6×. Ties break FIFO by
submission time.

## Determinism and replication

The entity tree, quota configuration (weights, multiplier table, decay
policy, penalty exponent — all integers), and per-entity
`(accumulated_usage, last_update_timestamp)` are Raft-replicated. Decay is
computed from timestamps carried in committed commands, never from wall
clock during apply, and all replicated arithmetic is bit-deterministic
integer fixed point. Effective scores are **derived** state recomputed by
the scheduler — the one place `f64` is allowed, tolerable because placements
are validated against replicated integer state at commit.

## Explainability

The scheduler must be able to explain why a job is pending: quota penalty
(entity at n× its soft quota, jobs deprioritized n²×), priority ordering,
constraints, resource shortage, allocation accrual, or policy. This
requirement is shared with
[../operations/observability.md](../operations/observability.md).

## Deliberately excluded from v1

Hard resource limits and preemption. Hard caps can be added later as an
optional per-entity field without disturbing this model.
