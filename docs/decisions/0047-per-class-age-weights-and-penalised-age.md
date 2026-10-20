# 47. Per-class age weights and a penalty-scaled age term

- **Status:** Accepted
- **Date:** 2026-10-20
- **Amends:** [ADR 0021](0021-effective-score-ranking.md) (the formula and
  its consequences 2 and 3) and [ADR 0044](0044-job-preemption-and-spot-capacity.md)
  §4 (what the score rule compares)
- **Extends:** [ADR 0005](0005-cost-based-soft-quotas.md)

## Context

ADR 0021 ranks queued work by

```
effective_score = m / P + w_age · age / H
```

with one scheduler-wide `w_age`. The age term is additive on purpose: it is
the anti-starvation guarantee, and it makes priority a bounded head start
measured in half-lives rather than a permanent lane. That was the right
posture for a handful of classes whose work is all meant to finish soon.

Two needs have since arrived that the formula cannot express.

**A backfill class.** Operators want a low class, say 0.5× against the 1×
default, for long-lived, non-urgent work such as grid searches that sit
queued for weeks and soak up spare capacity. Backfill should stay behind
higher-class work indefinitely, not for a few half-lives, unless the
higher-class work comes from an entity with a significant over-quota
penalty. Under ADR 0021 the age term crosses any class gap in finite
time: a 0.5 gap is worth twelve hours at the default horizon, so backfill
queued on Friday evening outranks Monday morning's fresh, under-quota
default work, and the penalty plays no part in it. The same age gap makes
old backfill effectively unpreemptible: ADR 0044 §4 scores a running
victim from its submit time, so a default job can only preempt backfill
submitted less than twelve hours before it.

**Autoscaling.** The autoscaling design under discussion scales a pool by
asking the real ranking function which queued jobs would win a new node.
Backfill makes that answer wrong twice over: a queue of grid searches
keeps the cluster looking permanently under-provisioned, and under the
additive age term a node bought for fresh default work is absorbed by the
backfill that outranks it. Autoscaling therefore depends on backfill
staying behind by construction, so that the estimator can count demand by
class and trust that capacity added for one class is taken by it.

Two further observations about the additive form shaped the decision:

- **The penalty's effect on waiting is capped at one horizon.** Because the
  age term is outside the quotient, an entity at any finite penalty reaches
  parity with fresh under-quota work of its class within `H`, however large
  `P` is. Past a penalty of about four the age term swamps the penalty
  entirely, which is why `penalty_exponent_milli` feels toothless.
- **A zero-weight class alone leaves a gap in the override.** With backfill
  not aging and default work still aging additively, a default job's own
  age exceeds the backfill multiplier after `m_backfill · H / w_age`, twelve
  hours by default, after which no penalty can let backfill pass it.

## Decision

### The formula

```
effective_score(j, now) = (m(j) + w(class(j)) · age(j, now) / H) / P(j, now)
```

`m`, `P`, `age` and `H` are exactly as ADR 0021 defines them. Two things
change:

1. **The age weight is per class.** `w(class)` is looked up by the job's
   `priority` index, the same index `priority_multipliers` is keyed by,
   in a scheduler-side table. A class with no entry uses the scheduler-wide
   `w_age`, which keeps its current meaning and default of 1.0. A weight may
   be zero: that class never ages.
2. **The penalty divides the whole numerator.** The age credit is scaled by
   `1 / P` along with the priority term. Equivalently, the penalty slows the
   entity's clock: an entity at penalty `P` ages at `w / P` points per
   horizon.

Both remain scheduler-local, on ADR 0019's derived side of the line, for
the reason ADR 0021 gave: scores order a proposal that apply re-validates
against replicated integer state, so replicas need not agree on them. The
evaluation shape is still one addition and one quotient, `P ∈ [1, +∞]`,
and `finite / +∞ = 0`, so no `NaN` is reachable.

### Configuration

The coordinator's `[scheduler]` table gains a per-class override table
beside `w_age`:

```toml
[scheduler]
w_age = 1.0            # default age weight, every class without an entry

[scheduler.age_weights] # overrides keyed by the class's priority index
-1 = 0.0                # backfill: never ages
```

Keys are the integer `priority` values of `priority_multipliers`; values
are finite and non-negative, validated at load like `w_age`. An entry for
a class the replicated table does not define is accepted and inert, so
scheduler TOML need not be sequenced against policy edits.

### The preemption score rule

ADR 0044 §4's score rule is unchanged in form: the victim's
`effective_score` must be below the beneficiary's, both by this formula
with the pass's weights and `now`. A zero-weight victim therefore scores
`m / P` however long it has run, and is preemptible by any higher-class
job whose own `m / P` exceeds it.

## Consequences

1. **Within an entity, nothing changes.** `P` cancels between two jobs on
   the same quota path, so a user's own queue orders by `m + w · age / H`
   as before, and ADR 0021's Friday-evening crossover holds with the same
   head start, `Δage < (Δm / w) · H`, now independent of `P`.
2. **A zero-weight class is a permanent lane behind every aging class,
   with the penalty as the only way through.** Fresh under-quota default
   work beats weeks-old backfill indefinitely. Queued backfill overtakes a
   default job only when the default entity's penalty exceeds
   `(m_default / m_backfill) · (1 + w · age_default / (m_default · H)) · P_backfill`,
   a threshold that grows with the default job's age but never closes.
   With the 2:1 class ratio and under-quota backfill, that is a penalty of
   two for fresh default work. Running backfill is preempted by default
   work whose entity penalty is below the same ratio. The two thresholds
   are the same number read from either side.
3. **The penalty now scales waiting time, not just rank.** An over-quota
   job of a class with weight `w` reaches parity with fresh under-quota
   work of its class after `(P − 1) · m · H / w` of static penalty. Since
   usage decays on the half-life and the default penalty is quadratic, `P`
   falls fourfold per horizon, so the practical wait grows roughly as
   `log₄(P) · H` rather than linearly. At the default policy an entity
   starting at a penalty of 10 waits about 27 hours instead of 17; at 100,
   about 58 hours instead of 24. `penalty_exponent_milli` is now a wait
   knob as well as a rank knob, and the policy documentation says so.
4. **ADR 0021's third consequence is narrowed.** Aging survives any finite
   penalty: the score still grows without bound in age for `P < ∞`. It does
   not survive an infinite penalty: a zero-quota entity with nonzero usage
   scores zero, so under contention it ranks last, behind every job with a
   finite penalty however old, until its usage decays to exactly zero,
   which ADR 0019's integer decay reaches within about `log₂(usage)`
   half-lives, at most around 64. A zero score is still a rank, not an
   admission rule: the pass seats every candidate that fits, so on a
   cluster with spare capacity the entity's work runs at once. ADR 0005's
   "no hard limits" therefore still holds; what changes is that a zero
   quota no longer ages back toward parity while others are waiting.
   Operators who want an entity to compete at all give it a nonzero quota.
5. **A lump charge retroactively divides accumulated age.** When a user
   places a whale, the entity's penalty spikes and its queued backlog's age
   credit is divided along with it; under ADR 0021 that credit was immune
   to the user's own placements. This is the clock-slowdown model applied
   consistently, softened for bounded jobs by true-up refunds, but it is
   visible: a large placement pushes the same user's week-old backlog
   behind other entities' fresh work until the charge decays.
6. **Autoscaling can trust the ranking.** A demand estimator that counts
   the jobs a hypothetical node would go to now gets a per-class answer
   that backfill cannot inflate, because backfill never outranks the aging
   classes it is meant to yield to.
7. **Between two aging classes the preemption age gap still never
   closes.** A running victim and its beneficiary age at the same rate, so
   with equal penalties a default job still cannot preempt a higher-class
   victim submitted long before it. The per-class weight resolves this for
   the backfill case, which is the one that motivated it; comparing
   `m / P` alone in the score rule would resolve it generally and is left
   for a later decision.

### Not taken

- **Reshaping the additive term** (a log or saturating curve, or a cap).
  Scheduler-only, but any additive curve with finite weight still crosses
  the class gap in finite time, and a cap below the gap starves across
  the board rather than per class.
- **Lexical ordering by class, or a replicated backfill tier flag.** Both
  remove the penalty override entirely and make every class boundary
  absolute, which is stronger than wanted and, for a tier flag, a policy
  and wire change for behaviour the per-class weight already gives.
- **Multiplicative age** (`m · g(age) / P`). At infinite penalty the score
  is zero whatever the age, and at finite penalty the crossover is no
  later than today's. It gives up the guarantee without buying the lane.
- **A separate ranking multiplier distinct from the charge multiplier.**
  Nothing here needs the price ratio and the override threshold to
  differ. The charge side already carries its own factors (ADR 0029, ADR
  0044 §1), so this can be added later without touching the formula.
