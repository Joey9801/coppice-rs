# 46. Interval charging for jobs with no declared runtime

- **Status:** Accepted
- **Date:** 2026-10-12
- **Amends:** [ADR 0019](0019-deterministic-quota-arithmetic.md) (an
  unbounded job's placement charge) and
  [ADR 0029](0029-runtime-declaration-incentives.md) (the synthetic
  `default_charge_runtime_s` window, and its rejection of periodic charging)
- **Part of:** issue #35, gap 3 (long-lived jobs). Service jobs themselves —
  a job kind, restart-on-exit, ports, service pools — are out of scope here.

## Context

A placement charges `rate × runtime × m'` to every ancestor up front and
trues up against actual consumption when the attempt reaches a terminal
state (ADR 0019). For a job with a declared `max_runtime` the runtime is that
bound, and the scheme is sound: the charge is the user's own claim, and the
retention of ADR 0029 prices padding it.

For a job with **no** declared bound, the runtime is a policy constant,
`default_charge_runtime_s` (24 h), priced at the elevated unbounded rate
`m' = m × unbounded_runtime_multiplier`. That number is arbitrary in both
directions:

- A two-minute unbounded job is charged a day of capacity at placement. It
  is all refunded at true-up (ADR 0029 never retains against the platform's
  own estimate), but while it runs the entity's decayed usage — and so its
  effective score — is inflated by ~720× the honest figure.
- A job that runs longer than 24 h has, from then on, consumed capacity it
  has not paid for. Nothing is billed until it terminates, and a job that
  never terminates never trues up: its single charge decays on the 24 h
  half-life while it holds capacity indefinitely. After a few half-lives the
  owner looks within quota and their queued work schedules as though the
  long runner were free. This is the hole issue #35 calls out for
  service-like jobs, but it already applies to any long unbounded batch job.

ADR 0029 rejected "continuous metering of running jobs" because it needs
periodic replicated writes per running job, for a result the charge/true-up
pair could express exactly *when the runtime is known at placement*. For an
unbounded job it is not known, which is precisely the case where the single
charge goes wrong.

## Decision

A job with no declared `max_runtime` pays for its runtime in **intervals
charged ahead of time**, each renewed as the previous one is used up. A job
with a declared bound is charged exactly as before.

### Policy

`PolicyConfig.default_charge_runtime_s` is **removed** and replaced by
`unbounded_charge_interval_s` — replicated policy (it changes replicated
arithmetic, ADR 0020's litmus test), **default 1800 (30 min)**, validated
`> 0` at `UpdatePolicy`. The proto field is retired by reservation (no
back-compat: the project has no deployed clusters to carry across).

### Placement

`CommitPlacements` charges an unbounded job **one interval**:
`cost_from_rate(rate, interval_s, m')`, with `m'` folded exactly as ADR 0029
specifies. The attempt's charge record captures the interval length and an
interval count of 1. As with weights, multipliers and the refund fraction,
the interval is frozen per attempt: a policy edit never re-chunks an attempt
already placed.

### Renewal

A new internal command, `RenewIntervalCharges`, carries a batch of
`(attempt, paid)` items and the proposer's timestamp `proposed_at`. Per item,
apply:

1. **skips** it if the attempt is missing, is not `Running` (before
   `Running` nothing has been consumed; a `Finalizing` attempt has already
   exited, and any overrun is the true-up surcharge's to bill), or its
   recorded interval count is no longer `paid` — the
   compare-and-set that makes duplicate proposals across a leader change
   and races with attempt termination harmless;
2. **rejects the batch** if the attempt's charge has no interval (a bounded
   job listed by the proposer is a bug, as with `EvictTerminalJobs`);
3. computes the intervals due, `target = ⌊(proposed_at − started_at) /
   interval⌋ + 1` — the interval the attempt is now in, plus everything
   before it — and, if `target > paid`, charges
   `cost_from_rate(rate, (target − paid) × interval_s, m')` to every
   ancestor at `proposed_at`, using the attempt's recorded rate and
   multiplier. The record's cumulative amount grows by that, its count
   becomes `target`, its charge timestamp becomes `proposed_at`, and it
   remembers this tranche's amount and the previous charge timestamp (see
   *Settlement*).

The interval count is measured in **runtime**, from `started_at`: an attempt
waiting in `Accruing` or `Ready` consumes nothing and renews nothing, so its
first interval covers its first `interval_s` of actual running. Because
`target` is computed from the elapsed runtime rather than incremented, a
leader outage longer than an interval is caught up in one item.

Renewal emits no event: it changes no job, attempt or allocation state, and
the job event stream (ADR 0043) has no consumer for it. A job's cost report
reads the cumulative charge from the attempt record as before.

### Proposer

The leader's housekeeping loop gains a renewal pass on its existing tick:
it finds running attempts whose paid intervals are used up, by its own wall
clock, and proposes `RenewIntervalCharges` in capped batches like the
eviction passes. Renewal therefore lags an interval boundary by at most one
housekeeping tick (60 s). That lag is the only runtime an unbounded job ever
consumes before paying for it, and if the job ends inside it the existing
true-up surcharge bills it.

### Settlement

True-up is unchanged arithmetic over the cumulative record: actual cost
`rate × ceil-seconds(runtime) × m'` against the sum of the intervals
charged. An unbounded job's refund fraction is already `1000` (ADR 0029), so
**the unused remainder of the final interval is refunded in full** — the
interval length is the platform's choice, not the submitter's claim, and the
submitter already pays for unboundedness through the multiplier.

The refund must decay from when the unused charge was made. Normally all of
it lies in the most recent tranche, but an attempt can exit just before a
renewal and have its exit reported just after, so the unused runtime spans
the new tranche and the tail of the previous one. The record therefore keeps
the most recent tranche's amount and the charge timestamp before it: the
refund up to that tranche decays from the latest charge, and any remainder
from the previous one. This is exact unless an exit report lags the exit by
more than a whole previous tranche, in which case the older remainder decays
from the previous charge — a small, bounded over-refund, accepted rather
than keeping a per-tranche history that would grow without limit for a
long-running job.

Platform outcomes and pre-`Running` resolutions are untouched: an attempt
revoked while accruing never renewed, and gets its one interval back in
full, so requeue stays free by arithmetic (ADR 0019).

## Consequences

- An unbounded job's usage tracks what it has actually run, rounded up to
  the interval, at the elevated rate. A long-running job keeps paying for as
  long as it holds capacity, so it can no longer decay out of its owner's
  quota picture — the fairness hole issue #35 identified closes for service
  jobs and long batch jobs alike. A short unbounded job no longer
  temporarily inflates its owner's usage by a day's worth of charge.
- The nominal settled cost is unchanged: an unbounded job still settles at
  `μ × actual`, so ADR 0029's break-even arithmetic, stated in µCU charged,
  still holds. But *when* the cost lands now differs from a bounded job's,
  and decayed usage is what the scheduler sees. A bounded job's net cost
  effectively enters usage at placement: it is charged up front, and its
  refund decays from that same instant, so the cost it keeps has been
  decaying since the start of the run. (Under the old synthetic 24 h
  charge, an unbounded job shorter than the window behaved the same way.)
  An unbounded job's cost now enters usage spread across the run, one
  interval at a time, so most of it has decayed for less time by the end
  of the run. Even at `μ = 1`, for the same runtime, an unbounded job
  leaves its owner's decayed usage higher at the end of the run, and from
  then on, than a bounded job does. Charged evenly over runtime `T` against
  half-life `H`, the excess factor is about `(H / (T ln 2)) × (2^(T/H) − 1)`:
  negligible for short jobs (~1.01× for one hour at the default 24 h
  half-life), about 1.44× when `T = H`, and growing exponentially beyond
  that. This is accepted deliberately, because it pushes in the right
  direction: it is a further, timing-based premium on declining to state a
  bound, on top of the multiplier. Measured in decayed usage rather than
  µCU charged, the honesty band of ADR 0029 is therefore somewhat wider
  than its formula states, more so for long-running jobs.
- This is the periodic replicated write ADR 0029 declined: one command per
  housekeeping tick in which any unbounded attempt crosses a boundary,
  batched across attempts. At a 30-minute interval the volume is roughly
  `running unbounded attempts / 1800` items per second, well inside the
  apply budget; operators trade write volume against how far usage can lag
  behind consumption by tuning the interval.
- Renewal timing depends on two clocks — the agent's `started_at` and the
  leader's `proposed_at` — as runtime measurement already does. Skew delays
  or advances a renewal by the skew; it can never double-charge, because of
  the compare-and-set on the interval count.
- The job cost report's charge window for an unbounded job is now the
  interval, and its estimate is the cost of one interval rather than of a
  synthetic day. `cost_ucu` stays gross-only and sums the intervals charged.
- Nothing here gives a never-ending job a scheduler-visible release event:
  ADR 0027's lend-freedom for nodes with unbounded accruals, and issue #35's
  service segregation, are unchanged and remain future work.
