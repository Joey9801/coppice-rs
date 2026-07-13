# 29. Runtime-declaration incentives: unbounded-rate multiplier and partial refunds

- **Status:** Accepted
- **Date:** 2026-07-13
- **Extends:** [ADR 0019](0019-deterministic-quota-arithmetic.md)
- **Amends:** [ADR 0005](0005-cost-based-soft-quotas.md) (true-up no longer
  always refunds the full unused charge)

## Context

Declared `max_runtime` bounds are the load-bearing input to everything the
scheduler can *prove*: they are the sole source of guaranteed release events,
of finite `projected_ready` bounds, and therefore of backfill legality and
the accrual-progress properties of
[ADR 0014](0014-accruing-allocations-replace-reservations.md) and
[ADR 0027](0027-finite-projected-ready-accrual-protection.md). A cluster
whose jobs carry honest, reasonably tight bounds backfills well and can
answer "when will this whale start?"; a cluster of unbounded or grossly
overdeclared jobs cannot.

The costing model gives users almost no price reason to supply such bounds.
Under ADR 0019, a placement charges `rate × max_runtime × multiplier` up
front (jobs with no bound are charged a policy default,
`default_charge_runtime_s`, currently 24 h), and true-up at resolution
refunds the *entire* unused portion, decayed. The declared bound is
therefore a **free option**: declaring 10× your true runtime elevates your
entity's decayed usage only while the job runs — a modest, temporary
effective-score penalty on your own queued work — and costs exactly nothing
once the job resolves. The two real incentives are asymmetric and weak in
the direction that matters:

- Declaring too *small* is severely punished: the job is killed at the bound
  (`MaxRuntimeExceeded`), and a retry pays for a whole fresh attempt.
- Declaring too *large*, or nothing at all, is punished only by backfill
  ineligibility and accrual repulsion (ADR 0027 rule 2) — real but indirect,
  invisible at submission time, and irrelevant to users whose jobs place
  promptly anyway.

Rational users therefore pad bounds heavily or omit them, which is
individually harmless and collectively corrosive: padded bounds push
`projected_ready` far into the future (making lends that would in fact be
safe illegal), and missing bounds make it indefinite (forbidding lends
entirely and repelling accruals).

Two remedies were considered and rejected:

- **Continuous metering of running jobs** (charge unbounded jobs
  incrementally at an elevated rate, rather than up-front-and-true-up). This
  matches the intuition "unbounded jobs accrue cost faster" literally, but
  requires periodic replicated writes per running job — a new tick-driven
  command stream through the apply loop for a result the existing
  charge/true-up pair can express exactly, since the elevated rate is known
  at placement time.
- **A per-entity calibration reputation** (track each entity's historical
  declared-vs-actual ratio and scale future charges by it). A sharper
  instrument, but it adds replicated per-entity state, a gaming surface
  (sacrificial well-calibrated small jobs laundering a whale's reputation),
  and a cold-start problem. Deferred, in the spirit of ADR 0027's
  lend-credit: the escape hatch if flat incentives prove too blunt, not the
  v1 mechanism.

## Decision

Two replicated policy knobs make runtime declarations priced honestly: an
**unbounded-rate multiplier** for jobs that decline to declare a bound, and
a **refund fraction** that makes overdeclared bounds cost something lasting.
Each mechanism covers exactly the hole the other leaves open, and they never
stack.

### Unbounded-rate multiplier

`PolicyConfig` gains `unbounded_runtime_multiplier: u64`, a **Q32.32**
multiplier (same representation as `PriorityMultiplier`), validated
`≥ 2³²` (i.e. ≥ 1.0), **default 2.0** (`8 589 934 592`).

At placement (`commit_placements`), a job with no enforced `max_runtime` has
the multiplier folded into its effective priority multiplier:

```
m' = m                                        if max_runtime declared
m' = ⌊m × unbounded_runtime_multiplier / 2³²⌋  otherwise      — u128, saturating
```

`m'` is what the existing arithmetic already threads everywhere: the
placement charge is `cost_from_rate(rate, default_charge_runtime_s, m')`,
`m'` is recorded on the attempt beside the charge record, and true-up
recomputes actual cost with the recorded `m'` — so an unbounded job is
charged, *and settles*, at the elevated rate, with **no new state-machine
arithmetic and no charge-record format change**. Policy edits mid-flight do
not reprice, exactly as for weights and priority multipliers. If an
unbounded job outruns `default_charge_runtime_s`, the existing surcharge
path (`A > C`) bills the excess — now at the elevated rate, as it should.

### Partial refund of unused charge

`PolicyConfig` gains `refund_fraction_milli: u32`, parts-per-thousand of the
unused charge that true-up refunds (the convention of
`penalty_exponent_milli`), validated `≤ 1000`, **default 750**. The
remainder stays in the entity's decayed usage — a lasting, decaying record
that the declared bound was not real.

The charge record gains the fraction captured at charge time,
`refund_fraction_milli: u32`, so mid-flight policy edits do not reprice.
True-up becomes:

```
unused     = C − A                                   (as today; A > C unchanged)
refundable = ⌊unused × f / 1000⌋                     — u128, saturating
R          = decay(refundable, charge_ts → resolution_ts)
usage      = usage.saturating_sub(R)                 per ancestor, after touch
```

where `f` is:

- **1000 (full refund)** when the attempt never reached `Running` — cancel
  or revoke while queued/accruing stays free;
- **1000 (full refund)** when the outcome's `OutcomeClass` is `Platform`
  (`Revoked`, `NodeLost`, `AgentError`, platform-side pull/start failures) —
  the user got nothing they asked for on a placement the platform chose;
- **1000 (full refund)** when the job declared no `max_runtime` — the
  synthetic `default_charge_runtime_s` is the platform's estimate, not the
  user's claim, and retaining half of an arbitrary 24 h charge would price
  unbounded jobs catastrophically instead of at the multiplier (see below);
- **the recorded fraction** otherwise: the attempt ran, ended in `Success`,
  `UserError`, or `UserRequest`, and the gap between bound and actual is the
  user's own declaration error.

The outcome, its class, and the observed runtime are already carried in the
committed outcome command, so `f` is a deterministic function of committed
data — no new inputs enter the state machine.

ADR 0019's invariants survive scoped, not broken:

- **Requeue is still free by arithmetic.** `Revoked` is `Platform`-class and
  pre-`Running`, so both guards give `f = 1000` and the full decayed charge
  returns, preserving ADR 0013's requirement without a carve-out *in the
  arithmetic* — the carve-out lives in outcome attribution, which ADR 0013
  already maintains for retry budgets.
- **Platform-fault retries stay cheap.** Each attempt trues up separately;
  a node loss refunds the unused portion in full, so the user pays actual
  consumption per failed attempt, as today.
- **A job that runs exactly its bound still trues up to exactly zero**
  (`unused = 0`), and `MaxRuntimeExceeded` retains ~nothing for the same
  reason — the kill, not the retention, is the underdeclaration penalty.
- Retention weakens the "entity lands where it would have been had `A` been
  charged at placement" identity to: had `A + retained` been charged at
  placement. That is the point — the retained portion *is* usage, decaying
  on the normal horizon, so overdeclaration now has exactly the lasting,
  self-healing effective-score consequence that charging has.

### The two mechanisms are exclusive and jointly closing

Retention without the exclusivity rule would recreate the problem it
solves: an unbounded job finishing in 2 h against the 24 h synthetic charge
would retain 11 h of cost, making "no bound" ruinously priced and pushing
users to declare fake huge bounds instead — strictly worse, because a fake
bound *enters the guaranteed sweep* and poisons `projected_ready` for every
accrual on the node, while an honest "unbounded" at least keeps the node
lend-free (ADR 0027). Hence: multiplier for undeclared bounds, retention for
declared ones, never both.

Closing both holes matters because each mechanism alone is gameable into the
other's gap: with only the multiplier, declare `max_runtime = 10 years` and
pay 1.0× with a worthless bound; with only retention, declare nothing and
pay 1.0× with no bound at all. Together they define a clean break-even. For
expected runtime `E`, declared bound `M`, multiplier `μ`, refund fraction
`φ`: declaring prices at `E + (1−φ)(M−E)` against `μE` unbounded. For
`φ < 1`, declaring is strictly cheaper when

```
M < E × (1 + (μ−1)/(1−φ))
```

indifferent at equality, and dearer beyond. At `φ = 1` (full refund — one
half of the neutral configuration) the declared price collapses to `E`
regardless of `M`: declaring beats unbounded whenever `μ > 1` and is
indifferent when `μ = 1`, i.e. the band is infinite and only the multiplier
carries the incentive.

At the defaults (`μ = 2.0`, `φ = 0.75`) the price system rewards any bound
within **5×** of expected runtime, is indifferent at 5×, and prefers honest
unboundedness beyond it — deliberately: a bound you cannot place within ~5×
is noise the backfill math is better off without, and the non-price
incentives (backfill eligibility, accrual attraction) still favour declaring
for anyone who wants to start sooner. Operators move the honesty band by
tuning `μ` or `φ` (tightening to 2× with `μ = 1.5`, `φ = 0.5`; widening to
9× with `μ = 3.0`, `φ = 0.75`); setting `unbounded_runtime_multiplier = 2³²` and
`refund_fraction_milli = 1000` restores today's behaviour exactly.

### Incentive properties

Stated for property tests, in the style of ADR 0027:

- **(I1) Overdeclaration costs, monotonically.** For a fixed actual runtime
  and job-attributable outcome, total settled cost is non-decreasing in the
  declared bound, and strictly increasing whenever the larger bound retains
  at least one more µCU through quantization. Exact strictness is not
  achievable in the implemented arithmetic: ceil-seconds rounding, the
  fixed-point floors, and saturation all plateau — two bounds within the
  same rounded second settle identically, as do any two once the charge
  saturates.
- **(I2) Truth-telling is optimal within the band.** For `φ < 1`, declaring
  `M` strictly under `E(1 + (μ−1)/(1−φ))` prices strictly below unbounded,
  and equality at the threshold is indifferent; for `φ = 1` declaring any
  `M ≥ E` prices below unbounded whenever `μ > 1`. In all cases `M = E` is
  the cheapest declaration that survives the run.
- **(I3) Requeue and platform faults are free of retention.** For any
  `Platform`-class or pre-`Running` resolution, settled cost equals the
  ADR 0019 value bit-for-bit.
- **(I4) Aborting earlier never costs more.** Settled cost
  `A + (1−φ)(C−A)` is non-decreasing in `A`, so killing a doomed job sooner
  is always weakly cheaper than letting it run.

### Encoding and compatibility

Both knobs are replicated policy (they change replicated arithmetic — the
ADR 0020 litmus test), carried as new fields on the `PolicyConfig` proto
message with **explicit presence**; absent decodes to the neutral values
(`2³²`, `1000`), so a policy written by an old coordinator round-trips to
today's behaviour and the fields are safe under ADR 0003's mixed-version
window. The charge-record field is likewise presence-gated: charges recorded
before the upgrade true up at `f = 1000`. Validation at policy-commit time
(the tooling side, like λ and quota stocks): `unbounded_runtime_multiplier ≥ 2³²`,
`refund_fraction_milli ≤ 1000`.

## Consequences

- Backfill viability stops depending on user goodwill: padding a bound or
  omitting one now has a price proportional to the padding, settled into
  decayed usage where it does what charging does — lowers the entity's
  effective score on the same rolling horizon, then heals.
- `true_up` grows a fraction parameter and its callers must supply the
  attribution — a deterministic function of the committed outcome via the
  existing `OutcomeClass`, but it does couple quota arithmetic to outcome
  classification for the first time. A future outcome variant must decide
  its class with retention in mind.
- The explainability contract widens: "why is my usage still high?" now has
  the answer "your job declared 10 h, ran 2 h, and 50% of the unused 8 h was
  retained". The observability surface (usage breakdowns, CLI, web UI)
  should attribute retained amounts distinctly from charges for running
  work; until it does, retention will read as a billing bug to users.
- The break-even band is a real cliff for high-variance workloads: a job
  whose runtime is genuinely unpredictable within 5× is now *correctly*
  priced toward unbounded, which costs the cluster backfill opportunities it
  never really had. If measurement shows honest-but-wide bounds being pushed
  to unbounded at scale, the remedies are tuning (`μ` up, `φ` up) or the
  deferred per-entity calibration mechanism — not exempting wide bounds.
- Users who abort promptly are rewarded (I4), but a failed job now settles
  above its consumption when its bound was padded (`UserError` retains).
  That is intended — the padding, not the failure, is what is billed — but
  it compounds with retry: a job that pads 10× and crash-loops through its
  retry budget pays the padding penalty per attempt. Documentation should
  say plainly: calibrate first, retry second.
- Two more integers in `PolicyConfig`, one more in the charge record, no new
  float, no new command, no change to decay, scoring, or the snapshot
  model beyond the presence-gated fields. Cross-version determinism holds
  because both fields decode to neutral values when absent and are captured
  per-charge thereafter.
