# 19. Deterministic fixed-point quota arithmetic

- **Status:** Accepted
- **Date:** 2026-07-07
- **Extends:** [ADR 0005](0005-cost-based-soft-quotas.md)

## Context

ADR 0005 settled the quota *model* — cost-based soft quotas over a generic
entity tree, decayed usage, no hard limits — but left the *arithmetic* loose.
The living doc described "a single float cost" and exponential decay of
replicated usage, and defined `penalty(x)` only as "grows monotonically past
1". That looseness is not survivable:

- Per-entity `(accumulated_usage, last_update_timestamp)` is **inside the
  Raft-replicated state machine**. The determinism rules
  ([high-availability.md](../architecture/high-availability.md)) require every
  replica to compute bit-identical state from the same committed commands.
  `f64` arithmetic — and especially transcendental functions like `exp` —
  is not bit-stable across platforms, libm versions, or compiler flag sets.
  A float in replicated state is a divergence bug waiting for a mixed-version
  or mixed-architecture cluster.
- Usage decays **lazily**: an entity's stored usage is only brought forward
  to "now" when a command touches it. Correctness therefore hinges on a
  composition property — decaying in two hops must equal decaying in one —
  that ordinary rounded floating-point (and most rounded fixed-point)
  exponentiation does not have.
- The protobuf schema (ADR 0003, separate task) is about to freeze field
  types for cost, usage, and policy. The integer encoding is easy; this ADR
  must fix what the integers *mean*.

The asymmetry ADR 0005 already established is the escape hatch: replicated
*bookkeeping* must be integer-exact, while *scoring*
(`effective_score`, `penalty`) is derived state recomputed by the scheduler
and validated at commit, where mild float nondeterminism is tolerable. This
ADR keeps every float strictly on the derived side of that line.

## Decision

The arithmetic is implemented once, as pure functions in
`coppice_core::quota`, called by both the apply loop and the scheduler.

### Cost units: `u64` micro-cost-units, saturating

Cost and usage are **`CostUnits`**, a newtype over `u64` counting
**micro-cost-units (µCU)**: 1 CU = 10⁶ µCU. All arithmetic is saturating;
intermediate products are computed in `u128` and saturated to `u64` on the
way out. Nothing in cost, usage, weights, or decay policy is ever a float,
and none of it can panic or wrap.

The canonical calibration (a documentation convention, not code — weights
are replicated policy): **1 CU ≈ 1 core-second**, with memory and disk
priced relative to it (reference weights: 4 GiB·s = 1 CU, 64 GiB·s of disk
= 1 CU).

**Overflow horizon.** `u64` holds ~1.8 × 10¹⁹ µCU ≈ 1.8 × 10¹³ CU.

- *Accumulated usage is bounded by decay, not by time.* An entity charged at
  a steady rate `R` CU/s converges to a stock of `R · T / ln 2` where `T` is
  the half-life. A 100 000-core cluster with memory and disk roughly
  tripling the CPU price (`R ≈ 3 × 10⁵` CU/s) at the default 24 h half-life
  converges to ~3.7 × 10¹⁶ µCU — about 500× headroom, **independent of how
  many years the cluster runs**. "1M live jobs" adds nothing beyond this:
  usage is charged at placement, so total charge rate is bounded by what the
  cluster can actually run.
- *Lump charges* are the sharper edge, because a placement charges
  `rate × max_runtime × multiplier` up front. A 10 000-core whale for 30
  days at 10× priority is ~7.8 × 10¹⁷ µCU — fits with ~20× headroom. A
  deliberately absurd configuration (100 000 cores × 30 days × 1000×
  priority) exceeds `u64` and **saturates**. Saturation is the designed
  safety valve: the entity pins at maximal usage (maximal penalty, which is
  the right degraded behaviour for someone charging a preposterous cost) and
  nothing panics, wraps, or diverges. Hitting it in practice means the
  deployment's weights are miscalibrated by orders of magnitude.

µCU rather than whole CU keeps `usage/quota` ratios meaningful for small
entities (a personal quota of a few core-hours still has ~10¹⁰ resolution);
µCU rather than nano keeps the horizon comfortable. `u128` state was
rejected: it doubles snapshot and proto width for headroom nobody needs
under decay.

### Job cost in fixed point

Replicated policy carries:

- **Cost weights**, one per resource dimension, as **Q32.32** fixed-point
  µCU per (resource unit × second). Q32.32 spans prices from
  ~2.3 × 10⁻¹⁰ µCU (cheap per-byte rates) to ~4 × 10⁹ µCU (expensive
  future dimensions — GPUs, licenses) per unit-second. New dimensions are
  new weight entries, no format change.
- **Priority multipliers**, a policy table mapping the user-facing
  `priority: i32` to a **Q32.32** multiplier. The state machine never sees
  the raw `i32` in arithmetic — only the resolved multiplier, carried in the
  committed command.

```
rate  (µCU/s) = Σ_dims floor(quantity × weight / 2³²)          — u128, saturating
cost  (µCU)   = floor(rate × runtime_s × multiplier / 2³²)     — u128, saturating
```

`runtime_s` is whole seconds, **rounded up** (`ceil`) from the declared
`max_runtime` at charge time and from observed runtime at true-up — the same
rounding on both sides so a job that runs exactly its declared runtime trues
up to exactly zero.

### Deterministic decay: an iterated per-tick step

This is the load-bearing decision. Requirements: pure integer math, and the
**composition invariant**

```
decay(decay(u, Δt₁), Δt₂) == decay(u, Δt₁ + Δt₂)      exactly, for all inputs
```

because lazy decay means the number of hops usage takes between two points
in time depends on which commands happened to touch the entity in between,
and bracketing must not change the result.

Any scheme that computes a rounded decay *factor* from Δt and multiplies —
including the obvious "fixed-point exp2 by table + interpolation" — fails
this invariant: `round(round(u·f(t₁))·f(t₂)) ≠ round(u·f(t₁+t₂))` in
general. Instead of fighting rounding, we choose the one structure for which
composition is a theorem rather than a test target: **function iteration**.

- Time is quantized into **ticks** on absolute boundaries:
  `tick_index(ts) = ts_us.div_euclid(tick_us)`. Euclidean (floor) division
  is defined for all `i64` timestamps including pre-epoch.
- Replicated decay policy is two integers:
  - `tick_us: i64 > 0` — the tick length (default **60 s** =
    60 000 000 µs);
  - `decay_per_tick: u64` — the per-tick retention factor λ as **Q0.64**
    (value λ = `decay_per_tick` / 2⁶⁴, always < 1).
- The per-tick step, with **truncation (floor) rounding**:

  ```
  step(u) = (u × decay_per_tick) >> 64        — u128 product, u64 result
  ```

- Decay over `n` ticks is **step applied n times**, short-circuiting when
  the value reaches 0. `decay(u, n₁ + n₂) = stepⁿ²(stepⁿ¹(u))` holds by
  definition — iteration is a semigroup action of (ℕ, +). Absolute tick
  indices make the timestamp-level splits sum exactly too:
  `(i(b) − i(a)) + (i(c) − i(b)) = i(c) − i(a)`.

**No exp2 in the state machine at all.** The only irrational computation —
deriving λ from a human half-life, λ = 2^(−tick/half-life) — happens in
config tooling (API/CLI) with high-precision arithmetic at
policy-authoring time, rounded half-even to Q0.64. The state machine only
ever replicates and multiplies integers; determinism needs replicas to agree
on λ's 64 bits, which replication itself guarantees. The default policy
(24 h half-life, 60 s tick, 1440 ticks per half-life) is the constant

```
decay_per_tick = 18 437 866 829 417 916 986      (λ ≈ 0.9995187636…,  λ¹⁴⁴⁰ = 0.5 − 1.6×10⁻¹⁷)
```

**Cost of iteration.** Floor-multiplication sheds at least
−log₂ λ bits per tick, so any value reaches 0 within
~64/(−log₂ λ) ticks and the short-circuit bounds the loop regardless of how
long an entity sat untouched. At the default policy that is ≤ 92 160
iterations (sub-millisecond, and only for an entity untouched for > 64
half-lives); typical touch gaps are minutes, i.e. a handful of iterations.
Policy validation enforces `decay_per_tick ≤ 2⁶⁴ − 2⁴⁸` (λ ≤ 1 − 2⁻¹⁶,
worst case ~2.9 M iterations) so no legal policy can turn a touch into
unbounded work, and `tick_us > 0`.

Rejected alternatives: *exp2 table + interpolation* (breaks composition, as
above); *per-half-life right-shifts* (floor division by 2ⁿ composes exactly
but quantizes time to whole half-lives — 24 h granularity is uselessly
coarse); *storing usage as (mantissa, epoch) and decaying on read* (exactly
compositional but needs periodic replicated rebasing to contain exponent
growth — more moving parts for no better semantics).

### Underflow

`step(u)` truncates, so any `u` whose product drops below 2⁶⁴ becomes 0 —
in particular 1 µCU decays to 0 in one tick for every legal λ, and 0 is
absorbing. Usage below one µCU is meaningless; it does not linger.

### Clock skew

Command timestamps are stamped by the proposing leader; across leader
changes they can regress. The rule, applied identically on every replica:

```
Δn                  = max(0, tick_index(cmd_ts) − tick_index(last_update_ts))
last_update_ts'     = max(last_update_ts, cmd_ts)
```

A regressed timestamp decays nothing and never moves `last_update_ts`
backwards, so usage can never *inflate* by replaying a decay interval twice.
The clamp preserves composition: an out-of-order middle touch quantizes to a
tick index ≤ the stored one, contributes Δn = 0, and leaves the stored
timestamp where the direct path would have read it.

### Charging and true-up

At **placement** (`CommitPlacements` apply, ADR 0013): every ancestor entity
is touched (decayed to the command's tick), then charged the job's full cost
`C` with saturating add. The attempt records `(C, charge_ts)` in replicated
state for later true-up.

At **attempt resolution** (the `Finalizing` funnel of ADR 0013): compute the
attempt's actual cost `A = rate × ceil(actual_runtime_s) × multiplier` using
the same weights and multiplier the charge used (carried with the charge
record; policy edits mid-flight do not retroactively reprice). An attempt
that never reached `Running` has `A = 0`.

- **A ≤ C (the normal case):** refund
  `R = decay(C − A, charge_ts → resolution_ts)` — the *unused* portion of
  the charge, **decayed as if it had been sitting in the entity's usage
  since placement**, which it has. Each ancestor is touched, then
  `usage = usage.saturating_sub(R)`. Decaying the refund is what makes
  true-up honest: the entity lands (within per-tick rounding noise, which is
  identical on every replica) exactly where it would have been had `A` been
  charged at placement. An undecayed refund would systematically over-refund
  long jobs; `saturating_sub` guarantees even accumulated rounding can never
  underflow.
- **A > C:** possible only via the post-`max_runtime` kill grace. The excess
  `A − C` is charged fresh at resolution time, no decay.

Consequences of the uniform rule — deliberately, there are **no special
cases**:

- **`Revoked`** is legal only while an allocation is accruing (ADR 0013), so
  the attempt never ran, `A = 0`, and the full charge comes back (decayed).
  ADR 0013's "requeue must be free" falls out of the arithmetic instead of
  being a carve-out.
- **Retry** charges each placement anew and trues up each attempt
  separately. A retried job pays for every attempt's actual consumption —
  platform-fault retries are cheap because the failed attempt's runtime was
  short, not because of an exemption.
- **Abort** trues up like any other end: you pay for what ran before the
  abort landed.

### Penalty: quadratic, float, derived only

`penalty` lives in derived scoring (recomputed by the scheduler, never
serialized into commands, state, or snapshots), so it may use `f64`:

```
x          = usage_µCU / quota_µCU          (f64 division)
penalty(x) = 1              for x ≤ 1
           = x^p            for x > 1,   p = penalty_exponent_milli / 1000
```

`penalty_exponent_milli: u32` is replicated policy (an integer, honouring
the no-floats-in-state rule), **default 2000 (p = 2, quadratic)**.

Shape rationale: it must be superlinear — with a linear penalty, an entity
at n× quota holding jobs at n× priority-multiplier scores even with everyone
else, i.e. sustained overuse can be linearly bought; quadratic makes that
race unwinnable while staying graceful. It must *not* be exponential —
penalties like 2^x explode past human intuition and flatten the ordering
among several over-quota entities into float noise. Quadratic keeps the
explainability contract crisp: **"your team is at 3.1× its soft quota, so
its jobs are deprioritized 9.6×"** — one multiplication a human can check.
Continuity at x = 1 (1^p = 1) means no cliff at the quota boundary.

The **soft quota** itself is replicated as a *stock* in µCU — the decayed
usage level that counts as "at quota". Humans think in rates ("100
core-hours per day"), so config tooling converts:
`quota_stock = rate × half_life / ln 2` (the fixed point of charging at
`rate` forever). The conversion, like λ, is tooling-side; the state machine
sees only the integer. Tooling must rescale stocks when the half-life
changes.

## Consequences

- The proto schema can freeze: cost/usage/quota are `uint64` (µCU), weights
  and multipliers `uint64` (Q32.32), decay policy `int64 tick_us` +
  `uint64 decay_per_tick`, penalty exponent `uint32` (milli). Timestamps
  stay `int64` Unix µs. **No float field exists anywhere in replicated
  data**, and cross-architecture replicas are bit-identical by construction.
- `coppice_core::quota` is the single arithmetic implementation; the apply
  loop (charging, true-up) and the scheduler (scoring) both call it.
  Divergence between "what was charged" and "what the scheduler thinks was
  charged" is structurally impossible.
- Composition-exactness rests on decay being *literally* the iterated step.
  Any future "fast path" (e.g. exponentiation by squaring) must be proven
  equal to iteration for all inputs — the property tests in `coppice-core`
  exist to catch exactly that refactor.
- Per-tick iteration makes very long untouched gaps cost up to ~10⁵
  multiplications at the default policy. Accepted: it is bounded, rare, and
  buys exactness; entities hot enough to matter are touched constantly.
- Decay resolution is one tick (60 s default): touches within the same tick
  see zero decay, and "half-life 24 h" is honoured at tick granularity. For
  fairness over a 24 h horizon this is far below observable.
- Changing `tick_us`/`decay_per_tick` at runtime is deterministic (policy
  changes are committed commands, ordered in the log) but re-times decay
  from each entity's next touch and changes what quota stocks mean; the
  tooling that writes the policy owns the rescale.
- The scheduler's float scoring can differ in the last ulp across replicas.
  That remains tolerable for exactly the reason ADR 0005 gave: scores order
  a *proposal*, and the placement is validated against replicated integer
  state at commit.
