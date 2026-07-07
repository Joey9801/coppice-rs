//! Deterministic fixed-point quota arithmetic.
//!
//! This module is the single implementation of the quota model's arithmetic
//! (ADR 0005), with representation and algorithms decided in
//! `docs/decisions/0019-deterministic-quota-arithmetic.md`. It is called from
//! two places with very different requirements:
//!
//! - the Raft apply loop (charging and true-up), where every operation must be
//!   bit-deterministic across replicas, platforms, and binary versions; and
//! - the scheduler (penalty and effective-score computation), which is derived
//!   state where `f64` is tolerable.
//!
//! Everything that touches replicated state — [`CostUnits`], [`DecayPolicy`],
//! [`UsageState`], [`CostWeights`], [`PriorityMultiplier`], charging and
//! true-up — is pure integer arithmetic: saturating, `u128` intermediates,
//! truncation (floor) rounding, no floats anywhere. Only [`penalty`] and
//! [`over_quota_ratio`] use `f64`, and their results must never be written
//! back into commands, state, or snapshots.
//!
//! The load-bearing invariant is **exact decay composition**:
//! `decay(decay(u, n1), n2) == decay(u, n1 + n2)` for all inputs, because
//! usage decays lazily and the number of hops between two points in time
//! depends on which commands happened to touch an entity. Composition holds
//! here *by construction* — decay is literally the n-fold iteration of a
//! single per-tick step — and the property tests in
//! `tests/quota_properties.rs` guard that any future fast path preserves it.

use crate::resource::Resources;

/// Micro-cost-units per cost unit: [`CostUnits`] counts µCU.
pub const MICRO_PER_COST_UNIT: u64 = 1_000_000;

/// A quantity of cost or accumulated usage, in micro-cost-units (µCU).
///
/// All arithmetic is saturating; overflow pins at `u64::MAX` (maximal usage,
/// hence maximal penalty) rather than wrapping or panicking. See ADR 0019 for
/// the overflow-horizon analysis behind the µCU scale.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub struct CostUnits(pub u64);

impl CostUnits {
    pub const ZERO: CostUnits = CostUnits(0);
    pub const MAX: CostUnits = CostUnits(u64::MAX);

    pub fn saturating_add(self, other: CostUnits) -> CostUnits {
        CostUnits(self.0.saturating_add(other.0))
    }

    pub fn saturating_sub(self, other: CostUnits) -> CostUnits {
        CostUnits(self.0.saturating_sub(other.0))
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
}

/// Errors from validating replicated quota policy.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PolicyError {
    #[error("tick_us must be positive, got {0}")]
    NonPositiveTick(i64),
    #[error("decay_per_tick {0} exceeds maximum {max} (λ too close to 1)", max = DecayPolicy::MAX_DECAY_PER_TICK)]
    DecayTooSlow(u64),
}

/// Replicated decay policy: tick length and per-tick retention factor.
///
/// The per-tick factor λ is Q0.64 fixed point (`decay_per_tick / 2^64`). It
/// is derived from the human-facing half-life by config *tooling* at
/// policy-authoring time — `decay_per_tick = round(2^64 · 2^(-tick/half_life))`
/// — so no transcendental function ever runs in the state machine; replicas
/// only need to agree on these two integers, which replication guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecayPolicy {
    /// Tick length in microseconds. Must be positive.
    pub tick_us: i64,
    /// Per-tick retention factor λ as Q0.64: the fraction of usage kept per
    /// tick is `decay_per_tick / 2^64`. Must not exceed
    /// [`DecayPolicy::MAX_DECAY_PER_TICK`].
    pub decay_per_tick: u64,
}

impl DecayPolicy {
    /// Upper bound on `decay_per_tick`: λ ≤ 1 − 2⁻¹⁶, which bounds the
    /// iterations needed to decay any value to zero (≈ 64/(−log₂ λ), worst
    /// case ~2.9M) so no legal policy turns a lazy-decay touch into unbounded
    /// work in the apply loop.
    pub const MAX_DECAY_PER_TICK: u64 = u64::MAX - ((1 << 48) - 1); // 2^64 - 2^48

    /// The default policy: 60 s ticks, 24 h half-life (1440 ticks), i.e.
    /// `decay_per_tick = round(2^64 · 2^(-1/1440))`. λ¹⁴⁴⁰ = 0.5 − 1.6×10⁻¹⁷.
    pub const DEFAULT: DecayPolicy = DecayPolicy {
        tick_us: 60_000_000,
        decay_per_tick: 18_437_866_829_417_916_986,
    };

    pub fn validate(&self) -> Result<(), PolicyError> {
        if self.tick_us <= 0 {
            return Err(PolicyError::NonPositiveTick(self.tick_us));
        }
        if self.decay_per_tick > Self::MAX_DECAY_PER_TICK {
            return Err(PolicyError::DecayTooSlow(self.decay_per_tick));
        }
        Ok(())
    }

    /// The absolute tick index containing a timestamp (Unix µs). Euclidean
    /// division floors toward −∞, so it is well-defined for pre-epoch times.
    /// Absolute indices (rather than relative Δt) make timestamp-level decay
    /// splits sum exactly: `(i(b) − i(a)) + (i(c) − i(b)) = i(c) − i(a)`.
    pub fn tick_index(&self, ts_us: i64) -> i64 {
        ts_us.div_euclid(self.tick_us)
    }

    /// Whole ticks elapsed from `from_us` to `to_us`, clamped at zero.
    ///
    /// The clamp is the clock-skew rule (ADR 0019): command timestamps come
    /// from different leaders and may regress; a regressed timestamp decays
    /// nothing rather than time-travelling.
    pub fn elapsed_ticks(&self, from_us: i64, to_us: i64) -> u64 {
        // Indices span at most the i64 range / tick_us, so the difference of
        // two indices cannot overflow i64's width in practice; saturate to be
        // airtight at the extremes.
        let dn = self.tick_index(to_us).saturating_sub(self.tick_index(from_us));
        dn.max(0) as u64
    }

    /// Decay `usage` by `ticks` ticks: the `ticks`-fold iteration of the
    /// per-tick step `u ← ⌊u · λ⌋` (truncation rounding).
    ///
    /// This must remain *literally* the iterated step. Iteration is a
    /// semigroup action of (ℕ, +), which is what makes the composition
    /// invariant exact; a closed-form or squaring fast path with per-call
    /// rounding would break it. Work is bounded: the loop short-circuits at
    /// zero, which floor-multiplication reaches within ~64/(−log₂ λ) ticks.
    pub fn decay_ticks(&self, usage: CostUnits, ticks: u64) -> CostUnits {
        let lambda = self.decay_per_tick as u128;
        let mut u = usage.0;
        for _ in 0..ticks {
            if u == 0 {
                break;
            }
            u = ((u as u128 * lambda) >> 64) as u64;
        }
        CostUnits(u)
    }

    /// Decay `usage` across the whole ticks elapsed between two timestamps.
    pub fn decay_between(&self, usage: CostUnits, from_us: i64, to_us: i64) -> CostUnits {
        self.decay_ticks(usage, self.elapsed_ticks(from_us, to_us))
    }
}

/// A quota entity's replicated usage accumulator: the
/// `(accumulated_usage, last_update_timestamp)` pair of ADR 0005.
///
/// Timestamps are Unix microseconds carried in committed commands (never wall
/// clock read during apply). Every mutation first brings the accumulator
/// forward to the command's tick ([`UsageState::touch`]), so decay is lazy
/// but exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageState {
    pub usage: CostUnits,
    pub last_update_us: i64,
}

impl UsageState {
    /// A fresh accumulator, zero usage as of `created_us`.
    pub fn new(created_us: i64) -> UsageState {
        UsageState { usage: CostUnits::ZERO, last_update_us: created_us }
    }

    /// Bring the accumulator forward to a command timestamp.
    ///
    /// A timestamp at or before the stored one (clock skew across leader
    /// changes, or the same tick) decays nothing and never moves
    /// `last_update_us` backwards, so a decay interval can never be applied
    /// twice.
    pub fn touch(&mut self, ts_us: i64, policy: &DecayPolicy) {
        self.usage = policy.decay_between(self.usage, self.last_update_us, ts_us);
        self.last_update_us = self.last_update_us.max(ts_us);
    }

    /// Decay to the command's tick, then add a charge (saturating).
    pub fn charge(&mut self, amount: CostUnits, ts_us: i64, policy: &DecayPolicy) {
        self.touch(ts_us, policy);
        self.usage = self.usage.saturating_add(amount);
    }

    /// Decay to the command's tick, then subtract a refund (saturating, so
    /// accumulated rounding in a decayed refund can never underflow).
    pub fn refund(&mut self, amount: CostUnits, ts_us: i64, policy: &DecayPolicy) {
        self.touch(ts_us, policy);
        self.usage = self.usage.saturating_sub(amount);
    }

    /// Apply a true-up adjustment from [`true_up`].
    pub fn settle(&mut self, adjustment: TrueUp, ts_us: i64, policy: &DecayPolicy) {
        match adjustment {
            TrueUp::Refund(amount) => self.refund(amount, ts_us, policy),
            TrueUp::Surcharge(amount) => self.charge(amount, ts_us, policy),
        }
    }
}

/// Replicated cost weights: Q32.32 fixed-point µCU per (resource unit ×
/// second), one per dimension of [`Resources`]. New resource dimensions
/// (GPUs, licenses) are priced by adding weight fields, not by changing the
/// representation. Q32.32 spans ~2.3×10⁻¹⁰ µCU (per-byte rates) to ~4×10⁹
/// µCU per unit-second.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CostWeights {
    /// µCU per (milli-CPU × second), Q32.32.
    pub per_cpu_milli_second: u64,
    /// µCU per (byte of memory × second), Q32.32.
    pub per_memory_byte_second: u64,
    /// µCU per (byte of disk × second), Q32.32.
    pub per_disk_byte_second: u64,
}

/// Replicated priority multiplier, Q32.32. The user-facing `priority: i32` is
/// mapped to a multiplier by a policy table outside this module; arithmetic
/// only ever sees the resolved fixed-point value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PriorityMultiplier(pub u64);

impl PriorityMultiplier {
    /// Multiplier of exactly 1×.
    pub const ONE: PriorityMultiplier = PriorityMultiplier(1 << 32);

    /// An exact integer multiple (e.g. `from_integer(3)` = 3×).
    pub fn from_integer(n: u32) -> PriorityMultiplier {
        PriorityMultiplier((n as u64) << 32)
    }
}

/// The cost *rate* of a resource request, in µCU per second: the weighted sum
/// over dimensions, each term `⌊quantity · weight / 2³²⌋`, saturating.
pub fn resource_rate(requests: &Resources, weights: &CostWeights) -> u64 {
    let term = |quantity: u64, weight: u64| (quantity as u128 * weight as u128) >> 32;
    let rate = term(requests.cpu_millis, weights.per_cpu_milli_second)
        + term(requests.memory_bytes, weights.per_memory_byte_second)
        + term(requests.disk_bytes, weights.per_disk_byte_second);
    u64::try_from(rate).unwrap_or(u64::MAX)
}

/// Round a runtime in microseconds up to whole seconds — the charge-side and
/// true-up-side rounding, chosen identically so a job that runs exactly its
/// declared runtime trues up to exactly zero.
pub fn runtime_seconds_ceil(runtime_us: u64) -> u64 {
    runtime_us.div_ceil(MICRO_PER_COST_UNIT)
}

/// Cost from an already-computed rate:
/// `cost = rate × runtime_seconds × priority_multiplier`, computed in `u128`
/// with a single `>> 32` for the Q32.32 multiplier, saturating. True-up uses
/// this with the rate stored in the [`ChargeRecord`]'s attempt, so a policy
/// edit mid-flight never reprices an in-flight charge (ADR 0019).
pub fn cost_from_rate(
    rate_ucu_per_second: u64,
    runtime_seconds: u64,
    multiplier: PriorityMultiplier,
) -> CostUnits {
    let scaled = (rate_ucu_per_second as u128)
        .saturating_mul(runtime_seconds as u128)
        .saturating_mul(multiplier.0 as u128);
    CostUnits(u64::try_from(scaled >> 32).unwrap_or(u64::MAX))
}

/// A job's scalar cost (ADR 0005):
/// `cost = resource_rate × runtime_seconds × priority_multiplier`.
pub fn job_cost(
    requests: &Resources,
    weights: &CostWeights,
    runtime_seconds: u64,
    multiplier: PriorityMultiplier,
) -> CostUnits {
    cost_from_rate(resource_rate(requests, weights), runtime_seconds, multiplier)
}

/// The replicated record of a placement charge, kept on the attempt so its
/// terminal resolution can true up against what was actually consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChargeRecord {
    /// The full job cost charged to every ancestor at placement.
    pub amount: CostUnits,
    /// Timestamp of the placement command that charged it (Unix µs).
    pub charged_at_us: i64,
}

/// A true-up adjustment, applied to every ancestor via [`UsageState::settle`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrueUp {
    /// The unused portion of the charge, already decayed from charge time to
    /// resolution time (it has been sitting in the accumulator that long).
    Refund(CostUnits),
    /// Consumption beyond the charge (post-`max_runtime` kill grace), charged
    /// fresh at resolution time.
    Surcharge(CostUnits),
}

/// True up a placement charge against the attempt's actual cost at terminal
/// resolution (ADR 0013's `Finalizing` funnel).
///
/// `actual_cost` uses the same weights, multiplier, and ceil-seconds rounding
/// as the charge; an attempt that never reached `Running` — including
/// `Revoked`, which is only legal while accruing — has actual cost zero and
/// gets the full (decayed) charge back, which is what makes revocation
/// requeue free without a special case.
pub fn true_up(
    charge: &ChargeRecord,
    actual_cost: CostUnits,
    resolved_at_us: i64,
    policy: &DecayPolicy,
) -> TrueUp {
    if actual_cost <= charge.amount {
        let unused = charge.amount.saturating_sub(actual_cost);
        TrueUp::Refund(policy.decay_between(unused, charge.charged_at_us, resolved_at_us))
    } else {
        TrueUp::Surcharge(actual_cost.saturating_sub(charge.amount))
    }
}

/// Default penalty exponent: 2000 milli = quadratic (ADR 0019).
pub const DEFAULT_PENALTY_EXPONENT_MILLI: u32 = 2000;

/// How far over its soft quota an entity is, as a ratio (derived state — the
/// one place floats are allowed; never serialize the result).
///
/// The quota is a *stock* in µCU (the decayed-usage level that counts as "at
/// quota"; config tooling converts human rates via `rate × half_life / ln 2`).
/// A zero quota with nonzero usage is infinitely over; zero over zero is not
/// over at all.
pub fn over_quota_ratio(usage: CostUnits, quota: CostUnits) -> f64 {
    if quota.is_zero() {
        if usage.is_zero() {
            0.0
        } else {
            f64::INFINITY
        }
    } else {
        usage.0 as f64 / quota.0 as f64
    }
}

/// The penalty an over-quota entity contributes to its descendants' effective
/// scores: 1 within quota, `x^p` above it, with `p = exponent_milli / 1000`
/// (default quadratic — superlinear so sustained overuse can't be linearly
/// bought with priority multipliers, polynomial so "at 3× quota ⇒ 9×
/// deprioritized" stays human-checkable). Continuous and monotone at x = 1.
pub fn penalty(over_quota_ratio: f64, exponent_milli: u32) -> f64 {
    if over_quota_ratio <= 1.0 {
        1.0
    } else {
        over_quota_ratio.powf(exponent_milli as f64 / 1000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The documented reference calibration (ADR 0019): 1 core-second = 1 CU,
    /// 4 GiB·s of memory = 1 CU, 64 GiB·s of disk = 1 CU. Test/docs fixture,
    /// not a shipped default — weights are replicated policy.
    const REFERENCE_WEIGHTS: CostWeights = CostWeights {
        per_cpu_milli_second: 1000 << 32,
        per_memory_byte_second: 1_000_000,
        per_disk_byte_second: 62_500,
    };

    #[test]
    fn default_policy_validates() {
        DecayPolicy::DEFAULT.validate().unwrap();
    }

    #[test]
    fn policy_validation_rejects_bad_configs() {
        let bad_tick = DecayPolicy { tick_us: 0, ..DecayPolicy::DEFAULT };
        assert_eq!(bad_tick.validate(), Err(PolicyError::NonPositiveTick(0)));
        let too_slow =
            DecayPolicy { decay_per_tick: DecayPolicy::MAX_DECAY_PER_TICK + 1, ..DecayPolicy::DEFAULT };
        assert_eq!(
            too_slow.validate(),
            Err(PolicyError::DecayTooSlow(DecayPolicy::MAX_DECAY_PER_TICK + 1))
        );
    }

    /// Golden decay vectors, frozen from the algorithm's exact output. These
    /// are the cross-platform / cross-version regression net: any drift here
    /// means replicas built from different binaries would diverge.
    #[test]
    fn golden_decay_vectors() {
        let p = DecayPolicy::DEFAULT;
        let cases: &[(u64, u64, u64)] = &[
            (1_000_000_000_000, 0, 1_000_000_000_000),
            (1_000_000_000_000, 1, 999_518_763_622),
            // One half-life: exact halving to within the ~1/(1−λ) ≈ 2078 µCU
            // floor-bias bound.
            (1_000_000_000_000, 1440, 499_999_999_486),
            (1_000_000_000_000, 2880, 249_999_999_234),
            (1_000_000_000_000, 10_000, 8_119_211_669),
            (u64::MAX, 1, 18_437_866_829_417_916_985),
            (u64::MAX, 1440, 9_223_372_036_854_775_001),
            (1, 1, 0),
            (2078, 1440, 637),
            (1_000_000, 20_000, 0),
        ];
        for &(u, n, expected) in cases {
            assert_eq!(
                p.decay_ticks(CostUnits(u), n),
                CostUnits(expected),
                "decay({u}, {n})"
            );
        }
    }

    #[test]
    fn golden_cost_and_true_up() {
        // 4 cores + 16 GiB + 128 GiB disk at reference weights: rate is
        // exactly 4 + 4 + 2 = 10 CU/s.
        let requests = Resources {
            cpu_millis: 4000,
            memory_bytes: 16 << 30,
            disk_bytes: 128 << 30,
        };
        assert_eq!(resource_rate(&requests, &REFERENCE_WEIGHTS), 10_000_000);

        // One hour at 3× priority: 10 CU/s × 3600 s × 3 = 108 000 CU.
        let charged = job_cost(
            &requests,
            &REFERENCE_WEIGHTS,
            3600,
            PriorityMultiplier::from_integer(3),
        );
        assert_eq!(charged, CostUnits(108_000_000_000));

        // Ran 900 s of the declared 3600, resolved one half-life later: the
        // unused 3/4 of the charge comes back halved (decayed golden value).
        let actual = job_cost(
            &requests,
            &REFERENCE_WEIGHTS,
            900,
            PriorityMultiplier::from_integer(3),
        );
        assert_eq!(actual, CostUnits(27_000_000_000));
        let charged_at = 1_760_000_000_000_000;
        let record = ChargeRecord { amount: charged, charged_at_us: charged_at };
        let adjustment = true_up(
            &record,
            actual,
            charged_at + 86_400_000_000,
            &DecayPolicy::DEFAULT,
        );
        assert_eq!(adjustment, TrueUp::Refund(CostUnits(40_499_999_487)));
    }

    #[test]
    fn true_up_surcharges_grace_overrun() {
        let record = ChargeRecord { amount: CostUnits(1000), charged_at_us: 0 };
        let adjustment = true_up(&record, CostUnits(1010), 60_000_000, &DecayPolicy::DEFAULT);
        assert_eq!(adjustment, TrueUp::Surcharge(CostUnits(10)));
    }

    #[test]
    fn revoked_attempt_refunds_full_decayed_charge() {
        // Actual cost zero (never ran) ⇒ the whole charge comes back, decayed
        // by however long it sat — requeue is free with no special case.
        let p = DecayPolicy::DEFAULT;
        let record = ChargeRecord { amount: CostUnits(1_000_000_000_000), charged_at_us: 0 };
        let resolved_at = 86_400_000_000; // one half-life
        assert_eq!(
            true_up(&record, CostUnits::ZERO, resolved_at, &p),
            TrueUp::Refund(p.decay_between(record.amount, 0, resolved_at))
        );
    }

    #[test]
    fn skewed_timestamps_never_inflate_or_rewind() {
        let p = DecayPolicy::DEFAULT;
        let mut state = UsageState::new(1_000_000_000_000);
        state.charge(CostUnits(500_000), 1_000_000_000_000, &p);
        let before = state;
        // A command stamped by a laggy new leader, hours in the past.
        state.touch(1_000_000_000_000 - 7_200_000_000, &p);
        assert_eq!(state, before, "regressed timestamp must be a no-op");
    }

    #[test]
    fn charge_and_refund_saturate() {
        let p = DecayPolicy::DEFAULT;
        let ts = 1_000_000_000_000;
        let mut state = UsageState::new(ts);
        state.charge(CostUnits::MAX, ts, &p);
        state.charge(CostUnits::MAX, ts, &p);
        assert_eq!(state.usage, CostUnits::MAX);
        state.refund(CostUnits::MAX, ts, &p);
        state.refund(CostUnits(1), ts, &p);
        assert_eq!(state.usage, CostUnits::ZERO);
    }

    #[test]
    fn runtime_rounds_up_to_whole_seconds() {
        assert_eq!(runtime_seconds_ceil(0), 0);
        assert_eq!(runtime_seconds_ceil(1), 1);
        assert_eq!(runtime_seconds_ceil(1_000_000), 1);
        assert_eq!(runtime_seconds_ceil(1_000_001), 2);
    }

    #[test]
    fn penalty_shape() {
        assert_eq!(penalty(0.0, DEFAULT_PENALTY_EXPONENT_MILLI), 1.0);
        assert_eq!(penalty(1.0, DEFAULT_PENALTY_EXPONENT_MILLI), 1.0);
        assert_eq!(penalty(3.0, DEFAULT_PENALTY_EXPONENT_MILLI), 9.0);
        assert_eq!(penalty(f64::INFINITY, DEFAULT_PENALTY_EXPONENT_MILLI), f64::INFINITY);
        // Zero quota: infinitely over unless also unused.
        assert_eq!(over_quota_ratio(CostUnits::ZERO, CostUnits::ZERO), 0.0);
        assert_eq!(over_quota_ratio(CostUnits(1), CostUnits::ZERO), f64::INFINITY);
    }

    #[test]
    fn tick_index_is_floor_for_negative_times() {
        let p = DecayPolicy::DEFAULT;
        assert_eq!(p.tick_index(0), 0);
        assert_eq!(p.tick_index(-1), -1);
        assert_eq!(p.tick_index(-60_000_000), -1);
        assert_eq!(p.tick_index(-60_000_001), -2);
    }
}
