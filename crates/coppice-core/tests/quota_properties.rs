//! Property tests for the deterministic quota arithmetic (ADR 0019).
//!
//! The exact composition invariant is the load-bearing one: lazy decay means
//! replicated usage is brought forward in however many hops the command
//! stream happens to produce, and bracketing must never change the result.
//! It holds by construction today (decay is literally the iterated per-tick
//! step); these tests exist to catch any future "fast path" that breaks it.

use coppice_core::bytes::ByteSize;
use coppice_core::quota::{
    job_cost, penalty, true_up, ChargeRecord, CostUnits, CostWeights, DecayPolicy,
    PriorityMultiplier, TrueUp, UsageState, FULL_REFUND_MILLI,
};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use proptest::prelude::*;

/// Any policy that passes validation.
fn valid_policy() -> impl Strategy<Value = DecayPolicy> {
    (1i64..=3_600_000_000, 0u64..=DecayPolicy::MAX_DECAY_PER_TICK).prop_map(
        |(tick_us, decay_per_tick)| DecayPolicy {
            tick: Duration::from_micros(tick_us),
            decay_per_tick,
        },
    )
}

/// Timestamps within a realistic window (± a few months around 2026) so
/// elapsed tick counts stay in the tens of thousands.
const TS_BASE: i64 = 1_760_000_000_000_000;
const TS_SPAN: i64 = 1_000_000_000_000; // ~11.6 days

/// Every instant these properties generate is inside `TS_BASE ± TS_SPAN`, far
/// inside the representable range, so the range check cannot fire.
fn ts(micros: i64) -> Timestamp {
    Timestamp::from_micros(micros).expect("test timestamps are in range")
}

/// The pre-ADR 0029 true-up, reimplemented independently: the entire unused
/// charge, decayed, comes back. The retention arithmetic must reduce to this
/// bit-for-bit whenever it does not retain (ADR 0029 I2/I3).
fn reference_full_refund(
    charge: &ChargeRecord,
    actual_cost: CostUnits,
    resolved_at: Timestamp,
    policy: &DecayPolicy,
) -> CostUnits {
    let unused = charge.amount.saturating_sub(actual_cost);
    policy.decay_between(unused, charge.charged_at, resolved_at)
}

/// The cost that settles into usage for a retaining true-up, evaluated with no
/// decay (resolution at charge time) so the retained amount is exact: the
/// charge minus the refund on the `A ≤ C` path, or the actual on the surcharge
/// path. Used by the monotonicity properties (ADR 0029 I1/I4).
fn settled_undecayed(amount: u64, actual: u64, refund_fraction_milli: u32) -> u64 {
    let record = ChargeRecord {
        amount: CostUnits(amount),
        charged_at: ts(0),
        refund_fraction_milli,
    };
    match true_up(
        &record,
        CostUnits(actual),
        ts(0),
        &DecayPolicy::DEFAULT,
        true,
    ) {
        TrueUp::Refund(r) => amount - r.0,
        TrueUp::Surcharge(s) => amount + s.0,
    }
}

proptest! {
    /// The composition invariant, at tick granularity, for arbitrary valid
    /// policies: decaying n1 then n2 ticks equals decaying n1 + n2 ticks,
    /// exactly.
    #[test]
    fn decay_composes_exactly_over_tick_splits(
        u in any::<u64>(),
        policy in valid_policy(),
        n1 in 0u64..=4096,
        n2 in 0u64..=4096,
    ) {
        let u = CostUnits(u);
        let split = policy.decay_ticks(policy.decay_ticks(u, n1), n2);
        let joined = policy.decay_ticks(u, n1 + n2);
        prop_assert_eq!(split, joined);
    }

    /// The composition invariant at timestamp granularity: an intermediate
    /// touch at any b with a ≤ b ≤ c is invisible. Absolute tick indices are
    /// what make the two elapsed-tick counts sum exactly.
    #[test]
    fn touch_composes_exactly_over_timestamp_splits(
        u in any::<u64>(),
        start in TS_BASE..TS_BASE + TS_SPAN,
        d1 in 0i64..TS_SPAN,
        d2 in 0i64..TS_SPAN,
    ) {
        let policy = DecayPolicy::DEFAULT;
        let (a, b, c) = (ts(start), ts(start + d1), ts(start + d1 + d2));

        let mut stepped = UsageState { usage: CostUnits(u), last_update: a };
        stepped.touch(b, &policy);
        stepped.touch(c, &policy);

        let mut direct = UsageState { usage: CostUnits(u), last_update: a };
        direct.touch(c, &policy);

        prop_assert_eq!(stepped, direct);

        // Same statement for the raw decay function.
        let split = policy.decay_between(policy.decay_between(CostUnits(u), a, b), b, c);
        prop_assert_eq!(split, policy.decay_between(CostUnits(u), a, c));
    }

    /// Clock-skew rule: a command timestamp at or before the stored one is a
    /// complete no-op — no decay, no rewind — so a decay interval can never
    /// be applied twice across leader changes.
    #[test]
    fn regressed_timestamps_are_no_ops(
        u in any::<u64>(),
        policy in valid_policy(),
        last in TS_BASE..TS_BASE + TS_SPAN,
        skew in 0i64..TS_SPAN,
    ) {
        let mut state = UsageState { usage: CostUnits(u), last_update: ts(last) };
        let before = state;
        state.touch(ts(last - skew), &policy);
        prop_assert_eq!(state, before);
    }

    /// Decay never increases usage, is monotone in elapsed ticks, and is
    /// monotone in the starting value.
    #[test]
    fn decay_is_monotone(
        u1 in any::<u64>(),
        u2 in any::<u64>(),
        policy in valid_policy(),
        n in 0u64..=4096,
        extra in 0u64..=4096,
    ) {
        let (lo, hi) = (CostUnits(u1.min(u2)), CostUnits(u1.max(u2)));
        prop_assert!(policy.decay_ticks(hi, n) <= hi);
        prop_assert!(policy.decay_ticks(hi, n + extra) <= policy.decay_ticks(hi, n));
        prop_assert!(policy.decay_ticks(lo, n) <= policy.decay_ticks(hi, n));
    }

    /// Saturating arithmetic end to end: no input to cost computation,
    /// charging, refunding, or true-up can panic or wrap, however extreme.
    #[test]
    fn extreme_inputs_never_panic(
        cpu in any::<u64>(),
        memory in any::<u64>().prop_map(ByteSize::from_bytes),
        disk in any::<u64>().prop_map(ByteSize::from_bytes),
        w_cpu in any::<u64>(),
        w_mem in any::<u64>(),
        w_disk in any::<u64>(),
        runtime_s in any::<u64>(),
        multiplier in any::<u64>(),
        policy in valid_policy(),
        charged_at_us in TS_BASE..TS_BASE + TS_SPAN,
        charge2 in any::<u64>(),
        refund_fraction in any::<u32>(),
        retain in any::<bool>(),
    ) {
        let requests = Resources { cpu_millis: cpu, memory, disk };
        let weights = CostWeights {
            per_cpu_milli_second: w_cpu,
            per_memory_byte_second: w_mem,
            per_disk_byte_second: w_disk,
        };
        let cost = job_cost(&requests, &weights, runtime_s, PriorityMultiplier(multiplier));

        let charged_at = ts(charged_at_us);
        let mut state = UsageState::new(charged_at - Duration::from_secs(1));
        state.charge(cost, charged_at, &policy);
        state.charge(CostUnits(charge2), charged_at, &policy);
        prop_assert!(state.usage <= CostUnits::MAX);

        // The fraction is deliberately unclamped: true-up must tolerate any
        // recorded value however extreme.
        let record = ChargeRecord {
            amount: cost,
            charged_at,
            refund_fraction_milli: refund_fraction,
        };
        let resolved_at = charged_at + Duration::from_micros(1);
        let adjustment = true_up(&record, CostUnits(charge2), resolved_at, &policy, retain);
        state.settle(adjustment, resolved_at, &policy);
        // Refunds saturate at zero: usage can never underflow.
        state.refund(CostUnits::MAX, charged_at + Duration::from_micros(2), &policy);
        prop_assert_eq!(state.usage, CostUnits::ZERO);
    }

    /// Penalty is 1 within quota, ≥ 1 always, and monotone in the ratio.
    #[test]
    fn penalty_is_monotone_and_anchored(
        x1 in 0.0f64..1e12,
        x2 in 0.0f64..1e12,
        exponent_milli in 1000u32..=4000,
    ) {
        let (lo, hi) = (x1.min(x2), x1.max(x2));
        if hi <= 1.0 {
            prop_assert_eq!(penalty(hi, exponent_milli), 1.0);
        }
        prop_assert!(penalty(lo, exponent_milli) >= 1.0);
        prop_assert!(penalty(lo, exponent_milli) <= penalty(hi, exponent_milli));
    }

    /// The fixed-point decay tracks the real exponential u·λⁿ. The f64
    /// version is the *reference documenting intent*; the fixed-point one is
    /// authoritative. Floor rounding loses < 1 µCU per tick and the loss
    /// itself decays, bounding the drift by min(n, 1/(1−λ)); on top of that
    /// we allow for f64's own rounding when computing the reference.
    #[test]
    fn fixed_point_decay_tracks_f64_reference(
        u in any::<u64>(),
        policy in valid_policy(),
        n in 0u64..=4096,
    ) {
        let lambda = policy.decay_per_tick as f64 / 2f64.powi(64);
        let reference = u as f64 * lambda.powi(n as i32);
        let fixed = policy.decay_ticks(CostUnits(u), n).0 as f64;

        // f64 slack: relative error of the reference product chain.
        let slack = reference * (n + 1) as f64 * 1e-15 + 2.0;
        let floor_drift = if lambda < 1.0 {
            (n as f64).min(1.0 / (1.0 - lambda))
        } else {
            n as f64
        };
        prop_assert!(fixed <= reference + slack,
            "fixed {fixed} exceeds reference {reference} beyond slack {slack}");
        prop_assert!(reference - fixed <= floor_drift + slack,
            "fixed {fixed} lags reference {reference} beyond drift {floor_drift} + slack {slack}");
    }

    /// (I2)/(I3) ADR 0029: retention reduces to the pre-change arithmetic
    /// bit-for-bit whenever it does not retain — a full refund of the unused
    /// charge. Two ways to not retain: `retain = false` (any recorded
    /// fraction, e.g. a platform outcome or an attempt that never ran, I3), or
    /// a recorded fraction of exactly 1000 (I2). Both must equal the
    /// independent reference on the whole `A ≤ C` refund path.
    #[test]
    fn not_retaining_reproduces_pre_adr0029_refund(
        amount in any::<u64>(),
        actual in any::<u64>(),
        fraction in any::<u32>(),
        policy in valid_policy(),
        charged_at_us in TS_BASE..TS_BASE + TS_SPAN,
        elapsed in 0i64..TS_SPAN,
    ) {
        // Constrain to the refund regime the reference covers (A ≤ C); the
        // surcharge path carries no fraction and is exercised elsewhere.
        let actual = CostUnits(actual.min(amount));
        let charged_at = ts(charged_at_us);
        let resolved = charged_at + Duration::from_micros(elapsed);
        let reference = reference_full_refund(
            &ChargeRecord { amount: CostUnits(amount), charged_at, refund_fraction_milli: FULL_REFUND_MILLI },
            actual,
            resolved,
            &policy,
        );

        // I3: retain = false ignores the recorded fraction entirely.
        let any_fraction = ChargeRecord { amount: CostUnits(amount), charged_at, refund_fraction_milli: fraction };
        prop_assert_eq!(
            true_up(&any_fraction, actual, resolved, &policy, false),
            TrueUp::Refund(reference)
        );
        // I2: a recorded fraction of 1000 is a full refund even when retaining.
        let full = ChargeRecord { amount: CostUnits(amount), charged_at, refund_fraction_milli: FULL_REFUND_MILLI };
        prop_assert_eq!(
            true_up(&full, actual, resolved, &policy, true),
            TrueUp::Refund(reference)
        );
    }

    /// (I1) ADR 0029: for a fixed actual cost and a retaining outcome, the
    /// settled cost is non-decreasing in the declared charge, and strictly
    /// increasing once the extra declaration retains at least one whole µCU
    /// (which needs `f < 1000`). Evaluated undecayed so the retained amount is
    /// exact.
    #[test]
    fn settled_cost_is_monotone_in_declared_charge(
        actual in any::<u64>(),
        gap1 in 0u64..=2_000_000_000_000,
        gap2 in 0u64..=2_000_000_000_000,
        f in 0u32..=FULL_REFUND_MILLI,
    ) {
        // Two declared charges, both ≥ the fixed actual (the refund regime).
        let c1 = actual.saturating_add(gap1);
        let c2 = c1.saturating_add(gap2);
        let s1 = settled_undecayed(c1, actual, f);
        let s2 = settled_undecayed(c2, actual, f);
        prop_assert!(s2 >= s1, "settled {s2} < {s1} for c2 {c2} ≥ c1 {c1}");
        // The retained slice of the added charge is (c2 − c1)(1000 − f)/1000;
        // once that reaches a whole µCU the settled cost must strictly rise.
        let retained_extra = (c2 as u128 - c1 as u128) * (FULL_REFUND_MILLI - f) as u128;
        if retained_extra >= 1000 {
            prop_assert!(s2 > s1, "settled {s2} not > {s1} despite retained extra");
        }
    }

    /// (I4) ADR 0029: aborting earlier never costs more. For a fixed declared
    /// charge and fraction, the settled cost is non-decreasing in the actual
    /// consumed cost, so a smaller actual (an earlier abort) settles no higher.
    #[test]
    fn aborting_earlier_never_costs_more(
        amount in any::<u64>(),
        a_hi in any::<u64>(),
        below in any::<u64>(),
        f in 0u32..=FULL_REFUND_MILLI,
    ) {
        // a_lo ≤ a_hi ≤ amount: both on the refund path.
        let a_hi = a_hi.min(amount);
        let a_lo = a_hi.saturating_sub(below);
        prop_assert!(
            settled_undecayed(amount, a_lo, f) <= settled_undecayed(amount, a_hi, f),
            "earlier abort (actual {a_lo}) settled above later (actual {a_hi})"
        );
    }
}
