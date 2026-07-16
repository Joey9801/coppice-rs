//! Property tests for the ADR 0021 effective-score formula
//! (`coppice_scheduler::score`). The formula lives isolated so these
//! properties pin its shape: monotone in priority, nondecreasing in age,
//! penalty divides only the priority term, the deliberate aging crossover
//! measured in half-lives, and bit-determinism (ADR 0019).

use coppice_core::id::QuotaEntityId;
use coppice_core::quota::{CostUnits, DecayPolicy, PriorityMultiplier, UsageState};
use coppice_core::time::{Duration, Timestamp};
use coppice_scheduler::score::{age_horizon, effective_score, penalty_product};
use coppice_state::{PolicyConfig, QuotaEntity, QUOTA_TREE_DEPTH_CAP};
use proptest::prelude::*;
use std::collections::BTreeMap;

/// 2³², the Q32.32 scale.
const Q: f64 = 4_294_967_296.0;
/// The default policy's half-life horizon (~24 h).
const H: Duration = Duration::from_days(1);

fn real(m: u64) -> f64 {
    m as f64 / Q
}

/// A fixture instant, `micros` past the epoch. The generated instants below
/// stay well inside the representable range.
fn ts(micros: i64) -> Timestamp {
    Timestamp::from_micros(micros).expect("generated timestamps are in range")
}

proptest! {
    /// Strictly increasing in the multiplier at a fixed penalty and age. The
    /// multiplier gap is kept ≥ 1× and the penalty bounded so the difference
    /// stays above the `f64` ULP.
    #[test]
    fn score_strictly_increases_with_multiplier(
        base in 0u64..(1u64 << 40),
        delta in (1u64 << 32)..(1u64 << 40),
        penalty in 1.0f64..1000.0,
        age in 0i64..1_000_000_000,
        w_age in 0.1f64..10.0,
    ) {
        let now = ts(2_000_000_000_000_000);
        let submitted = now - Duration::from_micros(age);
        let lo = effective_score(PriorityMultiplier(base), penalty, submitted, now, H, w_age);
        let hi = effective_score(PriorityMultiplier(base + delta), penalty, submitted, now, H, w_age);
        prop_assert!(hi > lo, "hi {hi} !> lo {lo}");
    }

    /// Strictly increasing in age given `w_age > 0`. Ages differ by whole
    /// horizons so the age-term increment (`w_age · gap`) clears the ULP of the
    /// bounded priority term.
    #[test]
    fn score_strictly_increases_with_age(
        m in 0u64..(1u64 << 44),
        penalty in 1.0f64..1_000_000.0,
        w_age in 0.1f64..10.0,
        base_horizons in 0i64..1000,
        gap_horizons in 1i64..1000,
    ) {
        let now = ts(5_000_000_000_000_000);
        let younger = effective_score(PriorityMultiplier(m), penalty, now - H.saturating_mul(base_horizons), now, H, w_age);
        let older = effective_score(
            PriorityMultiplier(m),
            penalty,
            now - H.saturating_mul(base_horizons + gap_horizons),
            now,
            H,
            w_age,
        );
        prop_assert!(older > younger, "older {older} !> younger {younger}");
    }

    /// At zero age the score is exactly the priority term `m / P` — the age
    /// term is a clean additive zero, so composition is bit-exact.
    #[test]
    fn zero_age_score_is_exactly_the_priority_term(
        m in 0u64..u64::MAX,
        penalty in 1.0f64..1e9,
        w_age in 0.0f64..10.0,
    ) {
        let now = ts(1_000_000);
        let s = effective_score(PriorityMultiplier(m), penalty, now, now, H, w_age);
        prop_assert_eq!(s, real(m) / penalty);
    }

    /// A larger penalty product strictly lowers the score: it divides the
    /// priority term only, leaving the age term untouched.
    #[test]
    fn larger_penalty_strictly_lowers_the_score(
        m in (1u64 << 32)..(1u64 << 44),
        penalty in 1.0f64..1000.0,
        extra in 0.5f64..1000.0,
        age_frac in 0i64..H.as_micros(),
    ) {
        let now = ts(7_000_000_000_000_000);
        let submitted = now - Duration::from_micros(age_frac);
        let lo_p = effective_score(PriorityMultiplier(m), penalty, submitted, now, H, 1.0);
        let hi_p = effective_score(PriorityMultiplier(m), penalty + extra, submitted, now, H, 1.0);
        prop_assert!(lo_p > hi_p, "score at P {penalty} ({lo_p}) !> at P {} ({hi_p})", penalty + extra);
    }

    /// Friday-evening: a later high-priority job outranks the morning's
    /// low-priority backlog while the age gap stays under the head-start
    /// `(Δm / (P · w_age)) · H` — priority buys a bounded lead, then aging wins.
    #[test]
    fn urgent_outranks_backlog_within_the_head_start(
        m_lo in (1u64 << 32)..(1u64 << 40),
        dm in (1u64 << 32)..(1u64 << 40),
        penalty in 1.0f64..100.0,
        w_age in 0.5f64..2.0,
        frac in 0.0f64..0.9,
    ) {
        let m_hi = m_lo + dm;
        let now = ts(9_000_000_000_000_000);
        // The exact head-start, then a gap strictly inside it.
        let head_start_us = (real(m_hi) - real(m_lo)) * H.as_micros() as f64 / (penalty * w_age);
        let gap = Duration::from_micros((head_start_us * frac) as i64);
        let backlog = effective_score(PriorityMultiplier(m_lo), penalty, now - gap, now, H, w_age);
        let urgent = effective_score(PriorityMultiplier(m_hi), penalty, now, now, H, w_age);
        prop_assert!(urgent > backlog, "urgent {urgent} !> backlog {backlog} at gap {gap}");
    }

    /// Bit-determinism: identical inputs yield bit-identical scores.
    #[test]
    fn score_is_bit_deterministic(
        m in 0u64..u64::MAX,
        penalty in 1.0f64..1e12,
        submitted_us in 0i64..2_000_000_000_000_000,
        now_us in 0i64..2_000_000_000_000_000,
        w_age in 0.0f64..10.0,
    ) {
        let (submitted, now) = (ts(submitted_us), ts(now_us));
        let a = effective_score(PriorityMultiplier(m), penalty, submitted, now, H, w_age);
        let b = effective_score(PriorityMultiplier(m), penalty, submitted, now, H, w_age);
        prop_assert_eq!(a.to_bits(), b.to_bits());
    }

    /// The penalty product is ≥ 1, bit-deterministic, and nondecreasing as an
    /// entity's usage rises (more over-quota ⇒ heavier penalty, ADR 0005).
    #[test]
    fn penalty_product_is_monotone_and_deterministic(
        quota in 1u64..1_000_000_000,
        usage_lo in 0u64..2_000_000_000,
        extra in 0u64..2_000_000_000,
    ) {
        let policy = PolicyConfig::default();
        let leaf = QuotaEntityId(uuid::Uuid::from_u128(1));
        let mut entities: BTreeMap<QuotaEntityId, QuotaEntity> = BTreeMap::new();
        let entity = |usage: u64| QuotaEntity {
            parent: None,
            name: String::new(),
            quota: CostUnits(quota),
            usage: UsageState { usage: CostUnits(usage), last_update: ts(0) },
        };
        entities.insert(leaf, entity(usage_lo));
        let lo = penalty_product(&entities, leaf, &policy, ts(0));
        let lo_again = penalty_product(&entities, leaf, &policy, ts(0));
        prop_assert_eq!(lo.to_bits(), lo_again.to_bits());
        prop_assert!(lo >= 1.0);
        entities.insert(leaf, entity(usage_lo.saturating_add(extra)));
        let hi = penalty_product(&entities, leaf, &policy, ts(0));
        prop_assert!(hi >= lo, "penalty must not fall as usage rises: {hi} < {lo}");
    }
}

// The horizon derivation is a total function of the replicated decay policy;
// it is always positive so the age term never divides by zero.
proptest! {
    #[test]
    fn horizon_is_always_positive(
        tick_us in 1i64..3_600_000_000,
        decay_per_tick in 0u64..=DecayPolicy::MAX_DECAY_PER_TICK,
    ) {
        let decay = DecayPolicy { tick: Duration::from_micros(tick_us), decay_per_tick };
        prop_assert!(age_horizon(&decay).is_positive());
    }
}

/// A leaf whose parent chain exceeds the depth cap is walked at most
/// `QUOTA_TREE_DEPTH_CAP` hops, exactly as apply charges — the product stays
/// finite and bounded rather than looping.
#[test]
fn penalty_product_is_depth_capped() {
    let policy = PolicyConfig::default();
    let mut entities: BTreeMap<QuotaEntityId, QuotaEntity> = BTreeMap::new();
    let depth = (QUOTA_TREE_DEPTH_CAP as u128) * 4;
    for i in 0..depth {
        let id = QuotaEntityId(uuid::Uuid::from_u128(i + 1));
        let parent = if i == 0 {
            None
        } else {
            Some(QuotaEntityId(uuid::Uuid::from_u128(i)))
        };
        // Every level is 2× over quota (penalty 4 at the default exponent).
        entities.insert(
            id,
            QuotaEntity {
                parent,
                name: String::new(),
                quota: CostUnits(1_000_000),
                usage: UsageState {
                    usage: CostUnits(2_000_000),
                    last_update: ts(0),
                },
            },
        );
    }
    let leaf = QuotaEntityId(uuid::Uuid::from_u128(depth));
    let product = penalty_product(&entities, leaf, &policy, ts(0));
    // Capped at 32 hops of 4× ⇒ exactly 4^32, not the full chain.
    assert_eq!(product, 4.0_f64.powi(QUOTA_TREE_DEPTH_CAP as i32));
}
