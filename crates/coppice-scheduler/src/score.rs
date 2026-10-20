//! The effective-score ranking formula (ADR 0021, resolving OD-13; amended
//! by ADR 0047).
//!
//! ```text
//! effective_score(j, now) = (m(j) + w(class(j)) · age(j, now) / H) / P(j, now)
//! ```
//!
//! - `m(j)` — the job's Q32.32 priority multiplier as a real number. This is
//!   `base(job)`: monotone in priority by construction (assuming the
//!   replicated `priority_multipliers` table is monotone, an operator
//!   obligation), with no size term — cheap-job bias already comes from
//!   packing and strict backfill (ADR 0014), and adding one here would
//!   double-count (OD-13).
//! - `P(j, now)` — ADR 0005's penalty product over the job's quota-entity
//!   path, computed on usage decayed to `now` with the exact integer decay
//!   from `coppice_core::quota` (never reimplemented here).
//! - the age credit — `w(class) · age / H`, where `w(class)` is the age weight
//!   of the job's priority class (ADR 0047's per-class table, falling back to
//!   the scheduler-wide `w_age`; see `SchedulerConfig::age_weight`). A weight
//!   of zero makes a class that never ages. `H` is the decay half-life derived
//!   from replicated policy, so within one entity "priority buys a bounded
//!   head-start measured in half-lives".
//! - the penalty divides the whole numerator (ADR 0047): an entity at penalty
//!   `P` ages at `w / P` points per horizon. Aging survives any finite
//!   penalty; an infinite penalty (zero quota, nonzero usage) scores zero,
//!   which ranks last under contention but is not an admission rule: the
//!   pass still seats a zero-scoring candidate that fits.
//!
//! Everything here is `f64` on ADR 0019's derived side of the line: scores
//! order a proposal and are discarded; only the resulting placement decisions
//! replicate. The evaluation shape is fixed (one addition, one quotient, one
//! product walked in path order) — there is no accumulation over unordered
//! collections, so the result is a deterministic function of the inputs.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use coppice_core::id::{JobId, QuotaEntityId};
use coppice_core::quota::{self, DecayPolicy, PriorityMultiplier};
use coppice_core::time::{Duration, Timestamp};
use coppice_state::{PolicyConfig, QuotaEntity, QUOTA_TREE_DEPTH_CAP};

/// 2³², the Q32.32 scale factor.
const Q32_SCALE: f64 = 4_294_967_296.0;

/// Default weight of the age term: one effective-priority point per
/// half-life waited.
///
/// A scheduler-side scoring knob, deliberately not replicated policy
/// (see `coppice-scheduler/README.md`).
pub const DEFAULT_AGE_WEIGHT: f64 = 1.0;

/// The ADR 0021 effective score, in ADR 0047's form:
/// `(m + w_age · age / H) / P`. Pure; `now` is an argument, never a clock
/// read.
///
/// `w_age` is the age weight of *this job's* class, resolved by the caller
/// (`SchedulerConfig::age_weight`). `penalty_product` comes from
/// [`penalty_product`]; `age_horizon` from [`age_horizon`].
///
/// Guaranteed NaN-free: the numerator is finite and `>= 0` (a finite
/// multiplier plus a finite, non-negative age credit — age is clamped at zero
/// and the horizon at one microsecond), the penalty product lies in
/// `[1, +∞]`, and `finite / +∞ == 0`.
pub fn effective_score(
    multiplier: PriorityMultiplier,
    penalty_product: f64,
    submitted_at: Timestamp,
    now: Timestamp,
    age_horizon: Duration,
    w_age: f64,
) -> f64 {
    let m = multiplier.0 as f64 / Q32_SCALE;
    // Clamped at zero: a submit stamped ahead of `now` by clock skew earns
    // no age, mirroring the decay clamp of ADR 0019.
    let age = (now - submitted_at).max(Duration::ZERO);
    let horizon = age_horizon.max(Duration::from_micros(1));
    (m + w_age * (age.as_secs_f64() / horizon.as_secs_f64())) / penalty_product
}

/// The penalty product over a job's quota-entity path (ADR 0005).
///
/// Walks leaf → root exactly as apply's `charge_ancestors` does: depth-capped
/// at [`QUOTA_TREE_DEPTH_CAP`], stopping at a missing parent. Usage is
/// brought forward to `now` with the replicated integer decay
/// ([`DecayPolicy::decay_between`]) before the float ratio is taken, so the
/// only floats are the derived ratio and penalty (ADR 0019).
pub fn penalty_product(
    entities: &BTreeMap<QuotaEntityId, QuotaEntity>,
    leaf: QuotaEntityId,
    policy: &PolicyConfig,
    now: Timestamp,
) -> f64 {
    let mut product = 1.0;
    let mut cur = Some(leaf);
    for _ in 0..QUOTA_TREE_DEPTH_CAP {
        let Some(id) = cur else { break };
        let Some(e) = entities.get(&id) else { break };
        let decayed = policy
            .decay
            .decay_between(e.usage.usage, e.usage.last_update, now);
        product *= quota::penalty(
            quota::over_quota_ratio(decayed, e.quota),
            policy.penalty_exponent_milli,
        );
        cur = e.parent;
    }
    product
}

/// The age horizon `H`: the decay half-life implied by the replicated decay
/// policy.
///
/// Derived with `f64` transcendentals, which is fine on this side of the
/// ADR 0019 line — the horizon shapes scores, never replicated state. Clamped
/// to at least one tick so degenerate policies (λ → 0 decays instantly) can't
/// divide the age term by zero.
pub fn age_horizon(decay: &DecayPolicy) -> Duration {
    let tick = decay.tick.max(Duration::from_micros(1));
    let lambda = decay.decay_per_tick as f64 / 18_446_744_073_709_551_616.0; // 2^64
    if !(lambda > 0.0 && lambda < 1.0) {
        return tick;
    }
    let half_life_ticks = 0.5_f64.ln() / lambda.ln();
    let horizon = half_life_ticks * tick.as_micros() as f64;
    if horizon.is_finite() && horizon >= tick.as_micros() as f64 {
        Duration::from_micros(horizon.min(i64::MAX as f64) as i64)
    } else {
        tick
    }
}

/// A scored candidate with its ADR 0021 tie-breakers.
///
/// Ordered: score descending, then FIFO by submission time (ADR 0005), then
/// `JobId` for a stable total order. `total_cmp` keeps this total even at ±∞
/// (NaN is unreachable, see [`effective_score`]), which is what makes the
/// manual `Eq` sound.
#[derive(Debug, Clone, Copy)]
pub struct Rank {
    pub score: f64,
    pub submitted_at: Timestamp,
    pub job: JobId,
}

impl Ord for Rank {
    fn cmp(&self, other: &Rank) -> Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.submitted_at.cmp(&other.submitted_at))
            .then_with(|| self.job.cmp(&other.job))
    }
}

impl PartialOrd for Rank {
    fn partial_cmp(&self, other: &Rank) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Rank {
    fn eq(&self, other: &Rank) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Rank {}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::quota::{CostUnits, UsageState};

    /// The fixture instant every test in this module measures from.
    fn ts() -> Timestamp {
        Timestamp::from_micros(1_760_000_000_000_000).expect("in range")
    }

    /// Half of `d` — the age gap several tests place a job at.
    fn half(d: Duration) -> Duration {
        d.checked_div(2).expect("nonzero divisor")
    }

    fn entity(parent: Option<QuotaEntityId>, quota: u64, usage: u64) -> QuotaEntity {
        QuotaEntity {
            parent,
            name: String::new(),
            quota: CostUnits(quota),
            usage: UsageState {
                usage: CostUnits(usage),
                last_update: ts(),
            },
            created_at: ts(),
            updated_at: ts(),
            preemptible: true,
        }
    }

    #[test]
    fn default_policy_horizon_is_one_half_life() {
        // 24 h at the default policy (60 s ticks, 1440-tick half-life).
        let h = age_horizon(&DecayPolicy::DEFAULT);
        let day = Duration::from_days(1);
        let tolerance = day.checked_div(100).expect("nonzero divisor");
        assert!((h - day).abs() < tolerance, "horizon {h} not ~24h");
    }

    #[test]
    fn degenerate_decay_clamps_to_one_tick() {
        let instant = DecayPolicy {
            tick: Duration::from_secs(60),
            decay_per_tick: 0,
        };
        assert_eq!(age_horizon(&instant), Duration::from_secs(60));
    }

    #[test]
    fn score_is_monotone_in_multiplier() {
        let h = age_horizon(&DecayPolicy::DEFAULT);
        let lo = effective_score(PriorityMultiplier::ONE, 1.0, ts(), ts(), h, 1.0);
        let hi = effective_score(PriorityMultiplier::from_integer(3), 1.0, ts(), ts(), h, 1.0);
        assert!(hi > lo);
    }

    #[test]
    fn score_grows_with_age_and_clamps_skew() {
        let h = age_horizon(&DecayPolicy::DEFAULT);
        let m = PriorityMultiplier::ONE;
        let fresh = effective_score(m, 1.0, ts(), ts(), h, 1.0);
        let aged = effective_score(m, 1.0, ts(), ts() + h, h, 1.0);
        assert!((aged - fresh - 1.0).abs() < 1e-9, "one horizon = one point");
        // A submit stamped in the future earns no age.
        let skewed = effective_score(m, 1.0, ts() + h, ts(), h, 1.0);
        assert_eq!(skewed, fresh);
    }

    #[test]
    fn infinite_penalty_scores_zero_whatever_the_age() {
        let h = age_horizon(&DecayPolicy::DEFAULT);
        for (m, age) in [
            (PriorityMultiplier::from_integer(10), half(h)),
            (PriorityMultiplier::ONE, h.saturating_mul(1_000)),
            (PriorityMultiplier::ONE, Duration::ZERO),
        ] {
            let s = effective_score(m, f64::INFINITY, ts(), ts() + age, h, 1.0);
            assert_eq!(s, 0.0, "an infinite penalty ranks last, not NaN");
        }
    }

    #[test]
    fn zero_weight_scores_the_priority_term_at_any_age() {
        let h = age_horizon(&DecayPolicy::DEFAULT);
        let m = PriorityMultiplier::from_integer(3);
        for age in [Duration::ZERO, h, h.saturating_mul(10_000)] {
            let s = effective_score(m, 4.0, ts(), ts() + age, h, 0.0);
            assert_eq!(s, 3.0 / 4.0, "a zero-weight class never ages");
        }
    }

    #[test]
    fn penalty_divides_the_age_credit() {
        let h = age_horizon(&DecayPolicy::DEFAULT);
        let m = PriorityMultiplier::from_integer(3);
        let s = effective_score(m, 2.0, ts(), ts() + h, h, 1.0);
        assert!(
            (s - (3.0 + 1.0) / 2.0).abs() < 1e-9,
            "expected (m + 1) / P, got {s}"
        );
    }

    #[test]
    fn penalty_product_multiplies_the_ancestor_path() {
        let policy = PolicyConfig::default();
        let root = QuotaEntityId::new();
        let leaf = QuotaEntityId::new();
        let mut entities = BTreeMap::new();
        // Root at 2x quota (penalty 4 at the default quadratic exponent),
        // leaf at 3x (penalty 9).
        entities.insert(root, entity(None, 1_000_000, 2_000_000));
        entities.insert(leaf, entity(Some(root), 1_000_000, 3_000_000));
        let p = penalty_product(&entities, leaf, &policy, ts());
        assert!((p - 36.0).abs() < 1e-9, "expected 9 * 4, got {p}");
    }

    #[test]
    fn penalty_product_decays_usage_to_now() {
        let policy = PolicyConfig::default();
        let leaf = QuotaEntityId::new();
        let mut entities = BTreeMap::new();
        // 4x over quota at TS; one half-life later it reads ~2x → penalty ~4.
        entities.insert(leaf, entity(None, 1_000_000, 4_000_000));
        let one_half_life = Duration::from_days(1);
        let p = penalty_product(&entities, leaf, &policy, ts() + one_half_life);
        assert!(
            (p - 4.0).abs() < 0.01,
            "expected ~4 after one half-life, got {p}"
        );
    }

    #[test]
    fn rank_orders_by_score_then_fifo_then_id() {
        let a = JobId::new();
        let b = JobId::new();
        let hi = Rank {
            score: 2.0,
            submitted_at: ts() + Duration::from_micros(10),
            job: a,
        };
        let lo = Rank {
            score: 1.0,
            submitted_at: ts(),
            job: b,
        };
        assert_eq!(hi.cmp(&lo), Ordering::Less, "higher score ranks first");
        let old = Rank {
            score: 1.0,
            submitted_at: ts(),
            job: a,
        };
        let new = Rank {
            score: 1.0,
            submitted_at: ts() + Duration::from_micros(1),
            job: b,
        };
        assert_eq!(old.cmp(&new), Ordering::Less, "FIFO breaks score ties");
        let (first, second) = if a < b { (a, b) } else { (b, a) };
        let x = Rank {
            score: 1.0,
            submitted_at: ts(),
            job: first,
        };
        let y = Rank {
            score: 1.0,
            submitted_at: ts(),
            job: second,
        };
        assert_eq!(x.cmp(&y), Ordering::Less, "JobId is the final tie-break");
    }

    #[test]
    fn friday_evening_backlog_outranked_within_the_head_start() {
        // Same submitter (same penalty product): a later 3x job outranks the
        // morning's 1x backlog while the age gap is under (Δm/w)·H. The
        // penalty divides both scores alike, so the head start no longer
        // depends on P (ADR 0047).
        let h = age_horizon(&DecayPolicy::DEFAULT);
        let p = 4.0; // the submitter is over quota; both jobs share the factor
        let backlog = effective_score(PriorityMultiplier::ONE, p, ts(), ts() + half(h), h, 1.0);
        let urgent = effective_score(
            PriorityMultiplier::from_integer(3),
            p,
            ts() + half(h),
            ts() + half(h),
            h,
            1.0,
        );
        assert!(urgent > backlog);
    }
}
