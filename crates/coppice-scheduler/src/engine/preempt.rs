//! Priority preemption (ADR 0044 §4): the pass's `try_preempt` step.
//!
//! A candidate with no free fit and no legal backfill compares `natural`,
//! the best `projected_ready` the ordinary rules offer it, with the bound a
//! preemption would manufacture, `now + notice + abort_grace` for the
//! victims' longest notice. It preempts when `natural` is indefinite, or
//! later than the manufactured bound by at least
//! [`SchedulerConfig::preemption_min_improvement`](crate::SchedulerConfig).
//! In ADR 0027's finite-first ordering that sits after "finite accrual
//! within the threshold" and before "indefinite accrual"; the callers in
//! `engine` place it there.
//!
//! The beneficiary is committed **accruing** on the victims' node, and the
//! victims ride its placement as `preemptions`: apply re-validates each one
//! and creates its termination record, and the release the acknowledged
//! notice manufactures funds the accrual through the ordinary pledge path.
//! Everything here reads the pass's working model and the pass-scoped
//! [`PreemptionLedger`](coppice_state::PreemptionLedger); nothing is stored.

use std::collections::{BTreeMap, BTreeSet};

use coppice_core::allocation::AllocationState;
use coppice_core::attempt::AttemptState;
use coppice_core::id::{AllocationId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::Job;
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::StateMachine;

use super::{entity_penalty, node_admits, node_satisfies_labels, simulate_batch, Pass};
use crate::{score, ProposedPlacement};

/// A running attempt the pass may choose as a victim at all: `Active`, its
/// job's current `Running` attempt, recorded effectively preemptible, with
/// no termination record, on a job not being aborted. Whether a particular
/// beneficiary outranks it is asked per beneficiary.
#[derive(Debug, Clone)]
pub(super) struct Victim {
    pub(super) alloc: AllocationId,
    /// Commit order: identifies the allocation's own release event.
    pub(super) seq: u64,
    pub(super) funded: Resources,
    /// `Job.priority`, the class the class rule compares.
    pub(super) priority: i32,
    /// ADR 0021's effective score, applied to the running job with the
    /// pass's own `w_age` and `now`.
    pub(super) score: f64,
    /// `(now − started_at) × rate`: the progress a preemption throws away,
    /// in µCU-scaled units so dimensions weigh as the cost weights do.
    pub(super) work_at_risk: u128,
    /// The victim's declared notice: what its agent counts down.
    pub(super) notice: Duration,
}

/// Every potential victim, per node, from one scan of the allocation map.
pub(super) fn candidate_victims(
    snapshot: &StateMachine,
    now: Timestamp,
    horizon: Duration,
    w_age: f64,
    penalties: &mut BTreeMap<QuotaEntityId, f64>,
) -> BTreeMap<NodeId, Vec<Victim>> {
    let mut out: BTreeMap<NodeId, Vec<Victim>> = BTreeMap::new();
    for rec in snapshot.allocations.values() {
        if rec.allocation.state != AllocationState::Active {
            continue;
        }
        let Some(attempt) = snapshot.attempts.get(&rec.allocation.attempt) else {
            continue;
        };
        if attempt.attempt.state != AttemptState::Running
            || !attempt.effective_preemptible
            || attempt.preemption.is_some()
        {
            continue;
        }
        let Some(job) = snapshot.jobs.get(&rec.allocation.job) else {
            continue;
        };
        // An aborting job is already being stopped; evicting it buys nothing.
        if job.current_attempt() != Some(attempt.attempt.id) || job.spec.abort_requested.is_some() {
            continue;
        }
        let penalty = entity_penalty(penalties, snapshot, job.spec.quota_entity, now);
        let ran = attempt
            .started_at
            .map(|started| now.duration_since(started).max(Duration::ZERO))
            .unwrap_or(Duration::ZERO);
        out.entry(rec.allocation.node).or_default().push(Victim {
            alloc: rec.allocation.id,
            seq: rec.seq,
            funded: rec.allocation.funded,
            priority: job.spec.priority,
            score: score::effective_score(
                job.multiplier,
                penalty,
                job.submitted_at,
                now,
                horizon,
                w_age,
            ),
            work_at_risk: u128::try_from(ran.as_micros()).unwrap_or(0)
                * u128::from(attempt.rate_ucu_per_second),
            notice: snapshot.declared_notice(&job.spec),
        });
    }
    out
}

/// The job a preemption would place.
pub(super) struct Beneficiary<'j> {
    pub(super) job_id: JobId,
    pub(super) job: &'j Job,
    /// Its ADR 0021 effective score, by the same function as the victims'.
    pub(super) score: f64,
    /// Resolved effective preemptibility, for the spot-only gate.
    pub(super) preemptible: bool,
    pub(super) required: &'j BTreeMap<String, String>,
    /// A revoke-and-reseat: the accrual the job already holds, and its node.
    pub(super) reseat: Option<(NodeId, AllocationId)>,
}

/// One node's eviction plan for a beneficiary.
struct Plan {
    idx: usize,
    /// The manufactured bound: `now + max(victim notice) + abort_grace`.
    bound: Timestamp,
    /// Total work at risk across the victims.
    risk: u128,
    victims: Vec<AllocationId>,
}

impl Pass<'_> {
    /// Try to place `b` by preemption against its `natural` bound (`None`
    /// = indefinite). On success the placement — with its eviction group,
    /// and the revocation of `b`'s current accrual when reseating — is in the
    /// batch, and the node is closed to further eviction this pass.
    pub(super) fn try_preempt(&mut self, b: &Beneficiary, natural: Option<Timestamp>) -> bool {
        if self.preempting_left == 0 || self.placements.len() >= self.max_placements {
            return false;
        }
        // One live attempt per job: a beneficiary whose victims are still
        // outstanding may not start a second group elsewhere.
        if self.ledger.beneficiaries.contains(&b.job_id) {
            return false;
        }
        // A new accrual past the cap K is refused at apply; a reseat swaps.
        if b.reseat.is_none() && self.base_before >= self.accrual_limit {
            return false;
        }
        let mut best: Option<Plan> = None;
        for idx in 0..self.nodes.len() {
            let Some(plan) = self.plan_on(idx, b) else {
                continue;
            };
            // Worth a victim's progress only when the ordinary rules offer
            // nothing, or something later by at least the threshold.
            if natural.is_some_and(|n| n < plan.bound.saturating_add(self.preempt_min_improvement))
            {
                continue;
            }
            // Earliest manufactured bound, then least work at risk; ties keep
            // the lowest `NodeId`.
            let better = match &best {
                None => true,
                Some(p) => (plan.bound, plan.risk) < (p.bound, p.risk),
            };
            if better {
                best = Some(plan);
            }
        }
        let Some(plan) = best else {
            return false;
        };

        let node_id = self.nodes[plan.idx].node.id;
        let requested = b.job.requests;
        let mut revocations = self.revocations.clone();
        if let Some((_, alloc)) = b.reseat {
            revocations.push(alloc);
        }
        let mut placements = self.placements.clone();
        placements.push(ProposedPlacement {
            job: b.job_id,
            node: node_id,
            requested,
            expect_funded: false,
            preemptions: plan.victims,
        });
        if simulate_batch(self.snapshot, &self.base_free, &revocations, &placements).rejected() {
            return false;
        }

        if let Some((source, alloc)) = b.reseat {
            self.revoke_accrual_on_source(source, alloc);
            self.revoked_nodes.insert(source);
        }
        self.revocations = revocations;
        self.placements = placements;
        let nm = &mut self.nodes[plan.idx];
        let pledge = nm.sim_free.component_min(&requested);
        nm.sim_free = nm.sim_free.saturating_sub(&pledge);
        let remaining = requested.saturating_sub(&pledge);
        if !remaining.is_zero() {
            nm.pending_accruals.push(remaining);
        }
        self.placed_nodes.insert(node_id);
        self.evicted_nodes.insert(node_id);
        self.preempting_left -= 1;
        true
    }

    /// The smallest eviction group on node `idx` that, with the node's free
    /// capacity and its other guaranteed releases inside the notice bound,
    /// fits `b` — or `None` when the node is not a candidate, or `b` would
    /// not fit with every eligible victim released.
    fn plan_on(&self, idx: usize, b: &Beneficiary) -> Option<Plan> {
        let nm = &self.nodes[idx];
        let node_id = nm.node.id;
        let requested = &b.job.requests;
        // `node_admits` also keeps a draining node's work out of reach.
        if nm.victims.is_empty()
            || !node_admits(nm, b.preemptible)
            || !requested.fits_within(&nm.node.capacity)
            || !node_satisfies_labels(b.required, &nm.node.labels)
        {
            return None;
        }
        // The per-node guard, and one group per node per pass.
        if self.ledger.guarded.contains(&node_id) || self.evicted_nodes.contains(&node_id) {
            return None;
        }
        // The cooldown: a node recently evicted on hosts no new victims.
        if self
            .ledger
            .latest_request
            .get(&node_id)
            .is_some_and(|at| self.now < at.saturating_add(self.cooldown))
        {
            return None;
        }
        // The beneficiary accrues here, so the node must hold no other
        // accrual (ADR 0027). Its own, in a reseat in place, is revoked first
        // and gives back its pledge — which must precede any placement the
        // batch already made on the node.
        let own = b
            .reseat
            .filter(|(source, _)| *source == node_id)
            .map(|(_, alloc)| alloc);
        if !nm.pending_accruals.is_empty() || nm.accruals.iter().any(|a| Some(a.alloc) != own) {
            return None;
        }
        if own.is_some() && self.placed_nodes.contains(&node_id) {
            return None;
        }
        let mut available = nm.sim_free;
        for a in nm.accruals.iter().filter(|a| Some(a.alloc) == own) {
            available = available.saturating_add(&a.funded);
        }

        // Eligibility: the class rule, then the score rule.
        let mut eligible: Vec<_> = nm
            .victims
            .iter()
            .filter(|v| v.priority < b.job.priority && v.score < b.score)
            .collect();
        // Class ascending, then score, then work at risk; `AllocationId`
        // makes the order total.
        eligible.sort_by(|x, y| {
            x.priority
                .cmp(&y.priority)
                .then(x.score.total_cmp(&y.score))
                .then(x.work_at_risk.cmp(&y.work_at_risk))
                .then(x.alloc.cmp(&y.alloc))
        });

        let abort_grace = self.snapshot.policy.abort_grace;
        let mut chosen: Vec<AllocationId> = Vec::new();
        let mut chosen_seqs: BTreeSet<u64> = BTreeSet::new();
        let mut max_notice = Duration::ZERO;
        let mut risk: u128 = 0;
        for v in eligible {
            chosen.push(v.alloc);
            chosen_seqs.insert(v.seq);
            available = available.saturating_add(&v.funded);
            max_notice = max_notice.max(v.notice);
            risk = risk.saturating_add(v.work_at_risk);
            let bound = self
                .now
                .saturating_add(max_notice)
                .saturating_add(abort_grace);
            let others = nm
                .events
                .iter()
                .filter(|e| e.at <= bound && !chosen_seqs.contains(&e.seq))
                .fold(Resources::ZERO, |acc, e| acc.saturating_add(&e.freed));
            if requested.fits_within(&available.saturating_add(&others)) {
                return Some(Plan {
                    idx,
                    bound,
                    risk,
                    victims: chosen,
                });
            }
        }
        None
    }
}
