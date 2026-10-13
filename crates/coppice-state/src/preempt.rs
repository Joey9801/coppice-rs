//! Scheduler preemption facts (ADR 0044 §4) that the scheduler's pass and
//! apply's `CommitPlacements` validation must agree on.
//!
//! Everything here is derived from the snapshot — read off attempt and
//! allocation records where it is needed — and never stored on the
//! [`StateMachine`]: the per-node guard and the cooldown are facts about the
//! attempts' termination records, and derived state does not get replicated.

use std::collections::{BTreeMap, BTreeSet};

use coppice_core::allocation::AllocationState;
use coppice_core::attempt::{AttemptState, PreemptReason};
use coppice_core::id::{AllocationId, JobId, NodeId};
use coppice_core::job::Job;
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};

use crate::command::{CommitPlacements, Placement};
use crate::{AllocationRecord, RejectionReason, StateMachine};

/// The scheduler's own terminations, per node, read off attempt records in
/// one scan: a handler-scoped memo the caller builds once per pass or batch
/// and throws away.
///
/// Only `Priority` and `Relocation` records count. A drain or an
/// interruption is not the scheduler's doing, so it neither guards a node
/// nor starts its cooldown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreemptionLedger {
    /// Nodes with *outstanding scheduler terminations*: some live attempt on
    /// the node carries a `Priority` or `Relocation` record. While it holds,
    /// no further eviction group is proposed there (the per-node guard).
    pub guarded: BTreeSet<NodeId>,
    /// Jobs some live attempt is still being terminated for — the
    /// `for_job` of an outstanding record. Such a job may not start a second
    /// eviction group elsewhere (ADR 0044 §4: one live attempt per job, so
    /// the guard follows the beneficiary).
    pub beneficiaries: BTreeSet<JobId>,
    /// The latest scheduler-initiated `requested_at` on each node, live or
    /// ended attempt alike: the cooldown's clock.
    pub latest_request: BTreeMap<NodeId, Timestamp>,
}

impl StateMachine {
    /// Build the [`PreemptionLedger`] in one scan of the attempt map.
    pub fn preemption_ledger(&self) -> PreemptionLedger {
        let mut ledger = PreemptionLedger::default();
        for record in self.attempts.values() {
            let Some(preemption) = record.preemption else {
                continue;
            };
            let Some(for_job) = preemption.reason.for_job() else {
                continue;
            };
            let node = record.attempt.node;
            let latest = ledger
                .latest_request
                .entry(node)
                .or_insert(preemption.requested_at);
            *latest = (*latest).max(preemption.requested_at);
            if !record.attempt.state.is_terminal() {
                ledger.guarded.insert(node);
                ledger.beneficiaries.insert(for_job);
            }
        }
        ledger
    }

    /// The notice a job's victim attempt is owed (ADR 0044 §1): the stored
    /// declaration, which apply resolves at submission so it is always
    /// present; the policy floor is what an absent one would have resolved to.
    pub fn declared_notice(&self, job: &Job) -> Duration {
        job.preemption
            .notice
            .unwrap_or(self.policy.preemption_notice_floor)
    }

    /// When a live allocation is *guaranteed* to release its funded capacity
    /// (ADR 0014, ADR 0044 §3): a `Running` attempt with a recorded start
    /// releases at the earlier of `start + max_runtime` (an enforced bound)
    /// and `deadline + abort_grace` (an acknowledged termination). An
    /// unacknowledged request bounds nothing — the workload has not been
    /// warned — and anything else has no guaranteed bound.
    ///
    /// The one definition the scheduler's release events and apply's
    /// preemption fit check both use.
    pub fn guaranteed_release(&self, record: &AllocationRecord) -> Option<Timestamp> {
        if record.allocation.state == AllocationState::Released {
            return None;
        }
        let attempt = self.attempts.get(&record.allocation.attempt)?;
        if attempt.attempt.state != AttemptState::Running {
            return None;
        }
        let started = attempt.started_at?;
        let runtime_bound = self
            .jobs
            .get(&record.allocation.job)
            .and_then(|job| job.spec.max_runtime)
            .map(|runtime| started.saturating_add(runtime));
        let preemption_bound = attempt
            .preemption
            .and_then(|p| p.acknowledged)
            .map(|ack| ack.deadline.saturating_add(self.policy.abort_grace));
        match (runtime_bound, preemption_bound) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (bound @ Some(_), None) | (None, bound @ Some(_)) => bound,
            (None, None) => None,
        }
    }
}

/// Why apply refused a preempting placement's victim or its eviction group
/// (ADR 0044 §4). Every one is a proposer bug: the scheduler's pass asks the
/// same questions of the same snapshot before it proposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PreemptionRefusal {
    #[error("named twice in the batch")]
    Duplicate,
    #[error("not on the node the beneficiary is placed on")]
    WrongNode,
    #[error("the allocation is not active")]
    NotActive,
    #[error("its attempt is not its job's current, running attempt")]
    NotRunning,
    #[error("its attempt already carries a termination record")]
    AlreadyTerminating,
    #[error("its attempt was not placed effectively preemptible")]
    NotPreemptible,
    #[error("its node is draining")]
    NodeDraining,
    #[error("the beneficiary does not outrank it (class rule) and relocation does not apply")]
    NotOutranked,
    #[error(
        "the node already has outstanding scheduler terminations, or the beneficiary \
         already has victims (per-node guard)"
    )]
    Guarded,
    #[error("the beneficiary does not fit once the victims are released")]
    DoesNotFit,
}

/// Apply's per-batch context for re-validating preempting placements: the
/// ledger and every live allocation on the eviction targets, each from one
/// scan, built only for a batch that carries a preemption. Lives for one
/// `CommitPlacements` and is thrown away.
pub(crate) struct PreemptCheck {
    now: Timestamp,
    ledger: PreemptionLedger,
    /// Per eviction-target node: each live allocation's id, funded hold and
    /// guaranteed release.
    node_allocs: BTreeMap<NodeId, Vec<(AllocationId, Resources, Option<Timestamp>)>>,
    /// Victims named so far in the batch.
    victims: BTreeSet<AllocationId>,
    /// Nodes an earlier placement of the batch already evicts on: one
    /// eviction group per node at a time.
    groups: BTreeSet<NodeId>,
}

impl PreemptCheck {
    /// `None` when no placement in the batch preempts — the common case pays
    /// for nothing.
    pub(crate) fn for_batch(state: &StateMachine, c: &CommitPlacements) -> Option<PreemptCheck> {
        let targets: BTreeSet<NodeId> = c
            .placements
            .iter()
            .filter(|p| !p.preemptions.is_empty())
            .flat_map(|p| p.allocations.iter().map(|a| a.node))
            .collect();
        if targets.is_empty() {
            return None;
        }
        let mut node_allocs: BTreeMap<NodeId, Vec<_>> = BTreeMap::new();
        for record in state.allocations.values() {
            let node = record.allocation.node;
            if !targets.contains(&node) || record.allocation.state == AllocationState::Released {
                continue;
            }
            node_allocs.entry(node).or_default().push((
                record.allocation.id,
                record.allocation.funded,
                state.guaranteed_release(record),
            ));
        }
        Some(PreemptCheck {
            now: c.proposed_at,
            ledger: state.preemption_ledger(),
            node_allocs,
            victims: BTreeSet::new(),
            groups: BTreeSet::new(),
        })
    }
}

impl StateMachine {
    /// Re-validate one placement's eviction group (ADR 0044 §4) and decide
    /// each victim's reason. The score rule is the proposer's alone: it
    /// depends on the scheduler-local `w_age`, which is not replicated.
    ///
    /// `revoked` is the batch's revocation set: a revoked accrual on the node
    /// (the beneficiary's own, in a revoke-and-reseat) holds nothing once the
    /// batch lands.
    pub(crate) fn validate_preemptions(
        &self,
        p: &Placement,
        revoked: &BTreeSet<AllocationId>,
        check: &mut PreemptCheck,
    ) -> Result<Vec<(AllocationId, PreemptReason)>, RejectionReason> {
        let Some(&first) = p.preemptions.first() else {
            return Ok(Vec::new());
        };
        let refuse = |allocation: AllocationId, refusal: PreemptionRefusal| {
            Err(RejectionReason::InvalidPreemption {
                allocation,
                refusal,
            })
        };
        // Item validation already pinned the job, the v1 shape and the node.
        let (Some(job), Some(spec)) = (self.jobs.get(&p.job), p.allocations.first()) else {
            return Err(RejectionReason::UnknownJob(p.job));
        };
        let node_id = spec.node;
        let Some(node) = self.nodes.get(&node_id) else {
            return Err(RejectionReason::UnknownNode(node_id));
        };
        if node.is_draining() {
            return refuse(first, PreemptionRefusal::NodeDraining);
        }
        if check.ledger.guarded.contains(&node_id)
            || check.ledger.beneficiaries.contains(&p.job)
            || !check.groups.insert(node_id)
        {
            return refuse(first, PreemptionRefusal::Guarded);
        }

        let mut out = Vec::with_capacity(p.preemptions.len());
        let mut max_notice = Duration::ZERO;
        for &victim in &p.preemptions {
            if !check.victims.insert(victim) {
                return refuse(victim, PreemptionRefusal::Duplicate);
            }
            let Some(alloc) = self.allocations.get(&victim) else {
                return Err(RejectionReason::UnknownAllocation(victim));
            };
            if alloc.allocation.node != node_id {
                return refuse(victim, PreemptionRefusal::WrongNode);
            }
            if alloc.allocation.state != AllocationState::Active {
                return refuse(victim, PreemptionRefusal::NotActive);
            }
            let attempt = self
                .attempts
                .get(&alloc.allocation.attempt)
                .filter(|a| a.attempt.state == AttemptState::Running);
            let victim_job = self.jobs.get(&alloc.allocation.job);
            let (Some(attempt), Some(victim_job)) = (attempt, victim_job) else {
                return refuse(victim, PreemptionRefusal::NotRunning);
            };
            if victim_job.current_attempt() != Some(attempt.attempt.id) {
                return refuse(victim, PreemptionRefusal::NotRunning);
            }
            if attempt.preemption.is_some() {
                return refuse(victim, PreemptionRefusal::AlreadyTerminating);
            }
            if !attempt.effective_preemptible {
                return refuse(victim, PreemptionRefusal::NotPreemptible);
            }
            // The class rule: a chain of priority preemptions is strictly
            // increasing in class.
            let reason = if victim_job.spec.priority < job.spec.priority {
                PreemptReason::Priority { for_job: p.job }
            } else {
                return refuse(victim, PreemptionRefusal::NotOutranked);
            };
            max_notice = max_notice.max(self.declared_notice(&victim_job.spec));
            out.push((victim, reason));
        }

        // The beneficiary fits once the victims are released, counting the
        // node's other guaranteed releases inside the notice bound — the
        // pass's own test, with the batch's other effects on the node left
        // out, which only ever makes this check the more lenient one.
        let bound = check
            .now
            .saturating_add(max_notice)
            .saturating_add(self.policy.abort_grace);
        let held = check
            .node_allocs
            .get(&node_id)
            .into_iter()
            .flatten()
            .filter(|(id, _, release)| {
                !revoked.contains(id)
                    && !p.preemptions.contains(id)
                    && !release.is_some_and(|at| at <= bound)
            })
            .fold(Resources::ZERO, |acc, (_, funded, _)| {
                acc.saturating_add(funded)
            });
        if !spec
            .requested
            .fits_within(&node.node.capacity.saturating_sub(&held))
        {
            return refuse(first, PreemptionRefusal::DoesNotFit);
        }
        Ok(out)
    }
}
