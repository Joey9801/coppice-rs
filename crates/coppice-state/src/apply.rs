//! Deterministic application of committed commands.
//!
//! Every handler is organized as a read-only validation phase that either
//! rejects or yields to an infallible effects phase — a rejected command has
//! zero effects beyond the `version` bump, because it was already committed
//! to the log on every replica and refusing it must be exactly as
//! reproducible as applying it. The per-command contract lives in
//! `docs/architecture/command-catalog.md`.

use std::collections::{BTreeMap, BTreeSet};

use coppice_core::allocation::{Allocation, AllocationState};
use coppice_core::attempt::{Attempt, AttemptOutcome, AttemptState, OutcomeClass};
use coppice_core::id::{AllocationId, AttemptId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{AbortRequest, Job, JobState};
use coppice_core::node::Node;
use coppice_core::quota::{self, ChargeRecord, CostUnits, TrueUp, UsageState};
use coppice_core::resource::Resources;

use crate::command::{
    AbortJob, BumpClusterVersion, CommitPlacements, ConfigureQuotaEntity, DeclareNodeLost,
    DispatchAttempt, EvictTerminalJobs, Placement, ReconcileNode, RecordAttemptExited,
    RecordAttemptOutcome, RecordAttemptStarted, RegisterNode, SetNodeSchedulable, SubmitJob,
    UpdatePolicy,
};
use crate::{
    AllocationRecord, Applied, AttemptRecord, Command, Event, JobRecord, NodeRecord, QuotaEntity,
    RejectionReason, StateMachine, QUOTA_TREE_DEPTH_CAP,
};

type ApplyResult = Result<Applied, RejectionReason>;

impl StateMachine {
    /// Deterministically apply a committed command.
    ///
    /// The only entry point that mutates authoritative state, invoked on
    /// every replica from the Raft apply loop. `version` bumps whether the
    /// command is accepted or rejected: it counts applied log entries, and a
    /// rejection is itself a (no-op) applied entry.
    pub fn apply(&mut self, command: &Command) -> Result<Applied, RejectionReason> {
        let result = match command {
            Command::SubmitJob(c) => self.submit_job(c),
            Command::AbortJob(c) => self.abort_job(c),
            Command::CommitPlacements(c) => self.commit_placements(c),
            Command::DispatchAttempt(c) => self.dispatch_attempt(c),
            Command::RecordAttemptStarted(c) => self.record_attempt_started(c),
            Command::RecordAttemptExited(c) => self.record_attempt_exited(c),
            Command::RecordAttemptOutcome(c) => self.record_attempt_outcome(c),
            Command::ReconcileNode(c) => self.reconcile_node(c),
            Command::RegisterNode(c) => self.register_node(c),
            Command::DeclareNodeLost(c) => self.declare_node_lost(c),
            Command::SetNodeSchedulable(c) => self.set_node_schedulable(c),
            Command::EvictTerminalJobs(c) => self.evict_terminal_jobs(c),
            Command::ConfigureQuotaEntity(c) => self.configure_quota_entity(c),
            Command::UpdatePolicy(c) => self.update_policy(c),
            Command::BumpClusterVersion(c) => self.bump_cluster_version(c),
        };
        self.version += 1;
        result
    }

    // ---- API-proposed ----

    fn submit_job(&mut self, c: &SubmitJob) -> ApplyResult {
        if c.job.abort_requested.is_some() {
            return Err(RejectionReason::InvalidCommand(
                "submitted job carries a pre-set abort flag".into(),
            ));
        }
        if let Some(existing) = self.jobs.get(&c.job.id) {
            // The job id is the submission's idempotency identity (ADR 0026):
            // a client retry after an unknown outcome, or a re-proposal across
            // a leader change, re-commits the same client-minted id. When the
            // spec matches the committed record the original commit stands —
            // an accepted no-op with no events, so the retrying client gets
            // success and the original JobId. An id reused with a different
            // payload is a distinct intent and rejects.
            return if same_submission(&existing.spec, &c.job) {
                Ok(Applied::default())
            } else {
                Err(RejectionReason::SubmitSpecMismatch(c.job.id))
            };
        }
        if !self.quota_entities.contains_key(&c.job.quota_entity) {
            return Err(RejectionReason::UnknownQuotaEntity(c.job.quota_entity));
        }
        let job = c.job.id;
        self.jobs.insert(
            job,
            JobRecord {
                spec: c.job.clone(),
                state: JobState::Queued,
                multiplier: c.multiplier,
                submitted_at_us: c.submitted_at_us,
                terminal_at_us: None,
                retries_used: 0,
                current_attempt: None,
                attempts: Vec::new(),
            },
        );
        // Admission is synchronous in v1: one apply walks Submitted →
        // Accepted → Queued; the intermediate states surface as events only.
        Ok(Applied {
            events: vec![
                Event::JobSubmitted { job },
                Event::JobStateChanged {
                    job,
                    from: JobState::Submitted,
                    to: JobState::Accepted,
                },
                Event::JobStateChanged {
                    job,
                    from: JobState::Accepted,
                    to: JobState::Queued,
                },
            ],
        })
    }

    fn abort_job(&mut self, c: &AbortJob) -> ApplyResult {
        let (state, current) = match self.jobs.get(&c.job) {
            None => return Err(RejectionReason::UnknownJob(c.job)),
            Some(r) if r.state.is_terminal() => return Err(RejectionReason::JobTerminal(c.job)),
            Some(r) => (r.state, r.current_attempt),
        };
        let mut events = Vec::new();
        if let Some(r) = self.jobs.get_mut(&c.job) {
            // A repeated abort is an accepted no-op: the first request wins.
            if r.spec.abort_requested.is_none() {
                r.spec.abort_requested = Some(AbortRequest {
                    reason: c.reason.clone(),
                    requested_at_us: c.requested_at_us,
                });
            }
        }
        match state {
            // No live attempt: abort is immediate.
            JobState::Submitted | JobState::Accepted | JobState::Queued => {
                self.job_terminal_transition(
                    c.job,
                    JobState::Aborted,
                    c.requested_at_us,
                    &mut events,
                );
            }
            JobState::Preparing | JobState::Running => {
                let attempt_state = current
                    .and_then(|id| self.attempts.get(&id))
                    .map(|a| a.attempt.state.clone());
                match (current, attempt_state) {
                    // Not yet dispatched: no agent interaction, terminate now.
                    (Some(id), Some(AttemptState::Accruing | AttemptState::Ready)) => {
                        self.terminate_attempt(
                            id,
                            AttemptOutcome::Aborted,
                            0,
                            c.requested_at_us,
                            true,
                            &mut events,
                            None,
                        );
                    }
                    // In the agent's hands: signal a StopJob; the outcome
                    // arrives through ingestion and truth wins the race.
                    (Some(id), Some(AttemptState::Dispatching | AttemptState::Running)) => {
                        if let Some(a) = self.attempts.get(&id) {
                            events.push(Event::StopRequested {
                                node: a.attempt.node,
                                allocation: a.attempt.allocation,
                                job: a.attempt.job,
                            });
                        }
                    }
                    _ => {}
                }
            }
            // Resolution honors abort-wins-over-retry from the flag.
            JobState::Finalizing => {}
            _ => {}
        }
        Ok(Applied { events })
    }

    // ---- Scheduler-proposed ----

    fn commit_placements(&mut self, c: &CommitPlacements) -> ApplyResult {
        // Validation. All-or-nothing with per-item diagnostics; item indices
        // cover revocations first, then placements. `expected_version` is an
        // audit record — semantic re-validation is what gates the batch.
        let mut items: Vec<(u32, RejectionReason)> = Vec::new();
        let mut idx: u32 = 0;
        let mut revoked: BTreeSet<AllocationId> = BTreeSet::new();
        for alloc_id in &c.revocations {
            match self.allocations.get(alloc_id) {
                None => items.push((idx, RejectionReason::UnknownAllocation(*alloc_id))),
                Some(r)
                    if r.allocation.state != AllocationState::Accruing
                        || revoked.contains(alloc_id) =>
                {
                    items.push((idx, RejectionReason::AllocationNotAccruing(*alloc_id)));
                }
                Some(_) => {
                    revoked.insert(*alloc_id);
                }
            }
            idx += 1;
        }
        let mut seen_jobs: BTreeSet<JobId> = BTreeSet::new();
        let mut seen_attempts: BTreeSet<AttemptId> = BTreeSet::new();
        let mut seen_allocs: BTreeSet<AllocationId> = BTreeSet::new();
        for p in &c.placements {
            match self.validate_placement(p, &revoked, &seen_jobs, &seen_attempts, &seen_allocs) {
                Some(reason) => items.push((idx, reason)),
                None => {
                    seen_jobs.insert(p.job);
                    seen_attempts.insert(p.attempt);
                    // Validation guarantees exactly one allocation (v1 shape).
                    if let Some(spec) = p.allocations.first() {
                        seen_allocs.insert(spec.id);
                    }
                }
            }
            idx += 1;
        }
        if !items.is_empty() {
            return Err(RejectionReason::InvalidBatch(items));
        }

        // One allocation scan builds the per-node funded-hold memo; every
        // free-capacity read in this batch consults it instead of rescanning,
        // and the batch keeps it current as it frees and seats capacity. This
        // is what makes a target-scale apply O(batch) rather than
        // O(batch × allocations) (KOI-5).
        let mut used = self.used_capacity_memo();

        if let Some(reason) = self.check_accrual_limit(c, &revoked, &used) {
            return Err(reason);
        }

        // Effects.
        let mut events = Vec::new();
        // Revocations first: freed capacity pledges onward in commit order
        // before any new placement sees it.
        for alloc_id in &c.revocations {
            if let Some(attempt) = self.allocations.get(alloc_id).map(|r| r.allocation.attempt) {
                self.terminate_attempt(
                    attempt,
                    AttemptOutcome::Revoked,
                    0,
                    c.proposed_at_us,
                    true,
                    &mut events,
                    Some(&mut used),
                );
            }
        }
        for p in &c.placements {
            // Validation guarantees exactly one allocation (v1 shape).
            let Some(spec) = p.allocations.first() else {
                continue;
            };
            let Some((entity, multiplier, bounded, runtime_s, requests)) =
                self.jobs.get(&p.job).map(|j| {
                    (
                        j.spec.quota_entity,
                        j.multiplier,
                        j.spec.max_runtime_us.is_some(),
                        j.spec
                            .max_runtime_us
                            .map(quota::runtime_seconds_ceil)
                            .unwrap_or(self.policy.default_charge_runtime_s),
                        j.spec.requests,
                    )
                })
            else {
                continue;
            };
            // ADR 0029: a job with no declared bound prices at the elevated
            // rate everywhere (charge, true-up, surcharge) via the folded
            // multiplier, and its synthetic charge refunds in full — the
            // declared-bound retention never applies to the platform's own
            // runtime estimate.
            let (multiplier, refund_fraction_milli) = if bounded {
                (multiplier, self.policy.refund_fraction_milli)
            } else {
                (
                    multiplier.saturating_mul(self.policy.unbounded_runtime_multiplier),
                    quota::FULL_REFUND_MILLI,
                )
            };
            let seq = self.next_allocation_seq;
            self.next_allocation_seq += 1;
            // Whether the allocation starts Funded or Accruing is decided
            // here from actual free capacity. Anything still free after the
            // standing pledge passes is capacity the accrual queue does not
            // want, so pledging it to a new allocation cannot starve the
            // queue.
            let free = self.free_capacity(&spec.node, Some(&used));
            let funded = free.component_min(&spec.requested);
            let fully = funded == spec.requested;
            let alloc_state = if fully {
                AllocationState::Funded
            } else {
                AllocationState::Accruing
            };
            self.allocations.insert(
                spec.id,
                AllocationRecord {
                    allocation: Allocation {
                        id: spec.id,
                        job: p.job,
                        attempt: p.attempt,
                        node: spec.node,
                        requested: spec.requested,
                        funded,
                        state: alloc_state,
                    },
                    seq,
                },
            );
            if !fully {
                self.accrual_queue.insert((spec.node, seq), spec.id);
            }
            // The seat's funded hold now loads the node for the rest of the
            // batch's free-capacity reads.
            Self::memo_add(&mut used, spec.node, &funded);
            // Accruing is skipped entirely when capacity is immediately
            // available (the common case).
            let attempt_state = if fully {
                AttemptState::Ready
            } else {
                AttemptState::Accruing
            };
            let rate = quota::resource_rate(&requests, &self.policy.cost_weights);
            let charge = quota::cost_from_rate(rate, runtime_s, multiplier);
            self.attempts.insert(
                p.attempt,
                AttemptRecord {
                    attempt: Attempt {
                        id: p.attempt,
                        job: p.job,
                        allocation: spec.id,
                        node: spec.node,
                        state: attempt_state.clone(),
                    },
                    group: p.group,
                    charge: ChargeRecord {
                        amount: charge,
                        charged_at_us: c.proposed_at_us,
                        refund_fraction_milli,
                    },
                    rate_ucu_per_second: rate,
                    multiplier,
                    started_at_us: None,
                },
            );
            events.push(Event::AttemptStateChanged {
                attempt: p.attempt,
                job: p.job,
                node: spec.node,
                state: attempt_state,
            });
            if fully {
                events.push(Event::AllocationFunded {
                    allocation: spec.id,
                    job: p.job,
                    node: spec.node,
                });
            }
            if let Some(j) = self.jobs.get_mut(&p.job) {
                j.current_attempt = Some(p.attempt);
                j.attempts.push(p.attempt);
            }
            self.job_transition(p.job, JobState::Preparing, &mut events);
            // Quota charge at placement (ADR 0019); true-up settles against
            // this at terminal resolution using the recorded rate and
            // multiplier.
            self.charge_ancestors(entity, charge, c.proposed_at_us);
        }
        Ok(Applied { events })
    }

    fn validate_placement(
        &self,
        p: &Placement,
        revoked: &BTreeSet<AllocationId>,
        seen_jobs: &BTreeSet<JobId>,
        seen_attempts: &BTreeSet<AttemptId>,
        seen_allocs: &BTreeSet<AllocationId>,
    ) -> Option<RejectionReason> {
        let Some(job) = self.jobs.get(&p.job) else {
            return Some(RejectionReason::UnknownJob(p.job));
        };
        // Queued, or Preparing with its current accrual revoked in this same
        // batch — the revoke-and-reseat re-plan of ADR 0014.
        let reseat = job.state == JobState::Preparing
            && job
                .current_attempt
                .and_then(|id| self.attempts.get(&id))
                .map(|a| revoked.contains(&a.attempt.allocation))
                .unwrap_or(false);
        if (job.state != JobState::Queued && !reseat) || seen_jobs.contains(&p.job) {
            return Some(RejectionReason::JobNotQueued(p.job));
        }
        // v1 shape gate: exactly one allocation, singleton groups keyed by
        // the job id. The plural field is the gang-scheduling seam; until
        // that ADR, other shapes are committed-but-rejected.
        if p.group.0 != p.job.0 || p.allocations.len() != 1 {
            return Some(RejectionReason::UnsupportedPlacementShape);
        }
        let spec = &p.allocations[0];
        if self.attempts.contains_key(&p.attempt) || seen_attempts.contains(&p.attempt) {
            return Some(RejectionReason::DuplicateAttempt(p.attempt));
        }
        if self.allocations.contains_key(&spec.id) || seen_allocs.contains(&spec.id) {
            return Some(RejectionReason::DuplicateAllocation(spec.id));
        }
        let Some(node) = self.nodes.get(&spec.node) else {
            return Some(RejectionReason::UnknownNode(spec.node));
        };
        if !node.node.schedulable {
            return Some(RejectionReason::NodeNotSchedulable(spec.node));
        }
        if !spec.requested.fits_within(&node.node.capacity) {
            return Some(RejectionReason::RequestExceedsNodeCapacity(spec.id));
        }
        if !self.quota_entities.contains_key(&job.spec.quota_entity) {
            return Some(RejectionReason::UnknownQuotaEntity(job.spec.quota_entity));
        }
        None
    }

    /// Simulate the batch and count distinct jobs left holding accruing
    /// allocations.
    ///
    /// Mirrors the pledge arithmetic of the effects phase. A batch may not
    /// grow the accruing set beyond K; a cluster already over the limit
    /// (after a policy change) may keep operating and swap accruals, it
    /// just cannot add more.
    fn check_accrual_limit(
        &self,
        c: &CommitPlacements,
        revoked: &BTreeSet<AllocationId>,
        used: &BTreeMap<NodeId, Resources>,
    ) -> Option<RejectionReason> {
        let mut accruing_jobs: BTreeSet<JobId> = BTreeSet::new();
        for id in self.accrual_queue.values() {
            if revoked.contains(id) {
                continue;
            }
            if let Some(a) = self.allocations.get(id) {
                accruing_jobs.insert(a.allocation.job);
            }
        }
        let before = accruing_jobs.len();

        let mut touched: BTreeSet<NodeId> = BTreeSet::new();
        for id in revoked {
            if let Some(a) = self.allocations.get(id) {
                touched.insert(a.allocation.node);
            }
        }
        for p in &c.placements {
            for spec in &p.allocations {
                touched.insert(spec.node);
            }
        }
        let mut sim_free: BTreeMap<NodeId, Resources> = BTreeMap::new();
        for node in &touched {
            let mut free = self.free_capacity(node, Some(used));
            for id in revoked {
                if let Some(a) = self.allocations.get(id) {
                    if a.allocation.node == *node {
                        free = free.saturating_add(&a.allocation.funded);
                    }
                }
            }
            // Freed capacity flows to the surviving queue in commit order
            // before any new placement sees it.
            for ((_, _), alloc_id) in self.accrual_queue.range((*node, 0)..=(*node, u64::MAX)) {
                if revoked.contains(alloc_id) {
                    continue;
                }
                let Some(rec) = self.allocations.get(alloc_id) else {
                    continue;
                };
                let need = rec
                    .allocation
                    .requested
                    .saturating_sub(&rec.allocation.funded);
                let pledge = free.component_min(&need);
                free = free.saturating_sub(&pledge);
                if pledge == need {
                    accruing_jobs.remove(&rec.allocation.job);
                }
                if free.is_zero() {
                    break;
                }
            }
            sim_free.insert(*node, free);
        }
        for p in &c.placements {
            // Item validation already pinned the v1 single-allocation shape.
            let Some(spec) = p.allocations.first() else {
                continue;
            };
            let Some(free) = sim_free.get_mut(&spec.node) else {
                continue;
            };
            let pledge = free.component_min(&spec.requested);
            *free = free.saturating_sub(&pledge);
            if pledge != spec.requested {
                accruing_jobs.insert(p.job);
            }
        }
        let after = accruing_jobs.len();
        let limit = self.policy.accrual_limit;
        if after > limit as usize && after > before {
            return Some(RejectionReason::AccrualLimitExceeded { limit });
        }
        None
    }

    fn dispatch_attempt(&mut self, c: &DispatchAttempt) -> ApplyResult {
        match self.attempts.get(&c.attempt) {
            None => return Err(RejectionReason::UnknownAttempt(c.attempt)),
            Some(a) if a.attempt.state != AttemptState::Ready => {
                return Err(RejectionReason::StaleAttemptState(c.attempt));
            }
            Some(_) => {}
        }
        let mut events = Vec::new();
        self.attempt_transition(c.attempt, AttemptState::Dispatching, &mut events);
        Ok(Applied { events })
    }

    // ---- Agent ingestion ----

    fn record_attempt_started(&mut self, c: &RecordAttemptStarted) -> ApplyResult {
        match self.attempts.get(&c.attempt) {
            None => return Err(RejectionReason::UnknownAttempt(c.attempt)),
            Some(a) if a.attempt.state != AttemptState::Dispatching => {
                return Err(RejectionReason::StaleAttemptState(c.attempt));
            }
            Some(_) => {}
        }
        let mut events = Vec::new();
        self.mark_attempt_running(c.attempt, c.observed_at_us, &mut events);
        Ok(Applied { events })
    }

    fn record_attempt_exited(&mut self, c: &RecordAttemptExited) -> ApplyResult {
        let job = match self.attempts.get(&c.attempt) {
            None => return Err(RejectionReason::UnknownAttempt(c.attempt)),
            Some(a) if a.attempt.state != AttemptState::Running => {
                return Err(RejectionReason::StaleAttemptState(c.attempt));
            }
            Some(a) => a.attempt.job,
        };
        let mut events = Vec::new();
        self.attempt_transition(c.attempt, AttemptState::Finalizing, &mut events);
        // The one state the job rests in mid-resolution.
        self.job_transition(job, JobState::Finalizing, &mut events);
        Ok(Applied { events })
    }

    fn record_attempt_outcome(&mut self, c: &RecordAttemptOutcome) -> ApplyResult {
        match self.attempts.get(&c.attempt) {
            None => return Err(RejectionReason::UnknownAttempt(c.attempt)),
            Some(a) if a.attempt.state.is_terminal() => {
                return Err(RejectionReason::StaleAttemptState(c.attempt));
            }
            Some(_) => {}
        }
        if c.outcome == AttemptOutcome::Revoked {
            return Err(RejectionReason::InvalidCommand(
                "outcome Revoked is only produced by CommitPlacements".into(),
            ));
        }
        let mut events = Vec::new();
        self.terminate_attempt(
            c.attempt,
            c.outcome.clone(),
            c.actual_runtime_us,
            c.observed_at_us,
            true,
            &mut events,
            None,
        );
        Ok(Applied { events })
    }

    fn reconcile_node(&mut self, c: &ReconcileNode) -> ApplyResult {
        let epoch = match self.nodes.get(&c.node) {
            None => return Err(RejectionReason::UnknownNode(c.node)),
            Some(n) => n.epoch,
        };
        if epoch != c.node_epoch {
            return Err(RejectionReason::StaleNodeEpoch {
                node: c.node,
                current: epoch,
                got: c.node_epoch,
            });
        }
        let mut items: Vec<(u32, RejectionReason)> = Vec::new();
        let mut idx: u32 = 0;
        for id in &c.adopted {
            match self.attempts.get(id) {
                None => items.push((idx, RejectionReason::UnknownAttempt(*id))),
                Some(a) if a.attempt.node != c.node => items.push((
                    idx,
                    RejectionReason::AttemptNotOnNode {
                        attempt: *id,
                        node: c.node,
                    },
                )),
                Some(_) => {}
            }
            idx += 1;
        }
        for l in &c.lost {
            match self.attempts.get(&l.attempt) {
                None => items.push((idx, RejectionReason::UnknownAttempt(l.attempt))),
                Some(a) if a.attempt.node != c.node => items.push((
                    idx,
                    RejectionReason::AttemptNotOnNode {
                        attempt: l.attempt,
                        node: c.node,
                    },
                )),
                Some(_) if l.outcome == AttemptOutcome::Revoked => {
                    items.push((
                        idx,
                        RejectionReason::InvalidCommand(
                            "outcome Revoked is only produced by CommitPlacements".into(),
                        ),
                    ));
                }
                Some(_) => {}
            }
            idx += 1;
        }
        if !items.is_empty() {
            return Err(RejectionReason::InvalidBatch(items));
        }
        let mut events = Vec::new();
        for id in &c.adopted {
            // Adopt = intended and observed running. A missed started report
            // is folded in here; already-running or already-terminal entries
            // are stale info and benign no-ops.
            let dispatching = self
                .attempts
                .get(id)
                .map(|a| a.attempt.state == AttemptState::Dispatching)
                .unwrap_or(false);
            if dispatching {
                self.mark_attempt_running(*id, c.observed_at_us, &mut events);
            }
        }
        for l in &c.lost {
            let live = self
                .attempts
                .get(&l.attempt)
                .map(|a| !a.attempt.state.is_terminal())
                .unwrap_or(false);
            if live {
                self.terminate_attempt(
                    l.attempt,
                    l.outcome.clone(),
                    l.actual_runtime_us,
                    c.observed_at_us,
                    true,
                    &mut events,
                    None,
                );
            }
        }
        Ok(Applied { events })
    }

    // ---- Node lifecycle ----

    fn register_node(&mut self, c: &RegisterNode) -> ApplyResult {
        let mut events = Vec::new();
        match self.nodes.get_mut(&c.node) {
            Some(rec) => {
                // Re-registration: the epoch bump fences every command issued
                // under earlier epochs (ADR 0009). Drain survives — desired
                // state owned by the admin, not the agent's restart.
                rec.epoch += 1;
                rec.node.capacity = c.capacity;
                rec.node.labels = c.labels.clone();
                events.push(Event::NodeEpochBumped {
                    node: c.node,
                    epoch: rec.epoch,
                });
            }
            None => {
                self.nodes.insert(
                    c.node,
                    NodeRecord {
                        node: Node {
                            id: c.node,
                            capacity: c.capacity,
                            labels: c.labels.clone(),
                            schedulable: true,
                        },
                        epoch: 1,
                    },
                );
                events.push(Event::NodeEpochBumped {
                    node: c.node,
                    epoch: 1,
                });
            }
        }
        // Capacity may have grown; fund waiting accruals.
        self.pledge_node(c.node, &mut events, None);
        Ok(Applied { events })
    }

    fn declare_node_lost(&mut self, c: &DeclareNodeLost) -> ApplyResult {
        let mut events = Vec::new();
        match self.nodes.get_mut(&c.node) {
            None => return Err(RejectionReason::UnknownNode(c.node)),
            Some(rec) => {
                rec.epoch += 1;
                rec.node.schedulable = false;
                events.push(Event::NodeEpochBumped {
                    node: c.node,
                    epoch: rec.epoch,
                });
            }
        }
        // Every live attempt on the node ends NodeLost, in commit order. No
        // pledge pass runs on a lost node.
        let mut victims: Vec<(u64, AttemptId)> = self
            .allocations
            .values()
            .filter(|r| {
                r.allocation.node == c.node && r.allocation.state != AllocationState::Released
            })
            .map(|r| (r.seq, r.allocation.attempt))
            .collect();
        victims.sort_unstable_by_key(|(seq, _)| *seq);
        for (_, attempt) in victims {
            let runtime_us = self
                .attempts
                .get(&attempt)
                .and_then(|a| a.started_at_us)
                .map(|s| (c.declared_at_us - s).max(0) as u64)
                .unwrap_or(0);
            self.terminate_attempt(
                attempt,
                AttemptOutcome::NodeLost,
                runtime_us,
                c.declared_at_us,
                false,
                &mut events,
                None,
            );
        }
        Ok(Applied { events })
    }

    fn set_node_schedulable(&mut self, c: &SetNodeSchedulable) -> ApplyResult {
        match self.nodes.get_mut(&c.node) {
            None => Err(RejectionReason::UnknownNode(c.node)),
            Some(rec) => {
                // Drain blocks new placements only: running work continues
                // and existing accruals keep funding.
                rec.node.schedulable = c.schedulable;
                Ok(Applied::default())
            }
        }
    }

    // ---- Housekeeping ----

    fn evict_terminal_jobs(&mut self, c: &EvictTerminalJobs) -> ApplyResult {
        // Missing ids are skipped: duplicate eviction proposals across leader
        // changes must be idempotent. A live listed job is a proposer bug.
        let mut items: Vec<(u32, RejectionReason)> = Vec::new();
        for (i, job) in c.jobs.iter().enumerate() {
            if let Some(r) = self.jobs.get(job) {
                if !r.state.is_terminal() {
                    items.push((i as u32, RejectionReason::JobNotTerminal(*job)));
                }
            }
        }
        if !items.is_empty() {
            return Err(RejectionReason::InvalidBatch(items));
        }
        let mut events = Vec::new();
        for job in &c.jobs {
            let Some(rec) = self.jobs.remove(job) else {
                continue;
            };
            for attempt in &rec.attempts {
                if let Some(a) = self.attempts.remove(attempt) {
                    if let Some(al) = self.allocations.remove(&a.attempt.allocation) {
                        self.accrual_queue.remove(&(al.allocation.node, al.seq));
                    }
                }
            }
            events.push(Event::JobEvicted { job: *job });
        }
        Ok(Applied { events })
    }

    // ---- Admin / policy ----

    fn configure_quota_entity(&mut self, c: &ConfigureQuotaEntity) -> ApplyResult {
        if let Some(parent) = c.parent {
            if parent == c.entity {
                return Err(RejectionReason::QuotaEntityCycle(c.entity));
            }
            if !self.quota_entities.contains_key(&parent) {
                return Err(RejectionReason::UnknownQuotaEntity(parent));
            }
            // Walk up from the parent: reaching the entity is a cycle,
            // exhausting the cap is too deep either way.
            let mut cur = Some(parent);
            let mut rooted = false;
            for _ in 0..QUOTA_TREE_DEPTH_CAP {
                match cur {
                    None => {
                        rooted = true;
                        break;
                    }
                    Some(id) if id == c.entity => break,
                    Some(id) => cur = self.quota_entities.get(&id).and_then(|e| e.parent),
                }
            }
            if !rooted {
                return Err(RejectionReason::QuotaEntityCycle(c.entity));
            }
        }
        match self.quota_entities.get_mut(&c.entity) {
            // Usage is preserved on update: reconfiguration is not an amnesty.
            Some(e) => {
                e.parent = c.parent;
                e.name = c.name.clone();
                e.quota = c.quota;
            }
            None => {
                self.quota_entities.insert(
                    c.entity,
                    QuotaEntity {
                        parent: c.parent,
                        name: c.name.clone(),
                        quota: c.quota,
                        usage: UsageState::new(c.updated_at_us),
                    },
                );
            }
        }
        Ok(Applied {
            events: vec![Event::QuotaEntityConfigured { entity: c.entity }],
        })
    }

    fn update_policy(&mut self, c: &UpdatePolicy) -> ApplyResult {
        if let Err(e) = c.policy.decay.validate() {
            return Err(RejectionReason::InvalidPolicy(e.to_string()));
        }
        // ADR 0029: the unbounded-rate multiplier may only surcharge (≥ 1.0),
        // and the refund fraction is parts-per-thousand of the unused charge.
        if c.policy.unbounded_runtime_multiplier < quota::PriorityMultiplier::ONE {
            return Err(RejectionReason::InvalidPolicy(format!(
                "unbounded_runtime_multiplier {} is below 1.0 ({})",
                c.policy.unbounded_runtime_multiplier.0,
                quota::PriorityMultiplier::ONE.0
            )));
        }
        if c.policy.refund_fraction_milli > quota::FULL_REFUND_MILLI {
            return Err(RejectionReason::InvalidPolicy(format!(
                "refund_fraction_milli {} exceeds maximum {}",
                c.policy.refund_fraction_milli,
                quota::FULL_REFUND_MILLI
            )));
        }
        // In-flight charge records keep their recorded rate and multiplier;
        // decay re-times from each entity's next touch (ADR 0019).
        self.policy = c.policy.clone();
        Ok(Applied {
            events: vec![Event::PolicyUpdated],
        })
    }

    fn bump_cluster_version(&mut self, c: &BumpClusterVersion) -> ApplyResult {
        if c.to <= self.cluster_version {
            return Err(RejectionReason::ClusterVersionNotMonotonic {
                current: self.cluster_version,
                requested: c.to,
            });
        }
        self.cluster_version = c.to;
        Ok(Applied {
            events: vec![Event::ClusterVersionBumped { to: c.to }],
        })
    }

    // ---- Shared effect helpers (infallible; validation happened first) ----

    fn job_transition(&mut self, job: JobId, to: JobState, events: &mut Vec<Event>) {
        if let Some(r) = self.jobs.get_mut(&job) {
            if r.state != to {
                let from = r.state;
                r.state = to;
                events.push(Event::JobStateChanged { job, from, to });
            }
        }
    }

    /// [`job_transition`](Self::job_transition) into a terminal state,
    /// stamping `terminal_at_us` — the timestamp the eviction retention
    /// clock runs from (ADR 0012).
    ///
    /// Callers reject or no-op on already-terminal jobs, so the first stamp
    /// is also the only one; the guard keeps that true even if a new caller
    /// slips.
    fn job_terminal_transition(
        &mut self,
        job: JobId,
        to: JobState,
        ts_us: i64,
        events: &mut Vec<Event>,
    ) {
        if let Some(r) = self.jobs.get_mut(&job) {
            if r.terminal_at_us.is_none() {
                r.terminal_at_us = Some(ts_us);
            }
        }
        self.job_transition(job, to, events);
    }

    fn attempt_transition(
        &mut self,
        attempt: AttemptId,
        to: AttemptState,
        events: &mut Vec<Event>,
    ) {
        if let Some(a) = self.attempts.get_mut(&attempt) {
            if a.attempt.state != to {
                a.attempt.state = to.clone();
                events.push(Event::AttemptStateChanged {
                    attempt,
                    job: a.attempt.job,
                    node: a.attempt.node,
                    state: to,
                });
            }
        }
    }

    fn mark_attempt_running(
        &mut self,
        attempt: AttemptId,
        observed_at_us: i64,
        events: &mut Vec<Event>,
    ) {
        let Some(a) = self.attempts.get_mut(&attempt) else {
            return;
        };
        if a.started_at_us.is_none() {
            a.started_at_us = Some(observed_at_us);
        }
        let (job, allocation) = (a.attempt.job, a.attempt.allocation);
        self.attempt_transition(attempt, AttemptState::Running, events);
        if let Some(al) = self.allocations.get_mut(&allocation) {
            if al.allocation.state == AllocationState::Funded {
                al.allocation.state = AllocationState::Active;
            }
        }
        self.job_transition(job, JobState::Running, events);
    }

    /// The shared terminal path: terminal outcome, allocation release plus
    /// funding cascade, quota true-up, and job resolution — all in one apply
    /// (the `Finalizing` funnel of ADR 0013).
    ///
    /// `pledge` is false only when the node itself is lost. `used` is the
    /// optional batch capacity memo threaded down to the funding cascade; only
    /// `CommitPlacements` (which terminates in a loop) supplies one.
    #[allow(clippy::too_many_arguments)]
    fn terminate_attempt(
        &mut self,
        attempt: AttemptId,
        outcome: AttemptOutcome,
        actual_runtime_us: u64,
        ts_us: i64,
        pledge: bool,
        events: &mut Vec<Event>,
        used: Option<&mut BTreeMap<NodeId, Resources>>,
    ) {
        let Some(a) = self.attempts.get(&attempt) else {
            return;
        };
        if a.attempt.state.is_terminal() {
            return;
        }
        let job = a.attempt.job;
        let allocation = a.attempt.allocation;
        let started = a.started_at_us.is_some();
        let charge = a.charge;
        let rate = a.rate_ucu_per_second;
        let multiplier = a.multiplier;

        self.attempt_transition(attempt, AttemptState::Terminal(outcome.clone()), events);
        self.release_allocation(allocation, pledge, events, used);

        // True-up (ADR 0019): an attempt that never reached Running has
        // actual cost zero — which is exactly what makes revocation requeue
        // free without a special case.
        let actual = if started {
            quota::cost_from_rate(
                rate,
                quota::runtime_seconds_ceil(actual_runtime_us),
                multiplier,
            )
        } else {
            CostUnits::ZERO
        };
        // Retention (ADR 0029) applies only when the attempt ran and its end
        // is the user's own — never to platform outcomes (Revoked, NodeLost,
        // …), so requeue and platform-fault retries stay free of it.
        let retain = started && outcome.class() != OutcomeClass::Platform;
        let decay = self.policy.decay;
        let adjustment = quota::true_up(&charge, actual, ts_us, &decay, retain);
        if let Some(entity) = self.jobs.get(&job).map(|j| j.spec.quota_entity) {
            self.settle_ancestors(entity, adjustment, ts_us);
        }

        self.resolve_job(job, &outcome, ts_us, events);
    }

    fn release_allocation(
        &mut self,
        allocation: AllocationId,
        pledge: bool,
        events: &mut Vec<Event>,
        mut used: Option<&mut BTreeMap<NodeId, Resources>>,
    ) {
        let Some(rec) = self.allocations.get_mut(&allocation) else {
            return;
        };
        if rec.allocation.state == AllocationState::Released {
            return;
        }
        let node = rec.allocation.node;
        let seq = rec.seq;
        let funded = rec.allocation.funded;
        rec.allocation.state = AllocationState::Released;
        self.accrual_queue.remove(&(node, seq));
        // The freed hold returns to the node's capacity for the rest of the
        // batch, then the pledge pass may hand some of it to waiting accruals.
        if let Some(memo) = used.as_deref_mut() {
            Self::memo_sub(memo, node, &funded);
        }
        if pledge {
            self.pledge_node(node, events, used);
        }
    }

    /// Resolve the job after its attempt reached a terminal outcome.
    ///
    /// `ts_us` is the resolving command's proposer timestamp; it becomes the
    /// job's `terminal_at_us` when resolution lands terminal (a requeue
    /// leaves the field `None`).
    fn resolve_job(
        &mut self,
        job: JobId,
        outcome: &AttemptOutcome,
        ts_us: i64,
        events: &mut Vec<Event>,
    ) {
        let Some(rec) = self.jobs.get(&job) else {
            return;
        };
        if rec.state.is_terminal() {
            return;
        }
        let abort_pending = rec.spec.abort_requested.is_some();
        let retries_used = rec.retries_used;
        let retry = rec.spec.retry;

        // Every attempt end funnels through Finalizing, even when resolution
        // completes within this same apply.
        self.job_transition(job, JobState::Finalizing, events);

        enum Resolution {
            Terminal(JobState),
            Requeue { consume_budget: bool },
        }
        let resolution = match outcome {
            // Truth wins the race: Aborted only when the abort mechanism
            // actually terminated the attempt.
            AttemptOutcome::Aborted => Resolution::Terminal(JobState::Aborted),
            o if o.class() == OutcomeClass::Success => Resolution::Terminal(JobState::Succeeded),
            // Revoked requeues free of retry budget — unless an abort is
            // pending, which always wins over any requeue (unreachable via
            // the ordered log; specified for completeness).
            AttemptOutcome::Revoked => {
                if abort_pending {
                    Resolution::Terminal(JobState::Aborted)
                } else {
                    Resolution::Requeue {
                        consume_budget: false,
                    }
                }
            }
            // Deterministic recurrence: never retried, opt-in does not apply.
            AttemptOutcome::MaxRuntimeExceeded => Resolution::Terminal(JobState::Failed),
            o => {
                let eligible = match o.class() {
                    OutcomeClass::Platform => true,
                    OutcomeClass::UserError => retry.retry_user_errors,
                    _ => false,
                };
                // Abort wins over retry: once requested, never back to Queued.
                if eligible && !abort_pending && retries_used < retry.max_retries {
                    Resolution::Requeue {
                        consume_budget: true,
                    }
                } else {
                    Resolution::Terminal(JobState::Failed)
                }
            }
        };
        match resolution {
            Resolution::Terminal(to) => {
                if let Some(r) = self.jobs.get_mut(&job) {
                    r.current_attempt = None;
                }
                self.job_terminal_transition(job, to, ts_us, events);
            }
            Resolution::Requeue { consume_budget } => {
                if let Some(r) = self.jobs.get_mut(&job) {
                    r.current_attempt = None;
                    if consume_budget {
                        r.retries_used += 1;
                    }
                }
                self.job_transition(job, JobState::Queued, events);
            }
        }
    }

    /// Advertised capacity minus the funded holds of the node's live
    /// (non-Released) allocations.
    ///
    /// `used` is an optional per-node funded-hold memo (see
    /// [`used_capacity_memo`](Self::used_capacity_memo)). A `CommitPlacements`
    /// batch builds one up front and threads it through every free-capacity
    /// read — the accrual-limit check, each revocation's funding cascade, and
    /// each placement's seat decision — so the whole batch costs one
    /// allocation scan rather than one per item, which at target scale was the
    /// difference between milliseconds and tens of seconds (KOI-5). Callers
    /// outside a batch pass `None` and take the direct scan.
    fn free_capacity(
        &self,
        node: &NodeId,
        used: Option<&BTreeMap<NodeId, Resources>>,
    ) -> Resources {
        let Some(rec) = self.nodes.get(node) else {
            return Resources::ZERO;
        };
        let used_here = match used {
            Some(memo) => memo.get(node).copied().unwrap_or(Resources::ZERO),
            None => {
                let mut u = Resources::ZERO;
                for a in self.allocations.values() {
                    if a.allocation.node == *node && a.allocation.state != AllocationState::Released
                    {
                        u = u.saturating_add(&a.allocation.funded);
                    }
                }
                u
            }
        };
        rec.node.capacity.saturating_sub(&used_here)
    }

    /// The funded holds of every node's live allocations, in one allocation
    /// scan — the memo `free_capacity` reads during a `CommitPlacements`
    /// batch. Purely derived, never stored on the state: it lives only for the
    /// duration of one `commit_placements`, which maintains it as it frees and
    /// seats capacity (`memo_sub` / `memo_add`).
    fn used_capacity_memo(&self) -> BTreeMap<NodeId, Resources> {
        let mut used: BTreeMap<NodeId, Resources> = BTreeMap::new();
        for r in self.allocations.values() {
            if r.allocation.state != AllocationState::Released {
                let e = used.entry(r.allocation.node).or_insert(Resources::ZERO);
                *e = e.saturating_add(&r.allocation.funded);
            }
        }
        used
    }

    /// Raise a node's funded holds in the batch memo — a fresh seat or an
    /// accrual pledge, both of which the batch must see on later reads.
    fn memo_add(used: &mut BTreeMap<NodeId, Resources>, node: NodeId, amount: &Resources) {
        let e = used.entry(node).or_insert(Resources::ZERO);
        *e = e.saturating_add(amount);
    }

    /// Return a released allocation's funded holds to free capacity in the
    /// batch memo.
    fn memo_sub(used: &mut BTreeMap<NodeId, Resources>, node: NodeId, amount: &Resources) {
        if let Some(e) = used.get_mut(&node) {
            *e = e.saturating_sub(amount);
        }
    }

    /// One pledge pass: free capacity on a node flows to its accruing
    /// allocations in commit (`seq`) order — never id order (ADR 0014).
    ///
    /// The head takes what it needs of each dimension; dimensions it does
    /// not need flow past it.
    fn pledge_node(
        &mut self,
        node: NodeId,
        events: &mut Vec<Event>,
        mut used: Option<&mut BTreeMap<NodeId, Resources>>,
    ) {
        let mut free = self.free_capacity(&node, used.as_deref());
        if free.is_zero() {
            return;
        }
        let queue: Vec<(u64, AllocationId)> = self
            .accrual_queue
            .range((node, 0)..=(node, u64::MAX))
            .map(|((_, seq), id)| (*seq, *id))
            .collect();
        let mut newly_funded: Vec<AllocationId> = Vec::new();
        for (seq, alloc_id) in queue {
            if free.is_zero() {
                break;
            }
            let Some(rec) = self.allocations.get_mut(&alloc_id) else {
                continue;
            };
            let need = rec
                .allocation
                .requested
                .saturating_sub(&rec.allocation.funded);
            let pledge = free.component_min(&need);
            if pledge.is_zero() {
                continue;
            }
            rec.allocation.funded = rec.allocation.funded.saturating_add(&pledge);
            free = free.saturating_sub(&pledge);
            if rec.allocation.funded == rec.allocation.requested {
                rec.allocation.state = AllocationState::Funded;
                let job = rec.allocation.job;
                self.accrual_queue.remove(&(node, seq));
                events.push(Event::AllocationFunded {
                    allocation: alloc_id,
                    job,
                    node,
                });
                newly_funded.push(alloc_id);
            }
            // The pledge stays on the same node and allocation; only the
            // funded total grew, so the node's used capacity rises by the
            // pledge for the rest of the batch.
            if let Some(memo) = used.as_deref_mut() {
                Self::memo_add(memo, node, &pledge);
            }
        }
        for alloc_id in newly_funded {
            if let Some(attempt) = self
                .allocations
                .get(&alloc_id)
                .map(|r| r.allocation.attempt)
            {
                self.check_ready_barrier(attempt, events);
            }
        }
    }

    /// The `Ready` barrier: an AND over the placement group's live attempts.
    ///
    /// v1 groups are singletons, but the evaluation is group-shaped from day
    /// one so gang scheduling adds members, not mechanism.
    fn check_ready_barrier(&mut self, attempt: AttemptId, events: &mut Vec<Event>) {
        let Some(group) = self.attempts.get(&attempt).map(|a| a.group) else {
            return;
        };
        let mut members: Vec<AttemptId> = Vec::new();
        for (id, a) in &self.attempts {
            if a.group == group && !a.attempt.state.is_terminal() {
                members.push(*id);
            }
        }
        let all_funded = members.iter().all(|id| {
            self.attempts
                .get(id)
                .and_then(|a| self.allocations.get(&a.attempt.allocation))
                .map(|r| {
                    matches!(
                        r.allocation.state,
                        AllocationState::Funded | AllocationState::Active
                    )
                })
                .unwrap_or(false)
        });
        if !all_funded {
            return;
        }
        for id in members {
            let accruing = self
                .attempts
                .get(&id)
                .map(|a| a.attempt.state == AttemptState::Accruing)
                .unwrap_or(false);
            if accruing {
                self.attempt_transition(id, AttemptState::Ready, events);
            }
        }
    }

    /// Charge every ancestor on the entity's path (ADR 0005).
    ///
    /// The walk is depth-capped so even a corrupted parent chain stays
    /// bounded.
    fn charge_ancestors(&mut self, entity: QuotaEntityId, amount: CostUnits, ts_us: i64) {
        let decay = self.policy.decay;
        let mut cur = Some(entity);
        for _ in 0..QUOTA_TREE_DEPTH_CAP {
            let Some(id) = cur else { break };
            let Some(e) = self.quota_entities.get_mut(&id) else {
                break;
            };
            e.usage.charge(amount, ts_us, &decay);
            cur = e.parent;
        }
    }

    fn settle_ancestors(&mut self, entity: QuotaEntityId, adjustment: TrueUp, ts_us: i64) {
        let decay = self.policy.decay;
        let mut cur = Some(entity);
        for _ in 0..QUOTA_TREE_DEPTH_CAP {
            let Some(id) = cur else { break };
            let Some(e) = self.quota_entities.get_mut(&id) else {
                break;
            };
            e.usage.settle(adjustment, ts_us, &decay);
            cur = e.parent;
        }
    }
}

/// Whether a resubmitted spec is the same logical submission as the
/// committed one (ADR 0026).
///
/// Compares every client-supplied field. `abort_requested` is excluded: it
/// is apply-owned after commit (an `AbortJob` may have set it since), and
/// `SubmitJob` validation already rejects a command that arrives with it
/// pre-set. The command's `multiplier` and `submitted_at_us` are likewise
/// not identity: a retry re-stamps both, and the original commit's values
/// stay authoritative.
fn same_submission(existing: &Job, retried: &Job) -> bool {
    existing.id == retried.id
        && existing.image == retried.image
        && existing.command == retried.command
        && existing.entrypoint == retried.entrypoint
        && existing.requests == retried.requests
        && existing.priority == retried.priority
        && existing.max_runtime_us == retried.max_runtime_us
        && existing.quota_entity == retried.quota_entity
        && existing.retry == retried.retry
}
