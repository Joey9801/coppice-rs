//! Admission on a draining node: the horizon, the deadline drain's notice
//! time, and bounded backfill (ADR 0044 §5).
//!
//! A node under an operator's regular drain keeps doing useful work for as
//! long as that work is guaranteed gone in time. "In time" is the node's
//! **horizon** `H`: the latest *enforced* completion bound among its live
//! commitments. A job with an enforced `max_runtime` whose bound
//! `now + max_runtime + abort_grace` lands at or before `H` is admitted as
//! *backfill*, and its attempt records `H` as its cutoff — so backfill can
//! never move `H` later, and the drain's worst-case emptying time is the one
//! it had when it began.
//!
//! A *deadline* drain empties the node by its deadline `D` whatever runs
//! there, so its cutoff is not the horizon but `T_notice` (see
//! [`PolicyConfig::deadline_drain_times`]): backfill admitted by then is gone
//! before notices would go out, and admission closes at `T_notice` by
//! construction.
//!
//! Everything here is a pure function of the snapshot, computed where it is
//! needed (apply's placement validation, the scheduler's pass, the read
//! model's drain status) and never stored on the [`StateMachine`]: the
//! horizon is derived state, and derived state does not get replicated.

use std::collections::{BTreeMap, BTreeSet};

use coppice_core::allocation::AllocationState;
use coppice_core::id::NodeId;
use coppice_core::job::Job;
use coppice_core::time::{Duration, Timestamp};

use crate::{AllocationRecord, NodeRecord, PolicyConfig, StateMachine};

/// A node's horizon `H` (ADR 0044 §5's table): the latest enforced
/// completion bound among its live commitments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeHorizon {
    /// The node holds no live allocation. Under a drain this is the derived
    /// "drain complete" status.
    Empty,
    /// Some live commitment has no enforced bound — a running attempt with no
    /// `max_runtime` and no acknowledged termination, an attempt not yet started that is not drain backfill,
    /// or an accrual. Nothing bounds when the node empties, so nothing is
    /// admitted as backfill.
    Unbounded,
    /// Every live commitment is bounded; the latest bound.
    Bounded(Timestamp),
}

impl NodeHorizon {
    /// Fold one commitment's bound (`None` = unbounded) into the horizon.
    fn absorb(self, bound: Option<Timestamp>) -> NodeHorizon {
        match (self, bound) {
            (NodeHorizon::Unbounded, _) | (_, None) => NodeHorizon::Unbounded,
            (NodeHorizon::Empty, Some(b)) => NodeHorizon::Bounded(b),
            (NodeHorizon::Bounded(h), Some(b)) => NodeHorizon::Bounded(h.max(b)),
        }
    }
}

/// What a node admits, before any particular job is asked about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeAdmission {
    /// No drain by either flag: any placement the other gates allow.
    Open,
    /// Under the operator's drain: bounded backfill that completes by
    /// `cutoff` — a regular drain's bounded horizon, or a deadline drain's
    /// `T_notice`.
    Backfill { cutoff: Timestamp },
    /// Admits nothing: the agent announced it is leaving, or the node is
    /// under a regular drain and its horizon is empty or unbounded.
    Closed,
}

/// The verdict for one job on one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// An ordinary placement.
    Open,
    /// Drain backfill: must be fully funded at once, and its attempt records
    /// `cutoff` (the node's horizon, or a deadline drain's `T_notice`) as its
    /// completion bound.
    Backfill { cutoff: Timestamp },
    /// Not admitted — `RejectionReason::NodeDraining`.
    Refused,
}

impl NodeAdmission {
    /// Whether `job` is admitted at `now`. Backfill needs an enforced
    /// `max_runtime` whose bound, padded by the stop grace the agent would
    /// need to end it, lands at or before the cutoff.
    pub fn admits(&self, job: &Job, now: Timestamp, abort_grace: Duration) -> Admission {
        match *self {
            NodeAdmission::Open => Admission::Open,
            NodeAdmission::Closed => Admission::Refused,
            NodeAdmission::Backfill { cutoff } => match job.max_runtime {
                Some(runtime)
                    if now.saturating_add(runtime).saturating_add(abort_grace) <= cutoff =>
                {
                    Admission::Backfill { cutoff }
                }
                _ => Admission::Refused,
            },
        }
    }
}

impl NodeRecord {
    /// Whether the node is being emptied, by the operator's drain record or
    /// by the agent's own announcement (ADR 0044 §5 amending ADR 0041).
    /// Retention GC and the liveness monitor ask this; display surfaces that
    /// report either flag on its own read `Node::drain` or
    /// [`draining`](Self::draining) directly.
    pub fn is_draining(&self) -> bool {
        self.node.drain.is_some() || self.draining
    }

    /// What this node admits (ADR 0044 §5). A deadline drain's cutoff is its
    /// `T_notice` under `policy`, regardless of the horizon. `horizon` is
    /// asked only for a node under the operator's *regular* drain that its
    /// agent has not announced leaving — the one case it decides — so a
    /// caller can compute it lazily.
    pub fn admission(
        &self,
        policy: &PolicyConfig,
        horizon: impl FnOnce() -> NodeHorizon,
    ) -> NodeAdmission {
        if self.draining {
            return NodeAdmission::Closed;
        }
        let Some(drain) = self.node.drain else {
            return NodeAdmission::Open;
        };
        if let Some(deadline) = drain.deadline {
            return NodeAdmission::Backfill {
                cutoff: policy.deadline_drain_times(deadline).t_notice,
            };
        }
        match horizon() {
            NodeHorizon::Bounded(cutoff) => NodeAdmission::Backfill { cutoff },
            NodeHorizon::Empty | NodeHorizon::Unbounded => NodeAdmission::Closed,
        }
    }

    /// The placement gate both apply and the scheduler call (ADR 0044 §5,
    /// replacing ADR 0041's `accepts_placements`). Free-capacity fit and the
    /// spot-only rule are checked separately by each caller.
    pub fn admits(
        &self,
        job: &Job,
        now: Timestamp,
        policy: &PolicyConfig,
        horizon: impl FnOnce() -> NodeHorizon,
    ) -> Admission {
        self.admission(policy, horizon)
            .admits(job, now, policy.abort_grace)
    }
}

impl StateMachine {
    /// The horizon of one node — see [`node_horizons`](Self::node_horizons).
    /// One allocation scan.
    pub fn node_horizon(&self, node: NodeId) -> NodeHorizon {
        self.node_horizons(&BTreeSet::from([node]))
            .remove(&node)
            .unwrap_or(NodeHorizon::Empty)
    }

    /// The horizon of every node in `nodes`, in one scan of the allocation
    /// map — a handler-scoped memo the caller builds once per batch or pass
    /// and throws away. Every listed node appears in the result.
    pub fn node_horizons(&self, nodes: &BTreeSet<NodeId>) -> BTreeMap<NodeId, NodeHorizon> {
        let mut out: BTreeMap<NodeId, NodeHorizon> =
            nodes.iter().map(|n| (*n, NodeHorizon::Empty)).collect();
        if nodes.is_empty() {
            return out;
        }
        for record in self.allocations.values() {
            let Some(h) = out.get_mut(&record.allocation.node) else {
                continue;
            };
            if record.allocation.state == AllocationState::Released {
                continue;
            }
            *h = h.absorb(self.commitment_bound(record));
        }
        out
    }

    /// One live allocation's enforced completion bound (ADR 0044 §5's
    /// table), or `None` when nothing bounds it.
    fn commitment_bound(&self, record: &AllocationRecord) -> Option<Timestamp> {
        // An accrual has not even been funded, so nothing bounds its start.
        if record.allocation.state == AllocationState::Accruing {
            return None;
        }
        let attempt = self.attempts.get(&record.allocation.attempt)?;
        let max_runtime = self
            .jobs
            .get(&record.allocation.job)
            .and_then(|j| j.spec.max_runtime);
        let grace = self.policy.abort_grace;
        match attempt.started_at {
            // Not yet started: only drain backfill is bounded, by the cutoff
            // it was admitted under — the agent refuses a launch that would
            // overrun it.
            None => attempt.drain_cutoff,
            Some(started) => {
                // `started_at` is the coordinator's observation, later than
                // the launch by the report latency; a backfill's cutoff stays
                // the tighter bound, so backfill never moves `H` later.
                let running = max_runtime.map(|m| started.saturating_add(m).saturating_add(grace));
                // An acknowledged termination (ADR 0044 §3) ends the attempt
                // by its notice deadline plus the stop grace, with or
                // without a `max_runtime` — the same bound the scheduler's
                // release events use. It only ever tightens the bound.
                let preempted = attempt
                    .preemption
                    .and_then(|p| p.acknowledged)
                    .map(|ack| ack.deadline.saturating_add(grace));
                [running, preempted, attempt.drain_cutoff]
                    .into_iter()
                    .flatten()
                    .min()
            }
        }
    }
}
