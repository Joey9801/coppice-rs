//! The self-converging membership loop (ADR 0037 §1/§4).
//!
//! Every coordinator runs this loop against the cluster itself, as a *client* of
//! the admin surface, using its own machine certificate (dialed through the
//! shared [`TlsStore`]). It replaces the operator's hand-driven add-learner /
//! promote dance: a new instance discovers the cluster, joins as a learner, and
//! promotes itself once caught up — and a restart re-enters the same loop, which
//! no-ops when this identity is already a caught-up voter.
//!
//! The loop is deliberately **seed-driven**: it only ever acts on candidates
//! returned by [`Discovery`] (ADR 0037 §2). With an empty discovery view it
//! reports `waiting`/`learner` and takes no action — discovery being stale,
//! partial, or empty can delay convergence but can never wedge it, and it can
//! never drive a membership change on its own. The membership verbs it calls are
//! idempotent by contract (ADR 0037 §4), so a process killed at any step
//! converges after respawn with no cleanup.
//!
//! The published [`ConvergenceStatus`] (a `watch`) is the machine-readable phase
//! the later `/readyz` package consumes; this module only produces it.

use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::Code;

use coppice_consensus::{CoordinatorId, MembershipPolicy, NodeHandle};
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::TlsStore;

use crate::admin::admin_channel_from_store;
use crate::discovery::Discovery;

/// The convergence phase of one replica (ADR 0037 §4/§7). Exactly the five
/// states the ADR names; the later `/readyz` package maps them to HTTP status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Parked: no initialized cluster found in discovery, no formation yet.
    Waiting,
    /// A cluster was found; this replica is driving `AddLearner` against it.
    Joining,
    /// Admitted as a learner; catching up before it can be promoted.
    Learner,
    /// A different, still-live pending learner holds this machine identity's
    /// replacement seat (ADR 0037 §6); watching without resubmitting.
    SeatConflict,
    /// This replica is a voter in the cluster — the converged terminal state.
    Voter,
}

/// The machine-readable convergence status published through a `watch`
/// (ADR 0037 §7). Reachable from the runtime/control-plane plumbing so the
/// `/readyz` package can render it; this package only produces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvergenceStatus {
    /// The current convergence phase.
    pub phase: Phase,
    /// The cluster this replica is stamped for.
    pub cluster_uuid: [u8; 16],
    /// This replica's raft identity once it is bound into membership; `None`
    /// while still parked (ADR 0037 §3 — a parked replica advertises no id).
    pub node_id: Option<CoordinatorId>,
}

/// Everything the convergence loop needs to drive itself against the cluster.
pub struct Convergence {
    /// This replica's own admin handle, for reading local membership state.
    pub handle: NodeHandle,
    /// This replica's minted raft identity (ADR 0025).
    pub node_id: CoordinatorId,
    /// The `host:port` this replica advertises to peers.
    pub advertise_addr: String,
    /// The cluster this replica is stamped for.
    pub cluster_uuid: [u8; 16],
    /// The discovery backend that seeds candidate addresses (ADR 0037 §2).
    pub discovery: Arc<dyn Discovery>,
    /// The shared mTLS store; the loop dials the admin surface through it,
    /// presenting this daemon's own machine certificate (ADR 0037 §6).
    pub tls: Arc<TlsStore>,
    /// Node-local membership policy (grace periods; ADR 0037 §5/§6).
    pub policy: MembershipPolicy,
}

/// Base re-probe cadence while converging (ADR 0037 §1: "a few seconds",
/// shortened here so tests and dev converge briskly; jitter is added per tick).
const PROBE_INTERVAL: Duration = Duration::from_millis(300);
/// Slow cadence once converged to a voter: nothing to do but observe.
const SETTLED_INTERVAL: Duration = Duration::from_secs(3);

/// Spawn the convergence loop. Returns the published status watch (for
/// `/readyz`) and the task handle; the loop runs until the handle is aborted at
/// shutdown (it holds only client dials and a watch sender, so cancellation at
/// any await is safe — the membership verbs it drives are idempotent).
pub fn spawn(convergence: Convergence) -> (watch::Receiver<ConvergenceStatus>, JoinHandle<()>) {
    let (status_tx, status_rx) = watch::channel(ConvergenceStatus {
        phase: Phase::Waiting,
        cluster_uuid: convergence.cluster_uuid,
        node_id: None,
    });

    let join = tokio::spawn(async move {
        let mut tick: u64 = 0;
        loop {
            let wait = convergence.step(&status_tx).await;
            tick = tick.wrapping_add(1);
            let jitter = Duration::from_millis((convergence.node_id.wrapping_add(tick)) % 250);
            tokio::time::sleep(wait + jitter).await;
        }
    });

    (status_rx, join)
}

/// One outcome of an attempt to join through a discovered leader.
enum JoinStep {
    /// Promotion succeeded: this replica is now a voter.
    Promoted,
    /// Admitted (or already) a learner, still catching up.
    Learner,
    /// This machine identity's replacement seat is held by a live pending
    /// learner (ADR 0037 §6); back off `replacement_grace` before retrying.
    SeatConflict,
    /// A retryable failure (not leader, election in flight, unreachable): the
    /// outer loop re-probes and retries.
    Retry,
    /// A terminal refusal (e.g. same id at a different address — a moved
    /// instance is a new instance, ADR 0037 §4). Park and surface it.
    Terminal,
}

impl Convergence {
    /// Run one convergence tick; publish the resulting phase and return how long
    /// to wait before the next tick.
    async fn step(&self, status_tx: &watch::Sender<ConvergenceStatus>) -> Duration {
        let summary = self.handle.cluster_summary();
        let me = summary.members.iter().find(|m| m.id == self.node_id);

        // Already a caught-up voter: the converged terminal state. Re-running
        // the loop here is a no-op (ADR 0037 §1) — just observe at a slow
        // cadence so a later membership change is still noticed.
        if me.is_some_and(|m| m.voter) {
            self.publish(status_tx, Phase::Voter, Some(self.node_id));
            return SETTLED_INTERVAL;
        }

        // Find the cluster to converge against — seed-driven only (ADR 0037 §2).
        let candidates = self.discovery.candidates().await;
        let leader_addr = self.find_leader(&candidates).await;
        let Some(leader_addr) = leader_addr else {
            // No initialized cluster visible in discovery. If we are already a
            // learner (someone else admitted us, or discovery went dark after we
            // joined) report `learner`; otherwise we are parked.
            let node_id = me.map(|_| self.node_id);
            self.publish(
                status_tx,
                if me.is_some() {
                    Phase::Learner
                } else {
                    Phase::Waiting
                },
                node_id,
            );
            return PROBE_INTERVAL;
        };

        // We have a leader to converge through.
        self.publish(
            status_tx,
            if me.is_some() {
                Phase::Learner
            } else {
                Phase::Joining
            },
            Some(self.node_id),
        );

        match self.attempt_join(&leader_addr).await {
            JoinStep::Promoted => {
                self.publish(status_tx, Phase::Voter, Some(self.node_id));
                SETTLED_INTERVAL
            }
            JoinStep::Learner => {
                self.publish(status_tx, Phase::Learner, Some(self.node_id));
                PROBE_INTERVAL
            }
            JoinStep::SeatConflict => {
                // Watch-without-resubmitting: don't retry while the incumbent is
                // live; the leader's stale-learner eviction (ADR 0037 §6) admits
                // us after `replacement_grace` if the incumbent goes stale.
                self.publish(status_tx, Phase::SeatConflict, Some(self.node_id));
                self.policy.replacement_grace
            }
            JoinStep::Retry => {
                self.publish(
                    status_tx,
                    if me.is_some() {
                        Phase::Learner
                    } else {
                        Phase::Joining
                    },
                    Some(self.node_id),
                );
                PROBE_INTERVAL
            }
            JoinStep::Terminal => {
                self.publish(status_tx, Phase::Waiting, Some(self.node_id));
                SETTLED_INTERVAL
            }
        }
    }

    /// Probe discovered candidates and return the dial address of a leader of an
    /// initialized cluster with our `cluster_uuid`, if any (ADR 0037 §3/§4).
    async fn find_leader(&self, candidates: &[String]) -> Option<String> {
        for candidate in candidates {
            let Ok(mut client) = admin_channel_from_store(candidate, &self.tls).await else {
                continue; // unreachable candidate is simply skipped (ADR 0037 §3)
            };
            let Ok(resp) = client.probe_cluster(pb::ProbeClusterRequest {}).await else {
                continue;
            };
            let resp = resp.into_inner();
            if !resp.initialized || resp.cluster_uuid != self.cluster_uuid.to_vec() {
                continue;
            }
            // Prefer the hinted leader's advertised address from the voter set;
            // fall back to the candidate itself (a voter) — `AddLearner` there
            // will name the real leader, and the next tick re-probes to it.
            if let Some(hint) = resp.leader_hint {
                if let Some(v) = resp.voters.iter().find(|v| v.node_id == hint) {
                    return Some(v.address.clone());
                }
            }
            return Some(candidate.clone());
        }
        None
    }

    /// Drive one add-learner → (catch-up) → promote pass against `leader_addr`
    /// (ADR 0037 §4). The idempotent verbs mean each pass is safe to repeat; the
    /// outer loop's cadence is the catch-up poll.
    async fn attempt_join(&self, leader_addr: &str) -> JoinStep {
        let Ok(mut client) = admin_channel_from_store(leader_addr, &self.tls).await else {
            return JoinStep::Retry;
        };

        // Step 2: AddLearner (idempotent). Decode the machine-readable refusal so
        // a seat conflict backs off rather than hammering (ADR 0037 §6).
        match client
            .add_learner(pb::AddLearnerRequest {
                cluster_uuid: self.cluster_uuid.to_vec(),
                node_id: self.node_id,
                address: self.advertise_addr.clone(),
            })
            .await
        {
            Ok(_) => {}
            Err(status) => return classify_add_learner(&status),
        }

        // Steps 3+4: promote when caught up. The server checks the lag gate and
        // returns a retryable "behind" status while the learner is still
        // catching up; treat that as `Learner` and let the next tick re-poll.
        match client
            .promote_voter(pb::PromoteVoterRequest {
                cluster_uuid: self.cluster_uuid.to_vec(),
                promote_node_id: self.node_id,
                remove_node_id: None,
            })
            .await
        {
            Ok(_) => JoinStep::Promoted,
            Err(status) if is_learner_behind(&status) => JoinStep::Learner,
            Err(status) if status.code() == Code::FailedPrecondition => JoinStep::Retry,
            Err(_) => JoinStep::Retry,
        }
    }

    fn publish(
        &self,
        status_tx: &watch::Sender<ConvergenceStatus>,
        phase: Phase,
        node_id: Option<CoordinatorId>,
    ) {
        let next = ConvergenceStatus {
            phase,
            cluster_uuid: self.cluster_uuid,
            node_id,
        };
        // Only send on change to keep the watch quiet; ignore a closed receiver.
        if *status_tx.borrow() != next {
            let _ = status_tx.send(next);
        }
    }
}

/// Classify an `AddLearner` failure into a convergence step (ADR 0037 §6): a
/// machine-seat-pending refusal backs off; a same-id-different-address refusal
/// is terminal; everything else is retryable.
fn classify_add_learner(status: &tonic::Status) -> JoinStep {
    if status.code() == Code::FailedPrecondition {
        if let Ok(refusal) = pb::MembershipRefusal::decode(status.details()) {
            match refusal.reason {
                Some(pb::membership_refusal::Reason::MachineSeatPending(_)) => {
                    return JoinStep::SeatConflict;
                }
                Some(pb::membership_refusal::Reason::SameIdDifferentAddress(_)) => {
                    return JoinStep::Terminal;
                }
                _ => {}
            }
        }
    }
    JoinStep::Retry
}

/// Whether a promotion failure is the retryable "learner still catching up"
/// case (the server's `LearnerNotCaughtUp`, whose message contains "behind").
/// The promotion-lag threshold itself (`PROMOTION_LAG_MAX`) is applied
/// server-side; the loop just re-polls on this signal.
fn is_learner_behind(status: &tonic::Status) -> bool {
    status.code() == Code::FailedPrecondition && status.message().contains("behind")
}
