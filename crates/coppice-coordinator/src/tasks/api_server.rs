//! API server (every replica).
//!
//! Implements `coppice_api::ControlPlane` by proposing through consensus —
//! the write path is `Consensus::propose`, mapping an `Applied.outcome`
//! rejection to a user-facing error
//! (`docs/architecture/coordinator-runtime.md`, "API server"). This runs on
//! every replica, including followers: a follower still accepts requests and
//! maps `ConsensusError::NotLeader` to a redirect, per the trait's contract.
//!
//! The HTTP transport is `coppice_api::http` (axum, ADR 0031): [`run`]
//! serves that router over the bound `listen.client_addr` listener, with
//! this file owning only the `ControlPlane` implementation behind it.

use std::sync::Arc;

use tokio::sync::watch;

use coppice_api::http::dto::{AbortJobRequest, SubmitJobRequest, SubmitJobResponse};
use coppice_api::{
    ApiError, Consistency, ControlPlane, QueueWindow, ReadOptions, ReadView, RecentClusterEvents,
    StampedEvent,
};
use coppice_consensus::{Applied, Consensus, ConsensusError, StateViews};
use coppice_core::id::ClusterId;
use coppice_core::job::Job;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::command::{AbortJob, SubmitJob};
use coppice_state::Command;

use crate::tasks::event_fanout::FanoutHandle;

/// Implements [`ControlPlane`] by proposing through the consensus seam.
#[allow(dead_code)] // fields are read by submit_job/abort_job, exercised in tests below.
pub struct CoordinatorControlPlane<C> {
    consensus: Arc<C>,
    views: StateViews,
    /// This replica's cluster identity, from node config (ADR 0020). Not
    /// replicated state — a replica knows it before it applies anything —
    /// so reads that report it (`GET /api/v1/overview`) take it from here.
    cluster_id: ClusterId,
    /// The derived-stats task's published window (ADR 0032, tier 3).
    /// Empty until [`with_derived`](Self::with_derived) attaches the real
    /// watch — an honest "no coverage", which is also what tests that never
    /// spawn the task serve.
    queue_window: watch::Receiver<QueueWindow>,
    /// Handle to the fanout's ring for `recent_events` (ADR 0032, tier 1);
    /// `None` (again: no coverage) until `with_derived`.
    fanout: Option<FanoutHandle>,
}

impl<C> CoordinatorControlPlane<C> {
    pub fn new(consensus: Arc<C>, views: StateViews, cluster_id: ClusterId) -> Self {
        // A watch whose sender is dropped immediately: borrows keep serving
        // the seeded empty window.
        let (_, queue_window) = watch::channel(QueueWindow::default());
        CoordinatorControlPlane {
            consensus,
            views,
            cluster_id,
            queue_window,
            fanout: None,
        }
    }

    /// Attach the replica-local derived read sources: the derived-stats
    /// task's window watch and the fanout's ring handle. The runtime calls
    /// this; a control plane without them serves honestly empty windows.
    pub fn with_derived(
        mut self,
        queue_window: watch::Receiver<QueueWindow>,
        fanout: FanoutHandle,
    ) -> Self {
        self.queue_window = queue_window;
        self.fanout = Some(fanout);
        self
    }
}

impl<C: Consensus> ControlPlane for CoordinatorControlPlane<C> {
    fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    async fn submit_job(&self, req: SubmitJobRequest) -> Result<SubmitJobResponse, ApiError> {
        // The client-minted job id is the submission's idempotency identity
        // (ADR 0026): a retry re-sends the same id, and apply resolves a
        // repeat of an already-committed submission as an accepted no-op, so
        // the retrying caller still lands in the `Ok` arm below with the
        // original id.
        //
        // The DTO already carries typed ids and required fields; what's
        // left to validate here are the rules serde can't express — the
        // same ones the conversion boundary enforces on core.v1.Job: a
        // command is required non-empty, and an entrypoint override, when
        // present, is non-empty.
        let job = req.job;
        if req.command.is_empty() {
            return Err(ApiError::Invalid("missing command".into()));
        }
        let entrypoint = match req.entrypoint {
            None => None,
            Some(argv) if argv.is_empty() => {
                return Err(ApiError::Invalid(
                    "entrypoint override must have at least one token".into(),
                ));
            }
            Some(argv) => Some(argv),
        };
        let max_runtime = match req.max_runtime_seconds {
            None => None,
            Some(seconds) if seconds <= 0 => {
                return Err(ApiError::Invalid(
                    "max_runtime_seconds must be positive".into(),
                ));
            }
            Some(seconds) => Some(Duration::from_secs(seconds)),
        };

        // Multiplier resolution reads the replicated table off the latest
        // view (ADR 0019: apply never sees the raw `priority: i32` in
        // arithmetic) — this is the "synchronous validation" that needs
        // `self.views` rather than being purely shape-level.
        let view = self.views.latest();
        let multiplier = *view
            .state()
            .policy
            .priority_multipliers
            .get(&req.priority)
            .ok_or_else(|| {
                ApiError::Invalid(format!(
                    "no multiplier configured for priority {}",
                    req.priority
                ))
            })?;

        let command = Command::SubmitJob(SubmitJob {
            job: Job {
                id: job,
                image: req.image,
                command: req.command,
                entrypoint,
                requests: req.requests.into(),
                priority: req.priority,
                max_runtime,
                quota_entity: req.quota_entity,
                retry: req.retry.map(Into::into).unwrap_or_default(),
                abort_requested: None,
            },
            multiplier,
            submitted_at: Timestamp::now(),
        });

        match self.consensus.propose(command).await {
            // `log_index` lets the caller pair this write with a strong read
            // (ADR 0007 read-your-writes). On an idempotent repeat it is the
            // repeat's own apply index — ≥ the original commit, so still a
            // valid cursor for the job.
            Ok(Applied {
                outcome: Ok(_),
                log_index,
            }) => Ok(SubmitJobResponse { job, log_index }),
            Ok(Applied {
                outcome: Err(rejection),
                ..
            }) => Err(ApiError::Rejected(rejection)),
            Err(e) => Err(map_consensus_error(e)),
        }
    }

    async fn abort_job(&self, req: AbortJobRequest) -> Result<(), ApiError> {
        // The HTTP layer resolves the authoritative id from the path; a
        // request arriving here without one skipped that resolution.
        let job = req
            .job
            .ok_or_else(|| ApiError::Invalid("missing job".into()))?;

        let command = Command::AbortJob(AbortJob {
            job,
            reason: req.reason,
            requested_at: Timestamp::now(),
        });

        match self.consensus.propose(command).await {
            Ok(Applied { outcome: Ok(_), .. }) => Ok(()),
            Ok(Applied {
                outcome: Err(rejection),
                ..
            }) => Err(ApiError::Rejected(rejection)),
            Err(e) => Err(map_consensus_error(e)),
        }
    }

    async fn read_state(&self, opts: ReadOptions) -> Result<ReadView, ApiError> {
        let view = match opts.consistency {
            Consistency::Strong => {
                let barrier = self
                    .consensus
                    .read_index()
                    .await
                    .map_err(map_consensus_error)?;
                let target = opts.min_index.map_or(barrier, |min| min.max(barrier));
                self.views
                    .at_least(target)
                    .await
                    .map_err(map_consensus_error)?
            }
            Consistency::Bounded | Consistency::Eventual => {
                let latest = self.views.latest();
                match opts.min_index {
                    Some(min) if latest.applied_index() < min => self
                        .views
                        .at_least(min)
                        .await
                        .map_err(map_consensus_error)?,
                    _ => latest,
                }
            }
        };

        // Sampled after the view resolves, and clamped to the applied
        // index: a barrier or `min_index` wait can apply entries past a
        // pre-sampled `known_committed`, and status publication is not
        // atomic with apply publication — either way, telling the caller
        // applied > committed would be a contradiction.
        let committed_index = self
            .consensus
            .status()
            .borrow()
            .known_committed
            .max(view.applied_index());

        Ok(ReadView::new(
            view.state().clone(),
            view.applied_index(),
            committed_index,
        ))
    }

    fn queue_window(&self) -> QueueWindow {
        self.queue_window.borrow().clone()
    }

    async fn recent_events(&self, limit: usize) -> RecentClusterEvents {
        // "No ring" and "ring unreachable at shutdown" both serve the same
        // honest answer: nothing covered — the exclusive cursor sits at
        // everything this replica has applied, with no events.
        let uncovered = || RecentClusterEvents {
            floor_index: self.views.latest().applied_index(),
            events: Vec::new(),
        };
        let Some(fanout) = &self.fanout else {
            return uncovered();
        };
        match fanout.recent(limit).await {
            Ok(recent) => RecentClusterEvents {
                floor_index: recent.floor_index,
                events: recent
                    .events
                    .into_iter()
                    .map(|e| StampedEvent {
                        index: e.index,
                        ordinal: e.ordinal,
                        at: e.at,
                        event: e.event,
                    })
                    .collect(),
            },
            Err(_closed) => uncovered(),
        }
    }
}

/// Map every non-`NotLeader` consensus failure to an API error.
///
/// Retryable failures (`Timeout`, `MembershipInProgress`, `LearnerNotCaughtUp`)
/// or fatal ones (`Shutdown`, `Fatal`) both surface as `Unavailable`: retryable
/// ones are literally "try again"; fatal ones still mean "this replica cannot
/// serve the write right now," which is the same actionable advice from the
/// caller's side.
fn map_consensus_error(e: ConsensusError) -> ApiError {
    match e {
        ConsensusError::NotLeader { leader } => {
            // `leader` is the raft CoordinatorId — useful in logs, useless
            // to a client, which needs a dialable client-API address. That
            // mapping does not exist yet (raft membership records only the
            // peer-plane address; ADR 0031 leaves advertising client
            // addresses through membership — or internal forwarding — as
            // the follow-up), so the hint stays empty rather than lying
            // with a bare integer the caller cannot retry against.
            tracing::debug!(leader = ?leader, "write refused: not the leader");
            ApiError::NotLeader { leader_hint: None }
        }
        other => ApiError::Unavailable(other.to_string()),
    }
}

/// Serve the public client API (ADR 0031) on the bound listener.
///
/// The router (routes, JSON error contract, consistency parameters) lives
/// in `coppice_api::http`; this task only marries it to this replica's
/// [`ControlPlane`] and the runtime's shutdown order. Most read routes are
/// `UNIMPLEMENTED` stubs until their endpoints land — implementing one
/// swaps a stub handler in `coppice-api`, not anything here.
pub async fn run<C: Consensus>(
    listener: crate::bootstrap::ClientListener,
    control_plane: Arc<CoordinatorControlPlane<C>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let app = coppice_api::http::router(control_plane);
    let graceful = async move {
        let _ = shutdown.wait_for(|s| *s).await;
    };
    tracing::debug!("API server ready");
    if let Err(e) = axum::serve(listener.into_inner(), app)
        .with_graceful_shutdown(graceful)
        .await
    {
        // axum::serve only errors on accept-loop failure; the runtime keeps
        // running (the cluster is still healthy without its API edge) and
        // the operator sees why the port went dark.
        tracing::error!(error = %e, "API server terminated with an error");
    }
    tracing::debug!("API server shut down");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeConsensus, ProposeOutcome};
    use coppice_api::http::dto;
    use coppice_core::id::JobId;

    fn control_plane(outcome: ProposeOutcome) -> CoordinatorControlPlane<FakeConsensus> {
        let (consensus, mut publisher) = FakeConsensus::new(outcome);

        // Seed a multiplier for priority 0 so submit_job's synchronous
        // validation passes and the test actually reaches `propose`.
        let mut policy = coppice_state::PolicyConfig::default();
        policy
            .priority_multipliers
            .insert(0, coppice_core::quota::PriorityMultiplier::ONE);
        let state = coppice_state::StateMachine {
            policy,
            ..coppice_state::StateMachine::default()
        };
        publisher.publish_now(&state, 1);

        let views = consensus.views();
        CoordinatorControlPlane::new(Arc::new(consensus), views, ClusterId::new())
    }

    fn submit_request(job: JobId) -> SubmitJobRequest {
        SubmitJobRequest {
            image: "busybox".to_string(),
            requests: dto::Resources {
                cpu_millis: 1000,
                memory_bytes: 0,
                disk_bytes: 0,
            },
            priority: 0,
            max_runtime_seconds: None,
            quota_entity: coppice_core::id::QuotaEntityId::new(),
            retry: None,
            job,
            command: vec!["run".to_string()],
            entrypoint: None,
        }
    }

    #[tokio::test]
    async fn accepted_submit_echoes_the_client_minted_job() {
        let cp = control_plane(ProposeOutcome::Accepted);
        let job = JobId::new();
        let response = cp.submit_job(submit_request(job)).await.expect("accepted");
        assert_eq!(response.job, job);
        assert!(response.log_index > 0);
    }

    #[tokio::test]
    async fn submit_with_an_empty_command_is_invalid() {
        let cp = control_plane(ProposeOutcome::Accepted);
        let mut req = submit_request(JobId::new());
        req.command.clear();
        let result = cp.submit_job(req).await;
        assert!(matches!(result, Err(ApiError::Invalid(_))));
    }

    #[tokio::test]
    async fn rejected_submit_maps_to_rejected() {
        let reason = coppice_state::RejectionReason::SubmitSpecMismatch(JobId::new());
        let cp = control_plane(ProposeOutcome::Rejected(reason));
        let result = cp.submit_job(submit_request(JobId::new())).await;
        assert!(matches!(result, Err(ApiError::Rejected(_))));
    }

    #[tokio::test]
    async fn not_leader_submit_maps_to_not_leader_without_a_fake_hint() {
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)));
        let result = cp.submit_job(submit_request(JobId::new())).await;
        // The raft CoordinatorId is not a dialable client address, so it
        // must not leak into the hint (which the HTTP layer would render
        // as a retry target).
        assert!(matches!(
            result,
            Err(ApiError::NotLeader { leader_hint: None })
        ));
    }

    #[tokio::test]
    async fn read_state_never_reports_applied_ahead_of_committed() {
        // FakeConsensus pins status at known_committed = 0 while its
        // publisher has published applied index 1 — the exact skew the
        // post-resolve clamp exists for.
        let cp = control_plane(ProposeOutcome::Accepted);
        let view = cp
            .read_state(ReadOptions {
                consistency: Consistency::Bounded,
                min_index: None,
            })
            .await
            .expect("bounded read");
        assert_eq!(view.applied_index(), 1);
        assert!(view.committed_index() >= view.applied_index());
    }

    #[tokio::test]
    async fn accepted_abort_returns_ok() {
        let cp = control_plane(ProposeOutcome::Accepted);
        let req = AbortJobRequest {
            job: Some(JobId::new()),
            reason: None,
        };
        assert!(cp.abort_job(req).await.is_ok());
    }
}
