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

use coppice_api::http::dto::{
    AbortJobRequest, ConfigureQuotaEntityRequest, ConfigureQuotaEntityResponse, SubmitJobRequest,
    SubmitJobResponse,
};
use coppice_api::{
    ApiError, Consistency, ControlPlane, CoordinatorMemberSummary, CoordinatorSummary,
    JobTimelineWindow, LogFetchError, LogFetchOutcome, LogFetchRequest, QueueWindow, ReadOptions,
    ReadView, RecentClusterEvents, StampedEvent,
};
use coppice_consensus::{Applied, Consensus, ConsensusError, NodeHandle, StateViews};
use coppice_core::id::{ClusterId, JobId, NodeId};

use crate::tasks::node_client::NodeLogClient;
use coppice_core::job::Job;
use coppice_core::quota::CostUnits;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::command::{AbortJob, ConfigureQuotaEntity, SubmitJob};
use coppice_state::Command;

use crate::tasks::event_fanout::{EventFilter, FanoutHandle};

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
    /// Admin handle to the consensus node, for `coordinator_status`'s raft-level
    /// view (leader/term/membership). `None` until [`with_node_handle`] attaches
    /// it — a plane without it answers `GET /api/v1/coordinators` with
    /// `UNAVAILABLE`, the same "no coverage" posture as a missing fanout ring.
    ///
    /// [`with_node_handle`]: Self::with_node_handle
    node_handle: Option<NodeHandle>,
    /// Dials agents' `NodeService` listeners for `fetch_logs` (ADR 0034).
    /// `None` until [`with_log_client`] attaches it — a plane without it
    /// answers every log fetch `Unreachable`, so `GET /api/v1/jobs/{job}/logs`
    /// degrades to "no node is reachable" rather than failing. Every replica
    /// dials identically; there is no leader gating.
    ///
    /// [`with_log_client`]: Self::with_log_client
    node_log_client: Option<Arc<NodeLogClient>>,
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
            node_handle: None,
            node_log_client: None,
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

    /// Attach the consensus admin handle backing `coordinator_status`. The
    /// runtime calls this with the replica's [`NodeHandle`]; a plane without it
    /// answers `GET /api/v1/coordinators` with `UNAVAILABLE`.
    pub fn with_node_handle(mut self, node_handle: NodeHandle) -> Self {
        self.node_handle = Some(node_handle);
        self
    }

    /// Attach the log-fetch client backing `fetch_logs` (ADR 0034). The runtime
    /// builds one from the coordinator's mTLS material and calls this; a plane
    /// without it answers every log fetch `Unreachable`.
    pub fn with_log_client(mut self, client: Arc<NodeLogClient>) -> Self {
        self.node_log_client = Some(client);
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
            Some(seconds) => match Duration::checked_from_secs(seconds) {
                Some(duration) => Some(duration),
                // Saturating here would accept the request and then run the
                // job to a wildly shorter limit than the one asked for.
                None => {
                    return Err(ApiError::Invalid(format!(
                        "max_runtime_seconds {seconds} is out of range (at most {})",
                        Duration::MAX.as_secs()
                    )));
                }
            },
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

    async fn configure_quota_entity(
        &self,
        req: ConfigureQuotaEntityRequest,
    ) -> Result<ConfigureQuotaEntityResponse, ApiError> {
        // The client-minted entity id is the upsert's idempotency identity
        // (ADR 0026), echoed back on success. Direct copy of abort_job's
        // propose-and-map shape; the id and quota ride the command as-is,
        // with `updated_at` stamped by this proposer (apply never reads a
        // clock). Cycle / unknown-parent refusals come back through the
        // rejection arm as a normal 409. No authz — matching the existing
        // submit_job/abort_job precedent (ADR 0023 is a separate subsystem).
        let entity = req.entity;
        let command = Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
            entity,
            parent: req.parent,
            name: req.name,
            quota: CostUnits(req.quota_ucu),
            updated_at: Timestamp::now(),
        });

        match self.consensus.propose(command).await {
            Ok(Applied {
                outcome: Ok(_),
                log_index,
            }) => Ok(ConfigureQuotaEntityResponse { entity, log_index }),
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

    async fn job_timeline(
        &self,
        job: JobId,
        after: Option<(u64, u32)>,
        limit: usize,
    ) -> JobTimelineWindow {
        // "No ring" and "ring unreachable at shutdown" both serve the same
        // honest answer as `recent_events`: nothing covered — the exclusive
        // floor sits at everything this replica has applied, with no events
        // and no continuation (there is nothing to continue).
        let uncovered = || JobTimelineWindow {
            floor_index: self.views.latest().applied_index(),
            events: Vec::new(),
            next: None,
        };
        let Some(fanout) = &self.fanout else {
            return uncovered();
        };
        match fanout.window(EventFilter::Job(job), after, limit).await {
            Ok(window) => JobTimelineWindow {
                floor_index: window.floor_index,
                events: window
                    .events
                    .into_iter()
                    .map(|e| StampedEvent {
                        index: e.index,
                        ordinal: e.ordinal,
                        at: e.at,
                        event: e.event,
                    })
                    .collect(),
                next: window.next,
            },
            Err(_closed) => uncovered(),
        }
    }

    fn coordinator_status(&self) -> Result<CoordinatorSummary, ApiError> {
        // No handle attached is "no coverage": the replicated-state reads still
        // work, but this raft-level view cannot be produced (mirrors the
        // missing-fanout branch in `recent_events`, but as an error — the raft
        // view *is* the endpoint, so there is no honest partial answer).
        let Some(handle) = &self.node_handle else {
            return Err(ApiError::Unavailable(
                "coordinator status unavailable: no consensus handle attached".into(),
            ));
        };

        // One point-in-time read of the consensus metrics; the matched-index
        // list is populated only while this replica is leader.
        let summary = handle.cluster_summary();
        let matched: std::collections::BTreeMap<u64, u64> =
            summary.replication.into_iter().collect();
        let members = summary
            .members
            .into_iter()
            .map(|m| CoordinatorMemberSummary {
                id: m.id,
                addr: m.addr,
                voter: m.voter,
                matched_index: matched.get(&m.id).copied(),
            })
            .collect();

        Ok(CoordinatorSummary {
            local_id: summary.local_id,
            leader: summary.leader,
            term: summary.term,
            known_committed: summary.known_committed,
            last_applied: summary.last_applied,
            snapshot_last_index: summary.snapshot_last_index,
            members,
        })
    }

    async fn fetch_logs(
        &self,
        node: NodeId,
        addr: &str,
        req: LogFetchRequest,
    ) -> Result<LogFetchOutcome, LogFetchError> {
        // No leadership gating: every replica dials agents identically so log
        // traffic load-balances (ADR 0034). Without a client attached the honest
        // answer is "unreachable", not an error page — the handler records it
        // per attempt and the walk advances.
        match &self.node_log_client {
            Some(client) => client.fetch_logs(node, addr, req).await,
            None => Err(LogFetchError::Unreachable {
                reason: "log-fetch client not attached to this replica".to_string(),
            }),
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
    async fn submit_with_an_unrepresentable_max_runtime_is_invalid() {
        // Rejecting beats saturating: accepting this and storing `Duration::MAX`
        // would run the job to a limit ~292 000 years short of the one asked
        // for, and report success while doing it.
        let cp = control_plane(ProposeOutcome::Accepted);
        let mut req = submit_request(JobId::new());
        req.max_runtime_seconds = Some(i64::MAX);
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

    fn configure_request(entity: coppice_core::id::QuotaEntityId) -> ConfigureQuotaEntityRequest {
        ConfigureQuotaEntityRequest {
            entity,
            parent: None,
            name: "team".to_string(),
            quota_ucu: 1_000_000,
        }
    }

    #[tokio::test]
    async fn accepted_configure_echoes_the_entity_and_log_index() {
        let cp = control_plane(ProposeOutcome::Accepted);
        let entity = coppice_core::id::QuotaEntityId::new();
        let response = cp
            .configure_quota_entity(configure_request(entity))
            .await
            .expect("accepted");
        assert_eq!(response.entity, entity);
        assert!(response.log_index > 0);
    }

    #[tokio::test]
    async fn rejected_configure_maps_to_rejected() {
        // A cycle / unknown-parent refusal is a committed-and-refused apply,
        // surfaced as a normal 409, not a server fault.
        let reason = coppice_state::RejectionReason::QuotaEntityCycle(
            coppice_core::id::QuotaEntityId::new(),
        );
        let cp = control_plane(ProposeOutcome::Rejected(reason));
        let result = cp
            .configure_quota_entity(configure_request(coppice_core::id::QuotaEntityId::new()))
            .await;
        assert!(matches!(result, Err(ApiError::Rejected(_))));
    }

    #[tokio::test]
    async fn not_leader_configure_maps_to_not_leader_without_a_fake_hint() {
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)));
        let result = cp
            .configure_quota_entity(configure_request(coppice_core::id::QuotaEntityId::new()))
            .await;
        assert!(matches!(
            result,
            Err(ApiError::NotLeader { leader_hint: None })
        ));
    }

    #[tokio::test]
    async fn job_timeline_without_a_fanout_is_honestly_empty() {
        // No ring attached (the plane is built without `with_derived`): the
        // honest answer is the same as `recent_events` — floor at everything
        // applied (the publisher seeded index 1), no events, no continuation.
        let cp = control_plane(ProposeOutcome::Accepted);
        let window = cp.job_timeline(JobId::new(), None, 100).await;
        assert_eq!(window.floor_index, 1);
        assert!(window.events.is_empty());
        assert_eq!(window.next, None);
    }

    #[tokio::test]
    async fn job_timeline_serves_the_fanout_ring_filtered_to_the_job() {
        use coppice_consensus::EventBatch;
        use coppice_state::Event;

        let (mut tap, tap_rx) = coppice_consensus::EventTap::channel(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, join) = crate::tasks::event_fanout::spawn(tap_rx, 0, shutdown_rx);

        let job = JobId::new();
        let other = JobId::new();
        // A mixed batch then a job-only batch; only `job`'s events return, and
        // the filtered event keeps its batch ordinal (never renumbered).
        tap.emit(EventBatch {
            applied_index: 5,
            at: Timestamp::UNIX_EPOCH,
            events: vec![
                Event::JobSubmitted { job: other },
                Event::JobSubmitted { job },
            ],
        });
        tap.emit(EventBatch {
            applied_index: 9,
            at: Timestamp::UNIX_EPOCH,
            events: vec![Event::JobSubmitted { job }],
        });
        // Let the current-thread fanout drain the tap into its ring.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        let (_tx, queue_window) = watch::channel(QueueWindow::default());
        let cp = control_plane(ProposeOutcome::Accepted).with_derived(queue_window, fanout);

        let window = cp.job_timeline(job, None, 100).await;
        let ids: Vec<(u64, u32)> = window.events.iter().map(|e| (e.index, e.ordinal)).collect();
        assert_eq!(ids, vec![(5, 1), (9, 0)]);
        assert_eq!(window.floor_index, 0);
        // Reached the newest retained event.
        assert_eq!(window.next, None);

        let _ = shutdown_tx.send(true);
        drop(tap);
        let _ = join.await;
    }
}
