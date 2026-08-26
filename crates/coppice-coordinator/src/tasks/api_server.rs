//! API server (every replica).
//!
//! Implements `coppice_api::ControlPlane` by proposing through consensus —
//! the write path is `Consensus::propose`, mapping an `Applied.outcome`
//! rejection to a user-facing error
//! (`docs/architecture/coordinator-runtime.md`, "API server"). This runs on
//! every replica, including followers: a follower still accepts requests and
//! maps `ConsensusError::NotLeader` to a redirect, per the trait's contract.
//!
//! A follower does not stop there, though: since ADR 0038 it **forwards**
//! the write to the leader over the coordinator mTLS admin channel rather
//! than redirecting, so any replica's address serves the whole API. The
//! redirect survives only as the fallback for the cases forwarding cannot
//! cover — no leader known, no address for the one that is. The write logic
//! itself lives in the `*_here` functions below, which the leader re-runs
//! verbatim for a forwarded request (`crate::admin`).
//!
//! The HTTP transport is `coppice_api::http` (axum, ADR 0031): [`run`]
//! serves that router over the bound `listen.client_addr` listener, with
//! this file owning only the `ControlPlane` implementation behind it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::watch;

use coppice_api::http::dto::{
    AbortJobRequest, ConfigureQuotaEntityRequest, ConfigureQuotaEntityResponse, SubmitJobRequest,
    SubmitJobResponse,
};
use coppice_api::{
    ApiError, Consistency, ControlPlane, CoordinatorMemberSummary, CoordinatorSummary,
    JobTimelineWindow, LogFetchError, LogFetchOutcome, LogFetchRequest, MetricsFetchError,
    MetricsFetchOutcome, MetricsFetchRequest, QueueWindow, ReadOptions, ReadView,
    RecentClusterEvents, StampedEvent,
};
use coppice_consensus::{
    Applied, Consensus, ConsensusError, CoordinatorId, NodeHandle, Role, StateViews,
};
use coppice_core::id::{ClusterId, JobId, NodeId};

use crate::tasks::node_client::NodeClient;
use coppice_core::job::Job;
use coppice_core::quota::CostUnits;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::command::{AbortJob, ConfigureQuotaEntity, SubmitJob};
use coppice_state::Command;

use crate::tasks::event_fanout::{EventFilter, FanoutHandle};

/// A boxed future, the shape a dyn-compatible async seam has to take.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Where a follower sends a client write it cannot serve itself (ADR 0038).
///
/// The trait exists so the decision to forward is testable without a
/// network: production plugs in
/// [`crate::clientwrite::AdminForwarder`], which resolves the leader's raft
/// address from membership and calls the `Forward*` admin RPCs; unit tests
/// plug in whatever outcome they want to see mapped.
///
/// Every method takes the raft [`CoordinatorId`] the failed propose named,
/// not an address: which address that id dials is the forwarder's business,
/// and it is the one thing a plane without a [`NodeHandle`] could not
/// answer.
pub trait LeaderWrites: Send + Sync + 'static {
    fn submit_job<'a>(
        &'a self,
        leader: CoordinatorId,
        req: &'a SubmitJobRequest,
    ) -> BoxFuture<'a, Result<SubmitJobResponse, ApiError>>;

    fn abort_job<'a>(
        &'a self,
        leader: CoordinatorId,
        job: JobId,
        reason: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), ApiError>>;

    fn configure_quota_entity<'a>(
        &'a self,
        leader: CoordinatorId,
        req: &'a ConfigureQuotaEntityRequest,
    ) -> BoxFuture<'a, Result<ConfigureQuotaEntityResponse, ApiError>>;
}

/// How a write attempted on *this* replica ended.
///
/// [`ApiError`] cannot express the one distinction the forwarding decision
/// turns on — a `NotLeader` refusal carries a *dialable* hint or nothing,
/// never the raft id — so the local write path reports that case separately
/// and lets its two callers decide: the control plane forwards, and the
/// leader-side handler of a forwarded write reports "not me" back rather
/// than chaining a second hop (ADR 0038).
pub(crate) enum LocalWriteError {
    /// A final answer for the client: validation refused it, or apply did.
    Api(ApiError),
    /// This replica is not the leader; `leader` is the raft id when it knows
    /// one.
    NotLeader { leader: Option<CoordinatorId> },
}

impl From<ConsensusError> for LocalWriteError {
    fn from(e: ConsensusError) -> Self {
        match e {
            ConsensusError::NotLeader { leader } => LocalWriteError::NotLeader { leader },
            other => LocalWriteError::Api(ApiError::Unavailable(other.to_string())),
        }
    }
}

/// Validate and propose one submission on this replica (ADR 0031's write
/// class), with no forwarding of any kind.
///
/// The whole write path for a submission lives here, so the leader running a
/// *forwarded* request runs exactly what it would have run for a direct one:
/// the shape checks, the multiplier lookup against **this** replica's view,
/// this replica's `submitted_at` stamp, and the propose. That is why
/// forwarding carries the request and never a pre-built [`Command`]
/// (ADR 0038).
///
/// The half of that which reads replicated state runs only on the leader: a
/// replica that is not the leader stops at the shape checks and reports
/// [`LocalWriteError::NotLeader`], so its own lagging view can never refuse a
/// submission the leader would have accepted.
pub(crate) async fn submit_job_here<C: Consensus>(
    consensus: &C,
    views: &StateViews,
    req: &SubmitJobRequest,
) -> Result<SubmitJobResponse, LocalWriteError> {
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
    let invalid = |m: String| LocalWriteError::Api(ApiError::Invalid(m));
    let job = req.job;
    if req.command.is_empty() {
        return Err(invalid("missing command".into()));
    }
    let entrypoint = match &req.entrypoint {
        None => None,
        Some(argv) if argv.is_empty() => {
            return Err(invalid(
                "entrypoint override must have at least one token".into(),
            ));
        }
        Some(argv) => Some(argv.clone()),
    };
    let max_runtime = match req.max_runtime_seconds {
        None => None,
        Some(seconds) if seconds <= 0 => {
            return Err(invalid("max_runtime_seconds must be positive".into()));
        }
        Some(seconds) => match Duration::checked_from_secs(seconds) {
            Some(duration) => Some(duration),
            // Saturating here would accept the request and then run the
            // job to a wildly shorter limit than the one asked for.
            None => {
                return Err(invalid(format!(
                    "max_runtime_seconds {seconds} is out of range (at most {})",
                    Duration::MAX.as_secs()
                )));
            }
        },
    };

    // Everything above this line is shape validation: it reads only the
    // request, so its verdict is the same on every replica and a follower is
    // entitled to reach it. Everything below reads *this replica's* applied
    // state, and a follower's copy of that is by definition behind the
    // leader's — so a follower must not render a verdict from it (ADR 0038).
    //
    // Concretely: a policy update that adds a priority class is committed on
    // the leader before it applies here. A submission at that priority
    // arriving in the gap would find no multiplier in this view and be
    // refused as invalid — a lagging follower vetoing a write the leader
    // would accept. So leadership is consulted first, and a replica that is
    // not the leader hands the whole request over instead of judging it.
    //
    // Both stale answers are safe. Stale "leader" on a follower: the lookup
    // runs, the propose below returns `NotLeader` anyway, and forwarding
    // happens one step later. Stale "follower" on the actual leader: the
    // request takes one self-directed hop, which the single-hop rule already
    // handles.
    //
    // Only this write path needs the gate. `abort_job_here` and
    // `configure_quota_entity_here` validate nothing against the view — they
    // build a command from the request and propose it — so their `NotLeader`
    // already comes from the propose, which cannot be stale.
    match consensus.status().borrow().role {
        Role::Leader { .. } => {}
        Role::Follower { leader } => return Err(LocalWriteError::NotLeader { leader }),
        // Mid-election, with no leader to name. Forwarding has no target, so
        // this is the redirect either way; what it must not be is an
        // `Invalid` minted from a view nobody is currently authoritative for.
        Role::Unknown => return Err(LocalWriteError::NotLeader { leader: None }),
    }

    // Multiplier resolution reads the replicated table off the latest
    // view (ADR 0019: apply never sees the raw `priority: i32` in
    // arithmetic) — this is the "synchronous validation" that needs
    // `views` rather than being purely shape-level.
    let view = views.latest();
    let multiplier = *view
        .state()
        .policy
        .priority_multipliers
        .get(&req.priority)
        .ok_or_else(|| {
            invalid(format!(
                "no multiplier configured for priority {}",
                req.priority
            ))
        })?;

    let command = Command::SubmitJob(SubmitJob {
        job: Job {
            id: job,
            image: req.image.clone(),
            command: req.command.clone(),
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

    match consensus.propose(command).await {
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
        }) => Err(LocalWriteError::Api(ApiError::Rejected(rejection))),
        Err(e) => Err(e.into()),
    }
}

/// Propose one abort on this replica, with no forwarding. The twin of
/// [`submit_job_here`]; `job` is the id the HTTP layer resolved from the path.
pub(crate) async fn abort_job_here<C: Consensus>(
    consensus: &C,
    job: JobId,
    reason: Option<String>,
) -> Result<(), LocalWriteError> {
    let command = Command::AbortJob(AbortJob {
        job,
        reason,
        requested_at: Timestamp::now(),
    });

    match consensus.propose(command).await {
        Ok(Applied { outcome: Ok(_), .. }) => Ok(()),
        Ok(Applied {
            outcome: Err(rejection),
            ..
        }) => Err(LocalWriteError::Api(ApiError::Rejected(rejection))),
        Err(e) => Err(e.into()),
    }
}

/// Propose one quota-entity upsert on this replica, with no forwarding.
///
/// The client-minted entity id is the upsert's idempotency identity
/// (ADR 0026), echoed back on success. Direct copy of [`abort_job_here`]'s
/// propose-and-map shape; the id and quota ride the command as-is, with
/// `updated_at` stamped by this proposer (apply never reads a clock). Cycle /
/// unknown-parent refusals come back through the rejection arm as a normal
/// 409. No authz — matching the existing submit_job/abort_job precedent
/// (ADR 0023 is a separate subsystem).
pub(crate) async fn configure_quota_entity_here<C: Consensus>(
    consensus: &C,
    req: &ConfigureQuotaEntityRequest,
) -> Result<ConfigureQuotaEntityResponse, LocalWriteError> {
    let entity = req.entity;
    let command = Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
        entity,
        parent: req.parent,
        name: req.name.clone(),
        quota: CostUnits(req.quota_ucu),
        updated_at: Timestamp::now(),
    });

    match consensus.propose(command).await {
        Ok(Applied {
            outcome: Ok(_),
            log_index,
        }) => Ok(ConfigureQuotaEntityResponse { entity, log_index }),
        Ok(Applied {
            outcome: Err(rejection),
            ..
        }) => Err(LocalWriteError::Api(ApiError::Rejected(rejection))),
        Err(e) => Err(e.into()),
    }
}

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
    /// Dials agents' `NodeService` listeners for `fetch_logs` and
    /// `fetch_metrics` (ADR 0034). `None` until [`with_log_client`] attaches it
    /// — a plane without it answers every fetch `Unreachable`, so
    /// `GET /api/v1/jobs/{job}/logs` and `.../usage` degrade to "no node is
    /// reachable" rather than failing. Every replica dials identically; there is
    /// no leader gating.
    ///
    /// [`with_log_client`]: Self::with_log_client
    node_log_client: Option<Arc<NodeClient>>,
    /// Where a write that landed on this replica while it is a follower gets
    /// sent (ADR 0038). `None` until [`with_forwarder`] attaches one — and a
    /// plane without one falls back to exactly the pre-0038 behaviour, the
    /// bare 421 with an empty hint, which is also what a plane *with* one
    /// answers when no leader is known.
    ///
    /// [`with_forwarder`]: Self::with_forwarder
    forwarder: Option<Arc<dyn LeaderWrites>>,
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
            forwarder: None,
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
    pub fn with_log_client(mut self, client: Arc<NodeClient>) -> Self {
        self.node_log_client = Some(client);
        self
    }

    /// Attach the leader-forwarding seam backing follower writes (ADR 0038).
    /// The runtime builds one over this replica's machine-plane identity; a
    /// plane without one refuses follower writes with the ADR 0031 redirect
    /// instead.
    pub fn with_forwarder(mut self, forwarder: Arc<dyn LeaderWrites>) -> Self {
        self.forwarder = Some(forwarder);
        self
    }

    /// The forwarder and the leader to send to, when both are known.
    ///
    /// `None` on either half missing is the ADR 0038 fallback: an election in
    /// progress leaves no id to forward to, and a plane with no seam attached
    /// cannot forward at all. Both answer the client the same way — the
    /// hintless 421 — because both are "ask again", not "ask over there".
    fn forward_to(
        &self,
        leader: Option<CoordinatorId>,
    ) -> Option<(&Arc<dyn LeaderWrites>, CoordinatorId)> {
        match (&self.forwarder, leader) {
            (Some(forwarder), Some(leader)) => Some((forwarder, leader)),
            _ => None,
        }
    }
}

impl<C: Consensus> ControlPlane for CoordinatorControlPlane<C> {
    fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    async fn submit_job(&self, req: SubmitJobRequest) -> Result<SubmitJobResponse, ApiError> {
        match submit_job_here(&*self.consensus, &self.views, &req).await {
            Ok(response) => Ok(response),
            Err(LocalWriteError::Api(e)) => Err(e),
            // A follower forwards rather than redirecting (ADR 0038): the
            // request crosses one internal mTLS hop and the leader re-runs
            // the whole write path on it. A forwarding failure is reported
            // as itself — never as a success this replica cannot vouch for.
            Err(LocalWriteError::NotLeader { leader }) => match self.forward_to(leader) {
                Some((forwarder, leader)) => forwarder.submit_job(leader, &req).await,
                None => Err(no_leader_here(leader)),
            },
        }
    }

    async fn abort_job(&self, req: AbortJobRequest) -> Result<(), ApiError> {
        // The HTTP layer resolves the authoritative id from the path; a
        // request arriving here without one skipped that resolution.
        let job = req
            .job
            .ok_or_else(|| ApiError::Invalid("missing job".into()))?;

        match abort_job_here(&*self.consensus, job, req.reason.clone()).await {
            Ok(()) => Ok(()),
            Err(LocalWriteError::Api(e)) => Err(e),
            Err(LocalWriteError::NotLeader { leader }) => match self.forward_to(leader) {
                Some((forwarder, leader)) => {
                    forwarder
                        .abort_job(leader, job, req.reason.as_deref())
                        .await
                }
                None => Err(no_leader_here(leader)),
            },
        }
    }

    async fn configure_quota_entity(
        &self,
        req: ConfigureQuotaEntityRequest,
    ) -> Result<ConfigureQuotaEntityResponse, ApiError> {
        match configure_quota_entity_here(&*self.consensus, &req).await {
            Ok(response) => Ok(response),
            Err(LocalWriteError::Api(e)) => Err(e),
            Err(LocalWriteError::NotLeader { leader }) => match self.forward_to(leader) {
                Some((forwarder, leader)) => forwarder.configure_quota_entity(leader, &req).await,
                None => Err(no_leader_here(leader)),
            },
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

    async fn fetch_metrics(
        &self,
        node: NodeId,
        addr: &str,
        req: MetricsFetchRequest,
    ) -> Result<MetricsFetchOutcome, MetricsFetchError> {
        // The metrics twin of `fetch_logs`: same client, same no-leader-gating
        // posture. Without a client attached the honest answer is "unreachable",
        // not an error page — the usage handler records it per attempt and the
        // walk advances.
        match &self.node_log_client {
            Some(client) => client.fetch_metrics(node, addr, req).await,
            None => Err(MetricsFetchError::Unreachable {
                reason: "node-fetch client not attached to this replica".to_string(),
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
        ConsensusError::NotLeader { leader } => no_leader_here(leader),
        other => ApiError::Unavailable(other.to_string()),
    }
}

/// The ADR 0031 redirect, now the ADR 0038 *fallback*: what a replica answers
/// when it cannot serve a write and cannot forward it either.
///
/// The hint stays empty. `leader` is the raft CoordinatorId — useful in logs,
/// useless to a client, which needs a dialable client-API address, and raft
/// membership records only the peer-plane one. Rendering the bare integer
/// would hand the caller a retry target it cannot dial. ADR 0038 chose not to
/// advertise client addresses through membership precisely because forwarding
/// makes this path rare: reaching it means no leader is known at all, or this
/// replica has no forwarding seam, and in both cases the honest advice is
/// "ask again", not "ask over there".
fn no_leader_here(leader: Option<CoordinatorId>) -> ApiError {
    tracing::debug!(leader = ?leader, "write refused: not the leader, and not forwarded");
    ApiError::NotLeader { leader_hint: None }
}

/// Serve the public client API (ADR 0031) on the bound listener.
///
/// The router (routes, JSON error contract, consistency parameters) lives
/// in `coppice_api::http`; this task only marries it to this replica's
/// [`ControlPlane`] and the runtime's shutdown order. Most read routes are
/// `UNIMPLEMENTED` stubs until their endpoints land — implementing one
/// swaps a stub handler in `coppice-api`, not anything here.
///
/// The same listener also serves the Prometheus `/metrics` scrape target
/// (issue #46): the runtime installs the recorder once at startup and passes
/// its [`MetricsEndpoint`](coppice_api::http::MetricsEndpoint) here, so the
/// coordinator has no separate metrics port — the endpoint rides the client
/// API edge.
pub async fn run<C: Consensus>(
    listener: crate::bootstrap::ClientListener,
    control_plane: Arc<CoordinatorControlPlane<C>>,
    metrics: coppice_api::http::MetricsEndpoint,
    readyz: coppice_api::http::ReadyzEndpoint,
    enroll: coppice_api::http::EnrollEndpoint,
    cluster_ca: crate::clientedge::ClusterCa,
    shutdown: watch::Receiver<bool>,
) {
    let app = coppice_api::http::router(control_plane, metrics, readyz, enroll);
    let (socket, tls) = listener.into_parts();
    tracing::debug!("API server ready");
    // The serving posture (`[client_tls]`, ADR 0037 §4) was decided at config
    // load and carried on the listener; the cluster CA is the client-cert trust
    // anchor, read per accept because it appears at formation and changes on a
    // re-root.
    crate::clientedge::serve(socket, app, tls.map(|store| (store, cluster_ca)), shutdown).await;
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

    /// What a [`FakeForwarder`] pretends the leader said.
    ///
    /// One answer per outcome the real transport can produce, so the mapping
    /// from "what the leader decided" to "what the client sees" is testable
    /// without a socket. `Timeout` stands for every lost hop: the request may
    /// have committed, and this replica cannot tell.
    #[derive(Clone)]
    enum ForwardAnswer {
        Applied(u64),
        Rejected(String),
        NotLeader,
        Timeout,
    }

    /// A [`LeaderWrites`] seam that answers from a script and records what
    /// crossed it.
    struct FakeForwarder {
        answer: std::sync::Mutex<ForwardAnswer>,
        seen: std::sync::Mutex<Vec<(CoordinatorId, Option<JobId>)>>,
    }

    impl FakeForwarder {
        fn answering(answer: ForwardAnswer) -> Arc<FakeForwarder> {
            Arc::new(FakeForwarder {
                answer: std::sync::Mutex::new(answer),
                seen: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn answer(&self, answer: ForwardAnswer) {
            *self.answer.lock().unwrap() = answer;
        }

        fn record(&self, leader: CoordinatorId, job: Option<JobId>) -> Result<u64, ApiError> {
            self.seen.lock().unwrap().push((leader, job));
            match self.answer.lock().unwrap().clone() {
                ForwardAnswer::Applied(index) => Ok(index),
                ForwardAnswer::Rejected(reason) => Err(ApiError::ForwardedRejection(reason)),
                ForwardAnswer::NotLeader => Err(ApiError::NotLeader { leader_hint: None }),
                ForwardAnswer::Timeout => Err(ApiError::Unavailable(
                    "the leader did not answer in time; the outcome is unknown".to_string(),
                )),
            }
        }

        fn calls(&self) -> usize {
            self.seen.lock().unwrap().len()
        }

        fn leaders(&self) -> Vec<CoordinatorId> {
            self.seen.lock().unwrap().iter().map(|(l, _)| *l).collect()
        }

        fn jobs(&self) -> Vec<JobId> {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(_, job)| *job)
                .collect()
        }
    }

    impl LeaderWrites for FakeForwarder {
        fn submit_job<'a>(
            &'a self,
            leader: CoordinatorId,
            req: &'a SubmitJobRequest,
        ) -> BoxFuture<'a, Result<SubmitJobResponse, ApiError>> {
            Box::pin(async move {
                let log_index = self.record(leader, Some(req.job))?;
                Ok(SubmitJobResponse {
                    job: req.job,
                    log_index,
                })
            })
        }

        fn abort_job<'a>(
            &'a self,
            leader: CoordinatorId,
            job: JobId,
            _reason: Option<&'a str>,
        ) -> BoxFuture<'a, Result<(), ApiError>> {
            Box::pin(async move {
                self.record(leader, Some(job))?;
                Ok(())
            })
        }

        fn configure_quota_entity<'a>(
            &'a self,
            leader: CoordinatorId,
            req: &'a ConfigureQuotaEntityRequest,
        ) -> BoxFuture<'a, Result<ConfigureQuotaEntityResponse, ApiError>> {
            Box::pin(async move {
                let log_index = self.record(leader, None)?;
                Ok(ConfigureQuotaEntityResponse {
                    entity: req.entity,
                    log_index,
                })
            })
        }
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
    async fn not_leader_submit_without_a_forwarder_still_redirects_without_a_fake_hint() {
        // The ADR 0038 fallback, and the pre-0038 behaviour unchanged: with
        // no forwarding seam attached there is nowhere to send the write, so
        // the client gets the redirect. The raft CoordinatorId is not a
        // dialable client address, so it must not leak into the hint (which
        // the HTTP layer would render as a retry target).
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)));
        let result = cp.submit_job(submit_request(JobId::new())).await;
        assert!(matches!(
            result,
            Err(ApiError::NotLeader { leader_hint: None })
        ));
    }

    #[tokio::test]
    async fn a_write_with_no_leader_to_forward_to_is_not_forwarded() {
        // An election in progress: the propose names no leader, so even a
        // plane with a seam attached has no target. Forwarding must not be
        // attempted (the fake would answer, and answering here would mean
        // guessing at a leader).
        let forwarder = FakeForwarder::answering(ForwardAnswer::Applied(99));
        let cp = control_plane(ProposeOutcome::NotLeader(None))
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);
        let result = cp.submit_job(submit_request(JobId::new())).await;
        assert!(matches!(
            result,
            Err(ApiError::NotLeader { leader_hint: None })
        ));
        assert_eq!(forwarder.calls(), 0, "nothing to forward to");
    }

    #[tokio::test]
    async fn a_follower_forwards_the_submission_and_serves_the_leaders_answer() {
        let forwarder = FakeForwarder::answering(ForwardAnswer::Applied(42));
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)))
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);

        let job = JobId::new();
        let response = cp.submit_job(submit_request(job)).await.expect("forwarded");
        // The leader's apply index, not one this replica made up, and the
        // client's own job id back.
        assert_eq!(response.log_index, 42);
        assert_eq!(response.job, job);
        // Forwarded to the leader the failed propose named — the one thing
        // `ConsensusError::NotLeader`'s id is now load-bearing for.
        assert_eq!(forwarder.leaders(), vec![7]);
    }

    #[tokio::test]
    async fn a_leader_that_has_moved_on_ends_the_hop_at_the_redirect() {
        // Single hop (ADR 0038): the coordinator this write was forwarded to
        // is no longer the leader, and says so. The follower surfaces its
        // ordinary redirect rather than chasing a second hop.
        let forwarder = FakeForwarder::answering(ForwardAnswer::NotLeader);
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)))
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);
        let result = cp.submit_job(submit_request(JobId::new())).await;
        assert!(matches!(
            result,
            Err(ApiError::NotLeader { leader_hint: None })
        ));
        assert_eq!(forwarder.calls(), 1, "exactly one hop");
    }

    #[tokio::test]
    async fn a_forwarding_timeout_is_reported_as_retriable_and_never_as_success() {
        // The outcome is genuinely unknown — the write may have committed —
        // so the one thing that must not happen is an `Ok`. `Unavailable` is
        // the "did not resolve to a replicated decision" answer, which is
        // exactly what a local propose timeout already produces.
        let forwarder = FakeForwarder::answering(ForwardAnswer::Timeout);
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)))
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);
        let result = cp.submit_job(submit_request(JobId::new())).await;
        match result {
            Err(ApiError::Unavailable(message)) => {
                assert!(message.contains("unknown"), "{message}");
            }
            other => panic!("a lost forwarding hop must be retriable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn retrying_after_an_unknown_forwarding_outcome_lands_on_the_same_job() {
        // The idempotency contract that makes the retriable error above safe
        // to act on (ADR 0026): the client re-sends the *identical* request,
        // including its own job id, and the leader's apply resolves the
        // repeat as an accepted no-op. Nothing on the forwarding path mints
        // an id or rewrites the request, so the retry cannot become a second
        // job — which is what this asserts by watching what crossed the hop.
        let forwarder = FakeForwarder::answering(ForwardAnswer::Timeout);
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)))
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);

        let job = JobId::new();
        let request = submit_request(job);
        let first = cp.submit_job(request.clone()).await;
        assert!(matches!(first, Err(ApiError::Unavailable(_))));

        // The retry: same request, same id. The leader has it now.
        forwarder.answer(ForwardAnswer::Applied(7));
        let second = cp.submit_job(request).await.expect("the retry resolves");
        assert_eq!(second.job, job);
        assert_eq!(
            forwarder.jobs(),
            vec![job, job],
            "both hops carried the client's id, unchanged"
        );
    }

    #[tokio::test]
    async fn a_rejection_from_the_leader_stays_a_rejection() {
        // Apply refused it on the leader; the client must still see the 409,
        // not a redirect and not a server fault.
        let forwarder =
            FakeForwarder::answering(ForwardAnswer::Rejected("job already exists".to_string()));
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)))
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);
        let result = cp.submit_job(submit_request(JobId::new())).await;
        match result {
            Err(ApiError::ForwardedRejection(reason)) => assert_eq!(reason, "job already exists"),
            other => panic!("expected a relayed rejection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_follower_forwards_aborts_and_quota_upserts_too() {
        let forwarder = FakeForwarder::answering(ForwardAnswer::Applied(11));
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)))
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);

        let job = JobId::new();
        cp.abort_job(AbortJobRequest {
            job: Some(job),
            reason: Some("done with it".to_string()),
        })
        .await
        .expect("forwarded abort");

        let entity = coppice_core::id::QuotaEntityId::new();
        let response = cp
            .configure_quota_entity(configure_request(entity))
            .await
            .expect("forwarded upsert");
        assert_eq!(response.entity, entity);
        assert_eq!(response.log_index, 11);
        assert_eq!(forwarder.calls(), 2);
    }

    #[tokio::test]
    async fn validation_is_refused_locally_and_never_forwarded() {
        // A follower is not a proxy: a request that cannot be valid anywhere
        // is refused here, before a hop is spent on it.
        let forwarder = FakeForwarder::answering(ForwardAnswer::Applied(1));
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)))
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);
        let mut req = submit_request(JobId::new());
        req.command.clear();
        assert!(matches!(
            cp.submit_job(req).await,
            Err(ApiError::Invalid(_))
        ));
        assert_eq!(forwarder.calls(), 0);
    }

    /// A submission at a priority the seeded policy has no multiplier for —
    /// the shape of a request a *lagging* replica cannot judge, because the
    /// class may have been added by a policy update it has not applied yet.
    fn submit_at_an_unconfigured_priority() -> SubmitJobRequest {
        let mut req = submit_request(JobId::new());
        req.priority = 5;
        req
    }

    #[tokio::test]
    async fn a_follower_forwards_a_priority_its_own_view_does_not_know_yet() {
        // The finding this gate exists for: the multiplier table is
        // replicated, so a follower behind a policy update that added
        // priority 5 sees no multiplier for it. Judging that locally would
        // refuse — with INVALID_ARGUMENT, which tells the client to change
        // the request — a submission the leader would accept. It goes to the
        // leader instead, which owns the authoritative table.
        let forwarder = FakeForwarder::answering(ForwardAnswer::Applied(42));
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)))
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);

        let response = cp
            .submit_job(submit_at_an_unconfigured_priority())
            .await
            .expect("forwarded, not refused");
        assert_eq!(response.log_index, 42);
        assert_eq!(forwarder.leaders(), vec![7]);
    }

    #[tokio::test]
    async fn an_unconfigured_priority_is_still_invalid_on_the_leader() {
        // The converse, and the reason the check is deferred rather than
        // dropped: on the replica that *is* authoritative, an unknown
        // priority is a genuinely malformed request and still gets the 400.
        let forwarder = FakeForwarder::answering(ForwardAnswer::Applied(42));
        let cp = control_plane(ProposeOutcome::Accepted)
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);

        let result = cp.submit_job(submit_at_an_unconfigured_priority()).await;
        assert!(matches!(result, Err(ApiError::Invalid(_))), "{result:?}");
        assert_eq!(forwarder.calls(), 0, "the leader forwards nothing");
    }

    #[tokio::test]
    async fn a_follower_with_no_leader_redirects_rather_than_refusing_the_priority() {
        // Mid-election there is nothing to forward to, but that does not make
        // this replica's view authoritative: the answer is the redirect
        // ("ask again"), never `Invalid` ("change your request").
        let forwarder = FakeForwarder::answering(ForwardAnswer::Applied(42));
        let cp = control_plane(ProposeOutcome::NotLeader(None))
            .with_forwarder(Arc::clone(&forwarder) as Arc<dyn LeaderWrites>);

        let result = cp.submit_job(submit_at_an_unconfigured_priority()).await;
        assert!(
            matches!(result, Err(ApiError::NotLeader { leader_hint: None })),
            "{result:?}"
        );
        assert_eq!(forwarder.calls(), 0, "no leader to forward to");
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
    async fn not_leader_configure_without_a_forwarder_still_redirects_without_a_fake_hint() {
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
