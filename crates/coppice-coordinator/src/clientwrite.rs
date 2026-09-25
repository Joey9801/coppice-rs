//! The transport half of follower write forwarding (ADR 0038).
//!
//! A follower that cannot propose a client write sends it to the leader over
//! the coordinator-to-coordinator mTLS admin channel — the same channel, the
//! same dial helper, and the same authorization posture the follower
//! `/enroll` proxy has used since ADR 0037 §4. This module holds only that
//! hop: resolving the leader's raft address out of membership, dialing,
//! calling one of the `Forward*` RPCs under a bounded timeout, and mapping
//! what comes back onto the ordinary [`ApiError`] vocabulary.
//!
//! Since ADR 0040 one **read** crosses the same hop: a replica that does not
//! lead fetches the leader's node-liveness marks, under a much shorter budget,
//! and degrades to "nothing to judge by" on any failure rather than to an
//! error. That asymmetry is the whole difference between [`LeaderWrites`] and
//! [`LeaderReads`] — and it is why the read keeps *one cached channel* per
//! leader ([`ChannelCache`]) where the write dials afresh: a forwarded write is
//! rare, while the read runs once per follower-served node page, at whatever
//! rate a dashboard polls.
//!
//! The *decision* to forward, and every rule about what a forwarded write
//! means, lives with the write path in
//! [`crate::tasks::api_server`]; the leader-side handlers live in
//! [`crate::admin`]. What crosses this hop is the client's request, never a
//! pre-built log command: the leader re-runs the whole write path on it.
//!
//! One hop, always. If the request reaches a coordinator that is not (or is
//! no longer) the leader, that coordinator answers `NotLeader` and this side
//! surfaces the ordinary redirect — it never chases a second hop.

use std::sync::{Arc, Mutex};

use coppice_api::http::dto::{
    ResolvedConfigureQuotaEntityRequest, ResolvedSubmitJobRequest,
    ResolvedUpdateAuthorizationRequest, SubmitJobResponse, UpdateAuthorizationResponse,
};
use coppice_api::{ApiError, QuotaEntityConfigured, RejectionKind};
use coppice_consensus::{CoordinatorId, NodeHandle};
use coppice_core::id::{JobId, NodeId};
use coppice_net::admin::Client;
use coppice_proto::convert::{
    env_from_pb, env_to_pb, metadata_from_pb, metadata_map_to_pb, metadata_to_pb, ConvertError,
};
use coppice_proto::pb::raft::v1 as pb;
use coppice_state::command::JobMetadataUpdate;
use coppice_state::Actor;
use coppice_tls::TlsStore;
use tonic::transport::Channel;
use tonic::Code;

use crate::tasks::api_server::{BoxFuture, LeaderReads, LeaderWrites};

/// How long a follower waits for the leader to answer a forwarded write —
/// **in total**, dial included.
///
/// The same 10s the `/enroll` proxy allows (`crate::enroll::PROXY_TIMEOUT`),
/// and for the same reason: generous next to a commit-and-apply round trip,
/// short enough that a wedged leader does not hold a client's connection —
/// or this replica's ingress slot — indefinitely.
///
/// It bounds the *dial* as well as the call. A leader address that blackholes
/// packets — the interesting failure, since a dead process refuses in
/// microseconds — leaves a TCP connect hanging for the OS's own retry budget,
/// which is minutes on every platform this runs on. Budgeting only the RPC
/// would have made the 10s a promise this path could not keep.
///
/// One budget, not one per stage: each forwarded write stamps a single
/// [`forward_deadline`] up front and both stages run against it, so a slow
/// dial spends the call's time rather than adding to it. Two independent 10s
/// timeouts would have advertised 10s and delivered up to 20.
const FORWARD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long a replica that does not lead waits for the leader's liveness
/// marks — **in total**, dial included (ADR 0040).
///
/// Deliberately a fifth of the write path's 10 s, and for a different job. A
/// forwarded write is a client's mutation: it is worth waiting a long time
/// for, because the alternative is telling the caller an outcome nobody knows.
/// This is a dashboard poll behind `GET /api/v1/nodes`, and its fallback
/// answer — health `unknown`, exactly what the replica served before ADR 0040
/// — is *fine*. Spending ten seconds of a viewer's page load to avoid a
/// degraded field would be the wrong trade in both directions: the read is on
/// a poll loop, so a slow answer is quickly a stale one, and the UI refreshes
/// anyway.
///
/// It bounds the dial as well as the call, for the same reason
/// [`FORWARD_TIMEOUT`] does: a leader address that blackholes packets leaves a
/// TCP connect hanging for the OS's retry budget, which is minutes.
const LIVENESS_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// The single instant every stage of one forwarded write must finish by.
///
/// Stamped once per [`LeaderWrites`] call and threaded through the dial and
/// the RPC, which is what makes the budget additive across stages rather than
/// per stage.
fn forward_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + FORWARD_TIMEOUT
}

/// What a cached read channel is pinned to: the leader it reaches, the address
/// it was dialed at, and the TLS material generation it presented.
///
/// All three, because all three are reasons the cached connection is the wrong
/// one to speak over. A re-election names a different leader; a membership
/// change moves the same leader's address; a rotation retires the leaf the
/// channel froze into its `ClientTlsConfig` when it was built. The generation
/// term is the same check the raft mesh's peer-channel map makes (ADR 0037 §4),
/// for exactly that last reason.
#[derive(Clone, PartialEq, Eq, Debug)]
struct ChannelKey {
    leader: CoordinatorId,
    addr: String,
    tls_generation: u64,
}

/// The one channel the ADR 0040 liveness read keeps, behind the key it was
/// dialed under.
///
/// One slot, not a map: a cluster has one leader at a time, so a second entry
/// could only ever be a stale one. Generic over the client purely so the reuse
/// and invalidation rules can be tested without a socket; the daemon
/// instantiates it at `Client<Channel>`.
///
/// The mutex is a `std` one and every method takes it, uses it, and drops it
/// without awaiting — the dial itself happens outside the lock, so two
/// concurrent first reads may both dial and the second simply replaces the
/// first's entry. Racing to dial twice is cheaper than serializing every node
/// page behind one connect.
struct ChannelCache<C>(Mutex<Option<(ChannelKey, C)>>);

impl<C> Default for ChannelCache<C> {
    fn default() -> Self {
        ChannelCache(Mutex::new(None))
    }
}

impl<C: Clone> ChannelCache<C> {
    /// The cached client, if it was dialed under exactly this key.
    fn get(&self, key: &ChannelKey) -> Option<C> {
        let cache = self.0.lock().expect("liveness read channel cache poisoned");
        cache
            .as_ref()
            .filter(|(cached, _)| cached == key)
            .map(|(_, client)| client.clone())
    }

    fn put(&self, key: ChannelKey, client: C) {
        *self.0.lock().expect("liveness read channel cache poisoned") = Some((key, client));
    }

    /// Drop the cached client, but only if it is still the one this key names.
    ///
    /// Keyed rather than unconditional so a read whose connection broke cannot
    /// throw away a replacement some concurrent read has already dialed — the
    /// entry it is entitled to retire is its own.
    fn invalidate(&self, key: &ChannelKey) {
        let mut cache = self.0.lock().expect("liveness read channel cache poisoned");
        if cache.as_ref().is_some_and(|(cached, _)| cached == key) {
            *cache = None;
        }
    }
}

/// Whether a non-OK status means the *connection* failed rather than the leader
/// having answered over it.
///
/// Only these four retire a cached channel. Anything else — a
/// `FAILED_PRECONDITION` history-id mismatch, a `PERMISSION_DENIED` — is the
/// leader speaking, which is evidence the connection works and that redialing
/// would change nothing. tonic's `Channel` reconnects on its own, so the point
/// of dropping the entry is not to force a reconnect but to re-resolve the
/// leader and rebuild the TLS config while we are at it.
fn is_transport_failure(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::Unknown | Code::Cancelled | Code::DeadlineExceeded
    )
}

/// Forwards client writes to the leader over the mTLS admin channel.
pub(crate) struct AdminForwarder {
    /// Resolves the leader's raft address; read per call, never cached, so a
    /// membership change between two writes is picked up immediately.
    node: NodeHandle,
    /// This replica's machine-plane identity for the dial. Read at dial time
    /// (not frozen) so a rotated leaf is presented on the next hop, exactly
    /// as [`crate::enroll::EnrollService`] does it.
    tls: Arc<TlsStore>,
    /// The cached channel for the ADR 0040 liveness read, and *only* for it.
    ///
    /// The write path keeps dialing per call (ADR 0038): it is rare enough that
    /// a handshake costs nothing anybody can measure, and a fresh dial is one
    /// less piece of state to reason about on the path that mutates the cluster.
    /// The read is the opposite shape — one per follower-served node list,
    /// detail, or overview request — so a dial per call would put dashboard-rate
    /// TCP and TLS churn on the leader.
    read_channel: ChannelCache<Client<Channel>>,
}

impl AdminForwarder {
    pub(crate) fn new(node: NodeHandle, tls: Arc<TlsStore>) -> Arc<AdminForwarder> {
        Arc::new(AdminForwarder {
            node,
            tls,
            read_channel: ChannelCache::default(),
        })
    }

    /// The leader's raft address in this replica's membership view.
    ///
    /// A leader with no address here is the fallback case, not a failure:
    /// nothing can be forwarded, so the client gets the hintless redirect and
    /// retries. A leader that *has* an address but will not answer is a genuine
    /// unavailability, which is the dial's business rather than this lookup's.
    fn leader_addr(&self, leader: CoordinatorId) -> Result<String, ApiError> {
        let summary = self.node.cluster_summary();
        summary
            .members
            .iter()
            .find(|m| m.id == leader)
            .map(|m| m.addr.clone())
            .ok_or_else(|| {
                tracing::debug!(
                    leader,
                    "cannot reach the leader over the admin channel: it has no address in this \
                     replica's membership view"
                );
                ApiError::NotLeader { leader_hint: None }
            })
    }

    /// Dial the leader's admin surface, or say why not.
    ///
    /// The write path's dial: one handshake per forwarded write, never cached.
    ///
    /// `budget` is the caller's whole allowance, named only so the timeout
    /// message quotes the budget that actually applied.
    async fn dial(
        &self,
        leader: CoordinatorId,
        deadline: tokio::time::Instant,
        budget: std::time::Duration,
    ) -> Result<(Client<Channel>, [u8; 16]), ApiError> {
        let addr = self.leader_addr(leader)?;
        let client = under_dial_timeout(
            deadline,
            budget,
            crate::admin::admin_channel_from_store(&addr, &self.tls),
        )
        .await?;
        Ok((client, self.node.history_id()))
    }

    /// The channel the ADR 0040 liveness read speaks over: the cached one when
    /// it still matches the leader, the address, and the TLS generation, a fresh
    /// dial under `deadline` when it does not.
    ///
    /// The key comes back with the client so the caller can retire *that* entry
    /// — and nothing else — if the RPC on it turns out to have failed at the
    /// transport.
    async fn read_client(
        &self,
        leader: CoordinatorId,
        deadline: tokio::time::Instant,
    ) -> Result<(Client<Channel>, ChannelKey, [u8; 16]), ApiError> {
        let key = ChannelKey {
            leader,
            addr: self.leader_addr(leader)?,
            tls_generation: self.tls.generation(),
        };
        if let Some(client) = self.read_channel.get(&key) {
            return Ok((client, key, self.node.history_id()));
        }
        let client = under_dial_timeout(
            deadline,
            LIVENESS_FETCH_TIMEOUT,
            crate::admin::admin_channel_from_store(&key.addr, &self.tls),
        )
        .await?;
        self.read_channel.put(key.clone(), client.clone());
        Ok((client, key, self.node.history_id()))
    }
}

/// The answer for every way the dial can end badly.
///
/// A dial that failed and a dial that never finished are the same fact:
/// nothing left this replica, so the outcome is *known* — the write did not
/// commit, and the client may retry it anywhere, including against a replica
/// that turns out to be the leader. That is the distinction this message
/// carries and [`under_timeout`]'s does not: once the request is on the wire,
/// silence means the leader may have committed it.
fn not_sent(detail: String) -> ApiError {
    ApiError::Unavailable(format!(
        "could not reach the leader to forward the write: {detail}; nothing was sent, so the \
         write did not commit — it is safe to retry against any replica"
    ))
}

/// Dial under the write's shared deadline — the *first* claim on it, not a
/// budget of its own.
///
/// Split out from [`AdminForwarder::dial`] so the budget is testable against a
/// future that never resolves. The failure it exists for — an address that
/// neither answers nor refuses — is exactly the one a unit test cannot
/// manufacture from a real socket: a listener it binds accepts, and one it
/// does not bind refuses.
async fn under_dial_timeout<T>(
    deadline: tokio::time::Instant,
    budget: std::time::Duration,
    dial: impl std::future::Future<Output = anyhow::Result<T>>,
) -> Result<T, ApiError> {
    match tokio::time::timeout_at(deadline, dial).await {
        Ok(Ok(client)) => Ok(client),
        Ok(Err(e)) => Err(not_sent(format!("{e:#}"))),
        Err(_elapsed) => Err(not_sent(format!(
            "the connection did not establish within the {}s budget",
            budget.as_secs()
        ))),
    }
}

/// Run one forwarding call under whatever the dial left of the write's shared
/// deadline, collapsing both failure shapes onto the retriable answer.
///
/// A timeout here means the outcome is genuinely **unknown**: the leader may
/// have committed the write and lost the answer on the way back. That is
/// exactly what `UNAVAILABLE` promises — "did not resolve to a replicated
/// decision" — and it is safe to retry because every write on this path
/// carries a client-minted identity (ADR 0026), so a repeat resolves as an
/// accepted no-op rather than a second job or a second entity. What must
/// never happen is reporting success for a write this replica cannot vouch
/// for.
async fn under_timeout<F, T>(deadline: tokio::time::Instant, call: F) -> Result<T, ApiError>
where
    F: std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    match tokio::time::timeout_at(deadline, call).await {
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(status)) => Err(ApiError::Unavailable(format!(
            "the leader did not complete the forwarded write: {}",
            status.message()
        ))),
        Err(_elapsed) => Err(ApiError::Unavailable(
            "the leader did not answer the forwarded write within the 10s forwarding budget \
             (dial included); the outcome is unknown and the request may still commit — retry \
             it unchanged (its id makes the retry a no-op if it did)"
                .to_string(),
        )),
    }
}

/// Map the leader's outcome onto this replica's answer to the client.
///
/// Returns the apply index on success; every refusal becomes the error the
/// client would have seen from the leader directly, which is the whole point
/// of carrying the outcome in the body rather than in a status code.
fn applied_index(outcome: Option<pb::ForwardWriteOutcome>) -> Result<u64, ApiError> {
    use pb::forward_write_outcome::Outcome;

    match outcome.and_then(|o| o.outcome) {
        Some(Outcome::Applied(applied)) => Ok(applied.log_index),
        Some(Outcome::Rejected(rejected)) => Err(ApiError::ForwardedRejection {
            kind: rejection_kind_from_pb(rejected.kind),
            reason: rejected.reason,
        }),
        Some(Outcome::Invalid(invalid)) => Err(ApiError::Invalid(invalid.message)),
        // The hop landed on a coordinator that is not the leader. One hop is
        // the rule, so this is where forwarding stops and the client gets the
        // redirect it would have got before ADR 0038.
        Some(Outcome::NotLeader(_)) => Err(ApiError::NotLeader { leader_hint: None }),
        // A response with no outcome at all is a peer speaking a schema this
        // one does not understand; it is not a decision, so it is not
        // reported as one.
        None => Err(ApiError::Unavailable(
            "the leader answered the forwarded write with no outcome".to_string(),
        )),
    }
}

/// The leader's rejection classification, as this replica's [`RejectionKind`]
/// (ADR 0023).
///
/// An unrecognized value decodes to [`RejectionKind::Other`], the 409 every
/// rejection was before the classification existed — the honest answer when a
/// peer tells us something this build has no mapping for, and never a guess at
/// a stricter status.
pub(crate) fn rejection_kind_from_pb(raw: i32) -> RejectionKind {
    use pb::forward_write_outcome::RejectionKind as Pb;
    match Pb::try_from(raw) {
        Ok(Pb::PermissionDenied) => RejectionKind::PermissionDenied,
        Ok(Pb::UnknownQuotaEntity) => RejectionKind::UnknownQuotaEntity,
        Ok(Pb::InvalidAuthorization) => RejectionKind::InvalidAuthorization,
        Ok(Pb::AuthorizationLockout) => RejectionKind::AuthorizationLockout,
        Ok(Pb::UnknownNode) => RejectionKind::UnknownNode,
        Ok(Pb::InvalidQuotaEntityName) => RejectionKind::InvalidQuotaEntityName,
        Ok(Pb::Unspecified) | Err(_) => RejectionKind::Other,
    }
}

/// The classification the leader puts on the wire — the inverse of
/// [`rejection_kind_from_pb`].
pub(crate) fn rejection_kind_to_pb(
    kind: RejectionKind,
) -> pb::forward_write_outcome::RejectionKind {
    use pb::forward_write_outcome::RejectionKind as Pb;
    match kind {
        RejectionKind::Other => Pb::Unspecified,
        RejectionKind::PermissionDenied => Pb::PermissionDenied,
        RejectionKind::UnknownQuotaEntity => Pb::UnknownQuotaEntity,
        RejectionKind::InvalidAuthorization => Pb::InvalidAuthorization,
        RejectionKind::AuthorizationLockout => Pb::AuthorizationLockout,
        RejectionKind::UnknownNode => Pb::UnknownNode,
        RejectionKind::InvalidQuotaEntityName => Pb::InvalidQuotaEntityName,
    }
}

impl LeaderWrites for AdminForwarder {
    fn submit_job<'a>(
        &'a self,
        leader: CoordinatorId,
        req: &'a ResolvedSubmitJobRequest,
        actor: &'a Actor,
    ) -> BoxFuture<'a, Result<SubmitJobResponse, ApiError>> {
        Box::pin(async move {
            let deadline = forward_deadline();
            let (mut client, history_id) = self.dial(leader, deadline, FORWARD_TIMEOUT).await?;
            let wire = submit_to_pb(history_id, req, actor);
            let response = under_timeout(deadline, client.forward_submit_job(wire)).await?;
            let log_index = applied_index(response.outcome)?;
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
        reason: Option<&'a str>,
        actor: &'a Actor,
    ) -> BoxFuture<'a, Result<(), ApiError>> {
        Box::pin(async move {
            let deadline = forward_deadline();
            let (mut client, history_id) = self.dial(leader, deadline, FORWARD_TIMEOUT).await?;
            let wire = pb::ForwardAbortJobRequest {
                history_id: history_id.to_vec(),
                job: Some(job.into()),
                reason: reason.map(str::to_string),
                actor: Some(actor.into()),
            };
            let response = under_timeout(deadline, client.forward_abort_job(wire)).await?;
            applied_index(response.outcome)?;
            Ok(())
        })
    }

    fn configure_quota_entity<'a>(
        &'a self,
        leader: CoordinatorId,
        req: &'a ResolvedConfigureQuotaEntityRequest,
        actor: &'a Actor,
    ) -> BoxFuture<'a, Result<QuotaEntityConfigured, ApiError>> {
        Box::pin(async move {
            let deadline = forward_deadline();
            let (mut client, history_id) = self.dial(leader, deadline, FORWARD_TIMEOUT).await?;
            let wire = pb::ForwardConfigureQuotaEntityRequest {
                history_id: history_id.to_vec(),
                entity: Some(req.entity.into()),
                parent: req.parent.map(Into::into),
                name: req.name.clone(),
                quota_ucu: req.quota_ucu,
                actor: Some(actor.into()),
            };
            let response =
                under_timeout(deadline, client.forward_configure_quota_entity(wire)).await?;
            let log_index = applied_index(response.outcome)?;
            Ok(QuotaEntityConfigured {
                entity: req.entity,
                log_index,
            })
        })
    }

    /// The bindings themselves cross as
    /// [`coppice.core.v1.Binding`](coppice_proto::pb::core::v1::Binding) — the
    /// same message the command log carries — rather than a second encoding of
    /// the same idea. A malformed one is refused by the *leader's* conversion,
    /// not filtered out here: what crosses the hop is the client's request.
    fn update_authorization<'a>(
        &'a self,
        leader: CoordinatorId,
        req: &'a ResolvedUpdateAuthorizationRequest,
        actor: &'a Actor,
    ) -> BoxFuture<'a, Result<UpdateAuthorizationResponse, ApiError>> {
        Box::pin(async move {
            let deadline = forward_deadline();
            let (mut client, history_id) = self.dial(leader, deadline, FORWARD_TIMEOUT).await?;
            let wire = authorization_to_pb(history_id, req, actor)?;
            let response =
                under_timeout(deadline, client.forward_update_authorization(wire)).await?;
            let log_index = applied_index(response.outcome)?;
            Ok(UpdateAuthorizationResponse { log_index })
        })
    }

    fn set_node_schedulable<'a>(
        &'a self,
        leader: CoordinatorId,
        node: NodeId,
        schedulable: bool,
        actor: &'a Actor,
    ) -> BoxFuture<'a, Result<(), ApiError>> {
        Box::pin(async move {
            let deadline = forward_deadline();
            let (mut client, history_id) = self.dial(leader, deadline, FORWARD_TIMEOUT).await?;
            let wire = pb::ForwardSetNodeSchedulableRequest {
                history_id: history_id.to_vec(),
                node: Some(node.into()),
                schedulable,
                actor: Some(actor.into()),
            };
            let response =
                under_timeout(deadline, client.forward_set_node_schedulable(wire)).await?;
            applied_index(response.outcome)?;
            Ok(())
        })
    }

    fn evict_nodes<'a>(
        &'a self,
        leader: CoordinatorId,
        nodes: &'a [NodeId],
        actor: &'a Actor,
    ) -> BoxFuture<'a, Result<(), ApiError>> {
        Box::pin(async move {
            let deadline = forward_deadline();
            let (mut client, history_id) = self.dial(leader, deadline, FORWARD_TIMEOUT).await?;
            let wire = pb::ForwardEvictNodesRequest {
                history_id: history_id.to_vec(),
                nodes: nodes.iter().map(|node| (*node).into()).collect(),
                actor: Some(actor.into()),
            };
            let response = under_timeout(deadline, client.forward_evict_nodes(wire)).await?;
            applied_index(response.outcome)?;
            Ok(())
        })
    }

    /// The ADR 0042 metadata write. `update` crosses as the domain
    /// [`JobMetadataUpdate`] enum's own oneof shape — `Replace` becomes the
    /// wire's `replace` arm, `Patch` becomes `patch` — the same conversion
    /// the command log itself uses (`coppice_proto::convert::command_to_pb`),
    /// so a forwarded write and a directly-proposed one encode identically.
    fn update_job_metadata<'a>(
        &'a self,
        leader: CoordinatorId,
        job: JobId,
        update: &'a JobMetadataUpdate,
        actor: &'a Actor,
    ) -> BoxFuture<'a, Result<u64, ApiError>> {
        Box::pin(async move {
            let deadline = forward_deadline();
            let (mut client, history_id) = self.dial(leader, deadline, FORWARD_TIMEOUT).await?;
            use pb::forward_update_job_metadata_request::Update;
            let update = match update {
                JobMetadataUpdate::Replace(metadata) => {
                    Update::Replace(metadata_map_to_pb(metadata))
                }
                JobMetadataUpdate::Patch { set, unset } => {
                    Update::Patch(coppice_proto::pb::command::v1::MetadataPatch {
                        set: metadata_to_pb(set),
                        unset: unset.iter().cloned().collect(),
                    })
                }
            };
            let wire = pb::ForwardUpdateJobMetadataRequest {
                history_id: history_id.to_vec(),
                job: Some(job.into()),
                update: Some(update),
                actor: Some(actor.into()),
            };
            let response =
                under_timeout(deadline, client.forward_update_job_metadata(wire)).await?;
            applied_index(response.outcome)
        })
    }
}

impl LeaderReads for AdminForwarder {
    /// One hop to the leader for its liveness marks (ADR 0040), over the one
    /// cached admin channel [`AdminForwarder::read_client`] keeps and under
    /// [`LIVENESS_FETCH_TIMEOUT`].
    ///
    /// Every failure is an `Err` the control plane turns into an empty map, and
    /// every one of them is logged at **debug**, never warn: an election, a
    /// membership view a beat behind, or a leader that has just stepped down
    /// all reach here, and a dashboard polling every few seconds would turn any
    /// of them into a torrent of warnings about a field that degraded exactly
    /// as designed.
    ///
    /// The errors [`under_dial_timeout`] builds are phrased for the write
    /// path — the only caller that ever shows one to anybody. This read
    /// discards the text and logs its own line, so that wording never reaches
    /// a human through here.
    ///
    /// A failure at the transport also retires the cached channel, so the next
    /// read redials: not because a `Channel` cannot reconnect by itself (it can,
    /// lazily), but because a connection that just broke is the moment to
    /// re-resolve the leader's address and rebuild the mTLS config from current
    /// material rather than to keep retrying the one that failed.
    fn node_liveness(
        &self,
        leader: CoordinatorId,
    ) -> BoxFuture<'_, Result<coppice_api::LivenessMarks, ApiError>> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + LIVENESS_FETCH_TIMEOUT;
            let (mut client, key, history_id) =
                self.read_client(leader, deadline).await.inspect_err(|e| {
                    tracing::debug!(
                        leader,
                        error = %e,
                        "could not reach the leader for its liveness marks; node health \
                         degrades to unknown"
                    );
                })?;

            let wire = pb::FetchNodeLivenessRequest {
                history_id: history_id.to_vec(),
            };
            let response =
                match tokio::time::timeout_at(deadline, client.fetch_node_liveness(wire)).await {
                    Ok(Ok(response)) => response.into_inner(),
                    Ok(Err(status)) => {
                        // A broken connection retires the entry; a refusal the
                        // leader actually spoke leaves it in place, since it is
                        // evidence the channel works.
                        if is_transport_failure(&status) {
                            self.read_channel.invalidate(&key);
                        }
                        tracing::debug!(
                        leader,
                        status = status.message(),
                        "the leader refused the liveness fetch; node health degrades to unknown"
                    );
                        return Err(ApiError::Unavailable(
                            "the leader did not serve its liveness marks".to_string(),
                        ));
                    }
                    Err(_elapsed) => {
                        // Silence is indistinguishable from a wedged
                        // connection, so the cached one does not get a second
                        // page's worth of trust.
                        self.read_channel.invalidate(&key);
                        tracing::debug!(
                            leader,
                            budget_secs = LIVENESS_FETCH_TIMEOUT.as_secs(),
                            "the leader did not answer the liveness fetch inside the budget; node \
                         health degrades to unknown"
                        );
                        return Err(ApiError::Unavailable(
                            "the leader did not answer the liveness fetch in time".to_string(),
                        ));
                    }
                };

            marks_from_pb(response.outcome).inspect_err(|e| {
                tracing::debug!(
                    leader,
                    error = %e,
                    "the leader's liveness answer was not usable; node health degrades to unknown"
                );
            })
        })
    }
}

/// The leader's liveness answer, as the marks the read model projects — or the
/// `Err` that becomes an empty map.
///
/// The `NotLeader` arm is where one hop stops: the receiver's answer is not
/// "nobody knows", it is "not me", and this side serves `unknown` rather than
/// chasing whoever does. A response with no arm at all is a peer speaking a
/// schema this build does not understand, which is not an answer either.
fn marks_from_pb(
    outcome: Option<pb::fetch_node_liveness_response::Outcome>,
) -> Result<coppice_api::LivenessMarks, ApiError> {
    use pb::fetch_node_liveness_response::Outcome;

    let marks = match outcome {
        Some(Outcome::Marks(marks)) => marks,
        Some(Outcome::NotLeader(_)) => return Err(ApiError::NotLeader { leader_hint: None }),
        None => {
            return Err(ApiError::Unavailable(
                "the leader answered the liveness fetch with no outcome".to_string(),
            ))
        }
    };

    marks.nodes.into_iter().map(mark_from_pb).collect()
}

/// One mark off the wire.
///
/// Fallible in both fields, and refusing rather than defaulting in both: a node
/// id that does not parse names no node, and a `last_heartbeat_at_us` outside
/// the representable range is not a heartbeat — substituting the epoch would
/// display 1970 as the last time a healthy node was heard from. Neither can
/// happen between builds of this tree; the point is that a peer which sent one
/// costs this read its whole answer (health `unknown`) instead of a plausible
/// wrong one.
fn mark_from_pb(
    mark: pb::NodeLivenessMark,
) -> Result<(coppice_core::id::NodeId, coppice_api::LivenessMark), ApiError> {
    let node: coppice_core::id::NodeId = required(mark.node, "NodeLivenessMark.node")
        .and_then(TryInto::try_into)
        .map_err(|e: ConvertError| ApiError::Invalid(e.to_string()))?;
    let last_heartbeat = match mark.last_heartbeat_at_us {
        Some(at_us) => Some(coppice_core::time::Timestamp::from_micros(at_us).ok_or_else(
            || ApiError::Invalid(format!("NodeLivenessMark.last_heartbeat_at_us {at_us} is not a representable timestamp")),
        )?),
        None => None,
    };
    Ok((
        node,
        coppice_api::LivenessMark {
            last_heartbeat,
            silent_for: std::time::Duration::from_micros(mark.silent_for_us),
        },
    ))
}

/// One mark on the wire — the leader's side of [`mark_from_pb`].
///
/// The monotonic span is computed by the leader before it encodes, so the
/// `silent_for` this read model derives health from never depends on two
/// coordinators' clocks agreeing (ADR 0040).
pub(crate) fn mark_to_pb(
    node: coppice_core::id::NodeId,
    mark: &coppice_api::LivenessMark,
) -> pb::NodeLivenessMark {
    pb::NodeLivenessMark {
        node: Some(node.into()),
        last_heartbeat_at_us: mark.last_heartbeat.map(|at| at.as_micros()),
        // Saturating rather than wrapping: a span past u64 µs (≈584 000 years)
        // is not reachable, and the clamp keeps the cast from being the one
        // place a nonsense value could become a *small* one.
        silent_for_us: mark.silent_for.as_micros().min(u128::from(u64::MAX)) as u64,
    }
}

// ---------------------------------------------------------------------------
// DTO ⇄ pb at the forwarding boundary
// ---------------------------------------------------------------------------
//
// `coppice-api` speaks DTOs and deliberately does not depend on
// `coppice-proto` (ADR 0031), so the conversion lives here — the same
// arrangement the log-fetch seam uses. Resources go through
// `coppice_proto::convert`'s canonicalizing boundary rather than being
// re-encoded by hand: three scalars are easy to transpose, and the boundary
// already rejects duplicate and unknown kinds.

/// Unwrap a required message field (prost decodes them as `Option`), the
/// local twin of `coppice_proto::convert`'s own — which is private to that
/// crate's boundary and has no business being public for one caller.
fn required<T>(field: Option<T>, name: &'static str) -> Result<T, ConvertError> {
    field.ok_or(ConvertError::MissingField(name))
}

/// The actor a forwarded request arrived with, as the domain type the leader's
/// write path takes.
///
/// A **required** field in practice, though the proto spells it optional:
/// every client write carries an actor (the authentication layer refuses the
/// request otherwise), so its absence here is a malformed forwarded request.
/// Defaulting it would be the dangerous reading — an empty `Actor` is not an
/// anonymous one, it is a principal named `""` with no groups, and letting one
/// through would silently convert a lost field into a denial or, worse, into a
/// grant if a binding ever named the empty string.
fn actor_from_pb(
    actor: Option<coppice_proto::pb::core::v1::Actor>,
    name: &'static str,
) -> Result<Actor, ConvertError> {
    Ok(required(actor, name)?.into())
}

/// The client's submission, on the wire.
///
/// `metadata` crosses unvalidated: this replica's own
/// [`api_server::submit_job_here`] already ran `metadata::validate` before
/// landing in the `NotLeader` arm that leads here, and the leader re-runs
/// the whole write path regardless of what this replica verified
/// (ADR 0042's two-place enforcement).
pub(crate) fn submit_to_pb(
    history_id: [u8; 16],
    req: &ResolvedSubmitJobRequest,
    actor: &Actor,
) -> pb::ForwardSubmitJobRequest {
    let requests: coppice_core::resource::Resources = req.requests.into();
    pb::ForwardSubmitJobRequest {
        actor: Some(actor.into()),
        history_id: history_id.to_vec(),
        job: Some(req.job.into()),
        image: req.image.clone(),
        command: req.command.clone(),
        entrypoint: req
            .entrypoint
            .as_ref()
            .map(|argv| coppice_proto::pb::core::v1::Entrypoint { argv: argv.clone() }),
        requests: Some((&requests).into()),
        priority: req.priority,
        max_runtime_seconds: req.max_runtime_seconds,
        quota_entity: Some(req.quota_entity.into()),
        retry: req
            .retry
            .map(|r| coppice_core::job::RetryPolicy::from(r).into()),
        metadata: Some(metadata_map_to_pb(&req.metadata)),
        env: env_to_pb(&req.env),
    }
}

/// The submission again, back as the DTO the leader's write path takes.
///
/// Fallible where the DTO's own deserializer is: an id that does not parse or
/// a resource vector with a duplicate kind is a malformed request, and the
/// leader refuses it as one rather than defaulting it into something
/// plausible. Everything the DTO calls *optional* stays optional here — the
/// leader's validation, not this conversion, decides what is required.
pub(crate) fn submit_from_pb(
    req: pb::ForwardSubmitJobRequest,
) -> Result<(ResolvedSubmitJobRequest, Actor), ConvertError> {
    let actor = actor_from_pb(req.actor, "ForwardSubmitJobRequest.actor")?;
    let requests: coppice_core::resource::Resources =
        required(req.requests, "ForwardSubmitJobRequest.requests")?.try_into()?;
    let entrypoint = match req.entrypoint {
        // Present-but-empty is not a second spelling of absent: the DTO
        // rejects it and so does `core.v1.Job`'s conversion, so it must
        // survive the hop as itself and be refused by the leader's
        // validation, not silently normalized here.
        Some(e) => Some(e.argv),
        None => None,
    };
    // Absent is the empty map, the same reading `ResolvedSubmitJobRequest`'s own
    // `#[serde(default)]` gives it — not a missing-field error, since an
    // empty metadata object is indistinguishable from one that was never
    // set once it has crossed the JSON boundary once already.
    let metadata = match req.metadata {
        Some(map) => metadata_from_pb(map.entries, "ForwardSubmitJobRequest.metadata")?,
        None => coppice_core::metadata::JobMetadata::new(),
    };
    // A plain `repeated`, not a wrapper message like `metadata` above — an
    // empty list is the empty map, with no absent/empty distinction to
    // preserve.
    let env = env_from_pb(req.env, "ForwardSubmitJobRequest.env")?;
    Ok((
        ResolvedSubmitJobRequest {
            job: required(req.job, "ForwardSubmitJobRequest.job")?.try_into()?,
            image: req.image,
            command: req.command,
            entrypoint,
            env,
            requests: (&requests).into(),
            priority: req.priority,
            max_runtime_seconds: req.max_runtime_seconds,
            quota_entity: required(req.quota_entity, "ForwardSubmitJobRequest.quota_entity")?
                .try_into()?,
            metadata,
            retry: req.retry.map(|r| {
                let core: coppice_core::job::RetryPolicy = r.into();
                coppice_api::http::dto::RetryPolicy {
                    max_retries: core.max_retries,
                    retry_user_errors: core.retry_user_errors,
                }
            }),
        },
        actor,
    ))
}

/// The quota upsert, back as the DTO the leader's write path takes, with the
/// actor the originating replica resolved.
pub(crate) fn configure_from_pb(
    req: pb::ForwardConfigureQuotaEntityRequest,
) -> Result<(ResolvedConfigureQuotaEntityRequest, Actor), ConvertError> {
    let actor = actor_from_pb(req.actor, "ForwardConfigureQuotaEntityRequest.actor")?;
    Ok((
        ResolvedConfigureQuotaEntityRequest {
            entity: required(req.entity, "ForwardConfigureQuotaEntityRequest.entity")?
                .try_into()?,
            parent: req.parent.map(TryInto::try_into).transpose()?,
            name: req.name,
            quota_ucu: req.quota_ucu,
        },
        actor,
    ))
}

/// The abort's actor. The abort has no DTO body to rebuild — `job` and
/// `reason` are read straight off the message by the handler — so only the
/// actor needs the conversion boundary.
pub(crate) fn abort_actor_from_pb(
    actor: Option<coppice_proto::pb::core::v1::Actor>,
) -> Result<Actor, ConvertError> {
    actor_from_pb(actor, "ForwardAbortJobRequest.actor")
}

/// A forwarded cordon's actor. Like the abort's: the node and the flag are
/// read straight off the message by the handler, so only the actor needs a
/// conversion boundary of its own.
pub(crate) fn set_node_schedulable_actor_from_pb(
    actor: Option<coppice_proto::pb::core::v1::Actor>,
) -> Result<Actor, ConvertError> {
    actor_from_pb(actor, "ForwardSetNodeSchedulableRequest.actor")
}

/// A forwarded eviction's actor.
pub(crate) fn evict_nodes_actor_from_pb(
    actor: Option<coppice_proto::pb::core::v1::Actor>,
) -> Result<Actor, ConvertError> {
    actor_from_pb(actor, "ForwardEvictNodesRequest.actor")
}

/// A forwarded metadata write, back as the job it names, the domain
/// [`JobMetadataUpdate`] it carries, and the actor that made the request.
///
/// Unlike the abort/cordon/eviction actor-only helpers above, this one has a
/// body to rebuild: `job` and the `update` oneof, decoded with the same
/// `coppice_proto::convert` helpers the command log itself uses — a
/// forwarded write is refused by exactly the same rule a direct one would
/// be.
pub(crate) fn update_job_metadata_from_pb(
    req: pb::ForwardUpdateJobMetadataRequest,
) -> Result<(JobId, JobMetadataUpdate, Actor), ConvertError> {
    use pb::forward_update_job_metadata_request::Update;
    let actor = actor_from_pb(req.actor, "ForwardUpdateJobMetadataRequest.actor")?;
    let job: JobId = required(req.job, "ForwardUpdateJobMetadataRequest.job")?.try_into()?;
    let update = match required(req.update, "ForwardUpdateJobMetadataRequest.update")? {
        Update::Replace(map) => JobMetadataUpdate::Replace(metadata_from_pb(
            map.entries,
            "ForwardUpdateJobMetadataRequest.replace",
        )?),
        Update::Patch(patch) => {
            let set = metadata_from_pb(patch.set, "ForwardUpdateJobMetadataRequest.patch.set")?;
            // Any order, no duplicates, no empty key — the same reader rule
            // the command conversion applies.
            let mut unset = std::collections::BTreeSet::new();
            for key in patch.unset {
                if key.is_empty() {
                    return Err(ConvertError::Invalid {
                        field: "ForwardUpdateJobMetadataRequest.patch.unset",
                        reason: "metadata key must not be empty",
                    });
                }
                if !unset.insert(key) {
                    return Err(ConvertError::DuplicateEntry(
                        "ForwardUpdateJobMetadataRequest.patch.unset",
                    ));
                }
            }
            JobMetadataUpdate::Patch { set, unset }
        }
    };
    Ok((job, update, actor))
}

/// The authorization replacement, on the wire.
///
/// Fallible where the direct path is: a binding naming both or neither of
/// `group`/`principal` has no `coppice.core.v1.Binding` to become, and that is
/// an `Invalid` request rather than something to encode as a guess. The HTTP
/// handler on this replica has already refused it, so reaching the error here
/// would take a caller that bypassed the router.
fn authorization_to_pb(
    history_id: [u8; 16],
    req: &ResolvedUpdateAuthorizationRequest,
    actor: &Actor,
) -> Result<pb::ForwardUpdateAuthorizationRequest, ApiError> {
    let mut bindings = Vec::with_capacity(req.bindings.len());
    for (i, dto) in req.bindings.iter().enumerate() {
        let binding = coppice_state::authz::Binding::try_from(dto)
            .map_err(|e| ApiError::Invalid(format!("binding {i}: {e}")))?;
        bindings.push((&binding).into());
    }
    Ok(pb::ForwardUpdateAuthorizationRequest {
        history_id: history_id.to_vec(),
        bindings,
        groups_claim: req.groups_claim.clone(),
        actor: Some(actor.into()),
    })
}

/// The authorization replacement, back as the DTO the leader's write path
/// takes.
///
/// Round-tripping through the DTO rather than handing the leader the domain
/// `Binding`s directly keeps one rule intact: the leader re-runs the *whole*
/// write path on the client's request, conversions included, so a forwarded
/// write and a direct one cannot be validated differently.
pub(crate) fn authorization_from_pb(
    req: pb::ForwardUpdateAuthorizationRequest,
) -> Result<(ResolvedUpdateAuthorizationRequest, Actor), ConvertError> {
    let actor = actor_from_pb(req.actor, "ForwardUpdateAuthorizationRequest.actor")?;
    let bindings = coppice_proto::convert::bindings_from_pb(req.bindings)?;
    Ok((
        ResolvedUpdateAuthorizationRequest {
            groups_claim: req.groups_claim,
            bindings: bindings.iter().map(Into::into).collect(),
        },
        actor,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_api::http::dto;
    use coppice_core::id::QuotaEntityId;

    const HISTORY: [u8; 16] = [7; 16];

    /// A bearer-authenticated actor with groups — the shape whose loss would
    /// be silent: an actor missing its groups still authenticates, and simply
    /// stops matching every group binding it should have.
    fn actor() -> Actor {
        Actor {
            principal: "user-42".to_string(),
            groups: vec!["batch-users".to_string(), "sre".to_string()],
            operator_cert: false,
            auth_disabled: false,
        }
    }

    fn submit_request() -> ResolvedSubmitJobRequest {
        ResolvedSubmitJobRequest {
            job: JobId::new(),
            image: "busybox:latest".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "true".to_string()],
            entrypoint: Some(vec!["/bin/sh".to_string()]),
            requests: dto::Resources {
                cpu_millis: 1500,
                memory_bytes: 1 << 30,
                disk_bytes: 0,
            },
            priority: -3,
            max_runtime_seconds: Some(3600),
            quota_entity: QuotaEntityId::new(),
            retry: Some(dto::RetryPolicy {
                max_retries: 2,
                retry_user_errors: true,
            }),
            metadata: [("name".to_string(), "nightly-build".to_string())]
                .into_iter()
                .collect(),
            env: [("RUST_LOG".to_string(), "info".to_string())]
                .into_iter()
                .collect(),
        }
    }

    #[test]
    fn a_submission_survives_the_hop_field_for_field() {
        // The leader validates what the *client* sent, so every field has to
        // arrive as it left — including the ones this replica already checked.
        // A dropped `max_runtime_seconds` or a transposed resource dimension
        // would run a different job than the one asked for.
        let original = submit_request();
        let (round_tripped, round_tripped_actor) =
            submit_from_pb(submit_to_pb(HISTORY, &original, &actor()))
                .expect("a well-formed request");

        // The actor is part of "what the client sent" now (ADR 0023): the
        // leader stamps `submitted_by` from it and re-checks the bindings
        // against it, so a principal or a group lost on the hop is a
        // different authorization decision, silently.
        assert_eq!(round_tripped_actor, actor());
        assert_eq!(round_tripped.job, original.job);
        assert_eq!(round_tripped.image, original.image);
        assert_eq!(round_tripped.command, original.command);
        assert_eq!(round_tripped.entrypoint, original.entrypoint);
        assert_eq!(round_tripped.requests, original.requests);
        assert_eq!(round_tripped.priority, original.priority);
        assert_eq!(
            round_tripped.max_runtime_seconds,
            original.max_runtime_seconds
        );
        assert_eq!(round_tripped.quota_entity, original.quota_entity);
        assert_eq!(round_tripped.metadata, original.metadata);
        assert_eq!(round_tripped.env, original.env);
        let retry = round_tripped.retry.expect("retry policy survives");
        assert_eq!(retry.max_retries, 2);
        assert!(retry.retry_user_errors);
    }

    #[test]
    fn an_absent_entrypoint_stays_absent_and_an_empty_one_stays_empty() {
        // Two distinct meanings — "use the image's entrypoint" and "a
        // malformed override" — and the hop must not collapse them: the
        // second is refused by the *leader's* validation, so it has to get
        // there recognizable.
        let mut req = submit_request();
        req.entrypoint = None;
        let (absent, _) =
            submit_from_pb(submit_to_pb(HISTORY, &req, &actor())).expect("absent entrypoint");
        assert_eq!(absent.entrypoint, None);

        req.entrypoint = Some(Vec::new());
        let (empty, _) =
            submit_from_pb(submit_to_pb(HISTORY, &req, &actor())).expect("empty entrypoint");
        assert_eq!(empty.entrypoint, Some(Vec::new()));
    }

    /// An operator certificate and the open posture are flags on the actor,
    /// not a separate credential — and they are the two things that grant
    /// implicit unscoped admin. Dropping either on the hop would turn a
    /// break-glass abort into a 403; *inventing* either would hand the
    /// forwarded write unscoped admin it never had.
    #[test]
    fn the_implicit_admin_flags_survive_the_hop_in_both_directions() {
        for (operator_cert, auth_disabled) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let sent = Actor {
                principal: "cert:alice".to_string(),
                groups: Vec::new(),
                operator_cert,
                auth_disabled,
            };
            let (_, back) = submit_from_pb(submit_to_pb(HISTORY, &submit_request(), &sent))
                .expect("a well-formed request");
            assert_eq!(back, sent);
        }
    }

    /// A forwarded write with no actor is malformed, not anonymous.
    ///
    /// The dangerous default: an `Actor::default()` is a principal named `""`
    /// with no groups and no flags, which is not "the system's own authority"
    /// and not "an anonymous caller" — it is a *different* identity that
    /// happens to be denied everything, until the day a binding names the
    /// empty string. Refusing outright keeps the failure loud.
    #[test]
    fn a_forwarded_write_with_no_actor_is_refused_rather_than_defaulted() {
        let mut wire = submit_to_pb(HISTORY, &submit_request(), &actor());
        wire.actor = None;
        assert!(submit_from_pb(wire).is_err());

        let mut wire = pb::ForwardConfigureQuotaEntityRequest {
            history_id: HISTORY.to_vec(),
            entity: Some(QuotaEntityId::new().into()),
            parent: None,
            name: "platform".to_string(),
            quota_ucu: 1,
            actor: Some((&actor()).into()),
        };
        wire.actor = None;
        assert!(configure_from_pb(wire).is_err());

        assert!(abort_actor_from_pb(None).is_err());
    }

    /// The authorization replacement round-trips through the *shared*
    /// `coppice.core.v1.Binding` — the message the command log already
    /// carries — so both subject kinds and both scope states arrive as
    /// themselves.
    #[test]
    fn an_authorization_replacement_survives_the_hop() {
        let scope = QuotaEntityId::new();
        let original = ResolvedUpdateAuthorizationRequest {
            groups_claim: Some("entitlements".to_string()),
            bindings: vec![
                dto::BindingDto {
                    group: Some("platform".to_string()),
                    principal: None,
                    role: dto::BindingRole::Admin,
                    scope: None,
                },
                dto::BindingDto {
                    group: None,
                    principal: Some("svc-ci".to_string()),
                    role: dto::BindingRole::Submitter,
                    scope: Some(scope),
                },
            ],
        };
        let wire = authorization_to_pb(HISTORY, &original, &actor()).expect("well-formed bindings");
        let (back, back_actor) = authorization_from_pb(wire).expect("a well-formed replacement");

        assert_eq!(back_actor, actor());
        assert_eq!(back.groups_claim.as_deref(), Some("entitlements"));
        assert_eq!(back.bindings, original.bindings);
    }

    /// An absent `groups_claim` stays absent: it means "leave the replicated
    /// name alone", and collapsing it into the empty string would rename the
    /// claim to nothing on the far side.
    #[test]
    fn an_absent_groups_claim_stays_absent_across_the_hop() {
        let req = ResolvedUpdateAuthorizationRequest {
            groups_claim: None,
            bindings: Vec::new(),
        };
        let wire = authorization_to_pb(HISTORY, &req, &actor()).expect("an empty list encodes");
        let (back, _) = authorization_from_pb(wire).expect("a well-formed replacement");
        assert_eq!(back.groups_claim, None);
        // An empty list is a meaningful request — apply's lockout guard is
        // what refuses it — so it must not be confused with "no bindings".
        assert!(back.bindings.is_empty());
    }

    /// A binding naming both subject kinds has no `coppice.core.v1.Binding`
    /// to become, and the encoder says so rather than silently picking one.
    #[test]
    fn a_binding_with_two_subjects_is_invalid_before_it_reaches_the_wire() {
        let req = ResolvedUpdateAuthorizationRequest {
            groups_claim: None,
            bindings: vec![dto::BindingDto {
                group: Some("platform".to_string()),
                principal: Some("svc-ci".to_string()),
                role: dto::BindingRole::Admin,
                scope: None,
            }],
        };
        assert!(matches!(
            authorization_to_pb(HISTORY, &req, &actor()),
            Err(ApiError::Invalid(_))
        ));
    }

    /// Every classification survives the hop, in both directions, and an
    /// unrecognized one degrades to the ordinary 409 rather than to a
    /// stricter status this build cannot justify.
    #[test]
    fn a_rejection_classification_round_trips_and_degrades_safely() {
        for kind in [
            RejectionKind::Other,
            RejectionKind::PermissionDenied,
            RejectionKind::UnknownQuotaEntity,
            RejectionKind::InvalidAuthorization,
            RejectionKind::AuthorizationLockout,
            RejectionKind::UnknownNode,
            RejectionKind::InvalidQuotaEntityName,
        ] {
            assert_eq!(
                rejection_kind_from_pb(rejection_kind_to_pb(kind) as i32),
                kind
            );
        }
        assert_eq!(rejection_kind_from_pb(9999), RejectionKind::Other);
    }

    #[test]
    fn an_upsert_survives_the_hop_including_a_null_parent() {
        let entity = QuotaEntityId::new();
        let parent = QuotaEntityId::new();
        for expected_parent in [None, Some(parent)] {
            let req = ResolvedConfigureQuotaEntityRequest {
                entity,
                parent: expected_parent,
                name: "platform".to_string(),
                quota_ucu: 12_345,
            };
            let wire = pb::ForwardConfigureQuotaEntityRequest {
                history_id: HISTORY.to_vec(),
                entity: Some(req.entity.into()),
                parent: req.parent.map(Into::into),
                name: req.name.clone(),
                quota_ucu: req.quota_ucu,
                actor: Some((&actor()).into()),
            };
            let (back, back_actor) = configure_from_pb(wire).expect("a well-formed upsert");
            assert_eq!(back_actor, actor());
            assert_eq!(back.entity, entity);
            assert_eq!(back.parent, expected_parent);
            assert_eq!(back.name, "platform");
            assert_eq!(back.quota_ucu, 12_345);
        }
    }

    #[test]
    fn a_missing_required_field_is_a_malformed_request_not_a_default() {
        let mut wire = submit_to_pb(HISTORY, &submit_request(), &actor());
        wire.quota_entity = None;
        assert!(submit_from_pb(wire).is_err());
    }

    #[test]
    fn each_outcome_becomes_the_answer_the_client_would_have_got_from_the_leader() {
        use pb::forward_write_outcome::Outcome;

        let wrap = |outcome: Outcome| {
            Some(pb::ForwardWriteOutcome {
                outcome: Some(outcome),
            })
        };

        assert_eq!(
            applied_index(wrap(Outcome::Applied(pb::forward_write_outcome::Applied {
                log_index: 91
            })))
            .expect("applied"),
            91
        );
        assert!(matches!(
            applied_index(wrap(Outcome::Rejected(
                pb::forward_write_outcome::Rejected {
                    reason: "job 1 is terminal".to_string(),
                    kind: rejection_kind_to_pb(RejectionKind::Other) as i32,
                }
            ))),
            Err(ApiError::ForwardedRejection { kind: RejectionKind::Other, reason })
                if reason == "job 1 is terminal"
        ));
        // The classification the follower has to see: an apply-time ADR 0023
        // refusal on the leader is a 403 to the client, not the 409 that
        // every other rejection is.
        assert!(matches!(
            applied_index(wrap(Outcome::Rejected(
                pb::forward_write_outcome::Rejected {
                    reason: "permission denied: principal \"u\" may not …".to_string(),
                    kind: rejection_kind_to_pb(RejectionKind::PermissionDenied) as i32,
                }
            ))),
            Err(ApiError::ForwardedRejection {
                kind: RejectionKind::PermissionDenied,
                ..
            })
        ));
        assert!(matches!(
            applied_index(wrap(Outcome::Invalid(pb::forward_write_outcome::Invalid {
                message: "bad priority".to_string()
            }))),
            Err(ApiError::Invalid(_))
        ));
        // One hop: the far side is not the leader either, and the client gets
        // the redirect rather than another forward.
        assert!(matches!(
            applied_index(wrap(Outcome::NotLeader(
                pb::forward_write_outcome::NotLeader {}
            ))),
            Err(ApiError::NotLeader { leader_hint: None })
        ));
        // No outcome at all is not a decision, and must not be reported as
        // one.
        assert!(matches!(applied_index(None), Err(ApiError::Unavailable(_))));
    }

    /// The ADR 0040 mark survives the hop in both directions and in both of
    /// its shapes: heard-from (a wall stamp to display) and granted-grace-only
    /// (no stamp at all). Collapsing the second into an epoch stamp would show
    /// an operator "last heard 1970" for a node the leader has never heard
    /// from, and collapsing the first would lose the only displayable fact
    /// behind `last_heartbeat`.
    #[test]
    fn a_liveness_mark_survives_the_hop_in_both_shapes() {
        let node = coppice_core::id::NodeId::new();
        let at = coppice_core::time::Timestamp::now();

        for last_heartbeat in [Some(at), None] {
            let sent = coppice_api::LivenessMark {
                last_heartbeat,
                silent_for: std::time::Duration::from_millis(1_234),
            };
            let (back_node, back) =
                mark_from_pb(mark_to_pb(node, &sent)).expect("a well-formed mark");
            assert_eq!(back_node, node);
            assert_eq!(back.last_heartbeat, last_heartbeat);
            // The span is what health is derived from, so it must survive to
            // the microsecond, not to the second.
            assert_eq!(back.silent_for, sent.silent_for);
        }
    }

    /// Present-but-empty and absent are different answers, which is the whole
    /// reason the response is a oneof: a leader tracking no nodes has told us
    /// something, and a replica that is not the leader has not.
    #[test]
    fn an_empty_mark_list_from_the_leader_is_an_answer_and_not_leader_is_not() {
        use pb::fetch_node_liveness_response::{Marks, NotLeader, Outcome};

        let empty = marks_from_pb(Some(Outcome::Marks(Marks { nodes: Vec::new() })))
            .expect("a leader tracking nothing has answered");
        assert!(empty.is_empty());

        // One hop: the far side is not the leader either, so the read stops
        // here and the caller serves `unknown`.
        assert!(matches!(
            marks_from_pb(Some(Outcome::NotLeader(NotLeader {}))),
            Err(ApiError::NotLeader { leader_hint: None })
        ));

        // No arm at all is a peer speaking a schema this build does not know;
        // it is not an answer, so it must not be reported as an empty one.
        assert!(matches!(marks_from_pb(None), Err(ApiError::Unavailable(_))));
    }

    /// A mark this build cannot decode costs the read its whole answer —
    /// health `unknown` — rather than being dropped or defaulted into
    /// something plausible. A node id that does not parse names no node, and
    /// an unrepresentable stamp is not a heartbeat.
    #[test]
    fn a_malformed_mark_is_refused_rather_than_defaulted() {
        use pb::fetch_node_liveness_response::{Marks, Outcome};

        let node = coppice_core::id::NodeId::new();
        let good = mark_to_pb(
            node,
            &coppice_api::LivenessMark {
                last_heartbeat: None,
                silent_for: std::time::Duration::ZERO,
            },
        );

        let mut no_node = good.clone();
        no_node.node = None;
        assert!(marks_from_pb(Some(Outcome::Marks(Marks {
            nodes: vec![no_node]
        })))
        .is_err());

        let mut absurd_stamp = good.clone();
        absurd_stamp.last_heartbeat_at_us = Some(i64::MAX);
        assert!(marks_from_pb(Some(Outcome::Marks(Marks {
            nodes: vec![absurd_stamp]
        })))
        .is_err());

        // And one bad entry does not quietly leave the good ones behind: the
        // answer is all of it or none of it.
        let mut broken = good.clone();
        broken.node = None;
        assert!(marks_from_pb(Some(Outcome::Marks(Marks {
            nodes: vec![good, broken]
        })))
        .is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn a_liveness_dial_that_never_completes_is_bounded_by_its_own_budget() {
        // The same blackhole as the write path's, against the read's budget:
        // a node list must not hold a viewer's request for the write path's
        // ten seconds to decorate a field whose fallback is fine.
        let started = tokio::time::Instant::now();
        let never = std::future::pending::<anyhow::Result<()>>();
        let deadline = tokio::time::Instant::now() + LIVENESS_FETCH_TIMEOUT;
        assert!(
            under_dial_timeout(deadline, LIVENESS_FETCH_TIMEOUT, never)
                .await
                .is_err(),
            "bounded"
        );
        assert_eq!(started.elapsed(), LIVENESS_FETCH_TIMEOUT);
        assert!(
            LIVENESS_FETCH_TIMEOUT < FORWARD_TIMEOUT,
            "the read's budget is deliberately shorter than the write's"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_dial_that_never_completes_is_bounded_by_the_forward_budget() {
        // The blackholed leader address: connect neither succeeds nor fails.
        // Without a budget here the client's request outlives the 10s the
        // forwarding path advertises, by however long the kernel keeps
        // retransmitting SYNs. Virtual time, so nothing waits and nothing
        // flakes.
        let started = tokio::time::Instant::now();
        let never = std::future::pending::<anyhow::Result<()>>();
        let error = under_dial_timeout(forward_deadline(), FORWARD_TIMEOUT, never)
            .await
            .expect_err("bounded");
        assert!(started.elapsed() >= FORWARD_TIMEOUT);
        match error {
            ApiError::Unavailable(message) => {
                // Known-not-sent, not outcome-unknown: nothing reached the
                // leader, so the retry advice is unconditional.
                assert!(message.contains("nothing was sent"), "{message}");
                assert!(!message.contains("unknown"), "{message}");
            }
            other => panic!("a bounded dial must be retriable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_dial_that_fails_says_the_write_was_not_sent() {
        // The other half of the same fact, and the reason both arms share one
        // message: a refused connection and an abandoned one are equally
        // "never left this replica".
        let refused =
            std::future::ready::<anyhow::Result<()>>(Err(anyhow::anyhow!("connection refused")));
        match under_dial_timeout(forward_deadline(), FORWARD_TIMEOUT, refused)
            .await
            .expect_err("dial failed")
        {
            ApiError::Unavailable(message) => {
                assert!(message.contains("connection refused"), "{message}");
                assert!(message.contains("nothing was sent"), "{message}");
            }
            other => panic!("a failed dial must be retriable, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_call_says_the_outcome_is_unknown_instead() {
        // The distinction the two budgets exist to keep apart: once the
        // request is on the wire, silence is not evidence that it did not
        // commit, so this message must never claim it was not sent.
        let never = std::future::pending::<Result<tonic::Response<()>, tonic::Status>>();
        match under_timeout(forward_deadline(), never)
            .await
            .expect_err("bounded")
        {
            ApiError::Unavailable(message) => {
                assert!(message.contains("unknown"), "{message}");
                assert!(!message.contains("nothing was sent"), "{message}");
            }
            other => panic!("a lost call must be retriable, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_dial_spends_the_calls_budget_rather_than_adding_to_it() {
        // The advertised budget is 10s *total*. A fresh timeout per stage
        // would let a 9s dial be followed by a further 10s of silence — 19s
        // against a promise of 10 — so the deadline is stamped once and both
        // stages run against it. The dial here eats almost all of it, leaving
        // the call about a second.
        let deadline = forward_deadline();
        let started = tokio::time::Instant::now();

        let slow_dial = async {
            tokio::time::sleep(std::time::Duration::from_secs(9)).await;
            anyhow::Ok(())
        };
        under_dial_timeout(deadline, FORWARD_TIMEOUT, slow_dial)
            .await
            .expect("the dial finished inside the budget");

        let never = std::future::pending::<Result<tonic::Response<()>, tonic::Status>>();
        let error = under_timeout(deadline, never).await.expect_err("bounded");

        // The whole operation ends at the one deadline, not at 9s + 10s.
        assert_eq!(started.elapsed(), FORWARD_TIMEOUT);
        match error {
            // Still the *sent* message: the dial succeeded, the request went
            // out, and a shared budget must not blur that distinction.
            ApiError::Unavailable(message) => {
                assert!(message.contains("unknown"), "{message}");
                assert!(!message.contains("nothing was sent"), "{message}");
            }
            other => panic!("a lost call must be retriable, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_dial_that_burns_the_whole_budget_leaves_the_call_none_of_it() {
        // The boundary of the same rule: with the deadline already spent, the
        // call does not get a fresh one — it fails immediately, and says so
        // in the *outcome-unknown* words, because by then the request is on
        // the wire.
        let deadline = forward_deadline();
        let started = tokio::time::Instant::now();
        tokio::time::sleep(FORWARD_TIMEOUT).await;

        let never = std::future::pending::<Result<tonic::Response<()>, tonic::Status>>();
        assert!(matches!(
            under_timeout(deadline, never).await,
            Err(ApiError::Unavailable(_))
        ));
        assert_eq!(started.elapsed(), FORWARD_TIMEOUT, "not a second more");
    }

    fn key(leader: CoordinatorId, addr: &str, tls_generation: u64) -> ChannelKey {
        ChannelKey {
            leader,
            addr: addr.to_string(),
            tls_generation,
        }
    }

    /// The read path's channel is reused for the same leader, address and TLS
    /// generation, and dropped the moment any of the three moves.
    ///
    /// The reuse is the point of the cache — a node page every couple of
    /// seconds must not be a TLS handshake on the leader — but each of the
    /// three misses is a correctness rule, not an optimization: a cached
    /// connection to a deposed leader reads the wrong replica's marks, one to a
    /// moved address reads nobody's, and one built before a rotation presents a
    /// retired leaf until the far side drops it.
    #[test]
    fn the_read_channel_is_reused_per_leader_address_and_tls_generation() {
        let cache: ChannelCache<&'static str> = ChannelCache::default();
        let warm = key(1, "10.0.0.1:7443", 3);

        assert_eq!(cache.get(&warm), None, "nothing is cached before a dial");
        cache.put(warm.clone(), "the dialed channel");
        assert_eq!(cache.get(&warm), Some("the dialed channel"));

        // A different leader, the same address: an election moved leadership to
        // a peer that happens to reuse an address this replica once dialed.
        assert_eq!(cache.get(&key(2, "10.0.0.1:7443", 3)), None);
        // The same leader at a new address: a membership change moved it.
        assert_eq!(cache.get(&key(1, "10.0.0.9:7443", 3)), None);
        // The same leader at the same address, after a rotation (ADR 0037 §4).
        assert_eq!(cache.get(&key(1, "10.0.0.1:7443", 4)), None);
        // And none of those misses disturbed the entry itself — only an
        // explicit retirement or a replacement does.
        assert_eq!(cache.get(&warm), Some("the dialed channel"));
    }

    /// Retirement is keyed: a read whose connection broke drops its own entry
    /// and never a replacement dialed in the meantime, which is what keeps two
    /// concurrent node pages from starving each other of a warm channel.
    #[test]
    fn invalidating_drops_only_the_entry_the_failing_read_was_using() {
        let cache: ChannelCache<&'static str> = ChannelCache::default();
        let first = key(1, "10.0.0.1:7443", 3);
        let replacement = key(1, "10.0.0.1:7443", 4);

        cache.put(replacement.clone(), "the redialed channel");
        cache.invalidate(&first);
        assert_eq!(
            cache.get(&replacement),
            Some("the redialed channel"),
            "a stale failure must not retire a channel it never used"
        );

        cache.invalidate(&replacement);
        assert_eq!(cache.get(&replacement), None);
    }

    /// Only a transport-level status retires the channel. A leader that
    /// answered — even to refuse — has proved the connection works, and
    /// redialing on its say-so would turn a persistent refusal into a handshake
    /// per poll.
    #[test]
    fn a_status_the_leader_spoke_keeps_the_channel_and_a_broken_one_does_not() {
        for code in [
            Code::Unavailable,
            Code::Unknown,
            Code::Cancelled,
            Code::DeadlineExceeded,
        ] {
            assert!(
                is_transport_failure(&tonic::Status::new(code, "")),
                "{code:?} means the connection failed"
            );
        }
        for code in [
            Code::FailedPrecondition,
            Code::PermissionDenied,
            Code::Unimplemented,
            Code::Internal,
        ] {
            assert!(
                !is_transport_failure(&tonic::Status::new(code, "")),
                "{code:?} is the leader answering over a channel that works"
            );
        }
    }
}
