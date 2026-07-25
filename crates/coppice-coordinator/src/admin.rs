//! The membership admin surface (ADR 0016).
//!
//! Two halves share this module. The **server** ([`AdminService`]) implements
//! the generated `RaftAdminService` over the local [`Consensus`] seam and
//! [`NodeHandle`]; `bootstrap` mounts it on the coordinator's mTLS server next
//! to the Raft transport. The **client** helpers ([`admin_channel`] and the
//! per-verb wrappers) dial that surface over mTLS; the CLI ([`run_cli`]) and
//! the multi-node integration test share them, so the poll-until-caught-up
//! promotion loop lives in exactly one place.
//!
//! Every RPC first checks the request's stamped cluster identity (ADR 0016)
//! before touching Raft, mirroring the transport handler in `coppice-consensus`.

// tonic's generated service trait returns `Result<_, Status>`; `Status` is a
// large error type, and the signatures here are dictated by that trait.
#![allow(clippy::result_large_err)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::{Code, Request, Response, Status};

use coppice_consensus::{ClusterSummary, Consensus, ConsensusError, CoordinatorId, NodeHandle};
use coppice_core::id::{EnrollTokenId, MachineId};
use coppice_core::time::{Duration as CoreDuration, Timestamp};
use coppice_net::admin::{Client, RaftAdminService};
use coppice_proto::convert::{enroll_role_from_pb, enroll_role_to_pb};
use coppice_proto::pb::raft::v1 as pb;
use coppice_state::command::{MintEnrollToken, RevokeEnrollToken, RevokeIdentity};
use coppice_state::{Command, RevokedIdentity};
use coppice_tls::pki;

use crate::cli::{AdminArgs, AdminVerb};
use crate::config;
use crate::enroll::{self, EnrollContext, EnrollError, EnrollRequest};

/// How often the promotion wrapper retries while a learner is still catching up.
const PROMOTE_POLL_INTERVAL: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

/// Serves the membership admin RPCs over the local consensus seam (ADR 0016),
/// plus `ProbeCluster` (ADR 0037 §3).
///
/// The service is mounted **before** a cluster exists, because a parked or
/// fail-stopped daemon must still answer `ProbeCluster` — that is how a peer
/// learns it is not the cluster, and how the closed pre-formation surface is
/// visible without being joinable. Every membership verb is therefore gated
/// on formation having completed: until then they are refused, which is the
/// ADR's "membership verbs are refused" made concrete in one place.
///
/// Cheaply cloneable: the daemon keeps one handle to attach the consensus
/// seam through and hands another to the mounted tonic server, so the service
/// can be mounted before the thing it serves exists.
pub struct AdminService<C: Consensus> {
    inner: Arc<AdminInner<C>>,
}

impl<C: Consensus> Clone for AdminService<C> {
    fn clone(&self) -> Self {
        AdminService {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct AdminInner<C: Consensus> {
    /// `None` until formation completes; the membership verbs need it.
    seam: RwLock<Option<Seam<C>>>,
    phase: Arc<crate::formation::PhaseState>,
    /// This daemon's data directory: where the CA key lives on a voter's disk
    /// (ADR 0037 §4). The signing verbs below load it per request rather than
    /// holding it, so a re-rooted key is picked up without a restart.
    data_dir: PathBuf,
}

/// The consensus-backed half of [`AdminService`], present only once formed.
struct Seam<C: Consensus> {
    consensus: Arc<C>,
    handle: NodeHandle,
}

impl<C: Consensus> AdminService<C> {
    /// A service on a daemon with no cluster yet: `ProbeCluster` answers from
    /// `phase`, every membership verb is refused.
    pub(crate) fn unformed(phase: Arc<crate::formation::PhaseState>, data_dir: PathBuf) -> Self {
        AdminService {
            inner: Arc::new(AdminInner {
                seam: RwLock::new(None),
                phase,
                data_dir,
            }),
        }
    }

    /// Attach the consensus seam once the cluster is formed. Called exactly
    /// once per process, from the formation path or straight after a normal
    /// start.
    pub(crate) fn attach(&self, consensus: Arc<C>, handle: NodeHandle) {
        *self.inner.seam.write().expect("admin seam lock") = Some(Seam { consensus, handle });
    }

    /// The consensus seam, or the ADR 0037 §3 refusal.
    fn formed(&self) -> Result<(Arc<C>, NodeHandle), Status> {
        match &*self.inner.seam.read().expect("admin seam lock") {
            Some(seam) => Ok((Arc::clone(&seam.consensus), seam.handle.clone())),
            None => Err(Status::failed_precondition(
                "this coordinator has not formed a cluster: membership verbs are not served \
                 until the formation_complete marker exists (ADR 0037 §3)",
            )),
        }
    }

    /// Refuse a request that is malformed or stamped for a different raft
    /// history (ADR 0016), before any Raft state is touched.
    ///
    /// The expected value comes from the running replica's own stamp — for a
    /// formed cluster that is the history `init` minted, which config cannot
    /// derive (ADR 0037 §3) — so the check runs after [`formed`](Self::formed)
    /// has produced a handle.
    fn check_cluster(incoming: &[u8], handle: &NodeHandle) -> Result<(), Status> {
        if incoming.len() != 16 {
            return Err(Status::invalid_argument(format!(
                "history_id must be 16 bytes, got {} (ADR 0016)",
                incoming.len()
            )));
        }
        if incoming != handle.history_id() {
            return Err(Status::failed_precondition(format!(
                "request is from history {}, this node is stamped for history {} — \
                 cross-cluster admin contact refused (ADR 0016)",
                hex(incoming),
                hex(&handle.history_id()),
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl<C: Consensus> RaftAdminService for AdminService<C> {
    async fn add_learner(
        &self,
        request: Request<pb::AddLearnerRequest>,
    ) -> Result<Response<pb::AddLearnerResponse>, Status> {
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;
        consensus
            .add_learner(req.node_id, req.address)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::AddLearnerResponse {}))
    }

    async fn promote_voter(
        &self,
        request: Request<pb::PromoteVoterRequest>,
    ) -> Result<Response<pb::PromoteVoterResponse>, Status> {
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;
        consensus
            .promote_voter(req.promote_node_id, req.remove_node_id)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::PromoteVoterResponse {}))
    }

    async fn remove_node(
        &self,
        request: Request<pb::RemoveNodeRequest>,
    ) -> Result<Response<pb::RemoveNodeResponse>, Status> {
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;
        consensus
            .remove_node(req.node_id)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::RemoveNodeResponse {}))
    }

    async fn cluster_status(
        &self,
        request: Request<pb::ClusterStatusRequest>,
    ) -> Result<Response<pb::ClusterStatusResponse>, Status> {
        let req = request.into_inner();
        let (_, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;
        Ok(Response::new(cluster_summary_to_pb(
            handle.cluster_summary(),
        )))
    }

    /// Answer a probe (ADR 0037 §3).
    ///
    /// Deliberately **not** gated on formation, and deliberately stamped with
    /// the logical `cluster_id` rather than a history id: the caller is
    /// typically a daemon with no stamp of its own yet. A mismatched
    /// `cluster_id` is answered rather than refused — the prober asked "which
    /// cluster are you?", and the honest answer to a stranger is this node's
    /// own name, which the prober compares itself.
    async fn probe_cluster(
        &self,
        _request: Request<pb::ProbeClusterRequest>,
    ) -> Result<Response<pb::ProbeClusterResponse>, Status> {
        let answer = self.inner.phase.probe();
        Ok(Response::new(pb::ProbeClusterResponse {
            cluster_id: answer.cluster_id,
            history_id: answer.history_id,
            initialized: answer.initialized,
            node_id: answer.node_id,
            leader_hint: answer.leader_hint,
            voters: answer
                .voters
                .into_iter()
                .map(|(node_id, address)| pb::ProbeVoter { node_id, address })
                .collect(),
        }))
    }

    /// Mint an enrollment token (ADR 0037 §5).
    ///
    /// The secret is generated here, hashed into replicated policy, and
    /// returned to the caller **exactly once** — nothing stores or logs the
    /// clear value, and there is no verb that can recover it. A lost secret is
    /// re-minted, not retrieved.
    ///
    /// TODO(ADR 0022/0023): today's admin surface is unauthenticated beyond
    /// mTLS, so this verb inherits that posture. When role bindings land, the
    /// narrow `mint-enroll-token` grant of ADR 0037 §5 becomes one row in
    /// ADR 0023's table and gates this RPC (and its `Revoke*` siblings)
    /// without conferring any other authority.
    async fn mint_enroll_token(
        &self,
        request: Request<pb::MintEnrollTokenRequest>,
    ) -> Result<Response<pb::MintEnrollTokenResponse>, Status> {
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;

        let role = enroll_role_from_pb(req.role, "MintEnrollTokenRequest.role")
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        if req.label.trim().is_empty() {
            return Err(Status::invalid_argument(
                "an enrollment token needs a non-empty label: it is what `enroll-token list` \
                 and `[[enroll_token]]` seeding identify it by",
            ));
        }

        let minted_at = Timestamp::now();
        let expires_at = match req.ttl_seconds {
            None => None,
            Some(secs) => {
                let ttl = i64::try_from(secs)
                    .ok()
                    .and_then(CoreDuration::checked_from_secs)
                    .ok_or_else(|| {
                        Status::invalid_argument("ttl_seconds is outside the representable range")
                    })?;
                Some(minted_at.saturating_add(ttl))
            }
        };

        // Generated, hashed, and immediately handed back: the clear secret
        // exists only in this frame and the response.
        let secret = pki::generate_secret();
        let hash = pki::hash_secret(&secret)
            .map_err(|e| Status::internal(format!("hashing the token secret: {e}")))?;
        let token = EnrollTokenId::new();

        let applied = consensus
            .propose(Command::MintEnrollToken(MintEnrollToken {
                token,
                hash,
                role,
                label: req.label.clone(),
                expires_at,
                minted_at,
            }))
            .await
            .map_err(consensus_error_to_status)?;
        applied
            .outcome
            .map_err(|reason| Status::failed_precondition(format!("mint rejected: {reason}")))?;
        // Read-your-writes: a returned secret must be redeemable and listable
        // on this node immediately — enrollment and listing read the
        // *published* view, which trails apply by the refresh cadence, so an
        // operator (or automation) minting and then instantly enrolling would
        // otherwise race it.
        await_visible(&*consensus, applied.log_index).await?;

        // Note the absence of the secret: label and role are the whole record
        // an operator log may carry.
        tracing::info!(%token, label = %req.label, "admin: minted an enrollment token");

        Ok(Response::new(pb::MintEnrollTokenResponse {
            token_id: Some(token.into()),
            secret,
            expires_at_us: expires_at.map(|t| t.as_micros()),
        }))
    }

    /// List the enrollment tokens, **without** their hashes: this is an
    /// operator inventory, not a credential export (ADR 0037 §5).
    ///
    /// Answered from local applied state on any coordinator, exactly as
    /// `ClusterStatus` is — a follower's view may trail the leader's by the
    /// usual applied-index bound, which for an inventory listing is fine.
    async fn list_enroll_tokens(
        &self,
        request: Request<pb::ListEnrollTokensRequest>,
    ) -> Result<Response<pb::ListEnrollTokensResponse>, Status> {
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;

        let view = consensus.views().latest();
        let tokens = view
            .state()
            .enroll_tokens
            .iter()
            .map(|(id, t)| pb::EnrollTokenSummary {
                token_id: Some((*id).into()),
                role: enroll_role_to_pb(t.role) as i32,
                label: t.label.clone(),
                minted_at_us: t.minted_at.as_micros(),
                expires_at_us: t.expires_at.map(|e| e.as_micros()),
                revoked: t.revoked,
            })
            .collect();
        Ok(Response::new(pb::ListEnrollTokensResponse { tokens }))
    }

    /// Revoke an enrollment token: future enrollments stop, already-issued
    /// leaves are untouched (ADR 0037 §5 — evicting those is `RevokeIdentity`).
    async fn revoke_enroll_token(
        &self,
        request: Request<pb::RevokeEnrollTokenRequest>,
    ) -> Result<Response<pb::RevokeEnrollTokenResponse>, Status> {
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;

        let token: EnrollTokenId = req
            .token_id
            .ok_or_else(|| Status::invalid_argument("RevokeEnrollTokenRequest.token_id"))?
            .try_into()
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;

        let applied = consensus
            .propose(Command::RevokeEnrollToken(RevokeEnrollToken {
                token,
                revoked_at: Timestamp::now(),
            }))
            .await
            .map_err(consensus_error_to_status)?;
        applied
            .outcome
            .map_err(|reason| Status::failed_precondition(format!("revoke rejected: {reason}")))?;
        // Read-your-writes, as in mint: when this returns, an enrollment
        // attempt against this node can no longer redeem the token.
        await_visible(&*consensus, applied.log_index).await?;
        tracing::info!(%token, "admin: revoked an enrollment token");
        Ok(Response::new(pb::RevokeEnrollTokenResponse {}))
    }

    /// Mark an issued identity revoked. The leader then refuses its renewals,
    /// and its short-lived leaf ages out — v1's revocation mechanism, with no
    /// CRL or OCSP (ADR 0037 §5).
    async fn revoke_identity(
        &self,
        request: Request<pb::RevokeIdentityRequest>,
    ) -> Result<Response<pb::RevokeIdentityResponse>, Status> {
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;

        let identity: RevokedIdentity = req
            .identity
            .ok_or_else(|| Status::invalid_argument("RevokeIdentityRequest.identity"))?
            .try_into()
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;

        let applied = consensus
            .propose(Command::RevokeIdentity(RevokeIdentity {
                identity: identity.clone(),
                revoked_at: Timestamp::now(),
            }))
            .await
            .map_err(consensus_error_to_status)?;
        applied.outcome.map_err(|reason| {
            Status::failed_precondition(format!("identity revocation rejected: {reason}"))
        })?;
        // Read-your-writes, as in mint. Renewal refusal already takes a
        // strong read, so this is for local list/status surfaces.
        await_visible(&*consensus, applied.log_index).await?;
        tracing::info!(identity = ?identity, "admin: revoked an identity");
        Ok(Response::new(pb::RevokeIdentityResponse {}))
    }

    /// The leader-side half of a follower's `/enroll` proxy (ADR 0037 §4).
    ///
    /// Nothing here inspects the token beyond handing it to the enrollment
    /// core: the uniform-failure discipline lives there, and this handler
    /// collapses every authentication outcome onto one `UNAUTHENTICATED` with
    /// a fixed message.
    async fn forward_enroll(
        &self,
        request: Request<pb::ForwardEnrollRequest>,
    ) -> Result<Response<pb::ForwardEnrollResponse>, Status> {
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;

        let node_id = req
            .node_id
            .map(TryInto::try_into)
            .transpose()
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;
        let machine_id = req
            .machine_id
            .map(TryInto::try_into)
            .transpose()
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;

        let ctx = EnrollContext {
            consensus: consensus.as_ref(),
            data_dir: &self.inner.data_dir,
            formed: true,
        };
        let issued = enroll::handle_enroll(
            &ctx,
            EnrollRequest {
                token: &req.token,
                csr_pem: req.csr_pem.as_bytes(),
                node_id,
                machine_id,
            },
        )
        .await
        .map_err(enroll_error_to_status)?;

        Ok(Response::new(pb::ForwardEnrollResponse {
            cert_pem: pem_string(issued.cert_pem)?,
            ca_pem: pem_string(issued.ca_pem)?,
        }))
    }

    /// Renew the caller's own coordinator leaf (ADR 0037 §4).
    ///
    /// The subject is read from the **verified** client certificate this
    /// request arrived on, never from the request, so renewal cannot change
    /// identity. A revoked machine is refused with the same opaque status an
    /// unauthenticated caller gets.
    async fn renew_coordinator(
        &self,
        request: Request<pb::RenewCoordinatorRequest>,
    ) -> Result<Response<pb::RenewCoordinatorResponse>, Status> {
        let (consensus, handle) = self.formed()?;
        let machine = self.peer_machine(&request, &consensus)?;
        let req = request.into_inner();
        Self::check_cluster(&req.history_id, &handle)?;

        let ctx = EnrollContext {
            consensus: consensus.as_ref(),
            data_dir: &self.inner.data_dir,
            formed: true,
        };
        let issued = enroll::renew_coordinator(&ctx, machine, req.csr_pem.as_bytes())
            .await
            .map_err(enroll_error_to_status)?;

        Ok(Response::new(pb::RenewCoordinatorResponse {
            cert_pem: pem_string(issued.cert_pem)?,
            ca_pem: pem_string(issued.ca_pem)?,
        }))
    }
}

impl<C: Consensus> AdminService<C> {
    /// The machine identity of the coordinator leaf this request arrived on.
    ///
    /// The chain was already verified by the TLS layer against the cluster CA;
    /// [`pki::verify_leaf`] re-runs it against the CA in replicated state and
    /// classifies the profile, so a leaf of any other profile — an operator
    /// certificate, an agent's — cannot renew a coordinator identity.
    fn peer_machine(
        &self,
        request: &Request<pb::RenewCoordinatorRequest>,
        consensus: &Arc<C>,
    ) -> Result<MachineId, Status> {
        let peer = request.peer_certs();
        let leaf = peer
            .as_ref()
            .and_then(|certs| certs.first())
            .ok_or_else(|| {
                Status::unauthenticated("renewal requires the current client certificate")
            })?;
        let ca_pem = consensus
            .views()
            .latest()
            .state()
            .ca
            .as_ref()
            .map(|ca| ca.bundle.pem().to_string())
            .ok_or_else(|| Status::failed_precondition("this cluster has no recorded CA"))?;
        let verified = pki::verify_leaf(ca_pem.as_bytes(), leaf.as_ref())
            .map_err(|_| Status::unauthenticated("client certificate does not verify"))?;
        match verified.profile {
            pki::Profile::Coordinator(machine) => Ok(machine),
            _ => Err(Status::permission_denied(
                "only a coordinator leaf may renew a coordinator identity (ADR 0037 §4)",
            )),
        }
    }
}

/// Map an enrollment-core failure onto its gRPC status.
///
/// Every authentication outcome — unknown, revoked, expired, wrong-role — is
/// already collapsed to [`EnrollError::Unauthorized`] before it gets here, and
/// this deliberately adds nothing to it.
fn enroll_error_to_status(err: EnrollError) -> Status {
    match err {
        EnrollError::Unauthorized => Status::unauthenticated("enrollment refused"),
        EnrollError::BadRequest(msg) => Status::invalid_argument(msg),
        EnrollError::NotFormed => Status::failed_precondition(EnrollError::NotFormed.to_string()),
        EnrollError::NotLeader { leader: Some(id) } => Status::failed_precondition(format!(
            "not the leader; current leader is node {id} — retarget the request"
        )),
        EnrollError::NotLeader { leader: None } => Status::failed_precondition(
            "not the leader; no leader currently known (election in progress) — retry",
        ),
        EnrollError::Internal(msg) => Status::internal(msg),
    }
}

/// Issued PEM bytes as the UTF-8 string the wire carries.
fn pem_string(pem: Vec<u8>) -> Result<String, Status> {
    String::from_utf8(pem).map_err(|_| Status::internal("issued material is not UTF-8"))
}

/// Map a consensus-seam failure onto the gRPC status the admin RPC returns.
///
/// The retryable variants (ADR 0016) become `FAILED_PRECONDITION` /`ABORTED` /
/// `DEADLINE_EXCEEDED` so a caller can branch and retry; terminal ones become
/// `UNAVAILABLE` / `INTERNAL`. The `LearnerNotCaughtUp` message deliberately
/// contains "behind" — the promotion client keys its poll loop on it.
/// Wait for the published views to include `log_index` — read-your-writes for
/// the admin verbs whose effects are read back through `views().latest()`
/// (enrollment, listing). Cheap and safe here: the admin plane is
/// authenticated, low-rate, and already leader-gated.
async fn await_visible<C: Consensus>(consensus: &C, log_index: u64) -> Result<(), Status> {
    consensus
        .views()
        .at_least(log_index)
        .await
        .map(|_| ())
        .map_err(consensus_error_to_status)
}

fn consensus_error_to_status(err: ConsensusError) -> Status {
    match err {
        ConsensusError::NotLeader { leader: Some(id) } => Status::failed_precondition(format!(
            "not the leader; current leader is node {id} — retarget the request"
        )),
        ConsensusError::NotLeader { leader: None } => Status::failed_precondition(
            "not the leader; no leader currently known (election in progress) — retry",
        ),
        ConsensusError::LearnerNotCaughtUp { lag } => Status::failed_precondition(format!(
            "learner is {lag} entries behind; retry after catch-up (ADR 0016)"
        )),
        ConsensusError::MembershipInProgress => Status::aborted(
            "a membership change is already in progress; only one may be outstanding (ADR 0016)",
        ),
        ConsensusError::Timeout => {
            Status::deadline_exceeded("consensus operation timed out; outcome unknown")
        }
        ConsensusError::Shutdown => Status::unavailable("consensus is shutting down"),
        ConsensusError::Fatal(msg) => Status::internal(format!("consensus fault: {msg}")),
    }
}

/// Convert a [`ClusterSummary`] into the `ClusterStatus` response wire form.
///
/// Canonicalizes per `raft.proto`: voters into a single ascending
/// [`VoterConfig`](pb::VoterConfig), members ascending by `node_id`.
pub fn cluster_summary_to_pb(summary: ClusterSummary) -> pb::ClusterStatusResponse {
    let mut voters: Vec<u64> = summary
        .members
        .iter()
        .filter(|m| m.voter)
        .map(|m| m.id)
        .collect();
    voters.sort_unstable();

    let mut members: Vec<pb::RaftMember> = summary
        .members
        .iter()
        .map(|m| pb::RaftMember {
            node_id: m.id,
            address: m.addr.clone(),
        })
        .collect();
    members.sort_by_key(|m| m.node_id);

    let replication = summary
        .replication
        .iter()
        .map(|(id, matched)| pb::ReplicationProgress {
            node_id: *id,
            matched_index: *matched,
        })
        .collect();

    pb::ClusterStatusResponse {
        local_node_id: summary.local_id,
        leader_node_id: summary.leader,
        term: summary.term,
        last_applied_index: summary.last_applied,
        known_committed_index: summary.known_committed,
        membership: Some(pb::Membership {
            configs: vec![pb::VoterConfig { voters }],
            members,
        }),
        replication,
    }
}

/// Lowercase hex of raw identity bytes, for operator-facing messages.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String is infallible.
        let _ = write!(out, "{b:02x}");
    }
    out
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

/// Dial the admin surface of `target` (`host:port`) over mTLS (ADR 0011).
///
/// The client presents this node's certificate and trusts the cluster CA; the
/// TLS domain is the host half of `target`, which must match the peer
/// certificate's SAN.
pub async fn admin_channel(
    target: &str,
    ca_pem: &[u8],
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<Client<Channel>> {
    let host = target
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(target)
        .to_string();

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
        .domain_name(host);

    let channel = Channel::from_shared(format!("https://{target}"))
        .with_context(|| format!("invalid admin target {target}"))?
        .tls_config(tls)
        .context("configuring admin client TLS")?
        .connect()
        .await
        .with_context(|| format!("connecting to admin target {target}"))?;

    Ok(Client::new(channel))
}

/// Add a learner (ADR 0016 step 2).
pub async fn add_learner(
    client: &mut Client<Channel>,
    history_id: [u8; 16],
    node_id: CoordinatorId,
    addr: String,
) -> Result<()> {
    client
        .add_learner(pb::AddLearnerRequest {
            history_id: history_id.to_vec(),
            node_id,
            address: addr,
        })
        .await
        .map_err(status_to_anyhow)?;
    Ok(())
}

/// Remove a node from membership (ADR 0016).
pub async fn remove_node(
    client: &mut Client<Channel>,
    history_id: [u8; 16],
    node_id: CoordinatorId,
) -> Result<()> {
    client
        .remove_node(pb::RemoveNodeRequest {
            history_id: history_id.to_vec(),
            node_id,
        })
        .await
        .map_err(status_to_anyhow)?;
    Ok(())
}

/// Fetch a coordinator's cluster-status view (ADR 0016).
pub async fn cluster_status(
    client: &mut Client<Channel>,
    history_id: [u8; 16],
) -> Result<pb::ClusterStatusResponse> {
    let resp = client
        .cluster_status(pb::ClusterStatusRequest {
            history_id: history_id.to_vec(),
        })
        .await
        .map_err(status_to_anyhow)?;
    Ok(resp.into_inner())
}

/// Promote a learner to voter, polling until it catches up or `wait` elapses
/// (ADR 0016 step 3).
///
/// A learner still behind the promotion threshold yields a retryable
/// `FAILED_PRECONDITION`/"behind" response; this retries every 500ms up to the
/// `wait` deadline before giving up, which is what makes `coordinator replace`
/// operable end to end. Any other failure returns immediately.
pub async fn promote_voter(
    client: &mut Client<Channel>,
    history_id: [u8; 16],
    promote: CoordinatorId,
    remove: Option<CoordinatorId>,
    wait: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let result = client
            .promote_voter(pb::PromoteVoterRequest {
                history_id: history_id.to_vec(),
                promote_node_id: promote,
                remove_node_id: remove,
            })
            .await;

        match result {
            Ok(_) => return Ok(()),
            Err(status) if is_learner_behind(&status) => {
                if tokio::time::Instant::now() + PROMOTE_POLL_INTERVAL >= deadline {
                    bail!(
                        "learner {promote} did not catch up within {}: {}",
                        humantime_serde::re::humantime::format_duration(wait),
                        status.message()
                    );
                }
                tokio::time::sleep(PROMOTE_POLL_INTERVAL).await;
            }
            Err(status) => return Err(status_to_anyhow(status)),
        }
    }
}

/// Whether a promotion failure is the retryable "learner still catching up"
/// case (ADR 0016) — the poll loop's continue condition.
fn is_learner_behind(status: &Status) -> bool {
    status.code() == Code::FailedPrecondition && status.message().contains("behind")
}

/// Flatten a gRPC [`Status`] into an `anyhow` error naming the code and message.
fn status_to_anyhow(status: Status) -> anyhow::Error {
    anyhow!(
        "admin RPC failed ({:?}): {}",
        status.code(),
        status.message()
    )
}

// ---------------------------------------------------------------------------
// CLI dispatch
// ---------------------------------------------------------------------------

/// Run one `admin` invocation: load config for TLS material and the default
/// target, dial the admin surface, and execute the verb.
pub async fn run_cli(args: AdminArgs) -> Result<()> {
    // `issue-operator-cert` rides the local admin socket, not the network
    // (ADR 0037 §3): it is the recovery for having lost the very credential
    // the network path would authenticate with, and the CA key it signs from
    // is on this host's disk. Dispatched before anything dials.
    if let AdminVerb::IssueOperatorCert {
        operator_csr,
        operator_cn,
        out_dir,
    } = &args.verb
    {
        return crate::localadmin::run_issue_operator_cert(
            &args.config,
            operator_csr.as_deref(),
            operator_cn.clone(),
            out_dir.as_deref(),
        )
        .await;
    }

    let resolved = config::load(&args.config, config::CliOverrides::default())
        .with_context(|| format!("reading config {}", args.config.display()))?;
    let cfg = &resolved.config;

    // Default `--target` to the first candidate from the configured discovery
    // backend (ADR 0037 §2: `[discovery.static] addrs` subsumes the old
    // top-level `peers`; the other backends answer the same question through
    // one consultation). Discovery is advisory and non-blocking, so a backend
    // that finds nothing degrades to the explicit-target error below.
    let target = match &args.target {
        Some(t) => t.clone(),
        None => {
            let discovery = crate::discovery::build(&cfg.discovery)
                .context("building the discovery backend for the default --target")?;
            discovery
                .candidates()
                .await
                .first()
                .cloned()
                .ok_or_else(|| {
                    anyhow!(
                        "no --target given and the config's \"{}\" discovery backend \
                     found no candidates; pass --target <host:port>",
                        cfg.discovery.backend.as_str()
                    )
                })?
        }
    };

    let ca = read_pem(&cfg.tls.ca_path)?;
    let cert = read_pem(&cfg.tls.cert_path)?;
    let key = read_pem(&cfg.tls.key_path)?;

    let mut client = admin_channel(&target, &ca, &cert, &key).await?;

    // Learn the target's stamped history before any verb: a cluster formed
    // by `coppice coordinator init` carries a history the config cannot
    // derive (ADR 0037 §3 mints it), and every membership RPC cross-checks
    // it. `ProbeCluster` is the verb that exists for exactly this — it
    // matches on the logical `cluster_id` and answers with the history. A
    // legacy cluster answers its config-derived value, so this also covers
    // directories the `--bootstrap`/`--join` flags stamped.
    let probe = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: cfg.cluster_id.to_string(),
        })
        .await
        .map_err(status_to_anyhow)?
        .into_inner();
    if probe.cluster_id != cfg.cluster_id.to_string() {
        bail!(
            "{target} serves cluster {:?}, this config names {:?} — wrong target or wrong \
             config",
            probe.cluster_id,
            cfg.cluster_id.to_string(),
        );
    }
    if !probe.initialized {
        bail!(
            "{target} has not formed a cluster (it is parked or mid-formation); membership \
             verbs are served only once the formation_complete marker exists (ADR 0037 §3)"
        );
    }
    let history_id: [u8; 16] = probe.history_id.as_slice().try_into().map_err(|_| {
        anyhow!(
            "{target} answered a malformed history id ({} bytes)",
            probe.history_id.len()
        )
    })?;

    match args.verb {
        AdminVerb::AddLearner { node_id, addr } => {
            add_learner(&mut client, history_id, node_id, addr.clone()).await?;
            println!("added node {node_id} as a learner ({addr})");
        }
        AdminVerb::Promote {
            node_id,
            remove,
            wait,
        } => {
            promote_voter(&mut client, history_id, node_id, remove, wait).await?;
            match remove {
                Some(r) => println!("promoted node {node_id} to voter, removed node {r}"),
                None => println!("promoted node {node_id} to voter"),
            }
        }
        AdminVerb::Remove { node_id } => {
            remove_node(&mut client, history_id, node_id).await?;
            println!("removed node {node_id} from membership");
        }
        AdminVerb::Status => {
            let status = cluster_status(&mut client, history_id).await?;
            print!("{}", render_status(&status));
        }
        // Dispatched above, before any network client was built.
        AdminVerb::IssueOperatorCert { .. } => unreachable!("handled on the local socket"),
    }
    Ok(())
}

/// Read a PEM file, naming the path on failure (ADR 0011).
fn read_pem(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("reading TLS material {}", path.display()))
}

/// Pretty-print a `ClusterStatus` response for the `admin status` verb.
fn render_status(s: &pb::ClusterStatusResponse) -> String {
    let mut out = String::new();
    let leader = match s.leader_node_id {
        Some(id) if id == s.local_node_id => format!("{id} (this node)"),
        Some(id) => id.to_string(),
        None => "unknown".to_string(),
    };
    let _ = writeln!(out, "node          {}", s.local_node_id);
    let _ = writeln!(out, "leader        {leader}");
    let _ = writeln!(out, "term          {}", s.term);
    let _ = writeln!(out, "applied       {}", s.last_applied_index);
    let _ = writeln!(out, "committed     {}", s.known_committed_index);

    let voters: std::collections::BTreeSet<u64> = s
        .membership
        .as_ref()
        .and_then(|m| m.configs.first())
        .map(|c| c.voters.iter().copied().collect())
        .unwrap_or_default();

    let _ = writeln!(out, "members:");
    if let Some(membership) = &s.membership {
        for member in &membership.members {
            let role = if voters.contains(&member.node_id) {
                "voter"
            } else {
                "learner"
            };
            let _ = writeln!(
                out,
                "  node {:<6} {:<8} {}",
                member.node_id, role, member.address
            );
        }
    }

    if !s.replication.is_empty() {
        let _ = writeln!(out, "replication (leader view):");
        for r in &s.replication {
            let lag = s.last_applied_index.saturating_sub(r.matched_index);
            let _ = writeln!(
                out,
                "  node {:<6} matched {:<12} lag {}",
                r.node_id, r.matched_index, lag
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_consensus::MemberSummary;

    #[test]
    fn not_leader_maps_to_failed_precondition_naming_leader() {
        let status = consensus_error_to_status(ConsensusError::NotLeader { leader: Some(7) });
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(status.message().contains("node 7"), "{}", status.message());
    }

    #[test]
    fn not_leader_unknown_maps_to_failed_precondition() {
        let status = consensus_error_to_status(ConsensusError::NotLeader { leader: None });
        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    #[test]
    fn learner_behind_maps_and_is_detected_by_the_poll_predicate() {
        let status = consensus_error_to_status(ConsensusError::LearnerNotCaughtUp { lag: 42 });
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(status.message().contains("42"));
        // The client poll loop must recognize exactly this status.
        assert!(is_learner_behind(&status));
    }

    #[test]
    fn membership_in_progress_maps_to_aborted() {
        let status = consensus_error_to_status(ConsensusError::MembershipInProgress);
        assert_eq!(status.code(), Code::Aborted);
        assert!(!is_learner_behind(&status));
    }

    #[test]
    fn timeout_maps_to_deadline_exceeded() {
        let status = consensus_error_to_status(ConsensusError::Timeout);
        assert_eq!(status.code(), Code::DeadlineExceeded);
    }

    #[test]
    fn shutdown_and_fatal_map_to_unavailable_and_internal() {
        assert_eq!(
            consensus_error_to_status(ConsensusError::Shutdown).code(),
            Code::Unavailable
        );
        assert_eq!(
            consensus_error_to_status(ConsensusError::Fatal("disk gone".into())).code(),
            Code::Internal
        );
    }

    #[test]
    fn summary_to_pb_canonicalizes_voters_and_members() {
        let summary = ClusterSummary {
            local_id: 2,
            leader: Some(1),
            term: 5,
            last_applied: 100,
            known_committed: 100,
            snapshot_last_index: Some(64),
            members: vec![
                MemberSummary {
                    id: 3,
                    addr: "c3:7071".into(),
                    voter: false,
                },
                MemberSummary {
                    id: 1,
                    addr: "c1:7071".into(),
                    voter: true,
                },
                MemberSummary {
                    id: 2,
                    addr: "c2:7071".into(),
                    voter: true,
                },
            ],
            replication: vec![(1, 100), (3, 40)],
            millis_since_quorum_ack: Some(0),
        };

        let pbm = cluster_summary_to_pb(summary);
        assert_eq!(pbm.local_node_id, 2);
        assert_eq!(pbm.leader_node_id, Some(1));
        assert_eq!(pbm.term, 5);

        let membership = pbm.membership.expect("membership present");
        // Voters ascending, learners excluded.
        assert_eq!(membership.configs.len(), 1);
        assert_eq!(membership.configs[0].voters, vec![1, 2]);
        // Members ascending by node_id, learners included.
        let ids: Vec<u64> = membership.members.iter().map(|m| m.node_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);

        assert_eq!(pbm.replication.len(), 2);
    }

    #[test]
    fn a_generic_failed_precondition_is_not_a_behind_signal() {
        let status = Status::failed_precondition("not the leader; current leader is node 3");
        assert!(!is_learner_behind(&status));
    }
}
