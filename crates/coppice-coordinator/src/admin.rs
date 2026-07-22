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
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::{Code, Request, Response, Status};

use coppice_consensus::{
    ClusterSummary, Consensus, ConsensusError, CoordinatorId, FormationControl, FormationError,
    FormationOutcome, NodeHandle,
};
use coppice_net::admin::{Client, RaftAdminService};
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::TlsStore;

use crate::cli::{AdminArgs, AdminVerb};
use crate::config;
use crate::discovery::Discovery;

/// How often the promotion wrapper retries while a learner is still catching up.
const PROMOTE_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// The organizational-unit marker on a coordinator *machine* leaf (ADR 0037 §6):
/// its common name is the stable machine identity.
pub const OU_COORDINATOR: &str = "coppice-coordinator";
/// The organizational-unit marker on an operator-profile leaf (ADR 0037 §6):
/// the break-glass credential ADR 0022 already defines. Authorizes the
/// operator-only membership verbs.
pub const OU_OPERATOR: &str = "coppice-operator";

// ---------------------------------------------------------------------------
// Certificate profiles (ADR 0037 §6)
// ---------------------------------------------------------------------------

/// Which certificate profile a request arrived under (ADR 0037 §6). The profile
/// is decided from the CA-attested subject of the mTLS leaf — its `OU` marker —
/// never from anything in the request body.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CertProfile {
    /// `OU=coppice-operator`: the break-glass credential; may call every verb,
    /// including `InitializeCluster` and `RemoveNode`.
    Operator,
    /// `OU=coppice-coordinator`: a machine leaf whose `CN` is the stable machine
    /// identity. The narrow self-service grant: probe/status, `AddLearner` for
    /// its own identity, `PromoteVoter` for the node bound to it.
    Machine { identity: String },
    /// No recognized OU (an agent leaf, or anything else): none of the
    /// membership surface is reachable.
    Agent,
}

/// Extract the certificate profile from a request's mTLS peer certificate
/// (ADR 0037 §6). No client certificate, or an unrecognized `OU`, is
/// [`CertProfile::Agent`] — refused from the whole membership surface.
fn peer_profile<T>(request: &Request<T>) -> CertProfile {
    let Some(subject) = request
        .peer_certs()
        .and_then(|certs| certs.first().cloned())
        .and_then(|leaf| coppice_tls::parse_leaf_subject_der(leaf.as_ref()))
    else {
        return CertProfile::Agent;
    };
    match subject.org_unit.as_deref() {
        Some(OU_OPERATOR) => CertProfile::Operator,
        Some(OU_COORDINATOR) => CertProfile::Machine {
            identity: subject.common_name.unwrap_or_default(),
        },
        _ => CertProfile::Agent,
    }
}

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

/// Serves the membership admin RPCs over the local consensus seam (ADR 0016),
/// enforcing the ADR 0037 §6 authorization matrix and driving formation.
pub struct AdminService<C: Consensus> {
    consensus: Arc<C>,
    handle: NodeHandle,
    cluster_uuid: [u8; 16],
    /// The shared hot-reload mTLS store (ADR 0037 §6): the leader dials the
    /// advertised endpoint through it for admission-time verification, and the
    /// formation probe guard dials candidates through it.
    tls: Arc<TlsStore>,
    /// The formation control surface (ADR 0037 §3), backing `InitializeCluster`.
    formation: FormationControl,
    /// The discovery backend (ADR 0037 §2), consulted by the formation probe
    /// guard to refuse a double-init against an already-initialized cluster.
    discovery: Arc<dyn Discovery>,
}

impl<C: Consensus> AdminService<C> {
    /// Bind the service to the local consensus seam, admin handle, stamped
    /// cluster identity, mTLS store, formation control, and discovery backend.
    pub fn new(
        consensus: Arc<C>,
        handle: NodeHandle,
        cluster_uuid: [u8; 16],
        tls: Arc<TlsStore>,
        formation: FormationControl,
        discovery: Arc<dyn Discovery>,
    ) -> Self {
        AdminService {
            consensus,
            handle,
            cluster_uuid,
            tls,
            formation,
            discovery,
        }
    }

    /// Refuse a request that is malformed or stamped for a different cluster
    /// (ADR 0016), before any Raft state is touched.
    fn check_cluster(&self, incoming: &[u8]) -> Result<(), Status> {
        if incoming.len() != 16 {
            return Err(Status::invalid_argument(format!(
                "cluster_uuid must be 16 bytes, got {} (ADR 0016)",
                incoming.len()
            )));
        }
        if incoming != self.cluster_uuid {
            return Err(Status::failed_precondition(format!(
                "request is from cluster {}, this node is stamped for cluster {} — \
                 cross-cluster admin contact refused (ADR 0016)",
                hex(incoming),
                hex(&self.cluster_uuid),
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
        // Authz (ADR 0037 §6): an operator may add any learner; a machine leaf
        // may add only a learner bound to ITS OWN identity. The binding is
        // always the CA-attested subject of the mTLS session, never claimed in
        // the body — so a machine leaf can only ever bind its own CN, and an
        // agent leaf is refused outright.
        let profile = peer_profile(&request);
        let machine_identity = match &profile {
            CertProfile::Operator => peer_common_name(&request).unwrap_or_default(),
            CertProfile::Machine { identity } => identity.clone(),
            CertProfile::Agent => {
                return Err(permission_denied(
                    "AddLearner requires an operator- or coordinator-profile certificate",
                ))
            }
        };
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;

        // Endpoint verification (ADR 0037 §6), leader-side only: the advertised
        // endpoint must actually present the requester's machine identity and
        // report the claimed stamped node id, so a claimed id without the
        // matching CA-attested subject cannot occupy a seat. Only the leader
        // admits; on a follower this is skipped and consensus returns NotLeader.
        // Operator-driven admissions are exempt: the operator vouches directly.
        if matches!(profile, CertProfile::Machine { .. }) && self.is_leader() {
            self.verify_endpoint(&req.address, &machine_identity, req.node_id)
                .await?;
        }

        self.consensus
            .add_learner(req.node_id, req.address, machine_identity)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::AddLearnerResponse {}))
    }

    async fn promote_voter(
        &self,
        request: Request<pb::PromoteVoterRequest>,
    ) -> Result<Response<pb::PromoteVoterResponse>, Status> {
        // Authz (ADR 0037 §6): operator may promote any node; a machine leaf may
        // promote only the node id bound to its own machine identity, and may
        // never drive the manual `remove` half (that is an operator verb).
        let profile = peer_profile(&request);
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;
        match &profile {
            CertProfile::Operator => {}
            CertProfile::Machine { identity } => {
                if req.remove_node_id.is_some() {
                    return Err(permission_denied(
                        "a machine certificate may not drive a manual removal in PromoteVoter",
                    ));
                }
                self.require_bound_to(req.promote_node_id, identity)?;
            }
            CertProfile::Agent => {
                return Err(permission_denied(
                    "PromoteVoter requires an operator- or coordinator-profile certificate",
                ))
            }
        }
        self.consensus
            .promote_voter(req.promote_node_id, req.remove_node_id)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::PromoteVoterResponse {}))
    }

    async fn remove_node(
        &self,
        request: Request<pb::RemoveNodeRequest>,
    ) -> Result<Response<pb::RemoveNodeResponse>, Status> {
        // RemoveNode is operator-only (ADR 0037 §6): a machine credential can
        // never remove, repoint, or occupy a second seat.
        if !matches!(peer_profile(&request), CertProfile::Operator) {
            return Err(permission_denied(
                "RemoveNode requires an operator-profile certificate",
            ));
        }
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;
        self.consensus
            .remove_node(req.node_id)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::RemoveNodeResponse {}))
    }

    async fn cluster_status(
        &self,
        request: Request<pb::ClusterStatusRequest>,
    ) -> Result<Response<pb::ClusterStatusResponse>, Status> {
        // ClusterStatus is reachable by operator + machine profiles only.
        if matches!(peer_profile(&request), CertProfile::Agent) {
            return Err(permission_denied(
                "ClusterStatus requires an operator- or coordinator-profile certificate",
            ));
        }
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;
        Ok(Response::new(cluster_summary_to_pb(
            self.handle.cluster_summary(),
        )))
    }

    async fn probe_cluster(
        &self,
        request: Request<pb::ProbeClusterRequest>,
    ) -> Result<Response<pb::ProbeClusterResponse>, Status> {
        // ProbeCluster is reachable by operator + machine profiles only
        // (ADR 0037 §6): agents can call none of the membership surface. The
        // request carries no cluster identity — it is precisely how a converging
        // process learns which cluster (if any) is behind this candidate — so
        // there is no cross-cluster check. A parked daemon (no membership yet)
        // answers `initialized = false`.
        if matches!(peer_profile(&request), CertProfile::Agent) {
            return Err(permission_denied(
                "ProbeCluster requires an operator- or coordinator-profile certificate",
            ));
        }
        let summary = self.handle.cluster_summary();
        let voters: Vec<pb::RaftMember> = cluster_summary_to_pb(summary.clone())
            .membership
            .map(|m| {
                let voter_ids: std::collections::BTreeSet<u64> = m
                    .configs
                    .first()
                    .map(|c| c.voters.iter().copied().collect())
                    .unwrap_or_default();
                m.members
                    .into_iter()
                    .filter(|member| voter_ids.contains(&member.node_id))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Response::new(pb::ProbeClusterResponse {
            cluster_uuid: self.cluster_uuid.to_vec(),
            initialized: !summary.members.is_empty(),
            // The stamped raft identity is always reported. (Deviation from the
            // ADR's "a parked replica advertises no id": this implementation
            // mints identity eagerly on an empty directory, so the id always
            // exists — and endpoint verification, ADR 0037 §6, requires a
            // joining-but-uninitialized node to report the id it claims.)
            node_id: Some(summary.local_id),
            leader_hint: summary.leader,
            voters,
        }))
    }

    async fn initialize_cluster(
        &self,
        request: Request<pb::InitializeClusterRequest>,
    ) -> Result<Response<pb::InitializeClusterResponse>, Status> {
        // InitializeCluster is operator-profile ONLY (ADR 0037 §6): at day zero
        // the replicated role bindings that would confer OIDC admin are part of
        // the very policy being created, and machine credentials must never form
        // a cluster.
        if !matches!(peer_profile(&request), CertProfile::Operator) {
            return Err(permission_denied(
                "InitializeCluster requires an operator-profile certificate (ADR 0037 §6)",
            ));
        }
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;
        if req.formation_token.is_empty() {
            return Err(Status::invalid_argument(
                "InitializeCluster requires a non-empty formation_token (ADR 0037 §3)",
            ));
        }

        // The formation probe guard (ADR 0037 §3 case c): only when genuinely
        // parked (raft uninitialized AND no token recorded) do we run one round
        // of discovery+probe and refuse a double-init against an already-live
        // cluster. Cases (a) already-initialized and (b) crash-resume are decided
        // inside `formation.form` from the recorded token, with no probe.
        if !self.formation.is_initialized().await && self.formation.recorded_token().is_none() {
            self.formation_probe_guard().await?;
        }

        match self.formation.form(&req.formation_token).await {
            Ok(outcome) => {
                // Policy seeding rides along idempotently (ADR 0037 §3). The
                // concrete TOML schema is owned by the `coppice-cli cluster init`
                // package; this handler accepts and routes the bytes.
                if let Some(policy) = req.policy_toml.filter(|p| !p.is_empty()) {
                    self.apply_formation_policy(&policy).await?;
                }
                Ok(Response::new(pb::InitializeClusterResponse {
                    node_id: self.formation.node_id(),
                    already_initialized: matches!(outcome, FormationOutcome::AlreadyFormed),
                }))
            }
            Err(FormationError::ConflictingToken { recorded }) => {
                Err(conflicting_formation_token_status(recorded))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

impl<C: Consensus> AdminService<C> {
    /// Whether this replica currently believes it is the leader (from its own
    /// cluster summary). Used to gate leader-side endpoint verification.
    fn is_leader(&self) -> bool {
        let summary = self.handle.cluster_summary();
        summary.leader == Some(summary.local_id)
    }

    /// Refuse unless node `id` is currently bound to `identity` in membership
    /// (ADR 0037 §6): a machine leaf may only promote the seat its own subject
    /// names.
    fn require_bound_to(&self, id: CoordinatorId, identity: &str) -> Result<(), Status> {
        let bound = self
            .handle
            .cluster_summary()
            .members
            .iter()
            .find(|m| m.id == id)
            .map(|m| m.machine_identity.clone());
        match bound {
            Some(bound) if bound == identity => Ok(()),
            _ => Err(permission_denied(
                "a machine certificate may only promote the node id bound to its own machine \
                 identity (ADR 0037 §6)",
            )),
        }
    }

    /// Endpoint verification before admission (ADR 0037 §6): dial `addr` over
    /// mTLS and require its serving leaf's CN to equal `identity`, then
    /// `ProbeCluster` there and require the claimed `node_id`.
    async fn verify_endpoint(
        &self,
        addr: &str,
        identity: &str,
        node_id: CoordinatorId,
    ) -> Result<(), Status> {
        let subject = coppice_tls::read_serving_leaf(&self.tls, addr)
            .await
            .map_err(|e| {
                Status::failed_precondition(format!(
                    "endpoint verification failed: could not read the serving certificate at \
                     {addr}: {e} (ADR 0037 §6)"
                ))
            })?;
        if subject.common_name.as_deref() != Some(identity) {
            return Err(Status::failed_precondition(format!(
                "endpoint verification failed: {addr} presents machine identity {:?}, not the \
                 claimed {identity} (ADR 0037 §6)",
                subject.common_name
            )));
        }
        let mut client = admin_channel_from_store(addr, &self.tls)
            .await
            .map_err(|e| Status::failed_precondition(format!("endpoint verification dial: {e}")))?;
        let probe = client
            .probe_cluster(pb::ProbeClusterRequest {})
            .await
            .map_err(|s| {
                Status::failed_precondition(format!("endpoint verification probe: {}", s.message()))
            })?
            .into_inner();
        if probe.node_id != Some(node_id) {
            return Err(Status::failed_precondition(format!(
                "endpoint verification failed: {addr} reports node id {:?}, not the claimed \
                 {node_id} (ADR 0037 §6)",
                probe.node_id
            )));
        }
        Ok(())
    }

    /// The formation double-init guard (ADR 0037 §3 case c): one round of
    /// discovery+probe; refuse if any reachable candidate already reports an
    /// initialized cluster with this `cluster_uuid`.
    async fn formation_probe_guard(&self) -> Result<(), Status> {
        for candidate in self.discovery.candidates().await {
            let Ok(mut client) = admin_channel_from_store(&candidate, &self.tls).await else {
                continue; // unreachable candidate: skip, this is a guard not a census
            };
            if let Ok(resp) = client.probe_cluster(pb::ProbeClusterRequest {}).await {
                let resp = resp.into_inner();
                if resp.initialized && resp.cluster_uuid == self.cluster_uuid {
                    return Err(Status::failed_precondition(format!(
                        "refusing to initialize: candidate {candidate} already reports an \
                         initialized cluster with this cluster id — this would fork formation \
                         (ADR 0037 §3)"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Apply the optional formation policy (ADR 0037 §3).
    ///
    /// Parses the operator-supplied bootstrap-policy TOML
    /// ([`crate::policy::FormationPolicy`]) and proposes its idempotent puts —
    /// the priority-multiplier table and the quota entities a fresh cluster
    /// needs. Applied against the current applied state so a same-token re-init
    /// (or a crash-resumed formation) proposes nothing already present. Runs
    /// immediately after `raft.initialize`, so it rides out the leaderless
    /// window before the founding voter wins its first election.
    async fn apply_formation_policy(&self, policy_toml: &[u8]) -> Result<(), Status> {
        let policy = crate::policy::FormationPolicy::parse_toml(policy_toml).map_err(|e| {
            Status::invalid_argument(format!(
                "invalid formation policy TOML: {e:#} (ADR 0037 §3)"
            ))
        })?;
        // Read the current applied state so the puts skip anything already
        // present (idempotent re-init). The view is a cheap Arc snapshot.
        let commands = {
            let view = self.consensus.views().latest();
            policy.commands(view.state(), coppice_core::time::Timestamp::now())
        };
        if commands.is_empty() {
            return Ok(());
        }
        crate::policy::propose_all(self.consensus.as_ref(), commands)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "applying the formation policy: {e:#} (ADR 0037 §3)"
                ))
            })?;
        Ok(())
    }
}

/// The subject CN a request arrived under (ADR 0037 §6), or `None` when there is
/// no client certificate or it carries no CN.
fn peer_common_name<T>(request: &Request<T>) -> Option<String> {
    request
        .peer_certs()
        .and_then(|certs| certs.first().cloned())
        .and_then(|leaf| coppice_tls::parse_leaf_subject_der(leaf.as_ref()))
        .and_then(|s| s.common_name)
}

/// A `PERMISSION_DENIED` refusal for the ADR 0037 §6 authorization matrix.
fn permission_denied(reason: &str) -> Status {
    Status::permission_denied(format!("{reason} (ADR 0037 §6)"))
}

/// Build a `FAILED_PRECONDITION` status carrying a decodable
/// [`ConflictingFormationToken`](pb::ConflictingFormationToken) refusal detail
/// (ADR 0037 §3), naming the recorded token so recovery re-runs `cluster init`
/// with it.
fn conflicting_formation_token_status(recorded: String) -> Status {
    refusal_status(
        pb::membership_refusal::Reason::ConflictingFormationToken(pb::ConflictingFormationToken {
            recorded_token: recorded.clone(),
        }),
        format!(
            "a different formation token is already recorded ({recorded}); re-run `cluster init` \
             with that token to resume (ADR 0037 §3)"
        ),
    )
}

/// Map a consensus-seam failure onto the gRPC status the admin RPC returns.
///
/// The retryable variants (ADR 0016) become `FAILED_PRECONDITION` /`ABORTED` /
/// `DEADLINE_EXCEEDED` so a caller can branch and retry; terminal ones become
/// `UNAVAILABLE` / `INTERNAL`. The `LearnerNotCaughtUp` message deliberately
/// contains "behind" — the promotion client keys its poll loop on it.
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
        // ADR 0037 refusals: FAILED_PRECONDITION with a machine-readable
        // `MembershipRefusal` in the Status details so a converging daemon can
        // branch without parsing prose (see admin.proto).
        ConsensusError::SameIdDifferentAddress { existing_addr } => refusal_status(
            pb::membership_refusal::Reason::SameIdDifferentAddress(pb::SameIdDifferentAddress {
                existing_address: existing_addr.clone(),
            }),
            format!(
                "node is already in membership at a different address ({existing_addr}); \
                 a moved instance is a new instance — no silent repointing (ADR 0037 §4)"
            ),
        ),
        ConsensusError::MachineSeatPending { incumbent } => refusal_status(
            pb::membership_refusal::Reason::MachineSeatPending(pb::MachineSeatPending {
                incumbent_id: incumbent,
            }),
            format!(
                "this machine's replacement seat is held by pending learner {incumbent}; \
                 watch status rather than resubmitting (ADR 0037 §6)"
            ),
        ),
        ConsensusError::PromotionRefused(reason) => {
            use coppice_consensus::PromotionRefusal;
            let pb_reason = match reason {
                PromotionRefusal::VoterSetFull => pb::promotion_refused::Reason::VoterSetFull,
                PromotionRefusal::NoRemovablePeer => pb::promotion_refused::Reason::NoRemovablePeer,
            };
            refusal_status(
                pb::membership_refusal::Reason::PromotionRefused(pb::PromotionRefused {
                    reason: pb_reason as i32,
                }),
                format!("promotion refused: {reason} (ADR 0037 §5)"),
            )
        }
        ConsensusError::UnknownNode { id } => {
            Status::failed_precondition(format!("node {id} is not in membership (ADR 0037 §4)"))
        }
    }
}

/// Build a `FAILED_PRECONDITION` status carrying a machine-readable
/// [`MembershipRefusal`](pb::MembershipRefusal) in its binary details
/// (ADR 0037 §4/§5/§6). The single refusal idiom across the admin surface: the
/// message is for operators, the details for automation.
fn refusal_status(reason: pb::membership_refusal::Reason, human: String) -> Status {
    let detail = pb::MembershipRefusal {
        reason: Some(reason),
    };
    Status::with_details(
        Code::FailedPrecondition,
        human,
        bytes::Bytes::from(prost::Message::encode_to_vec(&detail)),
    )
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
            machine_identity: m.machine_identity.clone(),
            superseded: m.superseded,
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

/// Dial the admin surface of `target` over mTLS using a [`TlsStore`]'s current
/// material (ADR 0037 §6), so a rotated leaf is used on the next dial. This is
/// the seam the daemon's own convergence loop, the formation probe guard, and
/// endpoint verification dial through — all present the daemon's own machine
/// certificate rather than PEM read from config paths.
pub async fn admin_channel_from_store(target: &str, store: &TlsStore) -> Result<Client<Channel>> {
    let material = store.current();
    admin_channel(
        target,
        material.ca_pem(),
        material.cert_pem(),
        material.key_pem(),
    )
    .await
}

/// Add a learner (ADR 0016 step 2).
pub async fn add_learner(
    client: &mut Client<Channel>,
    cluster_uuid: [u8; 16],
    node_id: CoordinatorId,
    addr: String,
) -> Result<()> {
    client
        .add_learner(pb::AddLearnerRequest {
            cluster_uuid: cluster_uuid.to_vec(),
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
    cluster_uuid: [u8; 16],
    node_id: CoordinatorId,
) -> Result<()> {
    client
        .remove_node(pb::RemoveNodeRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            node_id,
        })
        .await
        .map_err(status_to_anyhow)?;
    Ok(())
}

/// Fetch a coordinator's cluster-status view (ADR 0016).
pub async fn cluster_status(
    client: &mut Client<Channel>,
    cluster_uuid: [u8; 16],
) -> Result<pb::ClusterStatusResponse> {
    let resp = client
        .cluster_status(pb::ClusterStatusRequest {
            cluster_uuid: cluster_uuid.to_vec(),
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
    cluster_uuid: [u8; 16],
    promote: CoordinatorId,
    remove: Option<CoordinatorId>,
    wait: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let result = client
            .promote_voter(pb::PromoteVoterRequest {
                cluster_uuid: cluster_uuid.to_vec(),
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
    let resolved = config::load(&args.config)
        .with_context(|| format!("reading config {}", args.config.display()))?;
    let cfg = &resolved.config;

    // Default `--target` to the first `static` discovery seed (ADR 0037 §2:
    // `[discovery.static] addrs` subsumes the old top-level `peers`). Any other
    // backend (dns/file/ec2-asg) carries no literal seed usable as a default, so
    // `--target` is then required.
    let target = match &args.target {
        Some(t) => t.clone(),
        None => cfg
            .discovery
            .static_addrs()
            .first()
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "no --target given and no default is available: the config's \
                     discovery backend is \"{}\" with no usable seed address \
                     (only backend = \"static\" with a non-empty \
                     [discovery.static] addrs provides one). Pass --target <host:port>",
                    cfg.discovery.backend.as_str()
                )
            })?,
    };

    let cluster_uuid = *cfg.cluster_id.0.as_bytes();
    let ca = read_pem(&cfg.tls.ca_path)?;
    let cert = read_pem(&cfg.tls.cert_path)?;
    let key = read_pem(&cfg.tls.key_path)?;

    let mut client = admin_channel(&target, &ca, &cert, &key).await?;

    match args.verb {
        AdminVerb::AddLearner { node_id, addr } => {
            add_learner(&mut client, cluster_uuid, node_id, addr.clone()).await?;
            println!("added node {node_id} as a learner ({addr})");
        }
        AdminVerb::Promote {
            node_id,
            remove,
            wait,
        } => {
            promote_voter(&mut client, cluster_uuid, node_id, remove, wait).await?;
            match remove {
                Some(r) => println!("promoted node {node_id} to voter, removed node {r}"),
                None => println!("promoted node {node_id} to voter"),
            }
        }
        AdminVerb::Remove { node_id } => {
            remove_node(&mut client, cluster_uuid, node_id).await?;
            println!("removed node {node_id} from membership");
        }
        AdminVerb::SetAddress { node_id, addr } => {
            // Deliberately not implemented (ADR 0037 §4): the leader-side
            // verified repoint (dial the new address, match its TLS subject to
            // the target's machine-identity binding, confirm its stamped node id
            // by probe) has no RPC yet. Refuse rather than commit an unverified
            // `SetNodes`, which openraft warns can split-brain.
            bail!(
                "admin set-address (node {node_id} -> {addr}) is not implemented: the verified \
                 leader-side repoint of ADR 0037 §4 has no membership-repointing RPC yet. Under \
                 the immutable model an instance whose address changed is a new instance — let it \
                 self-join and retire the old identity via replacement promotion."
            );
        }
        AdminVerb::Status { json } => {
            let status = cluster_status(&mut client, cluster_uuid).await?;
            if json {
                println!("{}", render_status_json(&status));
            } else {
                print!("{}", render_status(&status));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `cluster init` (ADR 0037 §3)
// ---------------------------------------------------------------------------

/// The result of a successful `cluster init` (ADR 0037 §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterInitOutcome {
    /// The forming replica's stamped raft identity.
    pub node_id: u64,
    /// True when a matching-token request resumed/re-reported a formation that
    /// had already completed; false when this call performed formation.
    pub already_initialized: bool,
}

/// Drive `InitializeCluster` against one parked coordinator over mTLS
/// (ADR 0037 §3), the server half of `coppice cluster init`.
///
/// The caller presents operator-profile material (`ca`/`cert`/`key`); machine
/// certificates are refused this verb server-side (§6). `target` is the
/// coordinator's raft/admin listener. The stamped cluster identity is learned
/// from the target itself via `ProbeCluster` — the operator need not know the
/// cluster UUID — and then supplied to `InitializeCluster`. `token` is the
/// durable formation token; `policy_toml` is the optional bootstrap policy,
/// applied idempotently as part of formation.
///
/// A conflicting-token refusal is decoded from its machine-readable detail into
/// an error whose message names the recorded token and the recovery step.
pub async fn cluster_init(
    target: &str,
    ca_pem: &[u8],
    cert_pem: &[u8],
    key_pem: &[u8],
    token: &str,
    policy_toml: Option<Vec<u8>>,
) -> Result<ClusterInitOutcome> {
    let mut client = admin_channel(target, ca_pem, cert_pem, key_pem).await?;

    // Learn the target's stamped cluster identity (the InitializeCluster body
    // carries it, but the operator running away from any daemon config does not
    // have it to hand). ProbeCluster's body is empty precisely so a caller can
    // discover it; an operator cert may call it.
    let probe = client
        .probe_cluster(pb::ProbeClusterRequest {})
        .await
        .map_err(|s| {
            anyhow!(
                "probing {target} for its cluster identity failed ({:?}): {}",
                s.code(),
                s.message()
            )
        })?
        .into_inner();

    match client
        .initialize_cluster(pb::InitializeClusterRequest {
            cluster_uuid: probe.cluster_uuid,
            formation_token: token.to_string(),
            policy_toml,
        })
        .await
    {
        Ok(resp) => {
            let resp = resp.into_inner();
            Ok(ClusterInitOutcome {
                node_id: resp.node_id,
                already_initialized: resp.already_initialized,
            })
        }
        Err(status) => Err(cluster_init_error(status)),
    }
}

/// Turn an `InitializeCluster` failure [`Status`] into an operator-facing
/// error. A conflicting-token refusal (ADR 0037 §3) is decoded from its
/// `MembershipRefusal` detail into a recovery message naming the recorded
/// token; every other failure surfaces its code and message.
fn cluster_init_error(status: Status) -> anyhow::Error {
    if status.code() == Code::FailedPrecondition {
        if let Ok(refusal) = <pb::MembershipRefusal as prost::Message>::decode(status.details()) {
            if let Some(pb::membership_refusal::Reason::ConflictingFormationToken(conflict)) =
                refusal.reason
            {
                return anyhow!(
                    "cluster init refused: a different formation token is already recorded for \
                     this cluster.\n  recorded token: {}\nRe-run `coppice cluster init` with that \
                     token — pass it as `--formation-token <token>`, or write it into your \
                     `--formation-token-file` — to resume or report the existing formation \
                     (ADR 0037 §3).",
                    conflict.recorded_token
                );
            }
        }
    }
    anyhow!(
        "cluster init failed ({:?}): {}",
        status.code(),
        status.message()
    )
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

/// Render a `ClusterStatus` response as stable JSON for `admin status --json`
/// (ADR 0037 §7): the cluster-wide view — membership (with machine-identity
/// bindings and the superseded marking), per-follower matched/lag, leader,
/// term, applied/committed — as a single object for scripting. The human table
/// ([`render_status`]) remains the default; this is a parallel, additive
/// surface whose field names are a contract.
fn render_status_json(s: &pb::ClusterStatusResponse) -> String {
    let voters: std::collections::BTreeSet<u64> = s
        .membership
        .as_ref()
        .and_then(|m| m.configs.first())
        .map(|c| c.voters.iter().copied().collect())
        .unwrap_or_default();

    let members: Vec<serde_json::Value> = s
        .membership
        .as_ref()
        .map(|m| {
            m.members
                .iter()
                .map(|member| {
                    serde_json::json!({
                        "node_id": member.node_id,
                        "addr": member.address,
                        "role": if voters.contains(&member.node_id) { "voter" } else { "learner" },
                        "machine_identity": member.machine_identity,
                        "superseded": member.superseded,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    // Per-follower replication is the leader's view only; empty on a follower.
    let replication: Vec<serde_json::Value> = s
        .replication
        .iter()
        .map(|r| {
            serde_json::json!({
                "node_id": r.node_id,
                "matched_index": r.matched_index,
                "lag": s.last_applied_index.saturating_sub(r.matched_index),
            })
        })
        .collect();

    let view = serde_json::json!({
        "node_id": s.local_node_id,
        "leader": s.leader_node_id,
        "is_leader": s.leader_node_id == Some(s.local_node_id),
        "term": s.term,
        "applied_index": s.last_applied_index,
        "committed_index": s.known_committed_index,
        "members": members,
        "replication": replication,
    });
    // Pretty-print: `admin status` is an operator/script surface, and a stable
    // key order (serde_json preserves insertion order) keeps diffs readable.
    serde_json::to_string_pretty(&view).expect("cluster status JSON is always serializable")
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
                    machine_identity: "coord-3".into(),
                    superseded: false,
                },
                MemberSummary {
                    id: 1,
                    addr: "c1:7071".into(),
                    voter: true,
                    machine_identity: "coord-1".into(),
                    superseded: false,
                },
                MemberSummary {
                    id: 2,
                    addr: "c2:7071".into(),
                    voter: true,
                    machine_identity: "coord-2".into(),
                    superseded: false,
                },
            ],
            replication: vec![(1, 100), (3, 40)],
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
    fn status_json_has_the_stable_scripting_shape() {
        // A leader (node 1) with one caught-up voter (2), one lagging voter (3
        // marked superseded), rendered as `admin status --json`.
        let summary = ClusterSummary {
            local_id: 1,
            leader: Some(1),
            term: 5,
            last_applied: 100,
            known_committed: 100,
            snapshot_last_index: Some(64),
            members: vec![
                MemberSummary {
                    id: 1,
                    addr: "c1:7071".into(),
                    voter: true,
                    machine_identity: "coord-1".into(),
                    superseded: false,
                },
                MemberSummary {
                    id: 2,
                    addr: "c2:7071".into(),
                    voter: true,
                    machine_identity: "coord-2".into(),
                    superseded: false,
                },
                MemberSummary {
                    id: 3,
                    addr: "c3:7071".into(),
                    voter: true,
                    machine_identity: "coord-3".into(),
                    superseded: true,
                },
            ],
            replication: vec![(2, 100), (3, 40)],
        };

        let pbm = cluster_summary_to_pb(summary);
        let json: serde_json::Value =
            serde_json::from_str(&render_status_json(&pbm)).expect("valid JSON");

        assert_eq!(json["node_id"], 1);
        assert_eq!(json["leader"], 1);
        assert_eq!(json["is_leader"], true);
        assert_eq!(json["term"], 5);
        assert_eq!(json["applied_index"], 100);
        assert_eq!(json["committed_index"], 100);

        let members = json["members"].as_array().expect("members array");
        assert_eq!(members.len(), 3);
        // Members carry role, machine identity, and the superseded marking.
        let m3 = members.iter().find(|m| m["node_id"] == 3).unwrap();
        assert_eq!(m3["role"], "voter");
        assert_eq!(m3["machine_identity"], "coord-3");
        assert_eq!(m3["superseded"], true);
        assert_eq!(m3["addr"], "c3:7071");

        // Per-follower replication carries matched + derived lag (leader view).
        let repl = json["replication"].as_array().expect("replication array");
        let r3 = repl.iter().find(|r| r["node_id"] == 3).unwrap();
        assert_eq!(r3["matched_index"], 40);
        assert_eq!(r3["lag"], 60);
    }

    #[test]
    fn a_generic_failed_precondition_is_not_a_behind_signal() {
        let status = Status::failed_precondition("not the leader; current leader is node 3");
        assert!(!is_learner_behind(&status));
    }

    #[test]
    fn machine_seat_pending_carries_a_decodable_refusal_detail() {
        // ADR 0037 §6: refusals are FAILED_PRECONDITION with a machine-readable
        // `MembershipRefusal` in the Status details a converging daemon decodes.
        let status = consensus_error_to_status(ConsensusError::MachineSeatPending { incumbent: 7 });
        assert_eq!(status.code(), Code::FailedPrecondition);
        let refusal = <pb::MembershipRefusal as prost::Message>::decode(status.details())
            .expect("details decode as MembershipRefusal");
        assert_eq!(
            refusal.reason,
            Some(pb::membership_refusal::Reason::MachineSeatPending(
                pb::MachineSeatPending { incumbent_id: 7 }
            ))
        );
    }

    #[test]
    fn cluster_init_conflict_renders_a_recovery_message_naming_the_recorded_token() {
        // ADR 0037 §3: a conflicting-token refusal is decoded from its
        // machine-readable detail into an operator recovery message.
        let status = conflicting_formation_token_status("stack-42".to_string());
        let err = cluster_init_error(status);
        let msg = format!("{err:#}");
        assert!(msg.contains("stack-42"), "names the recorded token: {msg}");
        assert!(
            msg.contains("Re-run `coppice cluster init`"),
            "gives the recovery step: {msg}"
        );
    }

    #[test]
    fn cluster_init_generic_failure_surfaces_code_and_message() {
        let status = Status::unavailable("connection reset");
        let msg = format!("{:#}", cluster_init_error(status));
        assert!(msg.contains("Unavailable"), "{msg}");
        assert!(msg.contains("connection reset"), "{msg}");
    }

    #[test]
    fn promotion_refused_details_name_the_reason() {
        use coppice_consensus::PromotionRefusal;
        let status = consensus_error_to_status(ConsensusError::PromotionRefused(
            PromotionRefusal::VoterSetFull,
        ));
        assert_eq!(status.code(), Code::FailedPrecondition);
        let refusal = <pb::MembershipRefusal as prost::Message>::decode(status.details())
            .expect("details decode as MembershipRefusal");
        let Some(pb::membership_refusal::Reason::PromotionRefused(pr)) = refusal.reason else {
            panic!(
                "expected a PromotionRefused reason, got {:?}",
                refusal.reason
            );
        };
        assert_eq!(pr.reason(), pb::promotion_refused::Reason::VoterSetFull);
    }
}
