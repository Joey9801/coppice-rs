//! The leader-side enrollment core (ADR 0037 §4/§5).
//!
//! One machine with a role-scoped token and a CSR becomes a machine with a
//! cluster-signed leaf. This module is the single implementation of that step,
//! shared by every entry point that can reach it: the `ForwardEnroll` admin RPC
//! a follower proxies through, and — from the next chunk — the public
//! `POST /api/v1/enroll` handler on the client listener. Both call
//! [`handle_enroll`]; neither reimplements token verification, role dispatch, or
//! signing.
//!
//! Two rules shape everything here.
//!
//! **The token is the whole authentication, so its failure must be an oracle
//! for nothing.** [`verify_enroll_token`] scans *every* live token and hashes
//! against each one, with no early exit — an unknown secret, a revoked token, an
//! expired token, and a right-secret-wrong-role all cost the same work and
//! produce the same `None`. A valid token claiming a **revoked identity** is
//! refused the same way: renewal-refusal is the revocation mechanism (§5), and
//! re-enrolling the same subject through the reusable fleet token would be its
//! bypass. Callers get one opaque [`EnrollError::Unauthorized`] out of all
//! five, and must not enrich it.
//!
//! **The secret never leaves the request.** No type in this module derives
//! `Debug` over a secret, nothing logs a token field, and the only value that
//! travels back out is the issued certificate.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use coppice_consensus::{Consensus, ConsensusError};
use coppice_core::id::{EnrollTokenId, MachineId, NodeId};
use coppice_core::time::Timestamp;
use coppice_state::command::RecordEnrolledIdentity;
use coppice_state::{Command, EnrollRole, RevokedIdentity, StateMachine};
use coppice_tls::pki;

/// A verified token's identity: which record matched, and what it grants.
pub(crate) type VerifiedToken = (EnrollTokenId, EnrollRole);

/// Match `presented` against the live enrollment tokens in `state` at `now`,
/// returning the matching token's id and role (ADR 0037 §5).
///
/// **Uniform work by construction.** Every live token is hashed against, in id
/// order, with no early return — so the elapsed time does not distinguish
/// "unknown secret" from "matched the third token". Revoked and expired tokens
/// are excluded by [`StateMachine::live_enroll_tokens`] before the scan, which
/// makes them indistinguishable from tokens that never existed. The caller
/// compares the returned role itself and fails identically on a mismatch, so a
/// wrong-role token is no more informative than a wrong secret.
///
/// The argon2 verification cost is real and deliberate: token counts are
/// operator-scale (a handful), and this is the rate-limited public ingress
/// path's only credential check.
pub(crate) fn verify_enroll_token(
    state: &StateMachine,
    presented: &str,
    now: Timestamp,
) -> Option<VerifiedToken> {
    let mut matched: Option<VerifiedToken> = None;
    for (id, token) in state.live_enroll_tokens(now) {
        if pki::verify_secret(presented, &token.hash) {
            matched = Some((*id, token.role));
        }
    }
    matched
}

/// Everything [`handle_enroll`] needs from the running coordinator: the
/// consensus seam (to read applied state and to propose), the data directory
/// holding this voter's CA key, and the formation gate.
pub(crate) struct EnrollContext<'a, C: Consensus> {
    pub consensus: &'a C,
    pub data_dir: &'a Path,
    /// Whether the local `formation_complete` marker exists. Enrollment is
    /// refused before it does (ADR 0037 §3) — checked here as well as at every
    /// entry point, because this is the function that signs.
    pub formed: bool,
}

/// One enrollment request as the core sees it.
///
/// `Debug` is hand-written and redacts `token`: this struct is the value most
/// likely to end up in an error context or a tracing field by accident, and it
/// must be inert if it does (ADR 0037 §4: the token must never be logged).
pub(crate) struct EnrollRequest<'a> {
    pub token: &'a str,
    pub csr_pem: &'a [u8],
    /// The node id an agent-role enrollee claims (ADR 0011).
    pub node_id: Option<NodeId>,
    /// The machine id a coordinator-role enrollee minted for itself (§7).
    pub machine_id: Option<MachineId>,
    /// The hostnames/IPs the enrollee will serve on, requested as SANs.
    ///
    /// Metadata, never identity (ADR 0037 §4): the subject is dictated below
    /// from the claimed id, and the leaf's SANs decide only which addresses
    /// this machine can *serve* under. Accepting them from the enrollee is
    /// what makes a coordinator's own leaf usable for its raft listener at
    /// all — the cluster cannot know a node's advertised address before that
    /// node tells it. What makes a declared address trustworthy is the
    /// leader's dial-back verification at admission (§6), which happens
    /// against the running listener, not against this list.
    pub sans: &'a [String],
}

impl std::fmt::Debug for EnrollRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnrollRequest")
            .field("token", &"<redacted>")
            .field("csr_pem_len", &self.csr_pem.len())
            .field("node_id", &self.node_id)
            .field("machine_id", &self.machine_id)
            .field("sans", &self.sans)
            .finish()
    }
}

/// A successful enrollment: the issued leaf and the CA bundle that anchors it.
#[derive(Debug, Clone)]
pub(crate) struct Enrolled {
    pub cert_pem: Vec<u8>,
    pub ca_pem: Vec<u8>,
}

/// Why an enrollment did not produce a certificate.
///
/// [`Unauthorized`](EnrollError::Unauthorized) is the *only* authentication
/// outcome: unknown, revoked, expired, and wrong-role tokens all land on it
/// carrying no detail, and callers must map it to one indistinguishable
/// response. The other variants are honest operational failures that reveal
/// nothing about the credential.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EnrollError {
    /// The single, uniform authentication failure.
    #[error("enrollment token is not valid for this request")]
    Unauthorized,

    /// The request is malformed — an empty or unparseable CSR. Note the
    /// asymmetry with `Unauthorized`: a missing claimed identity for the
    /// token's role is a *wrong-role* presentation and lands on
    /// `Unauthorized`, because wrong-role must not be distinguishable from
    /// an unknown token (ADR 0037 §4).
    #[error("{0}")]
    BadRequest(String),

    /// This coordinator has not formed a cluster (ADR 0037 §3).
    #[error(
        "this coordinator has not formed a cluster: enrollment is refused until the \
             formation_complete marker exists (ADR 0037 §3)"
    )]
    NotFormed,

    /// Signing runs where the CA key is; this replica is not the leader.
    #[error("not the leader; the leader is where the CA key signs (ADR 0037 §4)")]
    NotLeader { leader: Option<u64> },

    /// Anything else — CA load, signing, or the replicated write.
    #[error("{0}")]
    Internal(String),
}

impl From<ConsensusError> for EnrollError {
    fn from(err: ConsensusError) -> Self {
        match err {
            ConsensusError::NotLeader { leader } => EnrollError::NotLeader { leader },
            other => EnrollError::Internal(other.to_string()),
        }
    }
}

/// Issue a leaf for an enrolling machine (ADR 0037 §4).
///
/// The token's **role** decides everything that follows, and the enrollee's
/// claim is checked against it rather than trusted:
///
/// - *agent* → a `node_id` is required, the leaf is `CN = node id` (ADR 0011),
///   and nothing is written to replicated state: an agent's admission is its
///   registration, which the gateway already authenticates against this leaf.
/// - *coordinator* → a `machine_id` is required (the enrollee minted and
///   persisted it itself, §7), the leaf carries it as its subject, and the
///   cluster records the enrollment as a replicated fact. Binding that machine
///   to a raft seat is admission, not enrollment, and stays in chunk 05.
///
/// The CSR contributes only a public key; the subject is dictated here.
pub(crate) async fn handle_enroll<C: Consensus>(
    ctx: &EnrollContext<'_, C>,
    request: EnrollRequest<'_>,
) -> Result<Enrolled, EnrollError> {
    // Defence in depth: every caller gates on formation too, but this is the
    // function with the CA key in reach.
    if !ctx.formed {
        return Err(EnrollError::NotFormed);
    }
    // Signing happens on the leader (ADR 0037 §4) — enforced HERE, not only by
    // the callers' routing, because the agent branch below signs without ever
    // proposing (nothing downstream would say `NotLeader`), every voter's disk
    // holds the CA key, and `ForwardEnroll` lands on whichever replica was
    // dialled. This is the local leadership fact, not a barrier: cheap, and
    // the residual instant-of-deposal race costs at most one leaf signed by a
    // voter that was the leader a moment ago — the routing invariant is what
    // this guard holds, not linearizability.
    require_leader(ctx.consensus)?;

    let now = Timestamp::now();
    // The latest published view, not a strong read. Renewal takes the
    // barrier (`renew_coordinator`) because refusing renewal is the whole
    // eviction lever; enrollment does not, because this is public,
    // rate-limited ingress and a read barrier per attempt would hand an
    // unauthenticated caller a lever on the leader. The cost is bounded:
    // a token or identity revoked microseconds ago may admit one more
    // enrollment within the view cadence, and short leaf lifetimes bound
    // what that buys.
    let view = ctx.consensus.views().latest();
    // The argon2 scan is deliberate CPU work (see `verify_enroll_token`), so
    // it runs off the async workers: concurrent enrollment attempts must not
    // stall the executor. The view is an `Arc` clone, and the owned copy of
    // the token dies with the closure — nothing here logs or returns it.
    let verified = {
        let view = view.clone();
        let token = request.token.to_owned();
        tokio::task::spawn_blocking(move || verify_enroll_token(view.state(), &token, now))
            .await
            .map_err(|e| EnrollError::Internal(format!("token verification task: {e}")))?
    };
    let (_, role) = verified.ok_or(EnrollError::Unauthorized)?;

    // Refuse an empty CSR before doing any work with the CA key; a malformed
    // one is caught by `issue_*` below, which verifies proof-of-possession.
    if request.csr_pem.is_empty() {
        return Err(EnrollError::BadRequest(
            "the enrollment request carries no certificate signing request".to_string(),
        ));
    }

    let (signer, ca_pem) = load_ca(ctx.data_dir, ctx.consensus)?;

    match role {
        EnrollRole::Agent => {
            // A missing claim for the token's role is a wrong-role
            // presentation, and wrong-role must be indistinguishable from an
            // unknown token (ADR 0037 §4: no validity oracle) — so this is
            // `Unauthorized`, not a descriptive bad-request.
            let node = request.node_id.ok_or(EnrollError::Unauthorized)?;
            // A revoked identity must not walk back in through the (reusable,
            // deliberately long-lived) fleet token: renewal-refusal is the
            // revocation mechanism (§5), and re-enrollment for the same
            // subject would be its bypass. Same uniform refusal — whether a
            // subject is revoked is not this endpoint's to reveal.
            if view
                .state()
                .is_identity_revoked(&RevokedIdentity::Node(node))
            {
                return Err(EnrollError::Unauthorized);
            }
            let cert_pem = pki::issue_agent(&signer, request.csr_pem, &node, request.sans)
                .map_err(|e| EnrollError::BadRequest(format!("signing the agent CSR: {e}")))?;
            tracing::info!(node = %node, "enroll: issued an agent leaf");
            Ok(Enrolled { cert_pem, ca_pem })
        }
        EnrollRole::Coordinator => {
            // Same rules as the agent arm: wrong-role and a revoked claimed
            // identity both collapse to the uniform authentication failure.
            let machine = request.machine_id.ok_or(EnrollError::Unauthorized)?;
            if view
                .state()
                .is_identity_revoked(&RevokedIdentity::Machine(machine))
            {
                return Err(EnrollError::Unauthorized);
            }
            let cert_pem = pki::issue_coordinator(&signer, request.csr_pem, &machine, request.sans)
                .map_err(|e| {
                    EnrollError::BadRequest(format!("signing the coordinator CSR: {e}"))
                })?;

            // The replicated fact that this installation enrolled. Idempotent:
            // a re-enrollment after a leaf loss re-applies as a no-op keeping
            // the original instant.
            ctx.consensus
                .propose(Command::RecordEnrolledIdentity(RecordEnrolledIdentity {
                    machine,
                    recorded_at: now,
                }))
                .await?
                .outcome
                .map_err(|reason| {
                    EnrollError::Internal(format!("recording the enrolled identity: {reason}"))
                })?;

            tracing::info!(machine = %machine, "enroll: issued a coordinator leaf");
            Ok(Enrolled { cert_pem, ca_pem })
        }
    }
}

/// Re-issue a coordinator leaf for an already-authenticated machine (ADR 0037
/// §4 renewal).
///
/// `machine` comes from the *verified* client certificate the request arrived
/// on — never from the request body — so renewal preserves the subject by
/// construction. An identity an operator has marked revoked is refused: that
/// refusal, with short leaf lifetimes, is v1's revocation mechanism (§5).
///
/// The revocation check is a **strong read** (read barrier, then a view at
/// least that fresh). Renewal is the whole eviction lever, so an operator who
/// has just revoked an identity must not see it renew once more off a view
/// that had not caught up — which the published-view cadence would otherwise
/// permit. Renewal is leader-only, so the barrier is always available.
pub(crate) async fn renew_coordinator<C: Consensus>(
    ctx: &EnrollContext<'_, C>,
    machine: MachineId,
    csr_pem: &[u8],
    sans: &[String],
) -> Result<Enrolled, EnrollError> {
    if !ctx.formed {
        return Err(EnrollError::NotFormed);
    }
    let index = ctx.consensus.read_index().await?;
    let view = ctx.consensus.views().at_least(index).await?;
    if view
        .state()
        .is_identity_revoked(&RevokedIdentity::Machine(machine))
    {
        return Err(EnrollError::Unauthorized);
    }
    drop(view);
    let (signer, ca_pem) = load_ca(ctx.data_dir, ctx.consensus)?;
    let cert_pem = pki::issue_coordinator(&signer, csr_pem, &machine, sans)
        .map_err(|e| EnrollError::BadRequest(format!("signing the coordinator CSR: {e}")))?;
    tracing::info!(machine = %machine, "renew: re-issued a coordinator leaf");
    Ok(Enrolled { cert_pem, ca_pem })
}

/// Re-issue an agent leaf for an already-authenticated node (ADR 0037 §4).
///
/// The agent-plane twin of [`renew_coordinator`]; `node` comes from the
/// verified client certificate, so the subject is preserved.
pub(crate) fn renew_agent(
    state: &StateMachine,
    signer: &pki::CaSigner,
    ca_pem: &[u8],
    node: NodeId,
    csr_pem: &[u8],
) -> Result<Enrolled, EnrollError> {
    if state.is_identity_revoked(&RevokedIdentity::Node(node)) {
        return Err(EnrollError::Unauthorized);
    }
    let cert_pem = pki::issue_agent(signer, csr_pem, &node, &[])
        .map_err(|e| EnrollError::BadRequest(format!("signing the agent CSR: {e}")))?;
    tracing::info!(node = %node, "renew: re-issued an agent leaf");
    Ok(Enrolled {
        cert_pem,
        ca_pem: ca_pem.to_vec(),
    })
}

/// Refuse unless this replica currently believes it is the leader, carrying
/// the leader hint when one is known.
fn require_leader<C: Consensus>(consensus: &C) -> Result<(), EnrollError> {
    let role = consensus.status().borrow().role.clone();
    refuse_unless_leader(role)
}

/// The decision half of [`require_leader`], separated so the refusal table is
/// directly testable: only a replica that currently believes it leads may
/// reach the CA key.
fn refuse_unless_leader(role: coppice_consensus::Role) -> Result<(), EnrollError> {
    match role {
        coppice_consensus::Role::Leader { .. } => Ok(()),
        coppice_consensus::Role::Follower { leader } => Err(EnrollError::NotLeader { leader }),
        // Not knowing is not leading.
        coppice_consensus::Role::Unknown => Err(EnrollError::NotLeader { leader: None }),
    }
}

fn load_ca<C: Consensus>(
    data_dir: &Path,
    consensus: &C,
) -> Result<(pki::CaSigner, Vec<u8>), EnrollError> {
    crate::formation::load_cluster_ca(data_dir, consensus)
        .map_err(|e| EnrollError::Internal(format!("{e:#}")))
}

// ---------------------------------------------------------------------------
// The public `POST /api/v1/enroll` endpoint (ADR 0037 §4)
// ---------------------------------------------------------------------------

/// How long a follower waits for the leader to answer its proxied enrollment
/// before giving up. Generous next to a signing operation, short enough that a
/// wedged leader does not hold an ingress slot.
const PROXY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The running coordinator behind `POST /api/v1/enroll`.
///
/// The route itself (limits, uniform refusals, header handling) lives in
/// `coppice-api`; this is only the part that needs a cluster: sign here when
/// this replica is the leader, and proxy to the leader when it is not.
pub(crate) struct EnrollService<C: Consensus> {
    consensus: Arc<C>,
    data_dir: PathBuf,
    node: coppice_consensus::NodeHandle,
    /// This node's machine-plane identity, for the proxy dial. Read at dial
    /// time (not frozen) so a rotated leaf is presented on the next hop.
    tls: Arc<coppice_tls::TlsStore>,
}

impl<C: Consensus> EnrollService<C> {
    pub(crate) fn new(
        consensus: Arc<C>,
        data_dir: PathBuf,
        node: coppice_consensus::NodeHandle,
        tls: Arc<coppice_tls::TlsStore>,
    ) -> Arc<EnrollService<C>> {
        Arc::new(EnrollService {
            consensus,
            data_dir,
            node,
            tls,
        })
    }

    /// The endpoint the post-formation router captures.
    pub(crate) fn endpoint(self: Arc<Self>) -> coppice_api::http::EnrollEndpoint {
        coppice_api::http::EnrollEndpoint::new(move |call| {
            let service = Arc::clone(&self);
            async move { service.issue(call).await }
        })
    }

    /// Issue for one request, wherever the CA key happens to be.
    ///
    /// **A follower never redirects and never signs.** Signing needs the CA
    /// key and the replicated write needs the leader, so a follower proxies the
    /// request internally over the mTLS admin channel (ADR 0037 §4) — the
    /// enrolling machine is holding a token, and telling it to re-send that
    /// token somewhere else is exactly what the ADR forbids.
    async fn issue(
        &self,
        call: coppice_api::http::EnrollCall,
    ) -> Result<coppice_enroll::EnrollResponse, coppice_api::http::EnrollRefusal> {
        let summary = self.node.cluster_summary();
        if summary.leader == Some(summary.local_id) {
            let ctx = EnrollContext {
                consensus: self.consensus.as_ref(),
                data_dir: &self.data_dir,
                // The route exists only on the post-formation router.
                formed: true,
            };
            let issued = handle_enroll(
                &ctx,
                EnrollRequest {
                    token: &call.token,
                    csr_pem: call.csr_pem.as_bytes(),
                    node_id: call.node_id,
                    machine_id: call.machine_id,
                    sans: &call.sans,
                },
            )
            .await
            .map_err(refusal)?;
            return Ok(coppice_enroll::EnrollResponse {
                cert_pem: pem_string(issued.cert_pem)?,
                ca_pem: pem_string(issued.ca_pem)?,
            });
        }

        let Some(leader) = summary.leader else {
            return Err(coppice_api::http::EnrollRefusal::Unavailable(
                "no leader is currently known (an election is in progress); retry".to_string(),
            ));
        };
        let Some(addr) = summary
            .members
            .iter()
            .find(|m| m.id == leader)
            .map(|m| m.addr.clone())
        else {
            return Err(coppice_api::http::EnrollRefusal::Unavailable(format!(
                "the leader (node {leader}) has no address in this replica's membership view; \
                 retry"
            )));
        };

        forward_to_leader(&addr, self.node.history_id(), &self.tls, &call).await
    }
}

/// Proxy one enrollment to the leader over the mTLS admin channel.
///
/// The token crosses one already-mutually-authenticated internal hop and is
/// never logged on either side. Failures collapse the same way the leader's own
/// do: an authentication refusal stays the single opaque refusal, so proxying
/// cannot become a validity oracle the direct path is not.
pub(crate) async fn forward_to_leader(
    target: &str,
    history_id: [u8; 16],
    tls: &coppice_tls::TlsStore,
    call: &coppice_api::http::EnrollCall,
) -> Result<coppice_enroll::EnrollResponse, coppice_api::http::EnrollRefusal> {
    use coppice_proto::pb::raft::v1 as pb;

    let material = tls.current();
    let attempt = async {
        let mut client = crate::admin::admin_channel(
            target,
            material.ca_pem(),
            material.cert_pem(),
            material.key_pem(),
        )
        .await
        .map_err(|e| {
            coppice_api::http::EnrollRefusal::Unavailable(format!(
                "could not reach the leader to enroll: {e:#}"
            ))
        })?;

        client
            .forward_enroll(pb::ForwardEnrollRequest {
                history_id: history_id.to_vec(),
                token: call.token.clone(),
                csr_pem: call.csr_pem.clone(),
                node_id: call.node_id.map(Into::into),
                machine_id: call.machine_id.map(Into::into),
                sans: call.sans.clone(),
            })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unauthenticated => coppice_api::http::EnrollRefusal::Unauthorized,
                tonic::Code::InvalidArgument => {
                    coppice_api::http::EnrollRefusal::BadRequest(status.message().to_string())
                }
                _ => coppice_api::http::EnrollRefusal::Unavailable(format!(
                    "the leader refused the proxied enrollment: {}",
                    status.message()
                )),
            })
    };

    let issued = tokio::time::timeout(PROXY_TIMEOUT, attempt)
        .await
        .map_err(|_| {
            coppice_api::http::EnrollRefusal::Unavailable(
                "the leader did not answer the proxied enrollment in time; retry".to_string(),
            )
        })??
        .into_inner();

    Ok(coppice_enroll::EnrollResponse {
        cert_pem: issued.cert_pem,
        ca_pem: issued.ca_pem,
    })
}

/// The follower half of `/enroll` on its own: an endpoint that signs nothing
/// locally and proxies every request to `target` (ADR 0037 §4).
///
/// This is the production proxy path — [`EnrollService::issue`] calls the same
/// [`forward_to_leader`] — packaged for a caller that already knows where the
/// leader is, which is what lets the follower-proxy behaviour be driven end to
/// end without standing up a second raft replica.
pub fn proxying_enroll_endpoint(
    target: String,
    history_id: [u8; 16],
    tls: Arc<coppice_tls::TlsStore>,
) -> coppice_api::http::EnrollEndpoint {
    coppice_api::http::EnrollEndpoint::new(move |call| {
        let target = target.clone();
        let tls = Arc::clone(&tls);
        async move { forward_to_leader(&target, history_id, &tls, &call).await }
    })
}

/// Map an enrollment-core failure onto the route's refusal.
///
/// Nothing is added to [`EnrollError::Unauthorized`]: the core has already
/// collapsed unknown, revoked, expired, and wrong-role onto it, and the HTTP
/// layer renders one body for all of them.
fn refusal(err: EnrollError) -> coppice_api::http::EnrollRefusal {
    match err {
        EnrollError::Unauthorized => coppice_api::http::EnrollRefusal::Unauthorized,
        EnrollError::BadRequest(message) => coppice_api::http::EnrollRefusal::BadRequest(message),
        EnrollError::NotFormed => {
            coppice_api::http::EnrollRefusal::Unavailable(EnrollError::NotFormed.to_string())
        }
        // Leadership moved between the summary read and the propose: the
        // caller's retry lands on the new leader (or is proxied there).
        EnrollError::NotLeader { .. } => coppice_api::http::EnrollRefusal::Unavailable(
            "leadership changed while enrolling; retry".to_string(),
        ),
        EnrollError::Internal(message) => coppice_api::http::EnrollRefusal::Unavailable(message),
    }
}

fn pem_string(pem: Vec<u8>) -> Result<String, coppice_api::http::EnrollRefusal> {
    String::from_utf8(pem).map_err(|_| {
        coppice_api::http::EnrollRefusal::Unavailable("issued material is not UTF-8".to_string())
    })
}

/// The signing half a non-consensus surface (the agent gateway) needs: the data
/// directory holding the CA key, resolved against the replicated CA
/// certificate at use time.
#[derive(Clone)]
pub struct Issuer {
    data_dir: PathBuf,
    views: coppice_consensus::StateViews,
    barrier: Arc<dyn ReadBarrier>,
}

/// The read-barrier half of [`Consensus`], as a trait object.
///
/// The agent gateway is not generic over the consensus implementation, but its
/// renewal RPC needs the same strong read [`renew_coordinator`] takes — a
/// revocation must not be missed because a published view had not caught up.
/// This is that one method, erased.
pub trait ReadBarrier: Send + Sync + 'static {
    fn read_index(&self) -> BoxFuture<'_, Result<u64, ConsensusError>>;
}

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

impl<C: Consensus> ReadBarrier for C {
    fn read_index(&self) -> BoxFuture<'_, Result<u64, ConsensusError>> {
        Box::pin(Consensus::read_index(self))
    }
}

impl Issuer {
    pub(crate) fn new(
        data_dir: PathBuf,
        views: coppice_consensus::StateViews,
        barrier: Arc<dyn ReadBarrier>,
    ) -> Issuer {
        Issuer {
            data_dir,
            views,
            barrier,
        }
    }

    /// A view at least as fresh as a read barrier taken now — the read every
    /// revocation check must use (see [`renew_coordinator`]).
    pub(crate) async fn strong_view(&self) -> Result<coppice_consensus::StateView, EnrollError> {
        let index = self.barrier.read_index().await?;
        Ok(self.views.at_least(index).await?)
    }

    /// Load the cluster CA for signing: the certificate from replicated state,
    /// the key from this voter's disk (ADR 0037 §4).
    pub(crate) fn load_ca(&self) -> Result<(pki::CaSigner, Vec<u8>), EnrollError> {
        let ca_pem = self
            .views
            .latest()
            .state()
            .ca
            .as_ref()
            .map(|ca| ca.bundle.pem().to_string())
            .ok_or_else(|| {
                EnrollError::Internal(
                    "this cluster has no cluster-owned CA recorded (ADR 0037 §4)".to_string(),
                )
            })?;
        let key_pem = pki::load_ca_key(&self.data_dir, ca_pem.as_bytes())
            .map_err(|e| EnrollError::Internal(format!("loading the cluster CA key: {e}")))?;
        let signer = pki::CaSigner::load(ca_pem.as_bytes(), &key_pem)
            .map_err(|e| EnrollError::Internal(format!("loading the cluster CA: {e}")))?;
        Ok((signer, ca_pem.into_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::time::Duration;
    use coppice_state::EnrollToken;

    fn state_with(tokens: Vec<(EnrollTokenId, EnrollToken)>) -> StateMachine {
        let mut state = StateMachine::default();
        for (id, token) in tokens {
            state.enroll_tokens.insert(id, token);
        }
        state
    }

    fn token(secret: &str, role: EnrollRole, now: Timestamp) -> EnrollToken {
        EnrollToken {
            hash: pki::hash_secret(secret).unwrap(),
            role,
            label: "seeded".to_string(),
            expires_at: None,
            minted_at: now,
            revoked: false,
        }
    }

    #[test]
    fn a_live_token_verifies_to_its_id_and_role() {
        let now = Timestamp::now();
        let secret = pki::generate_secret();
        let id = EnrollTokenId::new();
        let state = state_with(vec![(id, token(&secret, EnrollRole::Agent, now))]);

        let (matched, role) =
            verify_enroll_token(&state, &secret, now).expect("the live token matches");
        assert_eq!(matched, id);
        assert_eq!(role, EnrollRole::Agent);
    }

    #[test]
    fn an_unknown_secret_does_not_verify() {
        let now = Timestamp::now();
        let state = state_with(vec![(
            EnrollTokenId::new(),
            token(&pki::generate_secret(), EnrollRole::Agent, now),
        )]);
        assert!(verify_enroll_token(&state, &pki::generate_secret(), now).is_none());
    }

    #[test]
    fn a_revoked_token_does_not_verify() {
        let now = Timestamp::now();
        let secret = pki::generate_secret();
        let mut record = token(&secret, EnrollRole::Agent, now);
        record.revoked = true;
        let state = state_with(vec![(EnrollTokenId::new(), record)]);
        assert!(
            verify_enroll_token(&state, &secret, now).is_none(),
            "a revoked token is indistinguishable from one that never existed"
        );
    }

    #[test]
    fn an_expired_token_does_not_verify() {
        let now = Timestamp::now();
        let secret = pki::generate_secret();
        let mut record = token(&secret, EnrollRole::Agent, now);
        record.expires_at = Some(now.saturating_sub(Duration::from_secs(1)));
        let state = state_with(vec![(EnrollTokenId::new(), record)]);
        assert!(verify_enroll_token(&state, &secret, now).is_none());
    }

    #[test]
    fn a_token_expiring_later_still_verifies() {
        let now = Timestamp::now();
        let secret = pki::generate_secret();
        let mut record = token(&secret, EnrollRole::Coordinator, now);
        record.expires_at = Some(now.saturating_add(Duration::from_secs(900)));
        let state = state_with(vec![(EnrollTokenId::new(), record)]);
        let (_, role) = verify_enroll_token(&state, &secret, now).expect("not yet expired");
        assert_eq!(role, EnrollRole::Coordinator);
    }

    /// The role is *returned*, never used to filter: a coordinator token
    /// presented for an agent enrollment verifies here and is refused by the
    /// caller's role dispatch, so the two failures are one response.
    #[test]
    fn the_role_comes_back_for_the_caller_to_judge() {
        let now = Timestamp::now();
        let secret = pki::generate_secret();
        let state = state_with(vec![(
            EnrollTokenId::new(),
            token(&secret, EnrollRole::Coordinator, now),
        )]);
        let (_, role) = verify_enroll_token(&state, &secret, now).unwrap();
        assert_eq!(role, EnrollRole::Coordinator);
    }

    /// Every live token is hashed against, whichever one matches — the scan has
    /// no early exit, so position carries no timing signal.
    #[test]
    fn the_last_token_matches_as_readily_as_the_first() {
        let now = Timestamp::now();
        let secrets: Vec<String> = (0..4).map(|_| pki::generate_secret()).collect();
        let mut entries = Vec::new();
        for secret in &secrets {
            entries.push((EnrollTokenId::new(), token(secret, EnrollRole::Agent, now)));
        }
        let state = state_with(entries);
        for secret in &secrets {
            assert!(verify_enroll_token(&state, secret, now).is_some());
        }
    }

    /// Signing runs only where a replica believes it leads (ADR 0037 §4).
    /// Every voter's disk holds the CA key, and the agent branch of
    /// `handle_enroll` signs without proposing — so without this table a
    /// `ForwardEnroll` landing on a follower would mint agent leaves there.
    /// A genuine two-node follower test lands with chunk 05's convergence
    /// loop; until then this table plus `require_leader`'s position at the
    /// top of `handle_enroll` is the guard.
    #[test]
    fn only_a_believed_leader_may_sign() {
        use coppice_consensus::Role;

        assert!(refuse_unless_leader(Role::Leader { term: 3 }).is_ok());

        match refuse_unless_leader(Role::Follower { leader: Some(7) }) {
            Err(EnrollError::NotLeader { leader: Some(7) }) => {}
            other => panic!("a follower must refuse with the hint, got {other:?}"),
        }
        match refuse_unless_leader(Role::Follower { leader: None }) {
            Err(EnrollError::NotLeader { leader: None }) => {}
            other => panic!("a leaderless follower must refuse, got {other:?}"),
        }
        match refuse_unless_leader(Role::Unknown) {
            Err(EnrollError::NotLeader { leader: None }) => {}
            other => panic!("not knowing is not leading, got {other:?}"),
        }
    }

    #[test]
    fn the_request_debug_never_prints_the_token() {
        let request = EnrollRequest {
            token: "cpk_super_secret_value",
            csr_pem: b"-----BEGIN CERTIFICATE REQUEST-----",
            node_id: None,
            machine_id: None,
            sans: &[],
        };
        let rendered = format!("{request:?}");
        assert!(!rendered.contains("cpk_"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }
}
