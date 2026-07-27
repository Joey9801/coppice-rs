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
//! Every RPC first authorizes the caller against the ADR 0037 §7 refusal
//! matrix — keyed on the *profile and subject of the mTLS session's client
//! certificate*, never on anything in the request body — and then checks the
//! request's stamped cluster identity (ADR 0016) before touching Raft,
//! mirroring the transport handler in `coppice-consensus`.

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

use coppice_api::http::HealthVerdict;

use coppice_consensus::{ClusterSummary, Consensus, ConsensusError, CoordinatorId, NodeHandle};
use coppice_core::id::{EnrollTokenId, MachineId, NodeId};
use coppice_core::time::{Duration as CoreDuration, Timestamp};
use coppice_net::admin::{Client, RaftAdminService};
use coppice_proto::convert::{enroll_role_from_pb, enroll_role_to_pb};
use coppice_proto::pb::raft::v1 as pb;
use coppice_state::command::{
    BindMachineIdentity, MintEnrollToken, RebindMachineAddress, RevokeEnrollToken, RevokeIdentity,
};
use coppice_state::{Command, RejectionReason, RevokedIdentity};
use coppice_tls::pki;
use coppice_tls::TlsStore;

use crate::cli::{AdminArgs, AdminVerb};
use crate::config;
use crate::enroll::{self, EnrollContext, EnrollError, EnrollRequest};

/// How often the promotion wrapper retries while a learner is still catching up.
const PROMOTE_POLL_INTERVAL: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// Machine-readable status markers
// ---------------------------------------------------------------------------
//
// The convergence loop (ADR 0037 §6) classifies admin refusals by *prefix*,
// because gRPC carries one code and one string and the codes alone cannot
// separate "keep polling" from "stop, an operator must look at this". These are
// the stable prefixes; each is deliberately distinct from the `LearnerNotCaughtUp`
// message's "behind" marker, which `is_learner_behind` keys the promotion poll
// loop on. Changing one of these strings is a wire-visible change.

/// `PromoteVoter` refused because the voter set is already at `cluster_size`
/// (ADR 0037 §7). **Retryable by polling**: the caller stays a caught-up
/// learner and re-offers itself — it is then either the `new_node_id` of a
/// pending `ReplaceVoter` or waiting on an evidence-gated removal.
pub const VOTER_SET_FULL: &str = "voter-set-full";

/// `AddLearner`/`SetNodeAddress` named a node id that is not in membership
/// (ADR 0037 §6). Terminal: no amount of waiting introduces it.
pub const UNKNOWN_NODE: &str = "unknown-node";

/// `AddLearner` named a node already in membership at a *different* address
/// (ADR 0037 §6). Terminal — there is no silent repointing; an instance whose
/// address changed is a new instance.
pub const ADDRESS_CONFLICT: &str = "address-conflict";

/// A machine identity is already bound to a different raft seat, or the seat
/// to a different identity (ADR 0037 §7). Terminal: a duplicated or stolen
/// credential, or a misissuance — an operator problem to see.
pub const MACHINE_IDENTITY_CONFLICT: &str = "machine-identity-conflict";

/// The same `(machine, node id)` pair re-offered at a different address
/// (ADR 0037 §7). Terminal: address changes go through the operator-only
/// `set-address` path, never through re-admission.
pub const MACHINE_ADDRESS_CONFLICT: &str = "machine-address-conflict";

/// The ADR 0037 §7 refusal matrix said no for this caller profile. Terminal.
pub const NOT_AUTHORIZED: &str = "not-authorized";

/// The request's `history_id` names a different raft history than this node's
/// stamp (ADR 0016). Terminal: two histories can never merge — this is a
/// wrong data volume or a wiped-and-re-formed cluster, an operator problem
/// that no amount of retrying changes.
pub const HISTORY_CONFLICT: &str = "history-conflict";

/// Dial-back verification of an advertised endpoint failed (ADR 0037 §6
/// step 3). Retryable in principle — the endpoint may not be serving yet —
/// so the convergence loop re-enters from the top rather than giving up.
pub const ENDPOINT_UNVERIFIED: &str = "endpoint-unverified";

/// `PromoteVoter` refused because the learner is still catching up
/// (`LearnerNotCaughtUp`). Retryable by polling: the human tail keeps the
/// word "behind", which `is_learner_behind` keys the CLI's promotion poll
/// loop on.
pub const LEARNER_BEHIND: &str = "learner-behind";

/// Whether `message` begins with `marker` as a whole prefix (`marker` then
/// `:`), so classification can never be fooled by a marker string appearing
/// mid-sentence in some other refusal's human tail.
pub fn has_marker(message: &str, marker: &str) -> bool {
    message
        .strip_prefix(marker)
        .is_some_and(|rest| rest.starts_with(':'))
}

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
    /// The daemon's live TLS material, used for two things the admin surface
    /// cannot do without: the **CA bundle** that classifies a caller's leaf
    /// before the cluster is formed (ADR 0037 §7 authorization must work in
    /// the parked phase too, where there is no replicated state to read the
    /// CA from), and the **client identity** the leader dials back with when
    /// verifying an advertised endpoint (§6 step 3).
    ///
    /// Behind a lock and `Option` because the daemon may bind and serve this
    /// surface before it has any material at all — a store minted by
    /// formation is published here when the seam attaches. With no store and
    /// no replicated CA, no caller can be classified and every verb is
    /// refused, which is the fail-closed direction.
    tls: RwLock<Option<Arc<TlsStore>>>,
}

/// The consensus-backed half of [`AdminService`], present only once formed.
struct Seam<C: Consensus> {
    consensus: Arc<C>,
    handle: NodeHandle,
}

impl<C: Consensus> AdminService<C> {
    /// A service on a daemon with no cluster yet: `ProbeCluster` answers from
    /// `phase`, every membership verb is refused.
    ///
    /// `tls` is the daemon's material if it has any. It may legitimately be
    /// `None` — the ADR 0037 §4 minimal deployment provisions no certificates
    /// and formation mints the first leaf — but the admin surface can only be
    /// *served* over mTLS, so whenever it is reachable a store must be
    /// present, either here or via [`attach`](Self::attach).
    pub(crate) fn unformed(
        phase: Arc<crate::formation::PhaseState>,
        data_dir: PathBuf,
        tls: Option<Arc<TlsStore>>,
    ) -> Self {
        AdminService {
            inner: Arc::new(AdminInner {
                seam: RwLock::new(None),
                phase,
                data_dir,
                tls: RwLock::new(tls),
            }),
        }
    }

    /// Attach the consensus seam once the cluster is formed. Called exactly
    /// once per process, from the formation path or straight after a normal
    /// start.
    ///
    /// `tls` is republished here because formation can mint the material after
    /// [`unformed`](Self::unformed) ran with `None`, and because the dial-back
    /// verification of ADR 0037 §6 needs a client identity that verifies under
    /// the *cluster* CA.
    pub(crate) fn attach(&self, consensus: Arc<C>, handle: NodeHandle, tls: Arc<TlsStore>) {
        *self.inner.tls.write().expect("admin tls lock") = Some(tls);
        *self.inner.seam.write().expect("admin seam lock") = Some(Seam { consensus, handle });
    }

    /// The daemon's live TLS material, or the fail-closed refusal.
    fn tls(&self) -> Result<Arc<TlsStore>, Status> {
        self.inner
            .tls
            .read()
            .expect("admin tls lock")
            .clone()
            .ok_or_else(|| {
                Status::failed_precondition(
                    "this daemon holds no TLS material, so it can neither classify a caller \
                     nor dial back to verify an endpoint (ADR 0037 §7)",
                )
            })
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
            return Err(history_conflict_status(incoming, &handle.history_id()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Authorization: the ADR 0037 §7 refusal matrix
// ---------------------------------------------------------------------------

/// Who is calling an admin verb, as attested by the mTLS session's client
/// certificate (ADR 0037 §7). Never a request-body claim.
///
/// This is the *whole* input to the refusal matrix. Because the identity is
/// read off the session, a coordinator machine's "self-scope" grant needs no
/// extra check on the way in: it cannot present an identity it does not hold,
/// so `AddLearner` for its own identity is the only `AddLearner` it can issue.
/// Only `PromoteVoter` needs an explicit self-scope test, because there the
/// subject of the verb is a raft node id, not the identity itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Caller {
    /// An `OU=coppice-operators` leaf (ADR 0022): the break-glass human or
    /// automation credential, and the only profile that may administer other
    /// installations' seats.
    Operator { cn: String },
    /// An `OU=coppice-coordinator` leaf whose CN is the cluster-minted machine
    /// identity of one coordinator installation (ADR 0037 §7).
    Machine(MachineId),
    /// A compute node's leaf (ADR 0011). Holds none of the membership surface.
    Agent(NodeId),
}

impl Caller {
    /// How the caller is named in a refusal, for an operator reading a log or
    /// a status field. Deliberately includes the subject: the interesting
    /// question after a denial is usually *which* machine tried.
    fn describe(&self) -> String {
        match self {
            Caller::Operator { cn } => format!("operator {cn:?}"),
            Caller::Machine(machine) => format!("coordinator machine {machine}"),
            Caller::Agent(node) => format!("agent {node}"),
        }
    }
}

impl<C: Consensus> AdminService<C> {
    /// Classify the client certificate this request arrived under (ADR 0037
    /// §7).
    ///
    /// The chain was already verified by the TLS acceptor, which requires a
    /// client certificate; [`pki::verify_leaf`] re-runs the chain check
    /// against the CA *this cluster believes in* and classifies the subject
    /// into a profile. Re-running it here rather than trusting the handshake
    /// is what makes a re-rooted CA (ADR 0037 §4) take effect on the
    /// authorization plane without a restart.
    ///
    /// The CA comes from replicated state when the cluster is formed — the
    /// authority of record — and from the daemon's own bundle on disk when it
    /// is not, because a parked daemon still answers `ProbeCluster` and must
    /// still refuse an agent that asks.
    fn caller<T>(&self, request: &Request<T>) -> Result<Caller, Status> {
        let peer = request.peer_certs();
        let leaf = peer
            .as_ref()
            .and_then(|certs| certs.first())
            .ok_or_else(|| {
                Status::unauthenticated(
                    "the admin surface requires a client certificate (ADR 0011/0037 §7)",
                )
            })?;

        let ca_pem = self.authorizing_ca()?;
        let verified = pki::verify_leaf(&ca_pem, leaf.as_ref()).map_err(|e| {
            // Deliberately opaque about *why*: a caller whose leaf does not
            // verify learns nothing about this cluster's trust root from the
            // refusal. The detail goes to the log instead.
            tracing::debug!(error = %e, "admin: client certificate did not classify");
            Status::unauthenticated("client certificate does not verify against the cluster CA")
        })?;

        Ok(match verified.profile {
            pki::Profile::Coordinator(machine) => Caller::Machine(machine),
            pki::Profile::Agent(node) => Caller::Agent(node),
            pki::Profile::Operator { cn } => Caller::Operator { cn },
        })
    }

    /// The CA bundle that authorization classifies against: replicated state
    /// first (the cluster's own record of its root), the on-disk bundle as the
    /// pre-formation fallback.
    fn authorizing_ca(&self) -> Result<Vec<u8>, Status> {
        let replicated = self
            .inner
            .seam
            .read()
            .expect("admin seam lock")
            .as_ref()
            .and_then(|seam| {
                seam.consensus
                    .views()
                    .latest()
                    .state()
                    .ca
                    .as_ref()
                    .map(|ca| ca.bundle.pem().as_bytes().to_vec())
            });
        match replicated {
            Some(pem) => Ok(pem),
            None => Ok(self.tls()?.current().ca_pem().to_vec()),
        }
    }

    /// `verb` is refused to this caller (ADR 0037 §7). The message names the
    /// verb and the presented profile, because the two together are the whole
    /// diagnosis.
    fn deny(verb: &str, caller: &Caller) -> Status {
        Status::permission_denied(format!(
            "{NOT_AUTHORIZED}: {verb} is not granted to {} — see the refusal matrix in \
             ADR 0037 §7",
            caller.describe()
        ))
    }

    /// The operator-only verbs: everything that reaches beyond the caller's
    /// own seat (ADR 0037 §7).
    fn require_operator<T>(&self, request: &Request<T>, verb: &str) -> Result<Caller, Status> {
        let caller = self.caller(request)?;
        match caller {
            Caller::Operator { .. } => Ok(caller),
            _ => Err(Self::deny(verb, &caller)),
        }
    }

    /// The verbs a coordinator machine may call at all — subject, for
    /// `AddLearner`/`PromoteVoter`, to the self-scope tests in those handlers.
    /// Agents hold none of the membership surface.
    fn require_operator_or_machine<T>(
        &self,
        request: &Request<T>,
        verb: &str,
    ) -> Result<Caller, Status> {
        let caller = self.caller(request)?;
        match caller {
            Caller::Operator { .. } | Caller::Machine(_) => Ok(caller),
            Caller::Agent(_) => Err(Self::deny(verb, &caller)),
        }
    }

    /// Whether this replica is the leader, from its own metrics.
    ///
    /// Only the leader dial-back-verifies and binds: on a follower those are
    /// wasted work and a false refusal, because the consensus call that
    /// follows returns `NotLeader` and the caller retargets.
    fn is_leader(handle: &NodeHandle) -> bool {
        let summary = handle.cluster_summary();
        summary.leader == Some(summary.local_id)
    }
}

#[tonic::async_trait]
impl<C: Consensus> RaftAdminService for AdminService<C> {
    /// Admit a learner (ADR 0016 step 2, ADR 0037 §6 step 3).
    ///
    /// The order is **verify → bind → admit**, and it is load-bearing. A
    /// machine self-service admission must not create a seat before the
    /// cluster has proved the endpoint really is that installation, and must
    /// not admit before the one-identity↔one-seat binding has committed —
    /// otherwise a duplicated credential could occupy a seat for the window
    /// between the two. All three steps are idempotent, so a retried
    /// `AddLearner` (the convergence loop re-enters from the top on every
    /// restart) re-runs all three and converges — and an *exact* replay
    /// (same seat, same address, same bound identity) short-circuits to
    /// success before any of them, per the §6 contract's "before any other
    /// gate" (authz excepted; see the comment in the body).
    ///
    /// An **operator** admission runs the same three steps. Operator authority
    /// bypasses *self-scope* — an operator may admit any installation, not just
    /// its own — but never the binding invariant: §7's "no seat, including the
    /// first, is ever unbound" holds because admission itself creates the
    /// replicated binding. The one difference is where the identity comes
    /// from: a **machine** admission takes it from the mTLS session (ADR 0037
    /// §7 — never from the body, which carries no identity field at all) and
    /// requires the endpoint to present exactly that identity, while an
    /// operator holds no coordinator identity of its own, so the leader
    /// *extracts* the identity from the verified serving leaf at the
    /// advertised address and binds that.
    async fn add_learner(
        &self,
        request: Request<pb::AddLearnerRequest>,
    ) -> Result<Response<pb::AddLearnerResponse>, Status> {
        let caller = self.require_operator_or_machine(&request, "AddLearner")?;
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;

        // The §6 idempotency contract, server-side and BEFORE dial-back
        // verification: a retried AddLearner from an already-admitted machine
        // whose endpoint is momentarily unreachable (a restart, a listener
        // not yet re-bound) must be a plain no-op success, not an
        // `endpoint-unverified` refusal — the work it asks for is already
        // done, so there is nothing left to verify. Authz stays *ahead* of
        // the no-op deliberately: idempotency is a property of the verb's
        // effect, not a hole in the refusal matrix, and an agent (or an
        // unclassifiable caller) probing membership must learn nothing — not
        // even "that seat exists" — from the shape of the answer (§7).
        //
        // The no-op requires the request to be an exact replay: the node id
        // already in membership at exactly the requested address, AND the seat
        // already bound. For a machine caller the binding must be to its own
        // session identity; for an operator — who presents no coordinator
        // identity to compare — the seat must carry *some* binding at exactly
        // this address, because membership alone is not the verb's whole
        // effect any more: admission creates the binding (§7), so an admitted
        // but unbound seat is half-done work the replay must fall through and
        // finish. Any mismatch (different address, different identity, no
        // binding) falls through to the full verify → bind → admit path,
        // whose own gates produce the right refusal.
        let seat_addr = handle
            .cluster_summary()
            .members
            .iter()
            .find(|m| m.id == req.node_id)
            .map(|m| m.addr.clone());
        let already_admitted = seat_addr.as_deref() == Some(req.address.as_str());
        if already_admitted {
            let identity_settled = match &caller {
                Caller::Operator { .. } => consensus
                    .views()
                    .latest()
                    .state()
                    .machine_bindings
                    .iter()
                    .any(|(_, b)| b.raft_node_id == req.node_id && b.address == req.address),
                Caller::Machine(machine) => consensus
                    .views()
                    .latest()
                    .state()
                    .machine_bindings
                    .get(machine)
                    .is_some_and(|b| b.raft_node_id == req.node_id),
                // Filtered out by the authz gate above.
                Caller::Agent(_) => false,
            };
            if identity_settled {
                return Ok(Response::new(pb::AddLearnerResponse {}));
            }
        }

        // Leader-side only: on a follower these would be wasted work and a
        // false refusal — the consensus call below returns `NotLeader` and
        // the caller retargets. Also skipped when the seat is already in
        // membership at a *different* address: that request is terminally
        // `address-conflict` (re-admission never repoints, §6) and the
        // consensus call below says so — dialing the new endpoint first
        // could only mask that terminal refusal behind a transient
        // `endpoint-unverified` one.
        let known_at_other_addr = !already_admitted && seat_addr.is_some();
        if Self::is_leader(&handle) && !known_at_other_addr {
            let machine = match &caller {
                // The session identity is the claim; the endpoint must
                // present exactly it.
                Caller::Machine(machine) => {
                    self.verify_endpoint(&req.address, *machine, req.node_id)
                        .await?;
                    *machine
                }
                // The operator names no identity, so the verified serving
                // leaf at the advertised address *is* the identity: dial it,
                // require a coordinator-profile leaf under the cluster CA,
                // require its `ProbeCluster` to report the claimed seat, and
                // bind what it presented. Operator authority bypasses
                // self-scope, never the binding invariant (ADR 0037 §7).
                Caller::Operator { .. } => {
                    self.verify_endpoint_identity(&req.address, req.node_id)
                        .await?
                }
                // Filtered out by the authz gate above.
                Caller::Agent(_) => unreachable!("agents never pass the AddLearner authz gate"),
            };
            self.bind_machine(&consensus, machine, req.node_id, &req.address)
                .await?;
        }

        consensus
            .add_learner(req.node_id, req.address)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::AddLearnerResponse {}))
    }

    /// Promote a caught-up learner to voter (ADR 0016 step 3, ADR 0037 §6
    /// step 5).
    ///
    /// A coordinator machine may promote exactly one node id — the one its
    /// machine identity is bound to — and may never drive the `remove_node_id`
    /// half, which folds a removal into the same joint change and is therefore
    /// an operator operation (ADR 0037 §7: a machine credential "can never
    /// remove, replace, repoint, or initialize").
    async fn promote_voter(
        &self,
        request: Request<pb::PromoteVoterRequest>,
    ) -> Result<Response<pb::PromoteVoterResponse>, Status> {
        let caller = self.require_operator_or_machine(&request, "PromoteVoter")?;
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;

        if let Caller::Machine(machine) = &caller {
            if req.remove_node_id.is_some() {
                return Err(Status::permission_denied(format!(
                    "{NOT_AUTHORIZED}: {} may promote its own seat but may not remove a voter \
                     in the same joint change — removal is an operator operation \
                     (ADR 0037 §7)",
                    caller.describe()
                )));
            }
            let bound = consensus
                .views()
                .latest()
                .state()
                .machine_bindings
                .get(machine)
                .map(|b| b.raft_node_id);
            match bound {
                Some(node_id) if node_id == req.promote_node_id => {}
                Some(node_id) => {
                    return Err(Status::permission_denied(format!(
                        "{NOT_AUTHORIZED}: {} is bound to node {node_id} and may promote only \
                         that seat, not node {} (ADR 0037 §7)",
                        caller.describe(),
                        req.promote_node_id
                    )))
                }
                None => {
                    return Err(Status::permission_denied(format!(
                        "{NOT_AUTHORIZED}: {} holds no machine-identity binding, so it has no \
                         seat to promote — admission binds the seat (ADR 0037 §7)",
                        caller.describe()
                    )))
                }
            }
        }

        // chunk 06 seam: the ADR §4 confirmed-key-receipt precondition (the
        // incoming voter must have durably acknowledged the CA key *before*
        // the joint change) and the ADR §7 evidence-gated removal (fold in the
        // removal of at most one voter whose replication has been failing for
        // longer than `removal_grace`) hook in here, between the self-scope
        // check and the consensus call. Until they land, a full voter set
        // surfaces as `VOTER_SET_FULL` and the learner keeps polling.
        consensus
            .promote_voter(req.promote_node_id, req.remove_node_id)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::PromoteVoterResponse {}))
    }

    /// Remove a node from membership. Operator-only (ADR 0037 §7): removal is
    /// the one membership change no machine credential may ever reach, because
    /// it is the one that can shrink a quorum.
    async fn remove_node(
        &self,
        request: Request<pb::RemoveNodeRequest>,
    ) -> Result<Response<pb::RemoveNodeResponse>, Status> {
        self.require_operator(&request, "RemoveNode")?;
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;
        consensus
            .remove_node(req.node_id)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::RemoveNodeResponse {}))
    }

    /// Repoint an existing member's dial address (ADR 0037 §6).
    ///
    /// The operator-credential break-glass for the pet deployment whose
    /// address moved. There is deliberately no self-service form of this: a
    /// wrong `SetNodes` address can split-brain a raft, so no machine
    /// credential may repoint a voter — under the immutable model an instance
    /// whose address changed is simply a new instance.
    ///
    /// The leader commits only after dial-back verification of the **new**
    /// address, and the identity it verifies against is the one already bound
    /// to the target seat in replicated state — *not* the caller's. An
    /// operator asserting "node 3 now lives at `h:7071`" is checked against
    /// what the cluster already knows node 3 is, so a claimed node id without
    /// the matching CA-attested subject is not sufficient proof of endpoint
    /// ownership (ADR 0037 §6).
    ///
    /// A successful repoint updates **two** replicated facts: the raft
    /// membership address, and the machine binding's address (via
    /// `RebindMachineAddress`). The second is not cosmetic — the moved
    /// daemon's convergence loop re-offers `AddLearner(id, new_addr)` on
    /// every restart, and a binding still carrying the old address would
    /// refuse that forever as `machine-address-conflict`.
    async fn set_node_address(
        &self,
        request: Request<pb::SetNodeAddressRequest>,
    ) -> Result<Response<pb::SetNodeAddressResponse>, Status> {
        self.require_operator(&request, "SetNodeAddress")?;
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;

        // Idempotent, and checked before any dial: re-running the verb after
        // it succeeded must not depend on the endpoint still being reachable.
        // The no-op requires BOTH facts to already hold — the raft membership
        // address *and* the replicated machine binding's address — because a
        // crash between the two commits below leaves membership repointed and
        // the binding stale, and the re-run is exactly what repairs it.
        let summary = handle.cluster_summary();
        let member = summary.members.iter().find(|m| m.id == req.node_id);
        let bound_addr = consensus
            .views()
            .latest()
            .state()
            .machine_bindings
            .iter()
            .find(|(_, b)| b.raft_node_id == req.node_id)
            .map(|(_, b)| b.address.clone());
        match member {
            Some(m) if m.addr == req.address => {
                let binding_settled = bound_addr
                    .as_deref()
                    .map_or(true, |bound| bound == req.address);
                if binding_settled {
                    return Ok(Response::new(pb::SetNodeAddressResponse {}));
                }
            }
            Some(_) => {}
            None => {
                return Err(Status::not_found(format!(
                    "{UNKNOWN_NODE}: node {} is not in membership; set-address repoints an \
                     existing member, it never creates one (ADR 0037 §6)",
                    req.node_id
                )))
            }
        }

        if Self::is_leader(&handle) {
            let machine = consensus
                .views()
                .latest()
                .state()
                .machine_bindings
                .iter()
                .find(|(_, b)| b.raft_node_id == req.node_id)
                .map(|(machine, _)| *machine)
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "node {} carries no machine-identity binding, so there is nothing to \
                         verify the new endpoint against (ADR 0037 §7)",
                        req.node_id
                    ))
                })?;
            self.verify_endpoint(&req.address, machine, req.node_id)
                .await?;
        }

        consensus
            .set_node_address(req.node_id, req.address.clone())
            .await
            .map_err(consensus_error_to_status)?;

        // The replicated binding must follow the membership change, or the
        // moved daemon's own convergence loop is wedged forever: its next
        // AddLearner would rebind the same (machine, seat) pair at the new
        // address, which `BindMachineIdentity` rejects as
        // `machine-address-conflict` by design. This is the ONLY path that
        // proposes `RebindMachineAddress` — it runs under the operator-only
        // authz above, on the leader (a follower's membership change already
        // returned `NotLeader`), after dial-back verification — so the
        // command is unreachable from any self-service caller. If the
        // process dies between the two commits, the idempotent re-run above
        // falls through to here and completes the repair.
        if bound_addr.is_some() {
            let applied = consensus
                .propose(Command::RebindMachineAddress(RebindMachineAddress {
                    raft_node_id: req.node_id,
                    address: req.address.clone(),
                    rebound_at: Timestamp::now(),
                }))
                .await
                .map_err(consensus_error_to_status)?;
            applied.outcome.map_err(|reason| {
                Status::failed_precondition(format!(
                    "membership now dials node {} at {}, but repointing its machine binding \
                     was rejected: {reason}",
                    req.node_id, req.address
                ))
            })?;
            // Read-your-writes: `admin status --json` reads the published
            // view, and an operator checking their own repair must see it.
            await_visible(&*consensus, applied.log_index).await?;
        }
        Ok(Response::new(pb::SetNodeAddressResponse {}))
    }

    /// This coordinator's view of cluster state (ADR 0037 §9).
    ///
    /// Readable by an operator and by any coordinator machine — the
    /// convergence loop polls it while waiting for catch-up (§6 step 4) — and
    /// by no agent.
    ///
    /// `health` is the same leader-only stability-window verdict
    /// `?require=healthy` answers with, read from the same [`PhaseState`] —
    /// one verdict, two surfaces, so they cannot disagree. A follower leaves
    /// it absent rather than caching or guessing (§9: unknown health is not
    /// health).
    async fn cluster_status(
        &self,
        request: Request<pb::ClusterStatusRequest>,
    ) -> Result<Response<pb::ClusterStatusResponse>, Status> {
        self.require_operator_or_machine(&request, "ClusterStatus")?;
        let req = request.into_inner();
        let (consensus, handle) = self.formed()?;
        Self::check_cluster(&req.history_id, &handle)?;
        let mut resp = cluster_summary_to_pb(handle.cluster_summary());
        resp.bindings = machine_bindings_to_pb(&consensus);
        resp.health = match self.inner.phase.health() {
            HealthVerdict::Unknown => None,
            HealthVerdict::Sustained { live_voters } => Some(pb::ClusterHealth {
                healthy: true,
                live_voters: live_voters as u64,
            }),
            HealthVerdict::Degraded { live_voters } => Some(pb::ClusterHealth {
                healthy: false,
                live_voters: live_voters as u64,
            }),
        };
        Ok(Response::new(resp))
    }

    /// Answer a probe (ADR 0037 §3).
    ///
    /// Deliberately **not** gated on formation, and deliberately stamped with
    /// the logical `cluster_id` rather than a history id: the caller is
    /// typically a daemon with no stamp of its own yet. A mismatched
    /// `cluster_id` is answered rather than refused — the prober asked "which
    /// cluster are you?", and the honest answer to a stranger is this node's
    /// own name, which the prober compares itself.
    ///
    /// It *is* gated on the caller's profile (ADR 0037 §7): a coordinator
    /// machine probes as the first step of every convergence cycle and an
    /// operator probes to find a target, but an agent has no business on the
    /// membership plane at all. Because this verb answers before formation,
    /// the classification falls back to the on-disk CA bundle.
    async fn probe_cluster(
        &self,
        request: Request<pb::ProbeClusterRequest>,
    ) -> Result<Response<pb::ProbeClusterResponse>, Status> {
        self.require_operator_or_machine(&request, "ProbeCluster")?;
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
    /// Operator-only (ADR 0037 §7). The coordinator enrollment token is the
    /// credential that mints fresh promotable identities, and §5 classifies it
    /// as root-equivalent — so a machine credential minting one would let a
    /// bounded single-seat compromise manufacture unbounded new seats.
    ///
    /// TODO(ADR 0022/0023): the profile check here is the coarse form. When
    /// role bindings land, the narrow `mint-enroll-token` grant of ADR 0037 §5
    /// becomes one row in ADR 0023's table and refines this RPC (and its
    /// `Revoke*` siblings) below whole-operator authority.
    async fn mint_enroll_token(
        &self,
        request: Request<pb::MintEnrollTokenRequest>,
    ) -> Result<Response<pb::MintEnrollTokenResponse>, Status> {
        self.require_operator(&request, "MintEnrollToken")?;
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
    /// Operator-only, like the rest of the token surface (ADR 0037 §7).
    async fn list_enroll_tokens(
        &self,
        request: Request<pb::ListEnrollTokensRequest>,
    ) -> Result<Response<pb::ListEnrollTokensResponse>, Status> {
        self.require_operator(&request, "ListEnrollTokens")?;
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
    /// Operator-only (ADR 0037 §7).
    async fn revoke_enroll_token(
        &self,
        request: Request<pb::RevokeEnrollTokenRequest>,
    ) -> Result<Response<pb::RevokeEnrollTokenResponse>, Status> {
        self.require_operator(&request, "RevokeEnrollToken")?;
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
    /// CRL or OCSP (ADR 0037 §5). Operator-only: this is half of evicting
    /// another installation, which no machine credential may reach
    /// (ADR 0037 §7).
    async fn revoke_identity(
        &self,
        request: Request<pb::RevokeIdentityRequest>,
    ) -> Result<Response<pb::RevokeIdentityResponse>, Status> {
        self.require_operator(&request, "RevokeIdentity")?;
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
    ///
    /// Reachable by a coordinator machine as well as an operator (ADR 0037
    /// §7): leader-forwarding is coordinator-to-coordinator by construction —
    /// the follower that received the public `/enroll` request proxies it here
    /// under its own machine leaf. The enrollment token in the body, not the
    /// transport credential, is what authorizes the enrollment itself.
    async fn forward_enroll(
        &self,
        request: Request<pb::ForwardEnrollRequest>,
    ) -> Result<Response<pb::ForwardEnrollResponse>, Status> {
        self.require_operator_or_machine(&request, "ForwardEnroll")?;
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
                sans: &req.sans,
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
    ///
    /// The one verb an operator certificate may *not* call (ADR 0037 §7's
    /// matrix): there is no coordinator identity attached to an operator
    /// session for it to renew, so the profile check here is an equality
    /// rather than a floor.
    async fn renew_coordinator(
        &self,
        request: Request<pb::RenewCoordinatorRequest>,
    ) -> Result<Response<pb::RenewCoordinatorResponse>, Status> {
        let caller = self.caller(&request)?;
        let Caller::Machine(machine) = caller else {
            return Err(Self::deny("RenewCoordinator", &caller));
        };
        let (consensus, handle) = self.formed()?;
        let req = request.into_inner();
        Self::check_cluster(&req.history_id, &handle)?;

        let ctx = EnrollContext {
            consensus: consensus.as_ref(),
            data_dir: &self.inner.data_dir,
            formed: true,
        };
        let issued = enroll::renew_coordinator(&ctx, machine, req.csr_pem.as_bytes(), &req.sans)
            .await
            .map_err(enroll_error_to_status)?;

        Ok(Response::new(pb::RenewCoordinatorResponse {
            cert_pem: pem_string(issued.cert_pem)?,
            ca_pem: pem_string(issued.ca_pem)?,
        }))
    }
}

impl<C: Consensus> AdminService<C> {
    /// Dial-back verification of an advertised endpoint (ADR 0037 §6 step 3).
    ///
    /// Two independent facts have to hold before a seat is created, and
    /// neither is something the requester can assert:
    ///
    /// 1. the **serving** certificate at `addr` presents `machine` — so the
    ///    endpoint really belongs to this installation, not merely to someone
    ///    holding its client certificate; and
    /// 2. `ProbeCluster` at `addr` reports `node_id` — so the endpoint really
    ///    is the stamped data directory claiming the seat.
    ///
    /// Together these are what bound a stolen machine certificate: it "cannot
    /// even occupy [its seat] without the matching stamped data directory"
    /// (§7). A claimed node id without the matching CA-attested subject is not
    /// proof of endpoint ownership, and neither is the reverse.
    ///
    /// The dial presents this daemon's own leaf and trusts the cluster CA, so
    /// it exercises the same mutual authentication the raft listener requires.
    async fn verify_endpoint(
        &self,
        addr: &str,
        machine: MachineId,
        node_id: CoordinatorId,
    ) -> Result<(), Status> {
        let served = self.serving_machine(addr).await?;
        if served != machine {
            return Err(endpoint_unverified_status(format_args!(
                "{addr} serves machine identity {served}, not the claimed {machine}"
            )));
        }
        self.probe_endpoint_node(addr, node_id).await
    }

    /// The operator-admission variant of [`verify_endpoint`] (ADR 0037 §7):
    /// the caller names no machine identity, so the verified serving leaf at
    /// `addr` supplies it. Fact 2 (the stamped seat) is checked exactly as in
    /// the machine path; fact 1 becomes *extraction* — the leaf must classify
    /// as a coordinator profile under the cluster CA, and whatever identity it
    /// presents is what the admission then binds.
    async fn verify_endpoint_identity(
        &self,
        addr: &str,
        node_id: CoordinatorId,
    ) -> Result<MachineId, Status> {
        let served = self.serving_machine(addr).await?;
        self.probe_endpoint_node(addr, node_id).await?;
        Ok(served)
    }

    /// Read the serving leaf at `addr` and require it to classify as a
    /// coordinator machine identity (ADR 0037 §4 profile convention). The
    /// mTLS handshake inside [`coppice_tls::read_serving_leaf`] has already
    /// validated the chain against the cluster CA; this reads the subject it
    /// attested — `OU=coppice-coordinator` with a machine-id CN — and refuses
    /// anything else as unverified.
    async fn serving_machine(&self, addr: &str) -> Result<MachineId, Status> {
        let tls = self.tls()?;
        let subject = coppice_tls::read_serving_leaf(&tls, addr)
            .await
            .map_err(|e| {
                endpoint_unverified_status(format_args!(
                    "could not read the serving certificate at {addr}: {e}"
                ))
            })?;
        if subject.org_unit.as_deref() != Some(pki::COORDINATOR_OU) {
            return Err(endpoint_unverified_status(format_args!(
                "{addr} does not serve a coordinator-profile certificate (OU {:?})",
                subject.org_unit
            )));
        }
        subject
            .common_name
            .as_deref()
            .and_then(|cn| cn.parse::<MachineId>().ok())
            .ok_or_else(|| {
                endpoint_unverified_status(format_args!(
                    "{addr} serves a coordinator-profile certificate whose CN {:?} is not a \
                     machine identity",
                    subject.common_name
                ))
            })
    }

    /// Dial-back fact 2: `ProbeCluster` at `addr` must report `node_id`, so
    /// the endpoint really is the stamped data directory claiming the seat.
    async fn probe_endpoint_node(&self, addr: &str, node_id: CoordinatorId) -> Result<(), Status> {
        let tls = self.tls()?;
        let mut client = admin_channel_from_store(addr, &tls).await.map_err(|e| {
            endpoint_unverified_status(format_args!("dialing {addr} to probe it: {e:#}"))
        })?;
        let probe = client
            .probe_cluster(pb::ProbeClusterRequest {
                cluster_id: self.inner.phase.probe().cluster_id,
            })
            .await
            .map_err(|s| {
                endpoint_unverified_status(format_args!("probing {addr}: {}", s.message()))
            })?
            .into_inner();
        if probe.node_id != Some(node_id) {
            return Err(endpoint_unverified_status(format_args!(
                "{addr} reports raft node {:?}, not the claimed {node_id}",
                probe.node_id
            )));
        }
        Ok(())
    }

    /// Commit the machine-identity ↔ seat binding for an admission
    /// (ADR 0037 §7).
    ///
    /// The replicated state machine holds the one-identity↔one-seat invariant;
    /// this maps its two refusals onto statuses whose text is the whole
    /// diagnosis, because that text is what `/readyz` surfaces as
    /// `last_admission_refusal` and what an operator will read when a second
    /// installation restored from a snapshot of the first starts fighting for
    /// its seat.
    async fn bind_machine(
        &self,
        consensus: &Arc<C>,
        machine: MachineId,
        raft_node_id: CoordinatorId,
        address: &str,
    ) -> Result<(), Status> {
        let applied = consensus
            .propose(Command::BindMachineIdentity(BindMachineIdentity {
                machine,
                raft_node_id,
                address: address.to_string(),
                bound_at: Timestamp::now(),
            }))
            .await
            .map_err(consensus_error_to_status)?;
        match applied.outcome {
            // Includes the exact-replay no-op: a retried AddLearner rebinds
            // the same triple and applies cleanly (ADR 0037 §6 idempotency).
            Ok(_) => {
                // Read-your-writes: `ClusterStatus` joins bindings from the
                // published view, and a caller that just admitted a seat must
                // see its binding there (ADR 0037 §9).
                await_visible(&**consensus, applied.log_index).await?;
                Ok(())
            }
            Err(RejectionReason::MachineIdentityConflict { .. }) => {
                Err(machine_identity_conflict_status(machine, raft_node_id))
            }
            Err(RejectionReason::MachineAddressConflict { .. }) => {
                Err(machine_address_conflict_status(machine, raft_node_id))
            }
            Err(other) => Err(Status::failed_precondition(format!(
                "machine-identity binding rejected: {other}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Refusal formatting: one place per marker
// ---------------------------------------------------------------------------
//
// Each membership refusal a handler constructs goes through exactly one of
// these (or through `consensus_error_to_status` below), so the marker prefix
// and its human tail have a single author. The convergence loop's classify
// tests build their fixtures through the same functions — the point is that a
// reworded refusal automatically rewords the tests too, and only a deliberate
// marker change (a wire-visible change) can break classification.

/// Dial-back verification failed (ADR 0037 §6 step 3): the advertised
/// endpoint could not be read, presented the wrong identity, or reported the
/// wrong node. Routinely transient during a fleet boot — the leader may dial
/// back before the joiner's listener binds — so the convergence loop keeps
/// its fast retry cadence while still surfacing the message.
pub(crate) fn endpoint_unverified_status(detail: impl std::fmt::Display) -> Status {
    Status::failed_precondition(format!("{ENDPOINT_UNVERIFIED}: {detail} (ADR 0037 §6)"))
}

/// The ADR 0016 cross-history refusal: the request is stamped for a raft
/// history that is not this node's. Two histories can never merge, so the
/// refusal is terminal — the convergence loop's `classify` routes it to the
/// hard-backoff `Refused` path rather than the 300ms retry.
pub(crate) fn history_conflict_status(incoming: &[u8], stamped: &[u8; 16]) -> Status {
    Status::failed_precondition(format!(
        "{HISTORY_CONFLICT}: request is from history {}, this node is stamped for history {} — \
         cross-history admin contact refused; this is a wrong data volume or a \
         wiped-and-re-formed cluster, not something waiting fixes (ADR 0016)",
        hex(incoming),
        hex(stamped),
    ))
}

/// The one-identity↔one-seat invariant refused this admission (ADR 0037 §7).
pub(crate) fn machine_identity_conflict_status(
    machine: MachineId,
    raft_node_id: CoordinatorId,
) -> Status {
    Status::failed_precondition(format!(
        "{MACHINE_IDENTITY_CONFLICT}: machine {machine} cannot take raft node \
         {raft_node_id} — one machine identity binds to at most one seat, ever, and \
         one seat to at most one identity. This is a duplicated or stolen \
         coordinator credential, or a misissuance: a replacement installation starts \
         with fresh state and mints a fresh identity (ADR 0037 §7)"
    ))
}

/// The same `(machine, seat)` pair re-offered at a different address
/// (ADR 0037 §7): re-admission never repoints.
pub(crate) fn machine_address_conflict_status(
    machine: MachineId,
    raft_node_id: CoordinatorId,
) -> Status {
    Status::failed_precondition(format!(
        "{MACHINE_ADDRESS_CONFLICT}: machine {machine} is already bound to raft node \
         {raft_node_id} at a different address, and re-admission never repoints a \
         seat. An instance whose address changed is a new instance; a genuine move \
         is the operator-only `admin set-address` (ADR 0037 §6/§7)"
    ))
}

/// The replicated machine-identity bindings (ADR 0037 §7) in wire form,
/// ascending by machine id so the listing is stable across calls and replicas.
fn machine_bindings_to_pb<C: Consensus>(consensus: &Arc<C>) -> Vec<pb::MachineBinding> {
    consensus
        .views()
        .latest()
        .state()
        .machine_bindings
        .iter()
        .map(|(machine, binding)| pb::MachineBinding {
            machine_id: machine.to_string(),
            node_id: binding.raft_node_id,
            address: binding.address.clone(),
            bound_at_us: binding.bound_at.as_micros(),
        })
        .collect()
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

// `pub(crate)` rather than private: the convergence loop's classify unit tests
// build their fixture statuses through this exact function, so the wire text
// the server sends and the text the classifier is tested against can never
// drift apart.
pub(crate) fn consensus_error_to_status(err: ConsensusError) -> Status {
    match err {
        ConsensusError::NotLeader { leader: Some(id) } => Status::failed_precondition(format!(
            "not the leader; current leader is node {id} — retarget the request"
        )),
        ConsensusError::NotLeader { leader: None } => Status::failed_precondition(
            "not the leader; no leader currently known (election in progress) — retry",
        ),
        ConsensusError::LearnerNotCaughtUp { lag } => Status::failed_precondition(format!(
            "{LEARNER_BEHIND}: learner is {lag} entries behind; retry after catch-up (ADR 0016)"
        )),
        // Terminal caller errors (ADR 0037 §6). Their messages deliberately
        // avoid the word "behind", so `is_learner_behind` cannot mistake one
        // for a catch-up wait and poll a hopeless request forever.
        ConsensusError::UnknownNode { node } => Status::not_found(format!(
            "{UNKNOWN_NODE}: node {node} is not in this cluster's membership (ADR 0037 §6)"
        )),
        ConsensusError::AddressConflict {
            node,
            current,
            requested,
        } => Status::failed_precondition(format!(
            "{ADDRESS_CONFLICT}: node {node} is already in membership at {current}, not \
             {requested} — there is no silent repointing, and an instance whose address changed \
             is a new instance (ADR 0037 §6)"
        )),
        // Not terminal: the learner stays a caught-up learner and re-offers
        // itself, becoming either the `new_node_id` of a pending ReplaceVoter
        // or the beneficiary of an evidence-gated removal (ADR 0037 §7). The
        // marker is distinct from the catch-up one because the *reason* to
        // keep polling is different, and status output says which.
        ConsensusError::VoterSetFull {
            node,
            voters,
            cluster_size,
        } => Status::failed_precondition(format!(
            "{VOTER_SET_FULL}: cannot promote node {node}; the cluster already has {voters} of \
             {cluster_size} voters. Remaining a caught-up learner and re-offering is the \
             expected behaviour (ADR 0037 §7)"
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
///
/// `bindings` is left empty here and filled by the handler from replicated
/// state ([`machine_bindings_to_pb`]): a [`ClusterSummary`] is raft's view of
/// membership, and the machine-identity bindings are a *replicated fact* the
/// state machine owns (ADR 0037 §7), so joining them belongs one level up.
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
        bindings: Vec::new(),
        // Filled by the handler from `PhaseState`, like `bindings`: the
        // redundancy verdict is the daemon's (leader-only) observation, not
        // something a raw membership summary carries.
        health: None,
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

/// Dial the admin surface of `target` over mTLS using a [`TlsStore`]'s
/// *current* material, so a leaf rotated since the last dial is picked up
/// without a restart (ADR 0037 §4).
///
/// This is the seam every in-daemon caller uses — the convergence loop
/// (§6), dial-back endpoint verification, the formation probe guard — because
/// all of them must present the daemon's own machine certificate rather than
/// PEM re-read from the config's paths. [`admin_channel`] stays for the CLI,
/// which has files and no store.
pub(crate) async fn admin_channel_from_store(
    target: &str,
    tls: &Arc<TlsStore>,
) -> Result<Client<Channel>> {
    let material = tls.current();
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

/// Repoint an existing member's dial address (ADR 0037 §6).
///
/// Operator-credential break-glass; the leader dial-back-verifies the new
/// address against the machine identity already bound to that seat before it
/// commits anything.
pub async fn set_node_address(
    client: &mut Client<Channel>,
    history_id: [u8; 16],
    node_id: CoordinatorId,
    addr: String,
) -> Result<()> {
    client
        .set_node_address(pb::SetNodeAddressRequest {
            history_id: history_id.to_vec(),
            node_id,
            address: addr,
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

/// Where a `ClusterStatus` answer warrants one leader re-dial, if anywhere
/// (ADR 0037 §9).
///
/// `admin status` presents itself as the authoritative alternative to a
/// follower's `?require=healthy` → `health_unknown`, so a follower's answer —
/// recognizable because it carries no health verdict — is retargeted at the
/// leader it names, provided that leader's address is resolvable from the
/// follower's own membership view. `None` means render what we have: the
/// answer already is the leader's (it carries health, or names itself), no
/// leader is known, or the named leader has no membership address to dial.
/// Health is never fabricated in any of those cases.
pub fn leader_redial_target(status: &pb::ClusterStatusResponse) -> Option<String> {
    if status.health.is_some() {
        return None;
    }
    let leader = status.leader_node_id?;
    if leader == status.local_node_id {
        return None;
    }
    status
        .membership
        .as_ref()?
        .members
        .iter()
        .find(|m| m.node_id == leader)
        .map(|m| m.address.clone())
}

/// Fetch cluster status through `client`, re-dialing the leader once when the
/// first answer came from a follower ([`leader_redial_target`]).
///
/// Best-effort on the second hop: the leader may have died between the
/// follower's answer and our dial, and the follower's answer is still a true
/// document — membership and bindings are replicated facts — so it is
/// rendered with `health` null rather than failing the verb or faking a
/// verdict (ADR 0037 §9: unknown health is not health).
pub async fn cluster_status_resolving_leader(
    client: &mut Client<Channel>,
    history_id: [u8; 16],
    ca_pem: &[u8],
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<pb::ClusterStatusResponse> {
    let first = cluster_status(client, history_id).await?;
    let Some(leader_addr) = leader_redial_target(&first) else {
        return Ok(first);
    };
    let leader_answer = async {
        let mut leader_client = admin_channel(&leader_addr, ca_pem, cert_pem, key_pem).await?;
        cluster_status(&mut leader_client, history_id).await
    }
    .await;
    match leader_answer {
        Ok(status) => Ok(status),
        Err(e) => {
            tracing::debug!(
                leader = %leader_addr,
                error = %format!("{e:#}"),
                "admin status: leader re-dial failed; rendering the follower's answer"
            );
            Ok(first)
        }
    }
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

    // `local-status` rides the local admin socket too (ADR 0037 §3/§9): it
    // serves this daemon's readiness document, which is the only status a
    // parked or formation-failed daemon can answer — those phases have no
    // formed cluster behind the network verbs, and possibly no TLS material
    // to dial anything with. `status`, by contrast, is *always* the
    // cluster-wide document over the network, with or without `--target`
    // (ADR 0037 §9's one stable schema), and never touches the socket.
    if let AdminVerb::LocalStatus { json } = &args.verb {
        return crate::localadmin::run_status(&args.config, *json).await;
    }

    let resolved = config::load(&args.config)
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
        AdminVerb::SetAddress { node_id, addr } => {
            set_node_address(&mut client, history_id, node_id, addr.clone()).await?;
            println!("node {node_id} now advertises {addr}");
        }
        AdminVerb::Status { json } => {
            let status =
                cluster_status_resolving_leader(&mut client, history_id, &ca, &cert, &key).await?;
            if json {
                println!("{}", render_status_json(&status));
            } else {
                print!("{}", render_status(&status));
            }
        }
        // Dispatched above, before any network client was built.
        AdminVerb::IssueOperatorCert { .. } | AdminVerb::LocalStatus { .. } => {
            unreachable!("handled on the local socket")
        }
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

/// Render a `ClusterStatus` response as the documented `admin status --json`
/// object (ADR 0037 §9).
///
/// This is a **scripting contract**, not debug output: field names, nesting,
/// and types are stable, absent values are explicit `null` rather than omitted
/// keys, and the unit test below pins the shape. The human table stays the
/// default because a person reading membership wants a table.
///
/// Two joins happen here rather than on the wire. `role` is derived by testing
/// each member against the voter config, and `machine_id` is joined from
/// `bindings` — an unbound member (there should be none once formation and
/// admission both bind, ADR 0037 §7) reports `null` rather than being dropped,
/// because "a seat with no binding" is exactly what an operator needs to see.
/// `health` carries the responder's redundancy verdict — `null` values when
/// the responder is not the leader, because only the leader can answer (§9).
/// There is deliberately **no** `superseded` field: replacement is an explicit
/// operation, never an inference (ADR 0037 §7).
fn render_status_json(s: &pb::ClusterStatusResponse) -> String {
    let voters: std::collections::BTreeSet<u64> = s
        .membership
        .as_ref()
        .and_then(|m| m.configs.first())
        .map(|c| c.voters.iter().copied().collect())
        .unwrap_or_default();
    let machine_of: std::collections::BTreeMap<u64, &str> = s
        .bindings
        .iter()
        .map(|b| (b.node_id, b.machine_id.as_str()))
        .collect();

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
                        "machine_id": machine_of.get(&member.node_id),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    // Empty on a follower: replication progress is the leader's observation
    // and nobody else's, and a cached follower view would be a lie (§9).
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

    let bindings: Vec<serde_json::Value> = s
        .bindings
        .iter()
        .map(|b| {
            serde_json::json!({
                "machine_id": b.machine_id,
                "node_id": b.node_id,
                "address": b.address,
                "bound_at_us": b.bound_at_us,
            })
        })
        .collect();

    // The health object is always present with both keys, `null` when the
    // responder could not answer (a follower): scripts branch on the value,
    // never on a key's existence, and a `null` verdict is honestly different
    // from a `false` one (ADR 0037 §9 — unknown health is not health).
    let health = serde_json::json!({
        "healthy": s.health.as_ref().map(|h| h.healthy),
        "live_voters": s.health.as_ref().map(|h| h.live_voters),
    });

    let value = serde_json::json!({
        "node_id": s.local_node_id,
        "leader": s.leader_node_id,
        "is_leader": s.leader_node_id == Some(s.local_node_id),
        "term": s.term,
        "applied_index": s.last_applied_index,
        "committed_index": s.known_committed_index,
        "members": members,
        "replication": replication,
        "bindings": bindings,
        "health": health,
    });
    serde_json::to_string_pretty(&value).expect("a json! value always serializes")
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

    /// The three new consensus refusals carry their stable markers and their
    /// documented codes — the convergence loop branches on exactly this.
    #[test]
    fn membership_refusals_carry_their_machine_readable_markers() {
        let unknown = consensus_error_to_status(ConsensusError::UnknownNode { node: 9 });
        assert_eq!(unknown.code(), Code::NotFound);
        assert!(unknown.message().starts_with(UNKNOWN_NODE));

        let conflict = consensus_error_to_status(ConsensusError::AddressConflict {
            node: 9,
            current: "a:1".into(),
            requested: "b:2".into(),
        });
        assert_eq!(conflict.code(), Code::FailedPrecondition);
        assert!(conflict.message().starts_with(ADDRESS_CONFLICT));

        let full = consensus_error_to_status(ConsensusError::VoterSetFull {
            node: 9,
            voters: 3,
            cluster_size: 3,
        });
        assert_eq!(full.code(), Code::FailedPrecondition);
        assert!(full.message().starts_with(VOTER_SET_FULL));
    }

    /// None of the terminal-or-voter-full refusals may look like a learner
    /// still catching up: `is_learner_behind` keys on the substring "behind",
    /// and a false positive there polls a hopeless request until the deadline.
    #[test]
    fn no_new_refusal_is_mistaken_for_a_catch_up_wait() {
        for status in [
            consensus_error_to_status(ConsensusError::UnknownNode { node: 1 }),
            consensus_error_to_status(ConsensusError::AddressConflict {
                node: 1,
                current: "a:1".into(),
                requested: "b:2".into(),
            }),
            consensus_error_to_status(ConsensusError::VoterSetFull {
                node: 1,
                voters: 3,
                cluster_size: 3,
            }),
        ] {
            assert!(
                !is_learner_behind(&status),
                "must not read as a catch-up wait: {}",
                status.message()
            );
        }
    }

    fn status_fixture() -> pb::ClusterStatusResponse {
        pb::ClusterStatusResponse {
            local_node_id: 1,
            leader_node_id: Some(1),
            term: 5,
            last_applied_index: 100,
            known_committed_index: 100,
            membership: Some(pb::Membership {
                configs: vec![pb::VoterConfig { voters: vec![1, 2] }],
                members: vec![
                    pb::RaftMember {
                        node_id: 1,
                        address: "c1:7071".into(),
                    },
                    pb::RaftMember {
                        node_id: 2,
                        address: "c2:7071".into(),
                    },
                    pb::RaftMember {
                        node_id: 3,
                        address: "c3:7071".into(),
                    },
                ],
            }),
            replication: vec![pb::ReplicationProgress {
                node_id: 3,
                matched_index: 91,
            }],
            bindings: vec![pb::MachineBinding {
                machine_id: "machine-0f9b2c1e-0000-4000-8000-000000000001".into(),
                node_id: 1,
                address: "c1:7071".into(),
                bound_at_us: 1_700_000_000_000_000,
            }],
            health: Some(pb::ClusterHealth {
                healthy: true,
                live_voters: 2,
            }),
        }
    }

    /// `admin status --json` is a scripting contract (ADR 0037 §9): this pins
    /// the whole shape, including the joins (`role`, `machine_id`, `lag`) and
    /// the explicit `null` for an unbound member.
    #[test]
    fn status_json_shape_is_stable() {
        let rendered = render_status_json(&status_fixture());
        let v: serde_json::Value = serde_json::from_str(&rendered).expect("valid JSON");

        assert_eq!(v["node_id"], 1);
        assert_eq!(v["leader"], 1);
        assert_eq!(v["is_leader"], true);
        assert_eq!(v["term"], 5);
        assert_eq!(v["applied_index"], 100);
        assert_eq!(v["committed_index"], 100);

        let members = v["members"].as_array().expect("members is an array");
        assert_eq!(members.len(), 3);
        assert_eq!(members[0]["node_id"], 1);
        assert_eq!(members[0]["addr"], "c1:7071");
        assert_eq!(members[0]["role"], "voter");
        assert_eq!(
            members[0]["machine_id"],
            "machine-0f9b2c1e-0000-4000-8000-000000000001"
        );
        assert_eq!(members[2]["role"], "learner");
        // An unbound seat is reported as null, never omitted: the key is part
        // of the contract and its absence is the interesting signal.
        assert!(members[2]["machine_id"].is_null());
        assert!(members[2].as_object().unwrap().contains_key("machine_id"));

        let replication = v["replication"].as_array().expect("replication array");
        assert_eq!(replication.len(), 1);
        assert_eq!(replication[0]["node_id"], 3);
        assert_eq!(replication[0]["matched_index"], 91);
        assert_eq!(replication[0]["lag"], 9);

        let bindings = v["bindings"].as_array().expect("bindings array");
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0]["node_id"], 1);
        assert_eq!(bindings[0]["address"], "c1:7071");
        assert_eq!(bindings[0]["bound_at_us"], 1_700_000_000_000_000i64);

        // The leader's redundancy verdict (ADR 0037 §9), the same one
        // `?require=healthy` gates on.
        assert_eq!(v["health"]["healthy"], true);
        assert_eq!(v["health"]["live_voters"], 2);

        // Explicitly rejected by ADR 0037 §7 — replacement is never inferred.
        assert!(v.get("superseded").is_none());
    }

    /// A follower reports no replication, `is_leader: false`, and a `null`
    /// health verdict — never a cached or fabricated one — with the keys
    /// still present so a script need not branch on their existence.
    #[test]
    fn status_json_on_a_follower_has_empty_replication_and_unknown_health() {
        let mut fixture = status_fixture();
        fixture.local_node_id = 2;
        fixture.replication.clear();
        fixture.health = None;
        let v: serde_json::Value =
            serde_json::from_str(&render_status_json(&fixture)).expect("valid JSON");
        assert_eq!(v["is_leader"], false);
        assert_eq!(v["replication"].as_array().expect("array").len(), 0);
        let health = v["health"].as_object().expect("health object");
        assert!(health["healthy"].is_null());
        assert!(health["live_voters"].is_null());
    }

    /// No leader at all: `leader` is `null` and `is_leader` is false, rather
    /// than the key vanishing.
    #[test]
    fn status_json_reports_an_unknown_leader_as_null() {
        let mut fixture = status_fixture();
        fixture.leader_node_id = None;
        let v: serde_json::Value =
            serde_json::from_str(&render_status_json(&fixture)).expect("valid JSON");
        assert!(v["leader"].is_null());
        assert_eq!(v["is_leader"], false);
    }

    /// The retarget decision (ADR 0037 §9): a follower's answer — no health,
    /// a leader named elsewhere — is re-dialed at the leader's membership
    /// address, and nothing else is.
    #[test]
    fn a_followers_answer_names_the_leader_to_redial() {
        let mut fixture = status_fixture();
        fixture.local_node_id = 2;
        fixture.health = None;
        assert_eq!(
            leader_redial_target(&fixture).as_deref(),
            Some("c1:7071"),
            "a healthless follower answer retargets at the leader's membership address"
        );
    }

    #[test]
    fn a_leaders_answer_is_never_redialed() {
        // Names itself and carries health: already authoritative.
        let fixture = status_fixture();
        assert_eq!(leader_redial_target(&fixture), None);

        // Even a leader whose health verdict is (still) absent is not
        // re-dialed — dialing yourself again cannot add information.
        let mut fixture = status_fixture();
        fixture.health = None;
        assert_eq!(leader_redial_target(&fixture), None);

        // And an answer that carries health is authoritative regardless of
        // who it came from — health is leader-only by construction.
        let mut fixture = status_fixture();
        fixture.local_node_id = 2;
        assert_eq!(leader_redial_target(&fixture), None);
    }

    #[test]
    fn an_unknown_or_unresolvable_leader_is_not_redialed() {
        // No leader at all: nothing to dial, render what we have.
        let mut fixture = status_fixture();
        fixture.local_node_id = 2;
        fixture.leader_node_id = None;
        fixture.health = None;
        assert_eq!(leader_redial_target(&fixture), None);

        // A named leader with no address in the follower's membership view:
        // unresolvable, so the follower's answer stands (health null, never
        // fabricated).
        let mut fixture = status_fixture();
        fixture.local_node_id = 2;
        fixture.leader_node_id = Some(99);
        fixture.health = None;
        assert_eq!(leader_redial_target(&fixture), None);
    }

    /// The refusal message names the verb and the presented profile, because
    /// those two together are the whole diagnosis (ADR 0037 §7).
    #[test]
    fn a_denial_names_the_verb_the_profile_and_the_adr() {
        let caller = Caller::Agent(NodeId::new());
        let status =
            AdminService::<coppice_consensus::OpenraftConsensus>::deny("RemoveNode", &caller);
        assert_eq!(status.code(), Code::PermissionDenied);
        let msg = status.message();
        assert!(msg.starts_with(NOT_AUTHORIZED), "{msg}");
        assert!(msg.contains("RemoveNode"), "{msg}");
        assert!(msg.contains("agent"), "{msg}");
        assert!(msg.contains("ADR 0037 §7"), "{msg}");
        // A denial is terminal; it must never read as a catch-up wait.
        assert!(!is_learner_behind(&status));
    }
}
