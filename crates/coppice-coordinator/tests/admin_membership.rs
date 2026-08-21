//! The membership admin surface's authorization and idempotency contracts
//! (ADR 0037 §6/§7).
//!
//! Three groups of claims, all made against a *formed* cluster's real mTLS
//! admin listener:
//!
//! 1. **The §7 refusal matrix**: what each certificate profile — agent,
//!    coordinator machine, operator, and an unclassifiable leaf — may and may
//!    not do, asserted per verb via the stable refusal markers in
//!    `coppice_coordinator::admin`, never via prose.
//! 2. **The §6 idempotency contract**: every no-op case is an explicit
//!    service-layer test, because the contract deliberately amends the old
//!    semantics and the unit tests in the consensus adapter cannot prove the
//!    *service* orders its gates correctly (e.g. the already-voter no-op must
//!    fire before the lag gate).
//! 3. **`set-address` verification paths** (§6): the operator-only break-glass
//!    commits only after dial-back verification of the new endpoint, and a
//!    successful repoint carries the machine binding with it so the moved
//!    member's own convergence replay no-ops instead of wedging.
//!
//! The tests mint leaves of their choosing under the *cluster's* CA: the CA
//! key lives on the forming voter's disk (ADR 0037 §4), which is exactly the
//! custody model — a test holding the voter's data directory holds what the
//! voter holds, no more.

mod common;

use std::time::Duration;

use coppice_coordinator::admin::{
    self, has_marker, ADDRESS_CONFLICT, ENDPOINT_UNVERIFIED, LEARNER_BEHIND, NOT_AUTHORIZED,
    UNKNOWN_NODE,
};
use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::id::{ClusterId, MachineId, NodeId};
use coppice_proto::pb::core::v1 as pbcore;
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::pki;
use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose};
use tonic::{Code, Status};

use common::{free_port, poll, Ca, Daemon, Node};

type AdminClient = coppice_net::admin::Client<tonic::transport::Channel>;

/// Per-binary tracing, mirroring the other §6 suites: `run_with` installs no
/// subscriber (the binary's `run` does), so the harness supplies one.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Unwrap the refusal a call must produce, panicking with the success case.
fn refusal<T: std::fmt::Debug>(result: Result<tonic::Response<T>, Status>, what: &str) -> Status {
    match result {
        Ok(resp) => panic!(
            "{what} must be refused, but succeeded: {:?}",
            resp.into_inner()
        ),
        Err(status) => status,
    }
}

/// Assert `status` carries `marker` as its machine-readable prefix.
fn assert_marker(status: &Status, marker: &str, what: &str) {
    assert!(
        has_marker(status.message(), marker),
        "{what}: expected the {marker:?} marker, got ({:?}) {:?}",
        status.code(),
        status.message()
    );
}

/// Assert a §7 authorization denial that names the verb it refused: the verb
/// plus the presented profile are the whole diagnosis an operator gets.
fn assert_denied(status: &Status, verb: &str) {
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "{verb}: a matrix refusal is PERMISSION_DENIED, got ({:?}) {:?}",
        status.code(),
        status.message()
    );
    assert_marker(status, NOT_AUTHORIZED, verb);
    assert!(
        status.message().contains(verb),
        "{verb}: the denial must name the refused verb, got {:?}",
        status.message()
    );
}

/// Assert a §7 *self-scope* denial: a caller with a legitimate profile grant
/// reaching beyond its own seat. These refusals carry the `not-authorized`
/// marker and PERMISSION_DENIED but explain the binding situation rather than
/// naming the verb — the verb is the one the caller just issued, and the
/// useful diagnosis is which seat (if any) it *is* allowed to drive.
fn assert_out_of_scope(status: &Status, what: &str) {
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "{what}: a self-scope refusal is PERMISSION_DENIED, got ({:?}) {:?}",
        status.code(),
        status.message()
    );
    assert_marker(status, NOT_AUTHORIZED, what);
}

/// Dial `daemon`'s admin surface presenting the given leaf.
async fn dial(daemon: &Daemon, ca: &[u8], cert: &[u8], key: &[u8]) -> AdminClient {
    admin::admin_channel(&daemon.raft_target(), ca, cert, key)
        .await
        .expect("dial the admin surface")
}

/// Dial as the operator credential `init` minted.
async fn operator_client(daemon: &Daemon, operator: &OperatorPem) -> AdminClient {
    dial(
        daemon,
        operator.ca_pem.as_bytes(),
        operator.cert_pem.as_bytes(),
        operator
            .key_pem
            .as_ref()
            .expect("no CSR was supplied, so the cluster minted the keypair")
            .as_bytes(),
    )
    .await
}

/// A signer over the cluster CA, loaded from the forming voter's disk — the
/// same custody path the leader's own signing verbs take (ADR 0037 §4).
fn cluster_signer(daemon: &Daemon) -> pki::CaSigner {
    let (ca_pem, _, _) = daemon.tls_material();
    let key = pki::load_ca_key(&daemon.data_dir(), &ca_pem)
        .expect("the forming voter's data dir holds the CA key");
    pki::CaSigner::load(&ca_pem, &key).expect("load the cluster CA signer")
}

/// A leaf that chains to the cluster CA but classifies as no profile at all:
/// no OU, and a CN that is not a node id. The TLS acceptor admits it (the
/// chain is valid), and the §7 classifier must then reject it as
/// *unauthenticated* — an unknown caller, not a known caller denied a verb.
fn unclassifiable_leaf(daemon: &Daemon) -> (Vec<u8>, Vec<u8>) {
    let (ca_pem, _, _) = daemon.tls_material();
    let key = pki::load_ca_key(&daemon.data_dir(), &ca_pem).expect("load the CA key");
    let ca_key = KeyPair::from_pem(std::str::from_utf8(&key).expect("CA key is UTF-8"))
        .expect("parse the CA key");
    let issuer = CertificateParams::from_ca_cert_pem(
        std::str::from_utf8(&ca_pem).expect("CA cert is UTF-8"),
    )
    .expect("parse the CA cert")
    .self_signed(&ca_key)
    .expect("reconstruct the issuer");

    let leaf_key = KeyPair::generate().expect("generate leaf key");
    let mut params = CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
        .expect("leaf params");
    params
        .distinguished_name
        .push(DnType::CommonName, "definitely-not-a-node-id");
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    let cert = params
        .signed_by(&leaf_key, &issuer, &ca_key)
        .expect("sign the unclassifiable leaf");
    (
        cert.pem().into_bytes(),
        leaf_key.serialize_pem().into_bytes(),
    )
}

/// One formed single-voter cluster plus everything a test needs to speak to
/// it: the operator credential, the stamped history id, and the forming
/// voter's own seat (node id + advertised address).
struct Formed {
    daemon: Daemon,
    operator: OperatorPem,
    history_id: Vec<u8>,
    seat: u64,
    seat_addr: String,
}

/// Form a cluster the only way one can be formed: park, then `init` over the
/// local admin socket (ADR 0037 §3).
async fn form(daemon: &mut Daemon) -> OperatorPem {
    daemon.start();
    daemon.await_phase("waiting").await;
    let reply = daemon
        .admin(AdminCall::Init {
            policy: None,
            operator_csr: None,
            operator_cn: None,
        })
        .await;
    let AdminReply::Formed { operator, .. } = reply else {
        panic!("expected the cluster to form, got {reply:?}");
    };
    daemon.await_phase("voter").await;
    operator
}

async fn formed_single_voter(ca: &Ca) -> Formed {
    let mut daemon = Daemon::new_certless(ClusterId::new(), ca);
    let operator = form(&mut daemon).await;

    let mut client = operator_client(&daemon, &operator).await;
    let probe = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe the formed cluster")
        .into_inner();
    let seat = probe.node_id.expect("a formed voter reports its seat");
    let voter = probe
        .voters
        .iter()
        .find(|v| v.node_id == seat)
        .expect("the forming voter is in its own voter set");
    Formed {
        daemon,
        operator,
        history_id: probe.history_id,
        seat,
        seat_addr: voter.address.clone(),
    }
}

// ---------------------------------------------------------------------------
// 1. The §7 refusal matrix
// ---------------------------------------------------------------------------

/// A membership verb: the columns of the §7 matrix.
#[derive(Clone, Copy, Debug)]
enum Verb {
    ProbeCluster,
    ClusterStatus,
    AddLearner,
    PromoteVoter,
    RemoveNode,
    ReplaceVoter,
    TransferCaKey,
    SetNodeAddress,
}

impl Verb {
    /// The name a matrix denial's message must contain (see
    /// [`assert_denied`]) — also just a readable label for panics.
    fn name(self) -> &'static str {
        match self {
            Verb::ProbeCluster => "ProbeCluster",
            Verb::ClusterStatus => "ClusterStatus",
            Verb::AddLearner => "AddLearner",
            Verb::PromoteVoter => "PromoteVoter",
            Verb::RemoveNode => "RemoveNode",
            Verb::ReplaceVoter => "ReplaceVoter",
            Verb::TransferCaKey => "TransferCaKey",
            Verb::SetNodeAddress => "SetNodeAddress",
        }
    }
}

/// Per-cell request arguments. A verb reads only the fields it needs; the
/// rest are harmless defaults.
#[derive(Default)]
struct Params {
    node_id: u64,
    address: String,
    old_node_id: u64,
    new_node_id: u64,
    ca_key_pem: Vec<u8>,
}

/// Which certificate class a cell is aimed at — each maps to exactly one
/// already-dialed client, so the fixture and every mint happen once.
#[derive(Clone, Copy, Debug)]
enum Class {
    Agent,
    UnboundMachine,
    BoundMachine,
    Operator,
    Unclassifiable,
}

/// The outcome a cell expects — the whole vocabulary the superseded
/// functions' assertions used.
enum Expect {
    /// PERMISSION_DENIED + NOT_AUTHORIZED, and the message names the verb —
    /// the ordinary matrix "no" (see [`assert_denied`]).
    Denied,
    /// PERMISSION_DENIED + NOT_AUTHORIZED, but the message need not name the
    /// verb — a self-scope or leader-binding refusal with its own bespoke
    /// phrasing (see [`assert_out_of_scope`]).
    OutOfScope,
    /// UNAUTHENTICATED, and specifically NOT the NOT_AUTHORIZED marker — an
    /// unclassifiable caller, not a known profile denied a verb.
    Unauthenticated,
    /// Refused, but by a gate *past* authorization (ENDPOINT_UNVERIFIED /
    /// UNKNOWN_NODE / ADDRESS_CONFLICT / ...) — the operator-reachability
    /// claim, so this additionally asserts the refusal carries no
    /// NOT_AUTHORIZED marker.
    Marker(&'static str),
    /// The call must succeed outright.
    Ok,
}

/// Issue `verb` against `client` with `params`, discarding the response body
/// — every table cell cares only about success/failure and the failure's
/// markers.
async fn call_verb(
    client: &mut AdminClient,
    verb: Verb,
    hid: Vec<u8>,
    params: &Params,
) -> Result<tonic::Response<()>, Status> {
    match verb {
        Verb::ProbeCluster => client
            .probe_cluster(pb::ProbeClusterRequest {
                cluster_id: String::new(),
            })
            .await
            .map(|resp| resp.map(|_| ())),
        Verb::ClusterStatus => client
            .cluster_status(pb::ClusterStatusRequest { history_id: hid })
            .await
            .map(|resp| resp.map(|_| ())),
        Verb::AddLearner => client
            .add_learner(pb::AddLearnerRequest {
                history_id: hid,
                node_id: params.node_id,
                address: params.address.clone(),
            })
            .await
            .map(|resp| resp.map(|_| ())),
        Verb::PromoteVoter => client
            .promote_voter(pb::PromoteVoterRequest {
                history_id: hid,
                promote_node_id: params.node_id,
            })
            .await
            .map(|resp| resp.map(|_| ())),
        Verb::RemoveNode => client
            .remove_node(pb::RemoveNodeRequest {
                history_id: hid,
                node_id: params.node_id,
            })
            .await
            .map(|resp| resp.map(|_| ())),
        Verb::ReplaceVoter => client
            .replace_voter(pb::ReplaceVoterRequest {
                history_id: hid,
                old_node_id: params.old_node_id,
                new_node_id: params.new_node_id,
            })
            .await
            .map(|resp| resp.map(|_| ())),
        Verb::TransferCaKey => client
            .transfer_ca_key(pb::TransferCaKeyRequest {
                history_id: hid,
                ca_key_pem: params.ca_key_pem.clone(),
            })
            .await
            .map(|resp| resp.map(|_| ())),
        Verb::SetNodeAddress => client
            .set_node_address(pb::SetNodeAddressRequest {
                history_id: hid,
                node_id: params.node_id,
                address: params.address.clone(),
            })
            .await
            .map(|resp| resp.map(|_| ())),
    }
}

/// One row of the §7 matrix.
struct Cell {
    what: &'static str,
    class: Class,
    verb: Verb,
    params: Params,
    expect: Expect,
}

/// The §7 refusal matrix, table-driven: every (certificate class, verb) cell
/// the ADR draws a line for, aimed exactly as the granular tests it replaces
/// aimed them (same request arguments reach the same later gates), so the
/// assertions are unchanged — only their layout is.
///
/// One cluster fixture and one client per certificate class (agent, a
/// coordinator machine the cluster never bound, the forming voter's own
/// bound machine identity, operator, and an unclassifiable leaf), then the
/// loop drives every verb the ADR names against whichever class's client the
/// cell selects. The only cells that expect success are read-only or a true
/// no-op (an absent-seat RemoveNode, a machine's own ClusterStatus), so this
/// order is safe without a fixture per cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_refusal_matrix_covers_every_certificate_class_and_verb() {
    init_tracing();
    let ca = Ca::new();
    let formed = formed_single_voter(&ca).await;
    let hid = formed.history_id.clone();
    let (ca_pem, _, _) = formed.daemon.tls_material();
    let signer = cluster_signer(&formed.daemon);

    // Agent: a perfectly valid agent leaf under the cluster's own CA, so the
    // refusals below are the matrix saying no, not a chain failure saying
    // "who?".
    let (agent_cert, agent_key) =
        pki::mint_agent_local(&signer, &NodeId::new(), &[]).expect("mint an agent leaf");
    let mut agent_client = dial(&formed.daemon, &ca_pem, &agent_cert, &agent_key).await;

    // Unbound machine: a coordinator machine leaf whose identity the cluster
    // has NEVER bound — a second installation's credential arriving at
    // someone else's seat. For TransferCaKey this is also exactly the caller
    // the leader-binding gate's `bound != Some(leader)` check is built for:
    // the single-voter fixture leaves no *other* seat to bind a caller to
    // and still land in that arm, but an identity bound to no seat at all
    // exercises the same `bound != Some(leader)` comparison (`None !=
    // Some(formed.seat)`).
    let stranger = pki::mint_machine_identity();
    let (stranger_cert, stranger_key) =
        pki::mint_coordinator_local(&signer, &stranger, &[]).expect("mint a machine leaf");
    let mut unbound_client = dial(&formed.daemon, &ca_pem, &stranger_cert, &stranger_key).await;

    // Bound machine: the forming voter's own machine identity, minted under
    // the cluster CA — the sharper case, where even the right credential may
    // drive only its own seat.
    let bound_machine: MachineId = {
        let status_resp = operator_client(&formed.daemon, &formed.operator)
            .await
            .cluster_status(pb::ClusterStatusRequest {
                history_id: hid.clone(),
            })
            .await
            .expect("read bindings")
            .into_inner();
        let binding = status_resp
            .bindings
            .iter()
            .find(|b| b.node_id == formed.seat)
            .expect("formation bound the first voter (chunk 03)")
            .machine_id
            .clone();
        binding.parse().expect("binding machine id parses")
    };
    let (bound_cert, bound_key) =
        pki::mint_coordinator_local(&signer, &bound_machine, &[]).expect("mint the bound leaf");
    let mut bound_client = dial(&formed.daemon, &ca_pem, &bound_cert, &bound_key).await;

    // Operator: the credential `init` minted.
    let mut op_client = operator_client(&formed.daemon, &formed.operator).await;

    // Unclassifiable leaf: chains to the cluster CA, but its subject matches
    // no profile.
    let (unclass_cert, unclass_key) = unclassifiable_leaf(&formed.daemon);
    let mut unclass_client = dial(&formed.daemon, &ca_pem, &unclass_cert, &unclass_key).await;

    // Bespoke, does not fit the table: the unbound machine's ProbeCluster
    // answer must name the same history the fixture stamped — reachability
    // plus content, not just pass/fail. This also proves the read half of
    // the grant that the ClusterStatus cell below proves for status (§6
    // steps 3-4).
    let probe = unbound_client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("a machine cert may probe")
        .into_inner();
    assert_eq!(probe.history_id, hid);

    let cells = vec![
        // --- Agent: holds none of the membership surface. Every verb,
        // including the read-only ones — an agent probing membership must
        // learn nothing, not even what exists.
        Cell {
            what: "ProbeCluster from an agent",
            class: Class::Agent,
            verb: Verb::ProbeCluster,
            params: Params::default(),
            expect: Expect::Denied,
        },
        Cell {
            what: "ClusterStatus from an agent",
            class: Class::Agent,
            verb: Verb::ClusterStatus,
            params: Params::default(),
            expect: Expect::Denied,
        },
        Cell {
            what: "AddLearner from an agent",
            class: Class::Agent,
            verb: Verb::AddLearner,
            params: Params {
                node_id: formed.seat,
                address: formed.seat_addr.clone(),
                ..Default::default()
            },
            expect: Expect::Denied,
        },
        Cell {
            what: "PromoteVoter from an agent",
            class: Class::Agent,
            verb: Verb::PromoteVoter,
            params: Params {
                node_id: formed.seat,
                ..Default::default()
            },
            expect: Expect::Denied,
        },
        Cell {
            what: "RemoveNode from an agent",
            class: Class::Agent,
            verb: Verb::RemoveNode,
            params: Params {
                node_id: formed.seat,
                ..Default::default()
            },
            expect: Expect::Denied,
        },
        // ReplaceVoter joins RemoveNode: it removes a voter, so it is
        // operator-only and an agent is nowhere near it (§7).
        Cell {
            what: "ReplaceVoter from an agent",
            class: Class::Agent,
            verb: Verb::ReplaceVoter,
            params: Params {
                old_node_id: formed.seat,
                new_node_id: formed.seat + 1,
                ..Default::default()
            },
            expect: Expect::Denied,
        },
        // The CA-key transfer is coordinator-to-coordinator (§4); an agent
        // leaf reaching it would be a path from one compute node's
        // credential to root-equivalence.
        Cell {
            what: "TransferCaKey from an agent",
            class: Class::Agent,
            verb: Verb::TransferCaKey,
            params: Params {
                ca_key_pem: b"not a key".to_vec(),
                ..Default::default()
            },
            expect: Expect::Denied,
        },
        Cell {
            what: "SetNodeAddress from an agent",
            class: Class::Agent,
            verb: Verb::SetNodeAddress,
            params: Params {
                node_id: formed.seat,
                address: formed.seat_addr.clone(),
                ..Default::default()
            },
            expect: Expect::Denied,
        },
        // --- Unbound machine: the read half of the grant, refused
        // everywhere else.
        Cell {
            what: "ClusterStatus from an unbound machine",
            class: Class::UnboundMachine,
            verb: Verb::ClusterStatus,
            params: Params::default(),
            expect: Expect::Ok,
        },
        // AddLearner for a seat bound to a DIFFERENT identity. The service's
        // order is verify -> bind -> admit, so the stranger is stopped at
        // dial-back verification: the endpoint at that address serves the
        // bound machine's identity, not the caller's — which is precisely
        // why a stolen client credential alone cannot occupy someone else's
        // seat (§7).
        Cell {
            what: "AddLearner for another identity's seat",
            class: Class::UnboundMachine,
            verb: Verb::AddLearner,
            params: Params {
                node_id: formed.seat,
                address: formed.seat_addr.clone(),
                ..Default::default()
            },
            expect: Expect::Marker(ENDPOINT_UNVERIFIED),
        },
        // An unbound machine has no seat at all, so the self-scope check
        // refuses outright.
        Cell {
            what: "PromoteVoter from an unbound machine",
            class: Class::UnboundMachine,
            verb: Verb::PromoteVoter,
            params: Params {
                node_id: formed.seat,
                ..Default::default()
            },
            expect: Expect::OutOfScope,
        },
        // (a) The transfer protocol is leader-to-candidate only (§4): a
        // machine whose bound seat is not the leader this recipient
        // observes is refused with the handler's own bespoke phrasing, not
        // the matrix's verb-naming denial.
        Cell {
            what: "TransferCaKey from a machine not bound to the leader",
            class: Class::UnboundMachine,
            verb: Verb::TransferCaKey,
            params: Params {
                ca_key_pem: b"not a key".to_vec(),
                ..Default::default()
            },
            expect: Expect::OutOfScope,
        },
        // --- Bound machine: exactly the self-scope grant, and never the
        // membership-shrinking verbs, regardless of binding.
        // The same self-scope refusal as the unbound machine's PromoteVoter
        // cell above, but from a *bound* machine naming a seat that is not
        // its own binding — the sharper half: even the right credential may
        // drive only its own seat.
        Cell {
            what: "PromoteVoter for a seat that is not the caller's binding",
            class: Class::BoundMachine,
            verb: Verb::PromoteVoter,
            params: Params {
                node_id: formed.seat + 1,
                ..Default::default()
            },
            expect: Expect::OutOfScope,
        },
        Cell {
            what: "RemoveNode from a machine",
            class: Class::BoundMachine,
            verb: Verb::RemoveNode,
            params: Params {
                node_id: formed.seat,
                ..Default::default()
            },
            expect: Expect::Denied,
        },
        // "it can never remove, replace, repoint, or initialize" (§7) — even
        // for its own bound seat, and even when the caller is the very
        // machine that formed the cluster.
        Cell {
            what: "ReplaceVoter from a machine",
            class: Class::BoundMachine,
            verb: Verb::ReplaceVoter,
            params: Params {
                old_node_id: formed.seat,
                new_node_id: formed.seat + 1,
                ..Default::default()
            },
            expect: Expect::Denied,
        },
        // The sharper form: the bound machine names *itself* as the seat
        // that would be added (`new_node_id`), asking the cluster to
        // replace some other voter with the caller's own binding.
        // Self-interest is not a grant — ReplaceVoter is operator-only
        // regardless of which side of the swap the caller's own seat
        // appears on.
        Cell {
            what: "ReplaceVoter naming the caller's own seat as the replacement",
            class: Class::BoundMachine,
            verb: Verb::ReplaceVoter,
            params: Params {
                old_node_id: formed.seat + 1,
                new_node_id: formed.seat,
                ..Default::default()
            },
            expect: Expect::Denied,
        },
        Cell {
            what: "SetNodeAddress from a machine",
            class: Class::BoundMachine,
            verb: Verb::SetNodeAddress,
            params: Params {
                node_id: formed.seat,
                address: formed.seat_addr.clone(),
                ..Default::default()
            },
            expect: Expect::Denied,
        },
        // --- Operator: reachability. None of these may bounce off the §7
        // matrix — each is aimed past the authz layer at a later gate (or a
        // no-op success).
        // RemoveNode of an absent seat: reaches the verb and no-ops (§6).
        Cell {
            what: "operator RemoveNode of an absent seat",
            class: Class::Operator,
            verb: Verb::RemoveNode,
            params: Params {
                node_id: 424_242,
                ..Default::default()
            },
            expect: Expect::Ok,
        },
        // SetNodeAddress of an unknown seat: refused by a later gate, not
        // authz.
        Cell {
            what: "operator SetNodeAddress for an unknown node",
            class: Class::Operator,
            verb: Verb::SetNodeAddress,
            params: Params {
                node_id: 424_242,
                address: "localhost:1".to_string(),
                ..Default::default()
            },
            expect: Expect::Marker(UNKNOWN_NODE),
        },
        // AddLearner naming the existing seat at a different address:
        // refused by the no-silent-repointing gate, not authz.
        Cell {
            what: "operator AddLearner at a conflicting address",
            class: Class::Operator,
            verb: Verb::AddLearner,
            params: Params {
                node_id: formed.seat,
                address: "localhost:1".to_string(),
                ..Default::default()
            },
            expect: Expect::Marker(ADDRESS_CONFLICT),
        },
        // ReplaceVoter naming a seat that is not in membership: refused by
        // the membership gate, not by the matrix — the operator is the
        // *only* profile that reaches this verb at all (§7).
        Cell {
            what: "operator ReplaceVoter for an unknown new node",
            class: Class::Operator,
            verb: Verb::ReplaceVoter,
            params: Params {
                old_node_id: formed.seat,
                new_node_id: 424_242,
                ..Default::default()
            },
            expect: Expect::Marker(UNKNOWN_NODE),
        },
        // PromoteVoter of an unknown seat: refused by the membership gate,
        // not authz.
        Cell {
            what: "operator PromoteVoter for an unknown node",
            class: Class::Operator,
            verb: Verb::PromoteVoter,
            params: Params {
                node_id: 424_242,
                ..Default::default()
            },
            expect: Expect::Marker(UNKNOWN_NODE),
        },
        // (b) The transfer protocol is coordinator-to-coordinator only (§4):
        // an operator with a legitimate need to key a candidate must drive
        // it via promotion or replacement instead — not by pushing a key
        // itself. Refused with the handler's own bespoke phrasing, like the
        // unbound machine's cell above.
        Cell {
            what: "TransferCaKey from an operator",
            class: Class::Operator,
            verb: Verb::TransferCaKey,
            params: Params {
                ca_key_pem: b"not a key".to_vec(),
                ..Default::default()
            },
            expect: Expect::OutOfScope,
        },
        // --- Unclassifiable leaf: chains to the cluster CA — the TLS
        // acceptor lets the session in — but its subject matches no
        // profile. That is an *unknown caller*, which must read as
        // UNAUTHENTICATED, not as a known profile being denied a verb: the
        // matrix's PERMISSION_DENIED would leak that classification
        // succeeded.
        Cell {
            what: "ProbeCluster with an unclassifiable leaf",
            class: Class::Unclassifiable,
            verb: Verb::ProbeCluster,
            params: Params::default(),
            expect: Expect::Unauthenticated,
        },
    ];

    for Cell {
        what,
        class,
        verb,
        params,
        expect,
    } in cells
    {
        let client = match class {
            Class::Agent => &mut agent_client,
            Class::UnboundMachine => &mut unbound_client,
            Class::BoundMachine => &mut bound_client,
            Class::Operator => &mut op_client,
            Class::Unclassifiable => &mut unclass_client,
        };
        let result = call_verb(client, verb, hid.clone(), &params).await;
        match expect {
            Expect::Ok => {
                result.unwrap_or_else(|status| panic!("{what} must succeed: {status:?}"));
            }
            Expect::Denied => {
                let status = refusal(result, what);
                assert_denied(&status, verb.name());
            }
            Expect::OutOfScope => {
                let status = refusal(result, what);
                assert_out_of_scope(&status, what);
            }
            Expect::Unauthenticated => {
                let status = refusal(result, what);
                assert_eq!(
                    status.code(),
                    Code::Unauthenticated,
                    "{what}: an unclassifiable caller is unauthenticated, got ({:?}) {:?}",
                    status.code(),
                    status.message()
                );
                assert!(
                    !has_marker(status.message(), NOT_AUTHORIZED),
                    "{what}: not a matrix denial: {:?}",
                    status.message()
                );
            }
            Expect::Marker(marker) => {
                let status = refusal(result, what);
                assert_marker(&status, marker, what);
                assert!(
                    !has_marker(status.message(), NOT_AUTHORIZED),
                    "{what}: the caller must not be authz-refused: {:?}",
                    status.message()
                );
            }
        }
    }

    let mut daemon = formed.daemon;
    daemon.stop().await.expect("daemon stops cleanly");
}

// ---------------------------------------------------------------------------
// 2. The §6 idempotency contract, verb by verb
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replayed_add_learner_at_the_same_address_is_a_noop_success() {
    init_tracing();
    let ca = Ca::new();
    let formed = formed_single_voter(&ca).await;
    let mut client = operator_client(&formed.daemon, &formed.operator).await;

    // The forming voter is already in membership at exactly this address, so
    // the replay's work is already done — the §6 contract makes that a plain
    // success, which is what lets the convergence loop re-enter from the top
    // on every restart without special-casing "did I already do this?".
    client
        .add_learner(pb::AddLearnerRequest {
            history_id: formed.history_id.clone(),
            node_id: formed.seat,
            address: formed.seat_addr.clone(),
        })
        .await
        .expect("an exact AddLearner replay is a no-op success");

    let mut daemon = formed.daemon;
    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_add_learner_for_a_known_seat_at_a_new_address_is_an_address_conflict() {
    init_tracing();
    let ca = Ca::new();
    let formed = formed_single_voter(&ca).await;
    let mut client = operator_client(&formed.daemon, &formed.operator).await;

    // Same seat, different address: never a silent repoint (§6). An instance
    // whose address changed is a new instance; a genuine move is set-address.
    let status = refusal(
        client
            .add_learner(pb::AddLearnerRequest {
                history_id: formed.history_id.clone(),
                node_id: formed.seat,
                address: format!("localhost:{}", free_port()),
            })
            .await,
        "AddLearner for a known seat at a new address",
    );
    assert_marker(&status, ADDRESS_CONFLICT, "AddLearner at a new address");

    let mut daemon = formed.daemon;
    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promoting_an_already_voter_is_a_noop_before_the_lag_gate() {
    init_tracing();
    let ca = Ca::new();
    let formed = formed_single_voter(&ca).await;
    let mut client = operator_client(&formed.daemon, &formed.operator).await;

    // The seat is already a voter. The §6 contract puts the no-op check
    // BEFORE the lag gate, so this must be a success and specifically must
    // never bounce as `learner-behind` — a promoted voter has no learner lag
    // to evaluate, and evaluating one anyway is exactly the bug the ordering
    // clause exists to forbid. Twice, because the second call is the replay
    // the convergence loop actually issues after a restart.
    for round in 0..2 {
        match client
            .promote_voter(pb::PromoteVoterRequest {
                history_id: formed.history_id.clone(),
                promote_node_id: formed.seat,
            })
            .await
        {
            Ok(_) => {}
            Err(status) => {
                assert!(
                    !has_marker(status.message(), LEARNER_BEHIND),
                    "round {round}: an already-voter promotion reached the lag gate: {:?}",
                    status.message()
                );
                panic!(
                    "round {round}: promoting an already-voter must be a no-op success, \
                     got ({:?}) {:?}",
                    status.code(),
                    status.message()
                );
            }
        }
    }

    let mut daemon = formed.daemon;
    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promoting_an_unknown_node_is_refused_as_unknown() {
    init_tracing();
    let ca = Ca::new();
    let formed = formed_single_voter(&ca).await;
    let mut client = operator_client(&formed.daemon, &formed.operator).await;

    // Unknown seat: terminal, and the marker must say so — no amount of
    // waiting introduces a node into membership (§6).
    let status = refusal(
        client
            .promote_voter(pb::PromoteVoterRequest {
                history_id: formed.history_id.clone(),
                promote_node_id: 424_242,
            })
            .await,
        "PromoteVoter for an unknown node",
    );
    assert_marker(&status, UNKNOWN_NODE, "PromoteVoter for an unknown node");

    let mut daemon = formed.daemon;
    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_an_absent_node_is_a_noop_success() {
    init_tracing();
    let ca = Ca::new();
    let formed = formed_single_voter(&ca).await;
    let mut client = operator_client(&formed.daemon, &formed.operator).await;

    // Absent seat: the removal's work is already done, so the replayed verb
    // succeeds — which is what makes a retried decommission runbook safe.
    client
        .remove_node(pb::RemoveNodeRequest {
            history_id: formed.history_id.clone(),
            node_id: 424_242,
        })
        .await
        .expect("removing an absent node is a no-op success");

    let mut daemon = formed.daemon;
    daemon.stop().await.expect("daemon stops cleanly");
}

// ---------------------------------------------------------------------------
// 3. set-address verification paths (§6, operator-only break-glass)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_address_noops_when_settled_and_refuses_the_unverifiable() {
    init_tracing();
    let ca = Ca::new();
    let formed = formed_single_voter(&ca).await;
    let mut client = operator_client(&formed.daemon, &formed.operator).await;
    let hid = formed.history_id.clone();

    // Formation bound the first voter's machine identity (chunk 03) — assert
    // that first, because both the no-op below (binding must match) and the
    // dial-back path (there must be an identity to verify against) rest on it.
    let status_resp = client
        .cluster_status(pb::ClusterStatusRequest {
            history_id: hid.clone(),
        })
        .await
        .expect("read status")
        .into_inner();
    let binding = status_resp
        .bindings
        .iter()
        .find(|b| b.node_id == formed.seat)
        .expect("formation bound the first voter's machine identity");
    assert_eq!(binding.address, formed.seat_addr);

    // Happy path, strongest stageable form: set-address to the address the
    // member already serves at. Dial-back verification runs against the live
    // endpoint (serving cert presents the bound identity, probe reports the
    // seat), and membership + binding are already settled — a no-op success.
    client
        .set_node_address(pb::SetNodeAddressRequest {
            history_id: hid.clone(),
            node_id: formed.seat,
            address: formed.seat_addr.clone(),
        })
        .await
        .expect("set-address to the settled address is a no-op success");

    // An address where nothing listens: dial-back verification cannot pass,
    // so the repoint is refused...
    let dead = format!("localhost:{}", free_port());
    let status = refusal(
        client
            .set_node_address(pb::SetNodeAddressRequest {
                history_id: hid.clone(),
                node_id: formed.seat,
                address: dead,
            })
            .await,
        "set-address to a dead endpoint",
    );
    assert_marker(
        &status,
        ENDPOINT_UNVERIFIED,
        "set-address to a dead endpoint",
    );

    // ...and membership is unchanged: a refused repoint must not have moved
    // anything, or the "leader commits only after verification" clause is
    // hollow.
    let status_resp = client
        .cluster_status(pb::ClusterStatusRequest {
            history_id: hid.clone(),
        })
        .await
        .expect("re-read status")
        .into_inner();
    let member = status_resp
        .membership
        .as_ref()
        .expect("membership present")
        .members
        .iter()
        .find(|m| m.node_id == formed.seat)
        .expect("the seat is still in membership");
    assert_eq!(
        member.address, formed.seat_addr,
        "a refused set-address must leave membership untouched"
    );

    // An unknown seat: refused before any dial — set-address repoints an
    // existing member, it never creates one.
    let status = refusal(
        client
            .set_node_address(pb::SetNodeAddressRequest {
                history_id: hid,
                node_id: 424_242,
                address: formed.seat_addr.clone(),
            })
            .await,
        "set-address for an unknown node",
    );
    assert_marker(&status, UNKNOWN_NODE, "set-address for an unknown node");

    let mut daemon = formed.daemon;
    daemon.stop().await.expect("daemon stops cleanly");
}

/// Mint the coordinator enrollment token a fleet's config artifact carries
/// (ADR 0037 §5), using the operator credential `init` printed.
async fn coordinator_token(daemon: &Daemon, operator: &OperatorPem) -> String {
    let mut client = operator_client(daemon, operator).await;
    let history_id = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe")
        .into_inner()
        .history_id;
    client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id,
            role: pbcore::EnrollRole::Coordinator as i32,
            label: "coordinators".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("mint a coordinator enrollment token")
        .into_inner()
        .secret
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repoint_is_dialback_verified_rebinds_the_machine_and_replays_as_a_noop() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // A two-voter cluster stood up the self-converging way (§1): the second
    // member enrolls, joins, and promotes itself — because this test needs a
    // member with a *real* machine binding minted by the real admission path.
    let mut leader = Daemon::new_certless(cluster_id, &ca);
    leader.set_cluster_size(2);
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    let mut member = Daemon::new_certless(cluster_id, &ca);
    member.set_cluster_size(2);
    member.set_static_discovery(&[leader.raft_target()]);
    member.set_enrollment(&leader.api(""), &token);
    member.start();
    member.await_phase("voter").await;

    let mut client = operator_client(&leader, &operator).await;
    let hid = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe")
        .into_inner()
        .history_id;

    let status_resp = client
        .cluster_status(pb::ClusterStatusRequest {
            history_id: hid.clone(),
        })
        .await
        .expect("read status")
        .into_inner();
    let leader_seat = status_resp.local_node_id;
    let leader_addr = leader.raft_target();
    let member_rec = status_resp
        .membership
        .as_ref()
        .expect("membership present")
        .members
        .iter()
        .find(|m| m.node_id != leader_seat)
        .expect("the joined member is in membership")
        .clone();
    let member_machine: MachineId = status_resp
        .bindings
        .iter()
        .find(|b| b.node_id == member_rec.node_id)
        .expect("admission bound the member's machine identity")
        .machine_id
        .parse()
        .expect("binding machine id parses");

    // A live endpoint that belongs to a DIFFERENT machine: the leader's own.
    // The chain verifies and something answers — but the serving certificate
    // presents the leader's identity, not the identity bound to the member's
    // seat, so the repoint must be refused. A claimed address is not proof of
    // ownership (§6).
    let status = refusal(
        client
            .set_node_address(pb::SetNodeAddressRequest {
                history_id: hid.clone(),
                node_id: member_rec.node_id,
                address: leader_addr,
            })
            .await,
        "set-address to another machine's endpoint",
    );
    assert_marker(
        &status,
        ENDPOINT_UNVERIFIED,
        "set-address to another machine's endpoint",
    );

    // Now a genuine repoint: stop the member and restart it listening on a
    // new port — the "pet deployment whose address moved" the break-glass
    // exists for. The config rewrite targets exactly the raft_addr line, so
    // the daemon's other listeners keep their ports.
    member.stop().await.expect("member stops cleanly");
    let old_port = member
        .raft_target()
        .rsplit_once(':')
        .expect("raft target has a port")
        .1
        .to_string();
    let new_port = free_port();
    let config_path = member.config_path();
    let toml = std::fs::read_to_string(&config_path).expect("read member config");
    let rewritten = toml.replace(
        &format!("raft_addr = \"127.0.0.1:{old_port}\""),
        &format!("raft_addr = \"127.0.0.1:{new_port}\""),
    );
    assert_ne!(toml, rewritten, "raft_addr line not found to rewrite");
    std::fs::write(&config_path, rewritten).expect("rewrite member config");
    member.start();
    let new_addr = format!("localhost:{new_port}");

    // Wait until the moved member is serving at its new address — dial-back
    // verification needs a live endpoint, and "not serving yet" is a wait,
    // not a failure. The member's own convergence loop is meanwhile being
    // refused with `machine-address-conflict` (re-admission never repoints),
    // which is exactly the wedge set-address exists to clear.
    {
        let member_ref = &member;
        let operator_ref = &operator;
        let leader_ref = &leader;
        let new_addr = new_addr.clone();
        poll(
            Duration::from_secs(30),
            "the moved member serves probes at its new address",
            move || {
                let new_addr = new_addr.clone();
                async move {
                    let Ok(mut probe_client) = admin::admin_channel(
                        &new_addr,
                        operator_ref.ca_pem.as_bytes(),
                        operator_ref.cert_pem.as_bytes(),
                        operator_ref
                            .key_pem
                            .as_ref()
                            .expect("cluster-minted operator key")
                            .as_bytes(),
                    )
                    .await
                    else {
                        return false;
                    };
                    probe_client
                        .probe_cluster(pb::ProbeClusterRequest {
                            cluster_id: String::new(),
                        })
                        .await
                        .is_ok_and(|resp| resp.into_inner().node_id == Some(member_rec.node_id))
                }
            },
        )
        .await;
        let _ = (member_ref, leader_ref);
    }

    // The operator repoints the seat. Retried under poll() because the
    // verification dial can transiently race the member's listener coming up;
    // any terminal refusal keeps failing until the deadline names it.
    {
        let hid = hid.clone();
        let new_addr = new_addr.clone();
        let operator_ref = &operator;
        let leader_ref = &leader;
        poll(
            Duration::from_secs(30),
            "set-address verifies the new endpoint and commits",
            move || {
                let hid = hid.clone();
                let new_addr = new_addr.clone();
                async move {
                    let Ok(mut c) = admin::admin_channel(
                        &leader_ref.raft_target(),
                        operator_ref.ca_pem.as_bytes(),
                        operator_ref.cert_pem.as_bytes(),
                        operator_ref
                            .key_pem
                            .as_ref()
                            .expect("cluster-minted operator key")
                            .as_bytes(),
                    )
                    .await
                    else {
                        return false;
                    };
                    c.set_node_address(pb::SetNodeAddressRequest {
                        history_id: hid,
                        node_id: member_rec.node_id,
                        address: new_addr,
                    })
                    .await
                    .is_ok()
                }
            },
        )
        .await;
    }

    // Both replicated facts moved together: raft membership dials the new
    // address, and the machine binding follows it (§6 — without the rebind,
    // the member's own replay below would be `machine-address-conflict`
    // forever).
    let status_resp = client
        .cluster_status(pb::ClusterStatusRequest {
            history_id: hid.clone(),
        })
        .await
        .expect("re-read status")
        .into_inner();
    let member_after = status_resp
        .membership
        .as_ref()
        .expect("membership present")
        .members
        .iter()
        .find(|m| m.node_id == member_rec.node_id)
        .expect("the member is still in membership");
    assert_eq!(
        member_after.address, new_addr,
        "membership follows the repoint"
    );
    let binding_after = status_resp
        .bindings
        .iter()
        .find(|b| b.node_id == member_rec.node_id)
        .expect("the binding survives the repoint");
    assert_eq!(
        binding_after.address, new_addr,
        "the machine binding follows the repoint"
    );

    // And the member's own convergence replay is now a no-op success rather
    // than a conflict: present the member's machine identity (minted under
    // the cluster CA the leader's disk holds) and re-offer the §6 step-3
    // AddLearner exactly as its loop does on every restart.
    let signer = cluster_signer(&leader);
    let (member_cert, member_key) = pki::mint_coordinator_local(&signer, &member_machine, &[])
        .expect("mint the member's machine leaf");
    let (cluster_ca, _, _) = leader.tls_material();
    let mut member_as_machine = dial(&leader, &cluster_ca, &member_cert, &member_key).await;
    member_as_machine
        .add_learner(pb::AddLearnerRequest {
            history_id: hid,
            node_id: member_rec.node_id,
            address: new_addr,
        })
        .await
        .expect(
            "after set-address the member's own AddLearner replay is a no-op success, \
             not machine-address-conflict",
        );

    member.stop().await.expect("member stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}

// ---------------------------------------------------------------------------
// 4. Operator admission binds the seat, and status resolves the leader
// ---------------------------------------------------------------------------

/// A single-voter cluster of [`Node`] fixtures plus an operator client on its
/// leader: the §7 operator-admission tests stage live joiners against this.
/// The test `Ca` plays the cluster CA (every leaf, serving and client, chains
/// to it), and the history is the config-derived one legacy bootstrap stamps.
struct NodeCluster {
    ca: Ca,
    leader: Node,
    history_id: [u8; 16],
}

impl NodeCluster {
    async fn start() -> NodeCluster {
        let ca = Ca::new();
        let cluster_id = ClusterId::new();
        let mut leader = Node::new(1, cluster_id, &ca);
        leader.boot().await;
        {
            let leader_ref = &leader;
            poll(Duration::from_secs(20), "node 1 becomes leader", || async {
                leader_ref.is_leader()
            })
            .await;
        }
        NodeCluster {
            ca,
            leader,
            history_id: *cluster_id.0.as_bytes(),
        }
    }

    /// An operator-profile client dialing `target` (default: the leader).
    async fn operator(&self, target: &str) -> AdminClient {
        let leaf = self.ca.operator_leaf();
        admin::admin_channel(target, &self.ca.pem, &leaf.cert_pem, &leaf.key_pem)
            .await
            .expect("dial the admin surface as an operator")
    }
}

/// The Finding-2 contract end to end (ADR 0037 §7): an **operator** admission
/// is verify → bind → admit, exactly as a machine admission is. The operator's
/// authority bypasses self-scope, never the binding invariant — so the seat it
/// creates is bound to the machine identity the joiner's serving leaf
/// presented *before* promotion, `admin status` never shows a
/// `machine_id: null` seat for it, and the exact replay is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operator_admission_dial_back_verifies_binds_and_replays_as_a_noop() {
    init_tracing();
    let cluster = NodeCluster::start().await;
    let hid = cluster.history_id.to_vec();

    // A live joiner serving a coordinator-profile leaf under the cluster CA
    // (the Node default), standing in for a daemon whose convergence loop
    // automation cannot reach — the case the operator credential exists for.
    let mut joiner = Node::new(2, cluster.leader.cluster_id, &cluster.ca);
    joiner.boot_joining().await;
    let joiner_machine = joiner
        .machine
        .expect("the default fixture leaf carries one");

    let mut client = cluster.operator(&cluster.leader.advertise).await;
    client
        .add_learner(pb::AddLearnerRequest {
            history_id: hid.clone(),
            node_id: joiner.raft_id(),
            address: joiner.advertise.clone(),
        })
        .await
        .expect("operator admission of a live, verifiable joiner");

    // BEFORE promote: the seat is already bound to the identity the joiner's
    // serving leaf presented. This is the invariant the operator path used to
    // skip — an admitted-but-unbound seat showing `machine_id: null`.
    let status = client
        .cluster_status(pb::ClusterStatusRequest {
            history_id: hid.clone(),
        })
        .await
        .expect("read status after admission")
        .into_inner();
    let binding = status
        .bindings
        .iter()
        .find(|b| b.node_id == joiner.raft_id())
        .expect("operator admission bound the joiner's seat before promotion");
    assert_eq!(
        binding.machine_id,
        joiner_machine.to_string(),
        "the bound identity is the one the serving leaf presented, extracted by dial-back"
    );
    assert_eq!(binding.address, joiner.advertise);

    // Promotion then succeeds (the poll wrapper rides out catch-up)...
    admin::promote_voter(
        &mut client,
        cluster.history_id,
        joiner.raft_id(),
        Duration::from_secs(60),
        Duration::from_millis(50),
    )
    .await
    .expect("promote the admitted joiner");

    // ...and the operator's exact replay is a no-op success — which now
    // requires the binding to exist at that address, not membership alone.
    client
        .add_learner(pb::AddLearnerRequest {
            history_id: hid,
            node_id: joiner.raft_id(),
            address: joiner.advertise.clone(),
        })
        .await
        .expect("a repeated operator add-learner is a no-op success");

    joiner.graceful_stop().await;
    let mut leader = cluster.leader;
    leader.graceful_stop().await;
}

/// The negative half of Finding 2: an operator naming an address whose serving
/// leaf chains to the cluster CA but classifies as no coordinator identity
/// (the profile-less [`Ca::leaf`]) is refused as `endpoint-unverified`, and no
/// seat is created — there is nothing to bind, and admission never creates an
/// unbound seat (ADR 0037 §7).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operator_admission_of_an_unclassifiable_endpoint_creates_no_seat() {
    init_tracing();
    let cluster = NodeCluster::start().await;
    let hid = cluster.history_id.to_vec();

    let plain_leaf = cluster.ca.leaf();
    let mut plain = Node::new_with_leaf(2, cluster.leader.cluster_id, &cluster.ca, plain_leaf);
    plain.boot_joining().await;

    let mut client = cluster.operator(&cluster.leader.advertise).await;
    let status = refusal(
        client
            .add_learner(pb::AddLearnerRequest {
                history_id: hid.clone(),
                node_id: plain.raft_id(),
                address: plain.advertise.clone(),
            })
            .await,
        "operator AddLearner for an endpoint serving no coordinator identity",
    );
    assert_marker(
        &status,
        ENDPOINT_UNVERIFIED,
        "operator AddLearner for an unclassifiable endpoint",
    );

    // No seat was created: verification failed before anything committed.
    let after = client
        .cluster_status(pb::ClusterStatusRequest { history_id: hid })
        .await
        .expect("read status after the refusal")
        .into_inner();
    assert!(
        after
            .membership
            .as_ref()
            .expect("membership present")
            .members
            .iter()
            .all(|m| m.node_id != plain.raft_id()),
        "a refused operator admission must not have created a seat"
    );
    assert!(
        after.bindings.iter().all(|b| b.node_id != plain.raft_id()),
        "a refused operator admission must not have bound anything"
    );

    plain.graceful_stop().await;
    let mut leader = cluster.leader;
    leader.graceful_stop().await;
}

/// The Finding-3 retargeting contract (ADR 0037 §9): `admin status` against a
/// follower — whose answer carries no health verdict — re-dials the leader the
/// answer names and renders the *leader's* document, so the one stable JSON
/// schema keeps its leader-only fields whenever a leader is reachable. The
/// helper under test is exactly what `run_cli`'s `status` verb calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_status_against_a_follower_resolves_and_renders_the_leader() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // A two-voter cluster stood up the self-converging way (ADR 0037 §1): the
    // second member enrolls, joins and promotes itself, so the replica this
    // test queries is a real converged follower whose membership view already
    // names the leader's dialable address — nothing is admitted by hand.
    let mut first_member = Daemon::new_certless(cluster_id, &ca);
    first_member.set_cluster_size(2);
    let operator = form(&mut first_member).await;
    let token = coordinator_token(&first_member, &operator).await;

    let mut second_member = Daemon::new_certless(cluster_id, &ca);
    second_member.set_cluster_size(2);
    second_member.set_static_discovery(&[first_member.raft_target()]);
    second_member.set_enrollment(&first_member.api(""), &token);
    second_member.start();
    second_member.await_phase("voter").await;

    // Which of the two leads is observed, not assumed: leadership can move the
    // moment the second voter arrives, and this test needs a genuine follower.
    let (leader, follower) = {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let first_leads = first_member.readyz().await.1["is_leader"] == true;
            let second_leads = second_member.readyz().await.1["is_leader"] == true;
            if first_leads {
                break (&first_member, &second_member);
            }
            if second_leads {
                break (&second_member, &first_member);
            }
            assert!(
                std::time::Instant::now() < deadline,
                "neither member of a converged two-voter cluster reported leadership"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let leader_seat = node_id_of(leader).await;
    let follower_seat = node_id_of(follower).await;
    assert_ne!(leader_seat, follower_seat);

    let mut leader_client = operator_client(leader, &operator).await;
    let hid_bytes = leader_client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe")
        .into_inner()
        .history_id;
    let history_id: [u8; 16] = hid_bytes
        .try_into()
        .unwrap_or_else(|v: Vec<u8>| panic!("history id must be 16 bytes, got {}", v.len()));

    // Straight ClusterStatus at the follower: its own local view — no health
    // verdict (only the leader can answer one), leader named elsewhere.
    let mut follower_client = operator_client(follower, &operator).await;
    let first = admin::cluster_status(&mut follower_client, history_id)
        .await
        .expect("the follower answers cluster status");
    assert_eq!(first.local_node_id, follower_seat);
    assert!(
        first.health.is_none(),
        "a follower must not fabricate a health verdict"
    );
    assert_eq!(first.leader_node_id, Some(leader_seat));

    // The resolving form re-dials the leader once and renders its answer.
    let mut follower_client = operator_client(follower, &operator).await;
    let resolved = admin::cluster_status_resolving_leader(
        &mut follower_client,
        history_id,
        operator.ca_pem.as_bytes(),
        operator.cert_pem.as_bytes(),
        operator
            .key_pem
            .as_ref()
            .expect("no CSR was supplied, so the cluster minted the keypair")
            .as_bytes(),
    )
    .await
    .expect("status resolves the leader");
    assert_eq!(
        resolved.local_node_id, leader_seat,
        "the rendered document is the leader's answer"
    );
    assert_eq!(resolved.leader_node_id, Some(leader_seat));
    assert!(
        !resolved.replication.is_empty(),
        "the leader's answer carries per-follower replication progress"
    );

    let _ = second_member.stop().await;
    let _ = first_member.stop().await;
}

/// This daemon's own allocated raft seat, from `/readyz`.
async fn node_id_of(daemon: &Daemon) -> u64 {
    let (_, body) = daemon.readyz().await;
    body["node_id"]
        .as_u64()
        .unwrap_or_else(|| panic!("a formed member reports its node id: {body}"))
}
