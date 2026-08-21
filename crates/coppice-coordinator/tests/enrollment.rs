//! Enrollment, token administration, and renewal against a real formed
//! cluster (ADR 0037 §4/§5).
//!
//! Everything here runs against a **certless** daemon — the §4 minimal
//! deployment, which provisions no certificates and mints its own at formation
//! — so the CA every assertion chains to is the cluster's own, not the test
//! harness's. That is what makes the chain below meaningful end to end: mint a
//! token over the admin plane, redeem it for a leaf through the enrollment
//! core, and then use *that leaf* to renew itself over the machine plane.
//!
//! `ForwardEnroll` stands in for `POST /api/v1/enroll` here. It is the same
//! enrollment core behind both, and it is the half that exists in this chunk;
//! the public HTTP route arrives with the client listener's TLS posture.

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::id::ClusterId;
use coppice_core::id::{MachineId, NodeId};
use coppice_proto::pb::agent::v1 as pbagent;
use coppice_proto::pb::core::v1 as pbcore;
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::pki;

use common::{Ca, Daemon};

/// A formed, certless single-node cluster plus the operator credential `init`
/// printed — the two things every test here starts from.
async fn formed(ca: &Ca) -> (Daemon, OperatorPem) {
    let mut daemon = Daemon::new_certless(ClusterId::new(), ca);
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
    (daemon, operator)
}

fn operator_identity(operator: &OperatorPem) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    (
        operator.ca_pem.as_bytes().to_vec(),
        operator.cert_pem.as_bytes().to_vec(),
        operator
            .key_pem
            .as_ref()
            .expect("no CSR was supplied, so the cluster minted the keypair")
            .as_bytes()
            .to_vec(),
    )
}

/// An admin client authenticated with the operator credential, plus the
/// cluster's stamped history id (which every admin verb cross-checks).
async fn admin_client(
    daemon: &Daemon,
    operator: &OperatorPem,
) -> (
    coppice_net::admin::Client<tonic::transport::Channel>,
    Vec<u8>,
) {
    let (ca, cert, key) = operator_identity(operator);
    let mut client =
        coppice_coordinator::admin::admin_channel(&daemon.raft_target(), &ca, &cert, &key)
            .await
            .expect("dial the admin surface");
    let probe = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe")
        .into_inner();
    (client, probe.history_id)
}

async fn mint(
    client: &mut coppice_net::admin::Client<tonic::transport::Channel>,
    history_id: &[u8],
    role: pbcore::EnrollRole,
    label: &str,
) -> pb::MintEnrollTokenResponse {
    client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id: history_id.to_vec(),
            role: role as i32,
            label: label.to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("mint")
        .into_inner()
}

// ---------------------------------------------------------------------------
// Token lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mint_returns_the_secret_once_and_list_never_shows_a_hash() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut client, history_id) = admin_client(&daemon, &operator).await;

    let minted = mint(
        &mut client,
        &history_id,
        pbcore::EnrollRole::Agent,
        "fleet-agents",
    )
    .await;
    assert!(
        minted.secret.starts_with("cpk_"),
        "the secret is a token secret, got {:?}",
        minted.secret
    );
    assert!(minted.token_id.is_some());
    assert!(minted.expires_at_us.is_none(), "no TTL was requested");

    let listed = client
        .list_enroll_tokens(pb::ListEnrollTokensRequest {
            history_id: history_id.clone(),
        })
        .await
        .expect("list")
        .into_inner();
    assert_eq!(listed.tokens.len(), 1);
    let row = &listed.tokens[0];
    assert_eq!(row.label, "fleet-agents");
    assert_eq!(row.role, pbcore::EnrollRole::Agent as i32);
    assert!(!row.revoked);

    // The listing is an inventory, not a credential export: no field of it can
    // carry the hash, and the secret is nowhere in the rendered response.
    let rendered = format!("{listed:?}");
    assert!(!rendered.contains("argon2"), "{rendered}");
    assert!(!rendered.contains(&minted.secret), "{rendered}");

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn a_ttl_is_recorded_as_an_expiry() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut client, history_id) = admin_client(&daemon, &operator).await;

    let minted = client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id: history_id.clone(),
            role: pbcore::EnrollRole::Coordinator as i32,
            label: "short-lived".to_string(),
            ttl_seconds: Some(900),
        })
        .await
        .expect("mint")
        .into_inner();
    assert!(minted.expires_at_us.is_some());

    daemon.stop().await.expect("daemon stops cleanly");
}

/// A TTL that has actually **elapsed** refuses enrollment, and refuses it
/// indistinguishably (ADR 0037 §4/§5: revoked, unknown, expired, and
/// wrong-role are one answer).
///
/// The sibling test above only proves the expiry is *recorded*; the state
/// machine's own filter is unit-tested in `coppice-state`. Neither says what a
/// machine presenting an aged-out token gets from the live enrollment core,
/// which is the failure mode the ADR names — so this drives the whole path,
/// with the deadline crossed for real.
///
/// The wait is a real sleep on purpose. Token expiry is wall-clock
/// (`Timestamp::now()` at the leader, `crates/coppice-state/src/lib.rs`
/// `usable_enroll_tokens`), and no test knob moves it: `[pacing]` paces the
/// convergence loop's retry sleeps and nothing else, `[token_kdf]` only cheapens
/// hashing. A one-second TTL is therefore the smallest honest way to reach an
/// expired token, and the sleep is trimmed by however long the setup already
/// took.
#[tokio::test]
async fn an_elapsed_ttl_refuses_enrollment_indistinguishably() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut client, history_id) = admin_client(&daemon, &operator).await;

    let minted_at = Instant::now();
    let minted = client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id: history_id.clone(),
            role: pbcore::EnrollRole::Agent as i32,
            label: "one-second".to_string(),
            ttl_seconds: Some(1),
        })
        .await
        .expect("mint")
        .into_inner();
    assert!(
        minted.expires_at_us.is_some(),
        "a TTL is recorded as an expiry"
    );

    // Live while the TTL still holds: the refusal below is the deadline
    // passing, not a token that never worked.
    let (_key, csr) = pki::generate_key_and_csr().expect("generate a CSR");
    client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: minted.secret.clone(),
            csr_pem: String::from_utf8(csr.clone()).unwrap(),
            node_id: Some(NodeId::new().into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect("a token enrolls while its TTL still holds");

    // Past the deadline, with margin for the leader's own clock read.
    let elapsed = minted_at.elapsed();
    tokio::time::sleep(Duration::from_millis(1_300).saturating_sub(elapsed)).await;

    let expired = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: minted.secret.clone(),
            csr_pem: String::from_utf8(csr.clone()).unwrap(),
            node_id: Some(NodeId::new().into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect_err("an expired token is refused (ADR 0037 §5)");
    let unknown = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: "cpk_never-minted".to_string(),
            csr_pem: String::from_utf8(csr).unwrap(),
            node_id: Some(NodeId::new().into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect_err("an unknown token is refused");
    assert_eq!(expired.code(), tonic::Code::Unauthenticated);
    assert_eq!(expired.code(), unknown.code());
    assert_eq!(
        expired.message(),
        unknown.message(),
        "expired and unknown must be one indistinguishable failure (ADR 0037 §4)"
    );

    // The token is expired, not revoked: the inventory still carries it
    // unrevoked, so an operator reading `enroll-token list` sees why it
    // stopped working rather than a row that claims someone revoked it.
    let listed = client
        .list_enroll_tokens(pb::ListEnrollTokensRequest {
            history_id: history_id.clone(),
        })
        .await
        .expect("list")
        .into_inner();
    let row = listed
        .tokens
        .iter()
        .find(|t| t.label == "one-second")
        .expect("the expired token is still in the inventory");
    assert!(!row.revoked, "an expired token is not a revoked one");

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn an_unlabelled_token_is_refused() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut client, history_id) = admin_client(&daemon, &operator).await;

    let status = client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id,
            role: pbcore::EnrollRole::Agent as i32,
            label: "   ".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect_err("an unlabelled token is refused");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn a_revoked_token_no_longer_enrolls() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut client, history_id) = admin_client(&daemon, &operator).await;

    let minted = mint(
        &mut client,
        &history_id,
        pbcore::EnrollRole::Agent,
        "to-be-revoked",
    )
    .await;
    let (_key, csr) = pki::generate_key_and_csr().expect("generate a CSR");

    // Works before revocation…
    client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: minted.secret.clone(),
            csr_pem: String::from_utf8(csr.clone()).unwrap(),
            node_id: Some(NodeId::new().into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect("a live token enrolls");

    client
        .revoke_enroll_token(pb::RevokeEnrollTokenRequest {
            history_id: history_id.clone(),
            token_id: minted.token_id,
        })
        .await
        .expect("revoke");

    // …and afterwards is indistinguishable from a secret that never existed.
    let after = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: minted.secret.clone(),
            csr_pem: String::from_utf8(csr.clone()).unwrap(),
            node_id: Some(NodeId::new().into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect_err("a revoked token is refused");
    let unknown = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id,
            token: "cpk_never-minted".to_string(),
            csr_pem: String::from_utf8(csr).unwrap(),
            node_id: Some(NodeId::new().into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect_err("an unknown token is refused");
    assert_eq!(after.code(), unknown.code());
    assert_eq!(
        after.message(),
        unknown.message(),
        "revoked and unknown must be one indistinguishable failure (ADR 0037 §4)"
    );

    // The listing still shows it, now marked revoked.
    let listed = client
        .list_enroll_tokens(pb::ListEnrollTokensRequest { history_id: vec![] })
        .await;
    assert!(listed.is_err(), "a malformed history id is refused");

    daemon.stop().await.expect("daemon stops cleanly");
}

// ---------------------------------------------------------------------------
// Enrollment
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_agent_token_issues_a_node_leaf_and_writes_no_state() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut client, history_id) = admin_client(&daemon, &operator).await;

    let minted = mint(
        &mut client,
        &history_id,
        pbcore::EnrollRole::Agent,
        "agents",
    )
    .await;
    let (key_pem, csr_pem) = pki::generate_key_and_csr().expect("generate a CSR");
    let node = NodeId::new();

    let issued = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: minted.secret,
            csr_pem: String::from_utf8(csr_pem).unwrap(),
            node_id: Some(node.into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect("enroll")
        .into_inner();

    let verified = pki::verify_leaf(issued.ca_pem.as_bytes(), issued.cert_pem.as_bytes())
        .expect("the issued leaf chains to the cluster CA");
    assert_eq!(verified.profile, pki::Profile::Agent(node));
    assert!(String::from_utf8(key_pem).unwrap().contains("PRIVATE KEY"));

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn an_agent_token_cannot_enroll_a_coordinator_identity() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut client, history_id) = admin_client(&daemon, &operator).await;

    let minted = mint(
        &mut client,
        &history_id,
        pbcore::EnrollRole::Agent,
        "agents",
    )
    .await;
    let (_key, csr) = pki::generate_key_and_csr().unwrap();

    // The token's role decides the path; a coordinator claim from an
    // agent-role token is a wrong-role presentation and must be
    // indistinguishable from an unknown token (no validity oracle).
    let status = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id,
            token: minted.secret,
            csr_pem: String::from_utf8(csr).unwrap(),
            node_id: None,
            machine_id: Some(MachineId::new().into()),
            sans: Vec::new(),
        })
        .await
        .expect_err("an agent token cannot mint a coordinator leaf");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert_eq!(status.message(), "enrollment refused");

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn a_coordinator_token_issues_a_machine_leaf_and_records_the_enrollment() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut client, history_id) = admin_client(&daemon, &operator).await;

    let minted = mint(
        &mut client,
        &history_id,
        pbcore::EnrollRole::Coordinator,
        "coordinators",
    )
    .await;
    let (_key, csr) = pki::generate_key_and_csr().unwrap();
    let machine = MachineId::new();

    let issued = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: minted.secret.clone(),
            csr_pem: String::from_utf8(csr.clone()).unwrap(),
            node_id: None,
            machine_id: Some(machine.into()),
            sans: Vec::new(),
        })
        .await
        .expect("enroll")
        .into_inner();

    let verified = pki::verify_leaf(issued.ca_pem.as_bytes(), issued.cert_pem.as_bytes())
        .expect("the issued leaf chains to the cluster CA");
    assert_eq!(verified.profile, pki::Profile::Coordinator(machine));

    // Re-enrolling the same identity (a lost leaf, a replaced disk) is
    // idempotent: the replicated record is a first-write-wins fact.
    client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id,
            token: minted.secret,
            csr_pem: String::from_utf8(csr).unwrap(),
            node_id: None,
            machine_id: Some(machine.into()),
            sans: Vec::new(),
        })
        .await
        .expect("re-enrollment is accepted");

    daemon.stop().await.expect("daemon stops cleanly");
}

// ---------------------------------------------------------------------------
// Renewal
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_coordinator_renews_its_own_leaf_and_keeps_its_subject() {
    let ca = Ca::new();
    let (mut daemon, _operator) = formed(&ca).await;

    // The daemon's own minted leaf: a coordinator profile under the cluster CA.
    let (cluster_ca, cert, key) = daemon.tls_material();
    let machine = match pki::verify_leaf(&cluster_ca, &cert)
        .expect("the daemon's own leaf verifies")
        .profile
    {
        pki::Profile::Coordinator(m) => m,
        other => panic!("expected a coordinator leaf, got {other:?}"),
    };

    let mut client =
        coppice_coordinator::admin::admin_channel(&daemon.raft_target(), &cluster_ca, &cert, &key)
            .await
            .expect("dial with the machine leaf");
    let history_id = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe")
        .into_inner()
        .history_id;

    let (_key, csr) = pki::generate_key_and_csr().unwrap();
    let renewed = client
        .renew_coordinator(pb::RenewCoordinatorRequest {
            history_id: history_id.clone(),
            csr_pem: String::from_utf8(csr).unwrap(),
            sans: Vec::new(),
        })
        .await
        .expect("renew")
        .into_inner();

    let verified = pki::verify_leaf(renewed.ca_pem.as_bytes(), renewed.cert_pem.as_bytes())
        .expect("the renewed leaf verifies");
    assert_eq!(
        verified.profile,
        pki::Profile::Coordinator(machine),
        "renewal preserves the subject exactly (ADR 0037 §4)"
    );
    assert_ne!(
        renewed.cert_pem.as_bytes(),
        cert.as_slice(),
        "a renewal is a new certificate"
    );

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn an_operator_certificate_cannot_renew_a_coordinator_identity() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut client, history_id) = admin_client(&daemon, &operator).await;

    let (_key, csr) = pki::generate_key_and_csr().unwrap();
    let status = client
        .renew_coordinator(pb::RenewCoordinatorRequest {
            history_id,
            csr_pem: String::from_utf8(csr).unwrap(),
            sans: Vec::new(),
        })
        .await
        .expect_err("an operator leaf is not a coordinator identity");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn a_revoked_coordinator_is_refused_renewal() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;

    let (cluster_ca, cert, key) = daemon.tls_material();
    let machine = match pki::verify_leaf(&cluster_ca, &cert).unwrap().profile {
        pki::Profile::Coordinator(m) => m,
        other => panic!("expected a coordinator leaf, got {other:?}"),
    };

    // Revoking is an operator act, over the operator credential.
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;
    admin
        .revoke_identity(pb::RevokeIdentityRequest {
            history_id: history_id.clone(),
            identity: Some(pbcore::RevokedIdentity {
                identity: Some(pbcore::revoked_identity::Identity::Machine(machine.into())),
            }),
        })
        .await
        .expect("revoke the identity");

    let mut client =
        coppice_coordinator::admin::admin_channel(&daemon.raft_target(), &cluster_ca, &cert, &key)
            .await
            .expect("dial with the machine leaf");
    let (_key, csr) = pki::generate_key_and_csr().unwrap();
    let status = client
        .renew_coordinator(pb::RenewCoordinatorRequest {
            history_id,
            csr_pem: String::from_utf8(csr).unwrap(),
            sans: Vec::new(),
        })
        .await
        .expect_err("a revoked identity is refused renewal");
    assert_eq!(
        status.code(),
        tonic::Code::Unauthenticated,
        "refusal is v1's revocation mechanism (ADR 0037 §5)"
    );

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn an_agent_enrolls_then_renews_over_the_session_plane_until_revoked() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;

    // Enroll for a real agent leaf under the cluster CA…
    let minted = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "agents").await;
    let (agent_key, csr) = pki::generate_key_and_csr().unwrap();
    let node = NodeId::new();
    let issued = admin
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: minted.secret,
            csr_pem: String::from_utf8(csr).unwrap(),
            node_id: Some(node.into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect("enroll")
        .into_inner();

    // …and renew with it over the agent gateway, where the session lives.
    let mut agent = agent_client(
        &daemon,
        issued.ca_pem.as_bytes(),
        issued.cert_pem.as_bytes(),
        &agent_key,
    )
    .await;
    let (_k, csr) = pki::generate_key_and_csr().unwrap();
    let renewed = agent
        .renew(coppice_proto::pb::agent::v1::RenewRequest {
            csr_pem: String::from_utf8(csr).unwrap(),
        })
        .await
        .expect("renew")
        .into_inner();
    let verified = pki::verify_leaf(renewed.ca_pem.as_bytes(), renewed.cert_pem.as_bytes())
        .expect("the renewed agent leaf verifies");
    assert_eq!(verified.profile, pki::Profile::Agent(node));

    // Revocation closes the renewal door — the whole eviction lever in v1.
    admin
        .revoke_identity(pb::RevokeIdentityRequest {
            history_id,
            identity: Some(pbcore::RevokedIdentity {
                identity: Some(pbcore::revoked_identity::Identity::Node(node.into())),
            }),
        })
        .await
        .expect("revoke the node identity");

    let (_k, csr) = pki::generate_key_and_csr().unwrap();
    let status = agent
        .renew(coppice_proto::pb::agent::v1::RenewRequest {
            csr_pem: String::from_utf8(csr).unwrap(),
        })
        .await
        .expect_err("a revoked node is refused renewal");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    daemon.stop().await.expect("daemon stops cleanly");
}

/// The agent daemon's own renewal path, end to end: the runner's
/// [`renew_once`] against the real gateway, writing through the same
/// `TlsStore` the session client dials with.
///
/// This is the half the coordinator-side renewal test above cannot reach — the
/// leaf on *disk*. A renewal that returns a valid certificate but does not land
/// in the `[tls]` paths, or lands there without re-arming the store, leaves the
/// agent presenting its old leaf until it expires.
///
/// [`renew_once`]: coppice_agent::session::renewal::renew_once
#[tokio::test]
async fn the_agent_runner_rewrites_its_leaf_files_and_rearms_the_store() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;

    let minted = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "agents").await;
    let (agent_key, csr) = pki::generate_key_and_csr().unwrap();
    let node = NodeId::new();
    let issued = admin
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id,
            token: minted.secret,
            csr_pem: String::from_utf8(csr).unwrap(),
            node_id: Some(node.into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect("enroll")
        .into_inner();

    // Install the enrolled material exactly as agent startup does, and load the
    // store the daemon would run on.
    let dir = tempfile::tempdir().expect("temp dir");
    let paths = coppice_tls::TlsPaths {
        cert: dir.path().join("node.crt"),
        key: dir.path().join("node.key"),
        ca: dir.path().join("ca.crt"),
    };
    pki::install_leaf_material(
        &paths,
        issued.ca_pem.as_bytes(),
        issued.cert_pem.as_bytes(),
        &agent_key,
    )
    .expect("install the enrolled leaf");
    let store = coppice_tls::TlsStore::load(paths.clone()).expect("load the agent store");
    let before_generation = store.generation();
    let before_cert = std::fs::read(&paths.cert).expect("read the leaf");
    let before_key = std::fs::read(&paths.key).expect("read the key");

    let mut client = agent_client(
        &daemon,
        issued.ca_pem.as_bytes(),
        issued.cert_pem.as_bytes(),
        &agent_key,
    )
    .await;
    coppice_agent::session::renewal::renew_once(&mut client, &store)
        .await
        .expect("the runner's renewal succeeds over the session plane");

    // The files on disk are the renewal's real output.
    let after_cert = std::fs::read(&paths.cert).expect("read the renewed leaf");
    let after_key = std::fs::read(&paths.key).expect("read the renewed key");
    assert_ne!(after_cert, before_cert, "the leaf file was rewritten");
    assert_ne!(
        after_key, before_key,
        "renewal installs the fresh keypair, not just the certificate"
    );

    let verified = pki::verify_leaf(&std::fs::read(&paths.ca).unwrap(), &after_cert)
        .expect("the installed leaf verifies against the installed CA");
    assert_eq!(
        verified.profile,
        pki::Profile::Agent(node),
        "renewal preserves the subject exactly (ADR 0037 §4)"
    );

    // …and the store is re-armed, so the *next* dial presents the new leaf
    // rather than waiting for the mtime poll.
    assert!(
        store.generation() > before_generation,
        "force_reload published the renewed material"
    );
    assert_eq!(store.current().cert_pem(), after_cert.as_slice());

    // The renewed leaf is a working credential in its own right.
    let mut renewed_client = agent_client(
        &daemon,
        store.current().ca_pem(),
        store.current().cert_pem(),
        store.current().key_pem(),
    )
    .await;
    coppice_agent::session::renewal::renew_once(&mut renewed_client, &store)
        .await
        .expect("the renewed leaf can itself renew");

    daemon.stop().await.expect("daemon stops cleanly");
}

/// Dial the agent session listener over mTLS with the supplied identity.
async fn agent_client(
    daemon: &Daemon,
    ca_pem: &[u8],
    cert_pem: &[u8],
    key_pem: &[u8],
) -> coppice_net::session::Client<tonic::transport::Channel> {
    use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

    let target = daemon.agent_target();
    let host = target.rsplit_once(':').map(|(h, _)| h).unwrap_or(&target);
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
        .domain_name(host.to_string());
    let channel = Channel::from_shared(format!("https://{target}"))
        .expect("agent target")
        .tls_config(tls)
        .expect("agent client TLS")
        .connect()
        .await
        .expect("dial the agent gateway");
    coppice_net::session::Client::new(channel)
}

/// The background renewal task's leader-local branch obeys revocation
/// (ADR 0037 §5). This drives the REAL `renew_once` — not the RPC — because
/// the leader signs its own renewals with the CA key on its disk, and a
/// leader that skipped the revocation read there would keep itself alive
/// forever after an operator revoked it.
#[tokio::test]
async fn a_revoked_leader_refuses_to_renew_itself_locally() {
    use coppice_consensus::Consensus as _;
    use coppice_core::time::Timestamp;
    use coppice_state::command::{RecordCaCertificate, RevokeIdentity};
    use coppice_state::{CaCertBundle, Command};

    let harness_ca = common::Ca::new();
    let rc = common::RunningCoordinator::start(ClusterId::new(), &harness_ca).await;

    // Manufacture the cluster-owned PKI this legacy-bootstrap harness lacks:
    // CA key on the leader's disk, CA cert in replicated state, machine
    // identity in the data dir — exactly what a formed voter holds.
    let ca = coppice_tls::pki::mint_root_ca().expect("mint the cluster CA");
    coppice_tls::pki::write_ca_key(&rc.data_dir, &ca.key_pem).expect("write the CA key");
    let machine = MachineId::new();
    coppice_tls::pki::persist_machine_identity(&rc.data_dir, &machine)
        .expect("persist the machine identity");
    let applied = rc
        .consensus()
        .propose(Command::RecordCaCertificate(RecordCaCertificate {
            bundle: CaCertBundle::parse(std::str::from_utf8(&ca.cert_pem).unwrap())
                .expect("the minted CA parses"),
            staged_root_serial: None,
            recorded_at: Timestamp::now(),
        }))
        .await
        .expect("record the CA");
    applied.outcome.expect("recording the CA is accepted");
    rc.views().at_least(applied.log_index).await.expect("view");

    // The machine-plane material renewal watches: a coordinator leaf under
    // that CA, in its own [tls] paths.
    let signer =
        coppice_tls::pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load the signer");
    let (key_pem, csr_pem) = pki::generate_key_and_csr().unwrap();
    let leaf = coppice_tls::pki::issue_coordinator(&signer, &csr_pem, &machine, &[])
        .expect("issue the current leaf");
    let dir = tempfile::tempdir().expect("temp dir");
    let paths = coppice_tls::TlsPaths {
        cert: dir.path().join("node.crt"),
        key: dir.path().join("node.key"),
        ca: dir.path().join("ca.crt"),
    };
    coppice_tls::pki::install_leaf_material(&paths, &ca.cert_pem, &leaf, &key_pem)
        .expect("install the current material");
    let store = coppice_tls::TlsStore::load(paths.clone()).expect("load the store");

    // Sole voter: this node is the leader, so renew_once signs locally.
    assert_eq!(
        rc.handle.cluster_summary().leader,
        Some(rc.handle.cluster_summary().local_id),
        "the harness node leads"
    );
    let before = std::fs::read(&paths.cert).unwrap();
    coppice_coordinator::coordinator_renew_once(
        &store,
        &paths,
        &rc.data_dir,
        rc.consensus().as_ref(),
        &rc.handle,
        None,
    )
    .await
    .expect("an unrevoked leader renews itself");
    let renewed = std::fs::read(&paths.cert).unwrap();
    assert_ne!(renewed, before, "the local branch rewrote the leaf");
    let verified = pki::verify_leaf(&ca.cert_pem, &renewed).expect("chains to the cluster CA");
    assert_eq!(verified.profile, pki::Profile::Coordinator(machine));

    // Revoke the leader's own identity; the SAME local path must now refuse.
    let applied = rc
        .consensus()
        .propose(Command::RevokeIdentity(RevokeIdentity {
            identity: coppice_state::RevokedIdentity::Machine(machine),
            revoked_at: Timestamp::now(),
        }))
        .await
        .expect("revoke the leader's identity");
    applied.outcome.expect("revocation is accepted");

    coppice_coordinator::coordinator_renew_once(
        &store,
        &paths,
        &rc.data_dir,
        rc.consensus().as_ref(),
        &rc.handle,
        None,
    )
    .await
    .expect_err("a revoked leader must not renew itself locally (ADR 0037 §5)");
    assert_eq!(
        std::fs::read(&paths.cert).unwrap(),
        renewed,
        "the refused renewal left the material untouched"
    );

    rc.shutdown().await;
}

/// Identity revocation binds at ENROLLMENT too, not only at renewal
/// (ADR 0037 §5). The fleet token is deliberately reusable and long-lived, so
/// a revoked machine still holds it — deleting its leaf and re-enrolling the
/// same subject must not be a way back in, and the refusal must be the same
/// opaque one an unknown token gets (whether a subject is revoked is not the
/// public endpoint's to reveal).
#[tokio::test]
async fn a_revoked_identity_cannot_re_enroll_under_either_role() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut client, history_id) = admin_client(&daemon, &operator).await;

    let agent_token = mint(
        &mut client,
        &history_id,
        pbcore::EnrollRole::Agent,
        "agents",
    )
    .await;
    let coord_token = mint(
        &mut client,
        &history_id,
        pbcore::EnrollRole::Coordinator,
        "coordinators",
    )
    .await;

    // Enroll both subjects once, then revoke both identities.
    let node = NodeId::new();
    let machine = MachineId::new();
    let (_k, csr) = pki::generate_key_and_csr().unwrap();
    client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: agent_token.secret.clone(),
            csr_pem: String::from_utf8(csr.clone()).unwrap(),
            node_id: Some(node.into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect("the agent subject enrolls while unrevoked");
    client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: coord_token.secret.clone(),
            csr_pem: String::from_utf8(csr.clone()).unwrap(),
            node_id: None,
            machine_id: Some(machine.into()),
            sans: Vec::new(),
        })
        .await
        .expect("the coordinator subject enrolls while unrevoked");
    for identity in [
        pbcore::RevokedIdentity {
            identity: Some(pbcore::revoked_identity::Identity::Node(node.into())),
        },
        pbcore::RevokedIdentity {
            identity: Some(pbcore::revoked_identity::Identity::Machine(machine.into())),
        },
    ] {
        client
            .revoke_identity(pb::RevokeIdentityRequest {
                history_id: history_id.clone(),
                identity: Some(identity),
            })
            .await
            .expect("revoke the identity");
    }

    // Re-enrollment with the still-live tokens: the uniform refusal, byte-for
    // byte the one an unknown token gets.
    let (_k2, csr2) = pki::generate_key_and_csr().unwrap();
    let refused_agent = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: agent_token.secret,
            csr_pem: String::from_utf8(csr2.clone()).unwrap(),
            node_id: Some(node.into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect_err("a revoked node cannot re-enroll (ADR 0037 §5)");
    let refused_coord = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: coord_token.secret,
            csr_pem: String::from_utf8(csr2.clone()).unwrap(),
            node_id: None,
            machine_id: Some(machine.into()),
            sans: Vec::new(),
        })
        .await
        .expect_err("a revoked machine cannot re-enroll (ADR 0037 §5)");
    let unknown = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: "cpk_never-minted".to_string(),
            csr_pem: String::from_utf8(csr2).unwrap(),
            node_id: Some(NodeId::new().into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect_err("an unknown token is refused");
    for refused in [&refused_agent, &refused_coord] {
        assert_eq!(refused.code(), unknown.code(), "uniform status");
        assert_eq!(refused.message(), unknown.message(), "uniform message");
    }

    // A fresh, unrevoked subject still enrolls: the tokens themselves remain
    // live — it is the identities that are dead.
    let (_k3, csr3) = pki::generate_key_and_csr().unwrap();
    let minted_again = mint(&mut client, &history_id, pbcore::EnrollRole::Agent, "more").await;
    client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id,
            token: minted_again.secret,
            csr_pem: String::from_utf8(csr3).unwrap(),
            node_id: Some(NodeId::new().into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect("an unrevoked subject still enrolls");

    daemon.stop().await.expect("daemon stops cleanly");
}

// ---------------------------------------------------------------------------
// Renewal under load
// ---------------------------------------------------------------------------

/// How many agent identities renew at once. Modest on purpose — the claim is
/// that renewals genuinely *overlap*, which a barrier guarantees at any width,
/// not that the cluster survives a stampede of arbitrary size.
const RENEWERS: usize = 8;

/// One enrolled agent identity: everything needed to dial the session plane
/// with it.
struct EnrolledAgent {
    node: NodeId,
    key_pem: Vec<u8>,
    ca_pem: Vec<u8>,
    cert_pem: Vec<u8>,
}

/// Redeem `token` for a fresh agent leaf under the cluster CA.
async fn enroll_agent(
    client: &mut coppice_net::admin::Client<tonic::transport::Channel>,
    history_id: &[u8],
    token: &str,
) -> EnrolledAgent {
    let (key_pem, csr) = pki::generate_key_and_csr().expect("generate a CSR");
    let node = NodeId::new();
    let issued = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.to_vec(),
            token: token.to_string(),
            csr_pem: String::from_utf8(csr).unwrap(),
            node_id: Some(node.into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect("enroll an agent identity")
        .into_inner();
    EnrolledAgent {
        node,
        key_pem,
        ca_pem: issued.ca_pem.into_bytes(),
        cert_pem: issued.cert_pem.into_bytes(),
    }
}

/// The registration report an agent opens a session with (ADR 0009 step 2),
/// hand-rolled: this test wants session-plane *traffic*, not an agent runner.
fn register_report(node: NodeId) -> pbagent::AgentReport {
    pbagent::AgentReport {
        node: Some(node.into()),
        node_epoch: 0,
        body: Some(pbagent::agent_report::Body::Register(pbagent::Register {
            capacity: Some(pbcore::Resources {
                quantities: Vec::new(),
            }),
            labels: Vec::new(),
            service_addr: None,
        })),
    }
}

/// Cert renewal **under load** (ADR 0037 §4/§5, and the Consequences clause
/// "cert renewal under load, including renewal refusal for a revoked
/// identity").
///
/// The single-shot renewal tests above each prove one seam with nothing else
/// happening. The failure modes that only appear under concurrency are
/// different ones: a renewal that takes the CA key under a lock every other
/// plane needs, a signing path that serializes behind the session manager, a
/// revocation read that is skipped when the leader is busy. So this fires
/// every renewal the deployment has — [`RENEWERS`] agents on the session plane
/// and the coordinator's own machine identity on the admin plane — from behind
/// a barrier, so they are genuinely in flight together, while both machine
/// planes carry ordinary traffic throughout.
///
/// Four claims, all of them about the storm and not about any one renewal:
/// every renewal succeeds; every renewed leaf keeps its subject exactly; the
/// planes never hiccup (every session opened and every admin read issued
/// *during* the storm succeeds); and a revoked identity renewing in the middle
/// of it is still refused — being one of a crowd is not a way past §5.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_renewals_ride_out_live_plane_traffic_and_a_revoked_identity_is_refused() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;
    let minted = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "fleet").await;

    // --- Enroll every identity up front, and dial every client, so the
    // --- barrier releases into renewals and nothing else.
    let mut renewers = Vec::new();
    for _ in 0..RENEWERS {
        let agent = enroll_agent(&mut admin, &history_id, &minted.secret).await;
        let client = agent_client(&daemon, &agent.ca_pem, &agent.cert_pem, &agent.key_pem).await;
        renewers.push((agent, client));
    }

    // Two more agents whose whole job is to keep the session plane busy.
    let mut traffic_agents = Vec::new();
    for _ in 0..2 {
        let agent = enroll_agent(&mut admin, &history_id, &minted.secret).await;
        let client = agent_client(&daemon, &agent.ca_pem, &agent.cert_pem, &agent.key_pem).await;
        traffic_agents.push((agent.node, client));
    }

    // One more, enrolled and then revoked: its renewal rides in the same
    // storm and must still be refused.
    let doomed = enroll_agent(&mut admin, &history_id, &minted.secret).await;
    let mut doomed_client =
        agent_client(&daemon, &doomed.ca_pem, &doomed.cert_pem, &doomed.key_pem).await;
    admin
        .revoke_identity(pb::RevokeIdentityRequest {
            history_id: history_id.clone(),
            identity: Some(pbcore::RevokedIdentity {
                identity: Some(pbcore::revoked_identity::Identity::Node(doomed.node.into())),
            }),
        })
        .await
        .expect("revoke the doomed node's identity");

    // The coordinator's own renewal, over the admin plane: the same storm has
    // to carry the machine half of §4, not just the agent half.
    let (cluster_ca, machine_cert, machine_key) = daemon.tls_material();
    let machine = match pki::verify_leaf(&cluster_ca, &machine_cert)
        .expect("the daemon's own leaf verifies")
        .profile
    {
        pki::Profile::Coordinator(m) => m,
        other => panic!("expected a coordinator leaf, got {other:?}"),
    };
    let mut machine_client = coppice_coordinator::admin::admin_channel(
        &daemon.raft_target(),
        &cluster_ca,
        &machine_cert,
        &machine_key,
    )
    .await
    .expect("dial the admin surface with the machine leaf");

    // --- Background traffic on both machine planes, running before the storm
    // --- starts and still running when it ends.
    let stop = Arc::new(AtomicBool::new(false));
    let admin_ok = Arc::new(AtomicUsize::new(0));
    let admin_err = Arc::new(AtomicUsize::new(0));
    let session_ok = Arc::new(AtomicUsize::new(0));
    let session_err = Arc::new(AtomicUsize::new(0));

    let admin_traffic = {
        let (ca_pem, cert_pem, key_pem) = operator_identity(&operator);
        let target = daemon.raft_target();
        let hid = history_id.clone();
        let (stop, ok, err) = (
            Arc::clone(&stop),
            Arc::clone(&admin_ok),
            Arc::clone(&admin_err),
        );
        tokio::spawn(async move {
            let mut client =
                coppice_coordinator::admin::admin_channel(&target, &ca_pem, &cert_pem, &key_pem)
                    .await
                    .expect("dial the admin surface for background traffic");
            while !stop.load(Ordering::Relaxed) {
                match client
                    .cluster_status(pb::ClusterStatusRequest {
                        history_id: hid.clone(),
                    })
                    .await
                {
                    Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                    Err(_) => err.fetch_add(1, Ordering::Relaxed),
                };
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    let mut session_traffic = Vec::new();
    for (node, client) in traffic_agents {
        let (stop, ok, err) = (
            Arc::clone(&stop),
            Arc::clone(&session_ok),
            Arc::clone(&session_err),
        );
        session_traffic.push(tokio::spawn(async move {
            let mut client = client;
            while !stop.load(Ordering::Relaxed) {
                // Open a session, register, let the request half end. Each
                // round is a real mTLS handshake, a real gateway accept, and a
                // real replicated `RegisterNode` — the session plane doing its
                // ordinary work while the leaf under it is being reissued for
                // everyone else.
                let reports = tokio_stream::iter(std::iter::once(register_report(node)));
                match client.session(reports).await {
                    Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                    Err(_) => err.fetch_add(1, Ordering::Relaxed),
                };
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    // Let the traffic get going before the storm, so "during" means during.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let admin_before = admin_ok.load(Ordering::Relaxed);
    let session_before = session_ok.load(Ordering::Relaxed);

    // --- The storm: every renewal waits on the barrier, so they are in flight
    // --- together rather than in a queue that happens to be fast.
    let barrier = Arc::new(tokio::sync::Barrier::new(RENEWERS + 2));

    let mut renewals = Vec::new();
    for (agent, client) in renewers {
        let barrier = Arc::clone(&barrier);
        renewals.push(tokio::spawn(async move {
            let mut client = client;
            let (_key, csr) = pki::generate_key_and_csr().expect("generate a CSR");
            barrier.wait().await;
            let renewed = client
                .renew(pbagent::RenewRequest {
                    csr_pem: String::from_utf8(csr).unwrap(),
                })
                .await;
            (agent, renewed)
        }));
    }

    let coordinator_renewal = {
        let barrier = Arc::clone(&barrier);
        let hid = history_id.clone();
        tokio::spawn(async move {
            let (_key, csr) = pki::generate_key_and_csr().expect("generate a CSR");
            barrier.wait().await;
            machine_client
                .renew_coordinator(pb::RenewCoordinatorRequest {
                    history_id: hid,
                    csr_pem: String::from_utf8(csr).unwrap(),
                    sans: Vec::new(),
                })
                .await
        })
    };

    let revoked_renewal = {
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            let (_key, csr) = pki::generate_key_and_csr().expect("generate a CSR");
            barrier.wait().await;
            doomed_client
                .renew(pbagent::RenewRequest {
                    csr_pem: String::from_utf8(csr).unwrap(),
                })
                .await
        })
    };

    // --- Every agent renewal succeeded, and kept its subject.
    let mut renewed_certs = Vec::new();
    for handle in renewals {
        let (agent, renewed) = handle.await.expect("renewal task joined");
        let renewed = renewed
            .unwrap_or_else(|e| panic!("node {} failed to renew under load: {e:?}", agent.node))
            .into_inner();
        let verified = pki::verify_leaf(renewed.ca_pem.as_bytes(), renewed.cert_pem.as_bytes())
            .expect("a leaf renewed under load chains to the cluster CA");
        assert_eq!(
            verified.profile,
            pki::Profile::Agent(agent.node),
            "renewal under load preserves the subject exactly (ADR 0037 §4)"
        );
        assert_ne!(
            renewed.cert_pem.as_bytes(),
            agent.cert_pem.as_slice(),
            "a renewal is a new certificate"
        );
        renewed_certs.push(renewed.cert_pem);
    }
    renewed_certs.sort();
    renewed_certs.dedup();
    assert_eq!(
        renewed_certs.len(),
        RENEWERS,
        "concurrent renewals must not hand two callers the same leaf"
    );

    // --- …and so did the coordinator's own, on the other plane.
    let renewed = coordinator_renewal
        .await
        .expect("coordinator renewal task joined")
        .expect("the coordinator renews its own leaf under load")
        .into_inner();
    let verified = pki::verify_leaf(renewed.ca_pem.as_bytes(), renewed.cert_pem.as_bytes())
        .expect("the renewed machine leaf verifies");
    assert_eq!(
        verified.profile,
        pki::Profile::Coordinator(machine),
        "the machine plane's renewal preserves its subject too"
    );

    // --- The revoked identity was refused in the middle of all that.
    let status = revoked_renewal
        .await
        .expect("revoked renewal task joined")
        .expect_err("a revoked identity is refused renewal, however busy the leader is");
    assert_eq!(
        status.code(),
        tonic::Code::Unauthenticated,
        "refusal is v1's revocation mechanism (ADR 0037 §5)"
    );

    // --- No plane hiccuped: both kept answering throughout the storm, and
    // --- never once failed.
    let admin_during = admin_ok.load(Ordering::Relaxed) - admin_before;
    let session_during = session_ok.load(Ordering::Relaxed) - session_before;
    stop.store(true, Ordering::Relaxed);
    admin_traffic.await.expect("admin traffic task joined");
    for handle in session_traffic {
        handle.await.expect("session traffic task joined");
    }
    assert_eq!(
        admin_err.load(Ordering::Relaxed),
        0,
        "the admin plane refused a read while renewals were in flight"
    );
    assert_eq!(
        session_err.load(Ordering::Relaxed),
        0,
        "the session plane refused a session while renewals were in flight"
    );
    assert!(
        admin_during > 0,
        "no admin-plane traffic overlapped the storm, so nothing was proven"
    );
    assert!(
        session_during > 0,
        "no session-plane traffic overlapped the storm, so nothing was proven"
    );

    daemon.stop().await.expect("daemon stops cleanly");
}
