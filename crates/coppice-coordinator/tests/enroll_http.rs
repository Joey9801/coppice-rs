//! `POST /api/v1/enroll` over the real client listener (ADR 0037 §4).
//!
//! The sibling `enrollment.rs` drives the enrollment *core* through the
//! internal `ForwardEnroll` RPC; this drives the public route a machine with
//! nothing but a token and an address actually calls — over a real socket,
//! against a real formed cluster, through the real router.
//!
//! Everything here runs against a **certless** daemon (the §4 minimal
//! deployment), so the CA every issued leaf chains to is the cluster's own.

mod common;

use std::time::Duration;

use coppice_coordinator::config::CliOverrides;
use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::id::{ClusterId, MachineId, NodeId};
use coppice_proto::pb::core::v1 as pbcore;
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::pki;

use common::{Ca, Daemon};

const PARKED: CliOverrides = CliOverrides {
    bootstrap: false,
    join: false,
};

/// A formed, certless single-node cluster and the operator credential `init`
/// printed.
async fn formed(ca: &Ca) -> (Daemon, OperatorPem) {
    let mut daemon = Daemon::new_certless(ClusterId::new(), ca);
    daemon.start(PARKED);
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

async fn admin_client(
    daemon: &Daemon,
    operator: &OperatorPem,
) -> (
    coppice_net::admin::Client<tonic::transport::Channel>,
    Vec<u8>,
) {
    let key = operator
        .key_pem
        .as_ref()
        .expect("no CSR was supplied, so the cluster minted the keypair");
    let mut client = coppice_coordinator::admin::admin_channel(
        &daemon.raft_target(),
        operator.ca_pem.as_bytes(),
        operator.cert_pem.as_bytes(),
        key.as_bytes(),
    )
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

/// An HTTP client that **never follows a redirect**: a client carrying an
/// enrollment token must not be told to re-send it somewhere else, and the
/// tests below assert that by refusing to do it.
fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(20))
        .build()
        .expect("build the enrollment http client")
}

/// A response's status and body, after asserting the invariants that hold for
/// every answer this route gives.
async fn checked(response: reqwest::Response) -> (u16, Vec<u8>) {
    let status = response.status();
    assert!(
        !status.is_redirection(),
        "a token-carrying client is never redirected (ADR 0037 §4); got {status}"
    );
    assert!(
        response
            .headers()
            .get(reqwest::header::SET_COOKIE)
            .is_none(),
        "the enrollment route never sets a cookie"
    );
    assert!(
        !response
            .headers()
            .keys()
            .any(|k| k.as_str().starts_with("access-control-")),
        "the enrollment route never emits CORS headers: {:?}",
        response.headers()
    );
    let bytes = response.bytes().await.expect("read the body").to_vec();
    (status.as_u16(), bytes)
}

fn enroll_body(csr_pem: &[u8], node: Option<NodeId>, machine: Option<MachineId>) -> String {
    serde_json::to_string(&coppice_enroll::EnrollRequest {
        csr_pem: String::from_utf8(csr_pem.to_vec()).expect("PEM is UTF-8"),
        token: None,
        node_id: node,
        machine_id: machine,
    })
    .expect("serialize the enrollment request")
}

#[tokio::test]
async fn an_agent_token_issues_a_node_leaf_over_http() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;
    let minted = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "agents").await;

    let (key_pem, csr_pem) = pki::generate_key_and_csr().expect("generate a CSR");
    let node = NodeId::new();
    let response = http()
        .post(daemon.api(coppice_enroll::ENROLL_PATH))
        .bearer_auth(&minted.secret)
        .body(enroll_body(&csr_pem, Some(node), None))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .send()
        .await
        .expect("POST /enroll");

    let (status, body) = checked(response).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let issued: coppice_enroll::EnrollResponse =
        serde_json::from_slice(&body).expect("the success body is the shared DTO");

    let verified = pki::verify_leaf(issued.ca_pem.as_bytes(), issued.cert_pem.as_bytes())
        .expect("the issued leaf chains to the cluster CA it came with");
    assert_eq!(verified.profile, pki::Profile::Agent(node));
    assert!(String::from_utf8(key_pem).unwrap().contains("PRIVATE KEY"));

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn a_coordinator_token_issues_a_machine_leaf_over_http() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;
    let minted = mint(
        &mut admin,
        &history_id,
        pbcore::EnrollRole::Coordinator,
        "coordinators",
    )
    .await;

    let (_key, csr_pem) = pki::generate_key_and_csr().expect("generate a CSR");
    let machine = MachineId::new();
    let response = http()
        .post(daemon.api(coppice_enroll::ENROLL_PATH))
        .bearer_auth(&minted.secret)
        .body(enroll_body(&csr_pem, None, Some(machine)))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .send()
        .await
        .expect("POST /enroll");

    let (status, body) = checked(response).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let issued: coppice_enroll::EnrollResponse = serde_json::from_slice(&body).unwrap();
    let verified = pki::verify_leaf(issued.ca_pem.as_bytes(), issued.cert_pem.as_bytes())
        .expect("the issued leaf chains to the cluster CA");
    assert_eq!(verified.profile, pki::Profile::Coordinator(machine));

    // The coordinator arm writes a replicated fact; re-enrolling the same
    // identity (a lost leaf, a replaced disk) re-applies it as a no-op rather
    // than failing, which is what makes recovery a retry.
    let again = http()
        .post(daemon.api(coppice_enroll::ENROLL_PATH))
        .bearer_auth(&minted.secret)
        .body(enroll_body(&csr_pem, None, Some(machine)))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .send()
        .await
        .expect("POST /enroll");
    assert_eq!(checked(again).await.0, 200);

    daemon.stop().await.expect("daemon stops cleanly");
}

/// The body field is the redacted alternative to the header (ADR 0037 §4), and
/// a token in a query parameter is not a credential at all — the route never
/// looks there.
#[tokio::test]
async fn the_body_token_works_and_a_query_parameter_never_does() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;
    let minted = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "agents").await;

    let (_key, csr_pem) = pki::generate_key_and_csr().unwrap();
    let body = serde_json::to_string(&coppice_enroll::EnrollRequest {
        csr_pem: String::from_utf8(csr_pem.clone()).unwrap(),
        token: Some(minted.secret.clone()),
        node_id: Some(NodeId::new()),
        machine_id: None,
    })
    .unwrap();
    let response = http()
        .post(daemon.api(coppice_enroll::ENROLL_PATH))
        .body(body)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .send()
        .await
        .expect("POST /enroll");
    assert_eq!(checked(response).await.0, 200);

    let via_query = http()
        .post(format!(
            "{}?token={}",
            daemon.api(coppice_enroll::ENROLL_PATH),
            minted.secret
        ))
        .body(enroll_body(&csr_pem, Some(NodeId::new()), None))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .send()
        .await
        .expect("POST /enroll");
    assert_eq!(
        checked(via_query).await.0,
        401,
        "a query parameter is never read as a credential"
    );

    daemon.stop().await.expect("daemon stops cleanly");
}

/// Unknown, revoked, wrong-role, and absent credentials are one response —
/// same status, same bytes. Anything else would be a validity oracle on a
/// public endpoint.
#[tokio::test]
async fn every_authentication_failure_is_one_indistinguishable_response() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;

    let agent_token = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "agents").await;
    let doomed = mint(
        &mut admin,
        &history_id,
        pbcore::EnrollRole::Agent,
        "to-be-revoked",
    )
    .await;
    admin
        .revoke_enroll_token(pb::RevokeEnrollTokenRequest {
            history_id: history_id.clone(),
            token_id: doomed.token_id,
        })
        .await
        .expect("revoke");

    let (_key, csr_pem) = pki::generate_key_and_csr().unwrap();
    let url = daemon.api(coppice_enroll::ENROLL_PATH);

    let mut answers = Vec::new();
    for (label, token, body) in [
        (
            "unknown",
            Some("cpk_never-minted".to_string()),
            enroll_body(&csr_pem, Some(NodeId::new()), None),
        ),
        (
            "revoked",
            Some(doomed.secret.clone()),
            enroll_body(&csr_pem, Some(NodeId::new()), None),
        ),
        (
            // A live agent token presented for a coordinator identity.
            "wrong role",
            Some(agent_token.secret.clone()),
            enroll_body(&csr_pem, None, Some(MachineId::new())),
        ),
        (
            "absent",
            None,
            enroll_body(&csr_pem, Some(NodeId::new()), None),
        ),
    ] {
        let mut request = http()
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let answer = checked(request.send().await.expect("POST /enroll")).await;
        assert_eq!(answer.0, 401, "{label} must be refused");
        answers.push((label, answer));
    }

    let (_, first) = &answers[0].clone();
    for (label, answer) in &answers {
        assert_eq!(
            answer, first,
            "{label} must be byte-identical to the other refusals (ADR 0037 §4)"
        );
    }

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn an_oversized_request_is_refused_without_reaching_the_credential_check() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;
    let minted = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "agents").await;

    let huge = format!(
        r#"{{"csr_pem":"{}"}}"#,
        "A".repeat(coppice_api::http::MAX_ENROLL_BODY + 1)
    );
    let response = http()
        .post(daemon.api(coppice_enroll::ENROLL_PATH))
        .bearer_auth(&minted.secret)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(huge)
        .send()
        .await
        .expect("POST /enroll");
    let (status, _) = checked(response).await;
    assert_eq!(status, 413);

    daemon.stop().await.expect("daemon stops cleanly");
}

/// The follower half of the route (ADR 0037 §4): a replica that is not the
/// leader proxies the request over the mTLS admin channel and returns the
/// leader's leaf — it never redirects the enrolling machine.
///
/// The proxy hop under test is the production one
/// ([`coppice_coordinator::proxying_enroll_endpoint`], which
/// `EnrollService::issue` calls on a follower); it is driven here against a
/// real leader over a real client listener, with the leader's address supplied
/// directly rather than resolved from a membership view.
#[tokio::test]
async fn a_follower_proxies_to_the_leader_and_never_redirects() {
    use std::sync::Arc;

    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;
    let minted = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "agents").await;

    // The "follower" speaks the machine plane with a cluster-issued coordinator
    // identity, exactly as a real one would — enrolled here for the purpose.
    let (machine_key, machine_csr) = pki::generate_key_and_csr().unwrap();
    let machine = MachineId::new();
    let coordinator_token = mint(
        &mut admin,
        &history_id,
        pbcore::EnrollRole::Coordinator,
        "the-follower",
    )
    .await;
    let issued = admin
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: coordinator_token.secret,
            csr_pem: String::from_utf8(machine_csr).unwrap(),
            node_id: None,
            machine_id: Some(machine.into()),
        })
        .await
        .expect("enroll the follower's own identity")
        .into_inner();

    let store = common::tls_store_from_pem(
        issued.ca_pem.as_bytes(),
        issued.cert_pem.as_bytes(),
        &machine_key,
    );
    let history: [u8; 16] = history_id.as_slice().try_into().expect("16-byte history");
    let endpoint = coppice_coordinator::proxying_enroll_endpoint(
        daemon.raft_target(),
        history,
        Arc::clone(&store),
    );

    // A second client listener, serving the same router the follower would.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the follower's client listener");
    let addr = listener.local_addr().expect("local addr");
    let router = coppice_api::http::router(
        Arc::new(common::NoopPlane),
        coppice_api::http::MetricsEndpoint::detached_for_tests(),
        coppice_api::http::ReadyzEndpoint::detached_for_tests(),
        endpoint,
    );
    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let serving = tokio::spawn(async move {
        coppice_coordinator::clientedge::serve(listener, router, None, stop_rx).await;
    });

    let (_key, csr_pem) = pki::generate_key_and_csr().unwrap();
    let node = NodeId::new();
    let response = http()
        .post(format!("http://{addr}{}", coppice_enroll::ENROLL_PATH))
        .bearer_auth(&minted.secret)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(enroll_body(&csr_pem, Some(node), None))
        .send()
        .await
        .expect("POST /enroll to the follower");

    let (status, body) = checked(response).await;
    assert_eq!(
        status,
        200,
        "the follower answers with the leader's leaf: {}",
        String::from_utf8_lossy(&body)
    );
    let proxied: coppice_enroll::EnrollResponse = serde_json::from_slice(&body).unwrap();
    let verified = pki::verify_leaf(proxied.ca_pem.as_bytes(), proxied.cert_pem.as_bytes())
        .expect("the proxied leaf chains to the cluster CA");
    assert_eq!(verified.profile, pki::Profile::Agent(node));

    // A refusal proxies just as opaquely as it would direct.
    let refused = http()
        .post(format!("http://{addr}{}", coppice_enroll::ENROLL_PATH))
        .bearer_auth("cpk_never-minted")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(enroll_body(&csr_pem, Some(NodeId::new()), None))
        .send()
        .await
        .expect("POST /enroll to the follower");
    assert_eq!(checked(refused).await.0, 401);

    let _ = stop.send(true);
    let _ = serving.await;
    daemon.stop().await.expect("daemon stops cleanly");
}

/// The whole §8 agent story through production code only: `ensure_enrolled`
/// (the library agent startup calls) against the real `/api/v1/enroll` route,
/// the installed leaf dialling the real gateway, and the runner's renewal
/// rewriting it — token + address in, renewing machine credential out.
///
/// The sibling tests each prove one seam with the harness filling in the
/// rest; this one lets the production client do everything, so a drift
/// between what `ensure_enrolled` sends and what the route serves cannot
/// pass unnoticed.
#[tokio::test]
async fn an_agent_enrolls_over_http_registers_and_renews_with_production_code_only() {
    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;
    let minted = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "agents").await;

    let dir = tempfile::tempdir().expect("temp dir");
    let paths = coppice_tls::TlsPaths {
        cert: dir.path().join("node.crt"),
        key: dir.path().join("node.key"),
        ca: dir.path().join("ca.crt"),
    };
    let config = coppice_enroll::EnrollmentConfig {
        endpoint: format!("http://{}", daemon.client_addr()),
        token: Some(coppice_enroll::Secret::new(minted.secret)),
        token_path: None,
        insecure: true,
    };
    config.validate().expect("a valid dev/test posture");

    let node = NodeId::new();
    let outcome =
        coppice_enroll::ensure_enrolled(&paths, &config, coppice_enroll::Claim::Node(node))
            .await
            .expect("enroll over the public route");
    assert_eq!(outcome, coppice_enroll::Outcome::Enrolled);

    let leaf = std::fs::read(&paths.cert).expect("the leaf landed in the [tls] paths");
    let verified = pki::verify_leaf(&std::fs::read(&paths.ca).unwrap(), &leaf)
        .expect("the installed leaf chains to the cluster CA");
    assert_eq!(verified.profile, pki::Profile::Agent(node));

    // Idempotent: a restart with a usable leaf makes no network call.
    let again = coppice_enroll::ensure_enrolled(&paths, &config, coppice_enroll::Claim::Node(node))
        .await
        .expect("second startup");
    assert_eq!(again, coppice_enroll::Outcome::AlreadyEnrolled);

    // The enrolled material is a working gateway credential (registration's
    // CN check accepts it), and the runner's renewal rewrites it in place.
    let store = coppice_tls::TlsStore::load(paths.clone()).expect("load the agent store");
    let mut session = {
        use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
        let target = daemon.agent_target();
        let host = target.rsplit_once(':').map(|(h, _)| h).unwrap_or(&target);
        let material = store.current();
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(material.ca_pem()))
            .identity(Identity::from_pem(material.cert_pem(), material.key_pem()))
            .domain_name(host.to_string());
        let channel = Channel::from_shared(format!("https://{target}"))
            .expect("agent target")
            .tls_config(tls)
            .expect("agent client TLS")
            .connect()
            .await
            .expect("dial the agent gateway with the enrolled leaf");
        coppice_net::session::Client::new(channel)
    };
    coppice_agent::session::renewal::renew_once(&mut session, &store)
        .await
        .expect("the enrolled leaf renews over the session plane");
    let renewed = std::fs::read(&paths.cert).expect("read the renewed leaf");
    assert_ne!(renewed, leaf, "renewal rewrote the leaf on disk");
    let verified = pki::verify_leaf(&std::fs::read(&paths.ca).unwrap(), &renewed)
        .expect("the renewed leaf chains to the cluster CA");
    assert_eq!(
        verified.profile,
        pki::Profile::Agent(node),
        "renewal preserves the subject exactly (ADR 0037 §4)"
    );

    // Restart after expiry: an expired leaf cannot open the mTLS session
    // renewal rides on, so startup must treat it as unusable and go back
    // through enrollment — the only path that works without a live credential.
    let expired = {
        let key = rcgen::KeyPair::generate().expect("keypair");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, node.to_string());
        params.not_before = rcgen::date_time_ymd(2020, 1, 1);
        params.not_after = rcgen::date_time_ymd(2020, 1, 2);
        params.self_signed(&key).expect("self-sign").pem()
    };
    std::fs::write(&paths.cert, expired).expect("simulate a leaf that expired while down");
    let after_expiry =
        coppice_enroll::ensure_enrolled(&paths, &config, coppice_enroll::Claim::Node(node))
            .await
            .expect("restart after expiry re-enrolls");
    assert_eq!(
        after_expiry,
        coppice_enroll::Outcome::Enrolled,
        "an expired leaf is not 'already enrolled'"
    );
    let recovered = std::fs::read(&paths.cert).expect("read the re-enrolled leaf");
    let verified = pki::verify_leaf(&std::fs::read(&paths.ca).unwrap(), &recovered)
        .expect("the re-enrolled leaf chains to the cluster CA");
    assert_eq!(verified.profile, pki::Profile::Agent(node));

    daemon.stop().await.expect("daemon stops cleanly");
}
