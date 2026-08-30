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

use std::time::{Duration, Instant};

use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::id::{ClusterId, MachineId, NodeId};
use coppice_proto::pb::core::v1 as pbcore;
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::pki;

use common::{Ca, Daemon};

/// A formed, certless single-node cluster and the operator credential `init`
/// printed.
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

/// Like [`mint`], but with a real TTL — the `expired` row below needs one that
/// actually elapses.
async fn mint_with_ttl(
    client: &mut coppice_net::admin::Client<tonic::transport::Channel>,
    history_id: &[u8],
    role: pbcore::EnrollRole,
    label: &str,
    ttl_seconds: u64,
) -> pb::MintEnrollTokenResponse {
    client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id: history_id.to_vec(),
            role: role as i32,
            label: label.to_string(),
            ttl_seconds: Some(ttl_seconds),
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
        sans: Vec::new(),
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
        sans: Vec::new(),
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

    // Mint the soon-to-expire token FIRST, before the rest of setup below, and
    // sleep only for whatever time remains once everything else is done —
    // there is no clock knob for token *expiry* ([pacing] only paces the
    // convergence loop, not the TTL check), so the TTL must genuinely elapse.
    // A ~1s real sleep is test-sized and this ordering keeps it off the
    // critical path of every other assertion in this test.
    let mint_started = std::time::Instant::now();
    let expiring = mint_with_ttl(
        &mut admin,
        &history_id,
        pbcore::EnrollRole::Agent,
        "expiring",
        1,
    )
    .await;

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

    let remaining = std::time::Duration::from_millis(1_200).saturating_sub(mint_started.elapsed());
    if !remaining.is_zero() {
        tokio::time::sleep(remaining).await;
    }

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
            "expired",
            Some(expiring.secret.clone()),
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
            sans: Vec::new(),
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
        // `/api/v1/enroll` is on a public sub-router with no authentication
        // layer at all, so enrollment behaviour is unaffected regardless of
        // this chain; this suite exercises enrollment plumbing, not
        // authentication, so open mode resolves every other request to the
        // anonymous actor and the assertions are unchanged.
        Arc::new(coppice_authn::AuthnChain::open(coppice_authn::no_ca())),
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
        coppice_enroll::ensure_enrolled(&paths, &config, coppice_enroll::Claim::Node(node), &[])
            .await
            .expect("enroll over the public route");
    assert_eq!(outcome, coppice_enroll::Outcome::Enrolled);

    let leaf = std::fs::read(&paths.cert).expect("the leaf landed in the [tls] paths");
    let verified = pki::verify_leaf(&std::fs::read(&paths.ca).unwrap(), &leaf)
        .expect("the installed leaf chains to the cluster CA");
    assert_eq!(verified.profile, pki::Profile::Agent(node));

    // Idempotent: a restart with a usable leaf makes no network call.
    let again =
        coppice_enroll::ensure_enrolled(&paths, &config, coppice_enroll::Claim::Node(node), &[])
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
        coppice_enroll::ensure_enrolled(&paths, &config, coppice_enroll::Claim::Node(node), &[])
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

/// Wait until `{base}/readyz` (an HTTPS surface) reports `phase`, the way
/// [`Daemon::await_phase`] does for a plain-HTTP one — needed once
/// [`Daemon::set_client_tls`] is in effect, since that helper always dials
/// plain HTTP.
async fn await_https_phase(client: &reqwest::Client, base: &str, phase: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = client.get(format!("{base}/readyz")).send().await {
            if let Ok(body) = response.json::<serde_json::Value>().await {
                if body["phase"] == phase {
                    return;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "daemon never reached phase {phase} over HTTPS"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Dial the agent session listener over mTLS with the supplied identity.
/// Copied file-locally from the sibling `enrollment.rs`, whose version this
/// test cannot reach from a different integration-test binary.
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

/// A renewal-shaped request — one presented by a machine that already holds a
/// cluster-issued leaf — is refused on the public, certless `/enroll`, while
/// the SAME identity renews successfully over the machine-plane mTLS services
/// (ADR 0037 §4: "renewal over the machine-plane mTLS services (agent
/// session, coordinator admin) and refused on /enroll").
///
/// Post-formation the client listener requests client certificates and
/// verifies them against the CLUSTER CA
/// ([`clientedge::ClusterCa::from_views`](coppice_coordinator::clientedge::ClusterCa::from_views)),
/// so a cluster-issued leaf can be presented as a client identity there —
/// this is what makes the refusal below meaningful: it is not that the
/// listener cannot see the certificate, it is that `/enroll` refuses to look
/// past it.
#[tokio::test]
async fn a_client_certificate_is_refused_on_enroll_and_renews_on_the_machine_planes() {
    let ca = Ca::new();
    let mut daemon = Daemon::new_certless(ClusterId::new(), &ca);
    // `ca` here signs only the listener's OWN serving certificate — what the
    // client below must trust as the *server* root. That is a different
    // concern from the cluster-issued leaf enrolled below and presented as
    // the *client* identity, which the listener verifies against the
    // cluster's own CA instead.
    let (server_root_pem, https_base) = daemon.set_client_tls(&ca);
    daemon.start();
    // `Daemon::await_phase` polls `/readyz` over plain HTTP, which this
    // daemon no longer serves once `set_client_tls` is in effect — poll the
    // same surface over HTTPS instead.
    let root_only = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&server_root_pem).expect("root"))
        .build()
        .expect("build the https polling client");
    await_https_phase(&root_only, &https_base, "waiting").await;
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
    await_https_phase(&root_only, &https_base, "voter").await;

    let (mut admin, history_id) = admin_client(&daemon, &operator).await;

    // Enroll a coordinator identity and an agent identity through
    // `ForwardEnroll` over the admin plane, exactly as `enrollment.rs` does —
    // these are the "already holds a cluster-issued leaf" machines.
    let coordinator_token = mint(
        &mut admin,
        &history_id,
        pbcore::EnrollRole::Coordinator,
        "coordinators",
    )
    .await;
    let (machine_key, machine_csr) = pki::generate_key_and_csr().unwrap();
    let machine = MachineId::new();
    let coordinator_leaf = admin
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: coordinator_token.secret,
            csr_pem: String::from_utf8(machine_csr).unwrap(),
            node_id: None,
            machine_id: Some(machine.into()),
            sans: Vec::new(),
        })
        .await
        .expect("enroll the coordinator identity")
        .into_inner();

    let agent_token = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "agents").await;
    let (agent_key, agent_csr) = pki::generate_key_and_csr().unwrap();
    let node = NodeId::new();
    let agent_leaf = admin
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: agent_token.secret,
            csr_pem: String::from_utf8(agent_csr).unwrap(),
            node_id: Some(node.into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect("enroll the agent identity")
        .into_inner();

    // A fresh live token: the renewal-shaped request still carries a real
    // credential in the body, so the refusal below is demonstrably about the
    // presented certificate, not about a missing/invalid token.
    let renewal_shaped_token = mint(
        &mut admin,
        &history_id,
        pbcore::EnrollRole::Agent,
        "renewal-shaped",
    )
    .await;
    let (_key, fresh_csr) = pki::generate_key_and_csr().unwrap();

    let root = reqwest::Certificate::from_pem(&server_root_pem).expect("server root");
    let with_cert = reqwest::Client::builder()
        .add_root_certificate(root.clone())
        .identity({
            // reqwest's rustls identity takes one PEM blob: key then chain
            // (see crates/coppice-coordinator/tests/client_tls.rs).
            let mut pem = agent_key.clone();
            pem.extend_from_slice(agent_leaf.cert_pem.as_bytes());
            reqwest::Identity::from_pem(&pem).expect("client identity")
        })
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build the client-cert https client");

    let response = with_cert
        .post(format!("{https_base}{}", coppice_enroll::ENROLL_PATH))
        .bearer_auth(&renewal_shaped_token.secret)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(enroll_body(&fresh_csr, Some(NodeId::new()), None))
        .send()
        .await
        .expect("POST /enroll presenting a cluster-issued leaf");
    let (status, body) = checked(response).await;
    assert_eq!(status, 401, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        body.as_slice(),
        coppice_api::http::REFUSED_BODY.as_bytes(),
        "/enroll refuses ANY certificate-bearing request before it looks at the credential \
         (ADR 0037 §4)"
    );

    // The same request WITHOUT the client certificate, same live token:
    // succeeds — proving the refusal above is about the presented
    // certificate, not about the request being malformed.
    let certless = reqwest::Client::builder()
        .add_root_certificate(root)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build the certless https client");
    let response = certless
        .post(format!("{https_base}{}", coppice_enroll::ENROLL_PATH))
        .bearer_auth(&renewal_shaped_token.secret)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(enroll_body(&fresh_csr, Some(NodeId::new()), None))
        .send()
        .await
        .expect("POST /enroll without a client certificate");
    assert_eq!(checked(response).await.0, 200);

    // Renewal DOES work for the same identities, on the machine planes ADR
    // 0037 §4 names: the coordinator admin plane…
    let mut coord_admin = coppice_coordinator::admin::admin_channel(
        &daemon.raft_target(),
        coordinator_leaf.ca_pem.as_bytes(),
        coordinator_leaf.cert_pem.as_bytes(),
        &machine_key,
    )
    .await
    .expect("dial the admin surface with the coordinator leaf");
    let (_key, renew_csr) = pki::generate_key_and_csr().unwrap();
    let renewed_coordinator = coord_admin
        .renew_coordinator(pb::RenewCoordinatorRequest {
            history_id: history_id.clone(),
            csr_pem: String::from_utf8(renew_csr).unwrap(),
            sans: Vec::new(),
        })
        .await
        .expect("renew the coordinator identity over the admin plane")
        .into_inner();
    let verified = pki::verify_leaf(
        renewed_coordinator.ca_pem.as_bytes(),
        renewed_coordinator.cert_pem.as_bytes(),
    )
    .expect("the renewed coordinator leaf chains to the cluster CA");
    assert_eq!(
        verified.profile,
        pki::Profile::Coordinator(machine),
        "renewal preserves the subject exactly (ADR 0037 §4)"
    );

    // …and the agent session plane.
    let mut agent_session = agent_client(
        &daemon,
        agent_leaf.ca_pem.as_bytes(),
        agent_leaf.cert_pem.as_bytes(),
        &agent_key,
    )
    .await;
    let (_key, renew_csr) = pki::generate_key_and_csr().unwrap();
    let renewed_agent = agent_session
        .renew(coppice_proto::pb::agent::v1::RenewRequest {
            csr_pem: String::from_utf8(renew_csr).unwrap(),
        })
        .await
        .expect("renew the agent identity over the session plane")
        .into_inner();
    let verified = pki::verify_leaf(
        renewed_agent.ca_pem.as_bytes(),
        renewed_agent.cert_pem.as_bytes(),
    )
    .expect("the renewed agent leaf chains to the cluster CA");
    assert_eq!(
        verified.profile,
        pki::Profile::Agent(node),
        "renewal preserves the subject exactly (ADR 0037 §4)"
    );

    daemon.stop().await.expect("daemon stops cleanly");
}

/// Proxy one `EnrollCall` to the real leader over the mTLS admin channel,
/// mapping the RPC outcome onto the same refusal shape
/// [`coppice_coordinator::enroll::forward_to_leader`] (the production proxy
/// path) uses. That function is `pub(crate)` and unreachable from this
/// integration-test binary, so this is a file-local copy of just the mapping.
async fn forward_via_admin(
    mut admin: coppice_net::admin::Client<tonic::transport::Channel>,
    history_id: Vec<u8>,
    call: coppice_api::http::EnrollCall,
) -> Result<coppice_enroll::EnrollResponse, coppice_api::http::EnrollRefusal> {
    let reply = admin
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id,
            token: call.token,
            csr_pem: call.csr_pem,
            node_id: call.node_id.map(Into::into),
            machine_id: call.machine_id.map(Into::into),
            sans: call.sans,
        })
        .await
        .map_err(|status| match status.code() {
            tonic::Code::Unauthenticated => coppice_api::http::EnrollRefusal::Unauthorized,
            tonic::Code::InvalidArgument => {
                coppice_api::http::EnrollRefusal::BadRequest(status.message().to_string())
            }
            _ => coppice_api::http::EnrollRefusal::Unavailable(status.message().to_string()),
        })?
        .into_inner();
    Ok(coppice_enroll::EnrollResponse {
        cert_pem: reply.cert_pem,
        ca_pem: reply.ca_pem,
    })
}

/// Coordinator-level analogue of the `coppice-api` route unit tests (oversize
/// ~line 2370, concurrency cap ~line 2398, rate limiter ~line 2447 in
/// `crates/coppice-api/src/http/routes.rs`), whose observable is an
/// issuer-invocation counter on the [`EnrollEndpoint`] callback. Here that
/// callback is wired to a REAL leader over the real mTLS admin channel — the
/// same seam [`EnrollService::endpoint()`](coppice_coordinator) fills in
/// production — so a request that IS admitted does real signing work, and a
/// request that is shed provably never reaches it.
///
/// The rate limiter is 10/s with a burst of 20 (`crates/coppice-api/src/http/enroll.rs`);
/// a 40-request concurrent burst deterministically exceeds that burst.
// Real concurrency (not cooperative interleaving on one thread) matters here:
// the whole point is that 40 requests land on the limiter at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flood_is_shed_before_the_issuer_is_ever_invoked() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let ca = Ca::new();
    let (mut daemon, operator) = formed(&ca).await;
    let (mut admin, history_id) = admin_client(&daemon, &operator).await;
    let agent_token = mint(&mut admin, &history_id, pbcore::EnrollRole::Agent, "agents").await;

    let issuer_calls = Arc::new(AtomicUsize::new(0));
    let endpoint = {
        let admin_for_endpoint = admin.clone();
        let history_id = history_id.clone();
        let issuer_calls = Arc::clone(&issuer_calls);
        coppice_api::http::EnrollEndpoint::new(move |call| {
            let admin = admin_for_endpoint.clone();
            let history_id = history_id.clone();
            let issuer_calls = Arc::clone(&issuer_calls);
            async move {
                // Every request the limits admit reaches here — the same seam
                // `EnrollService::endpoint()` fills in production, and the one
                // the `routes.rs` unit tests count invocations on.
                issuer_calls.fetch_add(1, Ordering::SeqCst);
                forward_via_admin(admin, history_id, call).await
            }
        })
    };

    // A second client listener over the real router, exactly as
    // `a_follower_proxies_to_the_leader_and_never_redirects` sets one up, but
    // with our counting endpoint instead of the production proxy.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the flood target's client listener");
    let addr = listener.local_addr().expect("local addr");
    let router = coppice_api::http::router(
        Arc::new(common::NoopPlane),
        coppice_api::http::MetricsEndpoint::detached_for_tests(),
        coppice_api::http::ReadyzEndpoint::detached_for_tests(),
        endpoint,
        Arc::new(coppice_authn::AuthnChain::open(coppice_authn::no_ca())),
    );
    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let serving = tokio::spawn(async move {
        coppice_coordinator::clientedge::serve(listener, router, None, stop_rx).await;
    });

    let url = format!("http://{addr}{}", coppice_enroll::ENROLL_PATH);

    // Status, body, and (for the valid enrollments) the node id claimed — the
    // three things the tally below needs back from each in-flight request.
    type Answer = tokio::task::JoinHandle<(u16, Vec<u8>, Option<NodeId>)>;
    let mut handles: Vec<Answer> = Vec::new();

    // Oversized bodies: shed at the 413 byte cap, before the token is even
    // read from the body.
    for i in 0..16u32 {
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            let huge = format!(
                r#"{{"csr_pem":"{}"}}"#,
                "A".repeat(coppice_api::http::MAX_ENROLL_BODY + 1)
            );
            let response = http()
                .post(&url)
                .bearer_auth(format!("cpk_oversize-{i}"))
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(huge)
                .send()
                .await
                .expect("POST /enroll (oversize)");
            let (status, body) = checked(response).await;
            (status, body, None)
        }));
    }

    // Ordinary flooding requests: normal-sized bodies, unminted tokens. If
    // admitted (not shed by rate/concurrency), these reach the issuer and are
    // refused there (401) — the token is never a real one, but the CSR is
    // never even looked at, because the enrollment core checks the token
    // first.
    for i in 0..16u32 {
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            let body = enroll_body(b"not-a-real-csr", Some(NodeId::new()), None);
            let response = http()
                .post(&url)
                .bearer_auth(format!("cpk_flood-{i}"))
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await
                .expect("POST /enroll (flood)");
            let (status, body) = checked(response).await;
            (status, body, None)
        }));
    }

    // A handful of genuinely valid enrollments, with distinct node ids.
    for _ in 0..8u32 {
        let url = url.clone();
        let token = agent_token.secret.clone();
        let (_key, csr_pem) = pki::generate_key_and_csr().unwrap();
        let node = NodeId::new();
        handles.push(tokio::spawn(async move {
            let body = enroll_body(&csr_pem, Some(node), None);
            let response = http()
                .post(&url)
                .bearer_auth(token)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await
                .expect("POST /enroll (valid)");
            let (status, body) = checked(response).await;
            (status, body, Some(node))
        }));
    }

    let mut non_shed = 0usize;
    let mut saw_413 = false;
    let mut saw_429_or_503 = false;
    let mut valid_success: Option<(Vec<u8>, NodeId)> = None;

    for handle in handles {
        let (status, body, node) = handle.await.expect("request task joined");
        match status {
            413 => {
                saw_413 = true;
            }
            429 | 503 => {
                saw_429_or_503 = true;
            }
            _ => {
                non_shed += 1;
                if status == 200 {
                    if let Some(node) = node {
                        valid_success = Some((body, node));
                    }
                }
            }
        }
    }

    assert!(
        saw_413,
        "an oversized body among the burst must be shed at 413"
    );
    assert!(
        saw_429_or_503,
        "40 concurrent requests against a burst of 20 must shed some at 429/503"
    );
    assert_eq!(
        issuer_calls.load(Ordering::SeqCst),
        non_shed,
        "nothing shed by the limits ever reaches the issuer"
    );

    // A valid enrollment must succeed despite the flood — but on a starved
    // host the limiter may legitimately shed every valid request that raced
    // the burst itself. What the ADR promises is that shedding is a
    // liveness delay, not a lockout: once the burst subsides, a valid
    // enrollment is admitted. So take an in-burst success if one landed,
    // and otherwise retry a fresh one until the limiter recovers.
    let (body, node) = match valid_success {
        Some(hit) => hit,
        None => {
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let (_key, csr_pem) = pki::generate_key_and_csr().unwrap();
                let node = NodeId::new();
                let response = http()
                    .post(&url)
                    .bearer_auth(agent_token.secret.clone())
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(enroll_body(&csr_pem, Some(node), None))
                    .send()
                    .await
                    .expect("POST /enroll (post-burst)");
                let (status, body) = checked(response).await;
                match status {
                    200 => break (body, node),
                    429 | 503 if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    other => panic!(
                        "a valid enrollment after the burst must eventually be \
                         admitted, got {other} with the deadline {}",
                        if Instant::now() < deadline {
                            "open"
                        } else {
                            "elapsed"
                        }
                    ),
                }
            }
        }
    };
    let issued: coppice_enroll::EnrollResponse = serde_json::from_slice(&body).unwrap();
    let verified = pki::verify_leaf(issued.ca_pem.as_bytes(), issued.cert_pem.as_bytes())
        .expect("the issued leaf chains to the cluster CA");
    assert_eq!(verified.profile, pki::Profile::Agent(node));

    let _ = stop.send(true);
    let _ = serving.await;
    daemon.stop().await.expect("daemon stops cleanly");
}
