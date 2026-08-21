//! The token secret never reaches a log line (ADR 0037 §4).
//!
//! Its own test binary on purpose. The assertion is about what a *whole
//! daemon* emits, across tasks it spawns, so the subscriber has to be the
//! process-global one — `with_default` is scoped to one task and would miss
//! exactly the code paths at issue. A dedicated binary means this process
//! installs that global subscriber once, runs one flow through it, and
//! inspects everything written.
//!
//! The flow is the whole credential's life in this chunk, over **three**
//! surfaces that carry a token: mint it, redeem it over the internal
//! `ForwardEnroll` RPC, over the public `POST /api/v1/enroll` route direct to
//! the leader, and over that same route proxied from a follower to the
//! leader — revoke it, and try all three again. Success and refusal are
//! covered on each, because a "which token?" field is likeliest to appear on
//! the refusal path. If any of them ever grows a tracing field carrying the
//! secret, this fails.
//!
//! The proxy hop is driven in this same process, under this same global
//! subscriber, with both the follower's and the leader's ends of the hop
//! captured — that's what makes "neither side's logs" an assertion this test
//! can actually make (ADR 0037 §4).

mod common;

use std::sync::Arc;

use coppice_coordinator::localadmin::{AdminCall, AdminReply};
use coppice_core::id::{ClusterId, MachineId, NodeId};
use coppice_proto::pb::core::v1 as pbcore;
use coppice_proto::pb::raft::v1 as pb;
use coppice_testkit::tracing_capture::{assert_no_secret, CaptureBuffer};
use coppice_tls::pki;

use common::{Ca, Daemon};

#[tokio::test]
async fn no_enrollment_path_ever_logs_the_token_secret() {
    let buffer = CaptureBuffer::new();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("this binary installs the subscriber exactly once");

    let ca = Ca::new();
    let mut daemon = Daemon::new_certless(ClusterId::new(), &ca);
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

    let mut client = coppice_coordinator::admin::admin_channel(
        &daemon.raft_target(),
        operator.ca_pem.as_bytes(),
        operator.cert_pem.as_bytes(),
        operator
            .key_pem
            .as_ref()
            .expect("minted keypair")
            .as_bytes(),
    )
    .await
    .expect("dial the admin surface");
    let history_id = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe")
        .into_inner()
        .history_id;

    let minted = client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id: history_id.clone(),
            role: pbcore::EnrollRole::Agent as i32,
            label: "logging-probe".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("mint")
        .into_inner();

    let (_key, csr) = pki::generate_key_and_csr().unwrap();
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
        .expect("enroll");

    // The same redemption over the public route, which reads the token from an
    // `Authorization` header and hands it to the same core.
    let http = reqwest::Client::new();
    let enroll_url = daemon.api(coppice_enroll::ENROLL_PATH);
    let (_key, http_csr) = pki::generate_key_and_csr().unwrap();
    let body = serde_json::to_string(&coppice_enroll::EnrollRequest {
        csr_pem: String::from_utf8(http_csr.clone()).unwrap(),
        token: None,
        node_id: Some(NodeId::new()),
        machine_id: None,
        sans: Vec::new(),
    })
    .unwrap();
    let response = http
        .post(&enroll_url)
        .bearer_auth(&minted.secret)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.clone())
        .send()
        .await
        .expect("POST /enroll");
    assert_eq!(response.status().as_u16(), 200);

    client
        .revoke_enroll_token(pb::RevokeEnrollTokenRequest {
            history_id: history_id.clone(),
            token_id: minted.token_id,
        })
        .await
        .expect("revoke");

    // The refusal path is the one most likely to grow a "which token?" field.
    let _ = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: minted.secret.clone(),
            csr_pem: String::from_utf8(csr).unwrap(),
            node_id: Some(NodeId::new().into()),
            machine_id: None,
            sans: Vec::new(),
        })
        .await
        .expect_err("a revoked token is refused");

    // …and the public route's refusal path, the one most likely to want to say
    // which credential it turned away.
    let refused = http
        .post(&enroll_url)
        .bearer_auth(&minted.secret)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .expect("POST /enroll");
    assert_eq!(refused.status().as_u16(), 401);

    // The proxy hop: a follower with a cluster-issued coordinator identity
    // forwards `/api/v1/enroll` over the mTLS admin channel to the leader.
    // Both ends of the hop run in this process, under the one global
    // subscriber installed above, so both are in `captured` below.
    let coordinator_token = client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id: history_id.clone(),
            role: pbcore::EnrollRole::Coordinator as i32,
            label: "the-follower".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("mint")
        .into_inner();
    let (machine_key, machine_csr) = pki::generate_key_and_csr().unwrap();
    let machine = MachineId::new();
    let issued = client
        .forward_enroll(pb::ForwardEnrollRequest {
            history_id: history_id.clone(),
            token: coordinator_token.secret.clone(),
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

    // A second, fresh token minted for this hop: the direct route already
    // spent and revoked `minted.secret` above, so the proxy's success case
    // needs a live one.
    let proxy_minted = client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id: history_id.clone(),
            role: pbcore::EnrollRole::Agent as i32,
            label: "logging-probe-proxy".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("mint")
        .into_inner();

    let (_key, proxy_csr) = pki::generate_key_and_csr().unwrap();
    let proxy_body = serde_json::to_string(&coppice_enroll::EnrollRequest {
        csr_pem: String::from_utf8(proxy_csr).unwrap(),
        token: None,
        node_id: Some(NodeId::new()),
        machine_id: None,
        sans: Vec::new(),
    })
    .unwrap();
    let proxied_ok = http
        .post(format!("http://{addr}{}", coppice_enroll::ENROLL_PATH))
        .bearer_auth(&proxy_minted.secret)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(proxy_body.clone())
        .send()
        .await
        .expect("POST /enroll to the follower");
    assert_eq!(proxied_ok.status().as_u16(), 200);

    client
        .revoke_enroll_token(pb::RevokeEnrollTokenRequest {
            history_id: history_id.clone(),
            token_id: proxy_minted.token_id,
        })
        .await
        .expect("revoke");

    // The proxied refusal path: the follower forwards a now-dead token to
    // the leader and relays back whatever the leader says — the hop most
    // likely to grow a "which token?" field on either end.
    let proxied_refused = http
        .post(format!("http://{addr}{}", coppice_enroll::ENROLL_PATH))
        .bearer_auth(&proxy_minted.secret)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(proxy_body)
        .send()
        .await
        .expect("POST /enroll to the follower");
    assert_eq!(proxied_refused.status().as_u16(), 401);

    let _ = stop.send(true);
    let _ = serving.await;

    daemon.stop().await.expect("daemon stops cleanly");

    let captured = buffer.contents();
    // The exact secret, and the prefix that would catch any other one.
    assert_no_secret(&captured, &minted.secret);
    // The follower's own enrollment identity is minted with a coordinator-role
    // token, which is just as much a secret as the agent one above.
    assert_no_secret(&captured, &coordinator_token.secret);
    // The token that crossed the proxy hop, on both the follower's and the
    // leader's side of it.
    assert_no_secret(&captured, &proxy_minted.secret);
    assert_no_secret(&captured, pki::TOKEN_PREFIX);
    assert!(
        captured.contains("minted an enrollment token"),
        "the capture must actually be wired up; got {} bytes",
        captured.len()
    );
}
