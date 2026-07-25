//! The token secret never reaches a log line (ADR 0037 §4).
//!
//! Its own test binary on purpose. The assertion is about what a *whole
//! daemon* emits, across tasks it spawns, so the subscriber has to be the
//! process-global one — `with_default` is scoped to one task and would miss
//! exactly the code paths at issue. A dedicated binary means this process
//! installs that global subscriber once, runs one flow through it, and
//! inspects everything written.
//!
//! The flow is the whole credential's life in this chunk, over **both** the
//! surfaces that carry a token: mint it, redeem it over the internal
//! `ForwardEnroll` RPC and over the public `POST /api/v1/enroll` route, revoke
//! it, and try both again. Success and refusal are covered on each, because a
//! "which token?" field is likeliest to appear on the refusal path. If any of
//! them ever grows a tracing field carrying the secret, this fails.

mod common;

use coppice_coordinator::config::CliOverrides;
use coppice_coordinator::localadmin::{AdminCall, AdminReply};
use coppice_core::id::{ClusterId, NodeId};
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
    daemon.start(CliOverrides {
        bootstrap: false,
        join: false,
    });
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
            history_id,
            token: minted.secret.clone(),
            csr_pem: String::from_utf8(csr).unwrap(),
            node_id: Some(NodeId::new().into()),
            machine_id: None,
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

    daemon.stop().await.expect("daemon stops cleanly");

    let captured = buffer.contents();
    // The exact secret, and the prefix that would catch any other one.
    assert_no_secret(&captured, &minted.secret);
    assert_no_secret(&captured, pki::TOKEN_PREFIX);
    assert!(
        captured.contains("minted an enrollment token"),
        "the capture must actually be wired up; got {} bytes",
        captured.len()
    );
}
