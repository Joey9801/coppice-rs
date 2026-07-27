//! The `[enrollment]` posture matrix (ADR 0037 §4 transport bullet).
//!
//! Four postures, four tests: verified HTTPS enrolls; an untrusted endpoint and
//! a misnamed one are both refused *with the token still unsent*; plain HTTP is
//! a configuration error unless the operator opted in, and enrolls when they
//! did. Plus the two properties that hold across all of them — enrollment is
//! idempotent against a leaf already on disk, and the token never reaches a log
//! line.

mod common;

use coppice_core::id::NodeId;
use coppice_enroll::client::{
    ensure_enrolled_with, has_usable_leaf, Claim, EnrollClient, Outcome, TokenSource,
};
use coppice_enroll::{EnrollmentConfig, Secret};
use coppice_tls::{pki, TlsPaths};

/// The secret every test presents. Its `cpk_` prefix is the needle the leak
/// assertions hunt for.
const TOKEN: &str = "cpk_posture_matrix_secret";

fn paths(dir: &std::path::Path) -> TlsPaths {
    TlsPaths {
        cert: dir.join("node.crt"),
        key: dir.join("node.key"),
        ca: dir.join("ca.crt"),
    }
}

fn config(endpoint: String, insecure: bool) -> EnrollmentConfig {
    EnrollmentConfig {
        endpoint,
        token: Some(Secret::new(TOKEN)),
        token_path: None,
        insecure,
    }
}

fn inline_token() -> TokenSource {
    TokenSource::Inline(Secret::new(TOKEN))
}

// ---------------------------------------------------------------------------
// https + a verifiable certificate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_verified_https_endpoint_enrolls_and_installs_the_leaf() {
    let ca = common::TestCa::new();
    let (cert_pem, key_pem) = ca.server_cert(&["localhost"]);
    let node = NodeId::new();
    let (body, cluster_ca) = common::issued_body(node);
    let stub = common::spawn(Some(common::server_config(&cert_pem, &key_pem)), body).await;

    let dir = tempfile::tempdir().expect("temp dir");
    let paths = paths(dir.path());
    let endpoint = stub.endpoint("https", "localhost");

    let client = EnrollClient::with_extra_root_ca(&endpoint, false, &ca.pem).expect("build client");
    let outcome = ensure_enrolled_with(&paths, &client, &inline_token(), Claim::Node(node), &[])
        .await
        .expect("enrolls against a verified endpoint");
    assert_eq!(outcome, Outcome::Enrolled);

    // The three files landed, and the installed leaf is the one the endpoint
    // issued: it chains to the cluster CA with this node's CN.
    assert!(has_usable_leaf(&paths));
    let installed = std::fs::read(&paths.cert).expect("read the leaf");
    let verified = pki::verify_leaf(&cluster_ca, &installed).expect("the installed leaf verifies");
    assert_eq!(verified.profile, pki::Profile::Agent(node));
    assert_eq!(
        std::fs::read(&paths.ca).expect("read the CA"),
        cluster_ca,
        "the CA bundle is installed alongside the leaf"
    );
    assert!(
        String::from_utf8(std::fs::read(&paths.key).expect("read the key"))
            .expect("PEM")
            .contains("PRIVATE KEY")
    );

    // The token travelled in the Authorization header, not the body and not the
    // URL — and never in the clear, since this hop was TLS.
    let request = stub.requests().pop().expect("the stub saw the request");
    let text = String::from_utf8_lossy(&request).to_string();
    assert!(
        text.to_ascii_lowercase()
            .contains(&format!("authorization: bearer {TOKEN}")),
        "the token rides the Authorization header"
    );
    let (head, body) = text.split_once("\r\n\r\n").expect("headers and body");
    assert!(!head.starts_with(&format!("POST {}?", coppice_enroll::ENROLL_PATH)));
    assert!(
        !body.contains("token"),
        "the body's token field stays unset: {body}"
    );
    assert!(
        !String::from_utf8_lossy(&stub.raw_bytes()).contains(TOKEN),
        "nothing readable went on the wire under TLS"
    );
}

#[tokio::test]
async fn a_second_call_finds_the_leaf_and_never_touches_the_network() {
    let ca = common::TestCa::new();
    let (cert_pem, key_pem) = ca.server_cert(&["localhost"]);
    let node = NodeId::new();
    let (body, _cluster_ca) = common::issued_body(node);
    let stub = common::spawn(Some(common::server_config(&cert_pem, &key_pem)), body).await;

    let dir = tempfile::tempdir().expect("temp dir");
    let paths = paths(dir.path());
    let endpoint = stub.endpoint("https", "localhost");
    let client = EnrollClient::with_extra_root_ca(&endpoint, false, &ca.pem).expect("build client");

    let first = ensure_enrolled_with(&paths, &client, &inline_token(), Claim::Node(node), &[])
        .await
        .expect("first enrollment");
    assert_eq!(first, Outcome::Enrolled);
    let installed = std::fs::read(&paths.cert).expect("read the leaf");

    let second = ensure_enrolled_with(&paths, &client, &inline_token(), Claim::Node(node), &[])
        .await
        .expect("second call");
    assert_eq!(
        second,
        Outcome::AlreadyEnrolled,
        "a usable leaf short-circuits enrollment"
    );
    assert_eq!(stub.requests().len(), 1, "no second request was made");
    assert_eq!(
        std::fs::read(&paths.cert).expect("read the leaf"),
        installed,
        "the installed leaf is untouched"
    );
}

// ---------------------------------------------------------------------------
// https + an endpoint that does not verify
// ---------------------------------------------------------------------------

/// The strong form of §4's "verification failure → refuse *before sending the
/// token*": not that the client errored, but that the capture server holds no
/// token bytes at all. The handshake fails, so the request is never written.
async fn assert_refused_without_sending_the_token(
    stub: &common::Stub,
    client: &EnrollClient,
    paths: &TlsPaths,
    node: NodeId,
) {
    let error = ensure_enrolled_with(paths, client, &inline_token(), Claim::Node(node), &[])
        .await
        .expect_err("an unverifiable endpoint is refused");

    let raw = stub.raw_bytes();
    assert!(
        !String::from_utf8_lossy(&raw).contains("cpk_"),
        "the token reached the socket of an endpoint that never verified"
    );
    assert!(
        !raw.windows(4).any(|w| w == b"cpk_"),
        "the token reached the socket of an endpoint that never verified"
    );
    assert!(
        stub.requests().is_empty(),
        "no request was ever decrypted, so no request was ever sent"
    );
    assert!(!has_usable_leaf(paths), "nothing was installed");
    assert!(
        !format!("{error:?}").contains("cpk_"),
        "the error does not carry the token"
    );
}

#[tokio::test]
async fn an_untrusted_certificate_is_refused_before_the_token_is_sent() {
    // The endpoint is fronted by a CA the client has no reason to trust — the
    // client is given a *different* throwaway root.
    let serving_ca = common::TestCa::new();
    let unrelated_ca = common::TestCa::new();
    let (cert_pem, key_pem) = serving_ca.server_cert(&["localhost"]);
    let node = NodeId::new();
    let (body, _) = common::issued_body(node);
    let stub = common::spawn(Some(common::server_config(&cert_pem, &key_pem)), body).await;

    let dir = tempfile::tempdir().expect("temp dir");
    let paths = paths(dir.path());
    let client = EnrollClient::with_extra_root_ca(
        &stub.endpoint("https", "localhost"),
        false,
        &unrelated_ca.pem,
    )
    .expect("build client");

    assert_refused_without_sending_the_token(&stub, &client, &paths, node).await;
}

#[tokio::test]
async fn a_hostname_mismatch_is_refused_before_the_token_is_sent() {
    // The CA *is* trusted; the certificate simply names another host. `insecure`
    // is irrelevant here, and set, to prove it does not weaken https.
    let ca = common::TestCa::new();
    let (cert_pem, key_pem) = ca.server_cert(&["not-the-host.example"]);
    let node = NodeId::new();
    let (body, _) = common::issued_body(node);
    let stub = common::spawn(Some(common::server_config(&cert_pem, &key_pem)), body).await;

    let dir = tempfile::tempdir().expect("temp dir");
    let paths = paths(dir.path());
    let client =
        EnrollClient::with_extra_root_ca(&stub.endpoint("https", "localhost"), true, &ca.pem)
            .expect("build client");

    assert_refused_without_sending_the_token(&stub, &client, &paths, node).await;
}

// ---------------------------------------------------------------------------
// http, with and without the opt-in
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plain_http_without_the_opt_in_fails_at_configuration_time() {
    let config = config("http://coppice.example.com:7070".to_string(), false);
    let error = config
        .validate()
        .expect_err("a cleartext endpoint needs the conspicuous opt-in");
    let rendered = error.to_string();
    assert!(rendered.contains("enrollment.insecure"), "{rendered}");

    // And the client refuses to be built at all, so a validation path that
    // somehow skipped the check still cannot put the token on a cleartext wire.
    assert!(
        EnrollClient::new("http://coppice.example.com:7070", false).is_err(),
        "the client enforces the same posture"
    );
}

#[tokio::test]
async fn plain_http_with_the_opt_in_enrolls() {
    let node = NodeId::new();
    let (body, cluster_ca) = common::issued_body(node);
    let stub = common::spawn(None, body).await;

    let dir = tempfile::tempdir().expect("temp dir");
    let paths = paths(dir.path());
    let endpoint = stub.endpoint("http", "127.0.0.1");
    let config = config(endpoint.clone(), true);
    config.validate().expect("the opt-in makes it valid");

    let outcome = coppice_enroll::ensure_enrolled(&paths, &config, Claim::Node(node), &[])
        .await
        .expect("enrolls over cleartext when the operator asked for it");
    assert_eq!(outcome, Outcome::Enrolled);

    let installed = std::fs::read(&paths.cert).expect("read the leaf");
    let verified = pki::verify_leaf(&cluster_ca, &installed).expect("verifies");
    assert_eq!(verified.profile, pki::Profile::Agent(node));

    // This is exactly why the flag is documented dev/test-only: on a cleartext
    // hop the token is readable on the wire, and the test says so out loud.
    assert!(
        String::from_utf8_lossy(&stub.raw_bytes()).contains(TOKEN),
        "cleartext means cleartext"
    );
}
