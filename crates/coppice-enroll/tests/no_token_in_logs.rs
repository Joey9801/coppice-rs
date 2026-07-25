//! "The token must never appear in application logs or traces" (ADR 0037 §4),
//! asserted the only way it can be: by running the real enrollment flow under a
//! real `tracing` subscriber and reading what came out.
//!
//! Its own test binary on purpose. `tracing` caches a callsite's interest
//! globally the first time it is evaluated, so a sibling test exercising these
//! same log statements without a subscriber installed can leave them cached as
//! uninteresting and quietly empty this capture — turning the assertion
//! vacuous. One process, one test, no such race; the `captured.contains` guard
//! below is the second line of defence.

mod common;

use coppice_core::id::NodeId;
use coppice_enroll::client::{ensure_enrolled_with, Claim, EnrollClient, TokenSource};
use coppice_tls::TlsPaths;

const TOKEN: &str = "cpk_this_must_never_be_logged";

fn paths(dir: &std::path::Path) -> TlsPaths {
    TlsPaths {
        cert: dir.join("node.crt"),
        key: dir.join("node.key"),
        ca: dir.join("ca.crt"),
    }
}

#[test]
fn neither_a_successful_nor_a_failed_enrollment_logs_the_token() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");

    let (_, captured) = coppice_testkit::tracing_capture::capture(|| {
        runtime.block_on(async {
            let ca = common::TestCa::new();
            let (cert_pem, key_pem) = ca.server_cert(&["localhost"]);
            let node = NodeId::new();
            let (body, _) = common::issued_body(node);
            let stub = common::spawn(Some(common::server_config(&cert_pem, &key_pem)), body).await;

            let dir = tempfile::tempdir().expect("temp dir");
            let token_file = dir.path().join("token");
            std::fs::write(&token_file, TOKEN).expect("write the token");

            let good = EnrollClient::with_extra_root_ca(
                &stub.endpoint("https", "localhost"),
                false,
                &ca.pem,
            )
            .expect("build client");
            ensure_enrolled_with(
                &paths(dir.path()),
                &good,
                &TokenSource::Path(token_file.clone()),
                Claim::Node(node),
            )
            .await
            .expect("enrolls");

            // The failure path matters more than the success path: an error is
            // the value most likely to be logged with its whole context.
            let elsewhere = tempfile::tempdir().expect("temp dir");
            let untrusted = common::TestCa::new();
            let bad = EnrollClient::with_extra_root_ca(
                &stub.endpoint("https", "localhost"),
                false,
                &untrusted.pem,
            )
            .expect("build client");
            let error = ensure_enrolled_with(
                &paths(elsewhere.path()),
                &bad,
                &TokenSource::Path(token_file),
                Claim::Node(node),
            )
            .await
            .expect_err("an untrusted endpoint is refused");
            tracing::warn!(error = %error, "enrollment failed");
        });
    });

    assert!(
        captured.contains("enrolling for a cluster-signed leaf"),
        "the capture is empty, so its assertion would prove nothing"
    );
    assert!(captured.contains("enrollment failed"), "{captured}");
    coppice_testkit::tracing_capture::assert_no_secret(&captured, "cpk_");
}
