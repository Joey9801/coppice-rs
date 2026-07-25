//! The client listener's serving posture (ADR 0037 §4 `[client_tls]`).
//!
//! Three things are asserted here, all over a real socket:
//!
//! - a configured certificate serves HTTPS, and a whole daemon configured that
//!   way answers on it — the posture is one config decision, resolved at load;
//! - a client certificate presented on that listener reaches handlers as a
//!   request extension, so ADR 0022's operator authentication has somewhere to
//!   stand;
//! - and it is *requested, never required*: a certless client — every browser,
//!   every CLI, every enrolling machine — is served identically.
//!
//! The "neither, or a mix, is a startup error naming both options" half of the
//! matrix is a config-load property and is unit-tested in `config::client_tls`.

mod common;

use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use coppice_api::http::PeerCertificates;
use coppice_coordinator::clientedge::{self, ClusterCa};
use coppice_coordinator::config::CliOverrides;
use coppice_core::id::ClusterId;
use coppice_tls::{ClientTlsPaths, ClientTlsStore};

use common::{Ca, Daemon};

/// A router whose one route reports whether the connection presented a client
/// certificate — the plumbing under test, with nothing else in the way.
fn peer_router() -> Router {
    Router::new().route(
        "/peer",
        get(
            |peer: Option<axum::extract::Extension<PeerCertificates>>| async move {
                match peer {
                    Some(axum::extract::Extension(certs)) => {
                        format!("present:{}", certs.0.len())
                    }
                    None => "absent".to_string(),
                }
            },
        ),
    )
}

struct Serving {
    url: String,
    stop: tokio::sync::watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}

impl Serving {
    async fn shutdown(self) {
        let _ = self.stop.send(true);
        let _ = self.join.await;
    }
}

async fn serve(tls: Option<(Arc<ClientTlsStore>, ClusterCa)>) -> Serving {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let scheme = if tls.is_some() { "https" } else { "http" };
    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let join = tokio::spawn(async move {
        clientedge::serve(listener, peer_router(), tls, stop_rx).await;
    });
    Serving {
        url: format!("{scheme}://localhost:{port}"),
        stop,
        join,
    }
}

/// A store over a freshly-issued serving leaf, plus the CA that signed it.
fn serving_store(ca: &Ca, dir: &std::path::Path) -> Arc<ClientTlsStore> {
    let leaf = ca.leaf();
    let cert = dir.join("api.crt");
    let key = dir.join("api.key");
    std::fs::write(&cert, &leaf.cert_pem).expect("write serving cert");
    std::fs::write(&key, &leaf.key_pem).expect("write serving key");
    ClientTlsStore::load(ClientTlsPaths { cert, key }).expect("load the serving material")
}

#[tokio::test]
async fn a_client_certificate_is_requested_not_required_and_reaches_handlers() {
    let ca = Ca::new();
    let dir = tempfile::tempdir().expect("tempdir");
    let store = serving_store(&ca, dir.path());
    let serving = serve(Some((store, ClusterCa::fixed(ca.pem.clone())))).await;

    // A caller that presents an operator-style client certificate: the
    // handler sees the chain.
    let operator = ca.leaf_with_cn("operator@example.com");
    let with_cert = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca.pem).expect("root"))
        .identity({
            // reqwest's rustls identity takes one PEM blob: key then chain.
            let mut pem = operator.key_pem.clone();
            pem.extend_from_slice(&operator.cert_pem);
            reqwest::Identity::from_pem(&pem).expect("client identity")
        })
        .build()
        .expect("build the client-cert http client");
    let body = with_cert
        .get(format!("{}/peer", serving.url))
        .send()
        .await
        .expect("GET /peer over TLS")
        .text()
        .await
        .expect("body");
    assert_eq!(
        body, "present:1",
        "the peer certificate must reach the handler (ADR 0037 §4)"
    );

    // And a caller with no certificate at all is served just the same — this
    // listener never *requires* one, which is what keeps it usable by browsers,
    // CLIs, and enrolling machines.
    let certless = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca.pem).expect("root"))
        .build()
        .expect("build the certless http client");
    let body = certless
        .get(format!("{}/peer", serving.url))
        .send()
        .await
        .expect("GET /peer over TLS without a client certificate")
        .text()
        .await
        .expect("body");
    assert_eq!(body, "absent");

    serving.shutdown().await;
}

#[tokio::test]
async fn the_insecure_posture_serves_plain_http() {
    let serving = serve(None).await;
    let body = reqwest::get(format!("{}/peer", serving.url))
        .await
        .expect("GET /peer over plain HTTP")
        .text()
        .await
        .expect("body");
    assert_eq!(body, "absent");
    serving.shutdown().await;
}

/// The whole daemon under the TLS posture: `[client_tls]` cert/key in its
/// config file, and `/readyz` answering over HTTPS on the same port the plain
/// posture uses.
#[tokio::test]
async fn a_daemon_configured_with_a_certificate_serves_readyz_over_https() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);
    let (ca_pem, base) = daemon.set_client_tls(&ca);
    daemon.start(CliOverrides {
        bootstrap: false,
        join: false,
    });

    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca_pem).expect("root"))
        .build()
        .expect("build the https client");

    // The parked surface is served under the same posture the formed one will
    // be, so nothing an operator dials has to change at formation.
    let mut last = None;
    let mut body = None;
    for _ in 0..200 {
        match client.get(format!("{base}/readyz")).send().await {
            Ok(response) => {
                body = Some(response.json::<serde_json::Value>().await.expect("json"));
                break;
            }
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
    }
    let body = body.unwrap_or_else(|| panic!("/readyz never answered over TLS: {last:?}"));
    assert_eq!(body["phase"], "waiting");

    // Plain HTTP to a TLS listener is refused at the handshake, not silently
    // downgraded.
    let plain = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1{}/readyz",
            &base[base.rfind(':').unwrap()..]
        ))
        .send()
        .await;
    assert!(plain.is_err(), "a TLS listener never serves plain HTTP");

    daemon.stop().await.expect("daemon stops cleanly");
}
