//! Serving the public client listener (ADR 0037 §4 `[client_tls]`).
//!
//! The user-facing edge has two postures and this module is where they
//! diverge: `insecure = true` is `axum::serve` on the bare socket, exactly as
//! before; a configured certificate is a hand-rolled acceptor — TCP accept,
//! rustls handshake with the material *current at that moment*, then hyper
//! serving the same axum [`Router`] over the resulting stream.
//!
//! Two things force the hand-rolled path rather than a canned TLS wrapper:
//!
//! - **Per-accept resolution.** The serving certificate rotates on disk and the
//!   client-certificate trust anchor — the cluster CA — arrives from replicated
//!   state and does not exist before formation. Both are read at each accept,
//!   so a rotation, or the CA appearing, reaches new connections with no
//!   restart, exactly as [`coppice_tls::serve`] does for the machine plane.
//! - **The peer certificate must reach handlers.** Client certificates are
//!   *requested, never required* here (ADR 0037 §4), so a connection may or may
//!   not carry one; when it does, it is inserted as a
//!   [`PeerCertificates`](coppice_api::http::PeerCertificates) request
//!   extension. Nothing authorizes on it yet — `/enroll` reads it only to
//!   refuse — but ADR 0022's operator-certificate authentication layers on here
//!   without touching this file again.
//!
//! Both serving surfaces route through here: the pre-formation closed surface
//! and the post-formation API server, so a deployment's posture is one decision
//! that holds across the parked→formed handover.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use coppice_api::http::PeerCertificates;
use coppice_consensus::StateViews;
use coppice_tls::ClientTlsStore;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt;

/// Handshakes that stall are dropped after this bound, so a slow or malicious
/// client cannot leak accept tasks. Same value, same reason, as
/// [`coppice_tls`]'s machine-plane acceptor.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// The cluster CA, as the client listener sees it: the *client-certificate*
/// trust anchor, read fresh at each accept.
///
/// A closure rather than a value because the answer changes: a pre-formation
/// daemon has no CA at all (and therefore requests no client certificate), a
/// formed one publishes it in replicated state, and a re-root changes it.
#[derive(Clone)]
pub struct ClusterCa(Arc<dyn Fn() -> Option<Vec<u8>> + Send + Sync>);

impl ClusterCa {
    /// The CA recorded in replicated state, as the serving replica sees it.
    pub fn from_views(views: StateViews) -> ClusterCa {
        ClusterCa(Arc::new(move || {
            views
                .latest()
                .state()
                .ca
                .as_ref()
                .map(|ca| ca.bundle.pem().as_bytes().to_vec())
        }))
    }

    /// A constant anchor, for a caller that already holds the CA bundle and has
    /// no replicated view to read it from.
    pub fn fixed(ca_pem: Vec<u8>) -> ClusterCa {
        ClusterCa(Arc::new(move || Some(ca_pem.clone())))
    }

    /// No trust anchor: the pre-formation surface, which has no cluster CA yet
    /// and so cannot judge a client certificate — it therefore asks for none.
    pub fn none() -> ClusterCa {
        ClusterCa(Arc::new(|| None))
    }

    fn current(&self) -> Option<Vec<u8>> {
        (self.0)()
    }

    /// The same anchor, as the authentication chain's
    /// [`CaProvider`](coppice_authn::CaProvider).
    ///
    /// The two consumers ask different questions of one answer: this acceptor
    /// reads it per accept to decide whether to *request* a client certificate
    /// at all, and `coppice-authn` reads it per request to decide whether a
    /// presented certificate is an operator credential for this cluster
    /// (ADR 0022). Sharing the closure rather than building a second one is
    /// what keeps those two from disagreeing across a re-root.
    pub fn provider(&self) -> coppice_authn::CaProvider {
        Arc::clone(&self.0)
    }
}

impl std::fmt::Debug for ClusterCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterCa").finish_non_exhaustive()
    }
}

/// Resolve once `stop` has been flipped.
///
/// Deliberately not `watch::Receiver::wait_for`: its future holds a read guard
/// that is not `Send`, and every future here is held across an await inside a
/// `select!` on a spawned task. Borrowing inside the condition keeps the guard
/// out of the future entirely.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

/// Serve `app` on `listener` under this deployment's posture, returning once
/// `shutdown` has fired and serving has drained.
///
/// `tls` of `None` is the `insecure = true` posture: plain HTTP through
/// `axum::serve`, byte for byte the previous behavior.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    tls: Option<(Arc<ClientTlsStore>, ClusterCa)>,
    mut shutdown: watch::Receiver<bool>,
) {
    match tls {
        None => {
            let graceful = async move {
                stopped(&mut shutdown).await;
            };
            if let Err(e) = axum::serve(listener, app)
                .with_graceful_shutdown(graceful)
                .await
            {
                tracing::error!(error = %e, "client listener terminated with an error");
            }
        }
        Some((store, ca)) => serve_tls(listener, app, store, ca, shutdown).await,
    }
}

async fn serve_tls(
    listener: TcpListener,
    app: Router,
    store: Arc<ClientTlsStore>,
    ca: ClusterCa,
    mut shutdown: watch::Receiver<bool>,
) {
    // Connections are told to finish gracefully when the listener stops
    // accepting, rather than being dropped mid-response.
    let (drain, _) = watch::channel(false);

    loop {
        tokio::select! {
            _ = stopped(&mut shutdown) => break,
            accepted = listener.accept() => match accepted {
                Ok((tcp, peer)) => {
                    // Connection-time resolution: the current serving material
                    // and the current cluster CA, so a rotation (or the CA
                    // first appearing after formation) takes effect on new
                    // connections without touching established ones.
                    let config = match store.server_config(ca.current().as_deref()) {
                        Ok(config) => config,
                        Err(e) => {
                            tracing::error!(
                                %peer,
                                error = %e,
                                "client listener: could not assemble the serving config; \
                                 dropping this connection"
                            );
                            continue;
                        }
                    };
                    let app = app.clone();
                    let drain = drain.subscribe();
                    tokio::spawn(serve_connection(tcp, peer, config, app, drain));
                }
                Err(e) => {
                    // A per-accept error (transient fd exhaustion, say) must
                    // not kill the listener; back off a touch to avoid a hot
                    // loop, exactly as the machine-plane acceptor does.
                    tracing::warn!(error = %e, "client listener: tcp accept error");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            },
        }
    }

    let _ = drain.send(true);
    tracing::debug!("client listener stopped accepting");
}

async fn serve_connection(
    tcp: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    config: Arc<tokio_rustls::rustls::ServerConfig>,
    app: Router,
    mut drain: watch::Receiver<bool>,
) {
    let acceptor = TlsAcceptor::from(config);
    let stream = match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            tracing::debug!(%peer, error = %e, "client listener: tls handshake failed");
            return;
        }
        Err(_) => {
            tracing::debug!(%peer, "client listener: tls handshake timed out");
            return;
        }
    };

    // Present only when the peer took up the certificate *request*; the
    // overwhelming majority of connections here carry none.
    let peer_certs = stream.get_ref().1.peer_certificates().map(|chain| {
        PeerCertificates(Arc::new(
            chain.iter().map(|der| der.as_ref().to_vec()).collect(),
        ))
    });

    let service =
        hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
            let app = app.clone();
            let peer_certs = peer_certs.clone();
            async move {
                let (mut parts, body) = request.into_parts();
                if let Some(certs) = peer_certs {
                    parts.extensions.insert(certs);
                }
                let request =
                    axum::extract::Request::from_parts(parts, axum::body::Body::new(body));
                app.oneshot(request).await
            }
        });

    let builder =
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
    let connection = builder.serve_connection(hyper_util::rt::TokioIo::new(stream), service);
    tokio::pin!(connection);

    let outcome = tokio::select! {
        outcome = connection.as_mut() => outcome,
        _ = stopped(&mut drain) => {
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    };
    if let Err(e) = outcome {
        tracing::debug!(%peer, error = %e, "client listener: connection ended with an error");
    }
}
