//! The `/readyz` endpoint seam (ADR 0037 §7).
//!
//! `/readyz` reports a coordinator replica's convergence/readiness state as
//! JSON, alongside the existing `/metrics` on the client listener. The whole
//! answer — the convergence phase, leadership, replication lag, the voter
//! counts, and the leader-sourced `voters_live` health snapshot — is produced
//! from consensus/discovery state that lives in `coppice-coordinator`, which
//! depends on this crate rather than the other way round. So, exactly like
//! [`MetricsEndpoint`](super::metrics::MetricsEndpoint), this module holds only
//! a small transport-agnostic seam: a captured, boxed async handler the router
//! mounts. The coordinator builds one over its `ReadyzState`; the daemon has
//! it, and a router served without a coordinator (tests, mocks) simply omits
//! the route (a miss then answers 404 like any other), so no consensus type
//! ever leaks into this crate.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::response::Response;

/// The future a [`ReadyzEndpoint`] handler returns: an already-rendered HTTP
/// response (status code + JSON body), erased so this crate needs no knowledge
/// of the coordinator's state types.
pub type ReadyzFuture = Pin<Box<dyn Future<Output = Response> + Send>>;

/// A captured `/readyz` handler (ADR 0037 §7), transport-agnostic like
/// [`MetricsEndpoint`](super::metrics::MetricsEndpoint).
///
/// The single argument is the raw `?require=` query value (`None` when absent);
/// parsing it (`formed` / `healthy`, or a 400 on anything else) and evaluating
/// the readiness gate are the coordinator's job — this seam only routes the
/// request to it and returns the response it renders.
#[derive(Clone)]
pub struct ReadyzEndpoint {
    handler: Arc<dyn Fn(Option<String>) -> ReadyzFuture + Send + Sync>,
}

impl ReadyzEndpoint {
    /// Build the endpoint over a handler closure. The coordinator passes one
    /// that captures its `ReadyzState` (the convergence watch, node handle, TLS
    /// store, cluster size, and instance UUID) and renders the `/readyz` JSON.
    pub fn new<F>(handler: F) -> ReadyzEndpoint
    where
        F: Fn(Option<String>) -> ReadyzFuture + Send + Sync + 'static,
    {
        ReadyzEndpoint {
            handler: Arc::new(handler),
        }
    }

    /// Handle one `/readyz` request, given the raw `?require=` value.
    pub(crate) async fn handle(&self, require: Option<String>) -> Response {
        (self.handler)(require).await
    }
}
