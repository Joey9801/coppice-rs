//! The agent's optional Prometheus `/metrics` server (issue #46).
//!
//! A tiny, unauthenticated HTTP endpoint that renders this process's `metrics`
//! recorder — the scrape target for the agent's `agent_*` counters
//! (docker-executor.md §8.1, ADR 0034). It mirrors [`node_service`](crate::node_service)'s
//! bind-then-serve shape: [`prepare_listener`] binds eagerly at startup so a
//! port conflict fails the daemon fast, and [`serve`] spawns the axum server as
//! a fire-and-forget background task.
//!
//! **Why a local handler rather than `coppice_api::http::MetricsEndpoint`.**
//! The scrape contract here is identical to the coordinator's — sample gauges,
//! render with the Prometheus text content type (recorder upkeep runs on the
//! installer's timer task, not per scrape) — but depending on `coppice-api` to
//! share the type would drag its `coppice-state`,
//! `rust-embed` (the embedded web-UI SPA), and `mime_guess` dependencies into
//! the agent's build, an unwanted and surprising edge for a node daemon. The
//! handler is ~30 lines, so it is duplicated here instead; keep it in step with
//! `coppice_api::http::metrics::MetricsEndpoint`.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;

/// The `Content-Type` of a Prometheus text-exposition scrape (format version
/// 0.0.4). Kept identical to `coppice_api::http::metrics`.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

/// The process-local pieces of the describe/gather scrape contract (issue #46):
/// the recorder handle that renders the exposition text and a pointer to the
/// crate-root [`gather_metrics`](crate::gather_metrics), invoked before each
/// render so any point-in-time gauges are sampled fresh. The agent's metrics
/// are all push-style counters today, so `gather` is effectively a no-op, but
/// the pointer keeps the contract honest as sampled metrics are added.
struct MetricsEndpoint {
    handle: PrometheusHandle,
    gather: fn(),
}

impl MetricsEndpoint {
    /// Handle one scrape: sample point-in-time metrics, then render the
    /// Prometheus exposition text. Pure gather + render — no recorder upkeep.
    ///
    /// The histogram buckets are drained by a fixed-interval task the recorder
    /// installer spawns ([`run_daemon`](crate::run_daemon)), so they stay
    /// bounded whether or not anything scrapes.
    async fn render(&self) -> impl IntoResponse {
        (self.gather)();
        let body = self.handle.render();
        ([(CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)], body)
    }
}

/// Bind the `/metrics` TCP listener eagerly (issue #46).
///
/// Fail-fast in the [`node_service::prepare_listener`](crate::node_service::prepare_listener)
/// style: a port conflict surfaces here at startup, named in the error's
/// context, rather than after the daemon has registered.
pub async fn prepare_listener(addr: SocketAddr) -> Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding the agent metrics server on {addr}"))
}

/// Serve `GET /metrics` over the bound listener, spawning the axum server as a
/// fire-and-forget background task (its handle is returned for symmetry with
/// [`node_service::serve`](crate::node_service::serve), but the daemon runs it
/// detached).
///
/// `gather` is the crate-root [`gather_metrics`](crate::gather_metrics); the
/// caller has already installed the global recorder and kept its `handle`.
pub fn serve(
    listener: tokio::net::TcpListener,
    handle: PrometheusHandle,
    gather: fn(),
) -> tokio::task::JoinHandle<()> {
    let app = router(handle, gather);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            // Like the coordinator's API server: a dead scrape endpoint never
            // takes the agent down (the node keeps executing work); the
            // operator just sees why the port went dark.
            tracing::error!(error = %e, "agent metrics server terminated with an error");
        }
    })
}

/// The one-route metrics router, factored out so the test can drive it with
/// `tower::ServiceExt::oneshot` without binding a socket.
fn router(handle: PrometheusHandle, gather: fn()) -> Router {
    let endpoint = std::sync::Arc::new(MetricsEndpoint { handle, gather });
    Router::new().route(
        "/metrics",
        get(move || {
            let endpoint = std::sync::Arc::clone(&endpoint);
            async move { endpoint.render().await }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::{to_bytes, Body};
    use axum::http::{header, Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn metrics_route_renders_a_described_metric_over_http() {
        // A non-installing recorder (never the global slot, so this test is safe
        // to run in parallel with any other) with a metric described and
        // recorded into it via the thread-local recorder. The rendered scrape
        // must answer 200, the Prometheus content type, and carry the metric.
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            metrics::describe_counter!(
                "agent_metrics_server_selftest_total",
                metrics::Unit::Count,
                "A self-test counter proving the /metrics scrape renders (issue #46)."
            );
            metrics::counter!("agent_metrics_server_selftest_total").increment(1);
        });

        let response = router(handle, crate::gather_metrics)
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            PROMETHEUS_CONTENT_TYPE
        );
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(
            body.contains("agent_metrics_server_selftest_total"),
            "rendered scrape should carry the recorded counter, got:\n{body}"
        );
    }
}
