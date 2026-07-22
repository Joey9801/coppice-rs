//! The Prometheus `/metrics` endpoint (issue #46).
//!
//! Both daemons expose their `metrics` facade through one small handler: the
//! coordinator mounts it on the client API listener alongside `/api/v1` (ADR
//! 0031), the agent on its own optional `metrics_addr`. The scrape contract is
//! the crate-root describe/gather pattern already documented on every metrics
//! module — install a [`metrics_exporter_prometheus`] recorder once at startup,
//! call the process's crate-root `describe_metrics()` after the install, and
//! call `gather_metrics()` immediately before rendering each scrape so any
//! point-in-time gauges are sampled fresh.
//!
//! [`MetricsEndpoint`] captures exactly the two process-specific pieces that
//! contract needs — the recorder's [`PrometheusHandle`] and a pointer to the
//! crate-root `gather_metrics` — behind a transport-agnostic handler, so
//! neither daemon's router has to know anything about the recorder internals.

use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;

use metrics_exporter_prometheus::PrometheusHandle;

/// The `Content-Type` a Prometheus text-exposition scrape carries. The
/// version tag is the Prometheus text format version (0.0.4), not an HTTP
/// header versioning scheme — scrapers key their parser off it.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

/// The process-local pieces of the describe/gather scrape contract (issue #46):
/// the recorder handle that renders the exposition text, and a pointer to the
/// process's crate-root `gather_metrics` so a scrape samples any point-in-time
/// gauges just before rendering.
///
/// Deliberately transport-agnostic: the same value backs the coordinator's
/// `/metrics` route (mounted next to `/api/v1`) and the agent's standalone
/// metrics server, so the render logic lives here once rather than in each
/// daemon's router wiring.
pub struct MetricsEndpoint {
    /// Renders the installed recorder's registry to Prometheus text.
    handle: PrometheusHandle,
    /// The process's crate-root `gather_metrics`, invoked before every render
    /// so gauges sampled at scrape time (e.g. the agent's queue depths) are
    /// current. Push-style counters need nothing here, so a no-op `|| {}` is a
    /// valid gather for a process (or a test) with no sampled metrics.
    gather: fn(),
}

impl MetricsEndpoint {
    /// Build the endpoint over an already-installed recorder's handle and the
    /// process's crate-root `gather_metrics`.
    ///
    /// The caller owns the recorder lifecycle: it installs the global recorder
    /// (`metrics::set_global_recorder`), keeps its [`PrometheusHandle`], calls
    /// `describe_metrics()` once, and hands the handle plus `gather_metrics`
    /// here. This type never installs anything — installation is a
    /// once-per-process concern that belongs to the daemon, not the transport.
    pub fn new(handle: PrometheusHandle, gather: fn()) -> MetricsEndpoint {
        MetricsEndpoint { handle, gather }
    }

    /// A detached endpoint for tests: a non-installing recorder's handle and a
    /// no-op gather.
    ///
    /// [`PrometheusBuilder::build_recorder`](metrics_exporter_prometheus::PrometheusBuilder::build_recorder)
    /// builds a recorder **without** touching the global slot, so any number of
    /// these can exist across parallel tests in one process without the
    /// "recorder already installed" conflict a global install would raise. The
    /// handle keeps the recorder's registry alive, so `render()` works (it just
    /// renders an empty scrape unless the test also records into that recorder
    /// via `metrics::with_local_recorder`).
    pub fn detached_for_tests() -> MetricsEndpoint {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        MetricsEndpoint::new(recorder.handle(), || {})
    }

    /// Handle one scrape: sample point-in-time metrics, then render the
    /// Prometheus exposition text. Pure gather + render — no recorder upkeep.
    ///
    /// Draining the recorder's histogram buckets is not a scrape concern:
    /// `run_upkeep` runs on a fixed-interval task the installer spawns
    /// (`coppice_coordinator::install_metrics_recorder`), so the buckets drain
    /// on a timer whether or not anything scrapes, and a long-lived process's
    /// scrape latency and memory stay bounded regardless of scrape cadence.
    pub async fn render(&self) -> impl IntoResponse {
        (self.gather)();
        let body = self.handle.render();
        ([(CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)], body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::{to_bytes, Body};
    use axum::http::{header, Request, StatusCode};
    use std::sync::Arc;
    use tower::ServiceExt;

    /// A minimal router mounting only `/metrics` over a captured endpoint,
    /// mirroring how both daemons wire the top-level route.
    fn metrics_app(endpoint: MetricsEndpoint) -> axum::Router {
        let endpoint = Arc::new(endpoint);
        axum::Router::new().route(
            "/metrics",
            axum::routing::get(move || {
                let endpoint = Arc::clone(&endpoint);
                async move { endpoint.render().await }
            }),
        )
    }

    #[tokio::test]
    async fn metrics_endpoint_answers_200_with_the_prometheus_content_type() {
        let response = metrics_app(MetricsEndpoint::detached_for_tests())
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
    }

    #[tokio::test]
    async fn metrics_endpoint_renders_a_recorded_sample() {
        // Build a non-installing recorder, record into it via the thread-local
        // recorder (never the global slot — so parallel tests never conflict),
        // and prove the rendered scrape carries the metric back.
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("test_scrape_total").increment(1);
        });

        let response = metrics_app(MetricsEndpoint::new(handle, || {}))
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(
            body.contains("test_scrape_total"),
            "rendered scrape should carry the recorded counter, got:\n{body}"
        );
    }
}
