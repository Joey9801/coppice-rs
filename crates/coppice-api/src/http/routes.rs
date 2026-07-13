//! The `/api/v1` route map (ADR 0031) and its implemented handlers.
//!
//! One route per `CoppiceApi` method in `web/src/api/client.ts`, plus the
//! two writes `ControlPlane` already serves. Reads are stubbed with
//! [`unimplemented`] until their endpoint lands; implementing one means:
//! proto message pair in `proto/coppice/api/v1/` (shape mirrors
//! `web/src/api/types.ts`), a method on the (future) query-plane trait,
//! and swapping the stub for a real handler here — routing, errors, and
//! consistency parameters are already decided.

use std::future::ready;
use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use coppice_core::id::JobId;
use coppice_proto::pb::api::v1::{AbortJobRequest, AbortJobResponse, SubmitJobRequest};

use crate::ControlPlane;

use super::error::HttpError;

/// Build the client-listener router around a [`ControlPlane`].
///
/// Consistency defaults per route are the ADR 0031 table; they become code
/// (`ReadParams::class(default)`) as each read handler is implemented.
pub fn router<P: ControlPlane>(plane: Arc<P>) -> Router {
    Router::new()
        // Session / auth (ADR 0022) — local read, no raft involvement.
        .route("/api/v1/session", get(unimplemented("GetSession")))
        // Cluster overview — bounded reads.
        .route("/api/v1/overview", get(unimplemented("GetClusterOverview")))
        .route("/api/v1/queue/stats", get(unimplemented("GetQueueStats")))
        // Jobs. List/detail/timeline are bounded; usage is eventual
        // (derived samples); logs are provisional until log storage exists.
        .route(
            "/api/v1/jobs",
            get(unimplemented("ListJobs")).post(submit_job::<P>),
        )
        .route("/api/v1/jobs/:job", get(unimplemented("GetJob")))
        .route("/api/v1/jobs/:job/abort", post(abort_job::<P>))
        .route(
            "/api/v1/jobs/:job/timeline",
            get(unimplemented("GetJobTimeline")),
        )
        .route("/api/v1/jobs/:job/usage", get(unimplemented("GetJobUsage")))
        .route("/api/v1/jobs/:job/logs", get(unimplemented("GetJobLogs")))
        // Nodes. List/detail bounded; utilization/history eventual; logs
        // provisional.
        .route("/api/v1/nodes", get(unimplemented("ListNodes")))
        .route("/api/v1/nodes/:node", get(unimplemented("GetNode")))
        .route(
            "/api/v1/nodes/:node/utilization",
            get(unimplemented("GetNodeUtilization")),
        )
        .route(
            "/api/v1/nodes/:node/history",
            get(unimplemented("GetNodeHistory")),
        )
        .route(
            "/api/v1/nodes/:node/logs",
            get(unimplemented("GetNodeLogs")),
        )
        // Coordinators — local status read; logs provisional.
        .route(
            "/api/v1/coordinators",
            get(unimplemented("GetCoordinatorStatus")),
        )
        .route(
            "/api/v1/coordinators/:id/logs",
            get(unimplemented("GetCoordinatorLogs")),
        )
        // Quota entities. List bounded; detail defaults strong (ADR 0007:
        // configuration reads); configure is the ADR-0023-gated upsert.
        .route(
            "/api/v1/quota-entities",
            get(unimplemented("ListQuotaEntities")).post(unimplemented("ConfigureQuotaEntity")),
        )
        .route(
            "/api/v1/quota-entities/:entity",
            get(unimplemented("GetQuotaEntity")),
        )
        // Reserved: ADR 0008 event subscription (SSE, cursor-resumed).
        .route("/api/v1/events", get(unimplemented("SubscribeEvents")))
        // Everything unrouted: `/api/*` misses stay JSON 404s; anything
        // else serves the embedded web UI (static assets + SPA fallback,
        // ADR 0031 "Serving the UI").
        .fallback(super::ui::fallback)
        .with_state(plane)
}

/// Stub handler for a route whose endpoint is not implemented yet: routed
/// (so the path is claimed and typos 404 distinctly) but answering
/// `501 UNIMPLEMENTED` with the endpoint name.
fn unimplemented(
    endpoint: &'static str,
) -> impl Fn() -> std::future::Ready<HttpError> + Clone + Send + 'static {
    move || ready(HttpError::unimplemented(endpoint))
}

/// `POST /api/v1/jobs` — body `SubmitJobRequest`, response
/// `SubmitJobResponse` (echoed client-minted id + `logIndex` for a
/// read-your-writes `min_index`, ADR 0026/0007).
async fn submit_job<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    body: Result<Json<SubmitJobRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = body.map_err(bad_body)?;
    let response = plane.submit_job(request).await?;
    Ok(Json(response))
}

/// `POST /api/v1/jobs/{job}/abort` — body `AbortJobRequest`. The path
/// segment is authoritative for the job id: the body's `job` field may be
/// omitted (`{}` aborts with no reason) and, when present, must match the
/// path — a mismatch is `INVALID_ARGUMENT`, never silently resolved.
async fn abort_job<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    Path(job): Path<String>,
    body: Result<Json<AbortJobRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let job: JobId = job
        .parse()
        .map_err(|e: coppice_core::id::ParseIdError| HttpError::invalid(e.to_string()))?;
    let Json(mut request) = body.map_err(bad_body)?;
    match &request.job {
        None => request.job = Some(job.into()),
        Some(body_job) if *body_job != job.into() => {
            return Err(HttpError::invalid(
                "body job id does not match the path job id",
            ));
        }
        Some(_) => {}
    }
    plane.abort_job(request).await?;
    Ok(Json(AbortJobResponse {}))
}

fn bad_body(rejection: JsonRejection) -> HttpError {
    HttpError::invalid(rejection.body_text())
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::{to_bytes, Body};
    use axum::http::{header, Request, StatusCode};
    use tower::ServiceExt;

    use crate::ApiError;
    use coppice_proto::pb::api::v1::SubmitJobResponse;

    use crate::http::COPPICE_LEADER;

    /// A canned `ControlPlane`: submit echoes the request's job id with a
    /// fixed log index, or fails with the configured error.
    struct StubPlane {
        fail_with: Option<fn() -> ApiError>,
    }

    impl ControlPlane for StubPlane {
        async fn submit_job(&self, req: SubmitJobRequest) -> Result<SubmitJobResponse, ApiError> {
            match self.fail_with {
                Some(make) => Err(make()),
                None => Ok(SubmitJobResponse {
                    job: req.job,
                    log_index: 7,
                }),
            }
        }

        async fn abort_job(&self, _req: AbortJobRequest) -> Result<(), ApiError> {
            match self.fail_with {
                Some(make) => Err(make()),
                None => Ok(()),
            }
        }
    }

    fn app(fail_with: Option<fn() -> ApiError>) -> Router {
        router(Arc::new(StubPlane { fail_with }))
    }

    async fn body_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn post_json(uri: &str, body: &str) -> Request<Body> {
        Request::post(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn stub_routes_answer_501_with_the_endpoint_name() {
        let response = app(None)
            .oneshot(Request::get("/api/v1/nodes").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = body_json(response).await;
        assert_eq!(body["code"], "UNIMPLEMENTED");
        assert!(body["message"].as_str().unwrap().contains("ListNodes"));
    }

    #[tokio::test]
    async fn non_api_paths_are_ui_territory_never_json_errors() {
        // A client-side route like /jobs/<id> must be answered by the UI
        // fallback: the SPA shell when a `web/dist` build is present in
        // this environment, or the npm build hint when not — never the
        // API's JSON error contract.
        let response = app(None)
            .oneshot(
                Request::get("/jobs/job-00000000-0000-0000-0000-000000000001")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        match response.status() {
            StatusCode::OK => {
                let content_type = response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned();
                assert!(content_type.starts_with("text/html"), "{content_type}");
            }
            StatusCode::NOT_FOUND => {
                let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                let text = String::from_utf8(bytes.to_vec()).unwrap();
                assert!(text.contains("npm --prefix web run build"), "{text}");
            }
            other => panic!("expected the UI shell or the build hint, got {other}"),
        }
    }

    #[tokio::test]
    async fn unknown_routes_get_a_json_404() {
        let response = app(None)
            .oneshot(Request::get("/api/v1/nope").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn submit_round_trips_proto3_json() {
        let job = JobId::new().to_string();
        let request_body = format!(
            r#"{{
                "image": "busybox",
                "command": ["run"],
                "priority": 0,
                "job": {{ "value": "{job}" }},
                "quotaEntity": {{ "value": "{}" }}
            }}"#,
            coppice_core::id::QuotaEntityId::new()
        );
        let response = app(None)
            .oneshot(post_json("/api/v1/jobs", &request_body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        // proto3 JSON: camelCase keys, 64-bit ints as strings, typed ids.
        assert_eq!(body["job"]["value"], job.as_str());
        assert_eq!(body["logIndex"], "7");
    }

    #[tokio::test]
    async fn malformed_submit_bodies_are_invalid_argument() {
        let response = app(None)
            .oneshot(post_json("/api/v1/jobs", "{ not json"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn abort_takes_the_job_from_the_path() {
        let job = JobId::new();
        let response = app(None)
            .oneshot(post_json(&format!("/api/v1/jobs/{job}/abort"), "{}"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn abort_rejects_a_body_job_that_contradicts_the_path() {
        let body = format!(r#"{{ "job": {{ "value": "{}" }} }}"#, JobId::new());
        let response = app(None)
            .oneshot(post_json(
                &format!("/api/v1/jobs/{}/abort", JobId::new()),
                &body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn abort_rejects_a_malformed_path_id() {
        let response = app(None)
            .oneshot(post_json("/api/v1/jobs/not-a-job-id/abort", "{}"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn not_leader_maps_to_421_with_a_leader_hint_header() {
        let job = JobId::new();
        let response = app(Some(|| ApiError::NotLeader {
            leader_hint: Some("10.0.0.3:7070".to_string()),
        }))
        .oneshot(post_json(&format!("/api/v1/jobs/{job}/abort"), "{}"))
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(
            response.headers().get(COPPICE_LEADER).unwrap(),
            "10.0.0.3:7070"
        );
        assert_eq!(body_json(response).await["code"], "NOT_LEADER");
    }
}
