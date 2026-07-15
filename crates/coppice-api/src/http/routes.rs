//! The `/api/v1` route map (ADR 0031) and its implemented handlers.
//!
//! One route per `CoppiceApi` method in `web/src/api/client.ts`, plus the
//! two writes `ControlPlane` already serves. Reads are stubbed with
//! [`unimplemented`] until their endpoint lands; implementing one means:
//! response DTOs in [`super::dto`] (shape mirrors `web/src/api/types.ts`),
//! a projection in [`super::project`], and swapping the stub for a real
//! handler here — routing, errors, and consistency parameters are already
//! decided.

use std::future::ready;
use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use coppice_core::id::{JobId, NodeId, QuotaEntityId};

use super::dto::{AbortJobRequest, AbortJobResponse, SubmitJobRequest};
use crate::{Consistency, ControlPlane};

use super::error::HttpError;
use super::extract::{IdPath, ReadIndexes, ReadQuery};

/// Build the client-listener router around a [`ControlPlane`].
///
/// Consistency defaults per route are the ADR 0031 table; they become code
/// (`ReadParams::class(default)`) as each read handler is implemented.
pub fn router<P: ControlPlane>(plane: Arc<P>) -> Router {
    Router::new()
        // Session / auth (ADR 0022) — local read, no raft involvement.
        .route("/api/v1/session", get(unimplemented_read("GetSession")))
        // Cluster overview — bounded reads.
        .route("/api/v1/overview", get(get_overview::<P>))
        .route(
            "/api/v1/queue/stats",
            get(unimplemented_read("GetQueueStats")),
        )
        // Jobs. List/detail/timeline are bounded; usage is eventual
        // (derived samples); logs are provisional until log storage exists.
        .route(
            "/api/v1/jobs",
            get(unimplemented_read("ListJobs")).post(submit_job::<P>),
        )
        .route(
            "/api/v1/jobs/:job",
            get(unimplemented_id_read::<JobId>("GetJob")),
        )
        .route("/api/v1/jobs/:job/abort", post(abort_job::<P>))
        .route(
            "/api/v1/jobs/:job/timeline",
            get(unimplemented_id_read::<JobId>("GetJobTimeline")),
        )
        .route(
            "/api/v1/jobs/:job/usage",
            get(unimplemented_id_read::<JobId>("GetJobUsage")),
        )
        .route(
            "/api/v1/jobs/:job/logs",
            get(unimplemented_id_read::<JobId>("GetJobLogs")),
        )
        // Nodes. List/detail bounded; utilization/history eventual; logs
        // provisional.
        .route("/api/v1/nodes", get(list_nodes::<P>))
        .route("/api/v1/nodes/:node", get(get_node::<P>))
        .route(
            "/api/v1/nodes/:node/utilization",
            get(unimplemented_id_read::<NodeId>("GetNodeUtilization")),
        )
        .route(
            "/api/v1/nodes/:node/history",
            get(unimplemented_id_read::<NodeId>("GetNodeHistory")),
        )
        .route(
            "/api/v1/nodes/:node/logs",
            get(unimplemented_id_read::<NodeId>("GetNodeLogs")),
        )
        // Coordinators — local status read; logs provisional.
        .route(
            "/api/v1/coordinators",
            get(unimplemented_read("GetCoordinatorStatus")),
        )
        .route(
            "/api/v1/coordinators/:id/logs",
            // Coordinator ids are raft ids: plain u64, not typed uuids (ADR 0024).
            get(unimplemented_id_read::<u64>("GetCoordinatorLogs")),
        )
        // Quota entities. List bounded; detail defaults strong (ADR 0007:
        // configuration reads); configure is the ADR-0023-gated upsert.
        .route(
            "/api/v1/quota-entities",
            get(unimplemented_read("ListQuotaEntities"))
                .post(unimplemented("ConfigureQuotaEntity")),
        )
        .route(
            "/api/v1/quota-entities/:entity",
            get(unimplemented_id_read::<QuotaEntityId>("GetQuotaEntity")),
        )
        // Reserved: ADR 0008 event subscription (SSE, cursor-resumed).
        .route("/api/v1/events", get(unimplemented_read("SubscribeEvents")))
        // Everything unrouted: `/api/*` misses stay JSON 404s; anything
        // else serves the embedded web UI (static assets + SPA fallback,
        // ADR 0031 "Serving the UI").
        .fallback(super::ui::fallback)
        .with_state(plane)
}

/// Stub for an unimplemented write route: routed (so the path is claimed
/// and typos 404 distinctly) but answering `501 UNIMPLEMENTED` with the
/// endpoint name.
fn unimplemented(
    endpoint: &'static str,
) -> impl Fn() -> std::future::Ready<HttpError> + Clone + Send + 'static {
    move || ready(HttpError::unimplemented(endpoint))
}

/// Stub for an unimplemented read route. Extracting [`ReadQuery`] makes the
/// ADR 0007 parameter contract mechanical even before the endpoint exists:
/// `?consistency=bogus` is `INVALID_ARGUMENT` on every read, and the
/// eventual real handler inherits the extractor instead of re-adding it.
fn unimplemented_read(
    endpoint: &'static str,
) -> impl Fn(ReadQuery) -> std::future::Ready<HttpError> + Clone + Send + 'static {
    move |ReadQuery(_)| ready(HttpError::unimplemented(endpoint))
}

/// [`unimplemented_read`] for routes with a typed id path segment: the id
/// is validated ([`IdPath`]) before the 501, so malformed ids are
/// `INVALID_ARGUMENT` per the contract rather than leaking the stub.
fn unimplemented_id_read<T>(
    endpoint: &'static str,
) -> impl Fn(IdPath<T>, ReadQuery) -> std::future::Ready<HttpError> + Clone + Send + 'static
where
    T: std::str::FromStr + Send + 'static,
    T::Err: std::fmt::Display,
{
    move |IdPath(_), ReadQuery(_)| ready(HttpError::unimplemented(endpoint))
}

/// `POST /api/v1/jobs` — body `SubmitJobRequest`, response
/// `SubmitJobResponse` (echoed client-minted id + `log_index` for a
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
    IdPath(job): IdPath<JobId>,
    body: Result<Json<AbortJobRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(mut request) = body.map_err(bad_body)?;
    match request.job {
        None => request.job = Some(job),
        Some(body_job) if body_job != job => {
            return Err(HttpError::invalid(
                "body job id does not match the path job id",
            ));
        }
        Some(_) => {}
    }
    plane.abort_job(request).await?;
    Ok(Json(AbortJobResponse {}))
}

/// Events served in the overview's `recent_events` window — a display
/// window, deliberately smaller than the ring behind it (a client wanting
/// more history uses the timeline/subscription endpoints).
const RECENT_EVENTS_LIMIT: usize = 50;

/// `GET /api/v1/overview` — bounded by default (ADR 0031) for the
/// replicated-state fields; the rates/history and `recent_events` are
/// derived, replica-local reads (ADR 0032).
async fn get_overview<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Bounded))
        .await?;
    let window = plane.queue_window();
    let recent = plane.recent_events(RECENT_EVENTS_LIMIT).await;
    let response = super::project::cluster_overview(
        view.state(),
        plane.cluster_id(),
        now_us(),
        &window,
        &recent,
    );
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
}

/// Wall-clock now, in microseconds.
///
/// Only reads sample the clock — they are not replicated, so a handler may
/// (an *apply* may never: `coppice-state`'s determinism contract). It feeds
/// read-time ages like `oldest_queued_age_us`, never anything stored.
fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// `GET /api/v1/nodes` — bounded by default (ADR 0031).
async fn list_nodes<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Bounded))
        .await?;
    let response = super::project::list_nodes(view.state());
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
}

/// `GET /api/v1/nodes/{node}` — bounded by default (ADR 0031).
async fn get_node<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    IdPath(id): IdPath<NodeId>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Bounded))
        .await?;
    let response = super::project::get_node(view.state(), &id)
        .ok_or_else(|| HttpError::not_found(format!("node {id} not found")))?;
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
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

    use super::super::dto::SubmitJobResponse;
    use crate::{ApiError, QueueWindow, ReadOptions, ReadView, RecentClusterEvents};

    use crate::http::COPPICE_LEADER;

    /// A canned `ControlPlane`: submit echoes the request's job id with a
    /// fixed log index, or fails with the configured error. Reads serve an
    /// empty state, and the derived sources serve whatever the test seeded
    /// (by default: no coverage, like a fresh replica).
    struct StubPlane {
        fail_with: Option<fn() -> ApiError>,
        queue_window: QueueWindow,
        recent: RecentClusterEvents,
    }

    const STUB_CLUSTER: &str = "cluster-00000000-0000-0000-0000-000000000009";

    impl ControlPlane for StubPlane {
        fn cluster_id(&self) -> coppice_core::id::ClusterId {
            STUB_CLUSTER.parse().unwrap()
        }

        fn queue_window(&self) -> QueueWindow {
            self.queue_window.clone()
        }

        async fn recent_events(&self, limit: usize) -> RecentClusterEvents {
            let mut recent = self.recent.clone();
            recent.events.truncate(limit);
            recent
        }

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

        async fn read_state(&self, _opts: ReadOptions) -> Result<ReadView, ApiError> {
            Ok(ReadView::new(coppice_state::StateMachine::default(), 1, 1))
        }
    }

    fn app(fail_with: Option<fn() -> ApiError>) -> Router {
        router(Arc::new(StubPlane {
            fail_with,
            queue_window: QueueWindow::default(),
            recent: RecentClusterEvents {
                // ReadView serves applied index 1, so "nothing covered" is a
                // floor just above it.
                floor_index: 2,
                events: Vec::new(),
            },
        }))
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
            .oneshot(
                Request::get("/api/v1/queue/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = body_json(response).await;
        assert_eq!(body["code"], "UNIMPLEMENTED");
        assert!(body["message"].as_str().unwrap().contains("GetQueueStats"));
    }

    #[tokio::test]
    async fn overview_answers_from_the_replica_and_its_cluster_identity() {
        let response = app(None)
            .oneshot(
                Request::get("/api/v1/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // Bounded reads carry their staleness bound, like every other read.
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_APPLIED_INDEX));

        let body = body_json(response).await;
        // The cluster identity comes from the replica's config, not the view:
        // an empty state machine still knows which cluster it belongs to.
        assert_eq!(body["cluster_id"], STUB_CLUSTER);
        assert_eq!(body["queue"]["depth"], 0);
        assert_eq!(
            body["queue"]["oldest_queued_age_us"],
            serde_json::Value::Null
        );
        assert_eq!(body["queue"]["by_state"]["queued"], 0);
        assert_eq!(body["capacity"]["nodes"]["total"], 0);
        // No derived coverage: rates null, and the empty events window still
        // says how far back it would have covered (ADR 0032).
        assert_eq!(
            body["queue"]["drain_rate_per_minute"],
            serde_json::Value::Null
        );
        assert_eq!(body["recent_events"]["floor_index"], 2);
        assert_eq!(body["recent_events"]["events"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn overview_serves_derived_rates_history_and_recent_events() {
        let job = JobId::new();
        let plane = StubPlane {
            fail_with: None,
            queue_window: QueueWindow {
                bucket_us: 30_000_000,
                buckets: vec![crate::QueueBucket {
                    start_us: 60_000_000,
                    depth: 4,
                    arrivals: 2,
                    drains: 1,
                }],
            },
            recent: RecentClusterEvents {
                floor_index: 5,
                events: vec![crate::StampedEvent {
                    index: 8,
                    ordinal: 0,
                    at_us: 90_000_000,
                    event: coppice_state::Event::JobSubmitted { job },
                }],
            },
        };
        let response = router(Arc::new(plane))
            .oneshot(
                Request::get("/api/v1/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_json(response).await;
        assert_eq!(body["queue"]["arrival_rate_per_minute"], 4.0);
        assert_eq!(body["queue"]["drain_rate_per_minute"], 2.0);
        assert_eq!(body["queue"]["history"][0]["t_us"], 60_000_000);
        assert_eq!(body["recent_events"]["floor_index"], 5);
        let event = &body["recent_events"]["events"][0];
        assert_eq!(event["index"], 8);
        assert_eq!(event["ordinal"], 0);
        assert_eq!(event["at_us"], 90_000_000);
        assert_eq!(event["kind"], "job_submitted");
        assert_eq!(event["job"], job.to_string());
    }

    #[tokio::test]
    async fn list_nodes_returns_ok_with_empty_state() {
        let response = app(None)
            .oneshot(Request::get("/api/v1/nodes").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        // The DTO contract: empty lists are explicit `[]`, never omitted.
        assert_eq!(body["nodes"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn list_nodes_carries_staleness_headers() {
        let response = app(None)
            .oneshot(Request::get("/api/v1/nodes").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_APPLIED_INDEX));
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_COMMITTED_INDEX));
    }

    #[tokio::test]
    async fn get_node_returns_not_found_for_missing_node() {
        let node = coppice_core::id::NodeId::new();
        let response = app(None)
            .oneshot(
                Request::get(format!("/api/v1/nodes/{node}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn reads_validate_consistency_parameter() {
        // Bogus consistency is INVALID_ARGUMENT on both implemented and
        // stub endpoints.
        for uri in [
            "/api/v1/nodes?consistency=bogus",
            "/api/v1/overview?consistency=bogus",
        ] {
            let response = app(None)
                .oneshot(Request::get(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            assert_eq!(
                body_json(response).await["code"],
                "INVALID_ARGUMENT",
                "{uri}"
            );
        }
    }

    #[tokio::test]
    async fn stub_reads_validate_typed_path_ids_before_answering_501() {
        for uri in [
            "/api/v1/jobs/not-a-job-id",
            "/api/v1/coordinators/seven/logs",
        ] {
            let response = app(None)
                .oneshot(Request::get(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            assert_eq!(
                body_json(response).await["code"],
                "INVALID_ARGUMENT",
                "{uri}"
            );
        }
    }

    #[tokio::test]
    async fn well_formed_stub_reads_answer_501() {
        let job = JobId::new();
        let response = app(None)
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}?consistency=strong&min_index=3"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = body_json(response).await;
        assert!(body["message"].as_str().unwrap().contains("GetJob"));
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
    async fn submit_round_trips_the_dto_json() {
        let job = JobId::new().to_string();
        let request_body = format!(
            r#"{{
                "image": "busybox",
                "command": ["run"],
                "priority": 0,
                "requests": {{ "cpu_millis": 1000, "memory_bytes": 0, "disk_bytes": 0 }},
                "job": "{job}",
                "quota_entity": "{}"
            }}"#,
            coppice_core::id::QuotaEntityId::new()
        );
        let response = app(None)
            .oneshot(post_json("/api/v1/jobs", &request_body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        // DTO contract: snake_case keys, bare typed-string ids, integers
        // as JSON numbers.
        assert_eq!(body["job"], job.as_str());
        assert_eq!(body["log_index"], 7);
    }

    #[tokio::test]
    async fn submit_with_an_unknown_field_is_invalid_argument() {
        // `max_runtme_us` (typo) must not be accepted with the real
        // `max_runtime_us` silently defaulting to unbounded.
        let request_body = format!(
            r#"{{
                "image": "busybox",
                "command": ["run"],
                "requests": {{ "cpu_millis": 1000, "memory_bytes": 0, "disk_bytes": 0 }},
                "job": "{}",
                "quota_entity": "{}",
                "max_runtme_us": 3600000000
            }}"#,
            JobId::new(),
            coppice_core::id::QuotaEntityId::new()
        );
        let response = app(None)
            .oneshot(post_json("/api/v1/jobs", &request_body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn submit_missing_a_required_field_is_invalid_argument() {
        // No `requests` — the DTO owns required-ness, so this fails
        // deserialization rather than silently defaulting.
        let request_body = format!(
            r#"{{
                "image": "busybox",
                "command": ["run"],
                "job": "{}",
                "quota_entity": "{}"
            }}"#,
            JobId::new(),
            coppice_core::id::QuotaEntityId::new()
        );
        let response = app(None)
            .oneshot(post_json("/api/v1/jobs", &request_body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
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
        let body = format!(r#"{{ "job": "{}" }}"#, JobId::new());
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
