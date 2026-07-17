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

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use serde::Deserialize;

use coppice_core::id::{JobId, NodeId, QuotaEntityId};
use coppice_core::time::Timestamp;

use super::dto::{
    self, AbortJobRequest, AbortJobResponse, ConfigureQuotaEntityRequest, SubmitJobRequest,
};
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
        .route("/api/v1/queue/stats", get(get_queue_stats::<P>))
        // Jobs. List/detail/timeline are bounded; usage is eventual
        // (derived samples); logs are provisional until log storage exists.
        .route("/api/v1/jobs", get(list_jobs::<P>).post(submit_job::<P>))
        .route("/api/v1/jobs/:job", get(get_job::<P>))
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
        .route("/api/v1/coordinators", get(get_coordinators::<P>))
        .route(
            "/api/v1/coordinators/:id/logs",
            // Coordinator ids are raft ids: plain u64, not typed uuids (ADR 0024).
            get(unimplemented_id_read::<u64>("GetCoordinatorLogs")),
        )
        // Quota entities. List bounded; detail defaults strong (ADR 0007:
        // configuration reads); configure is the ADR-0023-gated upsert.
        .route(
            "/api/v1/quota-entities",
            get(list_quota_entities::<P>).post(configure_quota_entity::<P>),
        )
        .route("/api/v1/quota-entities/:entity", get(get_quota_entity::<P>))
        // Reserved: ADR 0008 event subscription (SSE, cursor-resumed).
        .route("/api/v1/events", get(unimplemented_read("SubscribeEvents")))
        // Everything unrouted: `/api/*` misses stay JSON 404s; anything
        // else serves the embedded web UI (static assets + SPA fallback,
        // ADR 0031 "Serving the UI").
        .fallback(super::ui::fallback)
        .with_state(plane)
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

/// Default page size when `?limit=` is absent.
const DEFAULT_JOB_LIMIT: u64 = 100;
/// Valid `?limit=` range; out of range is `INVALID_ARGUMENT`, never clamped.
const JOB_LIMIT_RANGE: std::ops::RangeInclusive<u64> = 1..=1000;

/// `GET /api/v1/jobs` list parameters, alongside the shared [`ReadQuery`].
///
/// A separate extractor rather than a flattened `ReadParams`: `serde_urlencoded`
/// (axum's `Query`) does not support `#[serde(flatten)]` for the non-string
/// `min_index`, so the read params ride their own [`ReadQuery`] extractor —
/// the same one every read route uses — and these list-only params ride here.
#[derive(Debug, Default, Deserialize)]
struct ListJobsParams {
    /// URL-encoded JSON [`dto::JobFilter`]; absent matches every job.
    #[serde(default)]
    filter: Option<String>,
    /// Opaque continuation token from a prior response.
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
}

/// `GET /api/v1/jobs` — bounded by default (ADR 0031). The filter AST,
/// cursor, and page size are validated here; the descending keyset scan and
/// projection live in [`super::project::list_jobs`].
async fn list_jobs<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    ReadQuery(read): ReadQuery,
    params: Result<Query<ListJobsParams>, QueryRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Query(params) = params.map_err(|e: QueryRejection| HttpError::invalid(e.body_text()))?;

    let limit = match params.limit {
        None => DEFAULT_JOB_LIMIT,
        Some(n) if JOB_LIMIT_RANGE.contains(&n) => n,
        Some(n) => {
            return Err(HttpError::invalid(format!(
                "limit {n} is out of range {}..={}",
                JOB_LIMIT_RANGE.start(),
                JOB_LIMIT_RANGE.end(),
            )))
        }
    };

    let filter = match &params.filter {
        None => None,
        Some(raw) => {
            let parsed: dto::JobFilter = serde_json::from_str(raw)
                .map_err(|e| HttpError::invalid(format!("invalid filter: {e}")))?;
            parsed.validate().map_err(HttpError::invalid)?;
            Some(parsed)
        }
    };

    let cursor = match &params.cursor {
        None => None,
        Some(token) => Some(dto::JobCursor::parse(token).map_err(HttpError::invalid)?),
    };

    let view = plane
        .read_state(read.into_options(Consistency::Bounded))
        .await?;
    let response = super::project::list_jobs(view.state(), filter.as_ref(), cursor, limit as usize);
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
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

/// `POST /api/v1/quota-entities` — body `ConfigureQuotaEntityRequest`, the
/// create-or-update upsert (ADR 0031's write class). Response echoes the
/// client-minted entity id + `log_index` for read-your-writes, exactly like
/// `SubmitJob`. A cycle / unknown-parent refusal maps to `REJECTED` (409),
/// the normal committed-and-refused outcome.
async fn configure_quota_entity<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    body: Result<Json<ConfigureQuotaEntityRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = body.map_err(bad_body)?;
    let response = plane.configure_quota_entity(request).await?;
    Ok(Json(response))
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
    // Only reads sample the clock — they are not replicated, so a handler
    // may (an *apply* may never: `coppice-state`'s determinism contract).
    // It feeds read-time ages like `oldest_queued_age_seconds`, never
    // anything stored.
    let response = super::project::cluster_overview(
        view.state(),
        plane.cluster_id(),
        Timestamp::now(),
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

/// `GET /api/v1/queue/stats` — bounded by default (ADR 0031). The bare
/// [`dto::QueueStats`] object (the same shape as the overview's `queue`
/// field), with no wrapper: it is already an object, so fields can still be
/// added later. Same derived queue-window source as [`get_overview`].
async fn get_queue_stats<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Bounded))
        .await?;
    let window = plane.queue_window();
    // A read may sample the clock (an apply may not): it feeds the read-time
    // `oldest_queued_age_seconds`, never anything stored.
    let response = super::project::queue_stats(view.state(), Timestamp::now(), &window);
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
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

/// `GET /api/v1/jobs/{job}` — bounded by default (ADR 0031). 404 when the id
/// is not in the read view, exactly as [`get_node`].
async fn get_job<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    IdPath(id): IdPath<JobId>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Bounded))
        .await?;
    // A read may sample the clock (an apply may not): `now` feeds the
    // read-time entity-usage decay, queue age, and penalty product.
    let response = super::project::get_job(view.state(), &id, Timestamp::now())
        .ok_or_else(|| HttpError::not_found(format!("job {id} not found")))?;
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
}

/// `GET /api/v1/quota-entities` — bounded by default (ADR 0031). `Timestamp::now()`
/// decays each entity's usage to read time (a read-time figure, never stored).
async fn list_quota_entities<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Bounded))
        .await?;
    let response = super::project::list_quota_entities(view.state(), Timestamp::now());
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
}

/// `GET /api/v1/quota-entities/{entity}` — **strong** by default (ADR 0031
/// puts it in the ADR 0007 configuration-read class, unlike the bounded list
/// and node reads). 404 when the id is not in the tree, like [`get_node`].
async fn get_quota_entity<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    IdPath(id): IdPath<QuotaEntityId>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Strong))
        .await?;
    let response = super::project::get_quota_entity(view.state(), &id, Timestamp::now())
        .ok_or_else(|| HttpError::not_found(format!("quota entity {id} not found")))?;
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
}

/// `GET /api/v1/coordinators` — local read (ADR 0031). Two sources: the
/// consensus/membership summary (raft-level, from `coordinator_status`) and a
/// replica-local state snapshot (version + object counts). The snapshot rides
/// the read plumbing so the response still carries staleness headers and
/// honours `?consistency=`/`?min_index=`; local defaults to `Eventual` (the
/// latest published view, no consensus round-trip).
///
/// When the consensus handle is not attached, `coordinator_status` is
/// `UNAVAILABLE` (503) and the route fails as a whole — the raft-level view is
/// the point of the endpoint.
async fn get_coordinators<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let summary = plane.coordinator_status()?;
    let view = plane
        .read_state(params.into_options(Consistency::Eventual))
        .await?;
    let response = super::project::coordinator_status(&summary, plane.cluster_id(), view.state());
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
    use crate::{
        ApiError, CoordinatorMemberSummary, CoordinatorSummary, QueueWindow, ReadOptions, ReadView,
        RecentClusterEvents,
    };

    use crate::http::COPPICE_LEADER;

    /// A canned `ControlPlane`: submit echoes the request's job id with a
    /// fixed log index, or fails with the configured error. Reads serve an
    /// empty state, and the derived sources serve whatever the test seeded
    /// (by default: no coverage, like a fresh replica).
    struct StubPlane {
        fail_with: Option<fn() -> ApiError>,
        queue_window: QueueWindow,
        recent: RecentClusterEvents,
        state: coppice_state::StateMachine,
        /// Every consistency class `read_state` was asked for, so a test can
        /// assert a route's default (e.g. the strong quota-entity detail).
        read_consistency: std::sync::Mutex<Vec<Consistency>>,
        /// The seeded raft summary, or `None` to model a control plane with no
        /// consensus handle attached (→ `coordinator_status` is `Unavailable`).
        coordinator: Option<CoordinatorSummary>,
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

        fn coordinator_status(&self) -> Result<CoordinatorSummary, ApiError> {
            self.coordinator
                .clone()
                .ok_or_else(|| ApiError::Unavailable("no consensus handle".into()))
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

        async fn configure_quota_entity(
            &self,
            req: dto::ConfigureQuotaEntityRequest,
        ) -> Result<dto::ConfigureQuotaEntityResponse, ApiError> {
            match self.fail_with {
                Some(make) => Err(make()),
                None => Ok(dto::ConfigureQuotaEntityResponse {
                    entity: req.entity,
                    log_index: 7,
                }),
            }
        }

        async fn read_state(&self, opts: ReadOptions) -> Result<ReadView, ApiError> {
            self.read_consistency.lock().unwrap().push(opts.consistency);
            Ok(ReadView::new(self.state.clone(), 1, 1))
        }
    }

    fn app(fail_with: Option<fn() -> ApiError>) -> Router {
        app_with_state(fail_with, coppice_state::StateMachine::default())
    }

    fn app_with_state(
        fail_with: Option<fn() -> ApiError>,
        state: coppice_state::StateMachine,
    ) -> Router {
        router(Arc::new(StubPlane {
            fail_with,
            queue_window: QueueWindow::default(),
            recent: RecentClusterEvents {
                // ReadView serves applied index 1, so "nothing covered" is
                // the exclusive cursor sitting at it.
                floor_index: 1,
                events: Vec::new(),
            },
            state,
            read_consistency: std::sync::Mutex::default(),
            // No handle by default: coordinator-status tests build their own
            // plane with a seeded summary.
            coordinator: None,
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
            .oneshot(Request::get("/api/v1/session").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = body_json(response).await;
        assert_eq!(body["code"], "UNIMPLEMENTED");
        assert!(body["message"].as_str().unwrap().contains("GetSession"));
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
            body["queue"]["oldest_queued_age_seconds"],
            serde_json::Value::Null
        );
        assert_eq!(body["queue"]["by_state"]["queued"], 0);
        assert_eq!(body["capacity"]["nodes"]["total"], 0);
        // No derived coverage: rates null, and the empty events window still
        // carries its exclusive coverage cursor (ADR 0032).
        assert_eq!(
            body["queue"]["drain_rate_per_minute"],
            serde_json::Value::Null
        );
        assert_eq!(body["recent_events"]["floor_index"], 1);
        assert_eq!(body["recent_events"]["events"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn overview_serves_derived_rates_history_and_recent_events() {
        let job = JobId::new();
        let plane = StubPlane {
            fail_with: None,
            queue_window: QueueWindow {
                buckets: vec![crate::QueueBucket {
                    start: Timestamp::from_micros(60_000_000).expect("in range"),
                    end: Timestamp::from_micros(90_000_000).expect("in range"),
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
                    at: Timestamp::from_micros(90_000_000).expect("in range"),
                    event: coppice_state::Event::JobSubmitted { job },
                }],
            },
            state: coppice_state::StateMachine::default(),
            read_consistency: std::sync::Mutex::default(),
            coordinator: None,
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
        assert_eq!(
            body["queue"]["history"][0]["t"],
            "1970-01-01T00:01:00.000000Z"
        );
        assert_eq!(body["recent_events"]["floor_index"], 5);
        let event = &body["recent_events"]["events"][0];
        assert_eq!(event["index"], 8);
        assert_eq!(event["ordinal"], 0);
        assert_eq!(event["at"], "1970-01-01T00:01:30.000000Z");
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
                Request::get(format!(
                    "/api/v1/jobs/{job}/timeline?consistency=strong&min_index=3"
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = body_json(response).await;
        assert!(body["message"].as_str().unwrap().contains("GetJobTimeline"));
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
        // `max_runtme_seconds` (typo) must not be accepted with the real
        // `max_runtime_seconds` silently defaulting to unbounded.
        let request_body = format!(
            r#"{{
                "image": "busybox",
                "command": ["run"],
                "requests": {{ "cpu_millis": 1000, "memory_bytes": 0, "disk_bytes": 0 }},
                "job": "{}",
                "quota_entity": "{}",
                "max_runtme_seconds": 3600
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

    /// A minimal queued job with a controllable id, for list-endpoint tests.
    fn queued_job(id: JobId) -> coppice_state::JobRecord {
        coppice_state::JobRecord {
            spec: coppice_core::job::Job {
                id,
                image: "busybox".to_string(),
                command: vec!["run".to_string()],
                entrypoint: None,
                requests: coppice_core::resource::Resources::ZERO,
                priority: 0,
                max_runtime: None,
                quota_entity: coppice_core::id::QuotaEntityId::new(),
                retry: Default::default(),
                abort_requested: None,
            },
            state: coppice_core::job::JobState::Queued,
            multiplier: coppice_core::quota::PriorityMultiplier::ONE,
            submitted_at: Timestamp::from_micros(0).unwrap(),
            terminal_at: None,
            retries_used: 0,
            attempts: Vec::new(),
        }
    }

    fn state_with_jobs(ids: &[JobId]) -> coppice_state::StateMachine {
        let mut state = coppice_state::StateMachine::default();
        for id in ids {
            state.jobs.insert(*id, queued_job(*id));
        }
        state
    }

    #[tokio::test]
    async fn list_jobs_serves_matches_newest_first_with_headers() {
        let lo: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let hi: JobId = "job-00000000-0000-0000-0000-000000000002".parse().unwrap();
        let response = app_with_state(None, state_with_jobs(&[lo, hi]))
            .oneshot(Request::get("/api/v1/jobs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // Bounded reads carry the staleness headers, like every other read.
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_APPLIED_INDEX));
        let body = body_json(response).await;
        assert_eq!(body["jobs"][0]["id"], hi.to_string());
        assert_eq!(body["jobs"][1]["id"], lo.to_string());
        // Scan reached the low end: cursor is explicit null, never omitted.
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn list_jobs_applies_a_url_encoded_json_filter() {
        let a: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let b: JobId = "job-00000000-0000-0000-0000-000000000002".parse().unwrap();
        let state = state_with_jobs(&[a, b]);
        // Filter by a single id — the value is URL-encoded JSON.
        let filter = format!(r#"{{"id":{{"in":["{a}"]}}}}"#);
        let uri = format!("/api/v1/jobs?filter={}", urlencoding_encode(&filter));
        let response = app_with_state(None, state)
            .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
        assert_eq!(body["jobs"][0]["id"], a.to_string());
    }

    /// Percent-encode the query-value bytes we care about (no dep on a URL
    /// crate for a test helper).
    fn urlencoding_encode(s: &str) -> String {
        let mut out = String::new();
        for byte in s.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(byte as char)
                }
                other => out.push_str(&format!("%{other:02X}")),
            }
        }
        out
    }

    #[tokio::test]
    async fn list_jobs_rejects_bad_filters_cursors_and_limits() {
        // (query, why) — every case must be INVALID_ARGUMENT.
        let cases = [
            // Malformed JSON.
            "/api/v1/jobs?filter=%7Bnot-json",
            // An empty `any` list (parses, fails validation).
            "/api/v1/jobs?filter=%7B%22any%22%3A%5B%5D%7D",
            // An unknown phase value.
            "/api/v1/jobs?filter=%7B%22phase%22%3A%7B%22in%22%3A%5B%22nope%22%5D%7D%7D",
            // A cursor that is not `v1:` + a valid job id.
            "/api/v1/jobs?cursor=v2%3Ajob-00000000-0000-0000-0000-000000000001",
            "/api/v1/jobs?cursor=garbage",
            // Limit out of range (never clamped).
            "/api/v1/jobs?limit=0",
            "/api/v1/jobs?limit=1001",
        ];
        for uri in cases {
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
    async fn list_jobs_rejects_a_filter_exceeding_the_node_cap() {
        // `all` + 64 leaves = 65 nodes > 64.
        let leaves = std::iter::repeat_n(r#"{"search":"x"}"#, 64)
            .collect::<Vec<_>>()
            .join(",");
        let filter = format!(r#"{{"all":[{leaves}]}}"#);
        let uri = format!("/api/v1/jobs?filter={}", urlencoding_encode(&filter));
        let response = app(None)
            .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn list_jobs_still_validates_the_consistency_parameter() {
        let response = app(None)
            .oneshot(
                Request::get("/api/v1/jobs?consistency=bogus")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    /// An `Arc<StubPlane>` kept alongside the router, so a test can both
    /// drive the app and inspect what the plane was asked (e.g. the read
    /// consistency a route defaulted to).
    fn stub_plane(state: coppice_state::StateMachine) -> Arc<StubPlane> {
        Arc::new(StubPlane {
            fail_with: None,
            queue_window: QueueWindow::default(),
            recent: RecentClusterEvents {
                floor_index: 1,
                events: Vec::new(),
            },
            state,
            read_consistency: std::sync::Mutex::default(),
            coordinator: None,
        })
    }

    /// A state machine holding one quota entity (root, at-quota) so the list
    /// and detail reads project a real node.
    fn state_with_entity(id: QuotaEntityId) -> coppice_state::StateMachine {
        let mut state = coppice_state::StateMachine::default();
        state.quota_entities.insert(
            id,
            coppice_state::QuotaEntity {
                parent: None,
                name: "root".to_string(),
                quota: coppice_core::quota::CostUnits(1_000_000),
                usage: coppice_core::quota::UsageState::new(Timestamp::from_micros(0).unwrap()),
                created_at: Timestamp::from_micros(1_000_000).unwrap(),
                updated_at: Timestamp::from_micros(1_000_000).unwrap(),
            },
        );
        state
    }

    #[tokio::test]
    async fn list_quota_entities_returns_an_envelope_with_headers() {
        let id = QuotaEntityId::new();
        let response = app_with_state(None, state_with_entity(id))
            .oneshot(
                Request::get("/api/v1/quota-entities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_APPLIED_INDEX));
        let body = body_json(response).await;
        // Object envelope, never a bare array (ADR 0031).
        assert_eq!(body["entities"][0]["id"], id.to_string());
        assert_eq!(body["entities"][0]["queued_count"], 0);
        // SSO provenance is omitted, not null.
        assert!(body["entities"][0].get("origin").is_none());
    }

    #[tokio::test]
    async fn list_quota_entities_defaults_to_a_bounded_read() {
        let plane = stub_plane(coppice_state::StateMachine::default());
        let response = router(plane.clone())
            .oneshot(
                Request::get("/api/v1/quota-entities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            plane.read_consistency.lock().unwrap().last(),
            Some(&Consistency::Bounded)
        );
    }

    #[tokio::test]
    async fn get_quota_entity_returns_not_found_for_missing() {
        let entity = QuotaEntityId::new();
        let response = app(None)
            .oneshot(
                Request::get(format!("/api/v1/quota-entities/{entity}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn get_quota_entity_defaults_to_a_strong_read() {
        // ADR 0031 puts the detail read in the configuration-read class:
        // strong by default, unlike the bounded list and node reads.
        let id = QuotaEntityId::new();
        let plane = stub_plane(state_with_entity(id));
        let response = router(plane.clone())
            .oneshot(
                Request::get(format!("/api/v1/quota-entities/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            plane.read_consistency.lock().unwrap().last(),
            Some(&Consistency::Strong)
        );
        let body = body_json(response).await;
        assert_eq!(body["entity"]["id"], id.to_string());
        assert_eq!(body["chain"][0]["id"], id.to_string());
        assert_eq!(body["stats"]["charged_ucu_24h"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn get_quota_entity_rejects_a_malformed_path_id() {
        let response = app(None)
            .oneshot(
                Request::get("/api/v1/quota-entities/not-an-entity")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn configure_quota_entity_echoes_the_entity_and_log_index() {
        let entity = QuotaEntityId::new();
        let body = format!(
            r#"{{ "entity": "{entity}", "parent": null, "name": "team", "quota_ucu": 1000 }}"#
        );
        let response = app(None)
            .oneshot(post_json("/api/v1/quota-entities", &body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["entity"], entity.to_string());
        assert_eq!(body["log_index"], 7);
    }

    #[tokio::test]
    async fn configure_quota_entity_maps_a_rejection_to_409() {
        let entity = QuotaEntityId::new();
        let response = app(Some(|| {
            ApiError::Rejected(coppice_state::RejectionReason::QuotaEntityCycle(
                QuotaEntityId::new(),
            ))
        }))
        .oneshot(post_json(
            "/api/v1/quota-entities",
            &format!(r#"{{ "entity": "{entity}", "name": "team", "quota_ucu": 1000 }}"#),
        ))
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(response).await["code"], "REJECTED");
    }

    #[tokio::test]
    async fn configure_quota_entity_with_an_unknown_field_is_invalid_argument() {
        let entity = QuotaEntityId::new();
        // camelCase `quotaUcu` must not be accepted alongside `quota_ucu`.
        let body = format!(
            r#"{{ "entity": "{entity}", "name": "team", "quota_ucu": 1000, "quotaUcu": 2000 }}"#
        );
        let response = app(None)
            .oneshot(post_json("/api/v1/quota-entities", &body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn queue_stats_answers_from_the_replica_with_staleness_headers() {
        let response = app(None)
            .oneshot(
                Request::get("/api/v1/queue/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // Bounded reads carry the staleness headers, like every other read.
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_APPLIED_INDEX));
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_COMMITTED_INDEX));

        let body = body_json(response).await;
        // The bare QueueStats object, no wrapper — the same shape as the
        // overview's `queue` field.
        assert_eq!(body["depth"], 0);
        assert_eq!(body["by_state"]["queued"], 0);
        assert_eq!(body["oldest_queued_age_seconds"], serde_json::Value::Null);
        // No derived coverage on a fresh replica: rates null, history empty.
        assert_eq!(body["drain_rate_per_minute"], serde_json::Value::Null);
        assert_eq!(body["history"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn queue_stats_counts_a_seeded_queue() {
        let lo: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let hi: JobId = "job-00000000-0000-0000-0000-000000000002".parse().unwrap();
        let response = app_with_state(None, state_with_jobs(&[lo, hi]))
            .oneshot(
                Request::get("/api/v1/queue/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["depth"], 2);
        assert_eq!(body["by_state"]["queued"], 2);
    }

    #[tokio::test]
    async fn queue_stats_validates_the_consistency_parameter() {
        let response = app(None)
            .oneshot(
                Request::get("/api/v1/queue/stats?consistency=bogus")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn get_job_returns_not_found_for_missing_job() {
        let job = JobId::new();
        let response = app(None)
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn get_job_rejects_a_malformed_path_id() {
        let response = app(None)
            .oneshot(
                Request::get("/api/v1/jobs/not-a-job-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn get_job_serves_a_queued_job_with_headers() {
        let job: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let response = app_with_state(None, state_with_jobs(&[job]))
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_APPLIED_INDEX));
        let body = body_json(response).await;
        assert_eq!(body["id"], job.to_string());
        assert_eq!(body["state"], "queued");
        // A queued job carries its explainer and no accrual.
        assert!(body["queue"].is_object());
        // Ranking fields are absent, not null — see the DTO doc.
        assert!(body["queue"].get("rank").is_none());
        assert!(body["queue"]["penalty_product"].is_number());
        assert_eq!(body["accrual"], serde_json::Value::Null);
        // Cost is always present; absent-data fields are explicit null.
        assert_eq!(body["cost"]["actual_ucu"], serde_json::Value::Null);
        assert_eq!(body["cost"]["true_up"], serde_json::Value::Null);
        // state_since falls back to submission time for a queued job.
        assert_eq!(body["state_since"], body["submitted_at"]);
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

    // ---- coordinators -----------------------------------------------------

    /// A control plane with a seeded raft summary and state, wired (a handle
    /// is present).
    fn coordinator_app(
        coordinator: CoordinatorSummary,
        state: coppice_state::StateMachine,
    ) -> Router {
        router(Arc::new(StubPlane {
            fail_with: None,
            queue_window: QueueWindow::default(),
            recent: RecentClusterEvents {
                floor_index: 1,
                events: Vec::new(),
            },
            state,
            read_consistency: std::sync::Mutex::default(),
            coordinator: Some(coordinator),
        }))
    }

    /// A three-member cluster: local leader (id 1), a follower (id 2), and a
    /// learner (id 3), from the perspective of the leader.
    fn seeded_summary() -> CoordinatorSummary {
        CoordinatorSummary {
            local_id: 1,
            leader: Some(1),
            term: 5,
            known_committed: 100,
            last_applied: 100,
            snapshot_last_index: Some(64),
            members: vec![
                CoordinatorMemberSummary {
                    id: 1,
                    addr: "10.0.0.1:9001".to_string(),
                    voter: true,
                    matched_index: Some(100),
                },
                CoordinatorMemberSummary {
                    id: 2,
                    addr: "10.0.0.2:9001".to_string(),
                    voter: true,
                    matched_index: Some(90),
                },
                CoordinatorMemberSummary {
                    id: 3,
                    addr: "10.0.0.3:9001".to_string(),
                    voter: false,
                    matched_index: None,
                },
            ],
        }
    }

    #[tokio::test]
    async fn coordinators_project_roles_lag_and_snapshot() {
        let response = coordinator_app(seeded_summary(), coppice_state::StateMachine::default())
            .oneshot(
                Request::get("/api/v1/coordinators")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // A local read still carries the staleness headers.
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_APPLIED_INDEX));

        let body = body_json(response).await;
        assert_eq!(body["leader"], 1);
        assert_eq!(body["term"], 5);
        assert_eq!(body["known_committed"], 100);
        assert_eq!(body["last_applied"], 100);

        // Roles derive from leader id + voter flag.
        let members = body["members"].as_array().unwrap();
        assert_eq!(members.len(), 3);
        assert_eq!(members[0]["role"], "leader");
        assert_eq!(members[1]["role"], "follower");
        assert_eq!(members[2]["role"], "learner");

        // last_applied: exact for the local leader, null for peers.
        assert_eq!(members[0]["last_applied"], 100);
        assert_eq!(members[1]["last_applied"], serde_json::Value::Null);
        assert_eq!(members[2]["last_applied"], serde_json::Value::Null);

        // Lag math: known_committed − matched, leader-only.
        assert_eq!(members[0]["replication_lag_entries"], 0); // 100 − 100
        assert_eq!(members[1]["replication_lag_entries"], 10); // 100 − 90
                                                               // The learner has no matched entry → null, never a fabricated 0.
        assert_eq!(
            members[2]["replication_lag_entries"],
            serde_json::Value::Null
        );

        // Snapshot: only the covered index is real; size/time are explicit null.
        assert_eq!(body["snapshot"]["last_included_index"], 64);
        assert_eq!(body["snapshot"]["entries_since_snapshot"], 36); // 100 − 64
        assert_eq!(body["snapshot"]["size_bytes"], serde_json::Value::Null);
        assert_eq!(body["snapshot"]["taken_at"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn coordinators_omit_the_invented_host_and_last_seen_fields() {
        let body = body_json(
            coordinator_app(seeded_summary(), coppice_state::StateMachine::default())
                .oneshot(
                    Request::get("/api/v1/coordinators")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        let member = &body["members"][0];
        // These have no data source; the DTO omits them rather than inventing.
        assert!(member.get("host").is_none());
        assert!(member.get("last_seen").is_none());
    }

    #[tokio::test]
    async fn coordinators_count_the_replicated_state() {
        let a: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let b: JobId = "job-00000000-0000-0000-0000-000000000002".parse().unwrap();
        let mut state = state_with_jobs(&[a, b]);
        state.version = 42;

        let body = body_json(
            coordinator_app(seeded_summary(), state)
                .oneshot(
                    Request::get("/api/v1/coordinators")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        // state_version is the applied-command count, not the raft log index.
        assert_eq!(body["state_version"], 42);
        assert_eq!(body["state_counts"]["jobs"], 2);
        assert_eq!(body["state_counts"]["attempts"], 0);
        assert_eq!(body["state_counts"]["allocations"], 0);
        assert_eq!(body["state_counts"]["nodes"], 0);
        assert_eq!(body["state_counts"]["quota_entities"], 0);
    }

    #[tokio::test]
    async fn coordinators_serve_a_null_snapshot_before_the_first_one() {
        let mut summary = seeded_summary();
        summary.snapshot_last_index = None;
        let body = body_json(
            coordinator_app(summary, coppice_state::StateMachine::default())
                .oneshot(
                    Request::get("/api/v1/coordinators")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        // No snapshot yet: the whole object is null, never a zeroed shape.
        assert_eq!(body["snapshot"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn coordinators_are_unavailable_without_a_consensus_handle() {
        // `app(None)` builds a plane with `coordinator: None` — no handle wired.
        let response = app(None)
            .oneshot(
                Request::get("/api/v1/coordinators")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body_json(response).await["code"], "UNAVAILABLE");
    }

    #[tokio::test]
    async fn coordinators_still_validate_the_consistency_parameter() {
        let response = coordinator_app(seeded_summary(), coppice_state::StateMachine::default())
            .oneshot(
                Request::get("/api/v1/coordinators?consistency=bogus")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }
}
