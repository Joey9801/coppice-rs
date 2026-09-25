//! Wire-path round-trip tests.
//!
//! Every test here stands up a small, canned in-process axum router, points
//! the real [`Client`] at it, drives one call (or a small sequence of
//! calls), and asserts on what the client actually put on the wire and what
//! it made of the answer. Nothing here mocks the client itself — it is the
//! real HTTP path, exercised against a server this crate controls, so a
//! wrong path, a misspelled query key, a missing header, or a
//! misclassified error is caught the same way a real coordinator would
//! surface it.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Query, Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post, put};
use axum::{Json, Router};

use coppice_client::{
    paths, AbortJobRequest, AttemptId, BearerToken, BoxError, Client, Consistency, Error,
    ErrorCode, FollowOptions, JobFilter, JobId, JobMetadata, JobPhase, JobStateKind,
    ListJobsParams, LogOrder, LogStreamName, LogsParams, NodeId, QuotaEntityId, ReadOptions,
    ReplaceJobMetadataRequest, Resources, SubmitJobRequest, Timestamp, TokenProvider,
    UpdateJobMetadataRequest, UsageParams,
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Bind `router` on an ephemeral loopback port and return its base URL.
async fn spawn(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}

/// The ADR 0031 wire error body.
fn error_body(code: &str, message: &str) -> serde_json::Value {
    serde_json::json!({ "code": code, "message": message })
}

/// One request the canned router saw.
#[derive(Debug, Clone)]
struct Captured {
    method: Method,
    path: String,
    query: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

type CaptureStore = Arc<Mutex<Vec<Captured>>>;

/// The decoded query parameters of each request a canned handler received —
/// what the query-encoding tests assert on.
type QueryStore = Arc<Mutex<Vec<HashMap<String, String>>>>;

fn capture_store() -> CaptureStore {
    Arc::new(Mutex::new(Vec::new()))
}

/// Records every request's method, path, raw query string, headers and body
/// before letting it through to the real handler — this is how the tests
/// below assert on what the client actually sent.
async fn capture_mw(State(store): State<CaptureStore>, req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();
    let (parts, body) = req.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("buffering a canned request body");
    store.lock().unwrap().push(Captured {
        method,
        path: uri.path().to_string(),
        query: uri.query().unwrap_or("").to_string(),
        headers,
        body: bytes.to_vec(),
    });
    let req = Request::from_parts(parts, axum::body::Body::from(bytes));
    next.run(req).await
}

/// Wrap `router` so every request it serves is recorded into `store`.
fn with_capture(router: Router, store: CaptureStore) -> Router {
    router.layer(middleware::from_fn_with_state(store, capture_mw))
}

/// A 2xx JSON response, with extra headers (e.g. the read-index headers).
fn json_with_headers(body: serde_json::Value, headers: &[(&str, &str)]) -> Response {
    let mut builder = Response::builder().status(StatusCode::OK);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap()
}

/// A non-2xx response carrying an arbitrary body and headers, exactly as a
/// misbehaving upstream might send it.
fn status_with_body(status: u16, body: &str, headers: &[(&str, &str)]) -> Response {
    let mut builder = Response::builder().status(StatusCode::from_u16(status).unwrap());
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder
        .body(axum::body::Body::from(body.to_string()))
        .unwrap()
}

/// Serve the JSON values in `responses` one per call, in order, repeating the
/// last one once exhausted — the scripting device behind every pagination
/// and follow test below.
fn sequence(
    responses: Vec<serde_json::Value>,
) -> impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Json<serde_json::Value>> + Send>>
       + Clone {
    let responses = Arc::new(responses);
    let index = Arc::new(Mutex::new(0usize));
    move || {
        let responses = responses.clone();
        let index = index.clone();
        Box::pin(async move {
            let mut i = index.lock().unwrap();
            let value = responses[(*i).min(responses.len() - 1)].clone();
            *i += 1;
            Json(value)
        })
    }
}

// ---------------------------------------------------------------------------
// Fixtures — minimal, but shape-complete: every non-`Option` field a
// response type requires is present, and every field routed through a
// custom `deserialize_with` (the `_seconds` durations, `resume_cursor`) is
// present even when its value is `null`.
// ---------------------------------------------------------------------------

fn quota_id() -> QuotaEntityId {
    "quota-00000000-0000-0000-0000-000000000001"
        .parse()
        .unwrap()
}

fn healthz_json() -> serde_json::Value {
    serde_json::json!({ "status": "ok" })
}

fn session_json() -> serde_json::Value {
    serde_json::json!({
        "principal": "anonymous", "groups": [], "auth_method": "open", "name": null,
        "email": null, "bindings": [], "implicit_admin": true
    })
}

fn auth_config_json() -> serde_json::Value {
    serde_json::json!({ "mode": "open" })
}

fn authorization_json() -> serde_json::Value {
    serde_json::json!({ "groups_claim": "groups", "bindings": [] })
}

fn queue_stats_json() -> serde_json::Value {
    serde_json::json!({
        "depth": 0, "accruing": 0, "drain_rate_per_minute": null,
        "arrival_rate_per_minute": null, "oldest_queued_age_seconds": null,
        "by_state": {}, "history": []
    })
}

fn cluster_capacity_json() -> serde_json::Value {
    serde_json::json!({
        "nodes": { "total": 0, "schedulable": 0, "lost": 0 },
        "capacity": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 },
        "allocated": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 },
        "used": null, "reporting_nodes": 0, "total_nodes": 0, "history": []
    })
}

fn overview_json() -> serde_json::Value {
    serde_json::json!({
        "cluster_id": "cluster-00000000-0000-0000-0000-000000000001",
        "queue": queue_stats_json(),
        "capacity": cluster_capacity_json(),
    })
}

fn coordinators_json() -> serde_json::Value {
    serde_json::json!({
        "cluster_id": "cluster-00000000-0000-0000-0000-000000000001",
        "leader": null, "term": 0, "known_committed": 0, "last_applied": 0, "state_version": 0,
        "snapshot": null,
        "state_counts": { "jobs": 0, "attempts": 0, "allocations": 0, "nodes": 0, "quota_entities": 0 },
        "members": [],
    })
}

fn list_nodes_json() -> serde_json::Value {
    serde_json::json!({ "nodes": [] })
}

fn node_summary_json(id: NodeId) -> serde_json::Value {
    serde_json::json!({
        "id": id.to_string(),
        "capacity": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 },
        "allocated": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 },
        "used": null, "labels": {}, "schedulable": true, "draining": false, "health": "healthy",
        "epoch": 0, "last_heartbeat": null, "running_count": 0, "accruing_count": 0
    })
}

fn node_json(id: NodeId) -> serde_json::Value {
    serde_json::json!({
        "summary": node_summary_json(id), "host": null, "detected_capacity": null,
        "active_attempts": [], "accrual_queue": []
    })
}

fn node_utilization_json() -> serde_json::Value {
    serde_json::json!({ "capacity": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 }, "samples": [] })
}

fn list_quota_entities_json() -> serde_json::Value {
    serde_json::json!({ "entities": [] })
}

fn quota_entity_node_json(id: QuotaEntityId) -> serde_json::Value {
    serde_json::json!({
        "id": id.to_string(), "name": "team", "path": "acme/team", "parent": null,
        "origin": "configured",
        "principal": null, "quota_ucu": 0, "usage_ucu": 0, "over_quota_ratio": 0.0,
        "penalty": 1.0, "created_at": "1970-01-01T00:00:00.000000Z",
        "updated_at": "1970-01-01T00:00:00.000000Z", "queued_count": 0, "running_count": 0
    })
}

fn quota_entity_response_json(id: QuotaEntityId) -> serde_json::Value {
    serde_json::json!({
        "entity": quota_entity_node_json(id), "chain": [], "children": [],
        "stats": {
            "by_state": {}, "oldest_queued_age_seconds": null, "burn_rate_ucu_per_second": 0,
            "charged_ucu_24h": null, "usage_history": []
        }
    })
}

fn cost_report_json() -> serde_json::Value {
    serde_json::json!({
        "rate_ucu_per_second": 0, "rate_breakdown": { "cpu": 0, "memory": 0, "disk": 0 },
        "priority_multiplier": 1.0, "unbounded_multiplier": 1.0,
        "effective_rate_ucu_per_second": 0, "charge_window_seconds": 0,
        "charge_window_is_default": true, "estimated_ucu": 0, "charged_ucu": 0,
        "refund_fraction": 0.0, "actual_ucu": null, "true_up": null
    })
}

fn job_spec_json() -> serde_json::Value {
    serde_json::json!({
        "image": "alpine", "command": ["true"], "entrypoint": null,
        "requests": { "cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0 }, "priority": 0,
        "max_runtime_seconds": null, "quota_entity": quota_id().to_string(),
        "quota_entity_path": "acme/team",
        "retry": { "max_retries": 0, "retry_user_errors": false }, "submitted_by": null
    })
}

/// A `JobDetail` body for `job`, in raw lifecycle `state` (`"queued"`,
/// `"attempting"`, `"succeeded"`, …). Every non-`Option` field on
/// `JobDetail` must be present, so this is written once and reused by every
/// test needing a job-detail fixture.
fn job_detail_json(job: &str, state: &str) -> serde_json::Value {
    serde_json::json!({
        "id": job, "state": state, "spec": job_spec_json(),
        "submitted_at": "1970-01-01T00:00:00.000000Z",
        "state_since": "1970-01-01T00:00:00.000000Z", "terminal_at": null, "retries_used": 0,
        "abort_requested": null, "entity_chain": [], "attempts": [], "queue": null,
        "accrual": null, "cost": cost_report_json(), "metadata": {}
    })
}

fn job_summary_json(job: JobId) -> serde_json::Value {
    serde_json::json!({
        "id": job.to_string(), "state": "queued", "attempt": null, "image": "alpine",
        "quota_entity": quota_id().to_string(), "quota_entity_path": "acme/team", "priority": 0,
        "submitted_at": "1970-01-01T00:00:00.000000Z", "submitted_by": null, "terminal_at": null,
        "node": null, "attempt_state": null, "funding_fraction": null, "cost_ucu": 0,
        "outcome": null, "metadata": {}
    })
}

fn list_jobs_response_json(
    jobs: Vec<serde_json::Value>,
    next_cursor: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({ "jobs": jobs, "next_cursor": next_cursor })
}

fn empty_object() -> serde_json::Value {
    serde_json::json!({})
}

fn job_write_response_json(job: JobId, log_index: u64) -> serde_json::Value {
    serde_json::json!({ "job": job.to_string(), "log_index": log_index })
}

fn log_source_json(attempt: AttemptId, truncated: bool, availability: &str) -> serde_json::Value {
    serde_json::json!({
        "attempt": attempt.to_string(), "node": null, "availability": availability,
        "truncated": truncated, "earliest_available_at": null, "reason": null
    })
}

fn job_logs_response_json(
    entries: Vec<serde_json::Value>,
    sources: Vec<serde_json::Value>,
    next_cursor: Option<&str>,
    resume_cursor: Option<&str>,
    live: bool,
) -> serde_json::Value {
    serde_json::json!({
        "resume_cursor": resume_cursor, "live": live, "entries": entries, "sources": sources,
        "next_cursor": next_cursor
    })
}

fn job_usage_response_json(next_cursor: Option<&str>) -> serde_json::Value {
    serde_json::json!({ "samples": [], "sources": [], "next_cursor": next_cursor })
}

// ---------------------------------------------------------------------------
// Auth header
// ---------------------------------------------------------------------------

/// `/healthz` is outside authentication: it carries no credential, and a
/// provider that cannot supply one must not turn a live coordinator into a
/// failed probe.
#[tokio::test]
async fn healthz_sends_no_credential_and_never_asks_the_provider() {
    let store = capture_store();
    let router = with_capture(
        Router::new().route("/healthz", get(|| async { Json(healthz_json()) })),
        store.clone(),
    );
    let base = spawn(router).await;

    let with_token = Client::builder(&base).token("s3cr3t").build().unwrap();
    with_token.healthz().await.unwrap();

    let canned = Canned(Arc::new(Mutex::new(Err(
        "the token endpoint is down".to_string()
    ))));
    let failing = Client::builder(&base)
        .token_provider(canned)
        .build()
        .unwrap();
    failing.healthz().await.unwrap();

    let captured = store.lock().unwrap();
    assert_eq!(captured.len(), 2);
    for request in captured.iter() {
        assert!(request.headers.get("authorization").is_none());
    }
}

/// `/auth/config` is the credential-free discovery endpoint: a client asks it
/// to learn *how* to authenticate, so it must work with no credential at all
/// and must never depend on a provider that could itself be behind the auth
/// being probed.
#[tokio::test]
async fn auth_config_sends_no_credential_and_never_asks_the_provider() {
    let store = capture_store();
    let router = with_capture(
        Router::new().route(
            "/api/v1/auth/config",
            get(|| async { Json(auth_config_json()) }),
        ),
        store.clone(),
    );
    let base = spawn(router).await;

    let with_token = Client::builder(&base).token("s3cr3t").build().unwrap();
    with_token.auth_config().await.unwrap();

    let canned = Canned(Arc::new(Mutex::new(Err(
        "the token endpoint is down".to_string()
    ))));
    let failing = Client::builder(&base)
        .token_provider(canned)
        .build()
        .unwrap();
    failing.auth_config().await.unwrap();

    let captured = store.lock().unwrap();
    assert_eq!(captured.len(), 2);
    for request in captured.iter() {
        assert!(request.headers.get("authorization").is_none());
    }
}

/// If this fails, a token stopped riding on every request — every call to a
/// secured cluster would 401.
#[tokio::test]
async fn a_token_attaches_the_bearer_header_to_every_request() {
    let store = capture_store();
    let router = with_capture(
        Router::new().route("/api/v1/session", get(|| async { Json(session_json()) })),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::builder(&base).token("s3cr3t").build().unwrap();
    client.session().await.unwrap();

    let captured = store.lock().unwrap();
    let auth = captured[0].headers.get("authorization").unwrap();
    assert_eq!(auth.to_str().unwrap(), "Bearer s3cr3t");
}

/// If this fails, an unauthenticated client leaks a header an open-mode
/// cluster must never see, which would break open-mode entirely.
#[tokio::test]
async fn no_token_sends_no_authorization_header_at_all() {
    let store = capture_store();
    let router = with_capture(
        Router::new().route("/api/v1/session", get(|| async { Json(session_json()) })),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    client.session().await.unwrap();

    let captured = store.lock().unwrap();
    assert!(captured[0].headers.get("authorization").is_none());
}

/// If this fails, a set-but-empty `COPPICE_TOKEN` environment variable would
/// silently attach an empty bearer header instead of behaving like no token
/// at all.
#[tokio::test]
async fn an_empty_or_whitespace_token_behaves_like_no_token() {
    let store = capture_store();
    let router = with_capture(
        Router::new().route("/api/v1/session", get(|| async { Json(session_json()) })),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::builder(&base).token("   ").build().unwrap();
    client.session().await.unwrap();

    let captured = store.lock().unwrap();
    assert!(captured[0].headers.get("authorization").is_none());
}

/// A provider answering with whatever its slot currently holds — the test
/// stand-in for a credential that rotates under a long-running process.
struct Canned(Arc<Mutex<Result<Option<BearerToken>, String>>>);

impl TokenProvider for Canned {
    fn token(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<BearerToken>, BoxError>> + Send + '_>> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .clone()
                .map_err(|e| -> BoxError { e.into() })
        })
    }
}

/// The whole point of a provider: the client asks it again for every request,
/// so a credential that rotates mid-process reaches the wire without the
/// client being rebuilt. If this fails, the client is caching something it
/// promised not to.
#[tokio::test]
async fn a_provider_is_asked_again_for_every_request() {
    let store = capture_store();
    let router = with_capture(
        Router::new().route("/api/v1/session", get(|| async { Json(session_json()) })),
        store.clone(),
    );
    let base = spawn(router).await;

    let slot = Arc::new(Mutex::new(Ok(BearerToken::new("first"))));
    let client = Client::builder(&base)
        .token_provider(Canned(Arc::clone(&slot)))
        .build()
        .unwrap();
    assert!(client.has_credential());

    client.session().await.unwrap();
    *slot.lock().unwrap() = Ok(BearerToken::new("second"));
    client.session().await.unwrap();
    // `Ok(None)` is the no-credential posture, decided per request.
    *slot.lock().unwrap() = Ok(None);
    client.session().await.unwrap();

    let captured = store.lock().unwrap();
    let auth = |i: usize| {
        captured[i]
            .headers
            .get("authorization")
            .map(|v| v.to_str().unwrap().to_string())
    };
    assert_eq!(auth(0).as_deref(), Some("Bearer first"));
    assert_eq!(auth(1).as_deref(), Some("Bearer second"));
    assert_eq!(auth(2), None);
}

/// A provider that cannot supply a token fails the call before a request
/// exists — a coordinator must never see an unauthenticated attempt the
/// caller believed was authenticated.
#[tokio::test]
async fn a_provider_failure_is_a_credential_error_and_sends_nothing() {
    let store = capture_store();
    let router = with_capture(
        Router::new().route("/api/v1/session", get(|| async { Json(session_json()) })),
        store.clone(),
    );
    let base = spawn(router).await;

    let canned = Canned(Arc::new(Mutex::new(Err(
        "the token endpoint is down".to_string()
    ))));
    let client = Client::builder(&base)
        .token_provider(canned)
        .build()
        .unwrap();

    let err = client.session().await.expect_err("the provider failed");
    assert!(matches!(err, Error::Credential(_)), "{err:?}");
    assert!(!err.is_retryable());
    assert_eq!(err.status(), None);
    assert!(err.code().is_none());
    assert_eq!(
        std::error::Error::source(&err)
            .expect("the provider's own error is the source")
            .to_string(),
        "the token endpoint is down"
    );

    assert!(store.lock().unwrap().is_empty(), "nothing was sent");
}

// ---------------------------------------------------------------------------
// Paths and methods
// ---------------------------------------------------------------------------

/// If this fails, one of the read endpoints is hitting the wrong path or
/// verb, which a real coordinator would answer with a 404 or 405.
#[tokio::test]
async fn every_read_endpoint_hits_its_exact_path_and_method() {
    let node = NodeId::new();
    let entity = quota_id();
    let store = capture_store();
    let router = with_capture(
        Router::new()
            .route("/api/v1/session", get(|| async { Json(session_json()) }))
            .route(
                "/api/v1/auth/config",
                get(|| async { Json(auth_config_json()) }),
            )
            .route(
                "/api/v1/authorization",
                get(|| async { Json(authorization_json()) })
                    .put(|| async { Json(serde_json::json!({ "log_index": 1 })) }),
            )
            .route("/api/v1/overview", get(|| async { Json(overview_json()) }))
            .route(
                "/api/v1/queue/stats",
                get(|| async { Json(queue_stats_json()) }),
            )
            .route(
                "/api/v1/coordinators",
                get(|| async { Json(coordinators_json()) }),
            )
            .route("/api/v1/nodes", get(|| async { Json(list_nodes_json()) }))
            .route(
                "/api/v1/nodes/:node",
                get(move || async move { Json(node_json(node)) }),
            )
            .route(
                "/api/v1/nodes/:node/utilization",
                get(|| async { Json(node_utilization_json()) }),
            )
            .route(
                "/api/v1/quota-entities",
                get(|| async { Json(list_quota_entities_json()) }),
            )
            .route(
                "/api/v1/quota-entities/:entity",
                get(move || async move { Json(quota_entity_response_json(entity)) }),
            )
            .route("/healthz", get(|| async { Json(healthz_json()) })),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    client.session().await.unwrap();
    client.auth_config().await.unwrap();
    client.authorization().await.unwrap();
    client
        .update_authorization(&coppice_client::UpdateAuthorizationRequest::new([]))
        .await
        .unwrap();
    client.overview().await.unwrap();
    client.queue_stats().await.unwrap();
    client.coordinators().await.unwrap();
    client.list_nodes().await.unwrap();
    client.node(node).await.unwrap();
    client.node_utilization(node).await.unwrap();
    client.list_quota_entities().await.unwrap();
    client.quota_entity(entity).await.unwrap();
    client.healthz().await.unwrap();

    let captured = store.lock().unwrap();
    let want: Vec<(Method, String)> = vec![
        (Method::GET, "/api/v1/session".to_string()),
        (Method::GET, "/api/v1/auth/config".to_string()),
        (Method::GET, "/api/v1/authorization".to_string()),
        (Method::PUT, "/api/v1/authorization".to_string()),
        (Method::GET, "/api/v1/overview".to_string()),
        (Method::GET, "/api/v1/queue/stats".to_string()),
        (Method::GET, "/api/v1/coordinators".to_string()),
        (Method::GET, "/api/v1/nodes".to_string()),
        (Method::GET, format!("/api/v1/nodes/{node}")),
        (Method::GET, format!("/api/v1/nodes/{node}/utilization")),
        (Method::GET, "/api/v1/quota-entities".to_string()),
        (Method::GET, format!("/api/v1/quota-entities/{entity}")),
        (Method::GET, "/healthz".to_string()),
    ];
    assert_eq!(captured.len(), want.len());
    for (got, (method, path)) in captured.iter().zip(want.iter()) {
        assert_eq!(&got.method, method, "{path}");
        assert_eq!(&got.path, path);
    }
}

/// If this fails, one of the write verbs is hitting the wrong path, the
/// wrong method, or (for the bodyless node-admin trio) sending a body the
/// server never expects.
#[tokio::test]
async fn every_write_endpoint_hits_its_exact_path_method_and_body_shape() {
    let job = JobId::new();
    let node = NodeId::new();
    let store = capture_store();
    let router = with_capture(
        Router::new()
            .route(
                "/api/v1/jobs",
                post(|| async { Json(job_write_response_json(JobId::new(), 1)) }),
            )
            .route(
                "/api/v1/jobs/:job/abort",
                post(|| async { Json(empty_object()) }),
            )
            .route(
                "/api/v1/jobs/:job/metadata",
                put(|| async { Json(job_write_response_json(JobId::new(), 1)) })
                    .post(|| async { Json(job_write_response_json(JobId::new(), 1)) }),
            )
            .route(
                "/api/v1/nodes/:node/drain",
                post(|| async { Json(empty_object()) }),
            )
            .route(
                "/api/v1/nodes/:node/undrain",
                post(|| async { Json(empty_object()) }),
            )
            .route(
                "/api/v1/nodes/:node/remove",
                post(|| async { Json(empty_object()) }),
            )
            .route(
                "/api/v1/quota-entities",
                post(|| async {
                    Json(serde_json::json!({
                        "entity": QuotaEntityId::new().to_string(),
                        "path": "acme/team",
                        "log_index": 1
                    }))
                }),
            ),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let submit = SubmitJobRequest::new(job, "alpine", ["true"], Resources::default(), quota_id());
    client.submit_job(&submit).await.unwrap();
    client
        .abort_job(job, &AbortJobRequest::new())
        .await
        .unwrap();
    client
        .replace_job_metadata(job, &ReplaceJobMetadataRequest::new(JobMetadata::new()))
        .await
        .unwrap();
    client
        .update_job_metadata(job, &UpdateJobMetadataRequest::new())
        .await
        .unwrap();
    client.drain_node(node).await.unwrap();
    client.undrain_node(node).await.unwrap();
    client.remove_node(node).await.unwrap();
    client
        .configure_quota_entity(&coppice_client::ConfigureQuotaEntityRequest::new(
            QuotaEntityId::new(),
            "team",
            0,
        ))
        .await
        .unwrap();

    let captured = store.lock().unwrap();
    let want = [
        (Method::POST, "/api/v1/jobs".to_string()),
        (Method::POST, format!("/api/v1/jobs/{job}/abort")),
        (Method::PUT, format!("/api/v1/jobs/{job}/metadata")),
        (Method::POST, format!("/api/v1/jobs/{job}/metadata")),
        (Method::POST, format!("/api/v1/nodes/{node}/drain")),
        (Method::POST, format!("/api/v1/nodes/{node}/undrain")),
        (Method::POST, format!("/api/v1/nodes/{node}/remove")),
        (Method::POST, "/api/v1/quota-entities".to_string()),
    ];
    assert_eq!(captured.len(), want.len());
    for (got, (method, path)) in captured.iter().zip(want.iter()) {
        assert_eq!(&got.method, method, "{path}");
        assert_eq!(&got.path, path);
    }
    // The bodyless node-admin trio: drain, undrain, remove.
    for i in 4..7 {
        assert!(captured[i].body.is_empty(), "request {i} carried a body");
    }
}

/// If this fails, a base URL a dev banner prints (already ending in
/// `/api/v1`) would double the prefix and every request would 404.
#[tokio::test]
async fn a_base_url_already_carrying_api_v1_does_not_double_it() {
    let store = capture_store();
    let router = with_capture(
        Router::new().route(
            "/api/v1/jobs",
            get(|| async { Json(list_jobs_response_json(vec![], None)) }),
        ),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::new(format!("{base}/api/v1/")).unwrap();
    client.list_jobs(&ListJobsParams::new()).await.unwrap();

    let captured = store.lock().unwrap();
    assert_eq!(captured[0].path, "/api/v1/jobs");
}

// ---------------------------------------------------------------------------
// Query encoding
// ---------------------------------------------------------------------------

/// If this fails, a `JobFilter` is either mis-encoded on the wire or the
/// sibling `limit`/`cursor` params are dropped when a filter is present.
#[tokio::test]
async fn list_jobs_sends_the_filter_as_percent_encoded_json_alongside_limit_and_cursor() {
    async fn handler(
        State(store): State<QueryStore>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Json<serde_json::Value> {
        store.lock().unwrap().push(params);
        Json(list_jobs_response_json(vec![], None))
    }

    let store: QueryStore = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .route("/api/v1/jobs", get(handler))
        .with_state(store.clone());
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let filter = JobFilter::phase_in([JobPhase::Running]);
    let params = ListJobsParams::new()
        .with_filter(filter.clone())
        .with_limit(50)
        .with_cursor(coppice_client::JobCursor::from("v1:job-x".to_string()));
    client.list_jobs(&params).await.unwrap();

    let captured = store.lock().unwrap();
    let seen = &captured[0];
    let decoded_filter: JobFilter = serde_json::from_str(&seen["filter"]).unwrap();
    assert_eq!(decoded_filter, filter);
    assert_eq!(seen["limit"], "50");
    assert_eq!(seen["cursor"], "v1:job-x");
}

/// If this fails, the logs endpoint's query encoding has drifted from the
/// server's wire keys or its RFC 3339 timestamp rendering.
#[tokio::test]
async fn job_logs_sends_every_parameter_under_its_exact_wire_key() {
    async fn handler(
        State(store): State<QueryStore>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Json<serde_json::Value> {
        store.lock().unwrap().push(params);
        Json(job_logs_response_json(vec![], vec![], None, None, false))
    }

    let store: QueryStore = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .route("/api/v1/jobs/:job/logs", get(handler))
        .with_state(store.clone());
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let job = JobId::new();
    let attempt = AttemptId::new();
    let params = LogsParams::new()
        .with_cursor(coppice_client::LogCursor::from("v1:c".to_string()))
        .with_limit(25)
        .with_stream(LogStreamName::Stderr)
        .with_attempt(attempt)
        .with_from(Timestamp::from_micros(1_000_000).unwrap())
        .with_to(Timestamp::from_micros(2_000_000).unwrap())
        .with_order(LogOrder::Asc);
    client.job_logs(job, &params).await.unwrap();

    {
        let captured = store.lock().unwrap();
        let seen = &captured[0];
        assert_eq!(seen["cursor"], "v1:c");
        assert_eq!(seen["limit"], "25");
        assert_eq!(seen["stream"], "stderr");
        assert_eq!(seen["attempt"], attempt.to_string());
        assert_eq!(seen["from"], "1970-01-01T00:00:01.000000Z");
        assert_eq!(seen["to"], "1970-01-01T00:00:02.000000Z");
        assert!(!seen.contains_key("to_inclusive"));
        assert_eq!(seen["order"], "asc");
    }

    // `to_inclusive` rides only when set.
    let params = LogsParams::new()
        .with_to(Timestamp::from_micros(2_000_000).unwrap())
        .with_to_inclusive(true);
    client.job_logs(job, &params).await.unwrap();
    let captured = store.lock().unwrap();
    assert_eq!(captured[1]["to_inclusive"], "true");
}

/// If this fails, the usage endpoint's query encoding has drifted from the
/// server's wire keys.
#[tokio::test]
async fn job_usage_sends_every_parameter_under_its_exact_wire_key() {
    async fn handler(
        State(store): State<QueryStore>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Json<serde_json::Value> {
        store.lock().unwrap().push(params);
        Json(job_usage_response_json(None))
    }

    let store: QueryStore = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .route("/api/v1/jobs/:job/usage", get(handler))
        .with_state(store.clone());
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let job = JobId::new();
    let attempt = AttemptId::new();
    let params = UsageParams::new()
        .with_cursor(coppice_client::UsageCursor::from("v1:u".to_string()))
        .with_limit(200)
        .with_attempt(attempt)
        .with_from(Timestamp::from_micros(1_000_000).unwrap())
        .with_to(Timestamp::from_micros(2_000_000).unwrap())
        .with_to_inclusive(true)
        .with_order(LogOrder::Desc);
    client.job_usage(job, &params).await.unwrap();

    let captured = store.lock().unwrap();
    let seen = &captured[0];
    assert_eq!(seen["cursor"], "v1:u");
    assert_eq!(seen["limit"], "200");
    assert_eq!(seen["attempt"], attempt.to_string());
    assert_eq!(seen["from"], "1970-01-01T00:00:01.000000Z");
    assert_eq!(seen["to"], "1970-01-01T00:00:02.000000Z");
    assert_eq!(seen["to_inclusive"], "true");
    assert_eq!(seen["order"], "desc");
}

/// If this fails, `ReadOptions` either fails to ride alongside an
/// endpoint's own query, or leaks from a scoped client back onto the one it
/// was cloned from.
#[tokio::test]
async fn read_options_ride_alongside_the_endpoints_own_query_and_stay_scoped() {
    async fn handler(
        State(store): State<QueryStore>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Json<serde_json::Value> {
        store.lock().unwrap().push(params);
        Json(list_jobs_response_json(vec![], None))
    }

    let store: QueryStore = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .route("/api/v1/jobs", get(handler))
        .with_state(store.clone());
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    // The bare client adds neither `consistency` nor `min_index`.
    client
        .list_jobs(&ListJobsParams::new().with_limit(10))
        .await
        .unwrap();
    {
        let captured = store.lock().unwrap();
        assert!(!captured[0].contains_key("consistency"));
        assert!(!captured[0].contains_key("min_index"));
        assert_eq!(captured[0]["limit"], "10");
    }

    // A scoped clone adds both, alongside the endpoint's own `limit`.
    let scoped = client.with_read_options(ReadOptions::strong().with_min_index(42));
    scoped
        .list_jobs(&ListJobsParams::new().with_limit(10))
        .await
        .unwrap();
    {
        let captured = store.lock().unwrap();
        assert_eq!(captured[1]["consistency"], "strong");
        assert_eq!(captured[1]["min_index"], "42");
        assert_eq!(captured[1]["limit"], "10");
    }

    // Scoping never mutated the original.
    assert!(client.read_options().is_empty());
    assert_eq!(scoped.read_options().consistency, Some(Consistency::Strong));
}

// ---------------------------------------------------------------------------
// Read indexes
// ---------------------------------------------------------------------------

/// If this fails, a reader cannot tell how stale an answer is — `lag()`
/// would silently report `None` even when the server told it exactly.
#[tokio::test]
async fn read_index_headers_surface_on_versioned_and_lag_is_their_difference() {
    let router = Router::new().route(
        "/api/v1/session",
        get(|| async {
            json_with_headers(
                session_json(),
                &[
                    (coppice_client::APPLIED_INDEX_HEADER, "10"),
                    (coppice_client::COMMITTED_INDEX_HEADER, "15"),
                ],
            )
        }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let versioned = client.session().await.unwrap();
    assert_eq!(versioned.applied_index, Some(10));
    assert_eq!(versioned.committed_index, Some(15));
    assert_eq!(versioned.lag(), Some(5));
}

/// If this fails, an answer with no read-index headers at all (a server
/// that never sends them) would be misreported as having a known lag.
#[tokio::test]
async fn missing_read_index_headers_leave_both_indexes_none() {
    let router = Router::new().route("/api/v1/session", get(|| async { Json(session_json()) }));
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let versioned = client.session().await.unwrap();
    assert_eq!(versioned.applied_index, None);
    assert_eq!(versioned.committed_index, None);
    assert_eq!(versioned.lag(), None);
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

/// If this fails, a JSON `{code, message}` error body is not classified
/// into the right `ErrorCode`, or the printed text an operator sees has
/// drifted.
#[tokio::test]
async fn a_json_error_body_becomes_a_typed_api_error() {
    let job = JobId::new();
    let router = Router::new().route(
        "/api/v1/jobs/:job",
        get(|| async {
            (
                StatusCode::NOT_FOUND,
                Json(error_body("NOT_FOUND", "no such job")),
            )
        }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let err = client.job(job).await.unwrap_err();
    match &err {
        Error::Api {
            status,
            code,
            message,
            leader,
        } => {
            assert_eq!(*status, 404);
            assert_eq!(*code, ErrorCode::NotFound);
            assert_eq!(message, "no such job");
            assert!(leader.is_none());
        }
        other => panic!("expected Error::Api, got {other:?}"),
    }
    assert_eq!(err.to_string(), "api error (NOT_FOUND): no such job");
}

/// If this fails, a non-JSON error body (a proxy's plain-text 502) is
/// either misparsed as a `{code, message}` body or its text is lost.
#[tokio::test]
async fn a_non_json_error_body_becomes_unexpected_status_with_its_text() {
    let router = Router::new().route(
        "/api/v1/nodes/:node/drain",
        post(|| async { status_with_body(502, "  bad gateway\n", &[]) }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let err = client.drain_node(NodeId::new()).await.unwrap_err();
    assert!(matches!(err, Error::UnexpectedStatus { status: 502, .. }));
    assert_eq!(err.to_string(), "api error (HTTP 502): bad gateway");
}

/// If this fails, an empty non-2xx body renders a dangling colon
/// (`api error (HTTP 502): `) instead of the clean no-body form.
#[tokio::test]
async fn an_empty_error_body_has_no_trailing_colon() {
    let router = Router::new().route(
        "/api/v1/nodes/:node/drain",
        post(|| async { status_with_body(502, "", &[]) }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let err = client.drain_node(NodeId::new()).await.unwrap_err();
    assert_eq!(err.to_string(), "api error (HTTP 502)");
}

/// If this fails, a follower's `Coppice-Leader` hint on a 421 is lost, so a
/// caller cannot retry against the leader and `is_retryable` misjudges a
/// perfectly retryable write.
#[tokio::test]
async fn a_421_leader_hint_surfaces_on_the_error_and_its_display() {
    let router = Router::new().route(
        "/api/v1/nodes/:node/drain",
        post(|| async {
            status_with_body(
                421,
                &error_body("NOT_LEADER", "not the leader").to_string(),
                &[(coppice_client::LEADER_HEADER, "10.0.0.2:7070")],
            )
        }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let err = client.drain_node(NodeId::new()).await.unwrap_err();
    assert_eq!(err.leader_hint(), Some("10.0.0.2:7070"));
    assert!(err.is_retryable());
    assert!(err
        .to_string()
        .ends_with("; retry against the leader at 10.0.0.2:7070"));
}

/// If this fails, a code from a newer server crashes an older client
/// instead of degrading to `ErrorCode::Other` with the spelling kept.
#[tokio::test]
async fn an_unknown_error_code_becomes_other_and_keeps_its_spelling() {
    let router = Router::new().route(
        "/api/v1/jobs",
        post(|| async {
            (
                StatusCode::TOO_MANY_REQUESTS,
                Json(error_body("RESOURCE_EXHAUSTED", "over budget")),
            )
        }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let request = SubmitJobRequest::new(
        JobId::new(),
        "alpine",
        ["true"],
        Resources::default(),
        quota_id(),
    );
    let err = client.submit_job(&request).await.unwrap_err();
    assert_eq!(
        err.code(),
        Some(&ErrorCode::Other("RESOURCE_EXHAUSTED".to_string()))
    );
}

/// If this fails, a 2xx body that does not match the endpoint's declared
/// shape is silently accepted (or panics) instead of surfacing as
/// `Error::Decode`.
#[tokio::test]
async fn a_malformed_2xx_body_becomes_a_decode_error() {
    let router = Router::new().route(
        "/api/v1/session",
        get(|| async { (StatusCode::OK, "\"just a string\"") }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let err = client.session().await.unwrap_err();
    assert!(matches!(err, Error::Decode(_)), "{err:?}");
}

/// If this fails, `is_not_found` no longer recognizes the server's own
/// `NOT_FOUND` code on a 404.
#[tokio::test]
async fn is_not_found_recognizes_a_404_not_found_error() {
    let router = Router::new().route(
        "/api/v1/jobs/:job",
        get(|| async { (StatusCode::NOT_FOUND, Json(error_body("NOT_FOUND", "gone"))) }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let err = client.job(JobId::new()).await.unwrap_err();
    assert!(err.is_not_found());
}

// ---------------------------------------------------------------------------
// Forward compatibility
// ---------------------------------------------------------------------------

/// If this fails, a response carrying a field this client does not know
/// about fails to decode, instead of the old client degrading gracefully
/// against a newer server.
#[tokio::test]
async fn an_extra_unknown_response_field_decodes_fine() {
    let mut body = session_json();
    body.as_object_mut()
        .unwrap()
        .insert("a_field_from_the_future".to_string(), serde_json::json!(42));
    let router = Router::new().route(
        "/api/v1/session",
        get(move || async move { Json(body.clone()) }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let session = client.session().await.unwrap();
    assert_eq!(session.principal, "anonymous");
}

/// If this fails, an enum value newer than this client crashes the whole
/// decode instead of landing in the `Unknown` catch-all with its spelling
/// kept.
#[tokio::test]
async fn an_unrecognized_job_state_decodes_into_the_unknown_variant() {
    let job = JobId::new();
    let mut summary = job_summary_json(job);
    summary["state"] = serde_json::json!("hibernating");
    let response = list_jobs_response_json(vec![summary], None);
    let router = Router::new().route(
        "/api/v1/jobs",
        get(move || {
            let response = response.clone();
            async move { Json(response) }
        }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();
    let page = client.list_jobs(&ListJobsParams::new()).await.unwrap();
    assert_eq!(
        page.jobs[0].state,
        JobStateKind::Unknown("hibernating".to_string())
    );
}

// ---------------------------------------------------------------------------
// Escape hatch
// ---------------------------------------------------------------------------

/// If this fails, the untyped `get_value` hatch has drifted from the typed
/// method it is supposed to mirror exactly — different path, different
/// query, or it loses a field the typed struct does not know about.
#[tokio::test]
async fn get_value_hits_the_same_url_as_the_typed_method_and_returns_the_body_verbatim() {
    let store = capture_store();
    let job = JobId::new();
    let mut summary = job_summary_json(job);
    summary["a_field_from_the_future"] = serde_json::json!("surprise");
    let response = list_jobs_response_json(vec![summary], None);
    let router = with_capture(
        Router::new().route(
            "/api/v1/jobs",
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            }),
        ),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let params = ListJobsParams::new().with_limit(10);
    client.list_jobs(&params).await.unwrap();
    let untyped = client
        .get_value(paths::JOBS, &params.query_pairs())
        .await
        .unwrap();

    let captured = store.lock().unwrap();
    assert_eq!(captured[0].query, captured[1].query);
    assert_eq!(captured[0].path, captured[1].path);
    assert_eq!(
        untyped.value["jobs"][0]["a_field_from_the_future"],
        serde_json::json!("surprise")
    );
}

/// If this fails, `post_value`/`put_value` are not sending exactly the body
/// they were given.
#[tokio::test]
async fn post_value_and_put_value_send_the_body_they_were_given() {
    let store = capture_store();
    let router = with_capture(
        Router::new()
            .route(
                "/api/v1/quota-entities",
                post(|| async { Json(empty_object()) }),
            )
            .route(
                "/api/v1/authorization",
                put(|| async { Json(empty_object()) }),
            ),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let post_body = serde_json::json!({ "entity": "quota-x", "name": "team", "quota_ucu": 5 });
    client
        .post_value(paths::QUOTA_ENTITIES, &post_body)
        .await
        .unwrap();
    let put_body = serde_json::json!({ "groups_claim": "g", "bindings": [] });
    client
        .put_value(paths::AUTHORIZATION, &put_body)
        .await
        .unwrap();

    let captured = store.lock().unwrap();
    let got_post: serde_json::Value = serde_json::from_slice(&captured[0].body).unwrap();
    let got_put: serde_json::Value = serde_json::from_slice(&captured[1].body).unwrap();
    assert_eq!(got_post, post_body);
    assert_eq!(got_put, put_body);
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

/// If this fails, a pager either stops early while more remains or loops
/// forever once the server signals it is done.
#[tokio::test]
async fn a_job_pager_continues_while_next_cursor_is_set_and_stops_on_null() {
    let store = capture_store();
    let page1 = list_jobs_response_json(vec![], Some("v1:c1"));
    let page2 = list_jobs_response_json(vec![], None);
    let router = with_capture(
        Router::new().route("/api/v1/jobs", get(sequence(vec![page1, page2]))),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let mut pager = client.list_jobs_paged(ListJobsParams::new());
    assert!(pager.next_page().await.unwrap().is_some());
    assert!(pager.next_page().await.unwrap().is_some());
    assert!(pager.next_page().await.unwrap().is_none());

    let captured = store.lock().unwrap();
    assert_eq!(captured.len(), 2, "no request should follow a null cursor");
    assert!(!captured[0].query.contains("cursor="));
    assert!(captured[1].query.contains("cursor=v1%3Ac1"));
}

/// The contract that trips people up: a short page (fewer rows than
/// `limit`) with a non-null cursor still means *continue*, never *done*.
/// If this fails, the pager is treating a short page as the end of the
/// scan.
#[tokio::test]
async fn a_short_page_with_a_non_null_cursor_still_continues() {
    let store = capture_store();
    let job = JobId::new();
    let page1 = list_jobs_response_json(vec![job_summary_json(job)], Some("v1:c1"));
    let page2 = list_jobs_response_json(vec![], None);
    let router = with_capture(
        Router::new().route("/api/v1/jobs", get(sequence(vec![page1, page2]))),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let mut pager = client.list_jobs_paged(ListJobsParams::new().with_limit(100));
    let page = pager.next_page().await.unwrap().unwrap();
    assert_eq!(page.jobs.len(), 1, "one row, far short of limit=100");
    assert!(page.next_cursor.is_some());
    assert!(pager.next_page().await.unwrap().is_some());

    let captured = store.lock().unwrap();
    assert_eq!(captured.len(), 2);
}

/// Paging is exactly when staleness matters — a long walk can straddle a
/// replica falling behind — so each page keeps the read indexes its own
/// response carried. If this fails, the pager is stripping them again and a
/// caller walking a list has no way to notice.
#[tokio::test]
async fn a_pager_page_keeps_the_read_indexes_of_the_response_that_served_it() {
    let router = Router::new().route(
        "/api/v1/jobs",
        get(|| async {
            json_with_headers(
                list_jobs_response_json(vec![], None),
                &[
                    (coppice_client::APPLIED_INDEX_HEADER, "41"),
                    (coppice_client::COMMITTED_INDEX_HEADER, "44"),
                ],
            )
        }),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let page = client
        .list_jobs_paged(ListJobsParams::new())
        .next_page()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(page.applied_index, Some(41));
    assert_eq!(page.committed_index, Some(44));
    assert_eq!(page.lag(), Some(3));
    // …and the body is still reached straight through the `Deref`.
    assert!(page.jobs.is_empty());
}

/// If this fails, a pager sends a second request even when the first page
/// already said the scan was complete.
#[tokio::test]
async fn a_first_page_with_a_null_cursor_stops_after_one_request() {
    let store = capture_store();
    let page1 = list_jobs_response_json(vec![], None);
    let router = with_capture(
        Router::new().route("/api/v1/jobs", get(sequence(vec![page1]))),
        store.clone(),
    );
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let mut pager = client.list_jobs_paged(ListJobsParams::new());
    assert!(pager.next_page().await.unwrap().is_some());
    assert!(pager.next_page().await.unwrap().is_none());

    assert_eq!(store.lock().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Log follower
// ---------------------------------------------------------------------------

/// If this fails, the follower is not walking `next_cursor` first and
/// switching to `resume_cursor` once caught up, so a live tail either
/// re-walks history forever or never catches up.
#[tokio::test]
async fn the_follower_walks_next_cursor_then_switches_to_resume_cursor() {
    let job = JobId::new();
    let logs_store = capture_store();

    let logs_router = with_capture(
        Router::new().route(
            "/api/v1/jobs/:job/logs",
            get(sequence(vec![
                job_logs_response_json(vec![], vec![], Some("v1:n1"), None, true),
                job_logs_response_json(vec![], vec![], None, Some("v1:r1"), true),
                job_logs_response_json(vec![], vec![], None, Some("v1:r2"), true),
            ])),
        ),
        logs_store.clone(),
    );
    let job_router = Router::new().route(
        "/api/v1/jobs/:job",
        get(move || {
            let body = job_detail_json(&job.to_string(), "attempting");
            async move { Json(body) }
        }),
    );
    let full_router = logs_router.merge(job_router);
    let base = spawn(full_router).await;
    let client = Client::new(&base).unwrap();

    let mut follower = client.follow_job_logs(
        job,
        FollowOptions::new().with_poll_interval(Duration::from_millis(5)),
    );
    follower.next_page().await.unwrap();
    follower.next_page().await.unwrap();
    follower.next_page().await.unwrap();

    let captured = logs_store.lock().unwrap();
    assert_eq!(captured.len(), 3);
    assert!(!captured[0].query.contains("cursor="));
    assert!(captured[1].query.contains("cursor=v1%3An1"));
    assert!(captured[2].query.contains("cursor=v1%3Ar1"));
}

/// If this fails, the follower either never stops once the job is terminal,
/// or stops immediately without the one further drain that catches the
/// last lines written between the final page and the job going terminal.
#[tokio::test]
async fn the_follower_stops_after_one_further_drain_past_terminal() {
    let job = JobId::new();
    let logs_store = capture_store();

    let caught_up = job_logs_response_json(vec![], vec![], None, None, true);
    let logs_router = with_capture(
        Router::new().route("/api/v1/jobs/:job/logs", get(sequence(vec![caught_up]))),
        logs_store.clone(),
    );
    let job_router = Router::new().route(
        "/api/v1/jobs/:job",
        get(sequence(vec![
            job_detail_json(&job.to_string(), "attempting"),
            job_detail_json(&job.to_string(), "attempting"),
            job_detail_json(&job.to_string(), "succeeded"),
        ])),
    );
    let router = logs_router.merge(job_router);
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let mut follower = client.follow_job_logs(
        job,
        FollowOptions::new().with_poll_interval(Duration::from_millis(5)),
    );
    let mut pages = 0usize;
    while follower.next_page().await.unwrap().is_some() {
        pages += 1;
        assert!(pages <= 10, "follower did not stop");
    }
    assert!(follower.is_finished());
    // Terminal was seen on the 3rd job check (3rd logs request); one more
    // drain follows before the walk ends: 4 logs requests total.
    assert_eq!(logs_store.lock().unwrap().len(), 4);
}

/// If this fails, the follower rewinds on a page that carries neither
/// cursor, re-walking output it already saw.
#[tokio::test]
async fn a_page_with_neither_cursor_leaves_the_position_unchanged() {
    let job = JobId::new();
    let logs_store = capture_store();
    let logs_router = with_capture(
        Router::new().route(
            "/api/v1/jobs/:job/logs",
            get(sequence(vec![
                job_logs_response_json(vec![], vec![], Some("v1:n1"), None, true),
                job_logs_response_json(vec![], vec![], None, None, true),
                job_logs_response_json(vec![], vec![], None, None, true),
            ])),
        ),
        logs_store.clone(),
    );
    let job_router = Router::new().route(
        "/api/v1/jobs/:job",
        get(move || {
            let body = job_detail_json(&job.to_string(), "attempting");
            async move { Json(body) }
        }),
    );
    let router = logs_router.merge(job_router);
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let mut follower = client.follow_job_logs(
        job,
        FollowOptions::new().with_poll_interval(Duration::from_millis(5)),
    );
    follower.next_page().await.unwrap();
    follower.next_page().await.unwrap();
    follower.next_page().await.unwrap();

    let captured = logs_store.lock().unwrap();
    assert_eq!(captured.len(), 3);
    assert!(!captured[0].query.contains("cursor="));
    assert!(captured[1].query.contains("cursor=v1%3An1"));
    // The 2nd page carried neither cursor: the 3rd request must still carry
    // the position from the 1st page, not rewind to no cursor.
    assert!(captured[2].query.contains("cursor=v1%3An1"));
}

/// If this fails, `sources()` either loses an attempt's record across pages
/// or forgets a `truncated` verdict once it has been set.
#[tokio::test]
async fn sources_merges_per_attempt_and_latches_truncated() {
    let job = JobId::new();
    let attempt_a = AttemptId::new();
    let attempt_b = AttemptId::new();
    let logs_router = Router::new().route(
        "/api/v1/jobs/:job/logs",
        get(sequence(vec![
            job_logs_response_json(
                vec![],
                vec![log_source_json(attempt_a, true, "available")],
                Some("v1:n1"),
                None,
                true,
            ),
            job_logs_response_json(
                vec![],
                vec![
                    log_source_json(attempt_a, false, "expired"),
                    log_source_json(attempt_b, false, "available"),
                ],
                None,
                None,
                true,
            ),
        ])),
    );
    let job_router = Router::new().route(
        "/api/v1/jobs/:job",
        get(move || {
            let body = job_detail_json(&job.to_string(), "attempting");
            async move { Json(body) }
        }),
    );
    let router = logs_router.merge(job_router);
    let base = spawn(router).await;
    let client = Client::new(&base).unwrap();

    let mut follower = client.follow_job_logs(
        job,
        FollowOptions::new().with_poll_interval(Duration::from_millis(5)),
    );
    follower.next_page().await.unwrap();
    follower.next_page().await.unwrap();

    let sources = follower.sources();
    assert_eq!(sources.len(), 2);
    let a = sources.iter().find(|s| s.attempt == attempt_a).unwrap();
    let b = sources.iter().find(|s| s.attempt == attempt_b).unwrap();
    assert!(a.truncated, "truncated must latch true once set");
    assert!(!b.truncated);
}
