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

use super::authn::RequestActor;
use super::enroll::EnrollEndpoint;
use super::error::HttpError;
use super::extract::{IdPath, ReadIndexes, ReadQuery};
use super::metrics::MetricsEndpoint;
use super::readyz::ReadyzEndpoint;

/// Build the client-listener router around a [`ControlPlane`] and the
/// process's metrics endpoint (issue #46).
///
/// The `/api/v1` map is [`api_v1_routes`], nested under its prefix here. The
/// top-level `/metrics` route is deliberately **not** under `/api/v1` — it is
/// the Prometheus scrape target, not part of the JSON API — and carries its own
/// captured [`MetricsEndpoint`], state that is entirely separate from the
/// `ControlPlane`. Nested-router misses under `/api/v1` fall through to the
/// outer [`fallback`](super::ui::fallback) with the full original path intact,
/// so `/api/*` misses still answer the JSON 404 and everything else reaches the
/// UI, exactly as before the nesting refactor.
///
/// `authn` is the deployment's authentication posture (ADR 0022), built by the
/// coordinator: [`api_v1_routes`] layers it over the protected half of the
/// `/api/v1` tree. `/metrics` and `/readyz` are outside `/api/v1` and
/// therefore outside authentication — a scrape and a readiness probe are
/// operational surfaces, and the listener's own posture is what guards them.
pub fn router<P: ControlPlane>(
    plane: Arc<P>,
    metrics: MetricsEndpoint,
    readyz: ReadyzEndpoint,
    enroll: EnrollEndpoint,
    authn: Arc<coppice_authn::AuthnChain>,
) -> Router {
    // The scrape and readiness handlers capture their own state, so they need
    // no router state and compose with the `Arc<P>` state the rest of the tree
    // carries — they are merged in before `.with_state(plane)` closes the tree.
    // `/enroll` captures its own [`EnrollEndpoint`] the same way, for the same
    // reason: certificate issuance is not a `ControlPlane` operation
    // (ADR 0037 §4).
    operational_routes(metrics, readyz)
        .nest("/api/v1", api_v1_routes::<P>(enroll, authn))
        // Everything unrouted: `/api/*` misses stay JSON 404s; anything
        // else serves the embedded web UI (static assets + SPA fallback,
        // ADR 0031 "Serving the UI").
        .fallback(super::ui::fallback)
        .with_state(plane)
}

/// The pre-formation client-listener router (ADR 0037 §3).
///
/// Until the `formation_complete` marker exists — a parked daemon, one
/// mid-formation, one in `formation-failed` — the client API is **not
/// served**: that closure is what confines a failed formation to the node
/// that attempted it. What remains is exactly what an operator and their
/// automation need to see the daemon and know why it is not ready:
/// `/readyz` and `/metrics`. Everything else, `/api/v1` included, answers
/// the JSON 404 through the same fallback the full router uses, so no
/// client mistakes a parked daemon for a broken one.
pub fn closed_router(metrics: MetricsEndpoint, readyz: ReadyzEndpoint) -> Router {
    operational_routes(metrics, readyz).fallback(super::ui::fallback)
}

/// The two operational routes both surfaces carry: the Prometheus scrape
/// target (issue #46) and the ADR 0037 §9 readiness gate. Neither is under
/// `/api/v1` — they are not part of the JSON API — and neither touches the
/// [`ControlPlane`], which is what lets a daemon with no consensus replica
/// serve them.
fn operational_routes<S: Clone + Send + Sync + 'static>(
    metrics: MetricsEndpoint,
    readyz: ReadyzEndpoint,
) -> Router<S> {
    let metrics = Arc::new(metrics);
    Router::new()
        // Prometheus scrape target (issue #46): the `/metrics` render contract
        // lives in `super::metrics`; this only marries it to the listener.
        .route(
            "/metrics",
            get(move || {
                let metrics = Arc::clone(&metrics);
                async move { metrics.render().await }
            }),
        )
        // Readiness (ADR 0037 §9): served in every daemon state, including
        // parked and formation-failed. `?require=healthy` is the only gate
        // query the endpoint recognizes; anything else is a 400 from
        // `ReadyzEndpoint::handle` itself.
        .route(
            "/readyz",
            get(move |Query(ReadyzQuery { require }): Query<ReadyzQuery>| {
                let readyz = readyz.clone();
                async move { readyz.handle(require).await }
            }),
        )
}

/// The `/api/v1` route map (ADR 0031), nested under its prefix by [`router`].
///
/// Every path here is written **without** the `/api/v1` prefix — [`router`]
/// restores it with `.nest("/api/v1", …)`. Consistency defaults per route are
/// the ADR 0031 table; they become code (`ReadParams::class(default)`) as each
/// read handler is implemented.
///
/// The tree is split in two by authentication (ADR 0022). [`public_routes`] is
/// the closed set of routes a caller reaches *without* a credential;
/// [`protected_routes`] is everything else, and carries the
/// [`authn::authenticate`](super::authn::authenticate) layer. The split is
/// structural rather than a per-handler check precisely so that adding a route
/// cannot accidentally leave it unauthenticated: a new `.route(…)` lands in
/// whichever sub-router it is written in, and the protected one is the one
/// every route below `/session` is written in.
fn api_v1_routes<P: ControlPlane>(
    enroll: EnrollEndpoint,
    authn: Arc<coppice_authn::AuthnChain>,
) -> Router<Arc<P>> {
    protected_routes::<P>()
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&authn),
            super::authn::authenticate,
        ))
        .merge(public_routes::<P>(enroll, authn))
}

/// The `/api/v1` routes served **without** authentication.
///
/// Exactly two, each for a reason that is about the credential itself:
///
/// - `POST /enroll` is a machine's certless first contact, authenticated by
///   its own role-scoped enrollment token (ADR 0037 §4). An enrollee has no
///   user identity to present and the endpoint's refusal contract — one
///   constant body, no validity oracle — is not the API's 401.
/// - `GET /auth/config` is how a client discovers *how to authenticate*.
///   Requiring a credential to learn which credential to obtain would be a
///   loop with no entry point.
fn public_routes<P: ControlPlane>(
    enroll: EnrollEndpoint,
    authn: Arc<coppice_authn::AuthnChain>,
) -> Router<Arc<P>> {
    Router::new()
        // Enrollment (ADR 0037 §4): certless first contact, authenticated
        // solely by a role-scoped bearer token, and served only here — the
        // pre-formation `closed_router` has no `/api/v1` tree at all, which is
        // what keeps "enrollment is refused until formation_complete" true
        // without a second check on this side. The body limit is a route layer
        // so an oversized request is refused by the framework before the
        // handler's own guards ever see it.
        .route(
            "/enroll",
            post(move |request: axum::extract::Request| {
                let enroll = enroll.clone();
                async move { enroll.handle(request).await }
            })
            .layer(axum::extract::DefaultBodyLimit::max(
                super::enroll::MAX_ENROLL_BODY,
            )),
        )
        // The auth posture (ADR 0022), captured like `/enroll` captures its
        // endpoint: the chain is not a `ControlPlane` operation either.
        .route(
            "/auth/config",
            get(move || {
                let authn = Arc::clone(&authn);
                async move { Json(auth_config(authn.mode())) }
            }),
        )
}

/// The authenticated `/api/v1` routes: everything that reads or writes this
/// cluster's state. [`api_v1_routes`] layers authentication over the whole
/// sub-router, so a route added here is authenticated by construction.
fn protected_routes<P: ControlPlane>() -> Router<Arc<P>> {
    Router::new()
        // Session / auth (ADR 0022) — local read, no raft involvement.
        .route("/session", get(get_session))
        // Cluster overview — bounded reads.
        .route("/overview", get(get_overview::<P>))
        .route("/queue/stats", get(get_queue_stats::<P>))
        // Jobs. List/detail are bounded; timeline and usage are eventual
        // (derived: ring events / samples); logs are provisional until log
        // storage exists.
        .route("/jobs", get(list_jobs::<P>).post(submit_job::<P>))
        .route("/jobs/:job", get(get_job::<P>))
        .route("/jobs/:job/abort", post(abort_job::<P>))
        .route("/jobs/:job/timeline", get(get_job_timeline::<P>))
        .route("/jobs/:job/usage", get(super::usage::get_job_usage::<P>))
        .route("/jobs/:job/logs", get(super::logs::get_job_logs::<P>))
        // Nodes. List/detail bounded; utilization/history eventual; logs
        // provisional.
        .route("/nodes", get(list_nodes::<P>))
        .route("/nodes/:node", get(get_node::<P>))
        .route(
            "/nodes/:node/utilization",
            get(unimplemented_id_read::<NodeId>("GetNodeUtilization")),
        )
        .route(
            "/nodes/:node/history",
            get(unimplemented_id_read::<NodeId>("GetNodeHistory")),
        )
        .route(
            "/nodes/:node/logs",
            get(unimplemented_id_read::<NodeId>("GetNodeLogs")),
        )
        // Coordinators — local status read; logs provisional.
        .route("/coordinators", get(get_coordinators::<P>))
        .route(
            "/coordinators/:id/logs",
            // Coordinator ids are raft ids: plain u64, not typed uuids (ADR 0024).
            get(unimplemented_id_read::<u64>("GetCoordinatorLogs")),
        )
        // Quota entities. List bounded; detail defaults strong (ADR 0007:
        // configuration reads); configure is the ADR-0023-gated upsert.
        .route(
            "/quota-entities",
            get(list_quota_entities::<P>).post(configure_quota_entity::<P>),
        )
        .route("/quota-entities/:entity", get(get_quota_entity::<P>))
        // Reserved: ADR 0008 event subscription (SSE, cursor-resumed).
        .route("/events", get(unimplemented_read("SubscribeEvents")))
}

/// `GET /api/v1/auth/config` — project the resolved posture (ADR 0022) into
/// its public DTO. Pure: everything it serves came from node config at
/// startup, so there is no state to read and no failure mode.
fn auth_config(mode: &coppice_authn::AuthMode) -> dto::GetAuthConfigResponse {
    match mode {
        coppice_authn::AuthMode::Oidc(config) => dto::GetAuthConfigResponse {
            mode: mode.as_str().to_string(),
            issuer: Some(config.issuer.clone()),
            client_id: Some(config.client_id.clone()),
            // The effective audience, already resolved by whoever built the
            // chain — a client is told what its token must actually carry,
            // never asked to re-apply the "defaults to client_id" rule.
            audience: Some(config.audience.clone()),
        },
        coppice_authn::AuthMode::Open => dto::GetAuthConfigResponse {
            mode: mode.as_str().to_string(),
            issuer: None,
            client_id: None,
            audience: None,
        },
    }
}

/// `GET /api/v1/session` — echo the identity the authentication layer resolved
/// for this very request (ADR 0022). A local read of the request itself: no
/// raft involvement, no state machine, no clock.
///
/// [`ReadQuery`] is extracted and discarded, like every other read route: the
/// ADR 0007 parameter contract (`?consistency=bogus` is `INVALID_ARGUMENT`)
/// holds across the whole read surface, including the reads that have nothing
/// to be consistent about.
///
/// **Seam for the role summary (ADR 0023).** This response is the actor and
/// nothing more; what the actor may *do* — the roles resolved against the
/// replicated bindings, scoped and unscoped — is the field it gains next, and
/// the reason `read_state` will appear in this handler when it does.
async fn get_session(
    RequestActor(actor): RequestActor,
    ReadQuery(_): ReadQuery,
) -> impl IntoResponse {
    Json(dto::GetSessionResponse {
        principal: actor.principal.clone(),
        groups: actor.groups.clone(),
        // Derived from the actor's flags rather than remembered separately, so
        // the reported method can never disagree with the grants it implies.
        auth_method: actor.method().as_str().to_string(),
    })
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

/// `GET /readyz` query parameters (ADR 0037 §9): the raw `require` value,
/// validated by [`ReadyzEndpoint::handle`] itself rather than here — the
/// endpoint owns the "healthy is the only recognized value" contract so it
/// stays true regardless of which router mounts it.
#[derive(Debug, Default, Deserialize)]
struct ReadyzQuery {
    #[serde(default)]
    require: Option<String>,
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

/// `GET /api/v1/jobs/{job}/timeline` query parameters, alongside the shared
/// [`ReadQuery`]. A separate extractor for the same reason as
/// [`ListJobsParams`]: `serde_urlencoded` cannot `#[serde(flatten)]` the
/// non-string read params, so those ride [`ReadQuery`] and these ride here.
#[derive(Debug, Default, Deserialize)]
struct TimelineParams {
    /// Opaque continuation token ([`dto::TimelineCursor`]) from a prior page.
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

/// `GET /api/v1/jobs/{job}/timeline` — one job's transition timeline, served
/// from this replica's fanout ring (ADR 0032, tier 1) and honestly partial.
///
/// **Eventual** by default: ADR 0032 re-classes this endpoint from bounded to
/// eventual — the events are a derived, replica-local read of the ring (and,
/// later, the durable store), not a point-in-time read of replicated state.
/// The read still honours `?consistency=`/`?min_index=` and carries the
/// staleness headers like every other read; the point-in-time state view is
/// used only for the 404-vs-empty verdict.
///
/// 404 only when a from-the-start scan (no `cursor`) exhausted the ring
/// (`next` is none) finding nothing, for a job unknown to this replica's
/// state: an evicted job with surviving ring events still answers 200, a
/// known job whose events aged out of the ring answers 200 with `floor_index`
/// telling the truncation story, and a budget-cut empty page answers 200 with
/// its continuation cursor rather than 404ing while events may sit deeper in
/// the ring — so pagination never dead-ends in a 404. A bad cursor or limit
/// is `INVALID_ARGUMENT` (400), never 404.
async fn get_job_timeline<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    IdPath(job): IdPath<JobId>,
    ReadQuery(read): ReadQuery,
    params: Result<Query<TimelineParams>, QueryRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Query(params) = params.map_err(|e: QueryRejection| HttpError::invalid(e.body_text()))?;

    // Same page-size contract as ListJobs (shared range + default); out of
    // range is INVALID_ARGUMENT, never clamped.
    let limit = match params.limit {
        None => DEFAULT_JOB_LIMIT,
        Some(n) if JOB_LIMIT_RANGE.contains(&(n as u64)) => n as u64,
        Some(n) => {
            return Err(HttpError::invalid(format!(
                "limit {n} is out of range {}..={}",
                JOB_LIMIT_RANGE.start(),
                JOB_LIMIT_RANGE.end(),
            )))
        }
    } as usize;

    let after = match &params.cursor {
        None => None,
        Some(token) => Some(dto::TimelineCursor::parse(token).map_err(HttpError::invalid)?),
    };

    let view = plane
        .read_state(read.into_options(Consistency::Eventual))
        .await?;
    let window = plane.job_timeline(job, after, limit).await;

    // An empty page is only proof of absence when the whole ring was scanned
    // from the start: a budget-cut page (`next` set) may have events deeper in
    // the ring, and a continuation page (`after` set) may be the empty tail of
    // a timeline already served — both answer 200, never a 404 dead-end. Only
    // a from-the-start, ring-exhausted, empty scan for a job this replica's
    // state has never heard of is NOT_FOUND.
    if window.events.is_empty()
        && window.next.is_none()
        && after.is_none()
        && !view.state().jobs.contains_key(&job)
    {
        return Err(HttpError::not_found(format!("job {job} not found")));
    }

    let response = super::project::job_timeline(&window);
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
    use coppice_authn::{no_ca, AuthnChain};
    use tower::ServiceExt;

    use super::super::dto::SubmitJobResponse;
    use crate::{
        ApiError, CoordinatorMemberSummary, CoordinatorSummary, JobTimelineWindow, QueueWindow,
        ReadOptions, ReadView, RecentClusterEvents, StampedEvent,
    };

    use crate::http::COPPICE_LEADER;

    /// Every test builds the router with a **detached** metrics endpoint: a
    /// non-installing recorder handle (issue #46), so `/metrics` exists but no
    /// global recorder is touched and parallel tests in one process never
    /// conflict. This shadows the crate [`super::router`] for the whole tests
    /// module so every existing call site stays a single `router(plane)`.
    ///
    /// The authn posture defaults to **open**: a route test is about the route,
    /// and open mode resolves every request to the anonymous actor and carries
    /// on, so the assertions here mean what they meant before authentication
    /// existed. The authentication tests build their own chain with
    /// [`router_with_authn`].
    fn router<P: ControlPlane>(plane: Arc<P>) -> Router {
        router_with_authn(plane, Arc::new(coppice_authn::AuthnChain::open(no_ca())))
    }

    /// [`router`] with an explicit authentication posture.
    fn router_with_authn<P: ControlPlane>(
        plane: Arc<P>,
        authn: Arc<coppice_authn::AuthnChain>,
    ) -> Router {
        super::router(
            plane,
            crate::http::MetricsEndpoint::detached_for_tests(),
            crate::http::ReadyzEndpoint::detached_for_tests(),
            crate::http::EnrollEndpoint::detached_for_tests(),
            authn,
        )
    }

    /// A canned `ControlPlane`: submit echoes the request's job id with a
    /// fixed log index, or fails with the configured error. Reads serve an
    /// empty state, and the derived sources serve whatever the test seeded
    /// (by default: no coverage, like a fresh replica).
    struct StubPlane {
        fail_with: Option<fn() -> ApiError>,
        queue_window: QueueWindow,
        recent: RecentClusterEvents,
        /// The ring window `job_timeline` serves, regardless of the job asked
        /// (the tier-1 backstop is exercised for its envelope/paging, not its
        /// filtering — that is unit-tested on the ring itself).
        timeline: JobTimelineWindow,
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

        async fn job_timeline(
            &self,
            _job: JobId,
            _after: Option<(u64, u32)>,
            _limit: usize,
        ) -> JobTimelineWindow {
            self.timeline.clone()
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

        async fn fetch_logs(
            &self,
            _node: coppice_core::id::NodeId,
            _addr: &str,
            _req: crate::LogFetchRequest,
        ) -> Result<crate::LogFetchOutcome, crate::LogFetchError> {
            // The log-endpoint walk is exercised against a dedicated fake in
            // `super::super::logs`; this plane never advertises a reachable
            // node, so it never reaches here.
            Err(crate::LogFetchError::Unreachable {
                reason: "stub plane serves no logs".to_string(),
            })
        }

        async fn fetch_metrics(
            &self,
            _node: coppice_core::id::NodeId,
            _addr: &str,
            _req: crate::MetricsFetchRequest,
        ) -> Result<crate::MetricsFetchOutcome, crate::MetricsFetchError> {
            // Like `fetch_logs`: the usage walk is exercised against a dedicated
            // fake in `super::super::usage`; this plane never reaches here.
            Err(crate::MetricsFetchError::Unreachable {
                reason: "stub plane serves no metrics".to_string(),
            })
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
            timeline: empty_timeline(),
            state,
            read_consistency: std::sync::Mutex::default(),
            // No handle by default: coordinator-status tests build their own
            // plane with a seeded summary.
            coordinator: None,
        }))
    }

    /// A `job_timeline` window covering nothing (a fresh replica), like the
    /// default `recent`: floor at the ReadView's applied index 1, no events,
    /// no continuation.
    fn empty_timeline() -> JobTimelineWindow {
        JobTimelineWindow {
            floor_index: 1,
            events: Vec::new(),
            next: None,
        }
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
        // `/events` rather than `/session`: the latter is a real handler now
        // (ADR 0022), and the reserved subscription route is the remaining
        // parameterless stub.
        let response = app(None)
            .oneshot(Request::get("/api/v1/events").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = body_json(response).await;
        assert_eq!(body["code"], "UNIMPLEMENTED");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("SubscribeEvents"));
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
            timeline: empty_timeline(),
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
        // A still-unimplemented id read (node utilization) with valid id and
        // read params answers 501 with its endpoint name — the timeline route
        // is now implemented and no longer part of this set.
        let node = NodeId::new();
        let response = app(None)
            .oneshot(
                Request::get(format!(
                    "/api/v1/nodes/{node}/utilization?consistency=strong&min_index=3"
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = body_json(response).await;
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("GetNodeUtilization"));
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
    async fn metrics_is_served_at_the_top_level_beside_the_nested_api() {
        // Issue #46: the scrape target rides the same listener as `/api/v1` but
        // is a sibling of it, not nested under it. A detached recorder means the
        // body may be empty here — the point is the route exists and answers the
        // Prometheus content type.
        let response = app(None)
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
            "text/plain; version=0.0.4"
        );
    }

    #[tokio::test]
    async fn metrics_is_not_reachable_under_the_api_prefix() {
        // `/api/v1/metrics` is not a route: it must fall through to the JSON 404
        // (an `/api/*` miss), proving `/metrics` was mounted top-level and the
        // nest did not accidentally absorb it.
        let response = app(None)
            .oneshot(Request::get("/api/v1/metrics").body(Body::empty()).unwrap())
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
                submitted_by: None,
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
            timeline: empty_timeline(),
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

    // ---- job timeline (GET /api/v1/jobs/{job}/timeline, ADR 0032) --------

    /// An `Arc<StubPlane>` serving a seeded ring window and state, so a
    /// timeline test can drive the route and inspect the read it defaulted to.
    fn timeline_stub(
        state: coppice_state::StateMachine,
        timeline: JobTimelineWindow,
    ) -> Arc<StubPlane> {
        Arc::new(StubPlane {
            fail_with: None,
            queue_window: QueueWindow::default(),
            recent: RecentClusterEvents {
                floor_index: 1,
                events: Vec::new(),
            },
            timeline,
            state,
            read_consistency: std::sync::Mutex::default(),
            coordinator: None,
        })
    }

    fn stamped(index: u64, ordinal: u32, job: JobId) -> StampedEvent {
        StampedEvent {
            index,
            ordinal,
            at: Timestamp::from_micros(0).unwrap(),
            event: coppice_state::Event::JobSubmitted { job },
        }
    }

    #[tokio::test]
    async fn timeline_serves_events_ascending_with_the_floor_and_headers() {
        let job: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let plane = timeline_stub(
            state_with_jobs(&[job]),
            JobTimelineWindow {
                floor_index: 5,
                events: vec![stamped(7, 0, job), stamped(9, 1, job)],
                next: None,
            },
        );
        let response = router(plane)
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}/timeline"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // Every read carries the staleness headers.
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_APPLIED_INDEX));
        assert!(response
            .headers()
            .contains_key(super::super::COPPICE_COMMITTED_INDEX));

        let body = body_json(response).await;
        assert_eq!(body["floor_index"], 5);
        // Ascending by (index, ordinal), the shared timeline shape.
        assert_eq!(body["events"][0]["index"], 7);
        assert_eq!(body["events"][0]["ordinal"], 0);
        assert_eq!(body["events"][0]["kind"], "job_submitted");
        assert_eq!(body["events"][0]["job"], job.to_string());
        assert_eq!(body["events"][1]["index"], 9);
        assert_eq!(body["events"][1]["ordinal"], 1);
        // Reached the newest retained event: explicit null, never omitted.
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn timeline_reports_a_continuation_cursor_and_accepts_it() {
        let job: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let plane = timeline_stub(
            state_with_jobs(&[job]),
            JobTimelineWindow {
                floor_index: 0,
                events: vec![stamped(7, 1, job)],
                next: Some((7, 1)),
            },
        );
        // Page 1 advertises the opaque cursor for the last examined event.
        let body = body_json(
            router(plane.clone())
                .oneshot(
                    Request::get(format!("/api/v1/jobs/{job}/timeline"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(body["next_cursor"], "v1:7:1");

        // That cursor is accepted verbatim on the follow-up (the route parses
        // it to an `(index, ordinal)` before asking the plane to continue).
        let response = router(plane)
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}/timeline?cursor=v1:7:1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn timeline_is_not_found_for_an_unknown_job_with_an_empty_window() {
        // A job this replica has never heard of, whose ring window is also
        // empty, is the one 404 case.
        let job = JobId::new();
        let plane = timeline_stub(coppice_state::StateMachine::default(), empty_timeline());
        let response = router(plane)
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}/timeline"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn timeline_answers_200_when_an_empty_page_still_has_a_continuation() {
        // The job is unknown to state and the page is empty, but the scan was
        // budget-cut (`next` set): events may sit deeper in the ring, so the
        // honest answer is 200 with the cursor — never a false 404 that
        // discards the continuation.
        let job = JobId::new();
        let plane = timeline_stub(
            coppice_state::StateMachine::default(),
            JobTimelineWindow {
                floor_index: 0,
                events: Vec::new(),
                next: Some((80_000, 3)),
            },
        );
        let response = router(plane)
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}/timeline"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["events"], serde_json::json!([]));
        assert_eq!(body["next_cursor"], "v1:80000:3");
    }

    #[tokio::test]
    async fn timeline_answers_200_for_an_empty_terminal_continuation_page() {
        // Page 2 of an evicted job whose events were all served on page 1: the
        // tail is empty and the ring is exhausted, but a resume (`cursor`
        // supplied) never dead-ends pagination in a 404 — it is the normal
        // terminal page.
        let job = JobId::new();
        let plane = timeline_stub(coppice_state::StateMachine::default(), empty_timeline());
        let response = router(plane)
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}/timeline?cursor=v1:7:1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["events"], serde_json::json!([]));
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn timeline_answers_200_for_a_known_job_with_an_empty_window() {
        // The job is in state but its events aged out of the ring: 200 with an
        // empty list and the floor telling the truncation story, not a 404.
        let job: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let plane = timeline_stub(
            state_with_jobs(&[job]),
            JobTimelineWindow {
                floor_index: 12,
                events: Vec::new(),
                next: None,
            },
        );
        let response = router(plane)
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}/timeline"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["events"], serde_json::json!([]));
        assert_eq!(body["floor_index"], 12);
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn timeline_answers_200_for_an_evicted_job_with_surviving_ring_events() {
        // The job is gone from state (evicted) but the ring still holds its
        // events: it answers, it is not a 404.
        let job = JobId::new();
        let plane = timeline_stub(
            coppice_state::StateMachine::default(),
            JobTimelineWindow {
                floor_index: 3,
                events: vec![stamped(4, 0, job)],
                next: None,
            },
        );
        let response = router(plane)
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}/timeline"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["events"][0]["index"], 4);
        assert_eq!(body["events"][0]["job"], job.to_string());
    }

    #[tokio::test]
    async fn timeline_rejects_bad_cursors_and_limits() {
        // Bad cursor/limit is INVALID_ARGUMENT (400), never 404 — parsed
        // before the job is ever looked up.
        let job = JobId::new();
        let cases = [
            format!("/api/v1/jobs/{job}/timeline?cursor=garbage"),
            format!("/api/v1/jobs/{job}/timeline?cursor=v2:7:1"),
            format!("/api/v1/jobs/{job}/timeline?cursor=v1:7"),
            format!("/api/v1/jobs/{job}/timeline?limit=0"),
            format!("/api/v1/jobs/{job}/timeline?limit=1001"),
        ];
        for uri in cases {
            let response = app(None)
                .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
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
    async fn timeline_defaults_to_an_eventual_read() {
        // ADR 0032 re-classes the timeline to eventual (derived, replica-local
        // ring), unlike the bounded job detail.
        let job: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let plane = timeline_stub(state_with_jobs(&[job]), empty_timeline());
        let response = router(plane.clone())
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job}/timeline"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            plane.read_consistency.lock().unwrap().last(),
            Some(&Consistency::Eventual)
        );
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
            timeline: empty_timeline(),
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
    // -----------------------------------------------------------------------
    // POST /api/v1/enroll (ADR 0037 §4)
    // -----------------------------------------------------------------------

    /// A router whose `/enroll` is backed by `issue`, with an otherwise
    /// canned control plane (nothing on this route touches it).
    fn enroll_app<F, Fut>(issue: F) -> Router
    where
        F: Fn(crate::http::EnrollCall) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<
                Output = Result<coppice_enroll::EnrollResponse, crate::http::EnrollRefusal>,
            > + Send
            + 'static,
    {
        super::router(
            Arc::new(StubPlane {
                fail_with: None,
                queue_window: QueueWindow::default(),
                recent: RecentClusterEvents {
                    floor_index: 1,
                    events: Vec::new(),
                },
                timeline: empty_timeline(),
                state: coppice_state::StateMachine::default(),
                read_consistency: std::sync::Mutex::default(),
                coordinator: None,
            }),
            crate::http::MetricsEndpoint::detached_for_tests(),
            crate::http::ReadyzEndpoint::detached_for_tests(),
            crate::http::EnrollEndpoint::new(issue),
            // `/enroll` is on the public sub-router, so the posture is
            // irrelevant to every assertion below; open is the quiet choice.
            Arc::new(AuthnChain::open(no_ca())),
        )
    }

    /// An endpoint that issues a canned leaf for any token.
    fn issuing_app() -> Router {
        enroll_app(|_call| async {
            Ok(coppice_enroll::EnrollResponse {
                cert_pem: "LEAF".to_string(),
                ca_pem: "CA".to_string(),
            })
        })
    }

    fn enroll_request(token: Option<&str>, body: &str) -> Request<Body> {
        let mut builder =
            Request::post("/api/v1/enroll").header(header::CONTENT_TYPE, "application/json");
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        builder.body(Body::from(body.to_string())).unwrap()
    }

    const CSR_BODY: &str = r#"{"csr_pem":"-----BEGIN CERTIFICATE REQUEST-----"}"#;

    /// The refusal every failed credential renders, captured once so the tests
    /// below compare against the same bytes the endpoint promises.
    async fn refusal_parts(response: axum::response::Response) -> (StatusCode, Vec<u8>) {
        let status = response.status();
        assert!(
            response.headers().get(header::SET_COOKIE).is_none(),
            "the enrollment route never sets a cookie"
        );
        assert!(
            !response
                .headers()
                .keys()
                .any(|k| k.as_str().starts_with("access-control-")),
            "the enrollment route never emits CORS headers"
        );
        assert!(
            !status.is_redirection(),
            "a token-carrying client is never redirected (ADR 0037 §4)"
        );
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, bytes.to_vec())
    }

    #[tokio::test]
    async fn a_bearer_token_enrolls_and_the_response_carries_the_leaf_and_ca() {
        let response = issuing_app()
            .oneshot(enroll_request(Some("cpk_good"), CSR_BODY))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::SET_COOKIE).is_none());
        let body = body_json(response).await;
        assert_eq!(body["cert_pem"], "LEAF");
        assert_eq!(body["ca_pem"], "CA");
    }

    #[tokio::test]
    async fn the_body_token_field_is_accepted_when_no_header_is_present() {
        let app = enroll_app(|call| async move {
            assert_eq!(call.token, "cpk_body");
            Ok(coppice_enroll::EnrollResponse {
                cert_pem: "LEAF".to_string(),
                ca_pem: "CA".to_string(),
            })
        });
        let response = app
            .oneshot(enroll_request(
                None,
                r#"{"csr_pem":"csr","token":"cpk_body"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn the_header_wins_over_the_body_token() {
        let app = enroll_app(|call| async move {
            assert_eq!(call.token, "cpk_header");
            Ok(coppice_enroll::EnrollResponse {
                cert_pem: "LEAF".to_string(),
                ca_pem: "CA".to_string(),
            })
        });
        let response = app
            .oneshot(enroll_request(
                Some("cpk_header"),
                r#"{"csr_pem":"csr","token":"cpk_body"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Every credential failure — none supplied, one the core refused, and a
    /// query parameter that is never read — is one indistinguishable response.
    #[tokio::test]
    async fn every_authentication_failure_is_byte_identical() {
        let refusing =
            || enroll_app(|_call| async { Err(crate::http::EnrollRefusal::Unauthorized) });

        let no_token = refusal_parts(
            refusing()
                .oneshot(enroll_request(None, CSR_BODY))
                .await
                .unwrap(),
        )
        .await;
        let refused_token = refusal_parts(
            refusing()
                .oneshot(enroll_request(Some("cpk_unknown"), CSR_BODY))
                .await
                .unwrap(),
        )
        .await;
        // A token in the query string is not a credential: the route never
        // looks there, so this is simply a request with no token.
        let query_token = refusal_parts(
            refusing()
                .oneshot(
                    Request::post("/api/v1/enroll?token=cpk_query")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(CSR_BODY))
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        // A malformed bearer scheme is also just "no credential".
        let bad_scheme = refusal_parts(
            refusing()
                .oneshot(
                    Request::post("/api/v1/enroll")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, "Basic cpk_unknown")
                        .body(Body::from(CSR_BODY))
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;

        assert_eq!(no_token.0, StatusCode::UNAUTHORIZED);
        assert_eq!(no_token, refused_token);
        assert_eq!(no_token, query_token);
        assert_eq!(no_token, bad_scheme);
        assert_eq!(
            String::from_utf8(no_token.1).unwrap(),
            crate::http::REFUSED_BODY
        );
    }

    /// A request that presented a client certificate is refused with the same
    /// response: `/enroll` is certless first contact, and a machine holding a
    /// leaf renews on the machine plane instead.
    #[tokio::test]
    async fn a_certificate_bearing_request_is_refused_identically() {
        let mut request = enroll_request(Some("cpk_good"), CSR_BODY);
        request
            .extensions_mut()
            .insert(crate::http::PeerCertificates(Arc::new(vec![vec![0u8; 4]])));

        let (status, body) = refusal_parts(issuing_app().oneshot(request).await.unwrap()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(String::from_utf8(body).unwrap(), crate::http::REFUSED_BODY);
    }

    #[tokio::test]
    async fn an_oversized_body_is_refused_before_anything_parses_it() {
        let issued = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = Arc::clone(&issued);
        let app = enroll_app(move |_call| {
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async {
                Ok(coppice_enroll::EnrollResponse {
                    cert_pem: "LEAF".to_string(),
                    ca_pem: "CA".to_string(),
                })
            }
        });

        let huge = format!(
            r#"{{"csr_pem":"{}"}}"#,
            "A".repeat(crate::http::MAX_ENROLL_BODY + 1)
        );
        let response = app
            .oneshot(enroll_request(Some("cpk_good"), &huge))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            issued.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the CSR must never reach the issuer"
        );
    }

    #[tokio::test]
    async fn the_concurrency_cap_sheds_the_ninth_in_flight_request() {
        // Eight enrollments park inside the issuer; the ninth must be shed
        // rather than queued, because each in-flight one holds real CPU.
        let gate = Arc::new(tokio::sync::Notify::new());
        let release = Arc::clone(&gate);
        let app = enroll_app(move |_call| {
            let gate = Arc::clone(&gate);
            async move {
                gate.notified().await;
                Ok(coppice_enroll::EnrollResponse {
                    cert_pem: "LEAF".to_string(),
                    ca_pem: "CA".to_string(),
                })
            }
        });

        let mut parked = Vec::new();
        for _ in 0..8 {
            let app = app.clone();
            parked.push(tokio::spawn(async move {
                app.oneshot(enroll_request(Some("cpk_good"), CSR_BODY))
                    .await
                    .unwrap()
                    .status()
            }));
        }
        // Let all eight reach the issuer and take their permits.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }

        let shed = app
            .clone()
            .oneshot(enroll_request(Some("cpk_good"), CSR_BODY))
            .await
            .unwrap();
        assert_eq!(shed.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body_json(shed).await["code"], "UNAVAILABLE");

        release.notify_waiters();
        for handle in parked {
            assert_eq!(handle.await.unwrap(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn the_rate_limiter_sheds_a_flood_with_429() {
        let app = issuing_app();
        // The burst is 20 and refills at 10/s; 21 back-to-back requests in one
        // test tick cannot all be admitted.
        let mut statuses = Vec::new();
        for _ in 0..21 {
            statuses.push(
                app.clone()
                    .oneshot(enroll_request(Some("cpk_good"), CSR_BODY))
                    .await
                    .unwrap()
                    .status(),
            );
        }
        assert_eq!(
            statuses
                .iter()
                .filter(|s| **s == StatusCode::TOO_MANY_REQUESTS)
                .count(),
            1,
            "exactly the over-burst request is shed: {statuses:?}"
        );
    }

    #[tokio::test]
    async fn a_malformed_body_is_an_invalid_argument_not_a_credential_verdict() {
        let response = issuing_app()
            .oneshot(enroll_request(Some("cpk_good"), "not json"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn enrollment_is_absent_from_the_pre_formation_surface() {
        // The closed router has no `/api/v1` tree at all, so enrollment is
        // refused until `formation_complete` exists (ADR 0037 §3) without a
        // second gate on this side.
        let closed = super::closed_router(
            crate::http::MetricsEndpoint::detached_for_tests(),
            crate::http::ReadyzEndpoint::detached_for_tests(),
        );
        let response = closed
            .oneshot(enroll_request(Some("cpk_good"), CSR_BODY))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ---- Authentication (ADR 0022) ---------------------------------------

    /// A router in the OIDC posture against a running fake IdP, with the JWKS
    /// already fetched.
    ///
    /// The fetch is explicit rather than left to the unknown-`kid` on-demand
    /// path so a failure here reads as "the fixture is not serving keys" and
    /// not as "the middleware rejected the token" — the on-demand refetch has
    /// its own tests, in the crate that owns it.
    async fn oidc_chain(idp: &coppice_testkit::oidc::FakeIdp) -> Arc<AuthnChain> {
        oidc_chain_with_ca(idp, no_ca()).await
    }

    /// [`oidc_chain`] with an explicit cluster CA, for the operator-certificate
    /// tests.
    async fn oidc_chain_with_ca(
        idp: &coppice_testkit::oidc::FakeIdp,
        ca: coppice_authn::CaProvider,
    ) -> Arc<AuthnChain> {
        let config = coppice_authn::OidcConfig {
            issuer: idp.issuer(),
            client_id: TEST_CLIENT_ID.to_string(),
            audience: TEST_CLIENT_ID.to_string(),
        };
        let cache = coppice_authn::JwksCache::new(
            coppice_authn::default_http_client(),
            config.issuer.clone(),
        );
        cache
            .refresh_now()
            .await
            .expect("the fake IdP serves discovery and a JWKS");
        let validator = coppice_authn::Validator::new(cache, config.clone());
        Arc::new(AuthnChain::oidc(
            validator,
            coppice_authn::static_groups_claim(coppice_authn::DEFAULT_GROUPS_CLAIM),
            ca,
            config,
        ))
    }

    /// The client id (and, by the "defaults to client_id" rule, the audience)
    /// every OIDC-posture test mints tokens for.
    const TEST_CLIENT_ID: &str = "coppice-test";

    fn bearer_request(uri: &str, token: &str) -> Request<Body> {
        Request::get(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    /// A freshly-minted cluster CA and an operator leaf under it, as the DER
    /// the TLS layer would hand the router.
    fn operator_credential(cn: &str) -> (Vec<u8>, Vec<u8>) {
        let ca = coppice_tls::pki::mint_root_ca().expect("mint a root CA");
        let signer =
            coppice_tls::pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load the signer");
        let (leaf_pem, _key_pem) =
            coppice_tls::pki::mint_operator_local(&signer, cn).expect("mint an operator leaf");
        let leaf_der = rustls_pemfile::certs(&mut leaf_pem.as_slice())
            .next()
            .expect("the leaf PEM holds a certificate")
            .expect("the leaf PEM parses")
            .to_vec();
        (ca.cert_pem, leaf_der)
    }

    /// `request`, as it would arrive over a connection that presented `leaf`.
    ///
    /// The extension is what `clientedge::serve` inserts after a successful
    /// handshake — end-entity first, exactly as rustls hands over the chain —
    /// so driving the router through `oneshot` exercises the same code the
    /// listener does.
    fn with_peer_cert(mut request: Request<Body>, leaf_der: Vec<u8>) -> Request<Body> {
        request
            .extensions_mut()
            .insert(crate::http::PeerCertificates(Arc::new(vec![leaf_der])));
        request
    }

    #[tokio::test]
    async fn oidc_mode_refuses_a_request_with_no_credentials() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let response = router_with_authn(stub_plane(Default::default()), oidc_chain(&idp).await)
            .oneshot(Request::get("/api/v1/session").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = body_json(response).await;
        assert_eq!(body["code"], "UNAUTHENTICATED");
        // The documented body is exactly the two fields, and the message says
        // what was missing without inventing a credential that was not sent.
        assert_eq!(body.as_object().unwrap().len(), 2);
        assert_eq!(body["message"], "no credentials were presented");
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn oidc_mode_admits_a_valid_bearer_and_session_echoes_the_actor() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let token = idp.sign(
            coppice_testkit::oidc::TokenClaims::new("user-42")
                .audience(TEST_CLIENT_ID)
                .claim("groups", serde_json::json!(["batch-users", "sre"])),
        );
        let response = router_with_authn(stub_plane(Default::default()), oidc_chain(&idp).await)
            .oneshot(bearer_request("/api/v1/session", &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["principal"], "user-42");
        assert_eq!(body["groups"], serde_json::json!(["batch-users", "sre"]));
        assert_eq!(body["auth_method"], "bearer");
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn oidc_mode_refuses_a_garbage_bearer() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let response = router_with_authn(stub_plane(Default::default()), oidc_chain(&idp).await)
            .oneshot(bearer_request("/api/v1/session", "not-a-jwt"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = body_json(response).await;
        assert_eq!(body["code"], "UNAUTHENTICATED");
        // The refusal names the mechanism, never the credential.
        let message = body["message"].as_str().unwrap();
        assert!(message.contains("bearer token"), "{message}");
        assert!(!message.contains("not-a-jwt"), "{message}");
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn an_operator_certificate_authenticates_as_its_common_name() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let (ca_pem, leaf_der) = operator_credential("alice");
        let chain = oidc_chain_with_ca(&idp, Arc::new(move || Some(ca_pem.clone()))).await;
        let response = router_with_authn(stub_plane(Default::default()), chain)
            .oneshot(with_peer_cert(
                Request::get("/api/v1/session").body(Body::empty()).unwrap(),
                leaf_der,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["principal"], "cert:alice");
        assert_eq!(body["groups"], serde_json::json!([]));
        assert_eq!(body["auth_method"], "operator_cert");
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn an_operator_certificate_wins_over_a_bearer_on_the_same_request() {
        // Break-glass that a stale token in the client's environment could
        // shadow would not be much of a break-glass (ADR 0022): the
        // certificate decides, and the token is never even validated.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let (ca_pem, leaf_der) = operator_credential("breakglass");
        let token =
            idp.sign(coppice_testkit::oidc::TokenClaims::new("user-42").audience(TEST_CLIENT_ID));
        let chain = oidc_chain_with_ca(&idp, Arc::new(move || Some(ca_pem.clone()))).await;
        let response = router_with_authn(stub_plane(Default::default()), chain)
            .oneshot(with_peer_cert(
                bearer_request("/api/v1/session", &token),
                leaf_der,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["principal"], "cert:breakglass");
        assert_eq!(body["auth_method"], "operator_cert");
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn open_mode_resolves_every_request_to_the_anonymous_actor() {
        let response = app(None)
            .oneshot(Request::get("/api/v1/session").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["principal"], "anonymous");
        assert_eq!(body["groups"], serde_json::json!([]));
        assert_eq!(body["auth_method"], "open");
    }

    #[tokio::test]
    async fn session_still_validates_the_consistency_parameter() {
        // The ADR 0007 read-parameter contract holds on every read route,
        // including the one that reads nothing but the request itself.
        let response = app(None)
            .oneshot(
                Request::get("/api/v1/session?consistency=bogus")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn auth_config_is_public_and_describes_the_open_posture() {
        let response = app(None)
            .oneshot(
                Request::get("/api/v1/auth/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // Exactly one field: an open deployment has no OIDC configuration to
        // describe, and three nulls would invite a login form.
        assert_eq!(
            body_json(response).await,
            serde_json::json!({"mode": "open"})
        );
    }

    #[tokio::test]
    async fn auth_config_is_public_and_describes_the_oidc_posture() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let issuer = idp.issuer();
        // No credentials: this is what a client reads to learn how to obtain
        // one, so requiring one would be a loop with no entry point.
        let response = router_with_authn(stub_plane(Default::default()), oidc_chain(&idp).await)
            .oneshot(
                Request::get("/api/v1/auth/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_json(response).await,
            serde_json::json!({
                "mode": "oidc",
                "issuer": issuer,
                "client_id": TEST_CLIENT_ID,
                // The *effective* audience, already resolved: a client is
                // never asked to re-apply the "defaults to client_id" rule.
                "audience": TEST_CLIENT_ID,
            })
        );
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn enrollment_is_not_touched_by_the_authn_layer() {
        // `/enroll` is on the public sub-router: a certless enrollee has no
        // user credential to present, and its own uniform refusal (ADR 0037
        // §4) must be what a caller sees — not the API's 401. The detached
        // endpoint refuses every token, so reaching that refusal, byte for
        // byte, is the proof the layer did not intercept.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let response = router_with_authn(stub_plane(Default::default()), oidc_chain(&idp).await)
            .oneshot(enroll_request(None, CSR_BODY))
            .await
            .unwrap();
        let (status, body) = refusal_parts(response).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body, crate::http::REFUSED_BODY.as_bytes());
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn the_layer_covers_the_whole_protected_tree_not_just_session() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;

        let response = router_with_authn(stub_plane(Default::default()), Arc::clone(&chain))
            .oneshot(
                Request::get("/api/v1/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(response).await["code"], "UNAUTHENTICATED");

        let token =
            idp.sign(coppice_testkit::oidc::TokenClaims::new("user-42").audience(TEST_CLIENT_ID));
        let response = router_with_authn(stub_plane(Default::default()), chain)
            .oneshot(bearer_request("/api/v1/overview", &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["cluster_id"], STUB_CLUSTER);
        idp.shutdown().await;
    }
}
