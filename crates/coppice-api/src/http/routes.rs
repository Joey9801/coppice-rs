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

// `method()` on the resolved actor: the actor is `coppice_state::Actor`, so
// the authentication edge's view of it arrives as an extension trait.
use coppice_authn::ActorExt;
use coppice_core::id::{JobId, NodeId, QuotaEntityId};
use coppice_core::time::Timestamp;

use super::dto::{
    self, AbortJobRequest, AbortJobResponse, ConfigureQuotaEntityRequest, SubmitJobRequest,
};
use crate::{Consistency, ControlPlane};

use super::authn::{RequestActor, RequestPresentation};
use super::authorize::{precheck, Intent};
use super::enroll::EnrollEndpoint;
use super::error::{authorization_error, HttpError};
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
/// `ControlPlane`. Its one link to the plane is the ADR 0039 usage *section*
/// attached below: a scrape-time render of `usage_window`, which is what makes
/// a departed node's series disappear instead of freezing.
///
/// `/api/v1` misses are answered by that nested router's **own** JSON-404
/// fallback rather than falling through to the outer
/// [`fallback`](super::ui::fallback) — that is what puts them inside the
/// authentication boundary (see [`api_v1_routes`]). The outer fallback still
/// owns everything else, unchanged: `/api/*` paths outside `/api/v1` (a
/// hypothetical `/api/v2`, or `/api/anything`) answer the same JSON 404, and
/// every other path reaches the UI (static assets + SPA shell, ADR 0031
/// "Serving the UI").
///
/// `authn` is the deployment's authentication posture (ADR 0022), built by the
/// coordinator: [`api_v1_routes`] layers it over the whole `/api/v1` tree.
/// `/metrics` and `/readyz` are outside `/api/v1` and therefore outside
/// authentication — a scrape and a readiness probe are operational surfaces,
/// and the listener's own posture is what guards them.
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
    // The one route that reads the plane *outside* `/api/v1`: the usage
    // section of a scrape (ADR 0039) renders from the live view at scrape
    // time, because a node-labelled gauge parked in the recorder would keep
    // rendering a departed node's last reading forever. `closed_router` has
    // no plane and therefore no such section — a pre-formation daemon has no
    // usage to report.
    let usage_plane = Arc::clone(&plane);
    let metrics = metrics.with_section(move || {
        super::usage_metrics::render(
            &usage_plane.usage_window(),
            coppice_core::time::Timestamp::now(),
        )
    });

    operational_routes(metrics, readyz)
        .nest("/api/v1", api_v1_routes::<P>(enroll, authn))
        // Everything unrouted *outside* `/api/v1`: other `/api/*` paths stay
        // JSON 404s; anything else serves the embedded web UI (static assets +
        // SPA fallback, ADR 0031 "Serving the UI").
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
/// ## One table, one boundary
///
/// This is a single flat route table — there is deliberately no
/// public/protected split of the router any more. Authentication (ADR 0022) is
/// a property of the **namespace**: the
/// [`authenticate`](super::authn::authenticate) layer wraps this whole router,
/// its own 404 fallback included, so *every* request under `/api/v1` runs the
/// chain. That closes the two holes a route-registration split left open — an
/// unrouted `/api/v1/…` path fell through to the outer fallback and answered
/// 404 uncredentialed, and a method-miss on a public path answered 405 the
/// same way — and it means a route added below cannot ship unauthenticated by
/// being written in the wrong sub-router, because there is no wrong
/// sub-router.
///
/// The two credential-less endpoints are instead named, once, in
/// [`UNAUTHENTICATED_ROUTES`](super::authn::UNAUTHENTICATED_ROUTES): exact
/// method-and-path pairs the middleware waves through. That list is the single
/// source of truth for "reachable without a credential", and it is checked
/// against the request rather than against how a handler was registered, so
/// the two can never drift.
///
/// The fallback matters as much as the routes: without one here, an `/api/v1`
/// miss would leave the nested router entirely and be answered by the outer
/// fallback, *outside* the layer. It renders the same JSON 404 the outer
/// fallback gives an `/api/*` path — every request that reaches it is under
/// `/api/v1` by construction, so it needs no path test to know it is an API
/// miss and not a UI route.
fn api_v1_routes<P: ControlPlane>(
    enroll: EnrollEndpoint,
    authn: Arc<coppice_authn::AuthnChain>,
) -> Router<Arc<P>> {
    all_routes::<P>(enroll, Arc::clone(&authn))
        .fallback(api_not_found)
        .layer(axum::middleware::from_fn_with_state(
            authn,
            super::authn::authenticate,
        ))
}

/// The JSON 404 for an `/api/v1` path that matches no route.
///
/// Byte-identical to what [`super::ui::fallback`] answers an `/api/*` miss —
/// the same [`HttpError::not_found`] with the same message — because it is the
/// same contract; the only difference is that this one sits *inside* the
/// authentication layer, so an anonymous caller is told 401 and never learns
/// which `/api/v1` paths exist.
async fn api_not_found() -> HttpError {
    HttpError::not_found("no such route")
}

/// Every `/api/v1` route, in one table.
fn all_routes<P: ControlPlane>(
    enroll: EnrollEndpoint,
    authn: Arc<coppice_authn::AuthnChain>,
) -> Router<Arc<P>> {
    credential_free_routes::<P>(enroll, authn).merge(state_routes::<P>())
}

/// The two routes the middleware's exemption table lets through
/// uncredentialed (see
/// [`UNAUTHENTICATED_ROUTES`](super::authn::UNAUTHENTICATED_ROUTES) for *why*
/// each is exempt).
///
/// They are grouped here only because they share a shape — neither is a
/// `ControlPlane` operation and neither extracts a
/// [`RequestActor`] — not because grouping them is what makes them public.
/// Moving a route out of this function grants it nothing and revokes nothing;
/// the exemption table is the only thing that decides.
fn credential_free_routes<P: ControlPlane>(
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

/// Everything that reads or writes this cluster's state — that is, every
/// `/api/v1` route but the two credential-free ones.
///
/// A separate function purely for readability; it is merged into one table by
/// [`all_routes`] and authenticated by [`api_v1_routes`]'s layer along with
/// everything else, so a route added here is authenticated by construction —
/// as is a route added anywhere else under the prefix.
fn state_routes<P: ControlPlane>() -> Router<Arc<P>> {
    Router::new()
        // Session / auth (ADR 0022) — the request's own identity, plus the
        // ADR 0023 authority summary read from replicated bindings.
        .route("/session", get(get_session::<P>))
        // Authorization policy (ADR 0023). The read is an ordinary bounded
        // read; the write is the unscoped-admin-only full replacement.
        .route(
            "/authorization",
            get(get_authorization::<P>).put(update_authorization::<P>),
        )
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
        .route("/nodes/:node/utilization", get(get_node_utilization::<P>))
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

/// `GET /api/v1/session` — the identity the authentication layer resolved for
/// this very request (ADR 0022), plus what that identity may do (ADR 0023).
///
/// Two sources, and the response says which is which. `principal`, `groups`,
/// and `auth_method` come from the request's own credential and involve no
/// state at all. `bindings` and `implicit_admin` are the **resolved-authority
/// summary**: the replicated bindings whose subject names this actor, each
/// reported faithfully as role + scope.
///
/// Faithfully, and not collapsed into one effective role. The union over
/// matching bindings is `authz::evaluate`'s business, and a server that
/// answered "you are an operator" would be publishing a second, simpler model
/// of authority beside the real one — which is exactly how the two come to
/// disagree. The subject matching itself reuses
/// [`Binding::matches`](coppice_state::authz::Binding::matches), the same
/// predicate `evaluate` applies, so this summary cannot drift from the
/// decisions it describes.
///
/// **Eventual** by default: an authority summary is a display, and paying for
/// a consensus round trip to render one would be the wrong trade — nothing is
/// authorized on the strength of what this endpoint says.
///
/// [`ReadQuery`] is honoured like every other read route, so the ADR 0007
/// parameter contract (`?consistency=bogus` is `INVALID_ARGUMENT`, and a
/// caller who *does* want a fresher answer can ask for one) holds here too.
async fn get_session<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    RequestActor(actor): RequestActor,
    RequestPresentation(presentation): RequestPresentation,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Eventual))
        .await?;
    let bindings = coppice_state::authz::matching_bindings(&view.state().bindings, &actor)
        .map(|b| dto::SessionBinding {
            role: (&b.role).into(),
            scope: b.scope,
        })
        .collect();
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(dto::GetSessionResponse {
            principal: actor.principal.clone(),
            groups: actor.groups.clone(),
            // Derived from the actor's flags rather than remembered separately,
            // so the reported method can never disagree with the grants it
            // implies.
            auth_method: actor.method().as_str().to_string(),
            // Presentation claims from the verified token, per request and
            // never stored (ADR 0022); both null for operator-cert/open.
            name: presentation.name,
            email: presentation.email,
            bindings,
            // An operator certificate or the open posture: unscoped admin from
            // outside the list, so it could never appear *in* `bindings`.
            implicit_admin: actor.is_implicit_admin(),
        }),
    ))
}

/// `GET /api/v1/authorization` — the replicated authorization policy: the full
/// bindings list and the `groups_claim` name (ADR 0023).
///
/// **Strong** by default (ADR 0031's table), which puts it in ADR 0007's
/// configuration-read class alongside [`get_quota_entity`] rather than with
/// the bounded list reads. The document this serves is the one an operator
/// edits and PUTs back: a read-modify-write over a stale snapshot silently
/// reverts whatever landed in between, and full replacement makes that a
/// deletion rather than a merge conflict.
///
/// Role-unchecked, like every read in v1: any authenticated principal may see
/// the bindings list. It names groups and principals, which the people in it
/// already know they are, and hiding it would mostly stop an operator from
/// debugging why their own grant does not apply.
async fn get_authorization<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Strong))
        .await?;
    let state = view.state();
    let response = dto::GetAuthorizationResponse {
        groups_claim: state.policy.groups_claim.clone(),
        bindings: state.bindings.iter().map(Into::into).collect(),
    };
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
}

/// `PUT /api/v1/authorization` — full replacement of the replicated bindings
/// (ADR 0023), an unscoped-admin verb. A `groups_claim` in the body rides the
/// same command and lands in the same apply, so renaming the claim while
/// swapping the admin group is one atomic edit, never a half-applied pair.
///
/// Three refusals, in the order they can be reached:
///
/// 1. A body whose bindings do not name exactly one of `group`/`principal` is
///    `INVALID_ARGUMENT` — serde cannot express "exactly one", so the
///    conversion does, and it is checked before anything is proposed.
/// 2. An actor without an unscoped admin binding is `PERMISSION_DENIED`, from
///    the same [`precheck`] every other mutating handler runs.
/// 3. Apply's own checks — an unknown scope, a malformed subject, a list that
///    would retain no unscoped admin — come back as rejections, and *this*
///    endpoint reads them as a malformed document rather than a lost race:
///    [`authorization_error`] maps them to `INVALID_ARGUMENT` with
///    distinguishing text. That mapping is endpoint-local by necessity;
///    `UnknownQuotaEntity` remains a 409 on submit and on the quota upsert.
async fn update_authorization<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    RequestActor(actor): RequestActor,
    body: Result<Json<dto::UpdateAuthorizationRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = body.map_err(bad_body)?;

    // The exactly-one-subject rule, checked here and discarded: the plane
    // takes the DTO (like every other write) and converts it itself, but a
    // violation is a malformed *request*, not a rejection, and must not cost a
    // consensus round trip to discover.
    for (i, binding) in request.bindings.iter().enumerate() {
        coppice_state::authz::Binding::try_from(binding)
            .map_err(|e| HttpError::invalid(format!("binding {i}: {e}")))?;
    }

    precheck(&*plane, &actor, Intent::UpdateAuthorization).await?;
    let response = plane
        .update_authorization(request, actor)
        .await
        .map_err(authorization_error)?;
    Ok(Json(response))
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
///
/// Gated on `submitter` or higher over the charged quota entity (ADR 0023):
/// [`precheck`] answers 403 before anything is proposed, and the actor rides
/// the command for apply's authoritative re-check.
async fn submit_job<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    RequestActor(actor): RequestActor,
    body: Result<Json<SubmitJobRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = body.map_err(bad_body)?;
    precheck(
        &*plane,
        &actor,
        Intent::Submit {
            entity: &request.quota_entity,
        },
    )
    .await?;
    let response = plane.submit_job(request, actor).await?;
    Ok(Json(response))
}

/// `POST /api/v1/jobs/{job}/abort` — body `AbortJobRequest`. The path
/// segment is authoritative for the job id: the body's `job` field may be
/// omitted (`{}` aborts with no reason) and, when present, must match the
/// path — a mismatch is `INVALID_ARGUMENT`, never silently resolved.
///
/// Gated on `operator` or higher over the job's quota entity, **or** on having
/// submitted the job (ADR 0023's ownership grant, which
/// `authz::evaluate` applies for free once [`precheck`] has resolved the job's
/// `submitted_by` from the view).
async fn abort_job<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    RequestActor(actor): RequestActor,
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
    precheck(&*plane, &actor, Intent::Abort { job }).await?;
    plane.abort_job(request, actor).await?;
    Ok(Json(AbortJobResponse {}))
}

/// `POST /api/v1/quota-entities` — body `ConfigureQuotaEntityRequest`, the
/// create-or-update upsert (ADR 0031's write class). Response echoes the
/// client-minted entity id + `log_index` for read-your-writes, exactly like
/// `SubmitJob`. A cycle / unknown-parent refusal maps to `REJECTED` (409),
/// the normal committed-and-refused outcome — and stays a 409 here even
/// though `PUT /api/v1/authorization` reads the same `UnknownQuotaEntity` as a
/// 400: on this endpoint an unknown parent really is a race with whoever
/// deleted it, which is what 409 means.
///
/// Gated on `admin` covering the entity's position, and — when the request
/// actually reparents it — covering the new parent too, under a single
/// binding (ADR 0023: reparenting moves authority, so a cross-subtree move,
/// like a move to the root, takes unscoped admin). The `new_parent` handed to
/// the check is the request's `parent` verbatim, including its absence, which
/// is what makes "move to the root" distinguishable from "not a move".
async fn configure_quota_entity<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    RequestActor(actor): RequestActor,
    body: Result<Json<ConfigureQuotaEntityRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = body.map_err(bad_body)?;
    precheck(
        &*plane,
        &actor,
        Intent::ConfigureQuotaEntity {
            entity: &request.entity,
            new_parent: request.parent.as_ref(),
        },
    )
    .await?;
    let response = plane.configure_quota_entity(request, actor).await?;
    Ok(Json(response))
}

/// `GET /api/v1/overview` — bounded by default (ADR 0031) for the
/// replicated-state fields; the rates/history are derived, replica-local
/// reads (ADR 0032).
async fn get_overview<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Bounded))
        .await?;
    let window = plane.queue_window();
    // Only reads sample the clock — they are not replicated, so a handler
    // may (an *apply* may never: `coppice-state`'s determinism contract).
    // It feeds read-time ages like `oldest_queued_age_seconds`, never
    // anything stored.
    let response = super::project::cluster_overview(
        view.state(),
        plane.cluster_id(),
        Timestamp::now(),
        &window,
        &plane.usage_window(),
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
    let response = super::project::list_nodes(view.state(), &plane.usage_window());
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
    let response = super::project::get_node(view.state(), &id, &plane.usage_window())
        .ok_or_else(|| HttpError::not_found(format!("node {id} not found")))?;
    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
}

/// `GET /api/v1/nodes/{node}/utilization` — eventual by default (ADR 0031:
/// a derived read). The replicated half (the node's existence and capacity)
/// comes from the read view, the samples from this replica's in-memory usage
/// window (ADR 0039) — so a follower answers 200 with an empty `samples`,
/// never a redirect. 404 when the node is not in the read view, exactly as
/// [`get_node`]: an unknown node and a known-but-uncollected one are
/// different answers.
async fn get_node_utilization<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    IdPath(id): IdPath<NodeId>,
    ReadQuery(params): ReadQuery,
) -> Result<impl IntoResponse, HttpError> {
    let view = plane
        .read_state(params.into_options(Consistency::Eventual))
        .await?;
    let response = super::project::node_utilization(view.state(), &id, &plane.usage_window())
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
        ReadOptions, ReadView, StampedEvent, UsageSnapshot,
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
        /// The ADR 0039 usage snapshot the plane serves; the default is a
        /// replica with nothing measured (every `used` absent).
        usage: UsageSnapshot,
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
        /// The actor each mutating call arrived with, in call order.
        ///
        /// Recorded because "the handler answered 200" is only half of what
        /// the authorization work has to prove: the other half is that the
        /// identity the authentication layer resolved is the one that reaches
        /// the plane, since that is what rides the command and what apply
        /// re-checks. A dropped actor passes every status assertion.
        actors: std::sync::Mutex<Vec<coppice_state::Actor>>,
        /// The last `PUT /api/v1/authorization` body the plane was handed.
        authorization: std::sync::Mutex<Option<dto::UpdateAuthorizationRequest>>,
    }

    impl StubPlane {
        /// Every actor that reached a mutating method, in call order.
        fn actors(&self) -> Vec<coppice_state::Actor> {
            self.actors.lock().unwrap().clone()
        }

        /// The single actor a one-write test drove — a sharper assertion than
        /// indexing, because an unexpected *second* call fails it too.
        fn only_actor(&self) -> coppice_state::Actor {
            let actors = self.actors();
            assert_eq!(actors.len(), 1, "expected exactly one mutating call");
            actors.into_iter().next().expect("one actor")
        }

        fn record(&self, actor: coppice_state::Actor) {
            self.actors.lock().unwrap().push(actor);
        }
    }

    const STUB_CLUSTER: &str = "cluster-00000000-0000-0000-0000-000000000009";

    impl ControlPlane for StubPlane {
        fn cluster_id(&self) -> coppice_core::id::ClusterId {
            STUB_CLUSTER.parse().unwrap()
        }

        fn queue_window(&self) -> QueueWindow {
            self.queue_window.clone()
        }

        fn usage_window(&self) -> UsageSnapshot {
            self.usage.clone()
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

        async fn submit_job(
            &self,
            req: SubmitJobRequest,
            actor: coppice_state::Actor,
        ) -> Result<SubmitJobResponse, ApiError> {
            self.record(actor);
            match self.fail_with {
                Some(make) => Err(make()),
                None => Ok(SubmitJobResponse {
                    job: req.job,
                    log_index: 7,
                }),
            }
        }

        async fn abort_job(
            &self,
            _req: AbortJobRequest,
            actor: coppice_state::Actor,
        ) -> Result<(), ApiError> {
            self.record(actor);
            match self.fail_with {
                Some(make) => Err(make()),
                None => Ok(()),
            }
        }

        async fn configure_quota_entity(
            &self,
            req: dto::ConfigureQuotaEntityRequest,
            actor: coppice_state::Actor,
        ) -> Result<dto::ConfigureQuotaEntityResponse, ApiError> {
            self.record(actor);
            match self.fail_with {
                Some(make) => Err(make()),
                None => Ok(dto::ConfigureQuotaEntityResponse {
                    entity: req.entity,
                    log_index: 7,
                }),
            }
        }

        /// Echoes an accepted replacement under one log index — the plane
        /// proposes a single command whether or not the request renames
        /// `groups_claim`. That the rename actually lands atomically is tested
        /// against a real state machine in `coppice-state`; here the point is
        /// that the field reaches the plane at all.
        async fn update_authorization(
            &self,
            req: dto::UpdateAuthorizationRequest,
            actor: coppice_state::Actor,
        ) -> Result<dto::UpdateAuthorizationResponse, ApiError> {
            self.record(actor);
            *self.authorization.lock().unwrap() = Some(req);
            match self.fail_with {
                Some(make) => Err(make()),
                None => Ok(dto::UpdateAuthorizationResponse { log_index: 7 }),
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
            usage: UsageSnapshot::default(),
            timeline: empty_timeline(),
            state,
            read_consistency: std::sync::Mutex::default(),
            actors: std::sync::Mutex::default(),
            authorization: std::sync::Mutex::default(),
            // No handle by default: coordinator-status tests build their own
            // plane with a seeded summary.
            coordinator: None,
        }))
    }

    /// [`app_with_state`] with an ADR 0039 usage snapshot attached, for the
    /// utilization route.
    fn app_with_usage(state: coppice_state::StateMachine, usage: UsageSnapshot) -> Router {
        router(Arc::new(StubPlane {
            fail_with: None,
            queue_window: QueueWindow::default(),
            usage,
            timeline: empty_timeline(),
            state,
            read_consistency: std::sync::Mutex::default(),
            actors: std::sync::Mutex::default(),
            authorization: std::sync::Mutex::default(),
            coordinator: None,
        }))
    }

    /// An 8-core node for the utilization tests.
    fn utilization_node(id: NodeId) -> coppice_state::NodeRecord {
        coppice_state::NodeRecord {
            node: coppice_core::node::Node {
                id,
                capacity: coppice_core::resource::Resources {
                    cpu_millis: 8_000,
                    memory: coppice_core::bytes::ByteSize::from_mib(16_384),
                    disk: coppice_core::bytes::ByteSize::ZERO,
                },
                labels: Default::default(),
                schedulable: true,
                service_addr: None,
            },
            epoch: 1,
        }
    }

    /// One 30 s usage bucket opening at `at_secs`, with `used` present or
    /// honestly absent.
    fn utilization_bucket(at_secs: i64, used_cpu: Option<u64>) -> crate::UsageBucket {
        let millis = |cpu: u64| coppice_core::resource::Resources {
            cpu_millis: cpu,
            memory: coppice_core::bytes::ByteSize::ZERO,
            disk: coppice_core::bytes::ByteSize::ZERO,
        };
        let at = |s: i64| Timestamp::UNIX_EPOCH + coppice_core::time::Duration::from_secs(s);
        crate::UsageBucket {
            start: at(at_secs),
            end: at(at_secs + 30),
            capacity: millis(8_000),
            allocated: millis(1_000),
            used: used_cpu.map(millis),
        }
    }

    /// A `job_timeline` window covering nothing (a fresh replica): floor at
    /// the ReadView's applied index 1, no events, no continuation.
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
        // No derived coverage: rates null rather than a fabricated 0.0
        // (ADR 0032).
        assert_eq!(
            body["queue"]["drain_rate_per_minute"],
            serde_json::Value::Null
        );
    }

    #[tokio::test]
    async fn overview_serves_derived_rates_and_history() {
        let plane = StubPlane {
            fail_with: None,
            usage: UsageSnapshot::default(),
            queue_window: QueueWindow {
                buckets: vec![crate::QueueBucket {
                    start: Timestamp::from_micros(60_000_000).expect("in range"),
                    end: Timestamp::from_micros(90_000_000).expect("in range"),
                    depth: 4,
                    arrivals: 2,
                    drains: 1,
                }],
            },
            timeline: empty_timeline(),
            state: coppice_state::StateMachine::default(),
            read_consistency: std::sync::Mutex::default(),
            actors: std::sync::Mutex::default(),
            authorization: std::sync::Mutex::default(),
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
    async fn node_utilization_404s_an_unknown_node() {
        // An unknown node and a known-but-uncollected one are different
        // answers (ADR 0039): the 404 comes from the read view, not from the
        // usage window being empty.
        let response = app(None)
            .oneshot(
                Request::get(format!("/api/v1/nodes/{}/utilization", NodeId::new()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn node_utilization_serves_the_buckets_the_window_holds() {
        let node = NodeId::new();
        let mut state = coppice_state::StateMachine::default();
        state.nodes.insert(node, utilization_node(node));

        // One bucket that measured usage and one that did not — the second
        // must serialize as `null`, never as a zero vector.
        let usage = crate::UsageSnapshot {
            current: Default::default(),
            history: std::sync::Arc::new(crate::ClusterUsage {
                nodes: std::collections::BTreeMap::from([(
                    node,
                    crate::UsageWindow {
                        buckets: vec![
                            utilization_bucket(0, Some(2_000)),
                            utilization_bucket(30, None),
                        ],
                    },
                )]),
                cluster: Vec::new(),
            }),
            total_nodes: 1,
        };

        let response = app_with_usage(state, usage)
            .oneshot(
                Request::get(format!("/api/v1/nodes/{node}/utilization"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["capacity"]["cpu_millis"], 8_000);
        assert_eq!(body["samples"].as_array().unwrap().len(), 2);
        assert_eq!(body["samples"][0]["used"]["cpu_millis"], 2_000);
        assert_eq!(body["samples"][0]["allocated"]["cpu_millis"], 1_000);
        assert_eq!(body["samples"][1]["used"], serde_json::Value::Null);
    }

    /// A known node the leader has collected nothing for answers 200 with an
    /// empty series — the honest "no coverage" answer a follower always gives.
    #[tokio::test]
    async fn node_utilization_of_an_uncollected_node_is_empty_not_absent() {
        let node = NodeId::new();
        let mut state = coppice_state::StateMachine::default();
        state.nodes.insert(node, utilization_node(node));

        let response = app_with_state(None, state)
            .oneshot(
                Request::get(format!("/api/v1/nodes/{node}/utilization"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["samples"], serde_json::json!([]));
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
            usage: UsageSnapshot::default(),
            timeline: empty_timeline(),
            state,
            read_consistency: std::sync::Mutex::default(),
            actors: std::sync::Mutex::default(),
            authorization: std::sync::Mutex::default(),
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
        // SSO provenance (ADR 0022) is now a present field, not an omitted
        // one. Replicated state records no auto-minted entities — nothing
        // creates one — so every entity reads `configured` with a null
        // principal, and saying so beats omitting the field: a client can
        // tell "this entity was configured by an operator" from "this server
        // does not report provenance", which an absent key cannot.
        assert_eq!(body["entities"][0]["origin"], "configured");
        assert_eq!(body["entities"][0]["principal"], serde_json::Value::Null);
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
            usage: UsageSnapshot::default(),
            timeline,
            state,
            read_consistency: std::sync::Mutex::default(),
            actors: std::sync::Mutex::default(),
            authorization: std::sync::Mutex::default(),
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
            usage: UsageSnapshot::default(),
            timeline: empty_timeline(),
            state,
            read_consistency: std::sync::Mutex::default(),
            actors: std::sync::Mutex::default(),
            authorization: std::sync::Mutex::default(),
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
                usage: UsageSnapshot::default(),
                timeline: empty_timeline(),
                state: coppice_state::StateMachine::default(),
                read_consistency: std::sync::Mutex::default(),
                actors: std::sync::Mutex::default(),
                authorization: std::sync::Mutex::default(),
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
        oidc_chain_with(
            idp,
            ca,
            coppice_authn::static_groups_claim(coppice_authn::DEFAULT_GROUPS_CLAIM),
        )
        .await
    }

    /// [`oidc_chain`] with an explicit groups-claim provider, for the tests
    /// that care that the claim *name* is read per request rather than pinned
    /// when the chain is built.
    async fn oidc_chain_with_groups_claim(
        idp: &coppice_testkit::oidc::FakeIdp,
        groups_claim: coppice_authn::GroupsClaimProvider,
    ) -> Arc<AuthnChain> {
        oidc_chain_with(idp, no_ca(), groups_claim).await
    }

    async fn oidc_chain_with(
        idp: &coppice_testkit::oidc::FakeIdp,
        ca: coppice_authn::CaProvider,
        groups_claim: coppice_authn::GroupsClaimProvider,
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
        Arc::new(AuthnChain::oidc(validator, groups_claim, ca, config))
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
        // No `name`/`email` claims on this token: explicit nulls, per the
        // wire convention for absent optionals.
        assert_eq!(body["name"], serde_json::Value::Null);
        assert_eq!(body["email"], serde_json::Value::Null);
        idp.shutdown().await;
    }

    /// The token's `name`/`email` claims reach the session response as
    /// presentation data (ADR 0022): read from the verified token on this
    /// request, reported back, and stored nowhere — nothing but this
    /// response ever carries them.
    #[tokio::test]
    async fn the_session_reports_presentation_claims_from_the_token() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let token = idp.sign(
            coppice_testkit::oidc::TokenClaims::new("user-42")
                .audience(TEST_CLIENT_ID)
                .claim("name", serde_json::json!("Ana Batch"))
                .claim("email", serde_json::json!("ana@example.com")),
        );
        let response = router_with_authn(stub_plane(Default::default()), oidc_chain(&idp).await)
            .oneshot(bearer_request("/api/v1/session", &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["principal"], "user-42");
        assert_eq!(body["name"], "Ana Batch");
        assert_eq!(body["email"], "ana@example.com");
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
        // Certificates carry no presentation claims.
        assert_eq!(body["name"], serde_json::Value::Null);
        assert_eq!(body["email"], serde_json::Value::Null);
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
        // including /session, which now also reads replicated bindings to
        // build its authority summary rather than the request alone.
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

    // ---- The authentication boundary is the whole namespace ---------------

    /// An unrouted `/api/v1` path is 401, not 404, to an anonymous caller.
    ///
    /// The regression this pins: while the layer sat on a protected
    /// *sub-router*, a path matching no route left the nested tree entirely
    /// and was answered by the outer UI fallback — outside the layer — so an
    /// uncredentialed probe got a 404 and could map the namespace by
    /// elimination. The `/api/v1` router now owns its own fallback and the
    /// layer wraps it too.
    #[tokio::test]
    async fn an_unrouted_api_path_is_unauthenticated_before_it_is_not_found() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let response = router_with_authn(stub_plane(Default::default()), oidc_chain(&idp).await)
            .oneshot(
                Request::get("/api/v1/not-a-route")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(response).await["code"], "UNAUTHENTICATED");
        idp.shutdown().await;
    }

    /// The wrong method on either credential-free path is authenticated like
    /// anything else: the exemption table is keyed on `(method, path)`, so
    /// `POST /auth/config` and `GET /enroll` are simply not those endpoints.
    ///
    /// Both used to answer 405 uncredentialed — the method-miss was decided by
    /// the public sub-router, which the layer did not cover.
    #[tokio::test]
    async fn a_wrong_method_on_a_credential_free_path_is_still_authenticated() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        for request in [
            post_json("/api/v1/auth/config", "{}"),
            Request::get("/api/v1/enroll").body(Body::empty()).unwrap(),
        ] {
            let uri = request.uri().to_string();
            let response = router_with_authn(stub_plane(Default::default()), Arc::clone(&chain))
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
            assert_eq!(
                body_json(response).await["code"],
                "UNAUTHENTICATED",
                "{uri}"
            );
        }
        idp.shutdown().await;
    }

    /// The exemption table is matched against the **prefix-stripped** path.
    ///
    /// The layer lives inside the nested router, and axum strips the nest's
    /// prefix from the request URI before the nested service — middleware
    /// included — runs, so the table is written as `/auth/config`, not
    /// `/api/v1/auth/config`. This test is the empirical proof of that: both
    /// exempt pairs are reachable with no credential in the OIDC posture,
    /// which is only true if the stripped spelling is the one that matches.
    #[tokio::test]
    async fn unauthenticated_routes_are_matched_on_the_prefix_stripped_path() {
        assert_eq!(
            crate::http::authn::UNAUTHENTICATED_ROUTES
                .iter()
                .map(|(m, p)| (m.as_str(), *p))
                .collect::<Vec<_>>(),
            vec![("GET", "/auth/config"), ("POST", "/enroll")],
        );

        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;

        // `GET /auth/config`: the discovery document, served to a caller with
        // no credential at all.
        let response = router_with_authn(stub_plane(Default::default()), Arc::clone(&chain))
            .oneshot(
                Request::get("/api/v1/auth/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["mode"], "oidc");

        // `POST /enroll`: the endpoint's own uniform refusal, not the API's
        // 401 — the same assertion `enrollment_is_not_touched_by_the_authn_layer`
        // makes, restated here as the second half of the pair.
        let response = router_with_authn(stub_plane(Default::default()), chain)
            .oneshot(enroll_request(None, CSR_BODY))
            .await
            .unwrap();
        let (status, body) = refusal_parts(response).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body, crate::http::REFUSED_BODY.as_bytes());

        idp.shutdown().await;
    }

    /// Once authenticated, the ordinary HTTP contract is restored: an unrouted
    /// `/api/v1` path is the JSON 404, and a method-miss on a real path is a
    /// 405. Authentication moves *when* a caller learns those answers, never
    /// what they are.
    #[tokio::test]
    async fn an_authenticated_caller_still_gets_404_and_405() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        let token =
            idp.sign(coppice_testkit::oidc::TokenClaims::new("user-42").audience(TEST_CLIENT_ID));

        let response = router_with_authn(stub_plane(Default::default()), Arc::clone(&chain))
            .oneshot(bearer_request("/api/v1/not-a-route", &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["code"], "NOT_FOUND");

        let mut request = post_json("/api/v1/auth/config", "{}");
        request.headers_mut().insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        let response = router_with_authn(stub_plane(Default::default()), chain)
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        idp.shutdown().await;
    }

    /// The nested router's fallback claims `/api/v1` misses and nothing else:
    /// other `/api/*` paths still reach the outer fallback's JSON 404, and
    /// non-`/api` paths still reach the UI.
    #[tokio::test]
    async fn the_api_v1_fallback_does_not_annex_the_outer_one() {
        let response = app(None)
            .oneshot(Request::get("/api/v2/nodes").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["code"], "NOT_FOUND");

        // And a bare `/api/…` path, which is neither.
        let response = app(None)
            .oneshot(Request::get("/api/whatever").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["code"], "NOT_FOUND");
    }

    /// The groups-claim name is read from the provider on **every request**,
    /// not captured when the chain is built.
    ///
    /// That is what makes `PolicyConfig.groups_claim` (ADR 0022) a live knob:
    /// an operator who changes it in replicated policy changes what the next
    /// request's groups are read from, with no coordinator restart. Here the
    /// provider is a shared cell rather than the replicated view — the
    /// coordinator's own test covers the view-backed one — and the token
    /// carries *both* candidate claims, so which one comes back on `/session`
    /// is unambiguous proof of which name was used.
    #[tokio::test]
    async fn the_groups_claim_name_is_read_per_request() {
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let claim = Arc::new(std::sync::RwLock::new("roles".to_string()));
        let chain = {
            let claim = Arc::clone(&claim);
            oidc_chain_with_groups_claim(&idp, Arc::new(move || claim.read().unwrap().clone()))
                .await
        };
        let token = idp.sign(
            coppice_testkit::oidc::TokenClaims::new("user-42")
                .audience(TEST_CLIENT_ID)
                .claim("roles", serde_json::json!(["a", "b"]))
                .claim("groups", serde_json::json!(["x"])),
        );

        // The non-default name wins while policy names it...
        let response = router_with_authn(stub_plane(Default::default()), Arc::clone(&chain))
            .oneshot(bearer_request("/api/v1/session", &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_json(response).await["groups"],
            serde_json::json!(["a", "b"])
        );

        // ...and the very next request through the same chain follows the
        // change, with the same token.
        *claim.write().unwrap() = "groups".to_string();
        let response = router_with_authn(stub_plane(Default::default()), chain)
            .oneshot(bearer_request("/api/v1/session", &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_json(response).await["groups"],
            serde_json::json!(["x"])
        );

        idp.shutdown().await;
    }

    /// Open mode authenticates *everything*, so an `/api/v1` miss there is the
    /// plain 404 it always was — the boundary changed who is refused, not what
    /// an authenticated deployment answers.
    #[tokio::test]
    async fn open_mode_still_answers_api_v1_misses_with_the_json_404() {
        let response = app(None)
            .oneshot(
                Request::get("/api/v1/not-a-route")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["code"], "NOT_FOUND");
    }

    // ---- Authorization (ADR 0023) ----------------------------------------
    //
    // The pre-log check, end to end through the router: a fake-IdP token
    // carrying a real principal and real groups, evaluated against real
    // replicated bindings by the same `authz::evaluate` apply uses.
    //
    // Every case asserts the *pair* (status, actor-that-reached-the-plane). A
    // 200 proves only that the request was not refused; that the identity the
    // token proved is the one that rides the command — groups and
    // implicit-admin flags included — is the other half, and no status
    // assertion can see it.

    use coppice_state::authz::{Binding, Role, Subject};

    /// The quota tree every case is scoped against:
    /// `org` → { `team-a` → `squad`, `team-b` }.
    ///
    /// Four entities covering every containment relationship the subtree rule
    /// has — a parent, a sibling outside the subtree, and a grandchild inside
    /// it — the same shape `authz`'s own unit tests use.
    struct Tree {
        org: QuotaEntityId,
        team_a: QuotaEntityId,
        team_b: QuotaEntityId,
        squad: QuotaEntityId,
        /// A queued job charging `team_a`, submitted by `"owner"`.
        job: JobId,
    }

    /// Build the tree, seed one owned job, and install the bindings `bind`
    /// derives from the (freshly minted) entity ids.
    ///
    /// The closure exists because scopes are ids: a caller cannot name
    /// `team-a` in a binding until the fixture has minted it.
    fn authz_fixture(
        bind: impl FnOnce(&Tree) -> Vec<Binding>,
    ) -> (coppice_state::StateMachine, Tree) {
        let tree = Tree {
            org: QuotaEntityId::new(),
            team_a: QuotaEntityId::new(),
            team_b: QuotaEntityId::new(),
            squad: QuotaEntityId::new(),
            job: JobId::new(),
        };
        let at = Timestamp::from_micros(1_760_000_000_000_000).expect("in range");
        let entity = |parent: Option<QuotaEntityId>, name: &str| coppice_state::QuotaEntity {
            parent,
            name: name.to_string(),
            quota: coppice_core::quota::CostUnits(1_000_000),
            usage: coppice_core::quota::UsageState::new(at),
            created_at: at,
            updated_at: at,
        };

        let mut state = coppice_state::StateMachine::default();
        state.quota_entities.insert(tree.org, entity(None, "org"));
        state
            .quota_entities
            .insert(tree.team_a, entity(Some(tree.org), "team-a"));
        state
            .quota_entities
            .insert(tree.team_b, entity(Some(tree.org), "team-b"));
        state
            .quota_entities
            .insert(tree.squad, entity(Some(tree.team_a), "squad"));

        let mut record = queued_job(tree.job);
        record.spec.quota_entity = tree.team_a;
        record.spec.submitted_by = Some("owner".to_string());
        state.jobs.insert(tree.job, record);

        state.bindings = bind(&tree);
        (state, tree)
    }

    fn group_binding(group: &str, role: Role, scope: Option<QuotaEntityId>) -> Binding {
        Binding {
            subject: Subject::Group(group.to_string()),
            role,
            scope,
        }
    }

    fn principal_binding(sub: &str, role: Role, scope: Option<QuotaEntityId>) -> Binding {
        Binding {
            subject: Subject::Principal(sub.to_string()),
            role,
            scope,
        }
    }

    fn submit_body(entity: QuotaEntityId) -> String {
        format!(
            r#"{{
                "image": "busybox",
                "command": ["run"],
                "requests": {{ "cpu_millis": 1000, "memory_bytes": 0, "disk_bytes": 0 }},
                "job": "{}",
                "quota_entity": "{entity}"
            }}"#,
            JobId::new()
        )
    }

    fn configure_body(entity: QuotaEntityId, parent: Option<QuotaEntityId>) -> String {
        let parent = match parent {
            Some(p) => format!(r#""{p}""#),
            None => "null".to_string(),
        };
        format!(r#"{{ "entity": "{entity}", "parent": {parent}, "name": "n", "quota_ucu": 1000 }}"#)
    }

    fn put_json(uri: &str, body: &str) -> Request<Body> {
        Request::put(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    /// `request`, credentialed as `principal` carrying `groups`.
    fn as_principal(
        idp: &coppice_testkit::oidc::FakeIdp,
        mut request: Request<Body>,
        principal: &str,
        groups: &[&str],
    ) -> Request<Body> {
        let token = idp.sign(
            coppice_testkit::oidc::TokenClaims::new(principal)
                .audience(TEST_CLIENT_ID)
                .claim("groups", serde_json::json!(groups)),
        );
        request.headers_mut().insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        request
    }

    /// One authorization case: status, body, and the plane it was (or was
    /// not) proposed to.
    async fn authz_case(
        idp: &coppice_testkit::oidc::FakeIdp,
        chain: Arc<AuthnChain>,
        state: coppice_state::StateMachine,
        principal: &str,
        groups: &[&str],
        request: Request<Body>,
    ) -> (StatusCode, serde_json::Value, Arc<StubPlane>) {
        let plane = stub_plane(state);
        let response = router_with_authn(Arc::clone(&plane), chain)
            .oneshot(as_principal(idp, request, principal, groups))
            .await
            .unwrap();
        let status = response.status();
        (status, body_json(response).await, plane)
    }

    /// A refusal is a 403 with the documented code, and — the assertion that
    /// matters most — nothing reached the control plane, so nothing was
    /// proposed and no log entry exists for a request that never applied.
    fn assert_denied(status: StatusCode, body: &serde_json::Value, plane: &StubPlane) {
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["code"], "PERMISSION_DENIED");
        assert!(
            plane.actors().is_empty(),
            "a refused request must not reach the control plane"
        );
    }

    /// The actor a 200 handed the plane, asserted to be exactly the token's.
    fn accepted_actor(plane: &StubPlane, principal: &str, groups: &[&str]) -> coppice_state::Actor {
        let actor = plane.only_actor();
        assert_eq!(actor.principal, principal);
        assert_eq!(
            actor.groups,
            groups.iter().map(|g| g.to_string()).collect::<Vec<_>>()
        );
        // Neither implicit-admin flag may be invented by the edge for an
        // ordinary bearer: they are what would make the actor unscoped admin.
        assert!(!actor.operator_cert, "a bearer is not an operator cert");
        assert!(!actor.auth_disabled, "the OIDC posture is not open mode");
        actor
    }

    #[tokio::test]
    async fn a_principal_with_no_bindings_is_refused_every_mutating_verb() {
        // Deny by default, which is the whole model: an authenticated caller
        // with no binding may do nothing at all — and each refusal is a 403,
        // not a 401, because they *are* who they say they are.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        let (state, tree) = authz_fixture(|_| Vec::new());

        for request in [
            post_json("/api/v1/jobs", &submit_body(tree.team_a)),
            post_json(&format!("/api/v1/jobs/{}/abort", tree.job), "{}"),
            post_json(
                "/api/v1/quota-entities",
                &configure_body(tree.team_a, Some(tree.org)),
            ),
            put_json("/api/v1/authorization", r#"{ "bindings": [] }"#),
        ] {
            let uri = request.uri().to_string();
            let (status, body, plane) = authz_case(
                &idp,
                Arc::clone(&chain),
                state.clone(),
                "nobody",
                &[],
                request,
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {body}");
            assert_eq!(body["code"], "PERMISSION_DENIED", "{uri}");
            assert!(plane.actors().is_empty(), "{uri} must not be proposed");
        }
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn a_scoped_submitter_submits_in_scope_and_nowhere_else() {
        // The subtree rule end to end. `squad` is a descendant of the scope
        // and `team-b` a sibling of it — the pair that decides whether "scope"
        // means a subtree or a single entity — and `org` is the ancestor,
        // which a scoped grant must never reach upward to.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        let (state, tree) = authz_fixture(|t| {
            vec![group_binding(
                "batch-users",
                Role::Submitter,
                Some(t.team_a),
            )]
        });

        for (entity, label, expected) in [
            (tree.team_a, "the scope itself", StatusCode::OK),
            (tree.squad, "a descendant", StatusCode::OK),
            (tree.team_b, "a sibling", StatusCode::FORBIDDEN),
            (tree.org, "the ancestor", StatusCode::FORBIDDEN),
        ] {
            let (status, body, plane) = authz_case(
                &idp,
                Arc::clone(&chain),
                state.clone(),
                "user-42",
                &["batch-users"],
                post_json("/api/v1/jobs", &submit_body(entity)),
            )
            .await;
            assert_eq!(status, expected, "{label}: {body}");
            if expected == StatusCode::OK {
                accepted_actor(&plane, "user-42", &["batch-users"]);
            } else {
                assert_denied(status, &body, &plane);
            }
        }
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn a_principal_subject_binding_grants_exactly_like_a_group_one() {
        // Subjects are two spellings of the same thing — an exact string
        // against `sub` or against a groups-claim entry — and neither is
        // privileged over the other.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        let (state, tree) =
            authz_fixture(|t| vec![principal_binding("svc-ci", Role::Submitter, Some(t.team_a))]);

        // The named principal, carrying no groups at all.
        let (status, body, plane) = authz_case(
            &idp,
            Arc::clone(&chain),
            state.clone(),
            "svc-ci",
            &[],
            post_json("/api/v1/jobs", &submit_body(tree.team_a)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        accepted_actor(&plane, "svc-ci", &[]);

        // Somebody whose *group* happens to be named `svc-ci` is not that
        // principal: subject kind is part of the match, not decoration.
        let (status, body, plane) = authz_case(
            &idp,
            chain,
            state,
            "impostor",
            &["svc-ci"],
            post_json("/api/v1/jobs", &submit_body(tree.team_a)),
        )
        .await;
        assert_denied(status, &body, &plane);
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn an_operator_aborts_anyones_job_and_a_submitter_does_not() {
        // Abort composes upward from operator, and submitter is strictly
        // below it: the same scope, the same job, two different answers.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        let (state, tree) = authz_fixture(|t| {
            vec![
                group_binding("sre", Role::Operator, Some(t.team_a)),
                group_binding("batch-users", Role::Submitter, Some(t.team_a)),
            ]
        });
        let abort = || post_json(&format!("/api/v1/jobs/{}/abort", tree.job), "{}");

        let (status, body, plane) = authz_case(
            &idp,
            Arc::clone(&chain),
            state.clone(),
            "on-call",
            &["sre"],
            abort(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        accepted_actor(&plane, "on-call", &["sre"]);

        let (status, body, plane) =
            authz_case(&idp, chain, state, "user-42", &["batch-users"], abort()).await;
        assert_denied(status, &body, &plane);
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn a_submitter_aborts_their_own_job_with_no_binding_at_all() {
        // ADR 0023's one implicit grant besides the operator cert: the
        // principal in the job's `submitted_by` may always abort it. The
        // fixture gives `owner` no binding whatsoever, so a 200 here can only
        // have come from ownership — and it is the *pre-log* check resolving
        // the job's owner out of the read view that produces it.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        let (state, tree) = authz_fixture(|_| Vec::new());

        let (status, body, plane) = authz_case(
            &idp,
            Arc::clone(&chain),
            state.clone(),
            "owner",
            &[],
            post_json(&format!("/api/v1/jobs/{}/abort", tree.job), "{}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        accepted_actor(&plane, "owner", &[]);

        // And ownership is not transitive to anyone else with no binding.
        let (status, body, plane) = authz_case(
            &idp,
            chain,
            state,
            "someone-else",
            &[],
            post_json(&format!("/api/v1/jobs/{}/abort", tree.job), "{}"),
        )
        .await;
        assert_denied(status, &body, &plane);
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn aborting_a_job_the_view_has_never_seen_is_left_to_apply() {
        // The pre-check declines to guess. This replica's view may simply be
        // behind, and both guesses are wrong in a visible way: a 403 would
        // refuse a caller who may own the job, and a 404 would answer a
        // different question *and* be an existence oracle for job ids. So the
        // request goes through, carrying its actor, and apply decides against
        // the state at its own log position.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        let (state, _tree) = authz_fixture(|_| Vec::new());

        let (status, body, plane) = authz_case(
            &idp,
            chain,
            state,
            "nobody",
            &[],
            post_json(&format!("/api/v1/jobs/{}/abort", JobId::new()), "{}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        accepted_actor(&plane, "nobody", &[]);
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn a_scoped_admin_configures_in_subtree_but_cannot_move_an_entity_out_of_it() {
        // Reparenting moves authority (ADR 0023), so a move must stay inside
        // one binding's subtree. Reconfiguring `squad` where it already sits
        // is fine; moving it under `team-b` is a cross-subtree move, and
        // moving it to the root is inside no subtree at all — both take
        // unscoped admin, which this actor does not have.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        let (state, tree) =
            authz_fixture(|t| vec![group_binding("platform", Role::Admin, Some(t.team_a))]);

        for (parent, label, expected) in [
            (Some(tree.team_a), "in place", StatusCode::OK),
            (Some(tree.team_b), "cross-subtree", StatusCode::FORBIDDEN),
            (None, "to the root", StatusCode::FORBIDDEN),
        ] {
            let (status, body, plane) = authz_case(
                &idp,
                Arc::clone(&chain),
                state.clone(),
                "admin-a",
                &["platform"],
                post_json(
                    "/api/v1/quota-entities",
                    &configure_body(tree.squad, parent),
                ),
            )
            .await;
            assert_eq!(status, expected, "{label}: {body}");
            if expected == StatusCode::OK {
                accepted_actor(&plane, "admin-a", &["platform"]);
            } else {
                assert_denied(status, &body, &plane);
            }
        }
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn the_authorization_write_takes_an_unscoped_admin_and_a_scoped_one_will_not_do() {
        // The cluster verbs are the one place the scope's *absence* is the
        // grant. A subtree admin reshapes their subtree; they do not rewrite
        // the list that says who administers what.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;

        let (scoped, _) =
            authz_fixture(|t| vec![group_binding("platform", Role::Admin, Some(t.team_a))]);
        let (status, body, plane) = authz_case(
            &idp,
            Arc::clone(&chain),
            scoped,
            "admin-a",
            &["platform"],
            put_json("/api/v1/authorization", r#"{ "bindings": [] }"#),
        )
        .await;
        assert_denied(status, &body, &plane);

        let (unscoped, _) = authz_fixture(|_| vec![group_binding("platform", Role::Admin, None)]);
        let (status, body, plane) = authz_case(
            &idp,
            chain,
            unscoped,
            "admin-a",
            &["platform"],
            put_json("/api/v1/authorization", r#"{ "bindings": [] }"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        accepted_actor(&plane, "admin-a", &["platform"]);
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn an_operator_certificate_passes_every_mutating_verb_with_no_bindings() {
        // Break-glass (ADR 0022): an implicit unscoped admin from *outside*
        // the bindings list, against a cluster whose list is empty — which is
        // exactly the lockout it exists to recover from. The flag reaches the
        // plane, because apply re-derives the same implicit grant from it.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let (ca_pem, leaf_der) = operator_credential("alice");
        let chain = oidc_chain_with_ca(&idp, Arc::new(move || Some(ca_pem.clone()))).await;
        let (state, tree) = authz_fixture(|_| Vec::new());

        for request in [
            post_json("/api/v1/jobs", &submit_body(tree.team_b)),
            post_json(&format!("/api/v1/jobs/{}/abort", tree.job), "{}"),
            post_json("/api/v1/quota-entities", &configure_body(tree.squad, None)),
            put_json("/api/v1/authorization", r#"{ "bindings": [] }"#),
        ] {
            let uri = request.uri().to_string();
            let plane = stub_plane(state.clone());
            let response = router_with_authn(Arc::clone(&plane), Arc::clone(&chain))
                .oneshot(with_peer_cert(request, leaf_der.clone()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let actor = plane.only_actor();
            assert_eq!(actor.principal, "cert:alice", "{uri}");
            assert!(actor.operator_cert, "{uri}");
            assert!(actor.is_implicit_admin(), "{uri}");
        }
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn open_mode_passes_every_mutating_verb_with_no_bindings() {
        // The formally-supported open posture: one static anonymous actor
        // with implicit unscoped admin. `auth_disabled` rides *in* the actor
        // rather than being read from node config, which is what lets apply
        // reach the same verdict without consulting anything unreplicated.
        let (state, tree) = authz_fixture(|_| Vec::new());

        for request in [
            post_json("/api/v1/jobs", &submit_body(tree.team_b)),
            post_json(&format!("/api/v1/jobs/{}/abort", tree.job), "{}"),
            post_json("/api/v1/quota-entities", &configure_body(tree.squad, None)),
            put_json("/api/v1/authorization", r#"{ "bindings": [] }"#),
        ] {
            let uri = request.uri().to_string();
            let plane = stub_plane(state.clone());
            let response = router(Arc::clone(&plane)).oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let actor = plane.only_actor();
            assert_eq!(actor.principal, "anonymous", "{uri}");
            assert!(actor.auth_disabled, "{uri}");
            assert!(actor.is_implicit_admin(), "{uri}");
        }
    }

    #[tokio::test]
    async fn an_apply_time_permission_denial_is_a_403_and_never_a_500() {
        // The revocation race, which is the reason apply re-checks at all: the
        // pre-check passed against this replica's view, and by the command's
        // log position the binding was gone. Apply refuses deterministically,
        // and that refusal must reach the client as the same 403 the
        // pre-check would have given — not as the 409 every other rejection
        // is, and above all not as an INTERNAL.
        let plane = Arc::new(StubPlane {
            fail_with: Some(|| {
                ApiError::Rejected(coppice_state::RejectionReason::PermissionDenied(
                    "principal \"user-42\" may not submit a job charging quota entity q".into(),
                ))
            }),
            queue_window: QueueWindow::default(),
            usage: UsageSnapshot::default(),
            timeline: empty_timeline(),
            state: coppice_state::StateMachine::default(),
            read_consistency: std::sync::Mutex::default(),
            actors: std::sync::Mutex::default(),
            authorization: std::sync::Mutex::default(),
            coordinator: None,
        });
        let response = router(plane)
            .oneshot(post_json(
                "/api/v1/jobs",
                &submit_body(QuotaEntityId::new()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = body_json(response).await;
        assert_eq!(body["code"], "PERMISSION_DENIED");
        // The apply's own rendered denial, verbatim — the same sentence the
        // pre-log check produces, so a client cannot tell which one refused.
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("may not submit a job"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn a_forwarded_permission_denial_is_a_403_too() {
        // The same rejection, relayed across the ADR 0038 hop. The rendered
        // text is all that used to cross; the classification crosses with it
        // now, precisely so this status survives a forward.
        let plane = Arc::new(StubPlane {
            fail_with: Some(|| ApiError::ForwardedRejection {
                kind: crate::RejectionKind::PermissionDenied,
                reason: "permission denied: principal \"user-42\" may not …".into(),
            }),
            queue_window: QueueWindow::default(),
            usage: UsageSnapshot::default(),
            timeline: empty_timeline(),
            state: coppice_state::StateMachine::default(),
            read_consistency: std::sync::Mutex::default(),
            actors: std::sync::Mutex::default(),
            authorization: std::sync::Mutex::default(),
            coordinator: None,
        });
        let response = router(plane)
            .oneshot(post_json(
                "/api/v1/jobs",
                &submit_body(QuotaEntityId::new()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(body_json(response).await["code"], "PERMISSION_DENIED");
    }

    /// A rejection with no special classification is still the 409 it always
    /// was — the new mapping added one status, it did not reclassify the rest.
    #[tokio::test]
    async fn an_ordinary_rejection_is_still_a_409() {
        let response = app(Some(|| {
            ApiError::Rejected(coppice_state::RejectionReason::JobTerminal(JobId::new()))
        }))
        .oneshot(post_json(
            "/api/v1/jobs",
            &submit_body(QuotaEntityId::new()),
        ))
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(response).await["code"], "REJECTED");
    }

    // ---- GET/PUT /api/v1/authorization -----------------------------------

    #[tokio::test]
    async fn the_authorization_document_round_trips_through_get_and_put() {
        // What `GET` writes is what `PUT` accepts, field for field — the
        // property `coppice-cli policy authz get | set` depends on. Both
        // subject kinds and both scope states, because those are the four
        // shapes the flat wire form has to keep distinct.
        let scope = QuotaEntityId::new();
        let (mut state, _) = authz_fixture(|_| Vec::new());
        state.policy.groups_claim = "entitlements".to_string();
        state.bindings = vec![
            group_binding("platform", Role::Admin, None),
            principal_binding("svc-ci", Role::Submitter, Some(scope)),
        ];
        let plane = stub_plane(state);

        let response = router(Arc::clone(&plane))
            .oneshot(
                Request::get("/api/v1/authorization")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // A configuration read (ADR 0007), like the quota-entity detail:
        // the document an operator is about to edit and PUT back.
        assert_eq!(
            plane.read_consistency.lock().unwrap().last().copied(),
            Some(Consistency::Strong)
        );
        let document = body_json(response).await;
        assert_eq!(document["groups_claim"], "entitlements");
        assert_eq!(
            document["bindings"],
            serde_json::json!([
                { "group": "platform", "role": "admin" },
                { "principal": "svc-ci", "role": "submitter", "scope": scope.to_string() },
            ])
        );

        // The very same document, PUT straight back.
        let response = router(Arc::clone(&plane))
            .oneshot(put_json(
                "/api/v1/authorization",
                &serde_json::to_string(&document).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let echoed = plane
            .authorization
            .lock()
            .unwrap()
            .clone()
            .expect("the plane saw the replacement");
        assert_eq!(echoed.groups_claim.as_deref(), Some("entitlements"));
        assert_eq!(
            echoed.bindings.iter().map(|b| b.role).collect::<Vec<_>>(),
            vec![dto::BindingRole::Admin, dto::BindingRole::Submitter]
        );
        assert_eq!(echoed.bindings[0].group.as_deref(), Some("platform"));
        assert_eq!(echoed.bindings[1].principal.as_deref(), Some("svc-ci"));
        assert_eq!(echoed.bindings[1].scope, Some(scope));
    }

    #[tokio::test]
    async fn a_groups_claim_change_reports_one_log_index() {
        // One command, one index (ADR 0023): `groups_claim` rides the
        // `UpdateAuthorization` that replaces the bindings, so a rename and a
        // binding swap share a log position and the response has a single
        // index to report — there is no second one, present or null.
        let (state, _) = authz_fixture(|_| Vec::new());
        let plane = stub_plane(state);
        let response = router(Arc::clone(&plane))
            .oneshot(put_json(
                "/api/v1/authorization",
                r#"{ "groups_claim": "roles", "bindings": [] }"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["log_index"], 7);
        assert!(body.get("policy_log_index").is_none());
        assert_eq!(
            plane
                .authorization
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|r| r.groups_claim.clone())
                .as_deref(),
            Some("roles"),
            "the rename reaches the plane, which carries it on the command"
        );

        // A request that does not mention the claim leaves it alone, and the
        // response shape does not change.
        let response = router(Arc::clone(&plane))
            .oneshot(put_json("/api/v1/authorization", r#"{ "bindings": [] }"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["log_index"], 7);
        assert!(body.get("policy_log_index").is_none());
        assert_eq!(
            plane
                .authorization
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|r| r.groups_claim.clone()),
            None
        );
    }

    #[tokio::test]
    async fn a_binding_with_both_or_neither_subject_is_invalid_argument() {
        // serde cannot express "exactly one of these two", so the conversion
        // does — and a violation is a malformed request the client must fix,
        // caught before a consensus round trip, never a rejection.
        for (label, bindings) in [
            (
                "both",
                r#"[{ "group": "g", "principal": "p", "role": "admin" }]"#,
            ),
            ("neither", r#"[{ "role": "admin" }]"#),
        ] {
            let plane = stub_plane(coppice_state::StateMachine::default());
            let response = router(Arc::clone(&plane))
                .oneshot(put_json(
                    "/api/v1/authorization",
                    &format!(r#"{{ "bindings": {bindings} }}"#),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{label}");
            let body = body_json(response).await;
            assert_eq!(body["code"], "INVALID_ARGUMENT", "{label}");
            // The index is in the message: a long list needs to say *which*.
            assert!(
                body["message"].as_str().unwrap().contains("binding 0"),
                "{label}: {body}"
            );
            assert!(
                plane.actors().is_empty(),
                "{label}: a malformed body must not be proposed"
            );
        }
    }

    /// Each apply-time authorization rejection is a distinguishable 400 on
    /// this endpoint — and only on this endpoint.
    ///
    /// They are not races the client lost; they are documents the client got
    /// wrong, and no retry of the identical body will ever land. The detail
    /// strings differ so an operator editing a bindings TOML learns which of
    /// the three they hit.
    #[tokio::test]
    async fn each_authorization_rejection_is_its_own_400() {
        /// One canned apply rejection and the phrase the 400 must carry.
        type Case = (fn() -> ApiError, &'static str);

        let cases: [Case; 3] = [
            (
                || {
                    ApiError::Rejected(coppice_state::RejectionReason::UnknownQuotaEntity(
                        QuotaEntityId::new(),
                    ))
                },
                "quota entity that does not exist",
            ),
            (
                || {
                    ApiError::Rejected(coppice_state::RejectionReason::InvalidAuthorization(
                        "binding 0 names an empty group subject".into(),
                    ))
                },
                "malformed",
            ),
            (
                || ApiError::Rejected(coppice_state::RejectionReason::AuthorizationLockout),
                "lock the cluster out",
            ),
        ];

        let mut messages = Vec::new();
        for (make, expected_fragment) in cases {
            let response = app(Some(make))
                .oneshot(put_json("/api/v1/authorization", r#"{ "bindings": [] }"#))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = body_json(response).await;
            assert_eq!(body["code"], "INVALID_ARGUMENT");
            let message = body["message"].as_str().unwrap().to_string();
            assert!(message.contains(expected_fragment), "{message}");
            messages.push(message);
        }
        // Distinguishable, not merely all-400: three different documents,
        // three different things to fix.
        messages.sort();
        messages.dedup();
        assert_eq!(messages.len(), 3, "each rejection must read differently");
    }

    #[tokio::test]
    async fn the_same_rejection_carried_across_the_forwarding_hop_is_the_same_400() {
        // The classification is what makes this work: the leader refused, the
        // follower answering the client never had a `RejectionReason`, and
        // matching on the rendered English would be a contract held together
        // by a string.
        let response = app(Some(|| ApiError::ForwardedRejection {
            kind: crate::RejectionKind::AuthorizationLockout,
            reason: "authorization would retain no unscoped admin binding".into(),
        }))
        .oneshot(put_json("/api/v1/authorization", r#"{ "bindings": [] }"#))
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = body_json(response).await;
        assert_eq!(body["code"], "INVALID_ARGUMENT");
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("lock the cluster out"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn an_unknown_quota_entity_is_still_a_409_on_submit_and_configure() {
        // The endpoint-local half of the mapping, stated as the thing it must
        // not break: `UnknownQuotaEntity` is a documented 409 everywhere else,
        // because there it really is a race with whoever deleted the entity.
        let make = || {
            ApiError::Rejected(coppice_state::RejectionReason::UnknownQuotaEntity(
                QuotaEntityId::new(),
            ))
        };
        for request in [
            post_json("/api/v1/jobs", &submit_body(QuotaEntityId::new())),
            post_json(
                "/api/v1/quota-entities",
                &configure_body(QuotaEntityId::new(), None),
            ),
        ] {
            let uri = request.uri().to_string();
            let response = app(Some(make)).oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CONFLICT, "{uri}");
            assert_eq!(body_json(response).await["code"], "REJECTED", "{uri}");
        }
    }

    #[tokio::test]
    async fn the_authorization_read_is_open_to_any_authenticated_principal() {
        // Reads are authn-only in v1 (ADR 0031): no role check. Hiding the
        // bindings list would mostly stop an operator from working out why
        // their own grant does not apply — everyone in it already knows who
        // they are.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        let (state, _) = authz_fixture(|_| vec![group_binding("platform", Role::Admin, None)]);

        let (status, body, _) = authz_case(
            &idp,
            chain,
            state,
            "nobody",
            &[],
            Request::get("/api/v1/authorization")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["bindings"][0]["group"], "platform");
        idp.shutdown().await;
    }

    // ---- Session authority summary (ADR 0023) ----------------------------

    #[tokio::test]
    async fn the_session_summarizes_every_binding_matching_the_actor() {
        // Faithfully, not collapsed: one entry per matching binding, scoped
        // and unscoped, group- and principal-matched, in the replicated
        // list's own order. A server that answered with a single "effective
        // role" would be publishing a second model of authority beside
        // `evaluate`'s, which is how the two come to disagree.
        let idp = coppice_testkit::oidc::FakeIdp::start().await;
        let chain = oidc_chain(&idp).await;
        let (state, tree) = authz_fixture(|t| {
            vec![
                // Matches by group, scoped.
                group_binding("batch-users", Role::Submitter, Some(t.team_a)),
                // Matches nobody in this test — a binding for someone else
                // must not leak into the summary.
                group_binding("sre", Role::Operator, None),
                // Matches by principal, unscoped.
                principal_binding("user-42", Role::Operator, None),
                // Matches by group again, deeper in the tree: two matching
                // bindings for one actor is the ordinary case, not an error.
                group_binding("batch-users", Role::Admin, Some(t.squad)),
            ]
        });

        let (status, body, _) = authz_case(
            &idp,
            chain,
            state,
            "user-42",
            &["batch-users"],
            Request::get("/api/v1/session").body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["principal"], "user-42");
        assert_eq!(body["auth_method"], "bearer");
        assert_eq!(
            body["bindings"],
            serde_json::json!([
                { "role": "submitter", "scope": tree.team_a.to_string() },
                { "role": "operator", "scope": null },
                { "role": "admin", "scope": tree.squad.to_string() },
            ])
        );
        // A bearer holds no authority outside the list, however much of it
        // the list gives them.
        assert_eq!(body["implicit_admin"], false);
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn an_implicit_admin_session_says_so_and_lists_no_bindings() {
        // Operator certificates and open mode are unscoped admin from outside
        // the list, so they can never appear *in* it: the flag is the only
        // place that authority is visible, which is exactly why the field
        // exists.
        let (state, _) = authz_fixture(|_| vec![group_binding("platform", Role::Admin, None)]);
        let response = router(stub_plane(state))
            .oneshot(Request::get("/api/v1/session").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["principal"], "anonymous");
        assert_eq!(body["implicit_admin"], true);
        assert_eq!(body["bindings"], serde_json::json!([]));
    }
}
