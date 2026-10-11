//! The API catalogue: one plain-data description of every HTTP operation the
//! client listener serves.
//!
//! The router is built from this table (`http::routes` looks each operation
//! up by id, so a route cannot exist without its documentation), the JSON
//! Schemas come from the DTOs' `schemars::JsonSchema` derives, and
//! [`render_openapi`] turns it into an OpenAPI 3.1 document. This table, not
//! the route map in ADR 0031, is the source of truth for what the API serves.
//!
//! Paths are OpenAPI style (`/jobs/{job}`) and relative to [`BASE_PATH`],
//! except the `operational` group's, which are absolute (`/healthz`).

use std::borrow::Cow;
use std::sync::OnceLock;

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde_json::{json, Map, Value};

use crate::http::dto;
use crate::http::ErrorCode;

/// Where every non-operational path is mounted.
pub const BASE_PATH: &str = "/api/v1";

/// The group whose paths are absolute rather than under [`BASE_PATH`].
pub const OPERATIONAL: &str = "operational";

/// The HTTP methods the API uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
        }
    }

    pub fn to_http(self) -> axum::http::Method {
        match self {
            Method::Get => axum::http::Method::GET,
            Method::Post => axum::http::Method::POST,
            Method::Put => axum::http::Method::PUT,
        }
    }
}

/// Who may call an operation.
#[derive(Debug, Clone, Copy)]
pub enum Auth {
    /// No credential: the operational routes, and the `/api/v1` routes in
    /// `UNAUTHENTICATED_ROUTES`.
    Public,
    /// Any authenticated principal (every read, ADR 0023).
    Authenticated,
    /// An authenticated principal whose role bindings cover this verb.
    Verb {
        verb: &'static str,
        description: &'static str,
    },
}

/// An operation's default read class (ADR 0007 / ADR 0031).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyDefault {
    Strong,
    Bounded,
    Eventual,
    /// Answered from the serving replica's own view, with no read class.
    Local,
}

impl ConsistencyDefault {
    pub fn as_str(self) -> &'static str {
        match self {
            ConsistencyDefault::Strong => "strong",
            ConsistencyDefault::Bounded => "bounded",
            ConsistencyDefault::Eventual => "eventual",
            ConsistencyDefault::Local => "local",
        }
    }
}

/// A path or query parameter.
#[derive(Debug, Clone, Copy)]
pub struct Param {
    pub name: &'static str,
    pub description: &'static str,
    pub required: bool,
    pub example: Option<&'static str>,
}

/// A DTO's JSON Schema, registered with a generator on demand.
#[derive(Clone, Copy)]
pub struct SchemaRef {
    /// The schema's name in the `schemas` map (the type's
    /// [`JsonSchema::schema_name`]).
    pub name: fn() -> Cow<'static, str>,
    /// Registers the type with the generator (`subschema_for::<T>`) and
    /// returns the `$ref` (or inline schema) for it.
    pub register: fn(&mut SchemaGenerator) -> Schema,
}

impl std::fmt::Debug for SchemaRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SchemaRef").field(&(self.name)()).finish()
    }
}

/// The [`SchemaRef`] for `T`.
pub fn schema<T: JsonSchema>() -> SchemaRef {
    fn register<T: JsonSchema>(generator: &mut SchemaGenerator) -> Schema {
        generator.subschema_for::<T>()
    }
    SchemaRef {
        name: T::schema_name,
        register: register::<T>,
    }
}

/// A success response body.
#[derive(Debug, Clone, Copy)]
pub struct Response {
    pub schema: SchemaRef,
    pub content_type: &'static str,
}

const JSON: &str = "application/json";

fn json_response<T: JsonSchema>() -> Option<Response> {
    Some(Response {
        schema: schema::<T>(),
        content_type: JSON,
    })
}

/// One documented failure: the code, and when it happens on this operation.
#[derive(Debug, Clone, Copy)]
pub struct ErrorWhen {
    pub code: ErrorCode,
    pub when: &'static str,
}

/// An [`ErrorWhen`] as a struct literal, so a `&[...]` of them is promoted to
/// `'static` inside a function body.
macro_rules! err {
    ($code:expr, $when:expr $(,)?) => {
        ErrorWhen {
            code: $code,
            when: $when,
        }
    };
}

/// A worked request/response pair. Every example is replayed against the real
/// router in the tests, so it cannot drift from what the server does.
#[derive(Debug, Clone)]
pub struct Example {
    pub name: &'static str,
    pub request: ExampleRequest,
    pub response: ExampleResponse,
}

#[derive(Debug, Clone)]
pub struct ExampleRequest {
    /// Concrete path, in the same base as the operation's `path`.
    pub path: String,
    pub query: Vec<(String, String)>,
    pub body: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ExampleResponse {
    pub status: u16,
    pub body: Option<Value>,
}

/// Whether an operation is served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Stable,
    /// Routed, but answers `UNIMPLEMENTED` until its backing store exists.
    Unimplemented,
}

/// One HTTP operation: one method on one path.
#[derive(Debug, Clone)]
pub struct Operation {
    /// Stable snake_case id; the router looks operations up by it.
    pub id: &'static str,
    pub group: &'static str,
    pub method: Method,
    /// OpenAPI-style template (`/jobs/{job}`), relative to [`BASE_PATH`]
    /// unless `group` is [`OPERATIONAL`].
    pub path: &'static str,
    pub summary: &'static str,
    /// Markdown.
    pub description: &'static str,
    pub auth: Auth,
    /// `None` for writes and for routes outside the read contract.
    pub consistency: Option<ConsistencyDefault>,
    pub path_params: &'static [Param],
    pub query_params: &'static [Param],
    pub request: Option<SchemaRef>,
    /// `None` only for a non-JSON body (or none at all).
    pub response: Option<Response>,
    pub errors: &'static [ErrorWhen],
    pub examples: Vec<Example>,
    pub adr: Option<&'static str>,
    pub status: Status,
}

impl Operation {
    /// The full request path template, base included.
    pub fn full_path(&self) -> String {
        if self.group == OPERATIONAL {
            self.path.to_string()
        } else {
            format!("{BASE_PATH}{}", self.path)
        }
    }

    /// The path as axum 0.7 spells it (`/jobs/:job`), relative to the router
    /// the operation is mounted in.
    pub fn axum_path(&self) -> String {
        self.path.replace('{', ":").replace('}', "")
    }
}

/// A heading the operations are grouped under.
#[derive(Debug, Clone, Copy)]
pub struct Group {
    pub id: &'static str,
    pub title: &'static str,
    pub description: &'static str,
}

/// One entry in the closed error vocabulary.
#[derive(Debug, Clone)]
pub struct ErrorCodeDoc {
    pub code: ErrorCode,
    pub status: u16,
    pub description: &'static str,
}

/// The wire error body every failure carries (ADR 0031). Schema-only: the
/// server renders it from `HttpError`.
pub struct ErrorEnvelope;

impl JsonSchema for ErrorEnvelope {
    fn schema_name() -> Cow<'static, str> {
        "ErrorEnvelope".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        let codes: Vec<&str> = error_codes().iter().map(|e| e.code.as_str()).collect();
        schemars::json_schema!({
            "type": "object",
            "description": "The body of every error response (ADR 0031).",
            "required": ["code", "message"],
            "additionalProperties": false,
            "properties": {
                "code": {
                    "type": "string",
                    "enum": codes,
                    "description": "The closed error vocabulary; clients switch on it."
                },
                "message": {
                    "type": "string",
                    "description": "Human-readable detail. Not a contract."
                }
            }
        })
    }
}

/// Any one frame of the `GET /api/v1/events` stream (ADR 0043). Schema-only:
/// each frame is rendered as an SSE `data:` line.
#[derive(schemars::JsonSchema)]
#[serde(untagged)]
#[allow(dead_code)]
pub enum EventFrame {
    Batch(dto::EventBatchFrame),
    Progress(dto::EventProgressFrame),
    Gap(dto::EventGapFrame),
}

/// The OpenAPI document served at `GET /api/v1/openapi.json`. Schema-only.
pub struct OpenApiDocument;

impl JsonSchema for OpenApiDocument {
    fn schema_name() -> Cow<'static, str> {
        "OpenApiDocument".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "object",
            "description": "An OpenAPI 3.1 document describing this API, generated from the catalogue.",
            "required": ["openapi", "info", "paths"]
        })
    }
}

/// The closed error vocabulary, documented. An exhaustive `match`, so a new
/// [`ErrorCode`] variant does not compile until it is described here.
pub fn error_codes() -> Vec<ErrorCodeDoc> {
    const ALL: [ErrorCode; 9] = [
        ErrorCode::InvalidArgument,
        ErrorCode::Unauthenticated,
        ErrorCode::PermissionDenied,
        ErrorCode::NotFound,
        ErrorCode::Rejected,
        ErrorCode::NotLeader,
        ErrorCode::Unavailable,
        ErrorCode::Unimplemented,
        ErrorCode::Internal,
    ];
    ALL.into_iter()
        .map(|code| {
            let description = match code {
                ErrorCode::InvalidArgument => {
                    "The request is malformed: a bad body, id, or query parameter. \
                     Retrying the identical request cannot help."
                }
                ErrorCode::Unauthenticated => "The credential is missing or invalid (ADR 0022).",
                ErrorCode::PermissionDenied => {
                    "The caller's role bindings do not cover the target (ADR 0023)."
                }
                ErrorCode::NotFound => "The id is well-formed but absent from the read view.",
                ErrorCode::Rejected => {
                    "The command committed and apply refused it deterministically: a normal \
                     race outcome, never a server fault."
                }
                ErrorCode::NotLeader => {
                    "A write reached a follower that could not forward it; the \
                     `Coppice-Leader` header carries the leader hint when one is known."
                }
                ErrorCode::Unavailable => {
                    "The request did not resolve: timeout, overload, shutdown, or a follower \
                     that cannot bound its staleness. Retryable."
                }
                ErrorCode::Unimplemented => {
                    "The route is reserved or provisional and has no implementation yet."
                }
                ErrorCode::Internal => "A bug. Details are logged server-side, never returned.",
            };
            ErrorCodeDoc {
                code,
                status: code.status().as_u16(),
                description,
            }
        })
        .collect()
}

static GROUPS: &[Group] = &[
    Group {
        id: "auth",
        title: "Identity & access",
        description: "How to authenticate, who the caller is, the replicated authorization \
                      policy, and machine enrollment.",
    },
    Group {
        id: "cluster",
        title: "Cluster",
        description: "Cluster-wide summaries: the overview, queue statistics, and the \
                      coordinator replicas.",
    },
    Group {
        id: "jobs",
        title: "Jobs",
        description: "Submit, inspect, annotate, and abort jobs, and read their timelines, \
                      usage, and logs.",
    },
    Group {
        id: "nodes",
        title: "Nodes",
        description: "The compute nodes: inventory, utilization, and the drain / remove \
                      lifecycle (ADR 0041).",
    },
    Group {
        id: "quota",
        title: "Quota entities",
        description: "The quota-entity tree jobs are charged against (ADR 0005), addressed by \
                      id or path (ADR 0045).",
    },
    Group {
        id: "events",
        title: "Events",
        description: "The filtered server-sent event stream of job transitions (ADR 0043).",
    },
    Group {
        id: "meta",
        title: "API description",
        description: "This API, described by itself.",
    },
    Group {
        id: OPERATIONAL,
        title: "Operational",
        description: "Probes and the Prometheus scrape target. Outside `/api/v1`, outside \
                      authentication, and served even before the cluster has formed.",
    },
];

/// The operation groups, in display order.
pub fn groups() -> &'static [Group] {
    GROUPS
}

/// Every operation, in display order.
pub fn operations() -> &'static [Operation] {
    // `OnceLock` rather than `LazyLock`: the workspace MSRV is 1.79.
    static OPS: OnceLock<Vec<Operation>> = OnceLock::new();
    OPS.get_or_init(build_operations)
}

/// The operation with this id. Panics on an unknown id: the router and the
/// catalogue are one table, and a typo there is a bug the tests catch.
pub fn op(id: &str) -> &'static Operation {
    operations()
        .iter()
        .find(|o| o.id == id)
        .unwrap_or_else(|| panic!("no catalogue operation `{id}`"))
}

// ---------------------------------------------------------------------------
// Shared parameters and error lists
// ---------------------------------------------------------------------------

/// Ids the examples use; the example-replay test seeds its state with them.
pub mod example_ids {
    pub const JOB: &str = "job-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c0d";
    pub const MISSING_JOB: &str = "job-0192f0c4-0000-7000-8000-000000000404";
    pub const NEW_JOB: &str = "job-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c30";
    pub const NODE: &str = "node-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c10";
    pub const ENTITY: &str = "quota-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c20";
    pub const ENTITY_NAME: &str = "acme";
}

const CONSISTENCY: Param = Param {
    name: "consistency",
    description: "Override the endpoint's default read class (ADR 0007): `strong` (a \
                  linearizable read through the leader), `bounded` (the replica's latest \
                  view, staleness reported in the `coppice-applied-index` / \
                  `coppice-committed-index` headers), or `eventual`.",
    required: false,
    example: Some("bounded"),
};
const MIN_INDEX: Param = Param {
    name: "min_index",
    description: "Serve from a view whose applied index is at least this — pass a write \
                  response's `log_index` to read your own write.",
    required: false,
    example: Some("42"),
};

/// The ADR 0007 read parameters every read takes.
pub const READ_PARAMS: &[Param] = &[CONSISTENCY, MIN_INDEX];

const JOB_PARAM: &[Param] = &[Param {
    name: "job",
    description: "The job id (`job-<uuid>`, ADR 0024). A malformed id is `INVALID_ARGUMENT`, \
                  never `NOT_FOUND`.",
    required: true,
    example: Some(example_ids::JOB),
}];
const NODE_PARAM: &[Param] = &[Param {
    name: "node",
    description: "The node id (`node-<uuid>`, ADR 0024).",
    required: true,
    example: Some(example_ids::NODE),
}];
const ENTITY_PARAM: &[Param] = &[Param {
    name: "entity",
    description: "A quota entity id (`quota-<uuid>`), or its path percent-encoded into the \
                  one segment (`acme%2Feng`, ADR 0045).",
    required: true,
    example: Some(example_ids::ENTITY),
}];
const COORDINATOR_PARAM: &[Param] = &[Param {
    name: "id",
    description: "The coordinator's raft id: a plain integer, not a typed id.",
    required: true,
    example: Some("1"),
}];

const fn q(name: &'static str, description: &'static str, example: Option<&'static str>) -> Param {
    Param {
        name,
        description,
        required: false,
        example,
    }
}

const LIST_JOBS_QUERY: &[Param] = &[
    q(
        "filter",
        "A URL-encoded JSON `JobFilter` tree (`all`/`any`/`not` over `phase`, `entity`, \
         `node`, `image`, `id`, `search`, `submitted`, `submitted_by`, `requests`, \
         `metadata` leaves). Absent matches every job.",
        Some(r#"{"phase":{"in":["queued","running"]}}"#),
    ),
    q(
        "cursor",
        "Opaque continuation token from a previous page's `next_cursor`.",
        None,
    ),
    q(
        "limit",
        "Page size, 1..=1000 (default 100). Out of range is `INVALID_ARGUMENT`, never clamped.",
        Some("50"),
    ),
    CONSISTENCY,
    MIN_INDEX,
];

const TIMELINE_QUERY: &[Param] = &[
    q(
        "cursor",
        "Opaque continuation token from a previous page's `next_cursor`.",
        None,
    ),
    q("limit", "Page size, 1..=1000 (default 100).", Some("100")),
    CONSISTENCY,
    MIN_INDEX,
];

const ATTEMPT_WINDOW: [Param; 5] = [
    q(
        "attempt",
        "Scope the walk to one attempt (`attempt-<uuid>`); it must belong to the job.",
        None,
    ),
    q(
        "from",
        "Inclusive lower bound, an RFC 3339 instant.",
        Some("2026-07-16T09:30:00Z"),
    ),
    q("to", "Exclusive upper bound, an RFC 3339 instant.", None),
    q("to_inclusive", "Make `to` inclusive.", Some("false")),
    q("order", "`asc` or `desc`.", Some("desc")),
];

const USAGE_QUERY: &[Param] = &[
    q(
        "cursor",
        "Opaque continuation token from a previous page.",
        None,
    ),
    q(
        "limit",
        "Samples per page, 1..=5000 (default 1000).",
        Some("1000"),
    ),
    ATTEMPT_WINDOW[0],
    ATTEMPT_WINDOW[1],
    ATTEMPT_WINDOW[2],
    ATTEMPT_WINDOW[3],
    q(
        "order",
        "`asc` (default: a chart-ordered series) or `desc`.",
        Some("asc"),
    ),
    CONSISTENCY,
    MIN_INDEX,
];

const LOGS_QUERY: &[Param] = &[
    q(
        "cursor",
        "Opaque continuation token from a previous page.",
        None,
    ),
    q(
        "limit",
        "Entries per page, 1..=1000 (default 200).",
        Some("200"),
    ),
    q(
        "stream",
        "`stdout` or `stderr`; absent is both.",
        Some("stderr"),
    ),
    ATTEMPT_WINDOW[0],
    ATTEMPT_WINDOW[1],
    ATTEMPT_WINDOW[2],
    ATTEMPT_WINDOW[3],
    q(
        "order",
        "`desc` (default: newest first) or `asc`.",
        Some("desc"),
    ),
    CONSISTENCY,
    MIN_INDEX,
];

const PROVISIONAL_LOGS_QUERY: &[Param] = &[
    q("cursor", "Opaque continuation token (reserved).", None),
    q("limit", "Page size (reserved).", None),
    CONSISTENCY,
    MIN_INDEX,
];

const EVENTS_QUERY: &[Param] = &[
    Param {
        name: "jobs",
        description: "A URL-encoded JSON `JobFilter` naming the jobs to follow, restricted to \
                      the leaves a subscription can answer. Required: there is no \
                      unfiltered firehose.",
        required: true,
        example: Some(r#"{"id":{"in":["job-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c0d"]}}"#),
    },
    q(
        "cursor",
        "Resume just after this applied index. A `Last-Event-ID` header, when sent, wins.",
        Some("42"),
    ),
];

const READYZ_QUERY: &[Param] = &[q(
    "require",
    "`healthy` adds the cluster-redundancy gate (answered only by the leader). The only \
     recognized value.",
    Some("healthy"),
)];

const NO_PARAMS: &[Param] = &[];

const BAD_READ_PARAMS: ErrorWhen = err!(
    ErrorCode::InvalidArgument,
    "`consistency` or `min_index` is malformed.",
);
const UNAUTHENTICATED: ErrorWhen = err!(
    ErrorCode::Unauthenticated,
    "The credential is missing or invalid.",
);
const UNAVAILABLE: ErrorWhen = err!(
    ErrorCode::Unavailable,
    "The read could not be served at the requested class (e.g. a strong read with no \
     reachable leader, or `min_index` not reached in time).",
);
const WRITE_UNAVAILABLE: ErrorWhen = err!(
    ErrorCode::Unavailable,
    "The write did not resolve: timeout, overload, or shutdown. Retry with the same body.",
);
const NOT_LEADER: ErrorWhen = err!(
    ErrorCode::NotLeader,
    "This replica could not forward the write to a leader.",
);
const FORBIDDEN: ErrorWhen = err!(
    ErrorCode::PermissionDenied,
    "The caller's bindings do not cover this verb.",
);
const BAD_BODY: ErrorWhen = err!(
    ErrorCode::InvalidArgument,
    "The body is not valid JSON for this request, has an unknown field, or fails \
     validation.",
);

const READ_ERRORS: &[ErrorWhen] = &[BAD_READ_PARAMS, UNAUTHENTICATED, UNAVAILABLE];

const JOB_READ_ERRORS: &[ErrorWhen] = &[
    err!(
        ErrorCode::InvalidArgument,
        "The job id is malformed, or a read parameter is.",
    ),
    UNAUTHENTICATED,
    err!(
        ErrorCode::NotFound,
        "No job with this id is in the read view."
    ),
    UNAVAILABLE,
];

const NODE_READ_ERRORS: &[ErrorWhen] = &[
    err!(
        ErrorCode::InvalidArgument,
        "The node id is malformed, or a read parameter is.",
    ),
    UNAUTHENTICATED,
    err!(
        ErrorCode::NotFound,
        "No node with this id is in the read view."
    ),
    UNAVAILABLE,
];

const UNIMPLEMENTED_ERRORS: &[ErrorWhen] = &[
    err!(
        ErrorCode::InvalidArgument,
        "The path id is malformed, or a read parameter is.",
    ),
    UNAUTHENTICATED,
    err!(
        ErrorCode::Unimplemented,
        "Always, until the backing store exists.",
    ),
];

const JOB_WRITE_ERRORS: &[ErrorWhen] = &[
    err!(
        ErrorCode::InvalidArgument,
        "The job id or the body is malformed.",
    ),
    UNAUTHENTICATED,
    FORBIDDEN,
    err!(
        ErrorCode::Rejected,
        "Apply refused the command (e.g. the job is unknown at its log position, or already \
         terminal).",
    ),
    NOT_LEADER,
    WRITE_UNAVAILABLE,
];

const NODE_WRITE_ERRORS: &[ErrorWhen] = &[
    err!(ErrorCode::InvalidArgument, "The node id is malformed."),
    UNAUTHENTICATED,
    FORBIDDEN,
    err!(ErrorCode::NotFound, "No node with this id exists."),
    NOT_LEADER,
    WRITE_UNAVAILABLE,
];

const VERB_SUBMIT: Auth = Auth::Verb {
    verb: "submit",
    description: "`submitter` or higher over the charged quota entity.",
};
const VERB_ABORT: Auth = Auth::Verb {
    verb: "abort",
    description: "`operator` or higher over the job's quota entity, or having submitted the \
                  job.",
};
const VERB_METADATA: Auth = Auth::Verb {
    verb: "update_job_metadata",
    description: "Exactly as abort: `operator` or higher over the job's quota entity, or \
                  having submitted the job (ADR 0042).",
};
const VERB_DRAIN: Auth = Auth::Verb {
    verb: "drain",
    description: "A cluster verb: an unscoped `operator` binding or higher.",
};
const VERB_CONFIGURE: Auth = Auth::Verb {
    verb: "configure_quota_entity",
    description: "`admin` covering the entity's position — for a create, the parent it is \
                  created under, so a new root takes unscoped `admin`.",
};
const VERB_AUTHORIZATION: Auth = Auth::Verb {
    verb: "update_authorization",
    description: "Unscoped `admin` only.",
};

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

fn ex(
    name: &'static str,
    path: impl Into<String>,
    query: &[(&str, &str)],
    body: Option<Value>,
    status: u16,
    response: Option<Value>,
) -> Example {
    Example {
        name,
        request: ExampleRequest {
            path: path.into(),
            query: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body,
        },
        response: ExampleResponse {
            status,
            body: response,
        },
    }
}

/// Parse an example body written as a JSON literal.
fn body(raw: &str) -> Option<Value> {
    Some(serde_json::from_str(raw).expect("example bodies are valid JSON"))
}

#[allow(clippy::too_many_lines)]
fn build_operations() -> Vec<Operation> {
    use example_ids as ids;
    use ConsistencyDefault as C;

    // Most operations share everything but a handful of fields.
    let base = Operation {
        id: "",
        group: "",
        method: Method::Get,
        path: "",
        summary: "",
        description: "",
        auth: Auth::Authenticated,
        consistency: None,
        path_params: NO_PARAMS,
        query_params: NO_PARAMS,
        request: None,
        response: None,
        errors: READ_ERRORS,
        examples: Vec::new(),
        adr: Some("0031"),
        status: Status::Stable,
    };

    vec![
        // ---- Identity & access ------------------------------------------
        Operation {
            id: "get_auth_config",
            group: "auth",
            path: "/auth/config",
            summary: "Discover how to authenticate",
            description: "The deployment's authentication posture (ADR 0022): `open`, or \
                          `oidc` with the issuer, client id and audience a client needs to \
                          obtain a bearer token. Served without a credential — requiring one \
                          to learn which one to obtain would be a loop with no entry point.",
            auth: Auth::Public,
            response: json_response::<dto::GetAuthConfigResponse>(),
            errors: &[],
            adr: Some("0022"),
            ..base.clone()
        },
        Operation {
            id: "get_session",
            group: "auth",
            path: "/session",
            summary: "Who am I",
            description: "The identity the authentication layer resolved for this very \
                          request, plus the replicated role bindings that match it (ADR \
                          0023), reported faithfully rather than collapsed into one \
                          effective role. A display: nothing is authorized on the strength \
                          of what it says, so it defaults to an eventual read.",
            consistency: Some(C::Eventual),
            query_params: READ_PARAMS,
            response: json_response::<dto::GetSessionResponse>(),
            adr: Some("0022"),
            examples: vec![ex(
                "Open mode",
                "/session",
                &[],
                None,
                200,
                body(
                    r#"{"principal":"anonymous","groups":[],"auth_method":"open",
                        "name":null,"email":null,"bindings":[],"implicit_admin":true}"#,
                ),
            )],
            ..base.clone()
        },
        Operation {
            id: "get_authorization",
            group: "auth",
            path: "/authorization",
            summary: "Read the authorization policy",
            description: "The replicated bindings list and the `groups_claim` name (ADR \
                          0023). Strong by default: this is the document an operator edits \
                          and PUTs back, and a read-modify-write over a stale snapshot would \
                          silently revert whatever landed in between.",
            consistency: Some(C::Strong),
            query_params: READ_PARAMS,
            response: json_response::<dto::GetAuthorizationResponse>(),
            adr: Some("0023"),
            ..base.clone()
        },
        Operation {
            id: "update_authorization",
            group: "auth",
            method: Method::Put,
            path: "/authorization",
            summary: "Replace the authorization policy",
            description: "Full replacement of the replicated bindings (ADR 0023). A \
                          `groups_claim` in the body rides the same command, so renaming the \
                          claim and swapping the admin group is one atomic edit. A list that \
                          scopes a role to an unknown entity, names no subject, or would \
                          leave no unscoped admin is `INVALID_ARGUMENT`.",
            auth: VERB_AUTHORIZATION,
            request: Some(schema::<dto::UpdateAuthorizationRequest>()),
            response: json_response::<dto::UpdateAuthorizationResponse>(),
            errors: &[
                err!(
                    ErrorCode::InvalidArgument,
                    "A binding names zero or two subjects, scopes to an unknown entity, or \
                     the list would lock the cluster out of its own authorization.",
                ),
                UNAUTHENTICATED,
                FORBIDDEN,
                NOT_LEADER,
                WRITE_UNAVAILABLE,
            ],
            adr: Some("0023"),
            ..base.clone()
        },
        Operation {
            id: "enroll",
            group: "auth",
            method: Method::Post,
            path: "/enroll",
            summary: "Enroll a machine",
            description: "A coordinator's or agent's certless first contact (ADR 0037 §4): \
                          authenticated solely by its role-scoped enrollment token, it \
                          exchanges a CSR for a cluster-signed certificate. Every refusal is \
                          one constant body, so the endpoint is no token-validity oracle.",
            auth: Auth::Public,
            request: Some(schema::<coppice_enroll::EnrollRequest>()),
            response: json_response::<coppice_enroll::EnrollResponse>(),
            // The refusal body is its own constant, not the error
            // envelope; documented here as the closest code.
            errors: &[err!(
                ErrorCode::PermissionDenied,
                "The token is unknown, expired, spent, or for another role (one constant \
                 body for all of them).",
            )],
            adr: Some("0037"),
            ..base.clone()
        },
        // ---- Cluster ----------------------------------------------------
        Operation {
            id: "get_overview",
            group: "cluster",
            path: "/overview",
            summary: "Cluster overview",
            description: "Queue depth, jobs by state, node counts and capacity — bounded \
                          reads of replicated state — plus queue rates and history, which \
                          are derived, replica-local, and coverage-annotated (ADR 0032).",
            consistency: Some(C::Bounded),
            query_params: READ_PARAMS,
            response: json_response::<dto::GetClusterOverviewResponse>(),
            examples: vec![ex(
                "Overview",
                "/overview",
                &[],
                None,
                200,
                body(EX_OVERVIEW),
            )],
            ..base.clone()
        },
        Operation {
            id: "get_queue_stats",
            group: "cluster",
            path: "/queue/stats",
            summary: "Queue statistics",
            description: "The overview's `queue` object on its own: depth, oldest queued \
                          age, and the derived rates and history.",
            consistency: Some(C::Bounded),
            query_params: READ_PARAMS,
            response: json_response::<dto::QueueStats>(),
            ..base.clone()
        },
        Operation {
            id: "get_coordinators",
            group: "cluster",
            path: "/coordinators",
            summary: "Coordinator status",
            description: "The consensus and membership summary as this replica sees it, \
                          plus a replica-local state snapshot (version and object counts). \
                          `UNAVAILABLE` when this replica has no consensus handle.",
            consistency: Some(C::Local),
            query_params: READ_PARAMS,
            response: json_response::<dto::GetCoordinatorStatusResponse>(),
            ..base.clone()
        },
        Operation {
            id: "get_coordinator_logs",
            group: "cluster",
            path: "/coordinators/{id}/logs",
            summary: "Coordinator process logs",
            description: "Reserved. Coordinator process logs have no capture substrate yet \
                          (ADR 0034 non-goals), so the route answers `UNIMPLEMENTED`.",
            consistency: Some(C::Eventual),
            path_params: COORDINATOR_PARAM,
            query_params: PROVISIONAL_LOGS_QUERY,
            // No response DTO exists for a reserved route.
            errors: UNIMPLEMENTED_ERRORS,
            status: Status::Unimplemented,
            ..base.clone()
        },
        // ---- Jobs -------------------------------------------------------
        Operation {
            id: "list_jobs",
            group: "jobs",
            path: "/jobs",
            summary: "List jobs",
            description: "Jobs newest first (descending job id), filtered by a JSON \
                          `JobFilter` tree and paged by an opaque keyset cursor. A short \
                          page with a non-null `next_cursor` means *continue*, never *done*.",
            consistency: Some(C::Bounded),
            query_params: LIST_JOBS_QUERY,
            response: json_response::<dto::ListJobsResponse>(),
            errors: &[
                err!(
                    ErrorCode::InvalidArgument,
                    "The filter, cursor, limit, or a read parameter is malformed, or the \
                     filter names an entity path that does not resolve.",
                ),
                UNAUTHENTICATED,
                UNAVAILABLE,
            ],
            examples: vec![
                ex("All jobs", "/jobs", &[], None, 200, body(EX_LIST_JOBS)),
                ex(
                    "Queued jobs, ten at a time",
                    "/jobs",
                    &[
                        ("filter", r#"{"phase":{"in":["queued"]}}"#),
                        ("limit", "10"),
                    ],
                    None,
                    200,
                    body(EX_LIST_JOBS),
                ),
            ],
            ..base.clone()
        },
        Operation {
            id: "submit_job",
            group: "jobs",
            method: Method::Post,
            path: "/jobs",
            summary: "Submit a job",
            description: "Submit a container job. The client mints the `job` id and it is \
                          the submission's idempotency key (ADR 0026): retry with the same \
                          body after a timeout and you get the same job, never a second one. \
                          Pair the response's `log_index` with `?min_index=` for \
                          read-your-writes.",
            auth: VERB_SUBMIT,
            request: Some(schema::<dto::SubmitJobRequest>()),
            response: json_response::<dto::SubmitJobResponse>(),
            errors: &[
                BAD_BODY,
                UNAUTHENTICATED,
                FORBIDDEN,
                err!(
                    ErrorCode::Rejected,
                    "The quota entity does not exist (or is not a leaf), or the id was \
                     reused with a different payload.",
                ),
                NOT_LEADER,
                WRITE_UNAVAILABLE,
            ],
            adr: Some("0026"),
            examples: vec![ex(
                "Submit a small job",
                "/jobs",
                &[],
                body(&format!(
                    r#"{{"job":"{}","image":"busybox:1.36","command":["sh","-c","echo hello"],
                        "requests":{{"cpu_millis":500,"memory_bytes":268435456,"disk_bytes":0}},
                        "quota_entity":"{}","max_runtime_seconds":600,
                        "metadata":{{"team":"platform"}}}}"#,
                    ids::NEW_JOB,
                    ids::ENTITY_NAME
                )),
                200,
                body(&format!(r#"{{"job":"{}","log_index":7}}"#, ids::NEW_JOB)),
            )],
            ..base.clone()
        },
        Operation {
            id: "get_job",
            group: "jobs",
            path: "/jobs/{job}",
            summary: "Get a job",
            description: "One job in full: spec, state, attempts, the quota-entity chain, \
                          the queue-position explainer while queued, the accrual view while \
                          accruing, the cost report, and its metadata.",
            consistency: Some(C::Bounded),
            path_params: JOB_PARAM,
            query_params: READ_PARAMS,
            response: json_response::<dto::JobDetail>(),
            errors: JOB_READ_ERRORS,
            examples: vec![
                ex(
                    "A queued job",
                    format!("/jobs/{}", ids::JOB),
                    &[("consistency", "bounded")],
                    None,
                    200,
                    body(EX_GET_JOB),
                ),
                ex(
                    "An unknown job",
                    format!("/jobs/{}", ids::MISSING_JOB),
                    &[],
                    None,
                    404,
                    body(&format!(
                        r#"{{"code":"NOT_FOUND","message":"job {} not found"}}"#,
                        ids::MISSING_JOB
                    )),
                ),
            ],
            ..base.clone()
        },
        Operation {
            id: "abort_job",
            group: "jobs",
            method: Method::Post,
            path: "/jobs/{job}/abort",
            summary: "Abort a job",
            description: "Request that a job stop. The path is authoritative for the job \
                          id: the body's `job` may be omitted (`{}` aborts with no reason) \
                          and, when present, must match the path.",
            auth: VERB_ABORT,
            path_params: JOB_PARAM,
            request: Some(schema::<dto::AbortJobRequest>()),
            response: json_response::<dto::AbortJobResponse>(),
            errors: JOB_WRITE_ERRORS,
            examples: vec![ex(
                "Abort with a reason",
                format!("/jobs/{}/abort", ids::JOB),
                &[],
                body(r#"{"reason":"superseded by a newer run"}"#),
                200,
                body("{}"),
            )],
            ..base.clone()
        },
        Operation {
            id: "replace_job_metadata",
            group: "jobs",
            method: Method::Put,
            path: "/jobs/{job}/metadata",
            summary: "Replace a job's metadata",
            description: "Replace the job's whole metadata map (ADR 0042). Every limit is \
                          checked at the edge, since the replacement *is* the resulting map.",
            auth: VERB_METADATA,
            path_params: JOB_PARAM,
            request: Some(schema::<dto::ReplaceJobMetadataRequest>()),
            response: json_response::<dto::ReplaceJobMetadataResponse>(),
            errors: JOB_WRITE_ERRORS,
            adr: Some("0042"),
            ..base.clone()
        },
        Operation {
            id: "update_job_metadata",
            group: "jobs",
            method: Method::Post,
            path: "/jobs/{job}/metadata",
            summary: "Patch a job's metadata",
            description: "Merge `set` over the map and drop the `unset` keys (ADR 0042). The \
                          request's own consistency is checked at the edge; the merged map's \
                          key count and size can only be checked at apply, so a breach there \
                          is `REJECTED`.",
            auth: VERB_METADATA,
            path_params: JOB_PARAM,
            request: Some(schema::<dto::UpdateJobMetadataRequest>()),
            response: json_response::<dto::UpdateJobMetadataResponse>(),
            errors: JOB_WRITE_ERRORS,
            adr: Some("0042"),
            ..base.clone()
        },
        Operation {
            id: "get_job_timeline",
            group: "jobs",
            path: "/jobs/{job}/timeline",
            summary: "A job's transition timeline",
            description: "The job's state transitions from this replica's event ring (ADR \
                          0032), honestly partial: `floor_index` says where retained history \
                          begins. Eventual by default — a derived, replica-local read.",
            consistency: Some(C::Eventual),
            path_params: JOB_PARAM,
            query_params: TIMELINE_QUERY,
            response: json_response::<dto::GetJobTimelineResponse>(),
            errors: JOB_READ_ERRORS,
            adr: Some("0032"),
            ..base.clone()
        },
        Operation {
            id: "get_job_usage",
            group: "jobs",
            path: "/jobs/{job}/usage",
            summary: "A job's resource usage samples",
            description: "Best-effort raw cumulative counter samples fetched from the agents \
                          that ran the job's attempts (ADR 0036), with a per-attempt \
                          `sources` availability verdict. A request that retrieves nothing is \
                          still a 200 with the full accounting.",
            consistency: Some(C::Eventual),
            path_params: JOB_PARAM,
            query_params: USAGE_QUERY,
            response: json_response::<dto::GetJobUsageResponse>(),
            errors: JOB_READ_ERRORS,
            adr: Some("0036"),
            ..base.clone()
        },
        Operation {
            id: "get_job_logs",
            group: "jobs",
            path: "/jobs/{job}/logs",
            summary: "A job's container logs",
            description: "Best-effort log retrieval from the agents' telemetry segment \
                          stores (ADR 0034), newest first by default, with per-attempt \
                          `sources` availability and an opaque cursor.",
            consistency: Some(C::Eventual),
            path_params: JOB_PARAM,
            query_params: LOGS_QUERY,
            response: json_response::<dto::GetJobLogsResponse>(),
            errors: JOB_READ_ERRORS,
            adr: Some("0034"),
            ..base.clone()
        },
        // ---- Nodes ------------------------------------------------------
        Operation {
            id: "list_nodes",
            group: "nodes",
            path: "/nodes",
            summary: "List nodes",
            description: "Every registered node with its capacity, allocation, measured \
                          usage where known, drain flags, and leader-sourced health (ADR \
                          0040).",
            consistency: Some(C::Bounded),
            query_params: READ_PARAMS,
            response: json_response::<dto::ListNodesResponse>(),
            examples: vec![ex(
                "All nodes",
                "/nodes",
                &[],
                None,
                200,
                body(EX_LIST_NODES),
            )],
            ..base.clone()
        },
        Operation {
            id: "get_node",
            group: "nodes",
            path: "/nodes/{node}",
            summary: "Get a node",
            description: "One node in full, including its host facts and the allocations \
                          and accruals placed on it.",
            consistency: Some(C::Bounded),
            path_params: NODE_PARAM,
            query_params: READ_PARAMS,
            response: json_response::<dto::GetNodeResponse>(),
            errors: NODE_READ_ERRORS,
            examples: vec![ex(
                "A node",
                format!("/nodes/{}", ids::NODE),
                &[],
                None,
                200,
                body(EX_GET_NODE),
            )],
            ..base.clone()
        },
        Operation {
            id: "get_node_utilization",
            group: "nodes",
            path: "/nodes/{node}/utilization",
            summary: "A node's utilization history",
            description: "30-second usage buckets from this replica's in-memory window (ADR \
                          0039). A follower answers 200 with empty `samples`; 404 only when \
                          the node is not in the read view.",
            consistency: Some(C::Eventual),
            path_params: NODE_PARAM,
            query_params: READ_PARAMS,
            response: json_response::<dto::GetNodeUtilizationResponse>(),
            errors: NODE_READ_ERRORS,
            adr: Some("0039"),
            ..base.clone()
        },
        Operation {
            id: "drain_node",
            group: "nodes",
            method: Method::Post,
            path: "/nodes/{node}/drain",
            summary: "Drain a node",
            description: "Turn the admin cordon on: no new placements. Bodyless and \
                          idempotent; watch `GET /nodes/{node}` for the work to drain away.",
            auth: VERB_DRAIN,
            path_params: NODE_PARAM,
            response: json_response::<dto::DrainNodeResponse>(),
            errors: NODE_WRITE_ERRORS,
            adr: Some("0041"),
            ..base.clone()
        },
        Operation {
            id: "undrain_node",
            group: "nodes",
            method: Method::Post,
            path: "/nodes/{node}/undrain",
            summary: "Undrain a node",
            description: "Lift the admin cordon. The exact twin of drain.",
            auth: VERB_DRAIN,
            path_params: NODE_PARAM,
            response: json_response::<dto::DrainNodeResponse>(),
            errors: NODE_WRITE_ERRORS,
            adr: Some("0041"),
            ..base.clone()
        },
        Operation {
            id: "remove_node",
            group: "nodes",
            method: Method::Post,
            path: "/nodes/{node}/remove",
            summary: "Remove a node's record",
            description: "Evict a drained, empty node's record. A node that still accepts \
                          placements or holds a live allocation is `REJECTED` (retry after \
                          the drain); one already gone is a 200.",
            auth: VERB_DRAIN,
            path_params: NODE_PARAM,
            response: json_response::<dto::RemoveNodeResponse>(),
            errors: &[
                err!(ErrorCode::InvalidArgument, "The node id is malformed."),
                UNAUTHENTICATED,
                FORBIDDEN,
                err!(
                    ErrorCode::Rejected,
                    "The node still accepts placements or holds a live allocation.",
                ),
                NOT_LEADER,
                WRITE_UNAVAILABLE,
            ],
            adr: Some("0041"),
            ..base.clone()
        },
        Operation {
            id: "get_node_history",
            group: "nodes",
            path: "/nodes/{node}/history",
            summary: "A node's history",
            description: "Reserved for a node's event history; answers `UNIMPLEMENTED`.",
            consistency: Some(C::Eventual),
            path_params: NODE_PARAM,
            query_params: READ_PARAMS,
            errors: UNIMPLEMENTED_ERRORS,
            status: Status::Unimplemented,
            ..base.clone()
        },
        Operation {
            id: "get_node_logs",
            group: "nodes",
            path: "/nodes/{node}/logs",
            summary: "A node agent's process logs",
            description: "Reserved. Agent process logs have no capture substrate yet (ADR \
                          0034 non-goals), so the route answers `UNIMPLEMENTED`.",
            consistency: Some(C::Eventual),
            path_params: NODE_PARAM,
            query_params: PROVISIONAL_LOGS_QUERY,
            errors: UNIMPLEMENTED_ERRORS,
            status: Status::Unimplemented,
            ..base.clone()
        },
        // ---- Quota entities ---------------------------------------------
        Operation {
            id: "list_quota_entities",
            group: "quota",
            path: "/quota-entities",
            summary: "List quota entities",
            description: "The whole quota-entity tree, each with its quota and usage \
                          decayed to read time.",
            consistency: Some(C::Bounded),
            query_params: READ_PARAMS,
            response: json_response::<dto::ListQuotaEntitiesResponse>(),
            examples: vec![ex(
                "The tree",
                "/quota-entities",
                &[],
                None,
                200,
                body(EX_LIST_QUOTA_ENTITIES),
            )],
            ..base.clone()
        },
        Operation {
            id: "configure_quota_entity",
            group: "quota",
            method: Method::Post,
            path: "/quota-entities",
            summary: "Create or update a quota entity",
            description: "The create-or-update upsert. The client mints the entity id; \
                          `name` and `parent` are fixed at creation (ADR 0045). The response \
                          echoes the id, the entity's path, and the `log_index`.",
            auth: VERB_CONFIGURE,
            request: Some(schema::<dto::ConfigureQuotaEntityRequest>()),
            response: json_response::<dto::ConfigureQuotaEntityResponse>(),
            errors: &[
                err!(
                    ErrorCode::InvalidArgument,
                    "The body is malformed, or `name` breaks the segment grammar.",
                ),
                UNAUTHENTICATED,
                FORBIDDEN,
                err!(
                    ErrorCode::Rejected,
                    "Unknown parent, a sibling name clash, a too-deep create, or an update \
                     that changes `name` or `parent`.",
                ),
                NOT_LEADER,
                WRITE_UNAVAILABLE,
            ],
            adr: Some("0045"),
            ..base.clone()
        },
        Operation {
            id: "get_quota_entity",
            group: "quota",
            path: "/quota-entities/{entity}",
            summary: "Get a quota entity",
            description: "One entity with its stats. Strong by default: a configuration \
                          read (ADR 0007).",
            consistency: Some(C::Strong),
            path_params: ENTITY_PARAM,
            query_params: READ_PARAMS,
            response: json_response::<dto::GetQuotaEntityResponse>(),
            errors: &[
                err!(
                    ErrorCode::InvalidArgument,
                    "The segment is neither a valid id nor a valid path.",
                ),
                UNAUTHENTICATED,
                err!(
                    ErrorCode::NotFound,
                    "No entity has this id, or the path names nothing.",
                ),
                UNAVAILABLE,
            ],
            adr: Some("0045"),
            ..base.clone()
        },
        // ---- Events -----------------------------------------------------
        Operation {
            id: "subscribe_events",
            group: "events",
            path: "/events",
            summary: "Subscribe to job events",
            description: "A server-sent event stream of job transitions matching the `jobs` \
                          filter (ADR 0043). Three frame kinds: `batch` (events, with an \
                          `id:` to resume from), `progress` (a coverage bookmark that doubles \
                          as the keepalive), and `gap` (history the replica no longer holds). \
                          Reconnect with `Last-Event-ID` to resume. The stream ends cleanly \
                          at the bearer token's expiry.",
            consistency: Some(C::Eventual),
            query_params: EVENTS_QUERY,
            response: Some(Response {
                schema: schema::<EventFrame>(),
                content_type: "text/event-stream",
            }),
            errors: &[
                err!(
                    ErrorCode::InvalidArgument,
                    "`jobs` is missing or malformed, names a leaf a subscription cannot \
                     answer, or `cursor` is not numeric.",
                ),
                UNAUTHENTICATED,
                err!(
                    ErrorCode::Unavailable,
                    "This replica's subscription cap is reached, or it has no event fanout.",
                ),
            ],
            adr: Some("0043"),
            ..base.clone()
        },
        // ---- Meta -------------------------------------------------------
        Operation {
            id: "get_openapi",
            group: "meta",
            path: "/openapi.json",
            summary: "This API as OpenAPI 3.1",
            description: "The OpenAPI document generated from the same catalogue the router \
                          is built from. Served without a credential.",
            auth: Auth::Public,
            response: json_response::<OpenApiDocument>(),
            errors: &[],
            adr: None,
            ..base.clone()
        },
        // ---- Operational ------------------------------------------------
        Operation {
            id: "healthz",
            group: OPERATIONAL,
            path: "/healthz",
            summary: "Liveness",
            description: "200 for as long as the process is serving. Never consults the \
                          cluster, so an orchestrator cannot kill a healthy daemon \
                          mid-election.",
            auth: Auth::Public,
            response: json_response::<dto::HealthzResponse>(),
            errors: &[],
            adr: Some("0041"),
            examples: vec![ex(
                "Alive",
                "/healthz",
                &[],
                None,
                200,
                body(r#"{"status":"ok"}"#),
            )],
            ..base.clone()
        },
        Operation {
            id: "readyz",
            group: OPERATIONAL,
            path: "/readyz",
            summary: "Readiness",
            description: "The ADR 0037 §9 readiness report: the daemon's phase and why it \
                          is or is not ready. 200 when ready, 503 otherwise (same body). \
                          `?require=healthy` adds the cluster-redundancy gate.",
            auth: Auth::Public,
            query_params: READYZ_QUERY,
            response: json_response::<crate::http::ReadyzReport>(),
            errors: &[
                err!(ErrorCode::InvalidArgument, "`require` is not `healthy`."),
                err!(
                    ErrorCode::Unavailable,
                    "Not ready (the body is the readiness report, not the error envelope).",
                ),
            ],
            adr: Some("0037"),
            ..base.clone()
        },
        Operation {
            id: "metrics",
            group: OPERATIONAL,
            path: "/metrics",
            summary: "Prometheus metrics",
            description: "The Prometheus scrape target, in the text exposition format.",
            auth: Auth::Public,
            errors: &[],
            adr: None,
            ..base
        },
    ]
}

// Example response bodies, as the example-replay test's seeded state answers
// them (timestamps aside: ages and `now`-derived fields vary per run).

const EX_OVERVIEW: &str = r#"{
  "capacity": {
    "allocated": {
      "cpu_millis": 0,
      "disk_bytes": 0,
      "memory_bytes": 0
    },
    "capacity": {
      "cpu_millis": 8000,
      "disk_bytes": 0,
      "memory_bytes": 17179869184
    },
    "history": [],
    "nodes": {
      "lost": 0,
      "schedulable": 1,
      "total": 1
    },
    "reporting_nodes": 0,
    "total_nodes": 1,
    "used": null
  },
  "cluster_id": "cluster-00000000-0000-0000-0000-000000000009",
  "queue": {
    "accruing": 0,
    "arrival_rate_per_minute": null,
    "by_state": {
      "aborted": 0,
      "accepted": 0,
      "accruing": 0,
      "failed": 0,
      "finalizing": 0,
      "preparing": 0,
      "queued": 1,
      "running": 0,
      "submitted": 0,
      "succeeded": 0
    },
    "depth": 1,
    "drain_rate_per_minute": null,
    "history": [],
    "oldest_queued_age_seconds": 420
  }
}"#;

const EX_LIST_JOBS: &str = r#"{
  "jobs": [
    {
      "attempt": null,
      "attempt_state": null,
      "cost_ucu": 0,
      "funding_fraction": null,
      "id": "job-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c0d",
      "image": "busybox",
      "metadata": {},
      "node": null,
      "outcome": null,
      "priority": 0,
      "quota_entity": "quota-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c20",
      "quota_entity_path": "acme",
      "state": "queued",
      "submitted_at": "2026-07-16T09:30:00.000000Z",
      "submitted_by": null,
      "terminal_at": null
    }
  ],
  "next_cursor": null
}"#;

const EX_GET_JOB: &str = r#"{
  "abort_requested": null,
  "accrual": null,
  "attempts": [],
  "cost": {
    "actual_ucu": null,
    "charge_window_is_default": true,
    "charge_window_seconds": 86400,
    "charged_ucu": 0,
    "effective_rate_ucu_per_second": 0,
    "estimated_ucu": 0,
    "priority_multiplier": 1.0,
    "rate_breakdown": {
      "cpu": 0,
      "disk": 0,
      "memory": 0
    },
    "rate_ucu_per_second": 0,
    "refund_fraction": 1.0,
    "true_up": null,
    "unbounded_multiplier": 2.0
  },
  "entity_chain": [
    {
      "id": "quota-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c20",
      "name": "acme",
      "over_quota_ratio": 0.0,
      "parent": null,
      "path": "acme",
      "penalty": 1.0,
      "quota_ucu": 1000000,
      "usage_ucu": 0
    }
  ],
  "id": "job-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c0d",
  "metadata": {},
  "queue": {
    "age_seconds": 420,
    "multiplier": 1.0,
    "penalty_chain": [
      {
        "entity": "quota-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c20",
        "name": "acme",
        "over_quota_ratio": 0.0,
        "path": "acme",
        "penalty": 1.0,
        "quota_ucu": 1000000,
        "usage_ucu": 0
      }
    ],
    "penalty_product": 1.0
  },
  "retries_used": 0,
  "spec": {
    "command": [
      "run"
    ],
    "entrypoint": null,
    "env": {},
    "image": "busybox",
    "max_runtime_seconds": null,
    "priority": 0,
    "quota_entity": "quota-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c20",
    "quota_entity_path": "acme",
    "requests": {
      "cpu_millis": 0,
      "disk_bytes": 0,
      "memory_bytes": 0
    },
    "retry": {
      "max_retries": 3,
      "retry_user_errors": false
    },
    "submitted_by": null
  },
  "state": "queued",
  "state_since": "2026-07-16T09:30:00.000000Z",
  "submitted_at": "2026-07-16T09:30:00.000000Z",
  "terminal_at": null
}"#;

const EX_LIST_NODES: &str = r#"{
  "nodes": [
    {
      "accruing_count": 0,
      "allocated": {
        "cpu_millis": 0,
        "disk_bytes": 0,
        "memory_bytes": 0
      },
      "capacity": {
        "cpu_millis": 8000,
        "disk_bytes": 0,
        "memory_bytes": 17179869184
      },
      "draining": false,
      "epoch": 1,
      "health": "unknown",
      "id": "node-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c10",
      "labels": {},
      "last_heartbeat": null,
      "running_count": 0,
      "schedulable": true,
      "used": null
    }
  ]
}"#;

const EX_GET_NODE: &str = r#"{
  "accrual_queue": [],
  "active_attempts": [],
  "detected_capacity": null,
  "host": null,
  "summary": {
    "accruing_count": 0,
    "allocated": {
      "cpu_millis": 0,
      "disk_bytes": 0,
      "memory_bytes": 0
    },
    "capacity": {
      "cpu_millis": 8000,
      "disk_bytes": 0,
      "memory_bytes": 17179869184
    },
    "draining": false,
    "epoch": 1,
    "health": "unknown",
    "id": "node-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c10",
    "labels": {},
    "last_heartbeat": null,
    "running_count": 0,
    "schedulable": true,
    "used": null
  }
}"#;

const EX_LIST_QUOTA_ENTITIES: &str = r#"{
  "entities": [
    {
      "created_at": "2026-07-01T00:00:00.000000Z",
      "id": "quota-0192f0c4-6d1e-7a3b-9c4d-5e6f7a8b9c20",
      "name": "acme",
      "origin": "configured",
      "over_quota_ratio": 0.0,
      "parent": null,
      "path": "acme",
      "penalty": 1.0,
      "principal": null,
      "queued_count": 1,
      "quota_ucu": 1000000,
      "running_count": 0,
      "updated_at": "2026-07-01T00:00:00.000000Z",
      "usage_ucu": 0
    }
  ]
}"#;

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// The generator every rendering shares: draft 2020-12, definitions under
/// `#/schemas/`.
///
/// One generator, schemars' default *deserialize* contract, for
/// request and response types alike. A response field the server always
/// writes can therefore read as optional where serde would accept it absent.
fn generator() -> SchemaGenerator {
    let mut settings = schemars::generate::SchemaSettings::draft2020_12();
    settings.definitions_path = "/schemas".into();
    settings.meta_schema = None;
    SchemaGenerator::new(settings)
}

/// Register a schema and return its name in the definitions map.
fn register(generator: &mut SchemaGenerator, schema: SchemaRef) -> String {
    let registered = (schema.register)(generator);
    match registered.get("$ref").and_then(Value::as_str) {
        Some(reference) => reference
            .strip_prefix("#/schemas/")
            .expect("refs point into #/schemas/")
            .to_string(),
        None => {
            // An inlined type (schemars inlines some primitives): give it a
            // definition of its own so the catalogue can name it.
            let name = (schema.name)().into_owned();
            generator
                .definitions_mut()
                .insert(name.clone(), registered.to_value());
            name
        }
    }
}

fn auth_json(auth: Auth) -> Value {
    match auth {
        Auth::Public => json!({"kind": "public"}),
        Auth::Authenticated => json!({"kind": "authenticated"}),
        Auth::Verb { verb, description } => {
            json!({"kind": "verb", "verb": verb, "description": description})
        }
    }
}

fn status_json(status: Status) -> &'static str {
    match status {
        Status::Stable => "stable",
        Status::Unimplemented => "unimplemented",
    }
}

/// An operation with its request/response schemas registered, ready to be
/// rendered.
struct Rendered<'a> {
    op: &'a Operation,
    request: Option<String>,
    response: Option<(String, &'static str)>,
}

fn render_all(generator: &mut SchemaGenerator) -> Vec<Rendered<'static>> {
    operations()
        .iter()
        .map(|op| Rendered {
            op,
            request: op.request.map(|s| register(generator, s)),
            response: op
                .response
                .map(|r| (register(generator, r.schema), r.content_type)),
        })
        .collect()
}

/// The definitions map: every registered DTO plus the error envelope.
fn take_schemas(generator: &mut SchemaGenerator) -> Map<String, Value> {
    generator.subschema_for::<ErrorEnvelope>();
    generator.take_definitions(true)
}

/// Rewrite every `#/schemas/` reference to `#/components/schemas/`.
fn rewrite_refs(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, v) in map.iter_mut() {
                if key == "$ref" {
                    if let Value::String(s) = v {
                        if let Some(rest) = s.strip_prefix("#/schemas/") {
                            *s = format!("#/components/schemas/{rest}");
                        }
                    }
                } else {
                    rewrite_refs(v);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(rewrite_refs),
        _ => {}
    }
}

fn schema_ref(name: &str) -> Value {
    json!({"$ref": format!("#/components/schemas/{name}")})
}

/// The OpenAPI 3.1 rendering of the catalogue.
pub fn render_openapi() -> Value {
    let mut generator = generator();
    let rendered = render_all(&mut generator);
    let mut schemas = Value::Object(take_schemas(&mut generator));
    rewrite_refs(&mut schemas);

    let mut paths = Map::new();
    for r in &rendered {
        let op = r.op;
        // Every parameter is typed as a string; the catalogue does not
        // carry parameter schemas.
        let mut parameters: Vec<Value> = op
            .path_params
            .iter()
            .map(|p| (p, "path"))
            .chain(op.query_params.iter().map(|p| (p, "query")))
            .map(|(p, location)| {
                let mut param = json!({
                    "name": p.name,
                    "in": location,
                    "description": p.description,
                    "required": p.required,
                    "schema": {"type": "string"},
                });
                if let Some(example) = p.example {
                    param["example"] = json!(example);
                }
                param
            })
            .collect();
        if op.method == Method::Get && op.group == "events" {
            parameters.push(json!({
                "name": "Last-Event-ID",
                "in": "header",
                "description": "The id of the last batch frame processed; wins over `cursor`.",
                "required": false,
                "schema": {"type": "string"},
            }));
        }

        let mut responses = Map::new();
        let ok = match (&r.response, op.id) {
            (Some((name, content_type)), _) => {
                let mut content = Map::new();
                content.insert(
                    (*content_type).to_string(),
                    json!({"schema": schema_ref(name)}),
                );
                json!({"description": op.summary, "content": content})
            }
            (None, "metrics") => json!({
                "description": op.summary,
                "content": {"text/plain": {"schema": {"type": "string"}}},
            }),
            (None, _) => json!({"description": op.summary}),
        };
        responses.insert("200".to_string(), ok);
        for e in op.errors {
            let status = e.code.status().as_u16().to_string();
            let entry = responses.entry(status).or_insert_with(|| {
                json!({
                    "description": "",
                    "content": {"application/json": {"schema": schema_ref("ErrorEnvelope")}},
                })
            });
            let description = entry["description"].as_str().unwrap_or_default();
            let joined = if description.is_empty() {
                format!("`{}`: {}", e.code.as_str(), e.when)
            } else {
                format!("{description} / `{}`: {}", e.code.as_str(), e.when)
            };
            entry["description"] = json!(joined);
        }

        let mut operation = json!({
            "operationId": op.id,
            "summary": op.summary,
            "description": op.description,
            "tags": [op.group],
            "parameters": parameters,
            "responses": responses,
            "x-coppice-consistency": op.consistency.map(ConsistencyDefault::as_str),
            "x-coppice-auth": auth_json(op.auth),
            "x-coppice-adr": op.adr,
            "x-coppice-status": status_json(op.status),
        });
        if let Some(name) = &r.request {
            operation["requestBody"] = json!({
                "required": true,
                "content": {"application/json": {"schema": schema_ref(name)}},
            });
        }
        if matches!(op.auth, Auth::Public) {
            operation["security"] = json!([]);
        }
        let item = paths.entry(op.full_path()).or_insert_with(|| json!({}));
        item[op.method.as_str().to_ascii_lowercase()] = operation;
    }

    json!({
        "openapi": "3.1.0",
        "jsonSchemaDialect": "https://json-schema.org/draft/2020-12/schema",
        "info": {
            "title": "Coppice API",
            "version": "v1",
            "description": "The Coppice control-plane HTTP API, generated from the \
                            `coppice-api` catalogue.",
        },
        "tags": GROUPS.iter().map(|g| json!({
            "name": g.id,
            "description": format!("**{}.** {}", g.title, g.description),
        })).collect::<Vec<_>>(),
        "security": [{"bearer": []}],
        "paths": paths,
        "components": {
            "schemas": schemas,
            "securitySchemes": {
                "bearer": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT",
                    "description": "An OIDC access token (ADR 0022). Open-mode deployments \
                                    need none.",
                },
            },
        },
    })
}

/// [`render_openapi`], computed once, for the live `GET /api/v1/openapi.json`.
pub fn openapi_document() -> &'static Value {
    static DOC: OnceLock<Value> = OnceLock::new();
    DOC.get_or_init(render_openapi)
}
