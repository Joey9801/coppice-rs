//! The wire error contract (ADR 0031).
//!
//! Every failure leaving the HTTP layer is `application/json`
//! `{ "code": "...", "message": "..." }` with a fixed code → status
//! mapping. `code` is a closed vocabulary: clients switch on it, so a new
//! variant is a contract change, not a refactor.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::{ApiError, RejectionKind};

use super::COPPICE_LEADER;

/// The closed error vocabulary carried in the `code` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Synchronous validation failure (bad body, bad id syntax, bad query
    /// parameter). Retrying the identical request cannot help.
    InvalidArgument,
    /// Missing or invalid credential (ADR 0022).
    Unauthenticated,
    /// The actor's role bindings do not cover the target (ADR 0023).
    PermissionDenied,
    /// The id is well-formed but absent from the read view.
    NotFound,
    /// The command committed and apply refused it deterministically — a
    /// normal race outcome (`ApiError::Rejected`), never a server fault.
    Rejected,
    /// A write hit a follower; the `Coppice-Leader` header carries the
    /// leader hint when one is known.
    NotLeader,
    /// The request did not resolve: timeout, overload, shutdown, or a
    /// follower that cannot bound its staleness. Retryable.
    Unavailable,
    /// A provisional or reserved route (ADR 0031's table) with no backing
    /// implementation yet.
    Unimplemented,
    /// A bug. Details are logged server-side, never leaked to the body.
    Internal,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InvalidArgument => "INVALID_ARGUMENT",
            ErrorCode::Unauthenticated => "UNAUTHENTICATED",
            ErrorCode::PermissionDenied => "PERMISSION_DENIED",
            ErrorCode::NotFound => "NOT_FOUND",
            ErrorCode::Rejected => "REJECTED",
            ErrorCode::NotLeader => "NOT_LEADER",
            ErrorCode::Unavailable => "UNAVAILABLE",
            ErrorCode::Unimplemented => "UNIMPLEMENTED",
            ErrorCode::Internal => "INTERNAL",
        }
    }

    pub fn status(self) -> StatusCode {
        match self {
            ErrorCode::InvalidArgument => StatusCode::BAD_REQUEST,
            ErrorCode::Unauthenticated => StatusCode::UNAUTHORIZED,
            ErrorCode::PermissionDenied => StatusCode::FORBIDDEN,
            ErrorCode::NotFound => StatusCode::NOT_FOUND,
            ErrorCode::Rejected => StatusCode::CONFLICT,
            ErrorCode::NotLeader => StatusCode::MISDIRECTED_REQUEST,
            ErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// An error on its way out of the HTTP layer. Handlers return this (or a
/// domain error `From`-converted into it); `IntoResponse` renders the
/// status, the JSON body, and the leader-hint header.
#[derive(Debug)]
pub struct HttpError {
    pub code: ErrorCode,
    pub message: String,
    /// Set only with `ErrorCode::NotLeader`; rendered as `Coppice-Leader`.
    pub leader_hint: Option<String>,
}

impl HttpError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        HttpError {
            code,
            message: message.into(),
            leader_hint: None,
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidArgument, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    pub fn unimplemented(endpoint: &'static str) -> Self {
        Self::new(
            ErrorCode::Unimplemented,
            format!("{endpoint} is not implemented yet"),
        )
    }
}

impl HttpError {
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::PermissionDenied, message)
    }
}

/// The status a rejection carries **everywhere** — the global half of the
/// mapping, the half that is a property of the rejection itself rather than of
/// the endpoint that provoked it.
///
/// Exactly one distinction lives here: apply's ADR 0023 re-check is a 403, not
/// a 409. It has to be global, because the re-check can refuse *any* mutating
/// verb — a revocation landing between the API's pre-check and the command's
/// log position is the whole reason the re-check exists, and answering it with
/// a 409 (or worse, letting it fall through to a 500) would tell the client
/// "retry, you raced" when the truth is "you may not do this".
///
/// The three authorization-shaped rejections are deliberately **not** here:
/// `UnknownQuotaEntity` is a documented 409 on submit and on the quota upsert,
/// and only `PUT /api/v1/authorization` reads it as a malformed body. That is
/// an endpoint's judgement, so it is made at the endpoint
/// ([`authorization_error`]).
fn rejection_code(kind: RejectionKind) -> ErrorCode {
    match kind {
        RejectionKind::PermissionDenied => ErrorCode::PermissionDenied,
        RejectionKind::Other
        | RejectionKind::UnknownQuotaEntity
        | RejectionKind::InvalidAuthorization
        | RejectionKind::AuthorizationLockout => ErrorCode::Rejected,
    }
}

/// `PUT /api/v1/authorization`'s error mapping: the global one, plus the three
/// rejections that endpoint reads as a malformed request body.
///
/// A bindings list scoped to an entity that does not exist, one with an empty
/// subject, or one that would leave the cluster with no unscoped admin are not
/// races the client lost — they are documents the client got wrong, and no
/// retry of the identical body will ever land. So they are `INVALID_ARGUMENT`
/// (400), each with its own detail text, and each keeps the leading phrase of
/// the rejection it came from so the three stay distinguishable.
pub fn authorization_error(e: ApiError) -> HttpError {
    let kind = match &e {
        ApiError::Rejected(r) => RejectionKind::of(r),
        ApiError::ForwardedRejection { kind, .. } => *kind,
        _ => return e.into(),
    };
    match kind {
        RejectionKind::UnknownQuotaEntity => HttpError::invalid(format!(
            "the bindings list scopes a role to a quota entity that does not exist: {e}"
        )),
        RejectionKind::InvalidAuthorization => {
            HttpError::invalid(format!("the bindings list is malformed: {e}"))
        }
        RejectionKind::AuthorizationLockout => HttpError::invalid(format!(
            "the bindings list would lock the cluster out of its own authorization: {e}"
        )),
        RejectionKind::PermissionDenied | RejectionKind::Other => e.into(),
    }
}

impl From<ApiError> for HttpError {
    fn from(e: ApiError) -> Self {
        match e {
            ApiError::Invalid(m) => HttpError::new(ErrorCode::InvalidArgument, m),
            ApiError::Rejected(ref r) => {
                HttpError::new(rejection_code(RejectionKind::of(r)), r.to_string())
            }
            // The same mapping as the arm above, off the classification the
            // leader sent rather than off a `RejectionReason` this replica
            // never had: a rejection is a rejection whether apply refused it
            // here or on the leader this one forwarded to (ADR 0038).
            ApiError::ForwardedRejection { kind, reason } => {
                HttpError::new(rejection_code(kind), reason)
            }
            ApiError::NotLeader { leader_hint } => HttpError {
                code: ErrorCode::NotLeader,
                message: "not the leader".to_string(),
                leader_hint,
            },
            ApiError::Unavailable(m) => HttpError::new(ErrorCode::Unavailable, m),
        }
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'static str,
    message: &'a str,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let mut response = (
            self.code.status(),
            Json(ErrorBody {
                code: self.code.as_str(),
                message: &self.message,
            }),
        )
            .into_response();
        if let Some(hint) = self.leader_hint {
            if let Ok(value) = hint.parse() {
                response.headers_mut().insert(COPPICE_LEADER, value);
            }
        }
        response
    }
}
