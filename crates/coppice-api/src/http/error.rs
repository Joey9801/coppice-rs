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

use crate::ApiError;

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

impl From<ApiError> for HttpError {
    fn from(e: ApiError) -> Self {
        match e {
            ApiError::Invalid(m) => HttpError::new(ErrorCode::InvalidArgument, m),
            ApiError::Rejected(r) => HttpError::new(ErrorCode::Rejected, r.to_string()),
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
