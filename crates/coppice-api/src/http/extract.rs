//! Shared extractors and response parts that make the ADR 0031 contract
//! mechanical rather than per-handler discipline.
//!
//! Every handler — including the `UNIMPLEMENTED` stubs — extracts its path
//! id through [`IdPath`] and its read parameters through [`ReadQuery`], so
//! a malformed id or a bogus `?consistency=` is `INVALID_ARGUMENT` on every
//! route from day one, and swapping a stub for a real handler cannot forget
//! the validation. Read handlers attach their staleness metadata by
//! returning [`ReadIndexes`] in their response tuple.

use std::convert::Infallible;
use std::fmt::Display;
use std::str::FromStr;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{FromRequestParts, Path, Query};
use axum::http::request::Parts;
use axum::response::{IntoResponseParts, ResponseParts};

use super::error::HttpError;
use super::read::ReadParams;
use super::{COPPICE_APPLIED_INDEX, COPPICE_COMMITTED_INDEX};

/// A typed id taken from the single path parameter (`/jobs/:job` →
/// [`JobId`](coppice_core::id::JobId), etc.). Parse failure — wrong prefix,
/// not a uuid — is `INVALID_ARGUMENT`, never `NOT_FOUND` (ADR 0031).
pub struct IdPath<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequestParts<S> for IdPath<T>
where
    S: Send + Sync,
    T: FromStr + Send,
    T::Err: Display,
{
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path(raw) = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(|e: PathRejection| HttpError::invalid(e.body_text()))?;
        raw.parse()
            .map(IdPath)
            .map_err(|e: T::Err| HttpError::invalid(e.to_string()))
    }
}

/// The ADR 0007 read parameters, with rejection in the JSON error contract:
/// `?consistency=bogus` is `INVALID_ARGUMENT`, not a transport-flavored 400.
/// Unknown query parameters are ignored (endpoints layer their own filters
/// on top with their own extractors).
pub struct ReadQuery(pub ReadParams);

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for ReadQuery {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(params) = Query::<ReadParams>::from_request_parts(parts, state)
            .await
            .map_err(|e: QueryRejection| HttpError::invalid(e.body_text()))?;
        Ok(ReadQuery(params))
    }
}

/// The staleness metadata every read response carries (ADR 0007/0031).
///
/// Returned as part of a handler's response tuple —
/// `(ReadIndexes { .. }, Json(body))` — so the headers are typed and
/// uniform instead of hand-inserted per handler.
#[derive(Debug, Clone, Copy)]
pub struct ReadIndexes {
    /// Applied index of the view the read was served from.
    pub applied_index: u64,
    /// The serving replica's last-known committed index.
    pub committed_index: u64,
}

impl IntoResponseParts for ReadIndexes {
    type Error = Infallible;

    fn into_response_parts(self, mut res: ResponseParts) -> Result<ResponseParts, Self::Error> {
        res.headers_mut()
            .insert(COPPICE_APPLIED_INDEX, self.applied_index.into());
        res.headers_mut()
            .insert(COPPICE_COMMITTED_INDEX, self.committed_index.into());
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    #[test]
    fn read_indexes_render_as_the_contract_headers() {
        let response = (
            ReadIndexes {
                applied_index: 41,
                committed_index: 42,
            },
            "body",
        )
            .into_response();
        assert_eq!(response.headers()[COPPICE_APPLIED_INDEX], "41");
        assert_eq!(response.headers()[COPPICE_COMMITTED_INDEX], "42");
    }
}
