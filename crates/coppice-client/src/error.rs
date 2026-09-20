//! What can go wrong, and how to tell the cases apart.

/// The result of every fallible call in this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// The closed error vocabulary the server carries in an error body's `code`
/// field (ADR 0031), plus a catch-all.
///
/// Clients switch on this, so the server treats a new variant as a contract
/// change. [`ErrorCode::Other`] exists anyway: an old client must be able to
/// name a newer server's code rather than fail to decode the error at all.
/// `FromStr` never fails — an unrecognized spelling becomes `Other` — which
/// is what lets [`crate::Client`] parse every error body's `code` field
/// unconditionally.
#[derive(Debug, Clone, PartialEq, Eq, strum::Display, strum::EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum ErrorCode {
    /// Synchronous validation failure — a bad body, a bad id, a bad query
    /// parameter. Retrying the identical request cannot help.
    InvalidArgument,
    /// Missing or invalid credential.
    Unauthenticated,
    /// The caller's role bindings do not cover the target.
    PermissionDenied,
    /// The id is well-formed but absent from the read view.
    NotFound,
    /// The command committed and apply refused it deterministically — a
    /// normal race outcome, never a server fault.
    Rejected,
    /// A write reached a follower. [`Error::leader_hint`] carries where to
    /// retry, when the follower knew.
    NotLeader,
    /// The request did not resolve: a timeout, overload, shutdown, or a
    /// follower that cannot bound its staleness. Retryable.
    Unavailable,
    /// A reserved route with no backing implementation yet.
    Unimplemented,
    /// A server-side bug. Details are logged there, never leaked here.
    Internal,
    /// A code this client does not know — a newer server's vocabulary.
    #[strum(default)]
    Other(String),
}

/// Everything a call to a coordinator can fail with.
///
/// The `Display` of the three server-answered cases is deliberately stable
/// text — `api error (NOT_FOUND): no such job …` — because it is what the
/// `coppice` CLI prints to operators.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The base URL was not a URL, or carried a scheme `reqwest` will not
    /// dial.
    #[error("invalid API base URL {base:?}: {reason}")]
    InvalidBaseUrl {
        /// The offending base.
        base: String,
        /// Why it was refused.
        reason: String,
    },

    /// Building the underlying HTTP client failed — a TLS backend that could
    /// not initialize, typically.
    #[error("building the HTTP client")]
    Build(#[source] reqwest::Error),

    /// The request never produced a response: DNS, connect, TLS, or the
    /// request timeout elapsed.
    #[error("sending the request")]
    Transport(#[source] reqwest::Error),

    /// The server answered non-2xx with the ADR 0031 `{code, message}` body.
    #[error("api error ({code}): {message}{}", leader_suffix(.leader))]
    Api {
        /// The HTTP status that carried it.
        status: u16,
        /// The wire code.
        code: ErrorCode,
        /// The server's human-readable detail.
        message: String,
        /// The `Coppice-Leader` hint, present on a 421/`NOT_LEADER` when the
        /// follower knew who the leader was.
        leader: Option<String>,
    },

    /// The server answered non-2xx with something that was not an error body:
    /// a proxy's HTML, a plain-text 502, an empty body.
    #[error("api error (HTTP {status}){}{}", body_suffix(.body), leader_suffix(.leader))]
    UnexpectedStatus {
        /// The HTTP status.
        status: u16,
        /// The response body, trimmed; empty when there was none.
        body: String,
        /// The `Coppice-Leader` hint, if the response carried one.
        leader: Option<String>,
    },

    /// A 2xx body did not decode as the type this endpoint promises.
    #[error("decoding the response body")]
    Decode(#[source] serde_json::Error),

    /// A request body — a filter, a metadata map — failed this crate's own
    /// checks before it was sent.
    #[error("{0}")]
    InvalidRequest(String),

    /// A [`TokenProvider`](crate::TokenProvider) could not supply a token, so
    /// the request was never sent. The provider's own error is the source.
    ///
    /// Not retryable by [`is_retryable`](Self::is_retryable): whether asking
    /// again could work is a fact about the provider, which only the caller
    /// knows. It carries no status and no wire code — nothing reached a
    /// server.
    #[error("obtaining a credential")]
    Credential(#[source] crate::credential::BoxError),
}

/// `; retry against the leader at …`, the suffix the CLI has always printed.
fn leader_suffix(leader: &Option<String>) -> String {
    match leader {
        Some(leader) => format!("; retry against the leader at {leader}"),
        None => String::new(),
    }
}

/// `: <body>`, omitted entirely when the body was empty — so an empty 502
/// reads `api error (HTTP 502)` with no dangling colon.
fn body_suffix(body: &str) -> String {
    if body.is_empty() {
        String::new()
    } else {
        format!(": {body}")
    }
}

impl Error {
    /// The HTTP status, when the failure came from a response at all.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Api { status, .. } | Error::UnexpectedStatus { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// The wire error code, when the server sent an error body.
    pub fn code(&self) -> Option<&ErrorCode> {
        match self {
            Error::Api { code, .. } => Some(code),
            _ => None,
        }
    }

    /// The address of the current leader, when a follower refused a write and
    /// said where to go instead.
    pub fn leader_hint(&self) -> Option<&str> {
        match self {
            Error::Api { leader, .. } | Error::UnexpectedStatus { leader, .. } => leader.as_deref(),
            _ => None,
        }
    }

    /// Whether this is the server saying the thing does not exist.
    pub fn is_not_found(&self) -> bool {
        matches!(self.code(), Some(ErrorCode::NotFound))
    }

    /// Whether the caller's credential was missing, invalid, or insufficient.
    pub fn is_auth(&self) -> bool {
        matches!(
            self.code(),
            Some(ErrorCode::Unauthenticated | ErrorCode::PermissionDenied)
        )
    }

    /// Whether re-sending the identical request could plausibly succeed.
    ///
    /// True for a transport failure, for `UNAVAILABLE`, for `NOT_LEADER` (aim
    /// the retry at [`leader_hint`](Self::leader_hint)), and for a 5xx that
    /// carried no error body. False for everything the caller got wrong, and
    /// false for `REJECTED`, which is apply's deterministic refusal — the
    /// identical command will be refused again.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Transport(_) => true,
            Error::Api { code, .. } => {
                matches!(code, ErrorCode::Unavailable | ErrorCode::NotLeader)
            }
            Error::UnexpectedStatus { status, .. } => *status >= 500,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact strings the CLI has always printed. Phase-2 output must not
    /// move, so these are pinned.
    #[test]
    fn display_matches_the_cli_wording() {
        let api = Error::Api {
            status: 404,
            code: ErrorCode::NotFound,
            message: "no such job".to_string(),
            leader: None,
        };
        assert_eq!(api.to_string(), "api error (NOT_FOUND): no such job");

        let redirected = Error::Api {
            status: 421,
            code: ErrorCode::NotLeader,
            message: "not the leader".to_string(),
            leader: Some("10.0.0.2:7070".to_string()),
        };
        assert_eq!(
            redirected.to_string(),
            "api error (NOT_LEADER): not the leader; retry against the leader at 10.0.0.2:7070"
        );

        let text = Error::UnexpectedStatus {
            status: 502,
            body: "bad gateway".to_string(),
            leader: None,
        };
        assert_eq!(text.to_string(), "api error (HTTP 502): bad gateway");

        let empty = Error::UnexpectedStatus {
            status: 502,
            body: String::new(),
            leader: None,
        };
        assert_eq!(empty.to_string(), "api error (HTTP 502)");
    }

    #[test]
    fn an_unknown_code_keeps_its_spelling() {
        let code: ErrorCode = "RESOURCE_EXHAUSTED".parse().unwrap();
        assert_eq!(code, ErrorCode::Other("RESOURCE_EXHAUSTED".to_string()));
        assert_eq!(code.to_string(), "RESOURCE_EXHAUSTED");
    }

    #[test]
    fn retryability_follows_the_code() {
        let rejected = Error::Api {
            status: 409,
            code: ErrorCode::Rejected,
            message: String::new(),
            leader: None,
        };
        assert!(!rejected.is_retryable());
        let unavailable = Error::Api {
            status: 503,
            code: ErrorCode::Unavailable,
            message: String::new(),
            leader: None,
        };
        assert!(unavailable.is_retryable());
    }
}
