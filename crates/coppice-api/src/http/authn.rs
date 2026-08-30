//! The authentication layer (ADR 0022): credentials off the request, an
//! [`Actor`] into its extensions.
//!
//! This module is the seam ADR 0031 asks for and nothing more. Every decision
//! about *what* a credential proves lives in `coppice-authn`'s
//! [`AuthnChain`]; what lives here is the axum plumbing on either side of it —
//! reading the `Authorization` header and the TLS peer certificate, and
//! rendering the one refusal ([`ErrorCode::Unauthenticated`]) the contract
//! allows.
//!
//! The layer is applied to the **protected** half of `/api/v1` only
//! (`super::routes`): `POST /enroll` carries its own bearer-token machine auth
//! (ADR 0037 §4) and `GET /auth/config` is what a client reads *before* it can
//! obtain a credential at all.

use std::sync::Arc;

use axum::extract::{FromRequestParts, Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use coppice_authn::{Actor, AuthnChain, Credentials};

use super::enroll::PeerCertificates;
use super::error::{ErrorCode, HttpError};

/// The resolved identity of one request, as handlers see it.
///
/// A newtype rather than extracting `coppice_authn::Actor` directly: the actor
/// is a foreign type, and the wrapper is also where the "a handler behind the
/// layer always has one" invariant is stated once.
#[derive(Debug, Clone)]
pub struct RequestActor(pub Actor);

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for RequestActor {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.extensions.get::<Actor>() {
            Some(actor) => Ok(RequestActor(actor.clone())),
            // Not reachable through the router: every route that extracts this
            // sits behind [`authenticate`], which inserts the extension or
            // refuses the request. Reaching it means a route was mounted on the
            // public half by mistake — a bug, and one that must not be served
            // as if the caller were anonymous.
            None => {
                tracing::error!("a route behind the authn layer saw no resolved actor");
                Err(HttpError::new(
                    ErrorCode::Internal,
                    "the request carried no resolved actor",
                ))
            }
        }
    }
}

/// The authentication middleware: run the chain, insert the [`Actor`], or
/// answer 401.
///
/// State is the chain itself (`from_fn_with_state`), separate from the
/// router's `ControlPlane` state — authentication has nothing to ask of the
/// replicated state machine.
pub async fn authenticate(
    State(chain): State<Arc<AuthnChain>>,
    mut request: Request,
    next: Next,
) -> Response {
    let bearer = request.headers().get(AUTHORIZATION).map(credential_value);
    let peer_leaf = request
        .extensions()
        .get::<PeerCertificates>()
        // rustls presents the peer chain end-entity first, so the leaf — the
        // certificate whose key the peer proved it holds — is the first
        // element (`clientedge::serve` inserts the chain verbatim).
        .and_then(|certs| certs.0.first())
        .map(|der| der.as_slice());

    let creds = Credentials {
        bearer: bearer.as_deref(),
        peer_leaf_der: peer_leaf,
    };

    match chain.authenticate(creds).await {
        Ok(actor) => {
            request.extensions_mut().insert(actor);
            next.run(request).await
        }
        // The `Unauthenticated` display text names the mechanism that failed
        // and why, and never the credential itself (that invariant is the
        // authn crate's, asserted there).
        Err(e) => HttpError::new(ErrorCode::Unauthenticated, e.to_string()).into_response(),
    }
}

/// The credential an `Authorization` header carries, as the chain should judge
/// it.
///
/// A well-formed `Bearer <token>` yields the token, scheme stripped and the
/// scheme matched case-insensitively (RFC 7235 makes it case-insensitive, and
/// clients do send `bearer`).
///
/// Anything else — another scheme, a bare value with no scheme — yields the
/// header value **unchanged** rather than nothing at all. That keeps the
/// posture decision where it belongs: in the OIDC posture the chain judges it,
/// finds it is not a JWT, and refuses (a presented-and-invalid credential is
/// never silently downgraded); in open mode, which has no bearer mechanism at
/// all, it is ignored exactly like a well-formed token would be. A header whose
/// bytes are not UTF-8 cannot be a credential in any spelling, so it presents
/// as the empty one, which the same two rules then judge.
fn credential_value(value: &axum::http::HeaderValue) -> String {
    let Ok(raw) = value.to_str() else {
        return String::new();
    };
    match raw.split_once(' ') {
        Some((scheme, rest)) if scheme.eq_ignore_ascii_case("bearer") => rest.trim().to_string(),
        _ => raw.to_string(),
    }
}
