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
//! ## The boundary is the namespace
//!
//! [`authenticate`] is layered over the **whole** `/api/v1` router
//! (`super::routes`) — every route it carries *and* its 404 fallback. Nothing
//! under that prefix reaches a handler, a method-not-allowed, or a
//! route-not-found without having been through the chain first. That is a
//! property of the namespace rather than of how any one route was registered,
//! so a route added tomorrow is authenticated by construction and an
//! `/api/v1` path that matches no route at all still answers 401 rather than
//! leaking its own existence (or non-existence) to an anonymous caller.
//!
//! The exceptions are the closed, explicit table in [`UNAUTHENTICATED_ROUTES`]
//! — exact method-and-path pairs, checked here and nowhere else.

use std::sync::Arc;

use axum::extract::{FromRequestParts, Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::http::Method;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use coppice_authn::{Actor, AuthnChain, Credentials, Presentation};

use super::enroll::PeerCertificates;
use super::error::{ErrorCode, HttpError};

/// The resolved identity of one request, as handlers see it.
///
/// A newtype rather than extracting [`Actor`] directly: the actor is a foreign
/// type (it is [`coppice_state::Actor`], the very value PR3 attaches to the
/// commands these handlers propose), and the wrapper is also where the "a
/// handler behind the layer always has one" invariant is stated once.
#[derive(Debug, Clone)]
pub struct RequestActor(pub Actor);

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for RequestActor {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.extensions.get::<Actor>() {
            Some(actor) => Ok(RequestActor(actor.clone())),
            // Not reachable through the router: every `/api/v1` route sits
            // behind [`authenticate`], which inserts the extension or refuses
            // the request. Reaching it means a handler that needs an identity
            // was listed in [`UNAUTHENTICATED_ROUTES`] — a bug, and one that
            // must not be served as if the caller were anonymous.
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

/// The presentation-only claims (ADR 0022) a bearer token carried, as
/// handlers see them.
///
/// Never store this and never let it near [`coppice_state::Actor`] or a
/// proposed command — `name` and `email` exist only so `GET /api/v1/session`
/// can hand them back to the caller who presented them. Falls back to
/// [`Presentation::default`] (both fields `None`) if the layer did not insert
/// one, which in practice only the operator-certificate and open-mode paths
/// do deliberately; a missing extension is not an error here the way a
/// missing [`Actor`] is, because a request with no presentation claims is
/// entirely ordinary.
// Not yet read by any handler — the session DTO wiring that consumes this is
// the main session's follow-up work; this type is the seam it extracts
// through. Exercised directly in this module's own tests below.
#[derive(Debug, Clone)]
pub(super) struct RequestPresentation(pub Presentation);

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for RequestPresentation {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(RequestPresentation(
            parts
                .extensions
                .get::<Presentation>()
                .cloned()
                .unwrap_or_default(),
        ))
    }
}

/// The `/api/v1` requests that are served **without** authentication: exact
/// `(method, path)` pairs, and the only ones in the system.
///
/// Paths are written **without** the `/api/v1` prefix, because that is what
/// this middleware sees. It is layered inside the nested router, and axum
/// strips a nest's prefix from the request URI before the nested service —
/// middleware included — ever runs; the untouched original is still available
/// as `axum::extract::OriginalUri`, which nothing here needs. The
/// `unauthenticated_routes_are_matched_on_the_prefix_stripped_path` test pins
/// this down so the table cannot silently stop matching.
///
/// Exactly two entries, each for a reason that is about the credential itself:
///
/// - `POST /enroll` is a machine's certless first contact, authenticated by
///   its own role-scoped enrollment token (ADR 0037 §4). An enrollee has no
///   user identity to present, and the endpoint's refusal contract — one
///   constant body, no validity oracle — is not the API's 401.
/// - `GET /auth/config` is how a client discovers *how to authenticate*.
///   Requiring a credential to learn which credential to obtain would be a
///   loop with no entry point.
///
/// The method is part of the key on purpose: `POST /auth/config` and
/// `GET /enroll` are not these endpoints, so they are authenticated like any
/// other request and answer 401 — a caller learns that a method is wrong only
/// after it has proved who it is.
pub const UNAUTHENTICATED_ROUTES: &[(Method, &str)] =
    &[(Method::GET, "/auth/config"), (Method::POST, "/enroll")];

/// Whether this request is one of the [`UNAUTHENTICATED_ROUTES`].
fn is_unauthenticated_route(method: &Method, path: &str) -> bool {
    UNAUTHENTICATED_ROUTES
        .iter()
        .any(|(m, p)| m == method && *p == path)
}

/// The authentication middleware: run the chain, insert the [`Actor`], or
/// answer 401.
///
/// Layered over the entire `/api/v1` router — its routes *and* its fallback —
/// so authentication is a property of the namespace and not of route
/// registration. The only way past it without a credential is the
/// [`UNAUTHENTICATED_ROUTES`] table, checked first; such a request runs on with
/// no `Actor` in its extensions, which is exactly why neither of those two
/// handlers extracts one.
///
/// State is the chain itself (`from_fn_with_state`), separate from the
/// router's `ControlPlane` state — authentication has nothing to ask of the
/// replicated state machine.
pub async fn authenticate(
    State(chain): State<Arc<AuthnChain>>,
    mut request: Request,
    next: Next,
) -> Response {
    if is_unauthenticated_route(request.method(), request.uri().path()) {
        return next.run(request).await;
    }

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
        Ok(authenticated) => {
            request.extensions_mut().insert(authenticated.actor);
            // Inserted alongside the actor, never folded into it: this is the
            // presentation-only seam `GET /api/v1/session` reads through
            // `RequestPresentation`. Every path through the chain inserts one
            // (bearer fills it, the others insert `Presentation::default()`),
            // so `RequestPresentation`'s own fallback is belt-and-braces.
            request.extensions_mut().insert(authenticated.presentation);
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

#[cfg(test)]
mod tests {
    use axum::extract::FromRequestParts;
    use axum::http::Request;

    use super::*;

    /// A request that never passed through [`authenticate`] (or one that did,
    /// on the certificate/open path, which inserts
    /// `Presentation::default()` anyway) still yields a usable
    /// `RequestPresentation` rather than a rejection — `name`/`email` are
    /// optional display data, not a precondition the way `RequestActor`'s
    /// `Actor` is.
    #[tokio::test]
    async fn request_presentation_falls_back_to_default_when_absent() {
        let request = Request::builder().body(()).expect("build a bare request");
        let (mut parts, ()) = request.into_parts();

        let RequestPresentation(presentation) =
            RequestPresentation::from_request_parts(&mut parts, &())
                .await
                .expect("the fallback never errors");

        assert_eq!(presentation, Presentation::default());
    }

    /// The happy path: the middleware's own insertion is what a handler
    /// downstream of it actually sees.
    #[tokio::test]
    async fn request_presentation_reads_the_inserted_extension() {
        let request = Request::builder().body(()).expect("build a bare request");
        let (mut parts, ()) = request.into_parts();
        parts.extensions.insert(Presentation {
            name: Some("Alice Example".to_string()),
            email: Some("alice@example.com".to_string()),
        });

        let RequestPresentation(presentation) =
            RequestPresentation::from_request_parts(&mut parts, &())
                .await
                .expect("the fallback never errors");

        assert_eq!(presentation.name.as_deref(), Some("Alice Example"));
        assert_eq!(presentation.email.as_deref(), Some("alice@example.com"));
    }
}
