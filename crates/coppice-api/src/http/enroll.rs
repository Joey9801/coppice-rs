//! `POST /api/v1/enroll` — certless first contact (ADR 0037 §4).
//!
//! This is the one route on the public listener that a machine with no
//! certificate and no user session may call, and the only thing standing
//! between it and the cluster's CA is a role-scoped bearer token. The route
//! therefore carries its own hardening, all of it *before* the body is even
//! read, and its own state — an [`EnrollEndpoint`] captured by the router the
//! way [`MetricsEndpoint`](super::MetricsEndpoint) and
//! [`ReadyzEndpoint`](super::ReadyzEndpoint) are, rather than another method on
//! [`ControlPlane`](crate::ControlPlane): issuing certificates is not a control
//! plane read or write, and the daemon behind it needs the CA key, the leader's
//! address, and its own machine identity, none of which belong in that trait.
//!
//! The rules this module exists to enforce:
//!
//! - **The token never appears anywhere but the request.** It is read from the
//!   `Authorization: Bearer` header or the redacted body field, *never* a query
//!   parameter (nothing here inspects `uri.query()`), and no value carrying it
//!   is ever a tracing field — [`EnrollCall`]'s `Debug` redacts it, and the
//!   handler logs outcomes without it.
//! - **Authentication failure is uniform.** Missing credential, unknown token,
//!   revoked, expired, wrong role, and a request arriving with a client
//!   certificate all produce [`REFUSED_BODY`] with the same status. Callers
//!   below (the enrollment core) already collapse their four cases into one;
//!   this collapses the rest onto the same response, byte for byte.
//! - **Limits precede parsing.** Concurrency and rate limits are checked before
//!   the body is read, and the body is read under a hard byte cap, so no
//!   unauthenticated caller can make this endpoint parse a CSR — let alone hash
//!   a token — at a rate it chooses.
//! - **Nothing is ever a redirect, a cookie, or a CORS grant.** A client
//!   carrying a token must never be told to re-send it elsewhere (a follower
//!   proxies internally instead, ADR 0037 §4), no `Set-Cookie` is emitted, and
//!   no `Access-Control-*` header exists for a browser to act on.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::Request;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use coppice_core::id::{MachineId, NodeId};
use coppice_enroll::{EnrollRequest, EnrollResponse};

/// Largest enrollment body accepted. A CSR plus two ids is a couple of
/// kilobytes; 64 KiB is generous and still refuses a body worth buffering.
pub const MAX_ENROLL_BODY: usize = 64 * 1024;

/// How many enrollments may be in flight on one coordinator at once. Each one
/// hashes a token per live candidate (argon2) and signs a certificate, so this
/// is deliberately small: it is the bound on how much CPU an unauthenticated
/// caller can occupy.
const MAX_CONCURRENT_ENROLLMENTS: usize = 8;

/// Sustained enrollment attempts per second, and the burst allowance. Sized for
/// a fleet booting together (a burst of enrollments is the normal case) while
/// making a brute-force scan of the token space hopeless.
const RATE_PER_SECOND: f64 = 10.0;
const RATE_BURST: f64 = 20.0;

/// The single authentication-failure body. One constant, rendered for every
/// refused credential, so no caller can distinguish "no token" from "revoked
/// token" from "right token, wrong role" (ADR 0037 §4: no validity oracle).
/// It follows the ADR 0031 error contract like every other body this crate
/// serves — uniformity is about it never *varying*, not about it being exotic.
pub const REFUSED_BODY: &str = r#"{"code":"UNAUTHENTICATED","message":"enrollment refused"}"#;

/// The body for a request shed by the concurrency cap.
const BUSY_BODY: &str = r#"{"code":"UNAVAILABLE","message":"enrollment is busy; retry shortly"}"#;

/// The body for a request shed by the rate limiter.
const RATE_LIMITED_BODY: &str =
    r#"{"code":"UNAVAILABLE","message":"enrollment rate limit exceeded; retry shortly"}"#;

/// The DER-encoded client certificate chain a TLS peer presented, when it
/// presented one.
///
/// The client listener requests but never requires client certificates
/// (ADR 0037 §4), so this extension is present only for the connections that
/// offered one — today's operator-profile break-glass certificates. No route
/// *authorizes* on it yet; it is plumbed so ADR 0022's operator authentication
/// can layer on without touching the serving path again. `/enroll` is the one
/// route that reads it, and reads it only to refuse: enrollment is certless
/// first contact, and anything with a certificate should be renewing on the
/// machine plane instead.
#[derive(Debug, Clone)]
pub struct PeerCertificates(pub Arc<Vec<Vec<u8>>>);

/// One enrollment request as the endpoint's owner sees it.
///
/// `Debug` is hand-written and redacts the token: this is the value most likely
/// to reach a log line or an error context by accident, and it must be inert if
/// it does.
pub struct EnrollCall {
    /// The bearer secret, from the header or the redacted body field.
    pub token: String,
    /// PEM-encoded certificate signing request.
    pub csr_pem: String,
    /// The node id an agent-role enrollee claims (ADR 0011).
    pub node_id: Option<NodeId>,
    /// The machine identity a coordinator-role enrollee minted for itself.
    pub machine_id: Option<MachineId>,
}

impl fmt::Debug for EnrollCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnrollCall")
            .field("token", &"<redacted>")
            .field("csr_pem_len", &self.csr_pem.len())
            .field("node_id", &self.node_id)
            .field("machine_id", &self.machine_id)
            .finish()
    }
}

/// Why an enrollment did not produce a certificate.
///
/// [`Unauthorized`](EnrollRefusal::Unauthorized) is the only authentication
/// outcome and carries nothing: implementors must map every credential failure
/// onto it, and this module renders it identically every time.
#[derive(Debug)]
pub enum EnrollRefusal {
    /// The uniform authentication failure.
    Unauthorized,
    /// A malformed request — an unparseable CSR, a missing claim. Never a
    /// statement about the credential.
    BadRequest(String),
    /// This replica cannot serve the enrollment right now (no leader known, the
    /// proxy hop failed, the cluster has not formed).
    Unavailable(String),
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The daemon-owned source of issued certificates, plus this route's limits.
///
/// The callback is what the coordinator implements: verify the token, sign on
/// the leader (or proxy there), and hand back the leaf. Everything above it —
/// limits, uniform failures, header handling — lives here so no implementor can
/// get the hardening subtly wrong.
#[derive(Clone)]
pub struct EnrollEndpoint {
    issue:
        Arc<dyn Fn(EnrollCall) -> BoxFuture<Result<EnrollResponse, EnrollRefusal>> + Send + Sync>,
    limits: Arc<Limits>,
}

impl EnrollEndpoint {
    /// Build the endpoint over the daemon's issuing callback.
    pub fn new<F, Fut>(issue: F) -> EnrollEndpoint
    where
        F: Fn(EnrollCall) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<EnrollResponse, EnrollRefusal>> + Send + 'static,
    {
        EnrollEndpoint {
            issue: Arc::new(move |call| Box::pin(issue(call))),
            limits: Arc::new(Limits::new()),
        }
    }

    /// An endpoint for tests and embedders with nothing behind it: every
    /// request is the uniform refusal, so the route exists and answers without
    /// a daemon attached.
    pub fn detached_for_tests() -> EnrollEndpoint {
        EnrollEndpoint::new(|_call| async { Err(EnrollRefusal::Unauthorized) })
    }

    /// Handle one `POST /api/v1/enroll`.
    pub(crate) async fn handle(&self, request: Request) -> Response {
        // 1. A request that presented a client certificate is refused before
        //    anything else: `/enroll` is certless first contact, and a machine
        //    holding a leaf renews on the machine plane (ADR 0037 §4). The
        //    refusal is the uniform one — a caller learns nothing from it.
        if request.extensions().get::<PeerCertificates>().is_some() {
            return refused();
        }

        // 2. Rate limit, then concurrency cap. Both before the body is read, so
        //    a flood costs one clock read and one atomic, never a parse.
        if !self.limits.bucket.allow() {
            return static_json(StatusCode::TOO_MANY_REQUESTS, RATE_LIMITED_BODY);
        }
        let Ok(_permit) = self.limits.inflight.clone().try_acquire_owned() else {
            return static_json(StatusCode::SERVICE_UNAVAILABLE, BUSY_BODY);
        };

        // 3. The header token, read before the body so a request that carries
        //    it there never depends on the body parsing at all.
        let header_token = bearer_token(request.headers().get(header::AUTHORIZATION));

        let (_, body) = request.into_parts();
        let bytes = match axum::body::to_bytes(body, MAX_ENROLL_BODY).await {
            Ok(bytes) => bytes,
            Err(_) => {
                return static_json(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    r#"{"code":"INVALID_ARGUMENT","message":"enrollment request body is too large"}"#,
                );
            }
        };

        // 4. Only now is anything parsed.
        let Ok(body) = serde_json::from_slice::<EnrollRequest>(&bytes) else {
            return static_json(
                StatusCode::BAD_REQUEST,
                r#"{"code":"INVALID_ARGUMENT","message":"enrollment request body is not valid JSON for this endpoint"}"#,
            );
        };

        // The header wins when both carry a token; a request with neither is
        // the same refusal as a request with a wrong one.
        let Some(token) = header_token.or(body.token) else {
            return refused();
        };

        let call = EnrollCall {
            token,
            csr_pem: body.csr_pem,
            node_id: body.node_id,
            machine_id: body.machine_id,
        };

        match (self.issue)(call).await {
            Ok(issued) => (StatusCode::OK, axum::Json(issued)).into_response(),
            Err(EnrollRefusal::Unauthorized) => refused(),
            Err(EnrollRefusal::BadRequest(message)) => {
                super::HttpError::new(super::ErrorCode::InvalidArgument, message).into_response()
            }
            Err(EnrollRefusal::Unavailable(message)) => {
                super::HttpError::new(super::ErrorCode::Unavailable, message).into_response()
            }
        }
    }
}

/// The per-endpoint admission limits.
struct Limits {
    inflight: Arc<tokio::sync::Semaphore>,
    bucket: TokenBucket,
}

impl Limits {
    fn new() -> Limits {
        Limits {
            inflight: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_ENROLLMENTS)),
            bucket: TokenBucket::new(RATE_PER_SECOND, RATE_BURST),
        }
    }
}

/// A plain token bucket: `rate` tokens accrue per second up to `burst`, one
/// spent per admitted request.
///
/// Hand-rolled rather than pulled in: the workspace has no tower-http middleware
/// stack, and the whole policy is four lines of arithmetic under a mutex that is
/// held for nanoseconds on a path already bounded to a handful of requests per
/// second.
struct TokenBucket {
    rate: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl TokenBucket {
    fn new(rate: f64, burst: f64) -> TokenBucket {
        TokenBucket {
            rate,
            burst,
            state: Mutex::new((burst, Instant::now())),
        }
    }

    fn allow(&self) -> bool {
        self.allow_at(Instant::now())
    }

    fn allow_at(&self, now: Instant) -> bool {
        let mut state = self.state.lock().expect("enroll rate limiter poisoned");
        let (tokens, last) = *state;
        let elapsed = now.saturating_duration_since(last).as_secs_f64();
        let refilled = (tokens + elapsed * self.rate).min(self.burst);
        if refilled < 1.0 {
            *state = (refilled, now);
            return false;
        }
        *state = (refilled - 1.0, now);
        true
    }
}

/// The uniform authentication failure. Every credential outcome renders exactly
/// this — same status, same bytes, same headers.
fn refused() -> Response {
    static_json(StatusCode::UNAUTHORIZED, REFUSED_BODY)
}

/// A fixed JSON body with the JSON content type and nothing else: no cookie, no
/// CORS header, no location.
fn static_json(status: StatusCode, body: &'static str) -> Response {
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        body,
    )
        .into_response()
}

/// The bearer secret from an `Authorization` header, if it is a well-formed
/// non-empty `Bearer` credential. Anything else is treated as absent, which
/// lands on the same uniform refusal.
fn bearer_token(value: Option<&HeaderValue>) -> Option<String> {
    let raw = value?.to_str().ok()?;
    let (scheme, credential) = raw.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let credential = credential.trim();
    if credential.is_empty() {
        None
    } else {
        Some(credential.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_bearer_header_yields_its_credential_and_nothing_else_does() {
        let bearer = HeaderValue::from_static("Bearer cpk_abc");
        assert_eq!(bearer_token(Some(&bearer)).as_deref(), Some("cpk_abc"));

        // Case-insensitive scheme, per RFC 7235.
        let lower = HeaderValue::from_static("bearer cpk_abc");
        assert_eq!(bearer_token(Some(&lower)).as_deref(), Some("cpk_abc"));

        for absent in ["Basic cpk_abc", "Bearer", "Bearer   ", "cpk_abc"] {
            let header = HeaderValue::from_str(absent).unwrap();
            assert!(
                bearer_token(Some(&header)).is_none(),
                "{absent:?} is not a bearer credential"
            );
        }
        assert!(bearer_token(None).is_none());
    }

    #[test]
    fn the_call_debug_never_prints_the_token() {
        let call = EnrollCall {
            token: "cpk_super_secret".to_string(),
            csr_pem: "-----BEGIN CERTIFICATE REQUEST-----".to_string(),
            node_id: None,
            machine_id: None,
        };
        let rendered = format!("{call:?}");
        assert!(!rendered.contains("cpk_"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }

    #[test]
    fn the_bucket_admits_a_burst_then_refills_at_its_rate() {
        let bucket = TokenBucket::new(RATE_PER_SECOND, RATE_BURST);
        let start = Instant::now();
        for i in 0..RATE_BURST as usize {
            assert!(bucket.allow_at(start), "burst token {i} must be admitted");
        }
        assert!(
            !bucket.allow_at(start),
            "the burst is exhausted within the same instant"
        );

        // A tenth of a second buys exactly one token back at 10/s.
        let later = start + Duration::from_millis(100);
        assert!(bucket.allow_at(later));
        assert!(!bucket.allow_at(later));
    }

    #[test]
    fn the_bucket_never_accrues_past_its_burst() {
        let bucket = TokenBucket::new(RATE_PER_SECOND, RATE_BURST);
        let start = Instant::now();
        let much_later = start + Duration::from_secs(3600);
        for _ in 0..RATE_BURST as usize {
            assert!(bucket.allow_at(much_later));
        }
        assert!(
            !bucket.allow_at(much_later),
            "an hour idle still only buys the burst"
        );
    }
}
