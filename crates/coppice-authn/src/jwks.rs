//! OIDC discovery and the JWKS cache.
//!
//! The design constraint from ADR 0022 is that **no request path ever calls
//! the IdP**. Validation reads a locally cached key; the network work happens
//! on a background task ([`JwksCache::run`]) and, rarely and under a rate
//! limit, on the unknown-`kid` path. Everything here follows from that:
//!
//! - Startup with an unreachable IdP must not block serving. The cache begins
//!   empty, bearer validation fails with [`ValidateError::KeysUnavailable`]
//!   until the first fetch lands, and operator certificates are unaffected.
//! - A fetch failure never invalidates what is cached. "Existing valid tokens
//!   remain usable during a short IdP outage" is an acceptance criterion, so
//!   the previous key set is served indefinitely and the failure is a warning
//!   plus a metric, not an eviction.
//! - A token with an unknown `kid` may be a token signed by a key the IdP
//!   published after our last refresh — or garbage. One refetch per
//!   [`JwksTimings::on_demand_min_interval`] resolves the first case without
//!   letting the second turn our edge into a load generator against the IdP.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey};

use crate::metrics;

/// The path every OIDC provider serves its discovery document on.
const DISCOVERY_PATH: &str = "/.well-known/openid-configuration";

/// How long the cache waits between scheduled refreshes, how far it will back
/// off while the IdP is unreachable, and how often an unknown `kid` may
/// trigger an out-of-band fetch.
///
/// Public and constructor-injected rather than hardcoded so tests can run the
/// same code paths on a millisecond scale. Production uses [`Default`].
#[derive(Debug, Clone)]
pub struct JwksTimings {
    /// Base interval between scheduled refreshes.
    pub refresh_interval: Duration,
    /// Maximum deviation applied to `refresh_interval`, in either direction.
    /// Spreads a fleet's refreshes so a large cluster does not fetch in
    /// lockstep after a simultaneous restart.
    pub refresh_jitter: Duration,
    /// First retry delay after a failed fetch; doubles up to `backoff_max`.
    pub backoff_base: Duration,
    /// Cap on the retry delay. Bounded so recovery after a long outage is
    /// prompt rather than an hour later.
    pub backoff_max: Duration,
    /// Minimum gap between unknown-`kid` fetches.
    pub on_demand_min_interval: Duration,
}

impl Default for JwksTimings {
    fn default() -> JwksTimings {
        JwksTimings {
            refresh_interval: Duration::from_secs(600),
            refresh_jitter: Duration::from_secs(60),
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
            on_demand_min_interval: Duration::from_secs(10),
        }
    }
}

/// A verification key decoded out of the JWKS, with the algorithm the **key**
/// declares.
///
/// Storing the algorithm alongside the key is the whole alg-confusion defence:
/// validation is configured from this field, never from the token's own
/// header, so a token claiming `HS256` against an EC key is rejected by
/// `jsonwebtoken`'s algorithm check rather than being verified as an HMAC over
/// the public key.
#[derive(Clone)]
pub(crate) struct CachedKey {
    pub(crate) key: DecodingKey,
    pub(crate) alg: Algorithm,
}

#[derive(Default)]
struct KeySet {
    keys: HashMap<String, CachedKey>,
    /// `None` until the first successful fetch.
    fetched_at: Option<Instant>,
}

/// Serialises unknown-`kid` refetches and enforces their rate limit.
#[derive(Default)]
struct OnDemand {
    last_attempt: Option<Instant>,
}

/// What an unknown-`kid` refetch attempt did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnDemandOutcome {
    /// A fetch ran (successfully or not); the caller should re-check the cache.
    Fetched,
    /// The rate limiter declined; the cache is unchanged.
    Suppressed,
}

/// A JWKS keyed by `kid`, refreshed in the background and served stale on
/// failure.
pub struct JwksCache {
    http: reqwest::Client,
    /// The issuer exactly as configured — also the discovery base, with any
    /// trailing slash trimmed at URL-join time only.
    issuer: String,
    timings: JwksTimings,
    keys: RwLock<KeySet>,
    on_demand: tokio::sync::Mutex<OnDemand>,
    /// Mixed into the refresh jitter so two caches in one process (tests) do
    /// not draw identical delays from a same-nanosecond clock read.
    jitter_seq: AtomicU64,
}

impl JwksCache {
    /// A cache for `issuer` with production timings.
    pub fn new(http: reqwest::Client, issuer: String) -> Arc<JwksCache> {
        Self::with_timings(http, issuer, JwksTimings::default())
    }

    /// As [`new`](Self::new), with explicit timings.
    pub fn with_timings(
        http: reqwest::Client,
        issuer: String,
        timings: JwksTimings,
    ) -> Arc<JwksCache> {
        if issuer.starts_with("http://") {
            // Allowed, because the test fixture and local development need it,
            // but an IdP reached over plain HTTP means the key set that
            // authenticates every request is whatever the network says it is.
            tracing::warn!(
                issuer = %issuer,
                "OIDC issuer is plain http; JWKS fetches are unauthenticated and \
                 tamperable — use https outside tests"
            );
        }
        Arc::new(JwksCache {
            http,
            issuer,
            timings,
            keys: RwLock::new(KeySet::default()),
            on_demand: tokio::sync::Mutex::new(OnDemand::default()),
            jitter_seq: AtomicU64::new(0),
        })
    }

    /// The configured issuer.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// The background refresh loop. The coordinator spawns this; it returns
    /// when `shutdown` goes true.
    ///
    /// It never returns an error and never panics on an unreachable IdP: a
    /// failed fetch backs off and retries, leaving whatever is cached in place.
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        if *shutdown.borrow() {
            return;
        }
        let mut backoff = self.timings.backoff_base;
        loop {
            let delay = match self.fetch().await {
                Ok(count) => {
                    backoff = self.timings.backoff_base;
                    tracing::debug!(keys = count, issuer = %self.issuer, "refreshed the JWKS");
                    self.jittered_interval()
                }
                Err(e) => {
                    let stale_for = self.age();
                    tracing::warn!(
                        issuer = %self.issuer,
                        error = %e,
                        retry_in_ms = backoff.as_millis(),
                        cached_keys = self.key_count(),
                        stale_for_s = stale_for.map(|d| d.as_secs()),
                        "JWKS refresh failed; continuing to serve the cached keys"
                    );
                    let this = backoff;
                    backoff = (backoff * 2).min(self.timings.backoff_max);
                    this
                }
            };

            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    /// Fetch discovery + JWKS once, replacing the cache on success.
    ///
    /// Exposed for tests that want a deterministic single fetch instead of
    /// racing [`run`](Self::run)'s schedule. Production goes through `run`.
    #[doc(hidden)]
    pub async fn refresh_now(&self) -> Result<usize, JwksError> {
        self.fetch().await
    }

    /// How many keys are cached right now.
    pub fn key_count(&self) -> usize {
        self.keys.read().expect("jwks lock").keys.len()
    }

    /// How long ago the cached key set was fetched; `None` if none ever was.
    pub fn age(&self) -> Option<Duration> {
        self.keys
            .read()
            .expect("jwks lock")
            .fetched_at
            .map(|t| t.elapsed())
    }

    /// True once a fetch has succeeded at least once.
    pub(crate) fn has_keys(&self) -> bool {
        self.keys.read().expect("jwks lock").fetched_at.is_some()
    }

    pub(crate) fn lookup(&self, kid: &str) -> Option<CachedKey> {
        self.keys.read().expect("jwks lock").keys.get(kid).cloned()
    }

    /// Refetch because a token presented an unknown `kid`, unless the rate
    /// limiter says no.
    ///
    /// The lock is held across the fetch on purpose: it makes concurrent
    /// unknown-`kid` requests collapse into one in-flight fetch rather than N,
    /// and it makes "one attempt per interval" true of attempts, not just of
    /// permission checks.
    pub(crate) async fn refresh_for_unknown_kid(&self) -> OnDemandOutcome {
        let mut guard = self.on_demand.lock().await;
        if let Some(last) = guard.last_attempt {
            if last.elapsed() < self.timings.on_demand_min_interval {
                metrics::record_jwks_on_demand_suppressed();
                return OnDemandOutcome::Suppressed;
            }
        }
        guard.last_attempt = Some(Instant::now());
        metrics::record_jwks_on_demand();
        if let Err(e) = self.fetch().await {
            tracing::warn!(
                issuer = %self.issuer,
                error = %e,
                "on-demand JWKS fetch for an unknown key id failed"
            );
        }
        OnDemandOutcome::Fetched
    }

    async fn fetch(&self) -> Result<usize, JwksError> {
        let result = self.fetch_inner().await;
        match &result {
            Ok(_) => metrics::record_jwks_fetch_ok(),
            Err(_) => metrics::record_jwks_fetch_failed(),
        }
        result
    }

    async fn fetch_inner(&self) -> Result<usize, JwksError> {
        let jwks_uri = self.discover().await?;
        let response = self
            .http
            .get(&jwks_uri)
            .send()
            .await
            .map_err(|e| JwksError::Unreachable(jwks_uri.clone(), e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(JwksError::Status(jwks_uri, status.as_u16()));
        }
        let set: JwkSet = response
            .json()
            .await
            .map_err(|e| JwksError::Malformed(jwks_uri.clone(), e.to_string()))?;

        let mut keys = HashMap::new();
        for jwk in &set.keys {
            let Some(kid) = jwk.common.key_id.clone() else {
                // A key we cannot address by `kid` can never be selected: the
                // validator requires a `kid` header precisely so key selection
                // is never inferred from the token.
                tracing::debug!(issuer = %self.issuer, "ignoring a JWKS entry with no kid");
                continue;
            };
            let Some(alg) = key_algorithm(jwk) else {
                tracing::debug!(
                    issuer = %self.issuer,
                    kid = %kid,
                    "ignoring a JWKS entry whose algorithm is absent, symmetric or unsupported"
                );
                continue;
            };
            match DecodingKey::from_jwk(jwk) {
                Ok(key) => {
                    keys.insert(kid, CachedKey { key, alg });
                }
                Err(e) => {
                    tracing::debug!(
                        issuer = %self.issuer,
                        kid = %kid,
                        error = %e,
                        "ignoring an undecodable JWKS entry"
                    );
                }
            }
        }

        if keys.is_empty() && !set.keys.is_empty() {
            return Err(JwksError::Malformed(
                jwks_uri,
                "no entry carried both a kid and a supported asymmetric algorithm".to_string(),
            ));
        }

        let count = keys.len();
        let mut guard = self.keys.write().expect("jwks lock");
        guard.keys = keys;
        guard.fetched_at = Some(Instant::now());
        Ok(count)
    }

    /// Fetch the discovery document and return its `jwks_uri`.
    ///
    /// The trailing slash is trimmed for the URL join only. The configured
    /// issuer string itself stays untouched: it is what a token's `iss` is
    /// compared against, byte for byte, and normalising it here would quietly
    /// change which tokens this cluster accepts.
    async fn discover(&self) -> Result<String, JwksError> {
        let url = format!("{}{DISCOVERY_PATH}", self.issuer.trim_end_matches('/'));
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| JwksError::Unreachable(url.clone(), e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(JwksError::Status(url, status.as_u16()));
        }
        let doc: DiscoveryDocument = response
            .json()
            .await
            .map_err(|e| JwksError::Malformed(url.clone(), e.to_string()))?;
        if doc.jwks_uri.trim().is_empty() {
            return Err(JwksError::Malformed(url, "empty jwks_uri".to_string()));
        }
        Ok(doc.jwks_uri)
    }

    /// `refresh_interval ± refresh_jitter`.
    ///
    /// Deliberately not drawn from an RNG crate: the requirement is only that
    /// a fleet's refreshes spread out, which a mix of the wall clock's
    /// sub-second bits and a per-cache counter satisfies without adding a
    /// dependency for a timer wobble.
    fn jittered_interval(&self) -> Duration {
        let jitter = self.timings.refresh_jitter;
        if jitter.is_zero() {
            return self.timings.refresh_interval;
        }
        let span = (jitter.as_nanos() as u64)
            .saturating_mul(2)
            .saturating_add(1);
        let clock = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or_default();
        let seq = self.jitter_seq.fetch_add(1, Ordering::Relaxed);
        let draw = splitmix64(clock ^ seq.wrapping_mul(0x9E37_79B9_7F4A_7C15)) % span;
        self.timings
            .refresh_interval
            .saturating_add(Duration::from_nanos(draw))
            .saturating_sub(jitter)
    }
}

/// The one field of the discovery document this crate needs. Everything else
/// (authorization/token endpoints, supported scopes) belongs to the browser
/// login flow, which happens in the SPA and not here.
#[derive(Debug, serde::Deserialize)]
struct DiscoveryDocument {
    jwks_uri: String,
}

/// The signing algorithm to pin for a JWK, or `None` if the key cannot be used
/// to verify an access token.
///
/// A JWK's `alg` is optional in the wild, so it falls back to the only
/// algorithm each key type/curve can be used with. Symmetric keys and
/// `alg: none` return `None`: a shared secret in a public JWKS would let
/// anyone who can read the JWKS mint tokens, which is the alg-confusion attack
/// with the extra step removed.
fn key_algorithm(jwk: &jsonwebtoken::jwk::Jwk) -> Option<Algorithm> {
    if let Some(declared) = jwk.common.key_algorithm {
        let alg: Algorithm = declared.to_string().parse().ok()?;
        return asymmetric(alg);
    }
    match &jwk.algorithm {
        AlgorithmParameters::EllipticCurve(ec) => match ec.curve {
            EllipticCurve::P256 => Some(Algorithm::ES256),
            EllipticCurve::P384 => Some(Algorithm::ES384),
            _ => None,
        },
        AlgorithmParameters::RSA(_) => Some(Algorithm::RS256),
        AlgorithmParameters::OctetKeyPair(_) => Some(Algorithm::EdDSA),
        AlgorithmParameters::OctetKey(_) => None,
    }
}

/// `Some(alg)` only for the asymmetric algorithms.
pub(crate) fn asymmetric(alg: Algorithm) -> Option<Algorithm> {
    match alg {
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => None,
        other => Some(other),
    }
}

fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Why a key-set fetch failed. Never fatal: the caller keeps serving whatever
/// it already has.
#[derive(Debug, thiserror::Error)]
pub enum JwksError {
    /// The request did not complete (DNS, connect, TLS, timeout, or a refused
    /// redirect).
    #[error("GET {0} failed: {1}")]
    Unreachable(String, String),
    /// The endpoint answered with a non-2xx status.
    #[error("GET {0} answered HTTP {1}")]
    Status(String, u16),
    /// The endpoint answered, but not with something usable.
    #[error("the document at {0} is unusable: {1}")]
    Malformed(String, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_inside_the_configured_band() {
        let cache = JwksCache::with_timings(
            reqwest::Client::new(),
            "https://idp.example".to_string(),
            JwksTimings {
                refresh_interval: Duration::from_secs(600),
                refresh_jitter: Duration::from_secs(60),
                ..JwksTimings::default()
            },
        );
        for _ in 0..1_000 {
            let d = cache.jittered_interval();
            assert!(
                d >= Duration::from_secs(540) && d <= Duration::from_secs(660),
                "jittered interval {d:?} left the ±60s band"
            );
        }
    }

    #[test]
    fn zero_jitter_is_the_bare_interval() {
        let cache = JwksCache::with_timings(
            reqwest::Client::new(),
            "https://idp.example".to_string(),
            JwksTimings {
                refresh_interval: Duration::from_secs(30),
                refresh_jitter: Duration::ZERO,
                ..JwksTimings::default()
            },
        );
        assert_eq!(cache.jittered_interval(), Duration::from_secs(30));
    }

    #[test]
    fn symmetric_algorithms_are_never_usable_verification_keys() {
        assert!(asymmetric(Algorithm::HS256).is_none());
        assert!(asymmetric(Algorithm::HS512).is_none());
        assert_eq!(asymmetric(Algorithm::ES256), Some(Algorithm::ES256));
        assert_eq!(asymmetric(Algorithm::RS256), Some(Algorithm::RS256));
    }
}
