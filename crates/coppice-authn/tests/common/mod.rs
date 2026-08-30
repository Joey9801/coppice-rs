//! Shared scaffolding for the authn integration tests.
//!
//! Compiled separately into each test binary, so not every helper is used by
//! every one of them.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use coppice_authn::{default_http_client, JwksCache, JwksTimings, OidcConfig, Validator};
use coppice_testkit::oidc::FakeIdp;

/// The audience every fixture-issued token is addressed to.
pub const AUDIENCE: &str = "coppice-api";

/// The OIDC config the tests validate against.
pub fn config(issuer: &str) -> OidcConfig {
    OidcConfig {
        issuer: issuer.to_string(),
        client_id: "coppice-web".to_string(),
        audience: AUDIENCE.to_string(),
    }
}

/// Timings for tests that drive the cache by hand: no background loop runs, so
/// only the on-demand limit matters, and it stays at a value long enough that
/// a second unknown-`kid` token inside one test is deterministically
/// suppressed.
pub fn manual_timings() -> JwksTimings {
    JwksTimings {
        refresh_interval: Duration::from_secs(3_600),
        refresh_jitter: Duration::ZERO,
        backoff_base: Duration::from_millis(20),
        backoff_max: Duration::from_millis(50),
        on_demand_min_interval: Duration::from_secs(30),
    }
}

/// A cache and validator wired to `idp`, with one successful fetch already
/// done — the steady state most tests want.
pub async fn ready_validator(idp: &FakeIdp) -> (Arc<JwksCache>, Validator) {
    let (cache, validator) = validator_with(idp, manual_timings());
    cache
        .refresh_now()
        .await
        .expect("the fixture IdP serves its key set");
    (cache, validator)
}

/// A cache and validator wired to `idp` with no fetch yet performed.
pub fn validator_with(idp: &FakeIdp, timings: JwksTimings) -> (Arc<JwksCache>, Validator) {
    let cache = JwksCache::with_timings(default_http_client(), idp.issuer(), timings);
    let validator = Validator::new(Arc::clone(&cache), config(&idp.issuer()));
    (cache, validator)
}

/// Poll `check` until it holds or the deadline passes.
pub async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}
