//! The crate's metric surface, following the repo-wide
//! `describe_metrics()` / `gather_metrics()` module pattern: the coordinator's
//! crate-root registration calls these two functions and knows nothing about
//! what is inside.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// One counter, labelled by authentication mechanism and outcome. Labels
/// rather than a name per combination: the interesting queries
/// ("what fraction of requests fail to authenticate", "is anyone still using
/// operator certs") are sums across one label.
const AUTH_OUTCOMES: &str = "authn_outcomes_total";

/// JWKS fetches that returned a usable key set.
const JWKS_FETCHES_OK: &str = "authn_jwks_fetches_total";
/// JWKS fetches that failed. A non-zero rate here with a flat
/// [`JWKS_STALENESS`] means the cache is serving stale keys, which is the
/// designed behaviour during an IdP outage — alert on the staleness, not on
/// this counter alone.
const JWKS_FETCHES_FAILED: &str = "authn_jwks_fetch_failures_total";
/// Fetches triggered by a token presenting an unknown `kid`, rather than by
/// the refresh interval.
const JWKS_ON_DEMAND: &str = "authn_jwks_on_demand_fetches_total";
/// Unknown-`kid` refetches that the rate limiter suppressed. A large number
/// here means someone is throwing garbage tokens at the edge — and that the
/// IdP is *not* being stampeded on their behalf.
const JWKS_ON_DEMAND_SUPPRESSED: &str = "authn_jwks_on_demand_suppressed_total";
/// Age of the newest successfully fetched key set.
const JWKS_STALENESS: &str = "authn_jwks_age_seconds";

/// Unix seconds of the last successful JWKS fetch; 0 = never fetched.
///
/// A process-global rather than a field on the cache because
/// [`gather_metrics`] takes no arguments by the pattern's contract, and a
/// coordinator runs exactly one JWKS cache. If a second cache is ever
/// constructed in one process the gauge reports whichever fetched last, which
/// is the right answer for the only question the gauge is asked ("is anything
/// still reaching the IdP").
static LAST_JWKS_SUCCESS_UNIX: AtomicU64 = AtomicU64::new(0);

/// Register this crate's metrics. Called from the process's metric
/// registration root.
pub fn describe_metrics() {
    metrics::describe_counter!(
        AUTH_OUTCOMES,
        "Authentication attempts by mechanism (`method`) and outcome (`result`)."
    );
    metrics::describe_counter!(
        JWKS_FETCHES_OK,
        "JWKS fetches that returned a usable key set."
    );
    metrics::describe_counter!(
        JWKS_FETCHES_FAILED,
        "JWKS fetches that failed; previously cached keys keep being served."
    );
    metrics::describe_counter!(
        JWKS_ON_DEMAND,
        "JWKS fetches triggered by a token presenting an unknown key id."
    );
    metrics::describe_counter!(
        JWKS_ON_DEMAND_SUPPRESSED,
        "Unknown-key-id refetches suppressed by the rate limiter."
    );
    metrics::describe_gauge!(
        JWKS_STALENESS,
        "Seconds since the last successful JWKS fetch; absent until one succeeds."
    );
}

/// Sample the point-in-time gauges.
pub fn gather_metrics() {
    let last = LAST_JWKS_SUCCESS_UNIX.load(Ordering::Relaxed);
    if last == 0 {
        // Never fetched: publishing 0 would read as "perfectly fresh", and
        // publishing a huge number would read as an outage. Publish nothing.
        return;
    }
    let age = now_unix().saturating_sub(last);
    metrics::gauge!(JWKS_STALENESS).set(age as f64);
}

/// Record the outcome of one authentication attempt.
pub(crate) fn record_auth_outcome(method: &'static str, result: &'static str) {
    metrics::counter!(AUTH_OUTCOMES, "method" => method, "result" => result).increment(1);
}

/// Record a successful key-set fetch, and stamp the staleness clock.
pub(crate) fn record_jwks_fetch_ok() {
    LAST_JWKS_SUCCESS_UNIX.store(now_unix(), Ordering::Relaxed);
    metrics::counter!(JWKS_FETCHES_OK).increment(1);
}

pub(crate) fn record_jwks_fetch_failed() {
    metrics::counter!(JWKS_FETCHES_FAILED).increment(1);
}

pub(crate) fn record_jwks_on_demand() {
    metrics::counter!(JWKS_ON_DEMAND).increment(1);
}

pub(crate) fn record_jwks_on_demand_suppressed() {
    metrics::counter!(JWKS_ON_DEMAND_SUPPRESSED).increment(1);
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}
