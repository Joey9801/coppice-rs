//! JWKS cache behaviour around IdP outages.
//!
//! The two acceptance criteria from ADR 0022 live here: an IdP outage must not
//! invalidate cached keys, and an edge that starts before its IdP is reachable
//! must serve (failing bearer auth cleanly) and recover on its own.

mod common;

use std::sync::Arc;
use std::time::Duration;

use coppice_authn::{JwksTimings, ValidateError, DEFAULT_GROUPS_CLAIM};
use coppice_testkit::oidc::{FakeIdp, TokenClaims};

use common::{eventually, ready_validator, validator_with, AUDIENCE};

#[tokio::test]
async fn cached_keys_survive_an_idp_outage() {
    let idp = FakeIdp::start().await;
    let (cache, validator) = ready_validator(&idp).await;

    idp.go_dark();

    // Nothing was evicted, so a token issued before the outage — and any token
    // signed by a key we already hold — keeps working for its full lifetime.
    let token = idp.sign(TokenClaims::new("user").audience(AUDIENCE));
    assert!(validator
        .validate(&token, DEFAULT_GROUPS_CLAIM)
        .await
        .is_ok());
    assert_eq!(cache.key_count(), 1);

    // A refresh attempt during the outage fails, and still changes nothing.
    assert!(cache.refresh_now().await.is_err());
    assert!(validator
        .validate(&token, DEFAULT_GROUPS_CLAIM)
        .await
        .is_ok());
    assert_eq!(cache.key_count(), 1);

    idp.shutdown().await;
}

#[tokio::test]
async fn a_key_published_during_an_outage_only_works_after_recovery() {
    let idp = FakeIdp::start().await;
    // A short on-demand limit: this test deliberately makes two unknown-kid
    // attempts and needs the second to be allowed to reach the IdP.
    let (cache, validator) = validator_with(
        &idp,
        JwksTimings {
            on_demand_min_interval: Duration::from_millis(50),
            ..common::manual_timings()
        },
    );
    cache.refresh_now().await.expect("initial fetch");

    idp.go_dark();
    idp.rotate_key();
    let token = idp.sign(TokenClaims::new("user").audience(AUDIENCE));

    // The signing key is real, but it was published while we could not see it,
    // and the on-demand refetch cannot reach the IdP either. This is an honest
    // failure, not a silent acceptance.
    assert!(matches!(
        validator.validate(&token, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::UnknownKeyId(_))
    ));

    idp.resume();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let validated = validator
        .validate(&token, DEFAULT_GROUPS_CLAIM)
        .await
        .expect("the same token validates once the IdP is reachable again");
    assert_eq!(validated.sub, "user");

    idp.shutdown().await;
}

#[tokio::test]
async fn the_refresh_loop_starts_against_a_dark_idp_and_recovers() {
    let idp = FakeIdp::start().await;
    idp.go_dark();

    let (cache, validator) = validator_with(
        &idp,
        JwksTimings {
            refresh_interval: Duration::from_millis(50),
            refresh_jitter: Duration::ZERO,
            backoff_base: Duration::from_millis(20),
            backoff_max: Duration::from_millis(50),
            on_demand_min_interval: Duration::from_millis(50),
        },
    );

    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let refresher = tokio::spawn(Arc::clone(&cache).run(stop_rx));

    // Startup does not block on the IdP: the cache is simply empty, and bearer
    // validation says so specifically rather than reporting a bad token.
    let token = idp.sign(TokenClaims::new("user").audience(AUDIENCE));
    assert_eq!(
        validator.validate(&token, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::KeysUnavailable)
    );

    // The loop keeps retrying under backoff; when the IdP appears, it lands.
    idp.resume();
    eventually("the refresh loop to pick up the key set", || {
        cache.key_count() > 0
    })
    .await;

    let validated = validator
        .validate(&token, DEFAULT_GROUPS_CLAIM)
        .await
        .expect("validation starts working once keys arrive");
    assert_eq!(validated.sub, "user");

    stop.send(true).expect("stop the refresher");
    refresher.await.expect("the refresh loop exits on shutdown");
    idp.shutdown().await;
}

#[tokio::test]
async fn the_refresh_loop_picks_up_a_rotation_without_an_on_demand_fetch() {
    let idp = FakeIdp::start().await;
    let (cache, _validator) = validator_with(
        &idp,
        JwksTimings {
            refresh_interval: Duration::from_millis(30),
            refresh_jitter: Duration::ZERO,
            ..common::manual_timings()
        },
    );

    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let refresher = tokio::spawn(Arc::clone(&cache).run(stop_rx));
    eventually("the first scheduled fetch", || cache.key_count() == 1).await;

    idp.rotate_key();
    eventually("the rotation to be picked up on schedule", || {
        cache.key_count() == 2
    })
    .await;

    stop.send(true).expect("stop the refresher");
    refresher.await.expect("the refresh loop exits on shutdown");
    idp.shutdown().await;
}
