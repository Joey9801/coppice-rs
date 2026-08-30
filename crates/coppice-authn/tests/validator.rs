//! Behavioural suite for bearer-token validation, driven end to end against
//! the fake IdP fixture.
//!
//! These tests deliberately assert on *behaviour* (expired, wrong audience,
//! unknown key id, …) rather than on `jsonwebtoken` internals, so swapping the
//! JWT library is a mechanical change confined to
//! `ValidateError::from_jwt` — SHARED.md decision 5.

mod common;

use coppice_authn::{ValidateError, DEFAULT_GROUPS_CLAIM};
use coppice_testkit::oidc::{FakeIdp, SigningKey, TokenClaims};
use serde_json::json;

use common::{ready_validator, AUDIENCE};

#[tokio::test]
async fn a_well_formed_token_yields_its_subject_and_groups() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    let token = idp.sign(
        TokenClaims::new("user-42")
            .audience(AUDIENCE)
            .claim(DEFAULT_GROUPS_CLAIM, json!(["batch-users", "sre"])),
    );

    let validated = validator
        .validate(&token, DEFAULT_GROUPS_CLAIM)
        .await
        .expect("a freshly issued token validates");
    assert_eq!(validated.sub, "user-42");
    assert_eq!(validated.groups, vec!["batch-users", "sre"]);

    idp.shutdown().await;
}

#[tokio::test]
async fn an_expired_token_is_rejected() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    // Well past the 60s skew allowance.
    let token = idp.sign(TokenClaims::new("user").audience(AUDIENCE).expires_in(-600));

    assert_eq!(
        validator.validate(&token, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::Expired)
    );

    idp.shutdown().await;
}

#[tokio::test]
async fn not_before_is_honoured_with_a_bounded_skew() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    // Inside the allowance: a coordinator whose clock trails the IdP's by a
    // few seconds must not reject freshly minted tokens.
    let soon = idp.sign(
        TokenClaims::new("user")
            .audience(AUDIENCE)
            .not_before_in(30),
    );
    assert!(validator
        .validate(&soon, DEFAULT_GROUPS_CLAIM)
        .await
        .is_ok());

    // Outside it: this is a token that is genuinely not usable yet.
    let later = idp.sign(
        TokenClaims::new("user")
            .audience(AUDIENCE)
            .not_before_in(600),
    );
    assert_eq!(
        validator.validate(&later, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::NotYetValid)
    );

    idp.shutdown().await;
}

#[tokio::test]
async fn a_token_from_a_different_issuer_is_rejected() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    let token = idp.sign(
        TokenClaims::new("user")
            .audience(AUDIENCE)
            .issuer("https://someone-elses-idp.example"),
    );

    assert_eq!(
        validator.validate(&token, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::WrongIssuer)
    );

    idp.shutdown().await;
}

#[tokio::test]
async fn the_audience_must_name_this_cluster() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    let wrong = idp.sign(TokenClaims::new("user").audience("some-other-api"));
    assert_eq!(
        validator.validate(&wrong, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::WrongAudience)
    );

    // An access token minted for a different service, replayed here, is the
    // attack this check exists for — so "no audience at all" is its own
    // rejection rather than a pass.
    let missing = idp.sign(TokenClaims::new("user"));
    assert_eq!(
        validator.validate(&missing, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::MissingAudience)
    );

    // The multi-audience form is legal and common.
    let array = idp.sign(TokenClaims::new("user").audiences(&["other-api", AUDIENCE]));
    assert!(validator
        .validate(&array, DEFAULT_GROUPS_CLAIM)
        .await
        .is_ok());

    idp.shutdown().await;
}

#[tokio::test]
async fn a_token_with_no_subject_is_rejected() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    let token = idp.sign(TokenClaims::new("user").no_subject().audience(AUDIENCE));

    assert_eq!(
        validator.validate(&token, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::MissingSubject)
    );

    idp.shutdown().await;
}

#[tokio::test]
async fn an_unknown_key_id_triggers_one_refetch_and_then_validates() {
    let idp = FakeIdp::start().await;
    let (cache, validator) = ready_validator(&idp).await;
    assert_eq!(cache.key_count(), 1);

    // The IdP rotates. Our cache still holds only the old key, and the next
    // token is signed by the new one — exactly the case the on-demand fetch
    // exists to cover, without waiting out a refresh interval.
    let new_kid = idp.rotate_key();
    let token = idp.sign(TokenClaims::new("user").audience(AUDIENCE));

    let validated = validator
        .validate(&token, DEFAULT_GROUPS_CLAIM)
        .await
        .expect("the refetched key set validates the token");
    assert_eq!(validated.sub, "user");
    assert_eq!(cache.key_count(), 2, "both kids stay published");
    assert_eq!(idp.current_kid(), new_kid);

    idp.shutdown().await;
}

#[tokio::test]
async fn unknown_key_id_refetches_are_rate_limited() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;
    let before = idp.jwks_fetches();

    // Two tokens naming key ids the IdP has never published. Without a rate
    // limit, anyone could turn a stream of garbage tokens into a stream of
    // JWKS fetches against the IdP.
    for kid in ["never-published-1", "never-published-2"] {
        let token = idp.sign_with_key(
            TokenClaims::new("user").audience(AUDIENCE),
            SigningKey::UnpublishedEs256 {
                kid: kid.to_string(),
            },
        );
        assert!(matches!(
            validator.validate(&token, DEFAULT_GROUPS_CLAIM).await,
            Err(ValidateError::UnknownKeyId(_))
        ));
    }

    assert_eq!(
        idp.jwks_fetches() - before,
        1,
        "the second unknown kid must not reach the IdP"
    );

    idp.shutdown().await;
}

#[tokio::test]
async fn a_token_signed_by_the_wrong_key_is_rejected() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    // Correct kid in the header, published key in our cache — but the
    // signature was made with a key nobody published.
    let token = idp.sign_with_key(
        TokenClaims::new("user").audience(AUDIENCE),
        SigningKey::UnpublishedEs256 {
            kid: idp.current_kid(),
        },
    );

    assert_eq!(
        validator.validate(&token, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::BadSignature)
    );

    idp.shutdown().await;
}

#[tokio::test]
async fn algorithm_confusion_is_rejected() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    // The classic: hand the server an HMAC token keyed on something it might
    // treat as a shared secret. Rejected before key selection even happens.
    let hmac = idp.sign_with_key(
        TokenClaims::new("user").audience(AUDIENCE),
        SigningKey::Hs256 {
            kid: idp.current_kid(),
            secret: b"not-a-secret".to_vec(),
        },
    );
    assert_eq!(
        validator.validate(&hmac, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::SymmetricAlgorithm)
    );

    // `alg: none` never parses into a usable header at all.
    let unsigned = idp.sign_with_key(
        TokenClaims::new("user").audience(AUDIENCE),
        SigningKey::NoneAlg {
            kid: Some(idp.current_kid()),
        },
    );
    assert!(matches!(
        validator.validate(&unsigned, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::Malformed(_))
    ));

    idp.shutdown().await;
}

#[tokio::test]
async fn the_groups_claim_name_is_a_per_call_parameter() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    // `groups_claim` is replicated policy: one validator must answer for
    // whatever name is configured at the moment of the call.
    let token = idp.sign(
        TokenClaims::new("user")
            .audience(AUDIENCE)
            .claim("roles", json!(["platform-admins"])),
    );

    assert_eq!(
        validator.validate(&token, "roles").await.unwrap().groups,
        vec!["platform-admins"]
    );
    assert!(validator
        .validate(&token, DEFAULT_GROUPS_CLAIM)
        .await
        .unwrap()
        .groups
        .is_empty());

    idp.shutdown().await;
}

#[tokio::test]
async fn a_missing_groups_claim_is_not_an_error() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    let token = idp.sign(TokenClaims::new("user").audience(AUDIENCE));
    let validated = validator
        .validate(&token, DEFAULT_GROUPS_CLAIM)
        .await
        .expect("a principal with no groups is ordinary");
    assert!(validated.groups.is_empty());

    idp.shutdown().await;
}

#[tokio::test]
async fn a_groups_claim_of_the_wrong_shape_is_an_error() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    let token = idp.sign(
        TokenClaims::new("user")
            .audience(AUDIENCE)
            .claim(DEFAULT_GROUPS_CLAIM, json!({ "a": true })),
    );

    assert!(matches!(
        validator.validate(&token, DEFAULT_GROUPS_CLAIM).await,
        Err(ValidateError::UnreadableGroupsClaim(_))
    ));

    idp.shutdown().await;
}
