//! The authenticator chain: which mechanism wins, and what a bad credential
//! does in each posture.

mod common;

use std::io::Cursor;

use coppice_authn::{
    no_ca, static_groups_claim, AuthMethod, AuthMode, AuthnChain, CaProvider, Credentials,
    Unauthenticated, DEFAULT_GROUPS_CLAIM,
};
use coppice_testkit::oidc::{FakeIdp, TokenClaims};
use coppice_tls::pki;

use common::{config, ready_validator, AUDIENCE};

/// A throwaway cluster CA and one operator leaf under it, minted through the
/// same `pki` entry points cluster formation uses.
struct Pki {
    ca_pem: Vec<u8>,
    operator_leaf_der: Vec<u8>,
}

fn mint_pki(cn: &str) -> Pki {
    let ca = pki::mint_root_ca().expect("mint a root CA");
    let signer = pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load the CA signer");
    let (leaf_pem, _key_pem) =
        pki::mint_operator_local(&signer, cn).expect("mint an operator leaf");
    Pki {
        ca_pem: ca.cert_pem,
        operator_leaf_der: pem_to_der(&leaf_pem),
    }
}

/// The HTTP edge hands the chain the leaf as DER (that is what rustls exposes
/// on the connection), so the tests do the same conversion the edge does.
fn pem_to_der(pem: &[u8]) -> Vec<u8> {
    let mut reader = Cursor::new(pem);
    let der = rustls_pemfile::certs(&mut reader)
        .next()
        .expect("one certificate in the PEM")
        .expect("a readable certificate")
        .to_vec();
    der
}

fn ca_provider(ca_pem: Vec<u8>) -> CaProvider {
    std::sync::Arc::new(move || Some(ca_pem.clone()))
}

#[tokio::test]
async fn an_operator_certificate_authenticates_as_its_common_name() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;
    let pki = mint_pki("alice");

    let chain = AuthnChain::oidc(
        validator,
        static_groups_claim(DEFAULT_GROUPS_CLAIM),
        ca_provider(pki.ca_pem),
        config(&idp.issuer()),
    );

    let actor = chain
        .authenticate(Credentials {
            bearer: None,
            peer_leaf_der: Some(&pki.operator_leaf_der),
        })
        .await
        .expect("a verified operator leaf authenticates");

    assert_eq!(actor.principal, "cert:alice");
    assert!(actor.operator_cert);
    assert!(!actor.auth_disabled);
    assert_eq!(actor.method(), AuthMethod::OperatorCert);

    idp.shutdown().await;
}

#[tokio::test]
async fn the_operator_certificate_wins_over_a_bearer_token() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;
    let pki = mint_pki("alice");
    let token = idp.sign(TokenClaims::new("user-42").audience(AUDIENCE));

    let chain = AuthnChain::oidc(
        validator,
        static_groups_claim(DEFAULT_GROUPS_CLAIM),
        ca_provider(pki.ca_pem),
        config(&idp.issuer()),
    );

    // A stale token in the operator's environment must not shadow the
    // break-glass credential they deliberately presented.
    let actor = chain
        .authenticate(Credentials {
            bearer: Some(&token),
            peer_leaf_der: Some(&pki.operator_leaf_der),
        })
        .await
        .expect("the certificate decides");

    assert_eq!(actor.principal, "cert:alice");
    assert!(actor.operator_cert);

    idp.shutdown().await;
}

#[tokio::test]
async fn a_valid_bearer_token_authenticates_as_its_subject() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;
    let token = idp.sign(
        TokenClaims::new("user-42")
            .audience(AUDIENCE)
            .claim(DEFAULT_GROUPS_CLAIM, serde_json::json!(["sre"])),
    );

    let chain = AuthnChain::oidc(
        validator,
        static_groups_claim(DEFAULT_GROUPS_CLAIM),
        no_ca(),
        config(&idp.issuer()),
    );

    let actor = chain
        .authenticate(Credentials {
            bearer: Some(&token),
            peer_leaf_der: None,
        })
        .await
        .expect("a valid token authenticates");

    assert_eq!(actor.principal, "user-42");
    assert_eq!(actor.groups, vec!["sre"]);
    assert!(!actor.operator_cert);
    assert!(!actor.auth_disabled);
    assert_eq!(actor.method(), AuthMethod::Bearer);
    assert!(matches!(chain.mode(), AuthMode::Oidc(_)));

    idp.shutdown().await;
}

#[tokio::test]
async fn no_credentials_in_the_oidc_posture_is_unauthenticated() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;

    let chain = AuthnChain::oidc(
        validator,
        static_groups_claim(DEFAULT_GROUPS_CLAIM),
        no_ca(),
        config(&idp.issuer()),
    );

    assert_eq!(
        chain
            .authenticate(Credentials::default())
            .await
            .unwrap_err(),
        Unauthenticated::NoCredentials
    );

    idp.shutdown().await;
}

#[tokio::test]
async fn an_invalid_bearer_token_is_rejected_rather_than_ignored() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;
    let expired = idp.sign(TokenClaims::new("user").audience(AUDIENCE).expires_in(-600));

    let chain = AuthnChain::oidc(
        validator,
        static_groups_claim(DEFAULT_GROUPS_CLAIM),
        no_ca(),
        config(&idp.issuer()),
    );

    // The chain stops at the mechanism whose credential failed; it does not
    // fall through to a weaker identity.
    assert!(matches!(
        chain
            .authenticate(Credentials {
                bearer: Some(&expired),
                peer_leaf_der: None,
            })
            .await,
        Err(Unauthenticated::InvalidBearer(_))
    ));

    idp.shutdown().await;
}

#[tokio::test]
async fn a_certificate_from_another_ca_is_rejected_in_the_oidc_posture() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;
    let ours = mint_pki("alice");
    let theirs = mint_pki("mallory");

    let chain = AuthnChain::oidc(
        validator,
        static_groups_claim(DEFAULT_GROUPS_CLAIM),
        ca_provider(ours.ca_pem),
        config(&idp.issuer()),
    );

    assert!(matches!(
        chain
            .authenticate(Credentials {
                bearer: None,
                peer_leaf_der: Some(&theirs.operator_leaf_der),
            })
            .await,
        Err(Unauthenticated::ClientCertificate(_))
    ));

    idp.shutdown().await;
}

#[tokio::test]
async fn open_mode_resolves_to_the_anonymous_actor() {
    let chain = AuthnChain::open(no_ca());
    assert!(matches!(chain.mode(), AuthMode::Open));

    let actor = chain
        .authenticate(Credentials::default())
        .await
        .expect("open mode authenticates everything");

    assert_eq!(actor.principal, "anonymous");
    assert!(actor.auth_disabled);
    assert!(!actor.operator_cert);
    assert_eq!(actor.method(), AuthMethod::Open);
}

#[tokio::test]
async fn open_mode_ignores_a_bearer_token_it_cannot_judge() {
    // Open mode has no `[sso]` block, so there is no issuer, no audience and
    // no key set — a bearer header is uninterpretable rather than invalid, and
    // 401-ing a credential on a deployment that requires none would be a
    // failure mode invented from nothing.
    let chain = AuthnChain::open(no_ca());

    let actor = chain
        .authenticate(Credentials {
            bearer: Some("not-even-a-jwt"),
            peer_leaf_der: None,
        })
        .await
        .expect("open mode still authenticates");

    assert_eq!(actor.principal, "anonymous");
    assert!(actor.auth_disabled);
}

#[tokio::test]
async fn open_mode_still_records_an_operator_certificate() {
    let pki = mint_pki("alice");
    let chain = AuthnChain::open(ca_provider(pki.ca_pem));

    let actor = chain
        .authenticate(Credentials {
            bearer: None,
            peer_leaf_der: Some(&pki.operator_leaf_der),
        })
        .await
        .expect("a verified operator leaf authenticates");

    // Strictly more information in the audit trail, at no cost: open mode
    // grants unscoped admin either way.
    assert_eq!(actor.principal, "cert:alice");
    assert!(actor.operator_cert);
}

#[tokio::test]
async fn open_mode_falls_through_an_unverifiable_certificate() {
    // The client listener requests certificates from everyone; browsers and
    // stray clients will present ones that have nothing to do with this
    // cluster. In a posture that requires no credential at all, that must not
    // be a 401.
    let ours = mint_pki("alice");
    let theirs = mint_pki("mallory");
    let chain = AuthnChain::open(ca_provider(ours.ca_pem));

    let actor = chain
        .authenticate(Credentials {
            bearer: None,
            peer_leaf_der: Some(&theirs.operator_leaf_der),
        })
        .await
        .expect("open mode falls through to anonymous");

    assert_eq!(actor.principal, "anonymous");
    assert!(actor.auth_disabled);
}

#[tokio::test]
async fn a_certificate_presented_before_formation_falls_through() {
    let idp = FakeIdp::start().await;
    let (_cache, validator) = ready_validator(&idp).await;
    let pki = mint_pki("alice");
    let token = idp.sign(TokenClaims::new("user-42").audience(AUDIENCE));

    // No cluster CA exists yet, so no certificate can be judged. The bearer
    // path — the one that still works — must stay reachable.
    let chain = AuthnChain::oidc(
        validator,
        static_groups_claim(DEFAULT_GROUPS_CLAIM),
        no_ca(),
        config(&idp.issuer()),
    );

    let actor = chain
        .authenticate(Credentials {
            bearer: Some(&token),
            peer_leaf_der: Some(&pki.operator_leaf_der),
        })
        .await
        .expect("the bearer token still authenticates");
    assert_eq!(actor.principal, "user-42");

    idp.shutdown().await;
}
