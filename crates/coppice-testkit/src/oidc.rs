//! A fake OpenID Connect identity provider for `coppice-authn`'s validator
//! and JWKS-cache tests.
//!
//! This is a real HTTP server on `127.0.0.1` serving real discovery and JWKS
//! documents, minting real ES256-signed JWTs — not a stub the validator
//! trusts by construction. The point is to exercise the actual code paths
//! (discovery fetch, JWKS fetch, signature verification, key rollover, and
//! on-demand refetch when a `kid` is missing) against a server the test
//! fully controls, including making it lie, go dark, or hand out malformed
//! tokens.
//!
//! ES256 (ECDSA P-256) only, no RSA: keygen is fast and doesn't need
//! `aws-lc`/OpenSSL — `rcgen`'s `ring` backend does it in microseconds,
//! which matters when a test rotates keys a dozen times. Real IdPs mostly
//! use RS256, but the validator doesn't care which curve/algorithm family
//! it's handed as long as it matches the JWK's `alg`, so ES256 exercises the
//! same code paths for a fraction of the cost.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{json, Map, Value};

/// A running fake IdP: one ES256 keypair published to start, an HTTP server
/// bound on an OS-assigned `127.0.0.1` port, and mutable server-side state
/// (published keys, "dark" toggle, request counters) reachable through
/// `&self` so a `FakeIdp` can be held behind a shared reference in tests
/// that need to mutate it from multiple call sites (e.g. rotate, then
/// assert, then go dark).
pub struct FakeIdp {
    state: Arc<IdpState>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}

struct IdpState {
    issuer: String,
    dark: AtomicBool,
    jwks_fetches: AtomicUsize,
    discovery_fetches: AtomicUsize,
    keys: Mutex<Keys>,
}

struct Keys {
    /// Every key ever generated stays here (and in the JWKS) once
    /// published, even after rotation — a client that cached the JWKS
    /// before a rotation must still be able to verify tokens signed with
    /// the key it already knows about, and tests that assert "old tokens
    /// still verify after rotation" need the server to keep serving it.
    published: Vec<PublishedKey>,
    current_kid: String,
    next_kid: usize,
}

struct PublishedKey {
    kid: String,
    encoding_key: EncodingKey,
    jwk: Value,
}

impl Keys {
    fn find(&self, kid: &str) -> Option<&PublishedKey> {
        self.published.iter().find(|k| k.kid == kid)
    }
}

/// Generate one ES256 keypair, returning (kid, jsonwebtoken encoding key,
/// public JWK JSON).
fn generate_es256_key(kid: String) -> (EncodingKey, Value) {
    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .expect("generate a P-256 keypair for the fake IdP");

    let pem = key_pair.serialize_pem();
    let encoding_key = EncodingKey::from_ec_pem(pem.as_bytes())
        .expect("jsonwebtoken accepts the PKCS#8 PEM rcgen just produced");

    // The SPKI DER's last 65 bytes are the uncompressed EC point:
    // 0x04 || x(32) || y(32). Everything before that is the SPKI
    // AlgorithmIdentifier, whose length varies by encoder, so anchor on
    // the fixed-size point from the end rather than parsing ASN.1.
    let spki = key_pair.public_key_der();
    assert!(
        spki.len() >= 65,
        "SPKI DER for a P-256 key must be at least 65 bytes, got {}",
        spki.len()
    );
    let point = &spki[spki.len() - 65..];
    assert_eq!(
        point[0], 0x04,
        "expected an uncompressed EC point (leading 0x04), got {:#x} — \
         rcgen's SPKI encoding must have changed",
        point[0]
    );
    let x = &point[1..33];
    let y = &point[33..65];

    let jwk = json!({
        "kty": "EC",
        "crv": "P-256",
        "alg": "ES256",
        "use": "sig",
        "kid": kid,
        "x": URL_SAFE_NO_PAD.encode(x),
        "y": URL_SAFE_NO_PAD.encode(y),
    });

    (encoding_key, jwk)
}

impl FakeIdp {
    /// Bind `127.0.0.1:0`, publish one ES256 key, and start serving
    /// discovery + JWKS from a spawned task.
    pub async fn start() -> FakeIdp {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the fake IdP's listener");
        let addr = listener.local_addr().expect("local addr of the fake IdP");
        let issuer = format!("http://{addr}");

        let (encoding_key, jwk) = generate_es256_key("key-0".to_string());
        let keys = Keys {
            published: vec![PublishedKey {
                kid: "key-0".to_string(),
                encoding_key,
                jwk,
            }],
            current_kid: "key-0".to_string(),
            next_kid: 1,
        };

        let state = Arc::new(IdpState {
            issuer,
            dark: AtomicBool::new(false),
            jwks_fetches: AtomicUsize::new(0),
            discovery_fetches: AtomicUsize::new(0),
            keys: Mutex::new(keys),
        });

        let router = Router::new()
            .route("/.well-known/openid-configuration", get(discovery_handler))
            .route("/jwks", get(jwks_handler))
            .with_state(Arc::clone(&state));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let join = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let mut rx = shutdown_rx;
                    // Wait until the sender flips the flag or is dropped;
                    // either way it's time to stop serving.
                    let _ = rx.wait_for(|stop| *stop).await;
                })
                .await
                .expect("fake IdP server task");
        });

        FakeIdp {
            state,
            shutdown_tx,
            join,
        }
    }

    /// e.g. `"http://127.0.0.1:53211"` — this exact string is both the
    /// discovery base and the `iss` claim of tokens minted by [`sign`].
    ///
    /// [`sign`]: FakeIdp::sign
    pub fn issuer(&self) -> String {
        self.state.issuer.clone()
    }

    /// The `kid` [`sign`](FakeIdp::sign) currently uses.
    pub fn current_kid(&self) -> String {
        self.state
            .keys
            .lock()
            .expect("fake IdP key lock")
            .current_kid
            .clone()
    }

    /// Sign `claims` with the current key, ES256.
    pub fn sign(&self, claims: TokenClaims) -> String {
        self.sign_with_key(claims, SigningKey::Current)
    }

    /// Sign `claims` under an explicit key selection — the escape hatch for
    /// wrong-key, alg-confusion, and unpublished-`kid` test cases that a
    /// legitimate IdP would never produce.
    pub fn sign_with_key(&self, claims: TokenClaims, key: SigningKey) -> String {
        let payload = claims.into_json(&self.state.issuer);

        match key {
            SigningKey::Current => {
                let keys = self.state.keys.lock().expect("fake IdP key lock");
                let kid = keys.current_kid.clone();
                let published = keys.find(&kid).expect("current kid is always published");
                encode_es256(&kid, &published.encoding_key, &payload)
            }
            SigningKey::Published(kid) => {
                let keys = self.state.keys.lock().expect("fake IdP key lock");
                let published = keys
                    .find(&kid)
                    .unwrap_or_else(|| panic!("kid {kid:?} is not published on this fake IdP"));
                encode_es256(&kid, &published.encoding_key, &payload)
            }
            SigningKey::UnpublishedEs256 { kid } => {
                // Deliberately not inserted into `keys.published`: the
                // header claims a kid, but the JWKS never advertises the
                // key that actually signed this token (or, if the caller
                // passed a kid that *is* published, the signature simply
                // won't match that key's public half).
                let (encoding_key, _jwk) = generate_es256_key(kid.clone());
                encode_es256(&kid, &encoding_key, &payload)
            }
            SigningKey::Hs256 { kid, secret } => {
                let mut header = Header::new(Algorithm::HS256);
                header.kid = Some(kid);
                jsonwebtoken::encode(&header, &payload, &EncodingKey::from_secret(&secret))
                    .expect("HS256 encode never fails for well-formed claims")
            }
            SigningKey::NoneAlg { kid } => encode_none_alg(kid, &payload),
        }
    }

    /// Generate and publish a second ES256 key; the old key stays
    /// published (see [`Keys::published`](Keys)). Returns the new `kid`;
    /// subsequent [`sign`](FakeIdp::sign) calls use it.
    pub fn rotate_key(&self) -> String {
        let mut keys = self.state.keys.lock().expect("fake IdP key lock");
        let kid = format!("key-{}", keys.next_kid);
        keys.next_kid += 1;
        let (encoding_key, jwk) = generate_es256_key(kid.clone());
        keys.published.push(PublishedKey {
            kid: kid.clone(),
            encoding_key,
            jwk,
        });
        keys.current_kid = kid.clone();
        kid
    }

    /// Stop answering discovery/JWKS requests (both handlers return
    /// `503`) while leaving the port bound, so [`resume`](FakeIdp::resume)
    /// restores the exact same issuer URL a cached discovery document
    /// still points at — the scenario this exists to test is a JWKS cache
    /// that already has a good issuer/`jwks_uri` and just needs to survive
    /// a transient outage on refetch.
    pub fn go_dark(&self) {
        self.state.dark.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.state.dark.store(false, Ordering::SeqCst);
    }

    #[doc(hidden)]
    pub fn is_dark(&self) -> bool {
        self.state.dark.load(Ordering::SeqCst)
    }

    /// Number of requests the JWKS endpoint has received, including while
    /// dark. Used to assert on-demand refetch is rate-limited rather than
    /// hammering the IdP once per unknown `kid`.
    pub fn jwks_fetches(&self) -> usize {
        self.state.jwks_fetches.load(Ordering::SeqCst)
    }

    /// Number of requests the discovery endpoint has received.
    pub fn discovery_fetches(&self) -> usize {
        self.state.discovery_fetches.load(Ordering::SeqCst)
    }

    /// Stop serving and wait for the server task to exit.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        self.join.await.expect("fake IdP server task join");
    }
}

fn encode_es256(kid: &str, encoding_key: &EncodingKey, payload: &Value) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(kid.to_string());
    jsonwebtoken::encode(&header, payload, encoding_key)
        .expect("ES256 encode never fails for well-formed claims")
}

/// Hand-assemble a `{"alg":"none",...}.<payload>.` token: `jsonwebtoken`
/// refuses to mint these (rightly — it's the classic alg-confusion
/// vulnerability), so a fake IdP that wants to test the validator's
/// rejection of it has to build the token by hand.
fn encode_none_alg(kid: Option<String>, payload: &Value) -> String {
    let mut header = Map::new();
    header.insert("alg".to_string(), json!("none"));
    header.insert("typ".to_string(), json!("JWT"));
    if let Some(kid) = kid {
        header.insert("kid".to_string(), json!(kid));
    }

    let header_b64 = URL_SAFE_NO_PAD.encode(Value::Object(header).to_string());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string());
    format!("{header_b64}.{payload_b64}.")
}

async fn discovery_handler(State(state): State<Arc<IdpState>>) -> Response {
    state.discovery_fetches.fetch_add(1, Ordering::SeqCst);
    if state.dark.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    Json(json!({
        "issuer": state.issuer,
        "jwks_uri": format!("{}/jwks", state.issuer),
        "authorization_endpoint": format!("{}/authorize", state.issuer),
        "token_endpoint": format!("{}/token", state.issuer),
    }))
    .into_response()
}

async fn jwks_handler(State(state): State<Arc<IdpState>>) -> Response {
    state.jwks_fetches.fetch_add(1, Ordering::SeqCst);
    if state.dark.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let keys = state.keys.lock().expect("fake IdP key lock");
    let jwks: Vec<Value> = keys.published.iter().map(|k| k.jwk.clone()).collect();
    Json(json!({ "keys": jwks })).into_response()
}

/// Selects how [`FakeIdp::sign_with_key`] signs a token — the normal path
/// plus the malformed/mis-keyed variants a validator's test suite needs.
pub enum SigningKey {
    /// The current published key and its `kid` — same as
    /// [`FakeIdp::sign`].
    Current,
    /// A specific already-published `kid`.
    Published(String),
    /// A freshly generated ES256 key that is **not** in the JWKS, but the
    /// header claims the given `kid`. Passing a `kid` that *is* published
    /// gives a valid-looking token with a signature that doesn't match the
    /// published key; passing an unknown `kid` gives the "no such key"
    /// case.
    UnpublishedEs256 { kid: String },
    /// HMAC-signed (the classic RS/ES-to-HS256 alg-confusion attack: an
    /// attacker who knows a public key tries to pass it off as an HMAC
    /// secret).
    Hs256 { kid: String, secret: Vec<u8> },
    /// A hand-assembled `{"alg":"none",...}.<payload>.` token with an
    /// empty signature segment.
    NoneAlg { kid: Option<String> },
}

/// A builder for the claims of a token [`FakeIdp`] mints.
///
/// Claims are held as a `serde_json::Map` plus pending time offsets rather
/// than materialised eagerly, so `exp`/`nbf` are computed relative to *now*
/// at sign time — a `TokenClaims` built once and signed later (or signed
/// twice) still produces a fresh, valid expiry each time.
pub struct TokenClaims {
    claims: Map<String, Value>,
    iss_override: Option<String>,
    expires_in_secs: i64,
    not_before_in_secs: Option<i64>,
}

impl TokenClaims {
    /// `sub` is set; `iss` defaults to the fixture's issuer (filled in at
    /// sign time); no `aud`; `exp` = now + 300s; no `nbf`.
    pub fn new(sub: impl Into<String>) -> TokenClaims {
        let mut claims = Map::new();
        claims.insert("sub".to_string(), json!(sub.into()));
        TokenClaims {
            claims,
            iss_override: None,
            expires_in_secs: 300,
            not_before_in_secs: None,
        }
    }

    /// Drop the `sub` claim entirely (for "no subject" validator tests).
    pub fn no_subject(mut self) -> Self {
        self.claims.remove("sub");
        self
    }

    /// Override the fixture's issuer (for the wrong-`iss` test).
    pub fn issuer(mut self, iss: impl Into<String>) -> Self {
        self.iss_override = Some(iss.into());
        self
    }

    /// Set `aud` as a JSON string.
    pub fn audience(mut self, aud: impl Into<String>) -> Self {
        self.claims.insert("aud".to_string(), json!(aud.into()));
        self
    }

    /// Set `aud` as a JSON array.
    pub fn audiences(mut self, auds: &[&str]) -> Self {
        self.claims.insert("aud".to_string(), json!(auds));
        self
    }

    /// Offset from now, in seconds, for `exp`. Negative = already expired.
    pub fn expires_in(mut self, secs: i64) -> Self {
        self.expires_in_secs = secs;
        self
    }

    /// Offset from now, in seconds, for `nbf`. Positive = not-yet-valid.
    pub fn not_before_in(mut self, secs: i64) -> Self {
        self.not_before_in_secs = Some(secs);
        self
    }

    /// An arbitrary claim under an arbitrary name — how tests set, e.g.,
    /// groups under a non-default claim name.
    pub fn claim(mut self, name: impl Into<String>, value: Value) -> Self {
        self.claims.insert(name.into(), value);
        self
    }

    fn into_json(self, fixture_issuer: &str) -> Value {
        let TokenClaims {
            mut claims,
            iss_override,
            expires_in_secs,
            not_before_in_secs,
        } = self;

        let now = jsonwebtoken::get_current_timestamp() as i64;
        claims.insert(
            "iss".to_string(),
            json!(iss_override.unwrap_or_else(|| fixture_issuer.to_string())),
        );
        claims.insert("exp".to_string(), json!(now + expires_in_secs));
        if let Some(offset) = not_before_in_secs {
            claims.insert("nbf".to_string(), json!(now + offset));
        }

        Value::Object(claims)
    }
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
    use serde_json::json;

    use super::*;

    fn decoding_key_for(jwks: &Value, kid: &str) -> DecodingKey {
        let key_jwk = jwks["keys"]
            .as_array()
            .expect("jwks.keys is an array")
            .iter()
            .find(|k| k["kid"] == kid)
            .unwrap_or_else(|| panic!("kid {kid:?} not found in jwks"));
        let x = key_jwk["x"].as_str().expect("jwk.x");
        let y = key_jwk["y"].as_str().expect("jwk.y");
        DecodingKey::from_ec_components(x, y).expect("build an EC decoding key from the jwk")
    }

    async fn fetch_jwks(idp: &FakeIdp) -> Value {
        // No HTTP client dependency in this crate: reach for the raw JWK
        // material directly through the same lock the handler uses. This
        // is intentionally a shortcut around the handler, not a
        // replacement for it — the real over-the-wire discovery/JWKS
        // fetch is exercised by `coppice-authn`'s integration tests.
        let keys = idp.state.keys.lock().expect("fake IdP key lock");
        let jwks: Vec<Value> = keys.published.iter().map(|k| k.jwk.clone()).collect();
        json!({ "keys": jwks })
    }

    #[tokio::test]
    async fn round_trip_verifies_and_preserves_claims() {
        let idp = FakeIdp::start().await;

        let token = idp.sign(
            TokenClaims::new("alice")
                .audience("coppice-cli")
                .claim("groups", json!(["operators"])),
        );

        let jwks = fetch_jwks(&idp).await;
        let header = decode_header(&token).expect("decode header");
        let kid = header.kid.expect("es256 tokens carry a kid");
        let decoding_key = decoding_key_for(&jwks, &kid);

        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_issuer(&[idp.issuer()]);
        validation.set_audience(&["coppice-cli"]);

        let data = decode::<Value>(&token, &decoding_key, &validation).expect("verify token");
        assert_eq!(data.claims["sub"], json!("alice"));
        assert_eq!(data.claims["aud"], json!("coppice-cli"));
        assert_eq!(data.claims["groups"], json!(["operators"]));

        idp.shutdown().await;
    }

    #[tokio::test]
    async fn rotate_key_keeps_old_key_verifiable() {
        let idp = FakeIdp::start().await;

        let old_kid = idp.current_kid();
        let old_token = idp.sign(TokenClaims::new("alice"));

        let new_kid = idp.rotate_key();
        assert_ne!(old_kid, new_kid);
        let new_token = idp.sign(TokenClaims::new("bob"));

        let jwks = fetch_jwks(&idp).await;
        assert_eq!(
            jwks["keys"].as_array().expect("keys array").len(),
            2,
            "both keys stay published after rotation"
        );

        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_issuer(&[idp.issuer()]);
        validation.validate_aud = false;

        let old_decoding_key = decoding_key_for(&jwks, &old_kid);
        let old_data =
            decode::<Value>(&old_token, &old_decoding_key, &validation).expect("verify old token");
        assert_eq!(old_data.claims["sub"], json!("alice"));

        let new_decoding_key = decoding_key_for(&jwks, &new_kid);
        let new_data =
            decode::<Value>(&new_token, &new_decoding_key, &validation).expect("verify new token");
        assert_eq!(new_data.claims["sub"], json!("bob"));

        idp.shutdown().await;
    }

    #[tokio::test]
    async fn go_dark_and_resume_are_idempotent_and_observable() {
        let idp = FakeIdp::start().await;

        assert_eq!(idp.jwks_fetches(), 0);
        assert_eq!(idp.discovery_fetches(), 0);
        assert!(!idp.is_dark());

        idp.go_dark();
        idp.go_dark();
        assert!(idp.is_dark());

        idp.resume();
        idp.resume();
        assert!(!idp.is_dark());

        // The real over-the-wire dark/resume behaviour (handlers actually
        // returning 503, the issuer URL staying stable across the
        // outage) is covered by `coppice-authn`'s integration tests.
        idp.shutdown().await;
    }

    #[tokio::test]
    async fn none_alg_token_has_empty_signature_and_alg_none_header() {
        let idp = FakeIdp::start().await;

        let token = idp.sign_with_key(
            TokenClaims::new("mallory"),
            SigningKey::NoneAlg {
                kid: Some("key-0".to_string()),
            },
        );

        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "still a three-part JWT shape");
        assert!(parts[2].is_empty(), "no signature segment");

        let header_json: Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(parts[0])
                .expect("decode header segment"),
        )
        .expect("header is JSON");
        assert_eq!(header_json["alg"], json!("none"));

        idp.shutdown().await;
    }
}
