//! Offline bearer-token validation.

use std::collections::HashSet;
use std::sync::Arc;

use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{decode, decode_header, Validation};
use serde_json::{Map, Value};

use crate::jwks::{JwksCache, OnDemandOutcome};
use crate::OidcConfig;

/// Clock skew tolerated on `exp` and `nbf`, in seconds.
///
/// ADR 0022 asks for "a small bounded skew". Sixty seconds absorbs ordinary
/// NTP drift between the IdP and a coordinator; larger values start extending
/// the lifetime of revoked tokens, which is the one thing this design has no
/// other defence against.
pub const CLOCK_SKEW_LEEWAY_SECS: u64 = 60;

/// The identity a token proved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedToken {
    /// The `sub` claim — the principal.
    pub sub: String,
    /// The groups claim's contents, or empty when the claim is absent.
    pub groups: Vec<String>,
    /// The `name` claim, if present and a non-empty string.
    ///
    /// Presentation-only (ADR 0022): this rides alongside the resolved
    /// identity for display purposes — `GET /api/v1/session` reports it — and
    /// is never stored, never replicated, and never part of
    /// [`coppice_state::Actor`].
    pub name: Option<String>,
    /// The `email` claim, if present and a non-empty string.
    ///
    /// Same presentation-only rule as [`name`](Self::name): display only,
    /// never stored, never replicated.
    pub email: Option<String>,
}

/// Validates bearer tokens against a [`JwksCache`] and an [`OidcConfig`].
///
/// Cheap to clone in spirit but held behind the chain; construction does no
/// I/O and validation does none either, except on the unknown-`kid` path.
pub struct Validator {
    cache: Arc<JwksCache>,
    config: OidcConfig,
}

impl Validator {
    /// Build a validator over an already-constructed cache.
    ///
    /// Nothing here checks that `config.issuer` and the cache's issuer agree:
    /// they come from one config block at the only construction site, and a
    /// validator pointed at a *different* issuer's keys is a deployment the
    /// caller would have had to assemble on purpose.
    pub fn new(cache: Arc<JwksCache>, config: OidcConfig) -> Validator {
        Validator { cache, config }
    }

    /// The configuration this validator enforces.
    pub fn config(&self) -> &OidcConfig {
        &self.config
    }

    /// Validate one token and extract `(sub, groups)`.
    ///
    /// `groups_claim` is a per-call parameter, not construction-time state,
    /// because the claim name is *replicated policy* (`PolicyConfig`): an
    /// operator can change it at runtime and the next request must use the new
    /// name without rebuilding the edge.
    pub async fn validate(
        &self,
        token: &str,
        groups_claim: &str,
    ) -> Result<ValidatedToken, ValidateError> {
        // `decode_header` also rejects `alg: none` outright — jsonwebtoken's
        // `Algorithm` has no `none` variant, so such a header does not
        // deserialize. It never reaches key selection.
        let header = decode_header(token).map_err(|e| ValidateError::Malformed(e.to_string()))?;
        if crate::jwks::asymmetric(header.alg).is_none() {
            // Belt and braces: the algorithm pinned below comes from the JWK,
            // so an HMAC token could not verify against an EC/RSA key anyway.
            // Rejecting here makes the reason legible in logs instead of
            // surfacing as a generic algorithm mismatch.
            return Err(ValidateError::SymmetricAlgorithm);
        }
        let Some(kid) = header.kid else {
            // Without a `kid` the only way to pick a key is to try them all,
            // which is how signers get confused with each other. The IdPs this
            // targets all publish `kid`.
            return Err(ValidateError::MissingKeyId);
        };

        let key = match self.cache.lookup(&kid) {
            Some(key) => key,
            None => {
                // Unknown `kid`: possibly a key rotation we have not picked up
                // yet. One rate-limited refetch, then one retry — never a loop.
                match self.cache.refresh_for_unknown_kid().await {
                    OnDemandOutcome::Fetched | OnDemandOutcome::Suppressed => {}
                }
                match self.cache.lookup(&kid) {
                    Some(key) => key,
                    None if !self.cache.has_keys() => return Err(ValidateError::KeysUnavailable),
                    None => return Err(ValidateError::UnknownKeyId(kid)),
                }
            }
        };

        let mut validation = Validation::new(key.alg);
        validation.leeway = CLOCK_SKEW_LEEWAY_SECS;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.set_issuer(&[self.config.issuer.as_str()]);
        validation.set_audience(&[self.config.audience.as_str()]);
        // Presence is required separately from value checks so a token missing
        // `aud` or `sub` entirely produces a specific error rather than the
        // generic mismatch a value check would report.
        validation.required_spec_claims =
            HashSet::from(["exp", "iss", "aud", "sub"].map(str::to_string));

        let claims = decode::<Map<String, Value>>(token, &key.key, &validation)
            .map_err(|e| ValidateError::from_jwt(e.kind()))?
            .claims;

        let sub = claims
            .get("sub")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or(ValidateError::MissingSubject)?
            .to_string();

        let groups = extract_groups(&claims, groups_claim)?;
        let name = extract_string_claim(&claims, "name");
        let email = extract_string_claim(&claims, "email");

        Ok(ValidatedToken {
            sub,
            groups,
            name,
            email,
        })
    }
}

/// Read a presentation-only string claim: `Some` only for a non-empty string,
/// `None` for anything else (absent, `null`, a number, an object, an empty
/// string).
///
/// Unlike [`extract_groups`], an unreadable shape here is never an error —
/// `name` and `email` carry no authorization meaning, so there is nothing to
/// guess at and refusing a token over a display field would be a real outage
/// caused by a cosmetic claim.
fn extract_string_claim(claims: &Map<String, Value>, name: &str) -> Option<String> {
    claims
        .get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Read the groups claim by name.
///
/// Three shapes are accepted, in decreasing order of how standard they are:
///
/// - **absent or `null` ⇒ no groups.** Not an error: a principal with no group
///   memberships is ordinary, and IdPs routinely omit an empty claim. ADR 0022
///   says so explicitly.
/// - **an array of strings ⇒ those groups.** The normal shape.
/// - **a bare string ⇒ one group.** Some IdPs emit a single-valued claim
///   unwrapped. Accepting it costs nothing and the alternative — rejecting the
///   token — would be a confusing authentication failure for what is really a
///   claim-shape quirk.
///
/// Anything else (a number, an object, an array with a non-string element) is
/// an error rather than a silent empty list: a groups claim we cannot read is
/// a claim whose *authorization* meaning we would be guessing at, and guessing
/// low is still guessing.
fn extract_groups(
    claims: &Map<String, Value>,
    groups_claim: &str,
) -> Result<Vec<String>, ValidateError> {
    match claims.get(groups_claim) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(one)) => Ok(vec![one.clone()]),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| ValidateError::UnreadableGroupsClaim(groups_claim.to_string()))
            })
            .collect(),
        Some(_) => Err(ValidateError::UnreadableGroupsClaim(
            groups_claim.to_string(),
        )),
    }
}

/// Why a bearer token was not accepted.
///
/// Every variant is a 401. None of them carries the token, any part of it, or
/// any claim value beyond a claim *name* — an error string ends up in logs and
/// in HTTP responses, and a bearer token is a credential in both places.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidateError {
    /// The token is not a well-formed JWT.
    #[error("the token is not a well-formed JWT: {0}")]
    Malformed(String),
    /// The header names a symmetric algorithm (or `none`).
    #[error("the token's algorithm is symmetric; only asymmetric signatures are accepted")]
    SymmetricAlgorithm,
    /// The header carries no `kid`.
    #[error("the token carries no key id")]
    MissingKeyId,
    /// The token's algorithm disagrees with the algorithm its key declares.
    #[error("the token's algorithm does not match its signing key")]
    AlgorithmMismatch,
    /// The `kid` is not in the key set, and a refetch did not find it.
    #[error("no published signing key with key id {0}")]
    UnknownKeyId(String),
    /// No key set has been fetched yet — the edge came up before the IdP was
    /// reachable. Distinct from [`UnknownKeyId`](Self::UnknownKeyId) because
    /// the remedy is entirely different: wait, or fix the IdP.
    #[error("no signing keys are available yet; the identity provider has not been reached")]
    KeysUnavailable,
    /// The signature did not verify under the named key.
    #[error("the token's signature is invalid")]
    BadSignature,
    /// `exp` is in the past, beyond the skew allowance.
    #[error("the token has expired")]
    Expired,
    /// `nbf` is in the future, beyond the skew allowance.
    #[error("the token is not valid yet")]
    NotYetValid,
    /// `iss` is not the configured issuer.
    #[error("the token was issued by a different issuer")]
    WrongIssuer,
    /// `aud` does not contain the configured audience.
    #[error("the token is not addressed to this cluster's audience")]
    WrongAudience,
    /// `aud` is absent entirely.
    #[error("the token carries no audience claim")]
    MissingAudience,
    /// `sub` is absent or empty.
    #[error("the token carries no subject claim")]
    MissingSubject,
    /// The groups claim is present but not a string or array of strings.
    #[error("the {0} claim is not a string or an array of strings")]
    UnreadableGroupsClaim(String),
}

impl ValidateError {
    /// Map `jsonwebtoken`'s error kinds onto this crate's vocabulary.
    ///
    /// Keeping the mapping in one place is what makes SHARED.md's "write the
    /// tests against behaviour so a library swap is mechanical" true: the
    /// tests assert these variants, and a different JWT library would need
    /// only this function rewritten.
    fn from_jwt(kind: &ErrorKind) -> ValidateError {
        match kind {
            ErrorKind::ExpiredSignature => ValidateError::Expired,
            ErrorKind::ImmatureSignature => ValidateError::NotYetValid,
            ErrorKind::InvalidIssuer => ValidateError::WrongIssuer,
            ErrorKind::InvalidAudience => ValidateError::WrongAudience,
            ErrorKind::InvalidSignature => ValidateError::BadSignature,
            ErrorKind::InvalidAlgorithm => ValidateError::AlgorithmMismatch,
            ErrorKind::MissingRequiredClaim(name) => match name.as_str() {
                "aud" => ValidateError::MissingAudience,
                "sub" => ValidateError::MissingSubject,
                "iss" => ValidateError::WrongIssuer,
                _ => ValidateError::Malformed(format!("the {name} claim is missing")),
            },
            other => ValidateError::Malformed(format!("{other:?}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn claims(value: Value) -> Map<String, Value> {
        value.as_object().expect("object").clone()
    }

    #[test]
    fn groups_shapes() {
        let c = claims(json!({ "groups": ["a", "b"] }));
        assert_eq!(extract_groups(&c, "groups").unwrap(), vec!["a", "b"]);

        let c = claims(json!({ "groups": "solo" }));
        assert_eq!(extract_groups(&c, "groups").unwrap(), vec!["solo"]);

        let c = claims(json!({ "sub": "x" }));
        assert!(extract_groups(&c, "groups").unwrap().is_empty());

        let c = claims(json!({ "groups": null }));
        assert!(extract_groups(&c, "groups").unwrap().is_empty());

        let c = claims(json!({ "roles": ["a"] }));
        assert_eq!(extract_groups(&c, "roles").unwrap(), vec!["a"]);
        assert!(extract_groups(&c, "groups").unwrap().is_empty());

        let c = claims(json!({ "groups": [1, 2] }));
        assert!(matches!(
            extract_groups(&c, "groups"),
            Err(ValidateError::UnreadableGroupsClaim(_))
        ));

        let c = claims(json!({ "groups": { "a": true } }));
        assert!(matches!(
            extract_groups(&c, "groups"),
            Err(ValidateError::UnreadableGroupsClaim(_))
        ));
    }

    #[test]
    fn string_claim_shapes() {
        let c = claims(json!({ "name": "Alice Example" }));
        assert_eq!(
            extract_string_claim(&c, "name"),
            Some("Alice Example".to_string())
        );

        let c = claims(json!({ "sub": "x" }));
        assert_eq!(extract_string_claim(&c, "name"), None);

        let c = claims(json!({ "name": 42 }));
        assert_eq!(extract_string_claim(&c, "name"), None);

        let c = claims(json!({ "name": { "first": "Alice" } }));
        assert_eq!(extract_string_claim(&c, "name"), None);

        let c = claims(json!({ "name": "" }));
        assert_eq!(extract_string_claim(&c, "name"), None);

        let c = claims(json!({ "name": null }));
        assert_eq!(extract_string_claim(&c, "name"), None);
    }

    #[test]
    fn missing_claim_mapping_is_specific() {
        assert_eq!(
            ValidateError::from_jwt(&ErrorKind::MissingRequiredClaim("aud".into())),
            ValidateError::MissingAudience
        );
        assert_eq!(
            ValidateError::from_jwt(&ErrorKind::MissingRequiredClaim("sub".into())),
            ValidateError::MissingSubject
        );
    }
}
