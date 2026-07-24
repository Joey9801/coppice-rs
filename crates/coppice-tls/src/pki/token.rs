//! Enrollment-token secrets: generation, hashing, verification (ADR 0037 §5).
//!
//! An enrollment token authorizes `POST /api/v1/enroll`. The **secret** is a
//! high-entropy bearer string handed to the enrolling machine; only its
//! **salted hash** is stored in replicated policy (listable/revocable), and the
//! secret is never derivable from the hash. This module owns the crypto half —
//! generating the secret, hashing it (argon2id, PHC string format), and the
//! constant-time verify. The replicated record (id, role, hash, TTL, label) is
//! the state layer's concern.

use argon2::password_hash::rand_core::{OsRng, RngCore};
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use base64::Engine;

/// The random-secret length: 256 bits.
const SECRET_BYTES: usize = 32;

/// A failure hashing a token secret.
#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    /// argon2 failed to hash the secret (effectively unreachable for a
    /// well-formed secret; surfaced rather than panicked on).
    #[error("hashing enrollment token secret: {0}")]
    Hash(String),
}

/// The recognizable prefix on every enrollment-token secret (`cpk_` = Coppice
/// PKI). Makes a leaked secret greppable and a mistyped one obvious.
pub const TOKEN_PREFIX: &str = "cpk_";

/// Generate a fresh enrollment-token secret: [`TOKEN_PREFIX`] followed by 256
/// bits of OS-CSPRNG randomness, URL-safe-base64 without padding (so the whole
/// string is a single header-safe token).
pub fn generate_secret() -> String {
    let mut raw = [0u8; SECRET_BYTES];
    OsRng.fill_bytes(&mut raw);
    let body = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
    format!("{TOKEN_PREFIX}{body}")
}

/// Hash `secret` with argon2id and a fresh random salt, returning the PHC
/// string (`$argon2id$v=19$m=…$<salt>$<hash>`) stored in replicated policy.
pub fn hash_secret(secret: &str) -> Result<String, TokenError> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|e| TokenError::Hash(e.to_string()))?;
    Ok(hash.to_string())
}

/// Verify `secret` against a stored PHC `phc_hash` in constant time (argon2's
/// own verify). Returns `false` — never panics — on a malformed hash string, so
/// a corrupt stored record can never crash the enrollment path.
pub fn verify_secret(secret: &str, phc_hash: &str) -> bool {
    match PasswordHash::new(phc_hash) {
        Ok(parsed) => Argon2::default()
            .verify_password(secret.as_bytes(), &parsed)
            .is_ok(),
        Err(_) => false,
    }
}
