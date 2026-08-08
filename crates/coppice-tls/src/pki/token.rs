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
    /// A [`TokenKdf`] cost argon2 refuses (zero iterations, zero lanes,
    /// memory below `8 × p_cost` KiB, …).
    #[error("invalid argon2 cost parameters: {0}")]
    InvalidParams(String),
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

/// Argon2id cost parameters for hashing enrollment-token secrets.
///
/// The defaults are the `argon2` crate's own recommended parameters — the
/// exact costs [`hash_secret`] has always used. Lowering them weakens every
/// hash minted under them; the one legitimate reason is a test or dev fleet
/// that mints throwaway tokens by the dozen and must not pay hundreds of
/// milliseconds of deliberate KDF work per mint. Verification reads its
/// parameters from the stored PHC string, so tokens minted cheap verify
/// cheap with no second knob — and a fleet with mixed-cost hashes verifies
/// each against the cost it was minted with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenKdf {
    /// Memory cost in KiB.
    pub m_cost_kib: u32,
    /// Iteration count.
    pub t_cost: u32,
    /// Parallelism lanes.
    pub p_cost: u32,
}

impl TokenKdf {
    /// Check this cost is one argon2 will accept (it rejects zero iterations,
    /// zero lanes, and memory below `8 × p_cost` KiB), so a configured cost
    /// can fail at daemon startup instead of at the first mint — a daemon
    /// with no seeded tokens would otherwise start cleanly and only surface
    /// the bad config as internal errors on every later mint request.
    pub fn validate(&self) -> Result<(), TokenError> {
        self.params().map(|_| ())
    }

    fn params(&self) -> Result<argon2::Params, TokenError> {
        argon2::Params::new(self.m_cost_kib, self.t_cost, self.p_cost, None)
            .map_err(|e| TokenError::InvalidParams(e.to_string()))
    }
}

impl Default for TokenKdf {
    fn default() -> Self {
        let params = argon2::Params::default();
        TokenKdf {
            m_cost_kib: params.m_cost(),
            t_cost: params.t_cost(),
            p_cost: params.p_cost(),
        }
    }
}

/// Hash `secret` with argon2id at [`TokenKdf::default`] cost and a fresh
/// random salt, returning the PHC string (`$argon2id$v=19$m=…$<salt>$<hash>`)
/// stored in replicated policy.
pub fn hash_secret(secret: &str) -> Result<String, TokenError> {
    hash_secret_with(secret, TokenKdf::default())
}

/// As [`hash_secret`], at an explicit cost.
pub fn hash_secret_with(secret: &str, kdf: TokenKdf) -> Result<String, TokenError> {
    let argon = Argon2::new(
        argon2::Algorithm::Argon2id,
        argon2::Version::V0x13,
        kdf.params()?,
    );
    let salt = SaltString::generate(&mut OsRng);
    let hash = argon
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
