//! Job environment: the immutable environment-variable overlay a submission
//! carries into its container.
//!
//! A job's `env` is a map from variable name to value, replicated with the
//! rest of the spec and fixed at submission — there is no update command, no
//! route, and no event. It reaches the agent on `StartJob` and becomes the
//! container's `Env` on create, layered *over* the image's own `ENV`: a name
//! the job sets wins, and a name it does not set keeps whatever the image
//! gave it.
//!
//! Names are POSIX portable variable names, which is what makes the Docker
//! `NAME=value` encoding unambiguous: a portable name can contain neither
//! `=` nor NUL, so the first `=` in an encoded entry is always the
//! separator.
//!
//! It is **not** a secret channel. The map is stored in replicated state,
//! rides every snapshot, and is served through the API and the UI
//! (`docs/decisions/0011-container-security-posture.md`), so a value put
//! here is readable by anyone who can read the job.

use std::collections::BTreeMap;

/// A job's environment overlay. Names are ordered, so iteration — and
/// therefore the canonical wire encoding and the `NAME=value` list handed to
/// Docker — is deterministic.
pub type JobEnv = BTreeMap<String, String>;

/// Maximum number of variables in one job's environment.
pub const MAX_VARS: usize = 64;
/// Maximum length of one variable name, in bytes.
pub const MAX_NAME_BYTES: usize = 128;
/// Maximum length of one variable value, in bytes of UTF-8.
pub const MAX_VALUE_BYTES: usize = 4096;
/// Maximum total size of the map: the sum of every name's and value's bytes.
///
/// The per-variable limits alone would allow 64 × (128 + 4096) bytes on a
/// job that is otherwise tiny; this bounds what one submission can cost the
/// replicated, snapshotted state.
pub const MAX_TOTAL_BYTES: usize = 32 * 1024;

/// Why an environment map was refused.
///
/// Rendered into `RejectionReason::InvalidJobEnv` at apply and into an
/// `INVALID_ARGUMENT` at the API edge, so every variant names the offending
/// variable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvError {
    #[error("environment carries {count} variables, more than the limit of 64")]
    TooManyVars { count: usize },
    #[error("environment variable name {name:?} is {len} bytes; names are 1 to 128 bytes")]
    NameLength { name: String, len: usize },
    #[error(
        "environment variable name {name:?} is not a portable name \
         (an ASCII letter or `_`, then ASCII letters, digits, and `_`)"
    )]
    NameCharset { name: String },
    #[error("environment value for {name:?} is {size} bytes, more than the limit of 4096")]
    ValueTooLarge { name: String, size: usize },
    #[error("environment value for {name:?} contains a NUL byte")]
    ValueNul { name: String },
    #[error("environment totals {total} bytes of names and values, more than the limit of 32768")]
    TooLarge { total: usize },
}

/// Check an environment map against every limit.
///
/// Enforced at the API for admission and re-checked at apply, so the
/// replicated state can never hold an oversized map whatever the proposer
/// did. Checks run cheapest-first and in a deterministic order (variable
/// count, then per-variable in ascending name order, then the total), so
/// every replica computes the identical rejection text.
pub fn validate(env: &JobEnv) -> Result<(), EnvError> {
    if env.len() > MAX_VARS {
        return Err(EnvError::TooManyVars { count: env.len() });
    }
    let mut total = 0usize;
    for (name, value) in env {
        validate_name(name)?;
        if value.len() > MAX_VALUE_BYTES {
            return Err(EnvError::ValueTooLarge {
                name: name.clone(),
                size: value.len(),
            });
        }
        if value.as_bytes().contains(&0) {
            return Err(EnvError::ValueNul { name: name.clone() });
        }
        total = total.saturating_add(name.len()).saturating_add(value.len());
    }
    if total > MAX_TOTAL_BYTES {
        return Err(EnvError::TooLarge { total });
    }
    Ok(())
}

/// Check one variable name against the length and charset rules.
fn validate_name(name: &str) -> Result<(), EnvError> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES {
        return Err(EnvError::NameLength {
            name: name.to_string(),
            len: name.len(),
        });
    }
    let bytes = name.as_bytes();
    // A portable name never starts with a digit, so `1PATH` — which no shell
    // can reference — is refused rather than quietly shipped.
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        || !bytes[1..]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        return Err(EnvError::NameCharset {
            name: name.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(entries: &[(&str, &str)]) -> JobEnv {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn validate_accepts_a_typical_map() {
        let env = map(&[
            ("PATH", "/usr/local/bin:/usr/bin:/bin"),
            ("RUST_LOG", "info"),
            ("_PRIVATE", "1"),
            ("SHARD9", "9"),
            ("EMPTY", ""),
        ]);
        assert_eq!(validate(&env), Ok(()));
        assert_eq!(validate(&JobEnv::new()), Ok(()));
    }

    #[test]
    fn validate_rejects_names_that_are_not_portable() {
        for bad in ["1PATH", "has space", "WITH-DASH", "a.b", "WITH=EQUALS", "é"] {
            assert_eq!(
                validate(&map(&[(bad, "v")])),
                Err(EnvError::NameCharset {
                    name: bad.to_string()
                }),
                "{bad:?} must be refused"
            );
        }
    }

    #[test]
    fn validate_enforces_the_name_length_bounds() {
        assert_eq!(
            validate(&map(&[("", "v")])),
            Err(EnvError::NameLength {
                name: String::new(),
                len: 0
            })
        );
        // 128 bytes is the boundary: allowed, 129 is not.
        assert_eq!(
            validate(&map(&[(&"A".repeat(MAX_NAME_BYTES), "v")])),
            Ok(())
        );
        assert_eq!(
            validate(&map(&[(&"A".repeat(MAX_NAME_BYTES + 1), "v")])),
            Err(EnvError::NameLength {
                name: "A".repeat(MAX_NAME_BYTES + 1),
                len: MAX_NAME_BYTES + 1
            })
        );
    }

    #[test]
    fn validate_enforces_the_variable_count_limit_at_the_boundary() {
        let at_limit: JobEnv = (0..MAX_VARS)
            .map(|i| (format!("V{i}"), String::new()))
            .collect();
        assert_eq!(validate(&at_limit), Ok(()));
        let over: JobEnv = (0..MAX_VARS + 1)
            .map(|i| (format!("V{i}"), String::new()))
            .collect();
        assert_eq!(
            validate(&over),
            Err(EnvError::TooManyVars {
                count: MAX_VARS + 1
            })
        );
    }

    #[test]
    fn validate_enforces_the_value_size_limit_at_the_boundary() {
        assert_eq!(
            validate(&map(&[("V", &"x".repeat(MAX_VALUE_BYTES))])),
            Ok(())
        );
        assert_eq!(
            validate(&map(&[("V", &"x".repeat(MAX_VALUE_BYTES + 1))])),
            Err(EnvError::ValueTooLarge {
                name: "V".to_string(),
                size: MAX_VALUE_BYTES + 1
            })
        );
        // The limit is bytes of UTF-8, not characters: a 3-byte character
        // counts three times.
        assert!(validate(&map(&[("V", &"☃".repeat(MAX_VALUE_BYTES / 3 + 1))])).is_err());
    }

    #[test]
    fn validate_rejects_a_nul_in_a_value() {
        assert_eq!(
            validate(&map(&[("V", "a\0b")])),
            Err(EnvError::ValueNul {
                name: "V".to_string()
            })
        );
    }

    #[test]
    fn validate_enforces_the_total_size_limit() {
        // Seven variables of 4096-byte values sit under the 32 KiB total;
        // eight do not, even though every per-variable limit is respected.
        let value = "x".repeat(MAX_VALUE_BYTES);
        let under: JobEnv = (0..7).map(|i| (format!("V{i}"), value.clone())).collect();
        assert_eq!(validate(&under), Ok(()));
        let over: JobEnv = (0..8).map(|i| (format!("V{i}"), value.clone())).collect();
        assert!(matches!(validate(&over), Err(EnvError::TooLarge { .. })));
    }

    #[test]
    fn error_display_names_the_offending_variable() {
        let err = validate(&map(&[("bad name", "v")])).unwrap_err();
        assert!(err.to_string().contains("\"bad name\""), "{err}");
        let err = validate(&map(&[("V", &"x".repeat(MAX_VALUE_BYTES + 1))])).unwrap_err();
        assert!(err.to_string().contains("\"V\""), "{err}");
    }
}
