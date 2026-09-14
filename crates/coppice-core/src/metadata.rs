//! Job metadata: small, mutable, replicated key/value annotations.
//!
//! A job's `metadata` is a map from string keys to string values, replicated
//! with the job spec and mutated after submission by `UpdateJobMetadata`
//! (`docs/decisions/0042-job-metadata.md`). It is descriptive only: the
//! scheduler, admission, quota arithmetic and the executor never read it.
//!
//! There is no value type: a value is a UTF-8 string, and any structure a
//! caller wants is theirs to encode in it. That keeps every problem a
//! recursive JSON value brought — a canonical text form, a numeric-equality
//! rule across renderings, depth limits, nested conversion — out of the
//! corpus, and keeps floating point out of a replicated `Eq` type.

use std::collections::BTreeMap;

/// A job's metadata map. Keys are ordered, so iteration — and therefore the
/// canonical wire encoding — is deterministic.
pub type JobMetadata = BTreeMap<String, String>;

/// Maximum number of keys in one job's metadata map.
pub const MAX_KEYS: usize = 64;
/// Maximum length of one metadata key, in bytes.
pub const MAX_KEY_BYTES: usize = 64;
/// Maximum length of one metadata value, in bytes of UTF-8.
pub const MAX_VALUE_BYTES: usize = 1024;

/// Why a metadata map was refused.
///
/// Rendered into `RejectionReason::InvalidJobMetadata` at apply and into an
/// `INVALID_ARGUMENT` at the API edge, so every variant names the offending
/// key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetadataError {
    #[error("metadata carries {count} keys, more than the limit of 64")]
    TooManyKeys { count: usize },
    #[error("metadata key {key:?} is {len} bytes; keys are 1 to 64 bytes")]
    KeyLength { key: String, len: usize },
    #[error(
        "metadata key {key:?} contains a character outside the allowed set \
         (ASCII letters, digits, and `.` `_` `-` `/` `:`)"
    )]
    KeyCharset { key: String },
    #[error("metadata value for key {key:?} is {size} bytes, more than the limit of 1024")]
    ValueTooLarge { key: String, size: usize },
}

/// Check a metadata map against every limit in ADR 0042.
///
/// Enforced at the API for admission and every mutation, and re-checked at
/// apply, so the replicated state can never hold an oversized map whatever
/// the proposer did. Checks run cheapest-first and in a deterministic order
/// (key count, then per-key in ascending key order), so every replica
/// computes the identical rejection text.
pub fn validate(metadata: &JobMetadata) -> Result<(), MetadataError> {
    if metadata.len() > MAX_KEYS {
        return Err(MetadataError::TooManyKeys {
            count: metadata.len(),
        });
    }
    for (key, value) in metadata {
        validate_key(key)?;
        if value.len() > MAX_VALUE_BYTES {
            return Err(MetadataError::ValueTooLarge {
                key: key.clone(),
                size: value.len(),
            });
        }
    }
    Ok(())
}

/// Check one metadata key against the length and charset rules.
///
/// Exposed for the `ListJobs` `metadata` filter leaf, whose key is validated
/// under the stored-key rules: a key that could never be stored can never
/// match, and refusing it names the typo.
pub fn validate_key(key: &str) -> Result<(), MetadataError> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(MetadataError::KeyLength {
            key: key.to_string(),
            len: key.len(),
        });
    }
    if !key.bytes().all(is_key_byte) {
        return Err(MetadataError::KeyCharset {
            key: key.to_string(),
        });
    }
    Ok(())
}

/// ASCII letters, digits, and `.` `_` `-` `/` `:` — the metadata key charset.
fn is_key_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/' | b':')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(entries: &[(&str, &str)]) -> JobMetadata {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn validate_accepts_a_typical_map() {
        let metadata = map(&[
            ("name", "nightly-train"),
            ("ticket", "INC-1234"),
            ("retry.of", "job-1683852a-0000-0000-0000-000000000000"),
            ("attempt/count", "3"),
            ("flaky:seen", "true"),
            ("notes", ""),
        ]);
        assert_eq!(validate(&metadata), Ok(()));
        assert_eq!(validate(&JobMetadata::new()), Ok(()));
    }

    #[test]
    fn validate_rejects_bad_keys() {
        assert_eq!(
            validate(&map(&[("", "v")])),
            Err(MetadataError::KeyLength {
                key: String::new(),
                len: 0
            })
        );
        // 64 bytes is the boundary: allowed, 65 is not.
        assert_eq!(validate(&map(&[(&"k".repeat(64), "v")])), Ok(()));
        assert_eq!(
            validate(&map(&[(&"k".repeat(65), "v")])),
            Err(MetadataError::KeyLength {
                key: "k".repeat(65),
                len: 65
            })
        );
        for bad in ["has space", "emoji🙂", "brace{", "under~score"] {
            assert_eq!(
                validate(&map(&[(bad, "v")])),
                Err(MetadataError::KeyCharset {
                    key: bad.to_string()
                }),
                "{bad:?} must be refused"
            );
        }
    }

    #[test]
    fn validate_key_matches_the_stored_key_rules() {
        assert_eq!(validate_key("name"), Ok(()));
        assert_eq!(validate_key("a.b_c-d/e:f"), Ok(()));
        assert!(validate_key("").is_err());
        assert!(validate_key("has space").is_err());
        assert!(validate_key(&"k".repeat(65)).is_err());
    }

    #[test]
    fn validate_enforces_the_key_count_limit_at_the_boundary() {
        let at_limit: JobMetadata = (0..MAX_KEYS)
            .map(|i| (format!("k{i}"), String::new()))
            .collect();
        assert_eq!(validate(&at_limit), Ok(()));
        let over: JobMetadata = (0..MAX_KEYS + 1)
            .map(|i| (format!("k{i}"), String::new()))
            .collect();
        assert_eq!(
            validate(&over),
            Err(MetadataError::TooManyKeys {
                count: MAX_KEYS + 1
            })
        );
    }

    #[test]
    fn validate_enforces_the_value_size_limit_at_the_boundary() {
        assert_eq!(
            validate(&map(&[("k", &"x".repeat(MAX_VALUE_BYTES))])),
            Ok(())
        );
        assert_eq!(
            validate(&map(&[("k", &"x".repeat(MAX_VALUE_BYTES + 1))])),
            Err(MetadataError::ValueTooLarge {
                key: "k".to_string(),
                size: MAX_VALUE_BYTES + 1
            })
        );
        // The limit is bytes of UTF-8, not characters: a 3-byte character
        // counts three times.
        assert!(validate(&map(&[("k", &"☃".repeat(MAX_VALUE_BYTES / 3 + 1))])).is_err());
    }

    #[test]
    fn error_display_names_the_offending_key() {
        let err = validate(&map(&[("bad key", "v")])).unwrap_err();
        assert!(err.to_string().contains("\"bad key\""), "{err}");
        let err = validate(&map(&[("k", &"x".repeat(MAX_VALUE_BYTES + 1))])).unwrap_err();
        assert!(err.to_string().contains("\"k\""), "{err}");
    }
}
