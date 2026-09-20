//! Job metadata: small, mutable, user-owned annotations on a job (ADR 0042).
//!
//! A job's metadata is a map from string keys to string values. There is no
//! value type: a value is a UTF-8 string, and any structure a caller wants is
//! theirs to encode in it.
//!
//! The server enforces the limits below at admission and again at apply. This
//! crate checks them too, in [`JobMetadata::insert`] and
//! [`JobMetadata::validate`], so a typo in a key fails locally with a typed
//! error instead of costing a round trip. Decoding is deliberately *not*
//! checked: whatever a server sends is read back faithfully, even if a future
//! server relaxes a limit this client still believes in.

use std::collections::btree_map;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Maximum number of keys in one job's metadata map.
pub const MAX_KEYS: usize = 64;
/// Maximum length of one metadata key, in bytes.
pub const MAX_KEY_BYTES: usize = 64;
/// Maximum length of one metadata value, in bytes of UTF-8.
pub const MAX_VALUE_BYTES: usize = 1024;

/// Why a metadata map or entry was refused.
///
/// Every variant names the offending key, because that is the one thing the
/// caller needs to fix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// More than [`MAX_KEYS`] keys.
    #[error("metadata carries {count} keys, more than the limit of 64")]
    TooManyKeys {
        /// How many keys the map held.
        count: usize,
    },
    /// A key outside the 1..=[`MAX_KEY_BYTES`] byte range.
    #[error("metadata key {key:?} is {len} bytes; keys are 1 to 64 bytes")]
    KeyLength {
        /// The offending key.
        key: String,
        /// Its length in bytes.
        len: usize,
    },
    /// A key with a character outside the allowed set.
    #[error(
        "metadata key {key:?} contains a character outside the allowed set \
         (ASCII letters, digits, and `.` `_` `-` `/` `:`)"
    )]
    KeyCharset {
        /// The offending key.
        key: String,
    },
    /// A value longer than [`MAX_VALUE_BYTES`].
    #[error("metadata value for key {key:?} is {size} bytes, more than the limit of 1024")]
    ValueTooLarge {
        /// The key whose value was too large.
        key: String,
        /// The value's length in bytes.
        size: usize,
    },
}

/// A job's metadata map.
///
/// Keys are ordered, so iteration — and therefore the wire encoding — is
/// deterministic. On the wire this is a plain JSON object of strings, so it
/// serializes transparently.
///
/// ```
/// use coppice_client::JobMetadata;
///
/// let mut metadata = JobMetadata::new();
/// metadata.insert("name", "nightly-train")?;
/// metadata.insert("ticket", "INC-1234")?;
/// assert_eq!(metadata.get("name"), Some("nightly-train"));
///
/// // A key that could never be stored fails here, not at the server.
/// assert!(metadata.insert("has space", "v").is_err());
/// # Ok::<(), coppice_client::MetadataError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobMetadata(BTreeMap<String, String>);

impl JobMetadata {
    /// An empty map.
    pub fn new() -> JobMetadata {
        JobMetadata::default()
    }

    /// Insert one entry, checking the key and value against the ADR 0042
    /// limits.
    ///
    /// The whole-map key count is *not* checked here — that is
    /// [`validate`](Self::validate)'s job, and a caller building a map up may
    /// legitimately pass through an over-large intermediate state only if they
    /// then remove keys. In practice a request is validated once before it is
    /// sent.
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, MetadataError> {
        let key = key.into();
        let value = value.into();
        validate_key(&key)?;
        if value.len() > MAX_VALUE_BYTES {
            return Err(MetadataError::ValueTooLarge {
                size: value.len(),
                key,
            });
        }
        Ok(self.0.insert(key, value))
    }

    /// The value stored under `key`, if any.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Whether `key` is present, whatever its value.
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    /// Remove `key`, returning the value it held.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    /// How many keys the map holds.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterate the entries in key order.
    pub fn iter(&self) -> btree_map::Iter<'_, String, String> {
        self.0.iter()
    }

    /// The underlying map, for a caller that wants the whole thing.
    pub fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }

    /// Check the whole map against every ADR 0042 limit.
    ///
    /// Checks run cheapest-first and in key order, matching the server, so a
    /// map refused here is refused there with the same reason.
    pub fn validate(&self) -> Result<(), MetadataError> {
        if self.0.len() > MAX_KEYS {
            return Err(MetadataError::TooManyKeys {
                count: self.0.len(),
            });
        }
        for (key, value) in &self.0 {
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
}

impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for JobMetadata {
    /// Collect entries **without** checking them — call
    /// [`validate`](JobMetadata::validate) afterwards, or use
    /// [`insert`](JobMetadata::insert) to check as you go. This impl exists so
    /// a literal map is cheap to write in a test or an example.
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> JobMetadata {
        JobMetadata(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl<'a> IntoIterator for &'a JobMetadata {
    type Item = (&'a String, &'a String);
    type IntoIter = btree_map::Iter<'a, String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl From<BTreeMap<String, String>> for JobMetadata {
    fn from(map: BTreeMap<String, String>) -> JobMetadata {
        JobMetadata(map)
    }
}

impl From<JobMetadata> for BTreeMap<String, String> {
    fn from(metadata: JobMetadata) -> BTreeMap<String, String> {
        metadata.0
    }
}

/// Check one metadata key against the length and charset rules.
///
/// Exposed because the `metadata` filter leaf takes a key under exactly these
/// rules: a key that could never be stored can never match, and refusing it
/// names the typo.
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

    #[test]
    fn a_map_serializes_as_a_plain_object() {
        let metadata: JobMetadata = [("name", "nightly"), ("attempt.count", "3")]
            .into_iter()
            .collect();
        assert_eq!(
            serde_json::to_value(&metadata).unwrap(),
            serde_json::json!({ "attempt.count": "3", "name": "nightly" })
        );
        let back: JobMetadata =
            serde_json::from_value(serde_json::json!({ "name": "nightly" })).unwrap();
        assert_eq!(back.get("name"), Some("nightly"));
    }

    #[test]
    fn insert_checks_the_key_and_value() {
        let mut metadata = JobMetadata::new();
        assert!(metadata.insert("a.b_c-d/e:f", "v").is_ok());
        assert!(matches!(
            metadata.insert("has space", "v"),
            Err(MetadataError::KeyCharset { .. })
        ));
        assert!(matches!(
            metadata.insert("", "v"),
            Err(MetadataError::KeyLength { .. })
        ));
        assert!(matches!(
            metadata.insert("k", "x".repeat(MAX_VALUE_BYTES + 1)),
            Err(MetadataError::ValueTooLarge { .. })
        ));
        assert!(metadata.insert("k", "x".repeat(MAX_VALUE_BYTES)).is_ok());
    }

    #[test]
    fn validate_enforces_the_key_count_at_the_boundary() {
        let at_limit: JobMetadata = (0..MAX_KEYS)
            .map(|i| (format!("k{i}"), String::new()))
            .collect();
        assert_eq!(at_limit.validate(), Ok(()));
        let over: JobMetadata = (0..MAX_KEYS + 1)
            .map(|i| (format!("k{i}"), String::new()))
            .collect();
        assert_eq!(
            over.validate(),
            Err(MetadataError::TooManyKeys {
                count: MAX_KEYS + 1
            })
        );
    }

    /// The server may one day relax a limit; a response must still decode.
    #[test]
    fn decoding_does_not_enforce_the_limits() {
        let oversized = serde_json::json!({ "has space": "v" });
        let metadata: JobMetadata = serde_json::from_value(oversized).unwrap();
        assert_eq!(metadata.get("has space"), Some("v"));
        assert!(metadata.validate().is_err());
    }
}
