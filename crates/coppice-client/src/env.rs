//! A job's environment overlay: the variables set in its container on top of
//! whatever the image declares.
//!
//! The map is fixed at submission — there is no update route — and it is part
//! of the submission's idempotency identity, so a retry must resend it
//! verbatim. It is **not a secret channel**: the environment is replicated
//! with the rest of the spec and served back through the API to anyone who
//! can read the job.
//!
//! The server checks the limits at admission and again at apply.
//! [`JobEnv::insert`] and [`JobEnv::validate`] apply the same rules here, with
//! the same wording, so a bad name fails where it was written rather than as
//! an `INVALID_ARGUMENT` a round trip later. Decoding stays permissive: a
//! response's map is taken as the server sent it.

use std::collections::btree_map;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Maximum number of variables in one job's environment.
pub const MAX_ENV_VARS: usize = 64;
/// Maximum length of one variable name, in bytes.
pub const MAX_ENV_NAME_BYTES: usize = 128;
/// Maximum length of one variable's value, in bytes of UTF-8.
pub const MAX_ENV_VALUE_BYTES: usize = 4096;
/// Maximum total size of every name and value together, in bytes.
pub const MAX_ENV_TOTAL_BYTES: usize = 32 * 1024;

/// Why an environment map or entry was refused.
///
/// Every per-entry variant names the offending variable, because that is the
/// one thing the caller needs to fix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EnvError {
    /// More than [`MAX_ENV_VARS`] variables.
    #[error("environment carries {count} variables, more than the limit of 64")]
    TooManyVars {
        /// How many variables the map held.
        count: usize,
    },
    /// A name outside the 1..=[`MAX_ENV_NAME_BYTES`] byte range.
    #[error("environment variable name {name:?} is {len} bytes; names are 1 to 128 bytes")]
    NameLength {
        /// The offending name.
        name: String,
        /// Its length in bytes.
        len: usize,
    },
    /// A name that is not a portable environment variable name.
    #[error(
        "environment variable name {name:?} is not a portable name \
         (an ASCII letter or `_`, then ASCII letters, digits, and `_`)"
    )]
    NameCharset {
        /// The offending name.
        name: String,
    },
    /// A value longer than [`MAX_ENV_VALUE_BYTES`].
    #[error("environment value for {name:?} is {size} bytes, more than the limit of 4096")]
    ValueTooLarge {
        /// The variable whose value was too large.
        name: String,
        /// The value's length in bytes.
        size: usize,
    },
    /// A value containing a NUL byte, which no process environment can carry.
    #[error("environment value for {name:?} contains a NUL byte")]
    ValueNul {
        /// The variable whose value held the NUL.
        name: String,
    },
    /// Names and values together exceed [`MAX_ENV_TOTAL_BYTES`].
    #[error("environment totals {total} bytes of names and values, more than the limit of 32768")]
    TooLarge {
        /// The combined size in bytes.
        total: usize,
    },
}

/// A job's environment overlay.
///
/// Names are ordered, so iteration — and therefore the wire encoding — is
/// deterministic. On the wire this is a plain JSON object of strings, so it
/// serializes transparently.
///
/// ```
/// use coppice_client::JobEnv;
///
/// let mut env = JobEnv::new();
/// env.insert("RUST_LOG", "info")?;
/// env.insert("EMPTY", "")?; // the empty string is a legal value
/// assert_eq!(env.get("RUST_LOG"), Some("info"));
///
/// // A name no shell could export fails here, not at the server.
/// assert!(env.insert("not-portable", "x").is_err());
/// # Ok::<(), coppice_client::EnvError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobEnv(BTreeMap<String, String>);

impl JobEnv {
    /// An empty environment.
    pub fn new() -> JobEnv {
        JobEnv::default()
    }

    /// Set one variable, checking its name and value against the limits.
    ///
    /// The whole-map limits (variable count and total size) are *not* checked
    /// here — that is [`validate`](Self::validate)'s job, which
    /// [`SubmitJobRequest::validate`](crate::SubmitJobRequest::validate) runs
    /// before a request is sent.
    pub fn insert(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, EnvError> {
        let name = name.into();
        let value = value.into();
        validate_name(&name)?;
        validate_value(&name, &value)?;
        Ok(self.0.insert(name, value))
    }

    /// The value of `name`, if set.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    /// Whether `name` is set, whatever its value.
    pub fn contains_key(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Unset `name`, returning the value it held.
    pub fn remove(&mut self, name: &str) -> Option<String> {
        self.0.remove(name)
    }

    /// How many variables the map holds.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterate the variables in name order.
    pub fn iter(&self) -> btree_map::Iter<'_, String, String> {
        self.0.iter()
    }

    /// The underlying map, for a caller that wants the whole thing.
    pub fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }

    /// Check the whole map against every limit.
    ///
    /// Checks run in the server's order — count, then each entry in name
    /// order, then the total — so a map refused here is refused there with the
    /// same reason.
    pub fn validate(&self) -> Result<(), EnvError> {
        if self.0.len() > MAX_ENV_VARS {
            return Err(EnvError::TooManyVars {
                count: self.0.len(),
            });
        }
        let mut total = 0usize;
        for (name, value) in &self.0 {
            validate_name(name)?;
            validate_value(name, value)?;
            total = total.saturating_add(name.len()).saturating_add(value.len());
        }
        if total > MAX_ENV_TOTAL_BYTES {
            return Err(EnvError::TooLarge { total });
        }
        Ok(())
    }
}

impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for JobEnv {
    /// Collect entries **without** checking them — call
    /// [`validate`](JobEnv::validate) afterwards, or use
    /// [`insert`](JobEnv::insert) to check as you go.
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> JobEnv {
        JobEnv(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl<'a> IntoIterator for &'a JobEnv {
    type Item = (&'a String, &'a String);
    type IntoIter = btree_map::Iter<'a, String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl From<BTreeMap<String, String>> for JobEnv {
    fn from(map: BTreeMap<String, String>) -> JobEnv {
        JobEnv(map)
    }
}

impl From<JobEnv> for BTreeMap<String, String> {
    fn from(env: JobEnv) -> BTreeMap<String, String> {
        env.0
    }
}

/// The portable-name rule: 1 to [`MAX_ENV_NAME_BYTES`] bytes, an ASCII letter
/// or `_` first, then ASCII letters, digits and `_`.
fn validate_name(name: &str) -> Result<(), EnvError> {
    if name.is_empty() || name.len() > MAX_ENV_NAME_BYTES {
        return Err(EnvError::NameLength {
            name: name.to_string(),
            len: name.len(),
        });
    }
    let bytes = name.as_bytes();
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

fn validate_value(name: &str, value: &str) -> Result<(), EnvError> {
    if value.len() > MAX_ENV_VALUE_BYTES {
        return Err(EnvError::ValueTooLarge {
            name: name.to_string(),
            size: value.len(),
        });
    }
    if value.as_bytes().contains(&0) {
        return Err(EnvError::ValueNul {
            name: name.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_checks_the_name_and_the_value() {
        let mut env = JobEnv::new();
        assert!(env.insert("PATH", "/usr/bin").is_ok());
        assert!(env.insert("_PRIVATE", "").is_ok());
        assert!(matches!(
            env.insert("", "x"),
            Err(EnvError::NameLength { len: 0, .. })
        ));
        assert!(matches!(
            env.insert("1ST", "x"),
            Err(EnvError::NameCharset { .. })
        ));
        assert!(matches!(
            env.insert("A-B", "x"),
            Err(EnvError::NameCharset { .. })
        ));
        assert!(matches!(
            env.insert("A", "x".repeat(MAX_ENV_VALUE_BYTES + 1)),
            Err(EnvError::ValueTooLarge { .. })
        ));
        assert!(matches!(
            env.insert("A", "a\0b"),
            Err(EnvError::ValueNul { .. })
        ));
        assert_eq!(env.len(), 2);
    }

    #[test]
    fn validate_checks_the_whole_map_limits() {
        let many: JobEnv = (0..=MAX_ENV_VARS)
            .map(|i| (format!("V{i}"), String::new()))
            .collect();
        assert_eq!(
            many.validate(),
            Err(EnvError::TooManyVars {
                count: MAX_ENV_VARS + 1
            })
        );

        let big: JobEnv = (0..9)
            .map(|i| (format!("V{i}"), "x".repeat(MAX_ENV_VALUE_BYTES)))
            .collect();
        assert!(matches!(big.validate(), Err(EnvError::TooLarge { .. })));
    }

    #[test]
    fn the_wire_form_is_a_plain_object_and_decoding_is_permissive() {
        let env = JobEnv::from_iter([("RUST_LOG", "info"), ("EMPTY", "")]);
        let json = serde_json::to_value(&env).unwrap();
        assert_eq!(json, serde_json::json!({ "EMPTY": "", "RUST_LOG": "info" }));
        // A name this client would refuse still decodes: the server said it.
        let odd: JobEnv = serde_json::from_value(serde_json::json!({ "a-b": "x" })).unwrap();
        assert_eq!(odd.get("a-b"), Some("x"));
    }
}
