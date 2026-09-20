//! Strongly-typed identifiers.
//!
//! Every entity Coppice names carries its own id type, so a [`JobId`] can
//! never be passed where a [`NodeId`] belongs. Every textual form — `Display`,
//! `FromStr`, and serde — is `<prefix>-<uuid>`, e.g.
//! `job-1683852a-993f-4497-a48b-6527b458fbd1`, which is exactly what the
//! server puts on the wire (ADR 0024).
//!
//! Ids are **client-minted** on two write paths: a job's id and a quota
//! entity's id are the idempotency identity of their submission (ADR 0026), so
//! mint one with [`JobId::new`] per logical submission and re-send it verbatim
//! on every retry.

use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

/// A textual id that was not `<prefix>-<uuid>`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {expected} id {input:?}: expected `{prefix}-<uuid>`")]
pub struct ParseIdError {
    /// The id type that was expected (e.g. `JobId`).
    pub expected: &'static str,
    /// The required prefix (e.g. `job`).
    pub prefix: &'static str,
    /// The offending input, truncated to 64 characters for display safety.
    pub input: String,
}

macro_rules! typed_id {
    ($(#[$meta:meta])* $name:ident, $prefix:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(
            /// The underlying UUID. Public so a caller holding one from
            /// elsewhere can re-tag it without a string round trip.
            pub Uuid,
        );

        impl $name {
            /// The type tag every serialized form of this id carries.
            pub const PREFIX: &'static str = $prefix;

            /// Mint a fresh identifier (UUIDv7, so fresh ids sort to the right
            /// edge of the server's id-keyed maps).
            ///
            /// Deliberately no `Default`: a defaulted id is always a bug.
            #[allow(clippy::new_without_default)]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}-{}", $prefix, self.0)
            }
        }

        impl FromStr for $name {
            type Err = ParseIdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let err = || ParseIdError {
                    expected: stringify!($name),
                    prefix: $prefix,
                    input: s.chars().take(64).collect(),
                };
                let rest = s.strip_prefix(concat!($prefix, "-")).ok_or_else(err)?;
                let uuid = Uuid::try_parse(rest).map_err(|_| err())?;
                Ok(Self(uuid))
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let s = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
                s.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

typed_id!(
    /// Identifies a submitted job across its whole lifecycle.
    JobId,
    "job"
);
typed_id!(
    /// Identifies a compute node registered with the coordinator.
    NodeId,
    "node"
);
typed_id!(
    /// Identifies a single placement of a job onto a node.
    AllocationId,
    "alloc"
);
typed_id!(
    /// Identifies one execution attempt of a job; a retry is a new attempt.
    AttemptId,
    "attempt"
);
typed_id!(
    /// Identifies a node in the quota-entity tree. Every job is submitted
    /// under exactly one entity.
    QuotaEntityId,
    "quota"
);
typed_id!(
    /// Identifies a coordinator cluster as a whole.
    ClusterId,
    "cluster"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_and_parse_round_trip() {
        let id = JobId::new();
        let text = id.to_string();
        assert!(text.starts_with("job-"), "{text}");
        assert_eq!(text.parse::<JobId>().unwrap(), id);
    }

    #[test]
    fn a_wrong_prefix_is_rejected() {
        let id = JobId::new();
        assert!(id.to_string().parse::<NodeId>().is_err());
        assert!(id.0.to_string().parse::<JobId>().is_err());
    }

    #[test]
    fn serde_uses_the_typed_string_form() {
        let id = AllocationId::new();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"alloc-{}\"", id.0));
        assert_eq!(serde_json::from_str::<AllocationId>(&json).unwrap(), id);
    }
}
