//! Strongly-typed identifiers.
//!
//! Every entity in the system carries a distinct id type so that, for example,
//! a [`JobId`] can never be passed where a [`NodeId`] is expected. Ids that
//! appear on the agent protocol (job, allocation, attempt) exist so retries are
//! safe; see `docs/protocols/agent-coordinator.md`.
//!
//! Serialized ids are self-describing: every textual form — `Display`, serde,
//! and the protobuf wire encoding — is `<prefix>-<uuid>`, e.g.
//! `job-1683852a-993f-4497-a48b-6527b458fbd1`, so an id seen in a log line,
//! JSON payload, or captured RPC can never be mistaken for another entity's
//! (ADR 0024).

use uuid::Uuid;

/// A textual id failed to parse as `<prefix>-<uuid>`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {expected} id {input:?}: expected `{prefix}-<uuid>`")]
pub struct ParseIdError {
    /// The id type that was expected (e.g. `JobId`).
    pub expected: &'static str,
    /// The required prefix (e.g. `job`).
    pub prefix: &'static str,
    /// The offending input, truncated for display safety.
    pub input: String,
}

macro_rules! typed_id {
    ($(#[$meta:meta])* $name:ident, $prefix:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub Uuid);

        impl $name {
            /// The type tag every serialized form of this id carries.
            pub const PREFIX: &'static str = $prefix;

            /// Generate a fresh identifier.
            ///
            /// UUIDv7: the millisecond-timestamp prefix makes fresh ids sort
            /// to the right edge of the id-keyed btrees (ADR 0024). Nothing
            /// may *rely* on that ordering — uniqueness rests on the random
            /// tail alone.
            ///
            /// Deliberately no `Default`: a defaulted id is always a bug, and
            /// `..Default::default()` struct updates must not mint one silently.
            #[allow(clippy::new_without_default)]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}-{}", $prefix, self.0)
            }
        }

        impl std::str::FromStr for $name {
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
    /// Identifies one execution attempt of a job (retries produce new attempts).
    AttemptId,
    "attempt"
);
typed_id!(
    /// Identifies a placement group whose attempts share the `Ready` barrier
    /// (gang scheduling). v1 groups are singletons: one job, one group.
    GroupId,
    "group"
);
typed_id!(
    /// Identifies a node in the quota-entity tree (ADR 0005). Levels carry no
    /// built-in meaning; every job is submitted under exactly one leaf.
    QuotaEntityId,
    "quota"
);
typed_id!(
    /// Identifies a coordinator cluster as a whole. Stamped into every raft
    /// transport/admin RPC (as raw bytes at that layer) so replicas from
    /// different clusters can never join each other.
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
    fn wrong_prefix_is_rejected() {
        let id = JobId::new();
        let text = id.to_string();
        // The same uuid under a different type tag must not parse.
        assert!(text.parse::<NodeId>().is_err());
        // And a bare uuid without any tag must not parse either.
        assert!(id.0.to_string().parse::<JobId>().is_err());
    }

    #[test]
    fn serde_uses_the_typed_string_form() {
        let id = AllocationId::new();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"alloc-{}\"", id.0));
        let back: AllocationId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }
}
