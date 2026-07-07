//! Strongly-typed identifiers.
//!
//! Every entity in the system carries a distinct id type so that, for example,
//! a [`JobId`] can never be passed where a [`NodeId`] is expected. Ids that
//! appear on the agent protocol (job, allocation, attempt) exist so retries are
//! safe; see `docs/protocols/agent-coordinator.md`.

use uuid::Uuid;

macro_rules! typed_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub Uuid);

        impl $name {
            /// Generate a fresh random identifier.
            ///
            /// Deliberately no `Default`: a defaulted id is always a bug, and
            /// `..Default::default()` struct updates must not mint one silently.
            #[allow(clippy::new_without_default)]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

typed_id!(
    /// Identifies a submitted job across its whole lifecycle.
    JobId
);
typed_id!(
    /// Identifies a compute node registered with the coordinator.
    NodeId
);
typed_id!(
    /// Identifies a single placement of a job onto a node.
    AllocationId
);
typed_id!(
    /// Identifies one execution attempt of a job (retries produce new attempts).
    AttemptId
);
typed_id!(
    /// Identifies a placement group whose attempts share the `Ready` barrier
    /// (gang scheduling). v1 groups are singletons: one job, one group.
    GroupId
);
typed_id!(
    /// Identifies a node in the quota-entity tree (ADR 0005). Levels carry no
    /// built-in meaning; every job is submitted under exactly one leaf.
    QuotaEntityId
);
