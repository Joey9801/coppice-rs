//! External interruption sources (ADR 0044 §5, "External interruption").
//!
//! A source watches the host's provider for a reclaim notice and publishes the
//! latest one on a [`tokio::sync::watch`] that the session loop consumes: a
//! notice starts the node's drain plan
//! ([`Session::on_interruption`](crate::session::Session::on_interruption)).
//! The source only reports; every decision about what a notice means is the
//! session's.

use coppice_core::time::Timestamp;

/// What the provider will do to the host. All three end the containers, and
/// the agent treats them alike (ADR 0044 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceAction {
    Terminate,
    Stop,
    /// Treated as `terminate`.
    Hibernate,
}

/// One reclaim notice: the provider's action and its deadline `D`, wall-clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interruption {
    pub action: InstanceAction,
    pub deadline: Timestamp,
}
