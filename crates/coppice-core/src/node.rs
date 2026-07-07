//! Compute node model.
//!
//! A node is described by the resources it advertises and the labels used to
//! satisfy hard placement constraints. Schedulability reflects drain and
//! maintenance state. See `docs/protocols/agent-coordinator.md`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::id::NodeId;
use crate::resource::Resources;

/// Authoritative record of a node's membership and schedulability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    /// Total advertised capacity.
    pub capacity: Resources,
    /// Labels used for hard/soft placement constraints. `BTreeMap` keeps
    /// iteration order deterministic for the replicated state machine.
    pub labels: BTreeMap<String, String>,
    /// Whether the scheduler may place new work here.
    pub schedulable: bool,
}
