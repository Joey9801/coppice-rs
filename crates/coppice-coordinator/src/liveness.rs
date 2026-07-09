//! Node liveness tracking for the leader's health monitor (ADR 0009).
//!
//! A shared map, deliberately **not** a channel. `mark` is called from the
//! ingestion loop on every inbound report; `seed`/`last_seen` from
//! housekeeping when it decides whether a silent node has missed the
//! liveness deadline. Both callers are leader-only, and a plain
//! `Mutex<BTreeMap>` introduces no `.await` edge into or out of these calls —
//! so it keeps the blocking-edge graph acyclic (`docs/architecture/coordinator-runtime.md`,
//! "Deadlock-freedom"), unlike a channel would.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use coppice_core::id::NodeId;

/// Shared last-seen instants for every node the leader is tracking.
///
/// Cloneable: every clone shares the same map. The map persists across
/// leadership terms (constructed once by `runtime::run`); [`seed`](Self::seed)
/// re-grants a grace window on each leadership gain.
#[derive(Clone, Default)]
pub struct NodeLiveness(Arc<Mutex<BTreeMap<NodeId, Instant>>>);

impl NodeLiveness {
    pub fn new() -> Self {
        NodeLiveness::default()
    }

    /// Record that `node` was just heard from (any report shape counts).
    pub fn mark(&self, node: NodeId) {
        self.0
            .lock()
            .expect("liveness map poisoned")
            .insert(node, Instant::now());
    }

    /// On gaining leadership, grant every currently-known node a fresh grace
    /// window (`now`) so no node is declared lost before its first report of
    /// the new term. Overwrites any stale last-seen from a prior term.
    pub fn seed(&self, nodes: impl IntoIterator<Item = NodeId>, now: Instant) {
        let mut map = self.0.lock().expect("liveness map poisoned");
        for node in nodes {
            map.insert(node, now);
        }
    }

    /// The last instant `node` was heard from, if it is being tracked.
    pub fn last_seen(&self, node: NodeId) -> Option<Instant> {
        self.0
            .lock()
            .expect("liveness map poisoned")
            .get(&node)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn mark_and_last_seen_round_trip() {
        let liveness = NodeLiveness::new();
        let node = NodeId::new();
        assert!(liveness.last_seen(node).is_none());
        liveness.mark(node);
        assert!(liveness.last_seen(node).is_some());
    }

    #[test]
    fn seed_grants_grace_to_known_nodes() {
        let liveness = NodeLiveness::new();
        let node = NodeId::new();
        let now = Instant::now();
        liveness.seed([node], now);
        assert_eq!(liveness.last_seen(node), Some(now));
    }

    #[test]
    fn seed_overwrites_a_stale_prior_entry() {
        let liveness = NodeLiveness::new();
        let node = NodeId::new();
        let stale = Instant::now() - Duration::from_secs(3600);
        liveness.seed([node], stale);
        let fresh = Instant::now();
        liveness.seed([node], fresh);
        assert_eq!(liveness.last_seen(node), Some(fresh));
    }
}
