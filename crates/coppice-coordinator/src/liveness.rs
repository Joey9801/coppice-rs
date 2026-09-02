//! Node liveness tracking for the leader's health monitor (ADR 0009).
//!
//! A shared map, deliberately **not** a channel. `mark` is called from the
//! ingestion loop on every inbound report; `seed`/`last_seen` from
//! housekeeping when it decides whether a silent node has missed the
//! liveness deadline. Both callers are leader-only, and a plain
//! `Mutex<BTreeMap>` introduces no `.await` edge into or out of these calls —
//! so it keeps the blocking-edge graph acyclic (`docs/architecture/coordinator-runtime.md`,
//! "Deadlock-freedom"), unlike a channel would.
//!
//! The map carries two clocks per node on purpose. The monotonic `Instant`
//! backs the health monitor's deadline arithmetic (spans between process-local
//! instants, immune to wall-clock steps). The wall `Timestamp` is the
//! displayable fact behind `NodeSummary.last_heartbeat` — stamped only by a
//! real report, never by `seed`, so a granted grace window can't masquerade
//! as a heartbeat.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use coppice_core::id::NodeId;
use coppice_core::time::Timestamp;

/// What the map remembers about one node.
#[derive(Clone, Copy)]
struct Seen {
    /// Monotonic last-seen, the health monitor's deadline input. Granted
    /// afresh by [`NodeLiveness::seed`] on each leadership gain.
    instant: Instant,
    /// Wall-clock stamp of the last *actual* report of any shape — `None`
    /// for a node this process has only seeded, never heard from.
    heartbeat_at: Option<Timestamp>,
}

/// Shared last-seen instants for every node the leader is tracking.
///
/// Cloneable: every clone shares the same map. The map persists across
/// leadership terms (constructed once by `runtime::run`); [`seed`](Self::seed)
/// re-grants a grace window on each leadership gain.
#[derive(Clone, Default)]
pub struct NodeLiveness(Arc<Mutex<BTreeMap<NodeId, Seen>>>);

impl NodeLiveness {
    pub fn new() -> Self {
        NodeLiveness::default()
    }

    /// Record that `node` was just heard from (any report shape counts).
    pub fn mark(&self, node: NodeId) {
        self.0.lock().expect("liveness map poisoned").insert(
            node,
            Seen {
                instant: Instant::now(),
                heartbeat_at: Some(Timestamp::now()),
            },
        );
    }

    /// On gaining leadership, grant every currently-known node a fresh grace
    /// window (`now`) so no node is declared lost before its first report of
    /// the new term. Overwrites any stale last-seen from a prior term — but
    /// leaves the wall-clock heartbeat stamp alone: the grace window is a
    /// monitor courtesy, not a report.
    pub fn seed(&self, nodes: impl IntoIterator<Item = NodeId>, now: Instant) {
        let mut map = self.0.lock().expect("liveness map poisoned");
        for node in nodes {
            map.entry(node)
                .and_modify(|seen| seen.instant = now)
                .or_insert(Seen {
                    instant: now,
                    heartbeat_at: None,
                });
        }
    }

    /// The last instant `node` was heard from, if it is being tracked.
    pub fn last_seen(&self, node: NodeId) -> Option<Instant> {
        self.0
            .lock()
            .expect("liveness map poisoned")
            .get(&node)
            .map(|seen| seen.instant)
    }

    /// The wall-clock stamp of the last actual report from every node this
    /// process has heard from — the API's `last_heartbeat` source
    /// (`UsageSnapshot::heartbeats`). Seed-only entries are absent: a node
    /// granted grace but never heard from has no heartbeat to report.
    pub fn heartbeats(&self) -> BTreeMap<NodeId, Timestamp> {
        self.0
            .lock()
            .expect("liveness map poisoned")
            .iter()
            .filter_map(|(node, seen)| seen.heartbeat_at.map(|at| (*node, at)))
            .collect()
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

    #[test]
    fn mark_stamps_a_wall_clock_heartbeat() {
        let liveness = NodeLiveness::new();
        let node = NodeId::new();
        assert!(liveness.heartbeats().is_empty());

        let before = Timestamp::now();
        liveness.mark(node);
        let after = Timestamp::now();

        let at = liveness.heartbeats()[&node];
        assert!(before <= at && at <= after);
    }

    /// A seeded grace window is monitor bookkeeping, not a report: it must
    /// never fabricate a heartbeat stamp, and must never erase a real one.
    #[test]
    fn seed_neither_fabricates_nor_erases_a_heartbeat() {
        let liveness = NodeLiveness::new();
        let (seeded_only, heard) = (NodeId::new(), NodeId::new());

        liveness.mark(heard);
        let heard_at = liveness.heartbeats()[&heard];

        liveness.seed([seeded_only, heard], Instant::now());

        let heartbeats = liveness.heartbeats();
        assert!(!heartbeats.contains_key(&seeded_only));
        assert_eq!(heartbeats[&heard], heard_at);
    }
}
