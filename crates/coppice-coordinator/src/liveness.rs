//! Node liveness tracking for the leader's health monitor (ADR 0009).
//!
//! A shared map, deliberately **not** a channel. `mark` is called from the
//! ingestion loop on every inbound report; `seed`/`last_seen` from
//! housekeeping when it decides whether a silent node has missed the
//! liveness deadline; `snapshot` from the API read path. None of them
//! introduces an `.await` edge into or out of the call, so a plain
//! `Mutex<BTreeMap>` keeps the blocking-edge graph acyclic
//! (`docs/architecture/coordinator-runtime.md`, "Deadlock-freedom"), unlike
//! a channel would.
//!
//! **Scoped to one leadership term.** The handle is constructed once by
//! `runtime::run` and shared for the process's lifetime, but what it holds
//! belongs to a single term: every writer names the term it is acting for,
//! a newer term replaces the map wholesale, and a stale term's writes are
//! dropped. So a replica that has stepped down has nothing to serve (its
//! marks are from a term it no longer leads), and a replica re-elected
//! starts the new term from its grace grants alone — a heartbeat heard as
//! leader of term 3 says nothing about whether the node is alive in term 5.
//!
//! The map carries two clocks per node on purpose. The monotonic `Instant`
//! backs every deadline decision — the health monitor's `DeclareNodeLost`
//! and the API's read-time health — as spans between process-local
//! instants, immune to wall-clock steps. The wall `Timestamp` is the
//! displayable fact behind `NodeSummary.last_heartbeat` — stamped only by a
//! real report, never by `seed`, so a granted grace window can't masquerade
//! as a heartbeat — and is never fed into a deadline comparison.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use coppice_api::LivenessMark;
use coppice_core::id::NodeId;
use coppice_core::time::Timestamp;

/// What the map remembers about one node.
#[derive(Clone, Copy)]
struct Seen {
    /// Monotonic last-seen, the deadline input: the last report, or the
    /// grace grant [`NodeLiveness::seed`] made on leadership gain.
    instant: Instant,
    /// Wall-clock stamp of the last *actual* report of any shape — `None`
    /// for a node this term has only seeded, never heard from.
    heartbeat_at: Option<Timestamp>,
}

/// The map plus the term it belongs to. `term` is `None` only before this
/// process has led at all.
#[derive(Default)]
struct Tracked {
    term: Option<u64>,
    nodes: BTreeMap<NodeId, Seen>,
}

impl Tracked {
    /// Make the map `term`'s. A newer term than the one held replaces the
    /// map wholesale — nothing a prior term knew carries over. Returns
    /// `false` for a term older than the one held: a write from a term this
    /// replica has already moved past, which the caller must drop.
    fn enter(&mut self, term: u64) -> bool {
        match self.term {
            Some(held) if held == term => true,
            Some(held) if held > term => false,
            _ => {
                self.term = Some(term);
                self.nodes.clear();
                true
            }
        }
    }
}

/// Shared last-seen state for every node the leader of the current term is
/// tracking.
///
/// Cloneable: every clone shares the same map. The handle persists across
/// leadership terms (constructed once by `runtime::run`); its contents do
/// not — see the module docs.
#[derive(Clone, Default)]
pub struct NodeLiveness(Arc<Mutex<Tracked>>);

impl NodeLiveness {
    pub fn new() -> Self {
        NodeLiveness::default()
    }

    /// Record that `node` was just heard from (any report shape counts) by
    /// the leader of `term`.
    ///
    /// A mark for a term older than the one held is dropped: it is a report
    /// ingested under a leadership this replica has since lost, and the
    /// current term has not heard from the node. A mark for a *newer* term
    /// begins that term (ingestion can observe the new term before
    /// housekeeping seeds it) — the seed then fills in around it.
    pub fn mark(&self, term: u64, node: NodeId) {
        let mut tracked = self.0.lock().expect("liveness map poisoned");
        if !tracked.enter(term) {
            return;
        }
        tracked.nodes.insert(
            node,
            Seen {
                instant: Instant::now(),
                heartbeat_at: Some(Timestamp::now()),
            },
        );
    }

    /// On gaining leadership of `term`, grant every currently-known node a
    /// fresh grace window (`now`) so no node is declared lost before its
    /// first report of the new term.
    ///
    /// Entering a new term drops everything a prior term held, marks and
    /// stamps alike. A node already marked in *this* term (ingestion got
    /// there first) keeps its mark: a report is better evidence than a
    /// grant. The grace window is a monitor courtesy, not a report, so it
    /// never carries a wall-clock heartbeat stamp. A seed for a term older
    /// than the one held is dropped.
    pub fn seed(&self, term: u64, nodes: impl IntoIterator<Item = NodeId>, now: Instant) {
        let mut tracked = self.0.lock().expect("liveness map poisoned");
        if !tracked.enter(term) {
            return;
        }
        for node in nodes {
            tracked.nodes.entry(node).or_insert(Seen {
                instant: now,
                heartbeat_at: None,
            });
        }
    }

    /// The last instant `node` was heard from (or granted grace) in the
    /// term the map currently belongs to, if it is being tracked.
    pub fn last_seen(&self, node: NodeId) -> Option<Instant> {
        self.0
            .lock()
            .expect("liveness map poisoned")
            .nodes
            .get(&node)
            .map(|seen| seen.instant)
    }

    /// Every node the leader of `term` is tracking, as of `now` on the
    /// monotonic clock — the API's `UsageSnapshot::liveness` source.
    ///
    /// Empty unless the map belongs to exactly `term`: a replica reading
    /// while it follows, or while it leads a term it has not yet seeded,
    /// has no marks it can vouch for. Each entry's `silent_for` is the
    /// monotonic span since the node's last report or grace grant — the
    /// same span the health monitor measures against the liveness deadline
    /// — and its `last_heartbeat` is the wall stamp of the last report,
    /// absent for a node only granted grace this term.
    pub fn snapshot(&self, term: u64, now: Instant) -> BTreeMap<NodeId, LivenessMark> {
        let tracked = self.0.lock().expect("liveness map poisoned");
        if tracked.term != Some(term) {
            return BTreeMap::new();
        }
        tracked
            .nodes
            .iter()
            .map(|(node, seen)| {
                (
                    *node,
                    LivenessMark {
                        last_heartbeat: seen.heartbeat_at,
                        silent_for: now.saturating_duration_since(seen.instant),
                    },
                )
            })
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
        liveness.mark(1, node);
        assert!(liveness.last_seen(node).is_some());
    }

    #[test]
    fn seed_grants_grace_to_known_nodes() {
        let liveness = NodeLiveness::new();
        let node = NodeId::new();
        let now = Instant::now();
        liveness.seed(1, [node], now);
        assert_eq!(liveness.last_seen(node), Some(now));
    }

    #[test]
    fn mark_stamps_a_wall_clock_heartbeat() {
        let liveness = NodeLiveness::new();
        let node = NodeId::new();
        assert!(liveness.snapshot(1, Instant::now()).is_empty());

        let before = Timestamp::now();
        liveness.mark(1, node);
        let after = Timestamp::now();

        let at = liveness.snapshot(1, Instant::now())[&node]
            .last_heartbeat
            .expect("a mark stamps a heartbeat");
        assert!(before <= at && at <= after);
    }

    /// A seeded grace window is monitor bookkeeping, not a report: it must
    /// never fabricate a heartbeat stamp, and must never erase a real one
    /// from the same term.
    #[test]
    fn seed_neither_fabricates_nor_erases_a_heartbeat() {
        let liveness = NodeLiveness::new();
        let (seeded_only, heard) = (NodeId::new(), NodeId::new());

        liveness.mark(1, heard);
        let heard_at = liveness.snapshot(1, Instant::now())[&heard].last_heartbeat;
        assert!(heard_at.is_some());

        liveness.seed(1, [seeded_only, heard], Instant::now());

        let marks = liveness.snapshot(1, Instant::now());
        assert_eq!(marks[&seeded_only].last_heartbeat, None);
        assert_eq!(marks[&heard].last_heartbeat, heard_at);
    }

    /// Silence is a monotonic span from the node's last report or grant —
    /// never wall-clock arithmetic on the heartbeat stamp.
    #[test]
    fn snapshot_measures_silence_on_the_monotonic_clock() {
        let liveness = NodeLiveness::new();
        let node = NodeId::new();
        let base = Instant::now();
        liveness.seed(1, [node], base);

        let later = base + Duration::from_secs(100);
        assert_eq!(
            liveness.snapshot(1, later)[&node].silent_for,
            Duration::from_secs(100)
        );
        // A reader whose instant somehow precedes the grant reads zero, not
        // a panic.
        assert_eq!(
            liveness.snapshot(1, base - Duration::from_secs(1))[&node].silent_for,
            Duration::ZERO
        );
    }

    /// Step-down: the marks belong to the term they were made in. A replica
    /// that no longer leads that term has nothing to serve — not a
    /// `healthy` that decays into `lost` as the old marks age.
    #[test]
    fn a_snapshot_for_another_term_is_empty() {
        let liveness = NodeLiveness::new();
        let node = NodeId::new();
        liveness.seed(3, [node], Instant::now());
        liveness.mark(3, node);

        assert_eq!(liveness.snapshot(3, Instant::now()).len(), 1);
        assert!(liveness.snapshot(2, Instant::now()).is_empty());
        assert!(liveness.snapshot(4, Instant::now()).is_empty());
    }

    /// Re-election: seeding a new term starts from the grace grants alone.
    /// The prior term's marks and stamps are gone — an old heartbeat must
    /// not read as a stale one and cost the node its grace window, and a
    /// node the old term knew but the new one does not is not tracked.
    #[test]
    fn seeding_a_new_term_drops_the_prior_terms_marks() {
        let liveness = NodeLiveness::new();
        let (kept, departed) = (NodeId::new(), NodeId::new());
        let long_ago = Instant::now() - Duration::from_secs(3600);
        liveness.seed(3, [kept, departed], long_ago);
        liveness.mark(3, kept);
        liveness.mark(3, departed);

        let regained = Instant::now();
        liveness.seed(5, [kept], regained);

        assert_eq!(liveness.last_seen(kept), Some(regained));
        assert_eq!(liveness.last_seen(departed), None);
        let marks = liveness.snapshot(5, regained);
        assert_eq!(marks.len(), 1);
        assert_eq!(marks[&kept].last_heartbeat, None);
        assert_eq!(marks[&kept].silent_for, Duration::ZERO);
    }

    /// A report ingested under a leadership this replica has since lost is
    /// not evidence for the term it now leads.
    #[test]
    fn a_mark_for_a_stale_term_is_dropped() {
        let liveness = NodeLiveness::new();
        let node = NodeId::new();
        liveness.seed(5, [node], Instant::now());

        liveness.mark(4, node);

        assert_eq!(
            liveness.snapshot(5, Instant::now())[&node].last_heartbeat,
            None
        );
        assert!(liveness.snapshot(4, Instant::now()).is_empty());
    }

    /// Ingestion can observe the new term before housekeeping seeds it. Its
    /// mark begins the term (dropping the old one) and survives the seed
    /// that follows, which fills in the rest.
    #[test]
    fn a_mark_ahead_of_the_seed_begins_the_term() {
        let liveness = NodeLiveness::new();
        let (early, other) = (NodeId::new(), NodeId::new());
        liveness.seed(1, [early, other], Instant::now());
        liveness.mark(1, other);

        liveness.mark(2, early);
        let marks = liveness.snapshot(2, Instant::now());
        assert_eq!(marks.len(), 1);
        assert!(marks[&early].last_heartbeat.is_some());

        liveness.seed(2, [early, other], Instant::now());
        let marks = liveness.snapshot(2, Instant::now());
        assert_eq!(marks.len(), 2);
        assert!(marks[&early].last_heartbeat.is_some());
        assert_eq!(marks[&other].last_heartbeat, None);
    }
}
