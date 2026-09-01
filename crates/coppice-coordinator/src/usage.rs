//! Best-effort node-usage telemetry on the leader (ADR 0039): the heartbeat
//! sample sink behind the API's `usage_window` read and the usage-history
//! task's buckets.
//!
//! The sink is deliberately the same shape as [`crate::liveness`]: a shared
//! `Mutex<BTreeMap>`, not a channel. `record` is called from the ingestion
//! loop as each heartbeat is peeled apart, `snapshot` from the API read path
//! and the usage-history task. None of the three introduces an `.await` edge,
//! so the blocking-edge graph stays acyclic
//! (`docs/architecture/coordinator-runtime.md`, "Deadlock-freedom") — which a
//! channel between ingestion and a read handler would not.
//!
//! Everything here is leader-only in practice: agent sessions terminate on the
//! leader, so a follower's sink is simply empty and every `used` it serves is
//! honestly absent (ADR 0039 — absence is never zero).
//!
//! **Freshness is measured on our clock, never the reporter's.** A sample is
//! stamped with the coordinator's `received_at` as it is recorded, and that is
//! the only stamp any cutoff reads. The agent's `sampled_at` rides along as
//! advisory metadata; trusting it for the cutoff would let an agent whose
//! clock is an hour fast keep its last reading "fresh" for an hour after the
//! node went silent.
//!
//! The `coppice_node_*` / `coppice_cluster_*` scrape surface built over this
//! sink is *not* here: it is rendered directly at scrape time from the live
//! view (`coppice_api::http`'s usage-metrics section), because a recorder-held
//! gauge outlives the node it describes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coppice_api::NodeUsageSample;
use coppice_core::id::NodeId;
use coppice_core::resource::Resources;
use coppice_core::time::Timestamp;

/// Shared freshest usage readings for every node the leader has heard from.
///
/// Cloneable: every clone shares the same map. It persists across leadership
/// terms (constructed once by `runtime::run`) and is never seeded — unlike
/// liveness, a stale reading has no benign reading, so staleness is resolved
/// by the age cutoff in [`snapshot`](Self::snapshot) instead.
#[derive(Clone, Default)]
pub struct NodeUsage(Arc<Mutex<BTreeMap<NodeId, NodeUsageSample>>>);

impl NodeUsage {
    pub fn new() -> Self {
        NodeUsage::default()
    }

    /// Record the usage a node just reported, stamped with our own receipt
    /// time. Last write wins: heartbeats carry a level, not a delta, so an
    /// out-of-order pair loses resolution and nothing else.
    ///
    /// The receipt stamp is taken here rather than accepted from the caller
    /// so there is no path — ingestion, test, or future one — by which a
    /// remote clock reaches a freshness decision.
    pub fn record(&self, node: NodeId, used: Resources, sampled_at: Timestamp) {
        self.record_at(node, used, sampled_at, Timestamp::now());
    }

    /// [`record`](Self::record) with an explicit receipt time, for tests that
    /// drive the cutoff off a fixture clock.
    pub(crate) fn record_at(
        &self,
        node: NodeId,
        used: Resources,
        sampled_at: Timestamp,
        received_at: Timestamp,
    ) {
        self.0.lock().expect("usage map poisoned").insert(
            node,
            NodeUsageSample {
                used,
                sampled_at,
                received_at,
            },
        );
    }

    /// Every reading *we received* within `max_age` of `now`.
    ///
    /// The cutoff is what turns a departed or silent node's last reading back
    /// into absence rather than leaving a frozen value on the dashboard, so
    /// it reads `received_at` — our clock at ingestion — and never the
    /// agent's advisory `sampled_at`, which a skewed reporter could hold
    /// arbitrarily far in the future.
    pub fn snapshot(&self, now: Timestamp, max_age: Duration) -> BTreeMap<NodeId, NodeUsageSample> {
        let cutoff = now
            - coppice_core::time::Duration::from_micros(
                i64::try_from(max_age.as_micros()).unwrap_or(i64::MAX),
            );
        self.0
            .lock()
            .expect("usage map poisoned")
            .iter()
            .filter(|(_, sample)| sample.received_at >= cutoff)
            .map(|(node, sample)| (*node, *sample))
            .collect()
    }

    /// Forget the readings for nodes that have left the replicated state.
    /// Called by the usage-history task as it drops their windows, so the two
    /// never disagree about which nodes exist.
    pub fn retain(&self, keep: impl Fn(&NodeId) -> bool) {
        self.0
            .lock()
            .expect("usage map poisoned")
            .retain(|node, _| keep(node));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::bytes::ByteSize;

    fn used() -> Resources {
        Resources {
            cpu_millis: 1_500,
            memory: ByteSize::from_mib(512),
            disk: ByteSize::from_mib(64),
        }
    }

    fn ts(secs: i64) -> Timestamp {
        Timestamp::UNIX_EPOCH + coppice_core::time::Duration::from_secs(secs)
    }

    #[test]
    fn record_and_snapshot_round_trip() {
        let usage = NodeUsage::new();
        let node = NodeId::new();
        assert!(!usage
            .snapshot(ts(100), Duration::from_secs(90))
            .contains_key(&node));
        usage.record_at(node, used(), ts(100), ts(100));
        let sample = usage.snapshot(ts(100), Duration::from_secs(90))[&node];
        assert_eq!(sample.used, used());
        assert_eq!(sample.sampled_at, ts(100));
        assert_eq!(sample.received_at, ts(100));
    }

    #[test]
    fn a_stale_sample_reads_as_absent() {
        let usage = NodeUsage::new();
        let node = NodeId::new();
        usage.record_at(node, used(), ts(100), ts(100));

        // Exactly at the cutoff still counts; past it does not.
        assert!(usage
            .snapshot(ts(190), Duration::from_secs(90))
            .contains_key(&node));
        assert!(!usage
            .snapshot(ts(191), Duration::from_secs(90))
            .contains_key(&node));
    }

    /// The regression this cutoff exists for: an agent whose clock runs an
    /// hour fast reports, then goes silent. Its `sampled_at` stays in our
    /// future indefinitely, so a cutoff on the agent's stamp would keep the
    /// dead node's reading on the dashboard for an hour. On receipt time it
    /// ages out at 90 s like everyone else.
    #[test]
    fn a_future_dated_sample_still_goes_stale_on_our_clock() {
        let usage = NodeUsage::new();
        let node = NodeId::new();
        let received = ts(1_000);
        let agent_clock_an_hour_fast = ts(1_000 + 3_600);
        usage.record_at(node, used(), agent_clock_an_hour_fast, received);

        // Received 91 s ago: stale, however the reporter stamped it.
        assert!(!usage
            .snapshot(
                received + coppice_core::time::Duration::from_secs(91),
                Duration::from_secs(90)
            )
            .contains_key(&node));
        // And it was fresh right after receipt, so the cutoff is doing the
        // work rather than the sample being unreadable.
        assert!(usage
            .snapshot(
                received + coppice_core::time::Duration::from_secs(1),
                Duration::from_secs(90)
            )
            .contains_key(&node));
    }

    #[test]
    fn retain_drops_departed_nodes() {
        let usage = NodeUsage::new();
        let (keep, drop) = (NodeId::new(), NodeId::new());
        usage.record_at(keep, used(), ts(100), ts(100));
        usage.record_at(drop, used(), ts(100), ts(100));

        usage.retain(|node| *node == keep);
        let snapshot = usage.snapshot(ts(100), Duration::from_secs(90));
        assert!(snapshot.contains_key(&keep));
        assert!(!snapshot.contains_key(&drop));
    }
}
