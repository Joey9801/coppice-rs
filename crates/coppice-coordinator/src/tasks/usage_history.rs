//! Rolling node-usage history (leader-only in practice) — ADR 0039.
//!
//! The structural sibling of [`super::derived_stats`], with one deliberate
//! difference: derived stats *counts* an event stream, so a coverage gap voids
//! its buckets; this task *samples* two independent sources at each tick — the
//! replicated state (capacity, allocated) and the leader's heartbeat sink
//! (used) — so there is no stream to lose coverage of. Each bucket is a
//! standalone reading, and a bucket that was never produced is simply absent
//! from the window.
//!
//! Absence is the whole point of the `used: Option` in a bucket. Nothing here
//! ever substitutes a zero: a node with no fresh sample at close records
//! `None`, and the cluster bucket says how many of the cluster's nodes did
//! report, so a partial sum can never be mistaken for a total.
//!
//! Task-local state only: nothing touches the `StateMachine`, the log, or a
//! snapshot, and none of it survives a restart or a leadership move. Long-term
//! retention is Prometheus's job, off the `coppice_node_*` series the
//! `/metrics` scrape renders from the same live view this task publishes.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use coppice_api::{ClusterUsage, ClusterUsageBucket, NodeUsageSample, UsageBucket, UsageWindow};
use coppice_consensus::StateViews;
use coppice_core::id::NodeId;
use coppice_core::resource::Resources;
use coppice_core::time::Timestamp;

use crate::limits::{USAGE_BUCKET_INTERVAL, USAGE_SAMPLE_MAX_AGE, USAGE_WINDOW_MAX_BUCKETS};
use crate::usage::NodeUsage;

/// One node's replicated-state facts at a tick, read before the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NodeReading {
    capacity: Resources,
    allocated: Resources,
}

/// The rolling windows plus the start of the bucket currently open.
///
/// Pure accounting, separated from the async loop so tests drive it with
/// explicit clocks and readings.
struct WindowState {
    /// Closed buckets per node, oldest first, each bounded by
    /// [`USAGE_WINDOW_MAX_BUCKETS`].
    nodes: BTreeMap<NodeId, VecDeque<UsageBucket>>,
    /// Closed cluster buckets, oldest first, bounded the same way.
    cluster: VecDeque<ClusterUsageBucket>,
    /// Start of the open bucket. There is nothing to accumulate into it —
    /// every field is sampled at close — so this is all an open bucket is.
    open_start: Timestamp,
}

impl WindowState {
    fn new(now: Timestamp) -> Self {
        WindowState {
            nodes: BTreeMap::new(),
            cluster: VecDeque::new(),
            open_start: now,
        }
    }

    /// Close the open bucket from one tick's readings and open the next at
    /// `now`.
    ///
    /// `now` becomes each bucket's `end`: after a stalled tick (missed ticks
    /// are skipped) this is one honest *long* bucket, exactly as in
    /// [`super::derived_stats`].
    ///
    /// Nodes absent from `readings` have left the replicated state; their
    /// windows are dropped whole rather than left to age out, so the window
    /// map is always the set of nodes that existed at the last close.
    fn close_bucket(
        &mut self,
        readings: &BTreeMap<NodeId, NodeReading>,
        used: &BTreeMap<NodeId, NodeUsageSample>,
        now: Timestamp,
    ) {
        self.nodes.retain(|node, _| readings.contains_key(node));

        let mut totals = NodeReading {
            capacity: Resources::ZERO,
            allocated: Resources::ZERO,
        };
        let mut total_used: Option<Resources> = None;
        let mut reporting_nodes: u32 = 0;

        for (node, reading) in readings {
            let node_used = used.get(node).map(|sample| sample.used);
            if let Some(u) = node_used {
                reporting_nodes += 1;
                total_used = Some(total_used.unwrap_or(Resources::ZERO).saturating_add(&u));
            }
            totals.capacity = totals.capacity.saturating_add(&reading.capacity);
            totals.allocated = totals.allocated.saturating_add(&reading.allocated);

            let buckets = self.nodes.entry(*node).or_default();
            buckets.push_back(UsageBucket {
                start: self.open_start,
                end: now,
                capacity: reading.capacity,
                allocated: reading.allocated,
                used: node_used,
            });
            while buckets.len() > USAGE_WINDOW_MAX_BUCKETS {
                buckets.pop_front();
            }
        }

        self.cluster.push_back(ClusterUsageBucket {
            bucket: UsageBucket {
                start: self.open_start,
                end: now,
                capacity: totals.capacity,
                allocated: totals.allocated,
                used: total_used,
            },
            reporting_nodes,
            total_nodes: readings.len().try_into().unwrap_or(u32::MAX),
        });
        while self.cluster.len() > USAGE_WINDOW_MAX_BUCKETS {
            self.cluster.pop_front();
        }

        self.open_start = now;
    }

    fn published(&self) -> ClusterUsage {
        ClusterUsage {
            nodes: self
                .nodes
                .iter()
                .map(|(node, buckets)| {
                    (
                        *node,
                        UsageWindow {
                            buckets: buckets.iter().copied().collect(),
                        },
                    )
                })
                .collect(),
            cluster: self.cluster.iter().copied().collect(),
        }
    }
}

/// Each node's capacity and its funded non-terminal allocations, from the
/// latest published view.
///
/// The allocation fold is the same one `coppice_api::http::project`'s
/// `build_node_memos` does, duplicated rather than shared: that function is a
/// private read-model detail of the API crate, and exporting it to make one
/// caller in another crate share ten lines would turn an internal projection
/// into a cross-crate contract. If a third caller ever appears, that is the
/// moment to lift it.
fn sample_readings(views: &StateViews) -> BTreeMap<NodeId, NodeReading> {
    let view = views.latest();
    let state = view.state();

    let mut readings: BTreeMap<NodeId, NodeReading> = state
        .nodes
        .iter()
        .map(|(id, record)| {
            (
                *id,
                NodeReading {
                    capacity: record.node.capacity,
                    allocated: Resources::ZERO,
                },
            )
        })
        .collect();

    for (_, alloc_record) in &state.allocations {
        let alloc = &alloc_record.allocation;
        if alloc.state.is_terminal() {
            continue;
        }
        // An allocation on a node that has left state has nothing to be
        // charged to; the node's window is gone with it.
        if let Some(reading) = readings.get_mut(&alloc.node) {
            reading.allocated = reading.allocated.saturating_add(&alloc.funded);
        }
    }

    readings
}

/// Spawn the usage-history task.
///
/// Returns the watch the API's `usage_window` read serves its history from
/// (seeded empty: no closed bucket exists until the first interval elapses)
/// and the task's `JoinHandle`. The `/metrics` usage surface reads the same
/// watch through that API read (`coppice_api::http`'s usage-metrics section),
/// so nothing is installed process-wide here.
pub fn spawn(
    views: StateViews,
    usage: NodeUsage,
    shutdown: watch::Receiver<bool>,
) -> (
    watch::Receiver<Arc<ClusterUsage>>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = watch::channel(Arc::new(ClusterUsage::default()));
    let join = tokio::spawn(run(views, usage, tx, shutdown));
    (rx, join)
}

async fn run(
    views: StateViews,
    usage: NodeUsage,
    tx: watch::Sender<Arc<ClusterUsage>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut state = WindowState::new(Timestamp::now());

    // Skip missed ticks, as derived stats does: a stalled loop should close
    // one (long) bucket, not burst out a run of buckets with fabricated
    // timestamps.
    let mut tick = tokio::time::interval(USAGE_BUCKET_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick.reset(); // the first tick fires after one full interval, not at once

    loop {
        tokio::select! {
            biased;
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = tick.tick() => {
                let now = Timestamp::now();
                let readings = sample_readings(&views);
                let used = usage.snapshot(now, USAGE_SAMPLE_MAX_AGE);
                // Forget readings for nodes that have left the cluster, so
                // the sink and the windows agree on who exists.
                usage.retain(|node| readings.contains_key(node));
                state.close_bucket(&readings, &used, now);
                let _ = tx.send(Arc::new(state.published()));
            }
        }
    }
    tracing::debug!("usage history shutting down");
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::bytes::ByteSize;

    /// Fixture instants are seconds from the epoch, so the range check
    /// cannot fire.
    fn ts(secs: i64) -> Timestamp {
        Timestamp::UNIX_EPOCH + coppice_core::time::Duration::from_secs(secs)
    }

    fn res(cpu: u64, mib: u64) -> Resources {
        Resources {
            cpu_millis: cpu,
            memory: ByteSize::from_mib(mib),
            disk: ByteSize::ZERO,
        }
    }

    fn reading(cpu: u64) -> NodeReading {
        NodeReading {
            capacity: res(cpu, 1024),
            allocated: res(cpu / 2, 512),
        }
    }

    fn sample(cpu: u64, at: Timestamp) -> NodeUsageSample {
        NodeUsageSample {
            used: res(cpu, 256),
            sampled_at: at,
            received_at: at,
        }
    }

    #[test]
    fn closes_a_bucket_per_node_and_one_for_the_cluster() {
        let (a, b) = (NodeId::new(), NodeId::new());
        let readings = BTreeMap::from([(a, reading(4_000)), (b, reading(2_000))]);
        // Only `a` reported: the cluster total is a partial sum, and says so.
        let used = BTreeMap::from([(a, sample(1_500, ts(25)))]);

        let mut state = WindowState::new(ts(0));
        state.close_bucket(&readings, &used, ts(30));
        let published = state.published();

        let node_a = &published.nodes[&a].buckets;
        assert_eq!(node_a.len(), 1);
        assert_eq!(node_a[0].start, ts(0));
        assert_eq!(node_a[0].end, ts(30));
        assert_eq!(node_a[0].capacity, res(4_000, 1024));
        assert_eq!(node_a[0].allocated, res(2_000, 512));
        assert_eq!(node_a[0].used, Some(res(1_500, 256)));

        // A node that reported nothing records absence, never a zero.
        assert_eq!(published.nodes[&b].buckets[0].used, None);

        assert_eq!(published.cluster.len(), 1);
        let cluster = published.cluster[0];
        assert_eq!(cluster.bucket.capacity, res(6_000, 2048));
        assert_eq!(cluster.bucket.allocated, res(3_000, 1024));
        assert_eq!(cluster.bucket.used, Some(res(1_500, 256)));
        assert_eq!(cluster.reporting_nodes, 1);
        assert_eq!(cluster.total_nodes, 2);
    }

    #[test]
    fn a_cluster_bucket_with_no_reporter_carries_no_total() {
        let node = NodeId::new();
        let readings = BTreeMap::from([(node, reading(1_000))]);

        let mut state = WindowState::new(ts(0));
        state.close_bucket(&readings, &BTreeMap::new(), ts(30));
        let published = state.published();

        assert_eq!(published.cluster[0].bucket.used, None);
        assert_eq!(published.cluster[0].reporting_nodes, 0);
        assert_eq!(published.cluster[0].total_nodes, 1);
    }

    #[test]
    fn the_next_bucket_opens_at_the_close_and_records_its_real_span() {
        let node = NodeId::new();
        let readings = BTreeMap::from([(node, reading(1_000))]);

        let mut state = WindowState::new(ts(0));
        state.close_bucket(&readings, &BTreeMap::new(), ts(30));
        // A stalled tick: one long bucket, not a run of fabricated ones.
        state.close_bucket(&readings, &BTreeMap::new(), ts(330));

        let buckets = &state.published().nodes[&node].buckets;
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[1].start, ts(30));
        assert_eq!(buckets[1].end, ts(330));
    }

    #[test]
    fn windows_are_bounded_by_evicting_the_oldest_bucket() {
        let node = NodeId::new();
        let readings = BTreeMap::from([(node, reading(1_000))]);

        let mut state = WindowState::new(ts(0));
        for i in 0..(USAGE_WINDOW_MAX_BUCKETS + 3) {
            state.close_bucket(&readings, &BTreeMap::new(), ts((i as i64 + 1) * 30));
        }
        let published = state.published();
        assert_eq!(
            published.nodes[&node].buckets.len(),
            USAGE_WINDOW_MAX_BUCKETS
        );
        assert_eq!(published.cluster.len(), USAGE_WINDOW_MAX_BUCKETS);
        // Oldest three were evicted: the window starts at the fourth bucket.
        assert_eq!(published.nodes[&node].buckets[0].start, ts(3 * 30));
    }

    #[test]
    fn a_departed_node_loses_its_window() {
        let (stays, leaves) = (NodeId::new(), NodeId::new());
        let mut state = WindowState::new(ts(0));
        state.close_bucket(
            &BTreeMap::from([(stays, reading(1_000)), (leaves, reading(1_000))]),
            &BTreeMap::new(),
            ts(30),
        );
        assert_eq!(state.published().nodes.len(), 2);

        state.close_bucket(
            &BTreeMap::from([(stays, reading(1_000))]),
            &BTreeMap::new(),
            ts(60),
        );
        let published = state.published();
        assert_eq!(published.nodes.len(), 1);
        assert!(published.nodes.contains_key(&stays));
        // The cluster window keeps its history: the departed node's past
        // buckets were real readings at the time they were taken.
        assert_eq!(published.cluster.len(), 2);
        assert_eq!(published.cluster[1].total_nodes, 1);
    }

    /// The task end to end under virtual time: readings folded off a real
    /// published view, `used` peeled from the sink, and the closed bucket
    /// published on the watch after one interval.
    #[tokio::test(start_paused = true)]
    async fn task_publishes_closed_buckets_from_state_and_the_sink() {
        use coppice_consensus::{ViewPublisher, ViewPublisherConfig};

        let node = NodeId::new();
        let mut sm = coppice_state::StateMachine::default();
        sm.nodes
            .insert(node, crate::test_support::node_record(node, 1, true));
        let (_publisher, views) = ViewPublisher::new(sm, 1, ViewPublisherConfig::default());

        let usage = NodeUsage::new();
        usage.record(node, res(900, 256), Timestamp::now());

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut rx, _join) = spawn(views, usage, shutdown_rx);

        rx.changed().await.expect("the task publishes a window");
        let published = rx.borrow().clone();
        let buckets = &published.nodes[&node].buckets;
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].used, Some(res(900, 256)));
        assert_eq!(published.cluster[0].reporting_nodes, 1);
        assert_eq!(published.cluster[0].total_nodes, 1);
    }
}
