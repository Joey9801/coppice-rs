//! Best-effort node-usage telemetry on the leader (ADR 0039): the heartbeat
//! sample sink, and the `coppice_node_*` scrape surface built over it.
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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::watch;

use coppice_api::{ClusterUsage, NodeUsageSample};
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

    /// Record the usage a node just reported. Last write wins: heartbeats
    /// carry a level, not a delta, so an out-of-order pair loses resolution
    /// and nothing else.
    pub fn record(&self, node: NodeId, sample: NodeUsageSample) {
        self.0
            .lock()
            .expect("usage map poisoned")
            .insert(node, sample);
    }

    /// Every reading taken within `max_age` of `now`, by the *agent's* clock.
    ///
    /// The cutoff is what turns a departed or silent node's last reading back
    /// into absence rather than leaving a frozen value on the dashboard. A
    /// sample stamped in the future (agent clock ahead of ours) is fresh, not
    /// discarded — the stamp is advisory.
    pub fn snapshot(&self, now: Timestamp, max_age: Duration) -> BTreeMap<NodeId, NodeUsageSample> {
        let cutoff = now
            - coppice_core::time::Duration::from_micros(
                i64::try_from(max_age.as_micros()).unwrap_or(i64::MAX),
            );
        self.0
            .lock()
            .expect("usage map poisoned")
            .iter()
            .filter(|(_, sample)| sample.sampled_at >= cutoff)
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

// ---------------------------------------------------------------------------
// The `/metrics` surface (issue #46)
// ---------------------------------------------------------------------------

/// Job-attributable milli-CPU a node's containers are consuming.
const NODE_USED_CPU_MILLIS: &str = "coppice_node_used_cpu_millis";
const NODE_USED_MEMORY_BYTES: &str = "coppice_node_used_memory_bytes";
const NODE_USED_DISK_BYTES: &str = "coppice_node_used_disk_bytes";
const NODE_ALLOCATED_CPU_MILLIS: &str = "coppice_node_allocated_cpu_millis";
const NODE_ALLOCATED_MEMORY_BYTES: &str = "coppice_node_allocated_memory_bytes";
const NODE_ALLOCATED_DISK_BYTES: &str = "coppice_node_allocated_disk_bytes";
const NODE_CAPACITY_CPU_MILLIS: &str = "coppice_node_capacity_cpu_millis";
const NODE_CAPACITY_MEMORY_BYTES: &str = "coppice_node_capacity_memory_bytes";
const NODE_CAPACITY_DISK_BYTES: &str = "coppice_node_capacity_disk_bytes";
const NODE_USAGE_SAMPLE_AGE_SECONDS: &str = "coppice_node_usage_sample_age_seconds";
const CLUSTER_USED_CPU_MILLIS: &str = "coppice_cluster_used_cpu_millis";
const CLUSTER_USED_MEMORY_BYTES: &str = "coppice_cluster_used_memory_bytes";
const CLUSTER_USED_DISK_BYTES: &str = "coppice_cluster_used_disk_bytes";
const CLUSTER_ALLOCATED_CPU_MILLIS: &str = "coppice_cluster_allocated_cpu_millis";
const CLUSTER_ALLOCATED_MEMORY_BYTES: &str = "coppice_cluster_allocated_memory_bytes";
const CLUSTER_ALLOCATED_DISK_BYTES: &str = "coppice_cluster_allocated_disk_bytes";
const CLUSTER_CAPACITY_CPU_MILLIS: &str = "coppice_cluster_capacity_cpu_millis";
const CLUSTER_CAPACITY_MEMORY_BYTES: &str = "coppice_cluster_capacity_memory_bytes";
const CLUSTER_CAPACITY_DISK_BYTES: &str = "coppice_cluster_capacity_disk_bytes";
const CLUSTER_REPORTING_NODES: &str = "coppice_cluster_usage_reporting_nodes";

/// What one scrape reads: the live sample sink and the usage-history task's
/// published window.
struct MetricsSource {
    sink: NodeUsage,
    history: watch::Receiver<Arc<ClusterUsage>>,
}

/// The installed source, read once per scrape.
///
/// A process-wide `OnceLock` rather than a value threaded to the metrics
/// server for the same reason as the agent's (`coppice_agent::usage`): the
/// client listener that serves `/metrics` is built before the usage-history
/// task exists, and the task is what owns both halves. First install wins;
/// there is no uninstall, because one coordinator process runs one such task.
static SOURCE: OnceLock<MetricsSource> = OnceLock::new();

/// Install the process's usage metrics source (the usage-history task, at
/// spawn). Subsequent calls are ignored — first install wins.
pub(crate) fn install_metrics_source(sink: NodeUsage, history: watch::Receiver<Arc<ClusterUsage>>) {
    if SOURCE.set(MetricsSource { sink, history }).is_err() {
        tracing::debug!(
            "a coordinator usage metrics source is already installed; keeping the first"
        );
    }
}

/// Register this module's metric names. Part of the crate-level
/// [`describe_metrics`](crate::describe_metrics) fan-out.
pub(crate) fn describe_metrics() {
    for (name, unit, help) in [
        (
            NODE_USED_CPU_MILLIS,
            metrics::Unit::Count,
            "Job-attributable milli-CPU (1000 = one core) a node's containers are consuming.",
        ),
        (
            NODE_USED_MEMORY_BYTES,
            metrics::Unit::Bytes,
            "Job-attributable resident memory across a node's containers.",
        ),
        (
            NODE_USED_DISK_BYTES,
            metrics::Unit::Bytes,
            "Job-attributable disk (image + writable layer) across a node's containers.",
        ),
        (
            NODE_ALLOCATED_CPU_MILLIS,
            metrics::Unit::Count,
            "Milli-CPU funded by a node's non-terminal allocations.",
        ),
        (
            NODE_ALLOCATED_MEMORY_BYTES,
            metrics::Unit::Bytes,
            "Memory funded by a node's non-terminal allocations.",
        ),
        (
            NODE_ALLOCATED_DISK_BYTES,
            metrics::Unit::Bytes,
            "Disk funded by a node's non-terminal allocations.",
        ),
        (
            NODE_CAPACITY_CPU_MILLIS,
            metrics::Unit::Count,
            "Milli-CPU a node registered as its capacity.",
        ),
        (
            NODE_CAPACITY_MEMORY_BYTES,
            metrics::Unit::Bytes,
            "Memory a node registered as its capacity.",
        ),
        (
            NODE_CAPACITY_DISK_BYTES,
            metrics::Unit::Bytes,
            "Disk a node registered as its capacity.",
        ),
        (
            NODE_USAGE_SAMPLE_AGE_SECONDS,
            metrics::Unit::Seconds,
            "Age of a node's freshest usage reading, by the reporting agent's clock. \
             Not emitted for a node that is not reporting.",
        ),
        (
            CLUSTER_USED_CPU_MILLIS,
            metrics::Unit::Count,
            "Job-attributable milli-CPU summed over the reporting nodes.",
        ),
        (
            CLUSTER_USED_MEMORY_BYTES,
            metrics::Unit::Bytes,
            "Job-attributable memory summed over the reporting nodes.",
        ),
        (
            CLUSTER_USED_DISK_BYTES,
            metrics::Unit::Bytes,
            "Job-attributable disk summed over the reporting nodes.",
        ),
        (
            CLUSTER_ALLOCATED_CPU_MILLIS,
            metrics::Unit::Count,
            "Milli-CPU funded by non-terminal allocations, cluster-wide.",
        ),
        (
            CLUSTER_ALLOCATED_MEMORY_BYTES,
            metrics::Unit::Bytes,
            "Memory funded by non-terminal allocations, cluster-wide.",
        ),
        (
            CLUSTER_ALLOCATED_DISK_BYTES,
            metrics::Unit::Bytes,
            "Disk funded by non-terminal allocations, cluster-wide.",
        ),
        (
            CLUSTER_CAPACITY_CPU_MILLIS,
            metrics::Unit::Count,
            "Registered milli-CPU, cluster-wide.",
        ),
        (
            CLUSTER_CAPACITY_MEMORY_BYTES,
            metrics::Unit::Bytes,
            "Registered memory, cluster-wide.",
        ),
        (
            CLUSTER_CAPACITY_DISK_BYTES,
            metrics::Unit::Bytes,
            "Registered disk, cluster-wide.",
        ),
        (
            CLUSTER_REPORTING_NODES,
            metrics::Unit::Count,
            "Nodes that contributed a usage reading to the newest closed bucket.",
        ),
    ] {
        metrics::describe_gauge!(name, unit, help);
    }
}

/// Sample the installed source and publish the gauges. Part of the
/// crate-level [`gather_metrics`](crate::gather_metrics) fan-out, run
/// immediately before each `/metrics` render.
///
/// This is the coordinator's first genuinely *sampled* gather (every other
/// module's is a no-op, because its metrics are pushed at the event that
/// changes them). A usage fold has no such event: it is a point-in-time
/// question, and the answer lives in the sink and the history watch.
///
/// **Stale label sets linger by design.** The gauges are labelled by node, and
/// a node that leaves the cluster keeps its last series until the process
/// restarts — there is no label GC here. Prometheus's own staleness handling
/// is the answer: a series that stops being scraped goes stale on its own, and
/// building a second bookkeeping layer to delete labels would only add a way
/// for the two to disagree. Usage gauges for a node that has stopped
/// *reporting* (but still exists) are not emitted at all, which is the same
/// absence-not-zero rule the API serves.
pub(crate) fn gather_metrics() {
    let Some(source) = SOURCE.get() else {
        return;
    };
    let current = source
        .sink
        .snapshot(Timestamp::now(), crate::limits::USAGE_SAMPLE_MAX_AGE);
    let now = Timestamp::now();

    for (node, sample) in &current {
        let node = node.to_string();
        set_node_triple(
            NODE_USED_CPU_MILLIS,
            NODE_USED_MEMORY_BYTES,
            NODE_USED_DISK_BYTES,
            &node,
            &sample.used,
        );
        // Clamped at zero: an agent clock ahead of ours is not negative age.
        let age = (now - sample.sampled_at).max(coppice_core::time::Duration::ZERO);
        metrics::gauge!(NODE_USAGE_SAMPLE_AGE_SECONDS, "node" => node).set(age.as_secs_f64());
    }

    // Capacity/allocated come from the history task's newest closed bucket
    // rather than from a view read: `gather_metrics` runs on the HTTP
    // handler's thread, and re-deriving the per-node allocation fold there
    // would put a full allocation-map scan on every scrape.
    let history = source.history.borrow().clone();
    for (node, window) in &history.nodes {
        let Some(bucket) = window.buckets.last() else {
            continue;
        };
        let node = node.to_string();
        set_node_triple(
            NODE_ALLOCATED_CPU_MILLIS,
            NODE_ALLOCATED_MEMORY_BYTES,
            NODE_ALLOCATED_DISK_BYTES,
            &node,
            &bucket.allocated,
        );
        set_node_triple(
            NODE_CAPACITY_CPU_MILLIS,
            NODE_CAPACITY_MEMORY_BYTES,
            NODE_CAPACITY_DISK_BYTES,
            &node,
            &bucket.capacity,
        );
    }

    if let Some(cluster) = history.cluster.last() {
        set_triple(
            CLUSTER_ALLOCATED_CPU_MILLIS,
            CLUSTER_ALLOCATED_MEMORY_BYTES,
            CLUSTER_ALLOCATED_DISK_BYTES,
            &cluster.bucket.allocated,
        );
        set_triple(
            CLUSTER_CAPACITY_CPU_MILLIS,
            CLUSTER_CAPACITY_MEMORY_BYTES,
            CLUSTER_CAPACITY_DISK_BYTES,
            &cluster.bucket.capacity,
        );
        metrics::gauge!(CLUSTER_REPORTING_NODES).set(f64::from(cluster.reporting_nodes));
    }

    // The cluster `used` total is summed from the live snapshot, not the
    // newest bucket, so it moves with the per-node gauges above rather than
    // lagging them by up to one bucket. Nothing is emitted when no node is
    // reporting.
    if !current.is_empty() {
        let total = current
            .values()
            .fold(Resources::ZERO, |acc, s| acc.saturating_add(&s.used));
        set_triple(
            CLUSTER_USED_CPU_MILLIS,
            CLUSTER_USED_MEMORY_BYTES,
            CLUSTER_USED_DISK_BYTES,
            &total,
        );
    }
}

/// Set one resource vector's three unlabelled cluster gauges.
fn set_triple(cpu: &'static str, memory: &'static str, disk: &'static str, r: &Resources) {
    metrics::gauge!(cpu).set(r.cpu_millis as f64);
    metrics::gauge!(memory).set(r.memory.as_u64() as f64);
    metrics::gauge!(disk).set(r.disk.as_u64() as f64);
}

/// Set one resource vector's three `node`-labelled gauges.
fn set_node_triple(
    cpu: &'static str,
    memory: &'static str,
    disk: &'static str,
    node: &str,
    r: &Resources,
) {
    metrics::gauge!(cpu, "node" => node.to_owned()).set(r.cpu_millis as f64);
    metrics::gauge!(memory, "node" => node.to_owned()).set(r.memory.as_u64() as f64);
    metrics::gauge!(disk, "node" => node.to_owned()).set(r.disk.as_u64() as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::bytes::ByteSize;

    fn sample(at: Timestamp) -> NodeUsageSample {
        NodeUsageSample {
            used: Resources {
                cpu_millis: 1_500,
                memory: ByteSize::from_mib(512),
                disk: ByteSize::from_mib(64),
            },
            sampled_at: at,
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
        usage.record(node, sample(ts(100)));
        assert_eq!(
            usage.snapshot(ts(100), Duration::from_secs(90)).get(&node),
            Some(&sample(ts(100)))
        );
    }

    #[test]
    fn a_stale_sample_reads_as_absent() {
        let usage = NodeUsage::new();
        let node = NodeId::new();
        usage.record(node, sample(ts(100)));

        // Exactly at the cutoff still counts; past it does not.
        assert!(usage
            .snapshot(ts(190), Duration::from_secs(90))
            .contains_key(&node));
        assert!(!usage
            .snapshot(ts(191), Duration::from_secs(90))
            .contains_key(&node));
        // A sample stamped ahead of our clock is fresh, not discarded.
        assert!(usage
            .snapshot(ts(50), Duration::from_secs(90))
            .contains_key(&node));
    }

    #[test]
    fn retain_drops_departed_nodes() {
        let usage = NodeUsage::new();
        let (keep, drop) = (NodeId::new(), NodeId::new());
        usage.record(keep, sample(ts(100)));
        usage.record(drop, sample(ts(100)));

        usage.retain(|node| *node == keep);
        let snapshot = usage.snapshot(ts(100), Duration::from_secs(90));
        assert!(snapshot.contains_key(&keep));
        assert!(!snapshot.contains_key(&drop));
    }
}
