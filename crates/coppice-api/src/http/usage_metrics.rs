//! The `coppice_node_*` / `coppice_cluster_*` scrape section (ADR 0039,
//! issue #46), rendered **directly** from the live usage view at scrape time
//! rather than pushed into the process's metrics recorder.
//!
//! Why not the recorder. `metrics-exporter-prometheus` keeps every series it
//! has ever been handed, so a node-labelled gauge set once outlives the node:
//! a departed or silent node would keep rendering its last `used` forever,
//! which is exactly the "absence is not zero" rule ADR 0039 is built on,
//! violated at the one surface an operator alerts off. The recorder offers a
//! global idle timeout, but it is *global* — other modules set event-time
//! gauges that must never be evicted — so it cannot be the answer here.
//!
//! Rendering directly makes the lifecycle correct by construction: the scrape
//! asks the [`ControlPlane`](crate::ControlPlane) what is valid *now*, and a
//! node with no fresh sample simply has no line. Prometheus's own staleness
//! handling then does what it is designed to do — a series that stops
//! appearing goes stale — with no bookkeeping layer to disagree with.
//!
//! Freshness here is receipt time (`NodeUsageSample::received_at`), applied
//! by the plane's `usage_window` read before this ever sees a sample; the
//! agent's advisory `sampled_at` decides nothing.
//!
//! `allocated` and `capacity` are different: they come from replicated state
//! (via the usage-history task's newest closed bucket), so they are valid for
//! every node the leader is tracking whether or not it reports usage, and are
//! rendered for all of them.

use std::fmt::Write as _;

use coppice_core::resource::Resources;
use coppice_core::time::Timestamp;

use crate::UsageSnapshot;

/// One metric family: its name, its `# HELP` text, and the sample lines
/// collected for this scrape. A family with no lines renders nothing at all —
/// not even a `# HELP` — so a silent node's absence is total.
struct Family {
    name: &'static str,
    help: &'static str,
    lines: Vec<String>,
}

impl Family {
    fn new(name: &'static str, help: &'static str) -> Family {
        Family {
            name,
            help,
            lines: Vec::new(),
        }
    }

    /// An unlabelled sample.
    fn push(&mut self, value: impl std::fmt::Display) {
        self.lines.push(format!("{} {}", self.name, value));
    }

    /// A `node`-labelled sample.
    fn push_node(&mut self, node: &str, value: impl std::fmt::Display) {
        self.lines
            .push(format!("{}{{node=\"{}\"}} {}", self.name, node, value));
    }

    fn render_into(&self, out: &mut String) {
        if self.lines.is_empty() {
            return;
        }
        let _ = writeln!(out, "# HELP {} {}", self.name, self.help);
        let _ = writeln!(out, "# TYPE {} gauge", self.name);
        for line in &self.lines {
            out.push_str(line);
            out.push('\n');
        }
    }
}

/// The three families one resource vector fans out into.
struct Triple {
    cpu: Family,
    memory: Family,
    disk: Family,
}

impl Triple {
    fn new(
        cpu: (&'static str, &'static str),
        memory: (&'static str, &'static str),
        disk: (&'static str, &'static str),
    ) -> Triple {
        Triple {
            cpu: Family::new(cpu.0, cpu.1),
            memory: Family::new(memory.0, memory.1),
            disk: Family::new(disk.0, disk.1),
        }
    }

    fn push(&mut self, r: &Resources) {
        self.cpu.push(r.cpu_millis);
        self.memory.push(r.memory.as_u64());
        self.disk.push(r.disk.as_u64());
    }

    fn push_node(&mut self, node: &str, r: &Resources) {
        self.cpu.push_node(node, r.cpu_millis);
        self.memory.push_node(node, r.memory.as_u64());
        self.disk.push_node(node, r.disk.as_u64());
    }

    fn render_into(&self, out: &mut String) {
        self.cpu.render_into(out);
        self.memory.render_into(out);
        self.disk.render_into(out);
    }
}

/// Render the usage section of a scrape from one live view.
///
/// Pure over its inputs: the caller (the `/metrics` route) takes the view
/// from the plane at scrape time and passes the wall clock the sample ages
/// are measured against. Appended to the recorder's own render, so every
/// other metric in the process is untouched.
pub fn render(usage: &UsageSnapshot, now: Timestamp) -> String {
    let mut used = Triple::new(
        (
            "coppice_node_used_cpu_millis",
            "Job-attributable milli-CPU (1000 = one core) a node's containers are consuming. \
             Absent for a node with no fresh reading — never a zero.",
        ),
        (
            "coppice_node_used_memory_bytes",
            "Job-attributable resident memory across a node's containers. \
             Absent for a node with no fresh reading — never a zero.",
        ),
        (
            "coppice_node_used_disk_bytes",
            "Job-attributable disk (image + writable layer) across a node's containers. \
             Absent for a node with no fresh reading — never a zero.",
        ),
    );
    let mut allocated = Triple::new(
        (
            "coppice_node_allocated_cpu_millis",
            "Milli-CPU funded by a node's non-terminal allocations.",
        ),
        (
            "coppice_node_allocated_memory_bytes",
            "Memory funded by a node's non-terminal allocations.",
        ),
        (
            "coppice_node_allocated_disk_bytes",
            "Disk funded by a node's non-terminal allocations.",
        ),
    );
    let mut capacity = Triple::new(
        (
            "coppice_node_capacity_cpu_millis",
            "Milli-CPU a node registered as its capacity.",
        ),
        (
            "coppice_node_capacity_memory_bytes",
            "Memory a node registered as its capacity.",
        ),
        (
            "coppice_node_capacity_disk_bytes",
            "Disk a node registered as its capacity.",
        ),
    );
    let mut sample_age = Family::new(
        "coppice_node_usage_sample_age_seconds",
        "Age of a node's freshest usage reading, by the coordinator's own receipt clock. \
         Absent for a node that is not reporting.",
    );
    let mut cluster_used = Triple::new(
        (
            "coppice_cluster_used_cpu_millis",
            "Job-attributable milli-CPU summed over the reporting nodes — a partial sum \
             unless reporting_nodes equals total_nodes.",
        ),
        (
            "coppice_cluster_used_memory_bytes",
            "Job-attributable memory summed over the reporting nodes — a partial sum \
             unless reporting_nodes equals total_nodes.",
        ),
        (
            "coppice_cluster_used_disk_bytes",
            "Job-attributable disk summed over the reporting nodes — a partial sum \
             unless reporting_nodes equals total_nodes.",
        ),
    );
    let mut cluster_allocated = Triple::new(
        (
            "coppice_cluster_allocated_cpu_millis",
            "Milli-CPU funded by non-terminal allocations, cluster-wide.",
        ),
        (
            "coppice_cluster_allocated_memory_bytes",
            "Memory funded by non-terminal allocations, cluster-wide.",
        ),
        (
            "coppice_cluster_allocated_disk_bytes",
            "Disk funded by non-terminal allocations, cluster-wide.",
        ),
    );
    let mut cluster_capacity = Triple::new(
        (
            "coppice_cluster_capacity_cpu_millis",
            "Registered milli-CPU, cluster-wide.",
        ),
        (
            "coppice_cluster_capacity_memory_bytes",
            "Registered memory, cluster-wide.",
        ),
        (
            "coppice_cluster_capacity_disk_bytes",
            "Registered disk, cluster-wide.",
        ),
    );
    let mut reporting_nodes = Family::new(
        "coppice_cluster_usage_reporting_nodes",
        "Nodes with a usage reading fresh enough to count right now.",
    );
    let mut total_nodes = Family::new(
        "coppice_cluster_usage_total_nodes",
        "Nodes the leader is tracking — the denominator the reporting count is partial against.",
    );

    // Per-node `used` and sample age: only for a node whose reading is valid
    // *now*. `usage.current` has already had the receipt-time cutoff applied,
    // so membership of that map is the whole freshness test.
    for (node, sample) in &usage.current {
        let node = escape_label(&node.to_string());
        used.push_node(&node, &sample.used);
        // Clamped at zero: our own receipt stamp cannot be in the future, but
        // a clock step between receipt and scrape could make it look so.
        let age = (now - sample.received_at).max(coppice_core::time::Duration::ZERO);
        sample_age.push_node(&node, format!("{:.3}", age.as_secs_f64()));
    }

    // Per-node `allocated`/`capacity` come from replicated state through the
    // history task's newest closed bucket, so they are valid for every node
    // tracked at that close — reporting or not.
    for (node, window) in &usage.history.nodes {
        let Some(bucket) = window.buckets.last() else {
            continue;
        };
        let node = escape_label(&node.to_string());
        allocated.push_node(&node, &bucket.allocated);
        capacity.push_node(&node, &bucket.capacity);
    }

    if let Some(cluster) = usage.history.cluster.last() {
        cluster_allocated.push(&cluster.bucket.allocated);
        cluster_capacity.push(&cluster.bucket.capacity);
    }

    // The cluster `used` total is summed from the live view, not the newest
    // bucket, so it moves with the per-node lines above rather than lagging
    // them by up to one bucket. Nothing is emitted when no node is reporting.
    if !usage.current.is_empty() {
        let total = usage
            .current
            .values()
            .fold(Resources::ZERO, |acc, s| acc.saturating_add(&s.used));
        cluster_used.push(&total);
    }

    // Coverage, always emitted (0 reporting is a fact worth alerting on, not
    // an absence). The denominator is the live replicated node count carried
    // on the snapshot — one reporting node in a sixteen-node cluster must
    // read 1/16, not 1/1, even before the first history bucket closes.
    reporting_nodes.push(usage.current.len());
    total_nodes.push(usage.total_nodes);

    let mut out = String::new();
    used.render_into(&mut out);
    allocated.render_into(&mut out);
    capacity.render_into(&mut out);
    sample_age.render_into(&mut out);
    cluster_used.render_into(&mut out);
    cluster_allocated.render_into(&mut out);
    cluster_capacity.render_into(&mut out);
    reporting_nodes.render_into(&mut out);
    total_nodes.render_into(&mut out);
    out
}

/// Prometheus label-value escaping. Node ids never contain any of these, but
/// the renderer emits exposition text by hand, so it escapes rather than
/// trusting a caller's id formatting forever.
fn escape_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use coppice_core::bytes::ByteSize;
    use coppice_core::id::NodeId;
    use coppice_core::time::Duration;

    use crate::{ClusterUsage, ClusterUsageBucket, NodeUsageSample, UsageBucket, UsageWindow};

    fn ts(secs: i64) -> Timestamp {
        Timestamp::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn res(cpu: u64, mib: u64) -> Resources {
        Resources {
            cpu_millis: cpu,
            memory: ByteSize::from_mib(mib),
            disk: ByteSize::ZERO,
        }
    }

    fn window(node_capacity: Resources, allocated: Resources) -> UsageWindow {
        UsageWindow {
            buckets: vec![UsageBucket {
                start: ts(0),
                end: ts(30),
                capacity: node_capacity,
                allocated,
                used: None,
            }],
        }
    }

    /// A snapshot with one node that has a fresh reading and one that is
    /// tracked but silent.
    fn snapshot(reporting: Option<(NodeId, Timestamp)>, tracked: &[NodeId]) -> UsageSnapshot {
        let mut current = BTreeMap::new();
        if let Some((node, received_at)) = reporting {
            current.insert(
                node,
                NodeUsageSample {
                    used: res(1_500, 256),
                    // Deliberately far in the future: an agent clock ahead of
                    // ours must not change a single rendered number.
                    sampled_at: ts(99_999),
                    received_at,
                },
            );
        }
        let nodes: BTreeMap<_, _> = tracked
            .iter()
            .map(|n| (*n, window(res(4_000, 1_024), res(2_000, 512))))
            .collect();
        let cluster = if nodes.is_empty() {
            Vec::new()
        } else {
            vec![ClusterUsageBucket {
                bucket: UsageBucket {
                    start: ts(0),
                    end: ts(30),
                    capacity: res(4_000 * nodes.len() as u64, 1_024 * nodes.len() as u64),
                    allocated: res(2_000 * nodes.len() as u64, 512 * nodes.len() as u64),
                    used: None,
                },
                reporting_nodes: 0,
                total_nodes: nodes.len() as u32,
            }]
        };
        UsageSnapshot {
            current,
            history: Arc::new(ClusterUsage { nodes, cluster }),
            heartbeats: Default::default(),
            total_nodes: tracked.len() as u32,
        }
    }

    /// One reporting node in a big cluster reads 1/N before the first
    /// history bucket closes — the denominator is the live replicated count,
    /// never floored to the reporting count.
    #[test]
    fn coverage_uses_the_live_node_count_not_the_history_window() {
        let node = NodeId::new();
        let mut snap = snapshot(Some((node, ts(100))), &[node]);
        snap.history = Arc::new(ClusterUsage::default());
        snap.total_nodes = 16;
        let out = render(&snap, ts(101));
        assert!(
            out.contains("coppice_cluster_usage_reporting_nodes 1"),
            "{out}"
        );
        assert!(
            out.contains("coppice_cluster_usage_total_nodes 16"),
            "{out}"
        );
    }

    #[test]
    fn a_reporting_node_renders_used_capacity_and_an_age_from_receipt_time() {
        let node = NodeId::new();
        let out = render(&snapshot(Some((node, ts(100))), &[node]), ts(112));

        assert!(out.contains("# TYPE coppice_node_used_cpu_millis gauge"));
        assert!(out.contains(&format!(
            "coppice_node_used_cpu_millis{{node=\"{node}\"}} 1500"
        )));
        assert!(out.contains(&format!(
            "coppice_node_capacity_cpu_millis{{node=\"{node}\"}} 4000"
        )));
        assert!(out.contains(&format!(
            "coppice_node_allocated_cpu_millis{{node=\"{node}\"}} 2000"
        )));
        // 12 s by our receipt clock — the agent's `sampled_at` is 99_999 s
        // and contributes nothing.
        assert!(
            out.contains(&format!(
                "coppice_node_usage_sample_age_seconds{{node=\"{node}\"}} 12.000"
            )),
            "expected a receipt-time age, got:\n{out}"
        );
        assert!(out.contains("coppice_cluster_used_cpu_millis 1500"));
        assert!(out.contains("coppice_cluster_usage_reporting_nodes 1"));
        assert!(out.contains("coppice_cluster_usage_total_nodes 1"));
    }

    #[test]
    fn a_silent_node_has_no_used_or_age_line_but_keeps_its_capacity() {
        let (reporting, silent) = (NodeId::new(), NodeId::new());
        let out = render(
            &snapshot(Some((reporting, ts(100))), &[reporting, silent]),
            ts(101),
        );

        assert!(!out.contains(&format!(
            "coppice_node_used_cpu_millis{{node=\"{silent}\"}}"
        )));
        assert!(!out.contains(&format!(
            "coppice_node_usage_sample_age_seconds{{node=\"{silent}\"}}"
        )));
        // Allocated/capacity come from replicated state, so they stay.
        assert!(out.contains(&format!(
            "coppice_node_capacity_cpu_millis{{node=\"{silent}\"}} 4000"
        )));
        assert!(out.contains("coppice_cluster_usage_reporting_nodes 1"));
        assert!(out.contains("coppice_cluster_usage_total_nodes 2"));
    }

    #[test]
    fn a_cluster_with_no_reporter_renders_capacity_but_no_used_series() {
        let node = NodeId::new();
        let out = render(&snapshot(None, &[node]), ts(101));

        assert!(!out.contains("coppice_node_used_"));
        assert!(!out.contains("coppice_cluster_used_"));
        assert!(!out.contains("coppice_node_usage_sample_age_seconds"));
        assert!(out.contains("coppice_cluster_capacity_cpu_millis 4000"));
        assert!(out.contains("coppice_cluster_usage_reporting_nodes 0"));
        assert!(out.contains("coppice_cluster_usage_total_nodes 1"));
    }

    #[test]
    fn an_empty_cluster_renders_only_the_coverage_counts() {
        let out = render(&UsageSnapshot::default(), ts(101));
        assert!(!out.contains("coppice_node_"));
        assert!(!out.contains("coppice_cluster_capacity"));
        assert!(out.contains("coppice_cluster_usage_reporting_nodes 0"));
        assert!(out.contains("coppice_cluster_usage_total_nodes 0"));
    }

    /// A family with no samples renders nothing at all — no orphan `# HELP`
    /// for a series this scrape has no value for.
    #[test]
    fn an_empty_family_renders_no_header() {
        let mut family = Family::new("coppice_test_gauge", "help");
        let mut out = String::new();
        family.render_into(&mut out);
        assert!(out.is_empty());
        family.push(1);
        family.render_into(&mut out);
        assert_eq!(
            out,
            "# HELP coppice_test_gauge help\n# TYPE coppice_test_gauge gauge\ncoppice_test_gauge 1\n"
        );
    }
}
