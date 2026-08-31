//! The node-usage scrape surface: `agent_node_used_*`.
//!
//! Three gauges carrying the same job-attributable vector the agent reports on
//! its heartbeat ([`Executor::sample_usage`](crate::executor::Executor::sample_usage)),
//! so an operator can see a node's real usage from Prometheus even when no
//! coordinator is collecting it.
//!
//! **This is the tree's first genuinely sampled `gather_metrics`.** Every other
//! module's is a no-op because its metrics are *pushed* at the event that
//! changes them (the view.rs push-on-transition convention); a usage fold has
//! no such event — it is a point-in-time question with an answer only the
//! executor can give. The `/metrics` handler calls
//! [`crate::gather_metrics`] immediately before each render, and this module's
//! half asks the [`SAMPLER`] installed at startup.
//!
//! The sampler is a process-wide [`OnceLock`] rather than a value threaded to
//! the metrics server because the two are wired at opposite ends of
//! `run_daemon`: the server binds before the session exists, and the session
//! owns the executor. First install wins and there is no uninstall — one agent
//! process runs one session over one executor, so a second install would be a
//! bug, not a reconfiguration.
//!
//! Absence is preserved end to end: with no sampler installed, or a sampler
//! answering `None` ("not measured"), the gauges are simply **not emitted** for
//! that scrape. A stale series is Prometheus's staleness problem; publishing a
//! zero would be a lie.
//!
//! The `agent_` prefix is load-bearing beyond convention: `coppice dev` runs an
//! agent and a coordinator in one process behind one recorder, and the
//! coordinator's own usage gauges are `coppice_node_used_*`. The two must never
//! collide.

use std::sync::OnceLock;

use coppice_core::resource::Resources;

/// Job-attributable milli-CPU currently consumed by this node's containers.
const AGENT_NODE_USED_CPU_MILLIS: &str = "agent_node_used_cpu_millis";
/// Job-attributable resident memory across this node's containers.
const AGENT_NODE_USED_MEMORY_BYTES: &str = "agent_node_used_memory_bytes";
/// Job-attributable disk across this node's containers.
const AGENT_NODE_USED_DISK_BYTES: &str = "agent_node_used_disk_bytes";

/// The installed usage source, called once per scrape. See the module docs for
/// why this is a static rather than a threaded value.
static SAMPLER: OnceLock<Box<dyn Fn() -> Option<Resources> + Send + Sync>> = OnceLock::new();

/// Install the process's usage source (the session, at startup). Subsequent
/// calls are ignored — first install wins.
pub fn install_sampler(sampler: Box<dyn Fn() -> Option<Resources> + Send + Sync>) {
    if SAMPLER.set(sampler).is_err() {
        tracing::debug!("a node-usage sampler is already installed; keeping the first");
    }
}

/// Register this module's metric names. Part of the crate-level
/// [`describe_metrics`](crate::describe_metrics) fan-out.
pub fn describe_metrics() {
    metrics::describe_gauge!(
        AGENT_NODE_USED_CPU_MILLIS,
        metrics::Unit::Count,
        "Job-attributable milli-CPU (1000 = one core) consumed by this node's containers. \
         Not emitted when usage is unmeasured."
    );
    metrics::describe_gauge!(
        AGENT_NODE_USED_MEMORY_BYTES,
        metrics::Unit::Bytes,
        "Job-attributable resident memory across this node's containers. \
         Not emitted when usage is unmeasured."
    );
    metrics::describe_gauge!(
        AGENT_NODE_USED_DISK_BYTES,
        metrics::Unit::Bytes,
        "Job-attributable disk (writable layer) across this node's containers. \
         Not emitted when usage is unmeasured."
    );
}

/// Sample the installed usage source and publish the gauges. Part of the
/// crate-level [`gather_metrics`](crate::gather_metrics) fan-out, run
/// immediately before each `/metrics` render.
///
/// Emits nothing when there is no sampler or the fold is `None` — see the
/// module docs on why absence is not zero.
pub fn gather_metrics() {
    let Some(used) = SAMPLER.get().and_then(|sampler| sampler()) else {
        return;
    };
    metrics::gauge!(AGENT_NODE_USED_CPU_MILLIS).set(used.cpu_millis as f64);
    metrics::gauge!(AGENT_NODE_USED_MEMORY_BYTES).set(used.memory.as_u64() as f64);
    metrics::gauge!(AGENT_NODE_USED_DISK_BYTES).set(used.disk.as_u64() as f64);
}
