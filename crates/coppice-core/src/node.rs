//! Compute node model.
//!
//! A node is described by the resources it advertises and the labels used to
//! satisfy hard placement constraints. Schedulability reflects drain and
//! maintenance state. See `docs/protocols/agent-coordinator.md`.

use std::collections::BTreeMap;

use crate::id::NodeId;
use crate::resource::Resources;

/// Authoritative record of a node's membership and schedulability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub id: NodeId,
    /// Total advertised capacity.
    pub capacity: Resources,
    /// Labels used for hard/soft placement constraints. `BTreeMap` keeps
    /// iteration order deterministic for the replicated state machine.
    pub labels: BTreeMap<String, String>,
    /// Whether the scheduler may place new work here.
    pub schedulable: bool,
    /// Advertised `host:port` of the agent's `NodeService` listener
    /// (ADR 0034); readers dial this to fetch job logs. `None` when the agent
    /// hosts no service, so its logs are unreachable off-node.
    pub service_addr: Option<String>,
    /// What the agent's host looks like, as reported at registration.
    /// Display-only — no scheduling decision reads it. `None` when the agent
    /// reported no facts at all.
    pub host_facts: Option<HostFacts>,
    /// What capacity detection read on that host before `[capacity]`
    /// overrides, so a reader can explain an advertised capacity that differs
    /// from the hardware. `None` when the agent detected nothing.
    pub detected_capacity: Option<Resources>,
}

/// Static description of the machine an agent runs on, collected once at agent
/// startup and refreshed only by re-registration.
///
/// Every field is best-effort and independently optional: a reading the agent
/// could not take is left at its zero value, which reads as "unknown" (empty
/// string, zero count). Nothing here is authoritative — [`Node::capacity`] is
/// what the node advertises, and these facts only explain where that number
/// came from. Deliberately flat: no NUMA topology, no per-device breakdown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostFacts {
    /// Operating system family, e.g. `linux`, `macos`.
    pub os: String,
    /// Human-readable OS release (`PRETTY_NAME`, `kern.osproductversion`).
    pub os_version: String,
    /// Kernel release string, as `uname -r` prints it.
    pub kernel_version: String,
    /// CPU architecture, e.g. `x86_64`, `aarch64`.
    pub arch: String,
    /// Marketing name of the CPU.
    pub cpu_model: String,
    /// Physical cores, ignoring SMT siblings. Zero = not determined.
    pub physical_cores: u32,
    /// Hardware threads the OS schedules on. Zero = not determined.
    pub logical_cores: u32,
    /// Total installed RAM in bytes. Zero = not determined.
    pub total_memory_bytes: u64,
    /// Total size of the filesystem holding the agent's data directory, in
    /// bytes. Zero = not determined.
    pub total_disk_bytes: u64,
    /// The agent binary's own version.
    pub agent_version: String,
}
