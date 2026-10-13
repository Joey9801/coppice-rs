//! Compute node model.
//!
//! A node is described by the resources it advertises and the labels used to
//! satisfy hard placement constraints. An operator's drain is recorded on the
//! node (ADR 0044 §5). See `docs/protocols/agent-coordinator.md`.

use std::collections::BTreeMap;

use crate::id::NodeId;
use crate::resource::Resources;
use crate::time::Timestamp;

/// Authoritative record of a node's membership and drain state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub id: NodeId,
    /// Total advertised capacity.
    pub capacity: Resources,
    /// Labels used for hard/soft placement constraints. `BTreeMap` keeps
    /// iteration order deterministic for the replicated state machine.
    pub labels: BTreeMap<String, String>,
    /// The operator's drain (ADR 0044 §5), or `None` when the node is open.
    ///
    /// Desired state owned by the admin: an agent restart never clears it
    /// (ADR 0009). A draining node admits only bounded backfill that is
    /// guaranteed gone by the node's horizon; it is distinct from the
    /// agent's own shutdown announcement, which lives on the state machine's
    /// node record.
    pub drain: Option<Drain>,
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
    /// The node accepts only *effectively preemptible* work
    /// (`docs/decisions/0044-job-preemption-and-spot-capacity.md` §4): set
    /// by the agent's `[preemption] preemptible_only`, typically on spot
    /// capacity, and rewritten by every registration. Both the scheduler's
    /// candidate filter and apply's placement validation enforce it.
    pub preemptible_only: bool,
}

/// An operator's drain of one node (ADR 0044 §5). A *regular* drain (no
/// deadline) lets existing work finish naturally and admits only bounded
/// backfill; a *deadline* drain additionally empties the node by `deadline`,
/// notifying whatever still runs at `T_notice` and stopping it at `T_stop`.
///
/// Set by `SetNodeDrain` (and by a loss declaration, as a regular drain);
/// cleared by undrain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Drain {
    /// When the drain was first requested. A repeated drain, and a deadline
    /// set or replaced on an existing drain, keeps the original stamp.
    pub requested_at: Timestamp,
    /// The deadline `D` by which the node must be empty: containers gone and
    /// exits reported. Absent for a regular drain.
    pub deadline: Option<Timestamp>,
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
    /// Human-readable OS release. Linux values preserve their useful
    /// `PRETTY_NAME`; known non-Linux families receive a label when normalized.
    pub os_version: String,
    /// Human-readable kernel release, including a family label when normalized.
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

impl HostFacts {
    /// Return OS and kernel releases with family labels where a bare release
    /// would otherwise be ambiguous.
    ///
    /// This is applied at the API boundary so older persisted registrations
    /// and new registrations share the same response contract without storing
    /// presentation strings in replicated state.
    pub fn normalized_versions(&self) -> (String, String) {
        (
            normalize_os_version(&self.os, &self.os_version),
            normalize_kernel_version(&self.os, &self.kernel_version),
        )
    }
}

fn normalize_os_version(os: &str, value: &str) -> String {
    let value = value.trim();
    if value.is_empty() || os.trim().eq_ignore_ascii_case("linux") {
        return value.to_string();
    }

    match os_label(os) {
        Some(label) => prefix_if_missing(value, label),
        None => value.to_string(),
    }
}

fn normalize_kernel_version(os: &str, value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        return String::new();
    }

    match kernel_label(os) {
        Some(label) => prefix_if_missing(value, label),
        None => value.to_string(),
    }
}

fn os_label(os: &str) -> Option<&'static str> {
    match os.trim().to_ascii_lowercase().as_str() {
        "macos" | "darwin" => Some("macOS"),
        "linux" => Some("Linux"),
        "android" => Some("Android"),
        "freebsd" => Some("FreeBSD"),
        "openbsd" => Some("OpenBSD"),
        "netbsd" => Some("NetBSD"),
        "dragonfly" => Some("DragonFly BSD"),
        "solaris" => Some("Solaris"),
        "illumos" => Some("illumos"),
        "aix" => Some("AIX"),
        "ios" => Some("iOS"),
        _ => None,
    }
}

fn kernel_label(os: &str) -> Option<&'static str> {
    match os.trim().to_ascii_lowercase().as_str() {
        "macos" | "darwin" | "ios" => Some("Darwin"),
        "android" => Some("Linux"),
        "solaris" => Some("SunOS"),
        _ => os_label(os),
    }
}

fn prefix_if_missing(value: &str, prefix: &str) -> String {
    if has_prefix(value, prefix) {
        value.to_string()
    } else {
        format!("{prefix} {value}")
    }
}

fn has_prefix(value: &str, prefix: &str) -> bool {
    let Some(head) = value.get(..prefix.len()) else {
        return false;
    };
    if !head.eq_ignore_ascii_case(prefix) {
        return false;
    }

    match value[prefix.len()..].chars().next() {
        None => true,
        Some(ch) => ch.is_whitespace(),
    }
}

#[cfg(test)]
mod tests {
    use super::HostFacts;

    fn versions(os: &str, os_version: &str, kernel_version: &str) -> (String, String) {
        let facts = HostFacts {
            os: os.into(),
            os_version: os_version.into(),
            kernel_version: kernel_version.into(),
            ..HostFacts::default()
        };
        facts.normalized_versions()
    }

    #[test]
    fn normalizes_mac_os_and_kernel_versions() {
        assert_eq!(
            versions("macos", "15.5", "24.5.0"),
            ("macOS 15.5".into(), "Darwin 24.5.0".into())
        );
    }

    #[test]
    fn preserves_already_labeled_and_linux_pretty_names() {
        assert_eq!(
            versions("macos", "macOS 15.5", "Darwin 24.5.0"),
            ("macOS 15.5".into(), "Darwin 24.5.0".into())
        );
        assert_eq!(
            versions("linux", "Debian GNU/Linux 12 (bookworm)", "6.1.0-21-amd64"),
            (
                "Debian GNU/Linux 12 (bookworm)".into(),
                "Linux 6.1.0-21-amd64".into()
            )
        );
    }

    #[test]
    fn labels_other_supported_unix_families() {
        assert_eq!(
            versions("freebsd", "14.1-RELEASE", "14.1-RELEASE"),
            ("FreeBSD 14.1-RELEASE".into(), "FreeBSD 14.1-RELEASE".into())
        );
        assert_eq!(
            versions("solaris", "11.4", "5.11"),
            ("Solaris 11.4".into(), "SunOS 5.11".into())
        );
        assert_eq!(versions("illumos", "", ""), (String::new(), String::new()));
    }

    #[test]
    fn missing_values_stay_missing_and_unknown_families_stay_unchanged() {
        assert_eq!(versions("macos", "", "  "), (String::new(), String::new()));
        assert_eq!(
            versions("custom-unix", "7.2", "kernel-1"),
            ("7.2".into(), "kernel-1".into())
        );
    }
}
