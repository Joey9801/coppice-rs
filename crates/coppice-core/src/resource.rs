//! Multi-dimensional resource model.
//!
//! CPU, memory, and disk are first-class from the start, but the representation
//! is designed to admit future scalar or structured resources (GPUs,
//! accelerators, licenses, NUMA-local devices) without reworking callers. See
//! `docs/scheduling/scheduling-model.md`.

use serde::{Deserialize, Serialize};

/// A vector of resource quantities requested by a job or offered by a node.
///
/// This is intentionally a coarse placeholder. The extensible-dimension design
/// (a map keyed by resource kind) is an open item in
/// `docs/roadmap/open-decisions.md`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resources {
    /// Milli-CPU units (1000 = one core).
    pub cpu_millis: u64,
    /// Memory in bytes.
    pub memory_bytes: u64,
    /// Disk in bytes.
    pub disk_bytes: u64,
}

impl Resources {
    /// Returns true if `self` fits within `capacity` on every dimension.
    pub fn fits_within(&self, capacity: &Resources) -> bool {
        self.cpu_millis <= capacity.cpu_millis
            && self.memory_bytes <= capacity.memory_bytes
            && self.disk_bytes <= capacity.disk_bytes
    }
}
