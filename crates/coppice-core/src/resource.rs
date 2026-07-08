//! Multi-dimensional resource model.
//!
//! CPU, memory, and disk are first-class from the start, but the representation
//! is designed to admit future scalar or structured resources (GPUs,
//! accelerators, licenses, NUMA-local devices) without reworking callers. See
//! `docs/scheduling/scheduling-model.md`.

/// A vector of resource quantities requested by a job or offered by a node.
///
/// This is intentionally a coarse placeholder. The extensible-dimension design
/// (a map keyed by resource kind) is an open item in
/// `docs/roadmap/open-decisions.md`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Resources {
    /// Milli-CPU units (1000 = one core).
    pub cpu_millis: u64,
    /// Memory in bytes.
    pub memory_bytes: u64,
    /// Disk in bytes.
    pub disk_bytes: u64,
}

impl Resources {
    pub const ZERO: Resources = Resources { cpu_millis: 0, memory_bytes: 0, disk_bytes: 0 };

    /// Returns true if `self` fits within `capacity` on every dimension.
    pub fn fits_within(&self, capacity: &Resources) -> bool {
        self.cpu_millis <= capacity.cpu_millis
            && self.memory_bytes <= capacity.memory_bytes
            && self.disk_bytes <= capacity.disk_bytes
    }

    pub fn is_zero(&self) -> bool {
        *self == Resources::ZERO
    }

    /// Component-wise saturating sum.
    ///
    /// The replicated state machine forbids panics on any input, so all
    /// resource bookkeeping saturates.
    pub fn saturating_add(&self, other: &Resources) -> Resources {
        Resources {
            cpu_millis: self.cpu_millis.saturating_add(other.cpu_millis),
            memory_bytes: self.memory_bytes.saturating_add(other.memory_bytes),
            disk_bytes: self.disk_bytes.saturating_add(other.disk_bytes),
        }
    }

    /// Component-wise saturating difference, clamped at zero.
    pub fn saturating_sub(&self, other: &Resources) -> Resources {
        Resources {
            cpu_millis: self.cpu_millis.saturating_sub(other.cpu_millis),
            memory_bytes: self.memory_bytes.saturating_sub(other.memory_bytes),
            disk_bytes: self.disk_bytes.saturating_sub(other.disk_bytes),
        }
    }

    /// Component-wise minimum — the per-dimension pledge in the funding
    /// algorithm (`min(free, requested − funded)` per dimension).
    pub fn component_min(&self, other: &Resources) -> Resources {
        Resources {
            cpu_millis: self.cpu_millis.min(other.cpu_millis),
            memory_bytes: self.memory_bytes.min(other.memory_bytes),
            disk_bytes: self.disk_bytes.min(other.disk_bytes),
        }
    }
}
