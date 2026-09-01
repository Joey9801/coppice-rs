//! The node's job-attributable usage fold (`Executor::sample_usage`).
//!
//! No new collection happens here. The per-container metrics sampler
//! ([`stats`](super::stats)) already polls every `telemetry.metrics_interval`
//! — including the disk enforcer's latest writable-layer reading for the
//! container — and this module keeps the *last two* of those readings per live
//! container in [`ExecutorState::live_usage`](super::ExecutorState::live_usage),
//! folding them into one [`Resources`] vector on demand.
//!
//! Two properties the heartbeat path depends on:
//!
//! - **No daemon call.** [`fold`] reads the shared state under the executor
//!   mutex and nothing else, so a heartbeat never waits on docker.
//! - **Absent means not measured — but idle is measured zero.** Absence is
//!   reserved for the one case that is genuinely unknown: containers *are*
//!   live and none of them has a fresh reading, i.e. a wedged or stalled
//!   sampler. A node with **no live containers at all** has nothing to
//!   attribute and answers `Some(Resources::ZERO)`: that is a healthy idle
//!   node, a fact, and reporting it absent would leave every idle node
//!   permanently unmeasured. Freshness is `2 × metrics_interval`: one missed
//!   tick is tolerated, two are not.
//!
//! CPU is the only derived dimension. Docker reports cumulative CPU time, so
//! the instantaneous rate is a difference quotient over the last two readings
//! ([`cpu_rate_millis`], pure and unit-tested); memory and disk are level
//! readings taken as-is.

use std::collections::HashMap;

use coppice_core::bytes::ByteSize;
use coppice_core::id::AllocationId;
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};

/// One container's metrics reading, reduced to the fields the fold needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Reading {
    /// When the reading was taken (the sample's `at`, i.e. the daemon's own
    /// stamp when it supplied one).
    pub(crate) at: Timestamp,
    /// Cumulative CPU time consumed by the container up to `at`.
    pub(crate) cpu_total: Duration,
    /// Resident memory at `at`.
    pub(crate) memory_bytes: u64,
    /// Disk consumed at `at`, on §6.2's definition: `writable_layer +
    /// image_size`. Image-inclusive so a node's `used.disk` is comparable to
    /// its `allocated.disk`, which is the same image-inclusive budget. (Under
    /// the quota strategy the writable half reads 0 — the documented §8.1 v1
    /// gap — leaving the image size as the honest floor.)
    pub(crate) disk_bytes: u64,
}

/// The rolling two-reading window kept for one live container.
///
/// `prev` is `None` for the first tick after the container's sampler starts:
/// one cumulative reading supports no rate, so such an entry contributes
/// memory and disk but zero cpu until its second tick lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveUsage {
    prev: Option<Reading>,
    last: Reading,
}

impl LiveUsage {
    /// Fold a fresh reading in, sliding the window.
    pub(crate) fn push(entry: Option<LiveUsage>, reading: Reading) -> LiveUsage {
        match entry {
            Some(existing) => LiveUsage {
                prev: Some(existing.last),
                last: reading,
            },
            None => LiveUsage {
                prev: None,
                last: reading,
            },
        }
    }

    /// This container's contribution to `cpu_millis`.
    fn cpu_millis(&self) -> u64 {
        match self.prev {
            Some(prev) => cpu_rate_millis(prev, self.last),
            None => 0,
        }
    }
}

/// The instantaneous CPU rate in milli-CPU implied by two cumulative readings:
/// `Δcpu_time / Δwall × 1000` (1000 = one core fully busy).
///
/// Defensive on both degenerate windows — a non-positive wall gap (two samples
/// carrying the same daemon stamp, or a daemon clock that stepped back) and a
/// non-positive CPU delta (a counter that did not move, or a container the
/// daemon restarted the counter for) both answer zero rather than dividing by
/// zero or wrapping. The arithmetic widens to `u128` so a long window cannot
/// overflow the multiply.
fn cpu_rate_millis(prev: Reading, last: Reading) -> u64 {
    let wall_us = (last.at - prev.at).as_micros();
    let cpu_us = (last.cpu_total - prev.cpu_total).as_micros();
    if wall_us <= 0 || cpu_us <= 0 {
        return 0;
    }
    let millis = (cpu_us as u128 * 1_000) / wall_us as u128;
    u64::try_from(millis).unwrap_or(u64::MAX)
}

/// Fold the live per-container windows into this node's job-attributable usage.
///
/// - `cpu_millis`: the sum of per-container rates (see [`cpu_rate_millis`]).
/// - `memory`: the sum of the latest resident readings.
/// - `disk`: the sum of the latest disk readings (writable layer + image, §6.2).
///
/// All three come from the same set of windows, so they stay attributable to
/// one set of containers.
///
/// Entries whose last reading is older than `max_age` are skipped as stale.
/// When nothing survives that filter, `live_containers` — the executor's count
/// of containers it currently has live (running, starting, or holding a
/// telemetry collector) — decides between the two very different reasons the
/// window set can be empty:
///
/// - `live_containers == 0`: nothing to attribute. `Some(Resources::ZERO)` —
///   an idle node's usage is known, and it is zero.
/// - otherwise: containers are running but nobody is reporting on them.
///   `None`, "not measured" (see the module docs).
pub(crate) fn fold(
    live: &HashMap<AllocationId, LiveUsage>,
    live_containers: usize,
    now: Timestamp,
    max_age: Duration,
) -> Option<Resources> {
    let mut used = Resources::ZERO;
    let mut fresh = 0usize;
    for entry in live.values() {
        if now - entry.last.at > max_age {
            continue;
        }
        fresh += 1;
        used.cpu_millis = used.cpu_millis.saturating_add(entry.cpu_millis());
        used.memory = used
            .memory
            .saturating_add(ByteSize::from_bytes(entry.last.memory_bytes));
        used.disk = used
            .disk
            .saturating_add(ByteSize::from_bytes(entry.last.disk_bytes));
    }
    if fresh > 0 || live_containers == 0 {
        Some(used)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(secs: i64) -> Timestamp {
        Timestamp::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn reading(secs: i64, cpu_micros: i64, memory_bytes: u64, disk_bytes: u64) -> Reading {
        Reading {
            at: ts(secs),
            cpu_total: Duration::from_micros(cpu_micros),
            memory_bytes,
            disk_bytes,
        }
    }

    fn window(prev: Reading, last: Reading) -> LiveUsage {
        LiveUsage::push(Some(LiveUsage::push(None, prev)), last)
    }

    #[test]
    fn cpu_rate_is_the_difference_quotient_over_the_window() {
        // 2.5 s of CPU across a 5 s wall gap = half a core = 500 milli-CPU.
        let entry = window(reading(0, 0, 0, 0), reading(5, 2_500_000, 0, 0));
        assert_eq!(entry.cpu_millis(), 500);
        // 8 s of CPU across 2 s of wall = four busy cores.
        let entry = window(reading(10, 1_000_000, 0, 0), reading(12, 9_000_000, 0, 0));
        assert_eq!(entry.cpu_millis(), 4_000);
    }

    #[test]
    fn a_single_reading_contributes_no_cpu_rate() {
        // The first tick after a container starts has no window to divide over.
        let entry = LiveUsage::push(None, reading(0, 5_000_000, 128, 64));
        assert_eq!(entry.cpu_millis(), 0);
    }

    #[test]
    fn degenerate_windows_rate_to_zero() {
        // Same stamp on both readings: no wall gap to divide by.
        let entry = window(reading(3, 0, 0, 0), reading(3, 1_000_000, 0, 0));
        assert_eq!(entry.cpu_millis(), 0);
        // A counter that went backwards (daemon restart) is not negative usage.
        let entry = window(reading(3, 9_000_000, 0, 0), reading(5, 1_000_000, 0, 0));
        assert_eq!(entry.cpu_millis(), 0);
    }

    #[test]
    fn fold_sums_every_dimension_over_live_containers() {
        let live = HashMap::from([
            // 1 s CPU / 2 s wall = 500 milli-CPU.
            (
                AllocationId::new(),
                window(
                    reading(8, 0, 1_024, 100),
                    reading(10, 1_000_000, 2_048, 100),
                ),
            ),
            // 4 s CPU / 2 s wall = 2000 milli-CPU.
            (
                AllocationId::new(),
                window(reading(8, 0, 512, 200), reading(10, 4_000_000, 4_096, 200)),
            ),
        ]);
        let used = fold(&live, 2, ts(10), Duration::from_secs(10)).expect("both are fresh");
        assert_eq!(used.cpu_millis, 2_500);
        assert_eq!(used.memory, ByteSize::from_bytes(6_144));
        assert_eq!(used.disk, ByteSize::from_bytes(300));
    }

    #[test]
    fn a_stale_window_under_a_live_container_is_unmeasured() {
        let live = HashMap::from([(
            AllocationId::new(),
            window(reading(0, 0, 0, 0), reading(2, 1_000_000, 4_096, 4_096)),
        )]);
        // Fresh at 2× the 5 s interval…
        assert!(fold(&live, 1, ts(12), Duration::from_secs(10)).is_some());
        // …and past it, with the container still live, the node reports "not
        // measured" rather than a zero it cannot vouch for.
        assert_eq!(fold(&live, 1, ts(13), Duration::from_secs(10)), None);
    }

    #[test]
    fn a_container_awaiting_its_first_reading_is_unmeasured() {
        // Live container, no window for it yet (the sampler has not ticked):
        // there is something to attribute and no reading to attribute it from.
        assert_eq!(
            fold(&HashMap::new(), 1, ts(2), Duration::from_secs(10)),
            None
        );
    }

    #[test]
    fn an_idle_node_measures_zero() {
        // No live containers: nothing to attribute, and that is a measurement.
        assert_eq!(
            fold(&HashMap::new(), 0, ts(2), Duration::from_secs(10)),
            Some(Resources::ZERO)
        );
    }
}
