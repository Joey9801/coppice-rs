//! Host disk-pressure monitor (docker-executor.md §9).
//!
//! One small task samples free space on the filesystems that matter — the
//! Docker data-root and the agent's `data_dir` — every [`SAMPLE_INTERVAL`] and
//! publishes the worst pressure it sees over a [`tokio::sync::watch`] channel.
//! A single shared signal, computed once, keeps every consumer's view of "how
//! full is the disk" consistent instead of each re-sampling on its own cadence.
//!
//! Consumers, in escalation order (§9): under [`DiskPressure::High`] the
//! telemetry retention sweep runs early (§8.4) and the image cache evicts
//! ahead of TTL (§7); under [`DiskPressure::Critical`] both sweep to their
//! floor and the agent refuses new `StartJob`s with a platform `StartError`
//! rather than wedging the node. That last consumer is wired by the executor
//! this session; the retention and cache consumers land in S5/S6. Job kills
//! are **never** driven by host pressure — only by each job's own disk limit
//! (§6.2).

use std::path::{Path, PathBuf};
use std::time::Duration;

use nix::sys::statvfs::statvfs;
use tokio::sync::watch;

use crate::config::PressureConfig;

/// How often each watched filesystem is sampled (§9). Not a config knob: the
/// signal only gates coarse, self-correcting reactions, so a fixed cadence is
/// enough and keeps the surface small.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

/// Host disk-pressure level, worst-across-watched-filesystems (§9). `Ord` is
/// derived so a caller folds several paths' levels with [`Ord::max`]; the
/// declaration order (`Ok < High < Critical`) is the escalation order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DiskPressure {
    /// Below the `high` threshold: normal operation.
    Ok,
    /// At or above `high_pct` used: retention and cache sweep early (§7, §8.4).
    High,
    /// At or above `critical_pct` used: sweep to floor and refuse new starts.
    Critical,
}

/// Classify one filesystem's fullness against the thresholds, purely.
///
/// `used = total − available`; the level is `Critical` when `used` is at or
/// above `critical_pct` of `total`, `High` when at or above `high_pct`, else
/// `Ok`. A zero-`total` filesystem (or one whose `statvfs` reported nonsense)
/// classifies as [`DiskPressure::Ok`] — absence of a reading is never
/// pressure. Comparison is exact integer arithmetic widened to `u128` so the
/// `used × 100` scaling cannot overflow even on a maxed-out `u64` byte count.
/// Bytes are taken as `u128` because the raw `statvfs` counts are widened there
/// (their concrete width — `u32` or `u64` — varies by platform).
fn classify(total: u128, available: u128, high_pct: u8, critical_pct: u8) -> DiskPressure {
    if total == 0 {
        return DiskPressure::Ok;
    }
    // `used = total − available`, clamped at zero: some network filesystems
    // report more free than total space, which must read as empty, not wrapped.
    let used = total.saturating_sub(available);
    let scaled = used * 100;
    if scaled >= total * u128::from(critical_pct) {
        DiskPressure::Critical
    } else if scaled >= total * u128::from(high_pct) {
        DiskPressure::High
    } else {
        DiskPressure::Ok
    }
}

/// Sample one path's filesystem via `statvfs`, returning its pressure level.
///
/// `total` and `available` are computed the way `df` does for an unprivileged
/// caller: `blocks × fragment_size` and `blocks_available × fragment_size`
/// (`f_bavail`, the space usable without root's reserve). An `Err` here — a
/// path that does not exist, or a Docker data-root that lives inside a VM or on
/// a remote daemon and so is not a local filesystem — is the caller's cue to
/// skip the path, not a pressure signal.
fn sample_path(path: &Path, high_pct: u8, critical_pct: u8) -> nix::Result<DiskPressure> {
    let stat = statvfs(path)?;
    // `From` (not `as`) widens the platform-specific `fsblkcnt_t`/`c_ulong`
    // without a cast, so the same code is warning-clean on 32- and 64-bit.
    let frsize = u128::from(stat.fragment_size());
    let total = u128::from(stat.blocks()).saturating_mul(frsize);
    let available = u128::from(stat.blocks_available()).saturating_mul(frsize);
    Ok(classify(total, available, high_pct, critical_pct))
}

/// Spawn the §9 monitor over `paths` and return a [`watch::Receiver`] of the
/// current pressure.
///
/// Callers pass the filesystems to watch (the Docker data-root and the agent's
/// `data_dir`). The task samples immediately at start — so the first real value
/// is available within a sample, not after 30s — then every [`SAMPLE_INTERVAL`]
/// thereafter, publishing the worst level across the paths that sampled
/// successfully. The channel's initial value is [`DiskPressure::Ok`].
///
/// A path whose `statvfs` fails is skipped and warned about exactly once (a
/// per-path flag suppresses a warning every 30s for a permanently-remote
/// data-root). If *every* path fails on a sweep, the last published value is
/// held rather than reset. The task exits when the last receiver is dropped.
pub fn spawn(paths: Vec<PathBuf>, config: PressureConfig) -> watch::Receiver<DiskPressure> {
    let (tx, rx) = watch::channel(DiskPressure::Ok);
    let high_pct = config.high_pct;
    let critical_pct = config.critical_pct;

    tokio::spawn(async move {
        let mut warned = vec![false; paths.len()];
        // The first tick fires immediately, giving consumers a real value fast.
        let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
        loop {
            ticker.tick().await;
            if tx.is_closed() {
                return;
            }

            let mut sampled_any = false;
            let mut worst = DiskPressure::Ok;
            for (i, path) in paths.iter().enumerate() {
                match sample_path(path, high_pct, critical_pct) {
                    Ok(level) => {
                        sampled_any = true;
                        worst = worst.max(level);
                    }
                    Err(err) => {
                        if !warned[i] {
                            warned[i] = true;
                            tracing::warn!(
                                path = %path.display(),
                                error = %err,
                                "disk-pressure sampling failed for path; skipping it \
                                 (warning suppressed for subsequent sweeps)"
                            );
                        }
                    }
                }
            }

            // Every path failed: hold the last known value rather than reset.
            if sampled_any {
                tx.send_if_modified(|current| {
                    if *current == worst {
                        false
                    } else {
                        *current = worst;
                        true
                    }
                });
            }
        }
    });

    rx
}

#[cfg(test)]
mod tests {
    use super::*;

    // Thresholds mirroring the defaults; total = 100 makes `used` a percentage.
    const HIGH: u8 = 85;
    const CRIT: u8 = 95;

    fn level(used: u128) -> DiskPressure {
        classify(100, 100 - used, HIGH, CRIT)
    }

    #[test]
    fn below_high_is_ok() {
        assert_eq!(level(0), DiskPressure::Ok);
        assert_eq!(level(84), DiskPressure::Ok);
    }

    #[test]
    fn high_boundary_is_inclusive() {
        assert_eq!(level(85), DiskPressure::High);
        assert_eq!(level(86), DiskPressure::High);
        assert_eq!(level(94), DiskPressure::High);
    }

    #[test]
    fn critical_boundary_is_inclusive() {
        assert_eq!(level(95), DiskPressure::Critical);
        assert_eq!(level(99), DiskPressure::Critical);
        assert_eq!(level(100), DiskPressure::Critical);
    }

    #[test]
    fn zero_total_is_ok() {
        assert_eq!(classify(0, 0, HIGH, CRIT), DiskPressure::Ok);
    }

    #[test]
    fn available_over_total_reads_empty() {
        // Some network filesystems report available > total; treat as empty.
        assert_eq!(classify(100, 200, HIGH, CRIT), DiskPressure::Ok);
    }

    #[test]
    fn overflow_scale_inputs_do_not_panic() {
        // used × 100 would overflow u64; the u128 widening keeps it exact.
        let max = u128::from(u64::MAX);
        assert_eq!(classify(max, 0, HIGH, CRIT), DiskPressure::Critical);
        assert_eq!(classify(max, max, HIGH, CRIT), DiskPressure::Ok);
        // Half full at u64 scale stays below High (50% used).
        assert_eq!(classify(max, max / 2, HIGH, CRIT), DiskPressure::Ok);
    }

    #[test]
    fn ordering_folds_with_max() {
        assert_eq!(
            DiskPressure::Ok
                .max(DiskPressure::High)
                .max(DiskPressure::Critical),
            DiskPressure::Critical
        );
        assert!(DiskPressure::Ok < DiskPressure::High);
        assert!(DiskPressure::High < DiskPressure::Critical);
    }

    #[test]
    fn statvfs_on_a_real_dir_yields_a_sample() {
        // CI disks vary, so assert only that a real path samples without error,
        // not which level it lands on.
        let dir = tempfile::tempdir().expect("temp dir");
        let result = sample_path(dir.path(), HIGH, CRIT);
        assert!(result.is_ok(), "statvfs on a temp dir should succeed");
    }
}
