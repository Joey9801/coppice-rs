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

use coppice_core::bytes::ByteSize;
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
/// pressure.
///
/// The sizes cross this boundary as [`ByteSize`], which is what a caller has
/// and what a reader of a log line wants. The `u128` widening the comparison
/// needs is an implementation detail and stays inside: `used × 100` overflows
/// a `u64` on any filesystem past ~184 PiB, so both sides of the percentage
/// are lifted through [`ByteSize::as_u128`] before being scaled, and the
/// comparison is then exact integer arithmetic with no rounding step.
fn classify(total: ByteSize, available: ByteSize, high_pct: u8, critical_pct: u8) -> DiskPressure {
    if total.is_zero() {
        return DiskPressure::Ok;
    }
    // `used = total − available`, clamped at zero: some network filesystems
    // report more free than total space, which must read as empty, not wrapped.
    let used = total.saturating_sub(available).as_u128();
    let total = total.as_u128();
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
/// Narrow a `statvfs`-derived byte count into a [`ByteSize`], saturating.
///
/// Only reachable by a filesystem reporting more than 16 EiB, which means the
/// reading is nonsense rather than a real disk — and [`ByteSize::MAX`] is the
/// answer that keeps such a filesystem classified as empty rather than full.
fn bytes_from(raw: u128) -> ByteSize {
    ByteSize::from_bytes(u64::try_from(raw).unwrap_or(u64::MAX))
}

fn sample_path(path: &Path, high_pct: u8, critical_pct: u8) -> nix::Result<DiskPressure> {
    let stat = statvfs(path)?;
    // The block count times the fragment size is where a raw kernel number
    // becomes a quantity of bytes, so that product is the crossing into
    // `ByteSize`. Both factors are platform-specific (`fsblkcnt_t`/`c_ulong`,
    // `u32` or `u64` depending on the target), so they are widened with `From`
    // rather than `as` — the same code compiles warning-clean on 32- and
    // 64-bit. The multiply happens in `u128` so a nonsense `statvfs` reading
    // cannot wrap, and the result saturates on the way down into `ByteSize`.
    let frsize = u128::from(stat.fragment_size());
    let total = bytes_from(u128::from(stat.blocks()) * frsize);
    let available = bytes_from(u128::from(stat.blocks_available()) * frsize);
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

    fn level(used: u64) -> DiskPressure {
        classify(
            ByteSize::from_bytes(100),
            ByteSize::from_bytes(100 - used),
            HIGH,
            CRIT,
        )
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
        assert_eq!(
            classify(ByteSize::ZERO, ByteSize::ZERO, HIGH, CRIT),
            DiskPressure::Ok
        );
    }

    #[test]
    fn available_over_total_reads_empty() {
        // Some network filesystems report available > total; treat as empty.
        assert_eq!(
            classify(
                ByteSize::from_bytes(100),
                ByteSize::from_bytes(200),
                HIGH,
                CRIT
            ),
            DiskPressure::Ok
        );
    }

    #[test]
    fn overflow_scale_inputs_do_not_panic() {
        // used × 100 would overflow u64; the u128 widening keeps it exact.
        let max = ByteSize::MAX;
        let half = max.checked_div(2).expect("2 is not zero");
        assert_eq!(
            classify(max, ByteSize::ZERO, HIGH, CRIT),
            DiskPressure::Critical
        );
        assert_eq!(classify(max, max, HIGH, CRIT), DiskPressure::Ok);
        // Half full at u64 scale stays below High (50% used).
        assert_eq!(classify(max, half, HIGH, CRIT), DiskPressure::Ok);
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
