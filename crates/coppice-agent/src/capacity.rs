//! Host capacity autodetect (`docs/roadmap/deployment-story.md` A3).
//!
//! The advertised resource vector is typed in by hand today; A3 makes
//! `[capacity]` an *override* over what the host reports. This module is the
//! detection half: it reads cpu, memory, and disk from the machine and hands
//! back one value per dimension, in exactly the units
//! [`crate::config::CapacityConfig`] and [`coppice_core::resource::Resources`]
//! use — milli-CPU (1000 = one core) and bytes. Nothing here reads config or
//! decides policy; applying overrides is the integration pass's job.
//!
//! # Per-dimension results
//!
//! Each field of [`DetectedCapacity`] is its own [`DetectResult`], because a
//! failed dimension is only fatal when the operator did not override *that*
//! dimension. A single `Result<DetectedCapacity>` would force startup to fail
//! on an unreadable `/proc/meminfo` even for a node whose `[capacity].memory`
//! is set — so [`detect`] itself is infallible and [`resolve`] is where a
//! missing reading with no override becomes an error.
//!
//! # What "detected" means per dimension
//!
//! * **CPU** — [`std::thread::available_parallelism`] × 1000, intersected on
//!   Linux with the effective cgroup v2 `cpu.max` quota — the minimum across
//!   the process's cgroup chain, since v2 limits are hierarchical and the
//!   common containerized layout carries the real ceiling in a parent slice
//!   while the leaf says `max`. A container pinned to half a core must not
//!   advertise the host's core count.
//! * **Memory** — `/proc/meminfo`'s `MemTotal` on Linux, intersected with the
//!   effective cgroup v2 `memory.max` (minimum across the chain, likewise);
//!   `hw.memsize` on macOS.
//! * **Disk** — the *total* size of the filesystem holding `data_dir`, via the
//!   same `statvfs` reading [`crate::pressure`] samples. Total, not available:
//!   the system reservation (`[reservation]`, §6.4) is what withholds room, and
//!   advertised capacity must not shrink because a job is currently using disk.
//!
//! A missing or unreadable cgroup file is never an error — it is the ordinary
//! shape of a host outside a container, and falls back to the host reading with
//! a debug log. Only the *host* reading failing makes a dimension unavailable.

use std::path::Path;

use anyhow::Result;
use coppice_core::bytes::ByteSize;
use coppice_core::resource::Resources;

/// One dimension's detected value, or why it could not be read. The error is a
/// plain string so [`DetectedCapacity`] stays `Clone` — callers only ever
/// render it.
pub type DetectResult = std::result::Result<u64, DetectError>;

/// A dimension that could not be detected on this host.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("detecting {dimension} capacity: {reason}")]
pub struct DetectError {
    /// The config key this dimension corresponds to (`capacity.memory`, …), so
    /// the message names what the operator would set to override it.
    pub dimension: &'static str,
    /// What went wrong, already rendered.
    pub reason: String,
}

impl DetectError {
    fn new(dimension: &'static str, reason: impl std::fmt::Display) -> Self {
        Self {
            dimension,
            reason: reason.to_string(),
        }
    }
}

/// What the host reports it has, one result per dimension.
///
/// Units match [`crate::config::CapacityConfig`]: `cpu_millis` is milli-CPU
/// (1000 = one core), `memory` and `disk` are bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedCapacity {
    /// Milli-CPU the host offers (1000 = one core).
    pub cpu_millis: DetectResult,
    /// Total RAM in bytes.
    pub memory: DetectResult,
    /// Total size in bytes of the filesystem holding the data directory.
    pub disk: DetectResult,
}

impl DetectedCapacity {
    /// The three readings as one [`Resources`] vector, for the agent to report
    /// alongside its advertised capacity (`Register.detected_capacity`).
    ///
    /// `None` unless *every* dimension read: a vector with a failed dimension
    /// zero-filled is indistinguishable from a host that genuinely detected
    /// nothing on it, and would render as a confident "0 cores" on the node
    /// detail view. Partial readings are not worth that; the per-dimension
    /// errors still reach the operator through the startup log.
    pub fn as_resources(&self) -> Option<Resources> {
        Some(Resources {
            cpu_millis: *self.cpu_millis.as_ref().ok()?,
            memory: ByteSize::from_bytes(*self.memory.as_ref().ok()?),
            disk: ByteSize::from_bytes(*self.disk.as_ref().ok()?),
        })
    }
}

/// Settle one dimension: an operator override always wins, else the detected
/// value, else the detection failure — the only path on which startup fails.
///
/// This is the whole of A3's "config becomes an optional override" rule,
/// factored out so the integration pass states it once per dimension.
pub fn resolve(detected: &DetectResult, override_value: Option<u64>) -> Result<u64> {
    match (override_value, detected) {
        (Some(value), _) => Ok(value),
        (None, Ok(value)) => Ok(*value),
        (None, Err(err)) => Err(anyhow::Error::new(err.clone()).context(
            "no configured override for this capacity dimension, and the host \
             reading failed",
        )),
    }
}

/// Detect cpu, memory, and disk for an agent whose data directory is
/// `data_dir`.
///
/// Infallible by construction: every dimension either carries a value or
/// carries its own error (see the module docs). `data_dir` is used only to pick
/// the filesystem for the disk reading; it need not be writable, but it must
/// exist for `statvfs` to resolve it.
pub fn detect(data_dir: &Path) -> DetectedCapacity {
    DetectedCapacity {
        cpu_millis: detect_cpu_millis(),
        memory: detect_memory(),
        disk: detect_disk(data_dir),
    }
}

/// The total size, in bytes, of the filesystem holding `path`.
///
/// The same `statvfs` reading [`detect`] uses for the agent's own `data_dir`
/// (§Disk above), exposed so callers with a *second* filesystem worth sizing —
/// notably a local Docker daemon's `data_root` (`docker-executor.md` §9) — can
/// reuse the exact arithmetic rather than re-deriving it. `path` must exist for
/// `statvfs` to resolve it; a VM-backed daemon's reported root commonly does
/// not exist on the local filesystem at all, which callers must check for
/// themselves before calling this (see `docker::api::data_root`'s caveats).
pub fn detect_disk_at(path: &Path) -> DetectResult {
    detect_disk(path)
}

// -- CPU ---------------------------------------------------------------------

/// Host parallelism × 1000, intersected with the cgroup v2 quota on Linux.
fn detect_cpu_millis() -> DetectResult {
    let parallelism = std::thread::available_parallelism()
        .map_err(|e| DetectError::new("capacity.cpu_millis", e))?
        .get();
    // Widened rather than `as`-cast so the arithmetic is identical on 32-bit,
    // and saturating so an absurd reading cannot wrap the multiply.
    let host = u64::try_from(parallelism)
        .unwrap_or(u64::MAX)
        .saturating_mul(1000);

    #[cfg(target_os = "linux")]
    let value = intersect(host, linux::cgroup_cpu_millis(), "cpu.max");
    #[cfg(not(target_os = "linux"))]
    let value = host;

    Ok(value)
}

// -- Memory ------------------------------------------------------------------

/// Total RAM in bytes: `/proc/meminfo` ∩ cgroup `memory.max` on Linux,
/// `hw.memsize` on macOS. Other targets have no reading, so the dimension is
/// override-only there.
fn detect_memory() -> DetectResult {
    #[cfg(target_os = "linux")]
    {
        let host = linux::meminfo_total().map_err(|e| DetectError::new("capacity.memory", e))?;
        Ok(intersect(host, linux::cgroup_memory_max(), "memory.max"))
    }
    #[cfg(target_os = "macos")]
    {
        macos::hw_memsize().map_err(|e| DetectError::new("capacity.memory", e))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Err(DetectError::new(
            "capacity.memory",
            "no total-memory source on this platform",
        ))
    }
}

// -- Disk --------------------------------------------------------------------

/// The total size of the filesystem holding `data_dir`.
///
/// `blocks × fragment_size`, the arithmetic [`crate::pressure`] uses for its
/// `total`: both factors are platform-specific integer types, so they are
/// widened with `From` rather than `as` and multiplied in `u128` so a nonsense
/// reading cannot wrap; the result saturates on the way back down to `u64`.
fn detect_disk(data_dir: &Path) -> DetectResult {
    let stat = nix::sys::statvfs::statvfs(data_dir)
        .map_err(|e| DetectError::new("capacity.disk", format!("statvfs {data_dir:?}: {e}")))?;
    let total = u128::from(stat.blocks()) * u128::from(stat.fragment_size());
    Ok(u64::try_from(total).unwrap_or(u64::MAX))
}

// -- Shared helpers ----------------------------------------------------------

/// Fold an optional container limit into a host reading: the smaller wins, and
/// a limit that could not be read (or is `max`) leaves the host value alone.
///
/// `what` names the cgroup file in the debug log, which is the only trace a
/// container-limited node leaves of *why* it advertises less than the host.
/// Compiled on every target though only the Linux paths call it: the pure
/// parse/fold helpers are unit-tested on any OS, which is the point of
/// keeping them free of platform code.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn intersect(host: u64, limit: Option<u64>, what: &str) -> u64 {
    match limit {
        Some(limit) if limit < host => {
            tracing::debug!(host, limit, source = what, "capacity limited by cgroup");
            limit
        }
        Some(_) => host,
        None => {
            tracing::debug!(host, source = what, "no cgroup limit; using the host value");
            host
        }
    }
}

/// Parse a cgroup v2 `cpu.max` (`"max 100000"` or `"<quota> <period>"`) into
/// milli-CPU.
///
/// `None` when unlimited or unparseable — both mean "no reading", which
/// [`intersect`] treats as no limit. The quota is rounded *down* (advertising
/// more than the kernel will schedule is the harmful direction) but never below
/// 1 milli-CPU: a cgroup with a real, tiny quota is limited, not zero-capacity,
/// and a zero would make every placement infeasible.
/// Compiled on every target though only the Linux paths call it: the pure
/// parse/fold helpers are unit-tested on any OS, which is the point of
/// keeping them free of platform code.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_cpu_max(raw: &str) -> Option<u64> {
    let mut parts = raw.split_whitespace();
    let quota = parts.next()?;
    if quota == "max" {
        return None;
    }
    let quota: u64 = quota.parse().ok()?;
    // A missing period means the kernel default, 100_000 µs.
    let period: u64 = match parts.next() {
        Some(p) => p.parse().ok()?,
        None => 100_000,
    };
    if period == 0 {
        return None;
    }
    Some((quota.saturating_mul(1000) / period).max(1))
}

/// Parse a cgroup v2 byte limit (`memory.max`): `"max"` or a decimal count.
/// `None` when unlimited or unparseable.
/// Compiled on every target though only the Linux paths call it: the pure
/// parse/fold helpers are unit-tested on any OS, which is the point of
/// keeping them free of platform code.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_bytes_max(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if raw == "max" {
        return None;
    }
    raw.parse().ok()
}

/// Every cgroup directory from the process's own (leaf) up to the hierarchy
/// root, as absolute filesystem paths under `/sys/fs/cgroup`.
///
/// cgroup v2 limits are **hierarchical**: the effective limit is the minimum
/// across the chain, and the common containerized layout puts `max` in the
/// leaf while a parent slice or pod cgroup carries the real ceiling. Reading
/// only the leaf would fall back to the host total there and over-advertise.
/// `rel` is the `0::` path from `/proc/self/cgroup` (absolute within the
/// hierarchy; `/` for the root cgroup).
/// Compiled on every target though only the Linux paths call it: the pure
/// parse/fold helpers are unit-tested on any OS, which is the point of
/// keeping them free of platform code.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn cgroup_ancestor_paths(rel: &str) -> Vec<String> {
    let mut current = rel.trim_end_matches('/').to_string();
    let mut paths = Vec::new();
    loop {
        paths.push(format!("/sys/fs/cgroup{current}"));
        if current.is_empty() {
            return paths;
        }
        current.truncate(current.rfind('/').unwrap_or(0));
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::io;

    /// The unified-hierarchy path from `/proc/self/cgroup`.
    ///
    /// The v2 line is `0::<path>`; the hybrid/v1 lines carry a nonzero
    /// hierarchy id and a controller list, and are ignored — this is a v2-only
    /// reader, and a v1-only host simply has no limit to intersect.
    pub(super) fn parse_self_cgroup(raw: &str) -> Option<String> {
        raw.lines()
            .find_map(|line| line.strip_prefix("0::"))
            .map(|path| path.trim().to_string())
    }

    /// The minimum finite limit named `name` across this process's cgroup
    /// chain, leaf to hierarchy root, parsed by `parse`.
    ///
    /// Limits are hierarchical (see [`super::cgroup_ancestor_paths`]): a level
    /// whose file is absent (the root cgroup exposes neither `cpu.max` nor
    /// `memory.max`), unreadable, or unlimited (`max`) simply contributes
    /// nothing, and `None` overall means no level imposes a limit.
    fn cgroup_min_limit(name: &str, parse: impl Fn(&str) -> Option<u64>) -> Option<u64> {
        let raw = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let rel = parse_self_cgroup(&raw)?;
        super::cgroup_ancestor_paths(&rel)
            .into_iter()
            .filter_map(|dir| {
                let path = format!("{dir}/{name}");
                match std::fs::read_to_string(&path) {
                    Ok(s) => parse(&s),
                    Err(err) => {
                        tracing::debug!(path, error = %err, "no cgroup limit file at this level");
                        None
                    }
                }
            })
            .min()
    }

    /// This process's effective cgroup v2 CPU quota in milli-CPU, if any:
    /// the minimum across the cgroup chain.
    pub(super) fn cgroup_cpu_millis() -> Option<u64> {
        cgroup_min_limit("cpu.max", super::parse_cpu_max)
    }

    /// This process's effective cgroup v2 memory ceiling in bytes, if any:
    /// the minimum across the cgroup chain.
    pub(super) fn cgroup_memory_max() -> Option<u64> {
        cgroup_min_limit("memory.max", super::parse_bytes_max)
    }

    /// `MemTotal` from a `/proc/meminfo` body, in bytes. The kernel reports it
    /// in kibibytes ("MemTotal:  16316908 kB"), so the value is scaled by 1024.
    pub(super) fn parse_meminfo_total(raw: &str) -> Option<u64> {
        let line = raw.lines().find_map(|l| l.strip_prefix("MemTotal:"))?;
        let kib: u64 = line.split_whitespace().next()?.parse().ok()?;
        Some(kib.saturating_mul(1024))
    }

    /// Total RAM in bytes from `/proc/meminfo`.
    pub(super) fn meminfo_total() -> io::Result<u64> {
        let raw = std::fs::read_to_string("/proc/meminfo")?;
        parse_meminfo_total(&raw).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "no MemTotal in /proc/meminfo")
        })
    }
}

#[cfg(target_os = "macos")]
mod macos {
    /// Total RAM in bytes from `hw.memsize`.
    ///
    /// Read through `/usr/sbin/sysctl` rather than `sysctlbyname` because the
    /// workspace **forbids** `unsafe_code` (root `Cargo.toml`), so the raw libc
    /// call is not available and no crate in the tree wraps this sysctl safely.
    /// macOS is a development platform here — production agents run on Linux
    /// and take the `/proc/meminfo` path above — so one `execve` at startup is
    /// the cheaper trade against a new dependency or a lint exemption.
    pub(super) fn hw_memsize() -> Result<u64, String> {
        let out = std::process::Command::new("/usr/sbin/sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .map_err(|e| format!("running sysctl -n hw.memsize: {e}"))?;
        if !out.status.success() {
            return Err(format!("sysctl -n hw.memsize failed: {}", out.status));
        }
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .map_err(|e| format!("parsing hw.memsize: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_max_unlimited_is_no_limit() {
        assert_eq!(parse_cpu_max("max 100000"), None);
        assert_eq!(parse_cpu_max("max\n"), None);
    }

    #[test]
    fn cpu_max_quota_is_milli_cpu() {
        // Half a core, two cores, and a quarter.
        assert_eq!(parse_cpu_max("50000 100000"), Some(500));
        assert_eq!(parse_cpu_max("200000 100000\n"), Some(2000));
        assert_eq!(parse_cpu_max("25000 100000"), Some(250));
    }

    #[test]
    fn cpu_max_rounds_down_but_never_to_zero() {
        // 1499 µs of every 100 ms is 14.99 milli-CPU: round down.
        assert_eq!(parse_cpu_max("1499 100000"), Some(14));
        // A quota below one milli-CPU still means "limited", not "nothing".
        assert_eq!(parse_cpu_max("1 100000"), Some(1));
    }

    #[test]
    fn cpu_max_defaults_the_period() {
        assert_eq!(parse_cpu_max("100000"), Some(1000));
    }

    #[test]
    fn cpu_max_garbage_reads_as_no_limit() {
        assert_eq!(parse_cpu_max(""), None);
        assert_eq!(parse_cpu_max("banana 100000"), None);
        assert_eq!(parse_cpu_max("100000 banana"), None);
        assert_eq!(parse_cpu_max("100000 0"), None);
    }

    #[test]
    fn memory_max_parses_bytes_and_max() {
        assert_eq!(parse_bytes_max("max\n"), None);
        assert_eq!(parse_bytes_max("2147483648"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_bytes_max("nonsense"), None);
    }

    #[test]
    fn intersection_takes_the_smaller_and_falls_back() {
        assert_eq!(intersect(8000, Some(2000), "cpu.max"), 2000);
        assert_eq!(intersect(8000, Some(16000), "cpu.max"), 8000);
        assert_eq!(intersect(8000, None, "cpu.max"), 8000);
        // Equal values are not "limited"; the result is the same either way.
        assert_eq!(intersect(8000, Some(8000), "cpu.max"), 8000);
    }

    #[test]
    fn resolve_prefers_the_override_then_the_reading() {
        let good: DetectResult = Ok(4000);
        let bad: DetectResult = Err(DetectError::new("capacity.memory", "no reading"));
        assert_eq!(resolve(&good, None).unwrap(), 4000);
        assert_eq!(resolve(&good, Some(1000)).unwrap(), 1000);
        assert_eq!(resolve(&bad, Some(1000)).unwrap(), 1000);

        let err = resolve(&bad, None).expect_err("no reading and no override is fatal");
        assert!(
            format!("{err:#}").contains("capacity.memory"),
            "the error names the overridable key: {err:#}"
        );
    }

    /// Memory detection is allowed to fail on macOS — a sandboxed test
    /// environment may refuse the `sysctl` subprocess — and that failure is
    /// exactly what the per-dimension `DetectResult` models. On Linux the
    /// `/proc/meminfo` read has no such mode, so a failure there is a bug.
    fn assert_memory_reading(memory: &DetectResult) {
        match memory {
            Ok(m) => assert!(*m > 0, "some memory: {m}"),
            Err(e) if cfg!(target_os = "linux") => panic!("memory must detect on Linux: {e}"),
            Err(e) => assert_eq!(e.dimension, "capacity.memory", "{e}"),
        }
    }

    #[test]
    fn detect_reports_the_host() {
        let dir = tempfile::tempdir().expect("temp dir");
        let detected = detect(dir.path());
        // Only a floor: a CI runner inside a cgroup-limited container legitimately
        // reports a fraction of a core, so the whole-core shape is not asserted.
        let cpu = detected.cpu_millis.expect("cpu detected");
        assert!(cpu >= 1, "some cpu: {cpu}");
        assert_memory_reading(&detected.memory);
        let disk = detected.disk.expect("disk detected");
        assert!(disk > 0, "some disk: {disk}");
    }

    #[test]
    fn a_nonexistent_data_dir_fails_only_the_disk_dimension() {
        let dir = tempfile::tempdir().expect("temp dir");
        let detected = detect(&dir.path().join("does-not-exist"));
        assert!(detected.disk.is_err(), "statvfs cannot resolve the path");
        assert!(detected.cpu_millis.is_ok());
        assert_memory_reading(&detected.memory);
        // …and an override for the failed dimension keeps startup alive.
        assert_eq!(resolve(&detected.disk, Some(42)).unwrap(), 42);
    }

    #[test]
    fn ancestor_paths_walk_leaf_to_hierarchy_root() {
        // Limits are hierarchical: the effective value is the minimum across
        // exactly these directories, leaf first (P1: a leaf holding `max`
        // under a limited parent slice must not fall back to the host total).
        assert_eq!(
            cgroup_ancestor_paths("/kubepods.slice/pod-1.slice/cri-abc.scope"),
            vec![
                "/sys/fs/cgroup/kubepods.slice/pod-1.slice/cri-abc.scope",
                "/sys/fs/cgroup/kubepods.slice/pod-1.slice",
                "/sys/fs/cgroup/kubepods.slice",
                "/sys/fs/cgroup",
            ]
        );
        // The root cgroup: just the hierarchy root.
        assert_eq!(cgroup_ancestor_paths("/"), vec!["/sys/fs/cgroup"]);
        // A trailing slash does not produce a duplicate level.
        assert_eq!(
            cgroup_ancestor_paths("/a/"),
            vec!["/sys/fs/cgroup/a", "/sys/fs/cgroup"]
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn self_cgroup_picks_the_unified_line() {
        assert_eq!(
            linux::parse_self_cgroup("0::/user.slice/user-1000.slice\n").as_deref(),
            Some("/user.slice/user-1000.slice")
        );
        // Hybrid: only the `0::` line is the v2 hierarchy.
        assert_eq!(
            linux::parse_self_cgroup("12:pids:/system.slice\n0::/system.slice/agent\n").as_deref(),
            Some("/system.slice/agent")
        );
        // v1-only: no unified line, so no limit to read.
        assert_eq!(linux::parse_self_cgroup("12:pids:/system.slice\n"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn meminfo_total_is_scaled_from_kib() {
        let fixture = "MemTotal:       16316908 kB\nMemFree:         1234 kB\n";
        assert_eq!(linux::parse_meminfo_total(fixture), Some(16_316_908 * 1024));
        assert_eq!(linux::parse_meminfo_total("MemFree: 1 kB\n"), None);
    }
}
