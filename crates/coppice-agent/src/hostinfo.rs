//! Host description for the node detail view.
//!
//! [`crate::capacity`] answers "how much can this machine offer"; this module
//! answers "what *is* this machine" — OS, kernel, CPU model, core counts, and
//! the agent's own version. The pair is what lets an operator see why a node
//! advertises 16 cores when its box has 10, which is the whole point of
//! shipping the detected reading alongside the advertised one.
//!
//! Nothing here is authoritative and nothing here is scheduled on. Every field
//! is independently best-effort: a reading that fails logs at debug and leaves
//! its field at the zero value, which the wire and the UI both read as
//! "unknown". There is no error type, because there is no caller decision to
//! make — [`collect`] is infallible by construction, the same discipline
//! [`crate::capacity::detect`] follows for a different reason.
//!
//! # Sources, per platform
//!
//! * **Linux** — `/etc/os-release`'s `PRETTY_NAME`, `/proc/sys/kernel/osrelease`,
//!   and `/proc/cpuinfo` for the CPU model plus the distinct `(physical id,
//!   core id)` pairs that make up the physical core count.
//! * **macOS** — one `sysctl` invocation covering every key at once. Like
//!   [`crate::capacity`]'s `hw.memsize` read, this shells out because the
//!   workspace forbids `unsafe_code` and no crate in the tree wraps these
//!   sysctls; macOS is a development platform here, so one `execve` at startup
//!   is the cheaper trade against a new dependency. An unsupported key (common
//!   in VMs, e.g. `machdep.cpu.brand_string`) makes `sysctl` exit nonzero, but
//!   it still writes every key it resolved to stdout first, so the parse runs
//!   on whatever stdout arrived regardless of exit status, keeping the
//!   per-field best-effort contract even when one key in the batch fails.
//! * **Everywhere** — `os` and `arch` from `std::env::consts`, the logical core
//!   count from [`std::thread::available_parallelism`], and the agent version
//!   from `CARGO_PKG_VERSION`.
//!
//! Memory and disk totals are **not** re-read here: they are the readings
//! [`crate::capacity::detect`] already took, passed in by the caller, so the
//! "total" the host card shows and the number capacity resolution worked from
//! can never disagree.

use coppice_core::node::HostFacts;

use crate::capacity::DetectedCapacity;

/// Describe the host this agent runs on, folding in the totals `detected`
/// already read.
///
/// A dimension `detected` could not read stays zero here — "unknown" on the
/// wire — rather than being re-read by a second, possibly disagreeing, syscall.
pub fn collect(detected: &DetectedCapacity) -> HostFacts {
    let mut facts = HostFacts {
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        logical_cores: logical_cores(),
        total_memory_bytes: detected.memory.clone().unwrap_or(0),
        total_disk_bytes: detected.disk.clone().unwrap_or(0),
        agent_version: env!("CARGO_PKG_VERSION").to_string(),
        ..HostFacts::default()
    };

    #[cfg(target_os = "linux")]
    linux::fill(&mut facts);
    #[cfg(target_os = "macos")]
    macos::fill(&mut facts);

    facts
}

/// Hardware threads the OS schedules on, or zero when the platform will not
/// say. Note this is the *host* count, deliberately un-intersected with the
/// cgroup quota [`crate::capacity`] applies: this field describes the machine,
/// and the quota's effect is already visible as advertised-below-detected.
fn logical_cores() -> u32 {
    match std::thread::available_parallelism() {
        Ok(n) => u32::try_from(n.get()).unwrap_or(u32::MAX),
        Err(err) => {
            tracing::debug!(error = %err, "no logical core count for host facts");
            0
        }
    }
}

/// The value of `key` in an `/etc/os-release`-style body, unquoted.
///
/// Values may be bare, single-, or double-quoted (`PRETTY_NAME="Debian GNU/Linux 12"`);
/// the file's shell-escape rules go no further than that in practice, and a
/// value this reader mangles is a cosmetic string, not a decision input.
/// Compiled on every target though only the Linux path calls it: the pure
/// parsers are unit-tested on any OS, which is the point of keeping them free
/// of platform code.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn os_release_value(raw: &str, key: &str) -> Option<String> {
    let value = raw
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))?;
    let unquoted = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
        .unwrap_or(value);
    Some(unquoted.to_string()).filter(|v| !v.is_empty())
}

/// The first `model name` in a `/proc/cpuinfo` body.
///
/// Every core repeats the same value on x86; on aarch64 the key is often
/// absent entirely, which is an ordinary "unknown" rather than a failure.
/// Compiled on every target though only the Linux path calls it (see above).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn cpuinfo_model(raw: &str) -> Option<String> {
    raw.lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(k, _)| k.trim() == "model name")
        })
        .map(|(_, value)| value.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Physical cores in a `/proc/cpuinfo` body: the number of distinct
/// `(physical id, core id)` pairs, which collapses SMT siblings onto the core
/// they share and sums correctly across sockets.
///
/// Returns `None` when the keys are absent — the aarch64 shape, where the file
/// carries no topology at all and guessing from the processor count would just
/// restate the logical count under a name that promises more.
/// Compiled on every target though only the Linux path calls it (see above).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn cpuinfo_physical_cores(raw: &str) -> Option<u32> {
    use std::collections::BTreeSet;

    let mut seen: BTreeSet<(u64, u64)> = BTreeSet::new();
    let (mut physical, mut core) = (None, None);
    // Blocks are separated by a blank line; a pair completes when both keys of
    // the current block have been seen.
    for line in raw.lines() {
        match line.split_once(':') {
            Some((key, value)) => {
                let value = value.trim().parse::<u64>().ok();
                match key.trim() {
                    "physical id" => physical = value,
                    "core id" => core = value,
                    _ => continue,
                }
            }
            None => {
                physical = None;
                core = None;
                continue;
            }
        }
        if let (Some(p), Some(c)) = (physical, core) {
            seen.insert((p, c));
            physical = None;
            core = None;
        }
    }
    u32::try_from(seen.len()).ok().filter(|n| *n > 0)
}

#[cfg(target_os = "linux")]
mod linux {
    use coppice_core::node::HostFacts;

    /// Read a best-effort file, logging (never failing) when it is unreadable.
    fn read(path: &str) -> Option<String> {
        match std::fs::read_to_string(path) {
            Ok(raw) => Some(raw),
            Err(err) => {
                tracing::debug!(path, error = %err, "unreadable source for host facts");
                None
            }
        }
    }

    pub(super) fn fill(facts: &mut HostFacts) {
        if let Some(raw) = read("/etc/os-release") {
            facts.os_version = super::os_release_value(&raw, "PRETTY_NAME").unwrap_or_default();
        }
        if let Some(raw) = read("/proc/sys/kernel/osrelease") {
            facts.kernel_version = raw.trim().to_string();
        }
        if let Some(raw) = read("/proc/cpuinfo") {
            facts.cpu_model = super::cpuinfo_model(&raw).unwrap_or_default();
            facts.physical_cores = super::cpuinfo_physical_cores(&raw).unwrap_or(0);
        }
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use coppice_core::node::HostFacts;

    /// Every key one `sysctl` call fetches, in one place so the parse and the
    /// argument list cannot drift apart.
    const KEYS: [&str; 6] = [
        "kern.osproductversion",
        "kern.osrelease",
        "hw.model",
        "machdep.cpu.brand_string",
        "hw.physicalcpu",
        "hw.logicalcpu",
    ];

    pub(super) fn fill(facts: &mut HostFacts) {
        let Some(raw) = sysctl() else { return };
        super::fill_from_sysctl(facts, &raw);
    }

    /// One `sysctl` covering [`KEYS`]. Deliberately *not* `-n`: with bare
    /// values a key this kernel does not know is silently skipped, shifting
    /// every later line onto the wrong field, and the failure mode would be a
    /// kernel version rendered as a CPU model rather than an honest blank.
    /// Keyed output costs nothing and is order-independent.
    ///
    /// A key `sysctl` does not recognize (common in VMs, e.g.
    /// `machdep.cpu.brand_string`) makes the whole invocation exit nonzero,
    /// but the process still writes every key it *did* resolve to stdout
    /// before that — only the unknown keys' errors go to stderr. So this
    /// returns the captured stdout whenever there is any, even on a nonzero
    /// exit, and lets the keyed parse in [`super::fill_from_sysctl`] fill
    /// whatever fields the process managed to answer.
    fn sysctl() -> Option<String> {
        let out = std::process::Command::new("/usr/sbin/sysctl")
            .args(KEYS)
            .output();
        match out {
            Ok(out) if out.status.success() => Some(String::from_utf8_lossy(&out.stdout).into()),
            Ok(out) if !out.stdout.is_empty() => {
                tracing::debug!(
                    status = %out.status,
                    stderr = %String::from_utf8_lossy(&out.stderr),
                    "sysctl exited nonzero; using the keys it did resolve",
                );
                Some(String::from_utf8_lossy(&out.stdout).into())
            }
            Ok(out) => {
                tracing::debug!(status = %out.status, "sysctl failed; no macOS host facts");
                None
            }
            Err(err) => {
                tracing::debug!(error = %err, "running sysctl for host facts");
                None
            }
        }
    }
}

/// Fill the sysctl-sourced fields of `facts` from raw keyed `sysctl` output.
///
/// Pulled out of the `macos` module so it is a pure function over an owned
/// string — testable on any OS without running `sysctl` — matching the
/// module's convention for the other per-platform parsers.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn fill_from_sysctl(facts: &mut coppice_core::node::HostFacts, raw: &str) {
    let values = parse_sysctl(raw);
    let get = |key: &str| values.get(key).cloned().unwrap_or_default();

    facts.os_version = get("kern.osproductversion");
    facts.kernel_version = get("kern.osrelease");
    // The brand string is the human-meaningful name ("Apple M2 Pro"); the
    // board id ("Mac14,9") is the fallback when a VM omits the former.
    facts.cpu_model = match get("machdep.cpu.brand_string") {
        brand if !brand.is_empty() => brand,
        _ => get("hw.model"),
    };
    facts.physical_cores = get("hw.physicalcpu").parse().unwrap_or(0);
    // Prefer the sysctl over `available_parallelism`, which reports the
    // *available* (QoS-clamped) count rather than what the box has.
    if let Ok(logical) = get("hw.logicalcpu").parse() {
        facts.logical_cores = logical;
    }
}

/// Parse keyed `sysctl` output (`kern.osrelease: 24.5.0`) into a map.
///
/// Splitting on the *first* `": "` keeps values that themselves contain a
/// colon intact; a line without the separator is not a reading and is dropped.
/// Compiled on every target though only the macOS path calls it: the pure
/// parsers are unit-tested on any OS.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn parse_sysctl(raw: &str) -> std::collections::BTreeMap<String, String> {
    raw.lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
        .filter(|(_, value)| !value.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn os_release_unquotes_values() {
        let fixture = "NAME=\"Debian GNU/Linux\"\nPRETTY_NAME=\"Debian GNU/Linux 12 (bookworm)\"\nID=debian\n";
        assert_eq!(
            os_release_value(fixture, "PRETTY_NAME").as_deref(),
            Some("Debian GNU/Linux 12 (bookworm)")
        );
        // Bare and single-quoted values both round-trip.
        assert_eq!(os_release_value(fixture, "ID").as_deref(), Some("debian"));
        assert_eq!(
            os_release_value("PRETTY_NAME='Alpine Linux v3.20'\n", "PRETTY_NAME").as_deref(),
            Some("Alpine Linux v3.20")
        );
    }

    #[test]
    fn os_release_missing_or_empty_is_unknown() {
        assert_eq!(os_release_value("ID=debian\n", "PRETTY_NAME"), None);
        assert_eq!(os_release_value("PRETTY_NAME=\n", "PRETTY_NAME"), None);
        assert_eq!(os_release_value("", "PRETTY_NAME"), None);
        // A key that only *contains* the sought name is not a match: the
        // prefix must be followed by `=`.
        assert_eq!(os_release_value("PRETTY_NAME_X=x\n", "PRETTY_NAME"), None);
    }

    #[test]
    fn cpuinfo_model_takes_the_first_core() {
        let fixture = "processor\t: 0\nmodel name\t: AMD EPYC 7763 64-Core Processor\n\nprocessor\t: 1\nmodel name\t: AMD EPYC 7763 64-Core Processor\n";
        assert_eq!(
            cpuinfo_model(fixture).as_deref(),
            Some("AMD EPYC 7763 64-Core Processor")
        );
        // The aarch64 shape carries no model name at all.
        assert_eq!(cpuinfo_model("processor\t: 0\nBogoMIPS\t: 50.00\n"), None);
    }

    #[test]
    fn physical_cores_collapse_smt_siblings() {
        // Two cores, each with two hyperthreads: four blocks, two pairs.
        let smt = "processor\t: 0\nphysical id\t: 0\ncore id\t: 0\n\n\
                   processor\t: 1\nphysical id\t: 0\ncore id\t: 1\n\n\
                   processor\t: 2\nphysical id\t: 0\ncore id\t: 0\n\n\
                   processor\t: 3\nphysical id\t: 0\ncore id\t: 1\n";
        assert_eq!(cpuinfo_physical_cores(smt), Some(2));
    }

    #[test]
    fn physical_cores_sum_across_sockets() {
        // The same core id on a second socket is a different core.
        let dual = "processor\t: 0\nphysical id\t: 0\ncore id\t: 0\n\n\
                    processor\t: 1\nphysical id\t: 1\ncore id\t: 0\n";
        assert_eq!(cpuinfo_physical_cores(dual), Some(2));
    }

    #[test]
    fn physical_cores_absent_without_topology() {
        // The aarch64 shape: processors, but no topology keys to count.
        assert_eq!(
            cpuinfo_physical_cores("processor\t: 0\n\nprocessor\t: 1\n"),
            None
        );
        assert_eq!(cpuinfo_physical_cores(""), None);
    }

    #[test]
    fn sysctl_output_is_keyed_not_positional() {
        let fixture = "kern.osproductversion: 15.5\n\
                       kern.osrelease: 24.5.0\n\
                       machdep.cpu.brand_string: Apple M2 Pro\n\
                       hw.physicalcpu: 10\n";
        let values = parse_sysctl(fixture);
        assert_eq!(values["kern.osrelease"], "24.5.0");
        assert_eq!(values["machdep.cpu.brand_string"], "Apple M2 Pro");
        assert_eq!(values["hw.physicalcpu"], "10");
        // A key the kernel did not answer is simply absent — the reason this
        // parses keys instead of positions.
        assert!(!values.contains_key("hw.model"));
    }

    #[test]
    fn sysctl_parse_skips_junk_and_keeps_colons_in_values() {
        let values = parse_sysctl("garbage\nkern.a: x: y\nkern.b: \n");
        assert_eq!(values["kern.a"], "x: y");
        assert!(!values.contains_key("kern.b"), "empty value is unknown");
        assert!(!values.contains_key("garbage"));
    }

    #[test]
    fn fill_from_sysctl_fills_present_fields_and_leaves_missing_empty() {
        // As `sysctl` would emit when `machdep.cpu.brand_string` is
        // unsupported (the VM case this exists to tolerate): every other key
        // resolved, that one did not, and the real invocation would have
        // exited nonzero here — but the parse never sees the exit status.
        let raw = "kern.osproductversion: 15.5\n\
                   kern.osrelease: 24.5.0\n\
                   hw.model: VirtualMac2,1\n\
                   hw.physicalcpu: 4\n\
                   hw.logicalcpu: 8\n";
        let mut facts = coppice_core::node::HostFacts::default();
        fill_from_sysctl(&mut facts, raw);

        assert_eq!(facts.os_version, "15.5");
        assert_eq!(facts.kernel_version, "24.5.0");
        // Falls back to the board id when the brand string is absent.
        assert_eq!(facts.cpu_model, "VirtualMac2,1");
        assert_eq!(facts.physical_cores, 4);
        assert_eq!(facts.logical_cores, 8);
    }

    #[test]
    fn fill_from_sysctl_empty_stdout_leaves_defaults() {
        // The genuinely-nothing-resolved case: every field stays at its
        // zero/empty default, "unknown" on the wire, rather than panicking.
        let mut facts = coppice_core::node::HostFacts {
            logical_cores: 3, // pre-seeded by the cross-platform fallback
            ..coppice_core::node::HostFacts::default()
        };
        fill_from_sysctl(&mut facts, "");

        assert_eq!(facts.os_version, "");
        assert_eq!(facts.kernel_version, "");
        assert_eq!(facts.cpu_model, "");
        assert_eq!(facts.physical_cores, 0);
        // No logical-core key resolved, so the pre-seeded value is untouched
        // rather than being clobbered with zero.
        assert_eq!(facts.logical_cores, 3);
    }

    #[test]
    fn collect_describes_this_host() {
        let dir = tempfile::tempdir().expect("temp dir");
        let detected = crate::capacity::detect(dir.path());
        let facts = collect(&detected);

        assert_eq!(facts.os, std::env::consts::OS);
        assert_eq!(facts.arch, std::env::consts::ARCH);
        assert!(
            !facts.agent_version.is_empty(),
            "the agent knows its version"
        );
        assert!(facts.logical_cores >= 1, "at least one thread");
        // The totals are exactly what capacity detection read — never a
        // second, possibly disagreeing, syscall.
        assert_eq!(facts.total_disk_bytes, detected.disk.clone().unwrap_or(0));
        assert_eq!(
            facts.total_memory_bytes,
            detected.memory.clone().unwrap_or(0)
        );
    }

    /// The platform readers are best-effort by contract, so this asserts the
    /// shape rather than any particular value: a sandboxed CI box may refuse
    /// the `sysctl` subprocess, and a container may mount no `/etc/os-release`.
    #[test]
    fn collect_leaves_unknown_fields_empty_rather_than_failing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let facts = collect(&crate::capacity::detect(dir.path()));
        if cfg!(target_os = "linux") {
            // `/proc/sys/kernel/osrelease` has no failure mode on a real Linux.
            assert!(!facts.kernel_version.is_empty(), "kernel release reads");
        }
        // Nothing panics and nothing is a placeholder string.
        assert!(!facts.os_version.contains("unknown"));
    }
}
