//! The shared shape of a `[discovery]` config section (ADR 0037 §2).
//!
//! Discovery answers "whom might I dial first?", never "who are the voters?";
//! its output is advisory seed addresses only. A section names a `backend` and
//! carries exactly one matching backend table.
//!
//! [`SeedConfig`] is the portable half: the selector plus the four backend
//! tables, everything [`build`](crate::build) needs and nothing else. A host
//! daemon that carries extra node-local knobs in the same TOML table (the
//! coordinator's `cluster_size`, for one) declares its own struct over these
//! same field types and projects a `SeedConfig` out of it — `serde(flatten)`
//! is deliberately not used, because it silently disables
//! `deny_unknown_fields` and a typo'd knob must still fail-stop.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

/// The portable half of a `[discovery]` section: which backend seeds candidate
/// addresses, and its one matching table.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedConfig {
    /// Which backend supplies candidate raft addresses. Exactly one matching
    /// backend table must be present (validated in [`SeedConfig::validate`]).
    #[serde(default)]
    pub backend: BackendKind,

    /// `[discovery.static]` — present iff `backend = "static"`.
    #[serde(default, rename = "static")]
    pub static_backend: Option<StaticBackend>,

    /// `[discovery.dns]` — present iff `backend = "dns"`.
    #[serde(default)]
    pub dns: Option<DnsBackend>,

    /// `[discovery.file]` — present iff `backend = "file"`.
    #[serde(default)]
    pub file: Option<FileBackend>,

    /// `[discovery.ec2_asg]` — present iff `backend = "ec2-asg"`
    /// (ADR 0037 §2).
    #[serde(default)]
    pub ec2_asg: Option<Ec2AsgBackend>,
}

/// The discovery backend selector. TOML spelling matches ADR 0037 §2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendKind {
    /// The literal seed list — today's `peers`, under a new roof.
    #[default]
    Static,
    /// Resolve one DNS name per consultation (A/AAAA + SRV).
    Dns,
    /// Enumerate a well-known directory of run-scoped registration files.
    File,
    /// EC2 auto-scaling-group membership (ADR 0037 §2).
    Ec2Asg,
}

/// `[discovery.static]`: the literal list of dialable raft addresses.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticBackend {
    #[serde(default)]
    pub addrs: Vec<String>,
}

/// `[discovery.dns]`: one name resolved per consultation. SRV records
/// supply their own ports; A/AAAA records use `port`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DnsBackend {
    pub name: String,
    /// Fallback port for A/AAAA records that carry none.
    pub port: u16,
}

/// `[discovery.file]`: a directory of run-scoped registration files, each
/// naming one candidate on its first line.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileBackend {
    pub dir: PathBuf,
}

/// `[discovery.ec2_asg]`: the EC2 auto-scaling-group backend (ADR 0037 §2).
///
/// The instance id and region are read from EC2 instance metadata (IMDSv2)
/// at each consultation, so neither is configured here. `port` is required:
/// discovery composes `private-ip:port` candidates and the raft listen port
/// is not plumbed into the discovery builder, so the operator names it
/// explicitly (the same shape as `[discovery.dns].port`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ec2AsgBackend {
    /// The raft port composed onto every discovered instance's private IP.
    pub port: u16,
    /// Explicit AWS region override. Optional: when unset the region is
    /// taken from this instance's IMDS document, which is the normal case
    /// for a coordinator running inside the group it discovers.
    #[serde(default)]
    pub region: Option<String>,
    /// Per-AWS-call timeout. Discovery must never hang startup (ADR 0037 §2
    /// contract), so each IMDS/ASG/EC2 call is bounded by this and a slow or
    /// unreachable control plane degrades to an empty candidate list with a
    /// warning rather than blocking convergence.
    #[serde(default = "default_ec2_asg_timeout", with = "humantime_serde")]
    pub timeout: Duration,
}

impl SeedConfig {
    /// Reject a section whose backend tables do not match `backend`:
    /// exactly the one table matching `backend` must be present — no
    /// foreign backend table, and no absent table (`static` included: an
    /// operator with no seeds writes an explicit empty `addrs`, so the
    /// migration off the old top-level `peers` is always visible in the
    /// file, ADR 0037 §2).
    pub fn validate(&self) -> anyhow::Result<()> {
        // No foreign tables.
        let foreign = [
            (self.backend != BackendKind::Static && self.static_backend.is_some())
                .then_some("static"),
            (self.backend != BackendKind::Dns && self.dns.is_some()).then_some("dns"),
            (self.backend != BackendKind::File && self.file.is_some()).then_some("file"),
            (self.backend != BackendKind::Ec2Asg && self.ec2_asg.is_some()).then_some("ec2_asg"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        if !foreign.is_empty() {
            anyhow::bail!(
                "[discovery] backend = \"{}\" but unrelated table(s) present: {} — \
                 keep exactly the one matching table (ADR 0037 §2)",
                self.backend.as_str(),
                foreign.join(", "),
            );
        }

        // Required table present, for every backend including static.
        match self.backend {
            BackendKind::Static if self.static_backend.is_none() => {
                anyhow::bail!(
                    "[discovery] backend = \"static\" requires a [discovery.static] table \
                     with `addrs` (an explicit empty list is valid, ADR 0037 §2)"
                );
            }
            BackendKind::Static => {}
            BackendKind::Dns if self.dns.is_none() => {
                anyhow::bail!(
                    "[discovery] backend = \"dns\" requires a [discovery.dns] table \
                     with `name` and `port` (ADR 0037 §2)"
                );
            }
            BackendKind::File if self.file.is_none() => {
                anyhow::bail!(
                    "[discovery] backend = \"file\" requires a [discovery.file] table \
                     with `dir` (ADR 0037 §2)"
                );
            }
            BackendKind::Ec2Asg if self.ec2_asg.is_none() => {
                anyhow::bail!(
                    "[discovery] backend = \"ec2-asg\" requires a [discovery.ec2_asg] \
                     table with `port` (ADR 0037 §2)"
                );
            }
            _ => {}
        }
        Ok(())
    }

    /// A `static` section over `addrs`, built in code rather than parsed.
    ///
    /// For a host that composes its seeds itself instead of reading them from a
    /// file — `coppice dev` naming its own in-process coordinator, a test
    /// harness naming its gateway — so those call sites do not each spell out
    /// the four `None`s the exactly-one-table rule requires.
    pub fn static_seeds(addrs: Vec<String>) -> SeedConfig {
        SeedConfig {
            backend: BackendKind::Static,
            static_backend: Some(StaticBackend { addrs }),
            dns: None,
            file: None,
            ec2_asg: None,
        }
    }

    /// The static seed list, if this config selects the `static` backend —
    /// the successor to the old top-level `peers`.
    pub fn static_addrs(&self) -> &[String] {
        match (self.backend, &self.static_backend) {
            (BackendKind::Static, Some(s)) => &s.addrs,
            _ => &[],
        }
    }
}

impl BackendKind {
    /// The TOML spelling, for operator-facing messages.
    pub fn as_str(self) -> &'static str {
        match self {
            BackendKind::Static => "static",
            BackendKind::Dns => "dns",
            BackendKind::File => "file",
            BackendKind::Ec2Asg => "ec2-asg",
        }
    }
}

/// The default `[discovery.ec2_asg] timeout`, exposed so a host daemon that
/// declares its own struct over [`Ec2AsgBackend`] keeps the same default.
pub fn default_ec2_asg_timeout() -> Duration {
    Duration::from_secs(3)
}
