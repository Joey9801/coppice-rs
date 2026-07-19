//! The sink traits and their configuration (docker-executor.md §8.3).
//!
//! A *sink* absorbs batches of telemetry. Two traits split the two streams so a
//! sink instance can consume metrics, logs, or both: [`MetricsSink`] and
//! [`LogSink`]. Sinks are selected from config through the internally-tagged
//! [`SinkConfig`] enum, so each variant keeps its own `deny_unknown_fields`
//! fields; the v1 variant is the [`FilesystemSink`](super::FilesystemSink).
//!
//! **Delivery semantics.** `append` is *infallible at the boundary*: a sink
//! absorbs its own errors (a tracing error plus an error-level counter) and
//! never propagates failure or backpressure to the caller. The hub→sink path is
//! at-most-once within one process; end-to-end log ingestion across follower
//! restarts is at-least-once (§8.2). Loss is sanctioned only in a crash (§8.4):
//! any steady-state loss is a defect signal, which is why the failure path is an
//! error-level counter rather than a silent drop.

use std::future::Future;
use std::path::PathBuf;

use serde::Deserialize;

use super::{LogChunk, MetricSample};

/// A sink that absorbs batches of resource [`MetricSample`]s (docker-executor.md
/// §8.3).
///
/// Native RPIT-in-trait rather than the `async-trait` crate, the repo
/// convention documented at `executor.rs` (`Executor::next_exit`); `+ Send` is
/// written explicitly because the drain task the hub runs a sink on requires a
/// `Send` future. `append` never returns an error: a sink that cannot persist a
/// batch logs it and drops it (see the module delivery contract), so no failure
/// or backpressure ever reaches container execution.
pub trait MetricsSink: Send + Sync {
    /// Persist `batch`, in slice order. Infallible at the boundary.
    fn append(&self, batch: &[MetricSample]) -> impl Future<Output = ()> + Send;
}

/// A sink that absorbs batches of raw [`LogChunk`]s (docker-executor.md §8.3).
/// The same infallible-boundary contract as [`MetricsSink`].
pub trait LogSink: Send + Sync {
    /// Persist `batch`, in slice order. Infallible at the boundary.
    fn append(&self, batch: &[LogChunk]) -> impl Future<Output = ()> + Send;
}

/// Which telemetry stream a sink instance consumes (docker-executor.md §8.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkKind {
    /// Periodic resource [`MetricSample`]s (§8.1).
    Metrics,
    /// Raw [`LogChunk`]s (§8.2).
    Logs,
}

/// One configured sink instance (docker-executor.md §8.3, §10). An
/// internally-tagged enum, so each `[[telemetry.sinks]]` array entry names its
/// `type` and carries that variant's own fields:
///
/// ```toml
/// [[telemetry.sinks]]
/// type      = "filesystem"          # the only v1 variant
/// kinds     = ["metrics", "logs"]
/// retention = "60m"
/// # dir defaults to <data_dir>/telemetry
/// ```
///
/// Future variants (`clickhouse`, `loki`, …) are new arms plus impls; multiple
/// sinks, including multiple of one type, are just more array entries.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkConfig {
    /// The v1 [`FilesystemSink`](super::FilesystemSink): a segmented per-attempt
    /// SQLite store, the local source of truth (§8.4).
    Filesystem(FilesystemSinkConfig),
}

/// Configuration for the [`FilesystemSink`](super::FilesystemSink)
/// (docker-executor.md §8.4). Durations are humane strings the repo way
/// (`humantime_serde`, which rejects bare integers).
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FilesystemSinkConfig {
    /// Which streams this instance consumes. Must be non-empty and free of
    /// duplicates (see [`SinkConfig::validate`]).
    pub kinds: Vec<SinkKind>,

    /// How long after an attempt ends its segments are kept before the normal
    /// retention sweep deletes them (§8.4). Must be nonzero. Default 60m.
    #[serde(default = "default_retention", with = "humantime_serde")]
    pub retention: std::time::Duration,

    /// Root directory for this sink's segments. Overrides the default of
    /// `<data_dir>/telemetry`.
    #[serde(default)]
    pub dir: Option<PathBuf>,
}

impl SinkConfig {
    /// Reject semantically invalid values that `serde` alone cannot catch —
    /// bounds and set constraints — the way `config.rs` does: `anyhow::bail`
    /// naming the offending key so an operator can fix it directly. `serde`'s
    /// `deny_unknown_fields` and the humane-duration codec handle shape and
    /// typos; this handles meaning.
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            SinkConfig::Filesystem(config) => config.validate(),
        }
    }
}

impl FilesystemSinkConfig {
    fn validate(&self) -> anyhow::Result<()> {
        if self.kinds.is_empty() {
            anyhow::bail!("telemetry.sinks[].kinds must list at least one kind (§8.3)");
        }
        // A duplicate kind is a config mistake, not a doubling of writes: dedupe
        // by scanning for a repeat (the list is at most two entries).
        for (index, kind) in self.kinds.iter().enumerate() {
            if self.kinds[..index].contains(kind) {
                anyhow::bail!("telemetry.sinks[].kinds has a duplicate entry {kind:?} (§8.3)");
            }
        }
        if self.retention.is_zero() {
            anyhow::bail!("telemetry.sinks[].retention must be greater than zero (§8.4)");
        }
        Ok(())
    }
}

fn default_retention() -> std::time::Duration {
    std::time::Duration::from_secs(60 * 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deserialize one `[[telemetry.sinks]]` array into `Vec<SinkConfig>`, the
    /// shape the agent config nests it under, so the tests exercise exactly the
    /// operator-facing surface.
    #[derive(Deserialize)]
    struct SinksTable {
        sinks: Vec<SinkConfig>,
    }

    fn parse_one(entry: &str) -> Result<SinkConfig, toml::de::Error> {
        let toml = format!("[[sinks]]\n{entry}");
        toml::from_str::<SinksTable>(&toml).map(|mut table| table.sinks.remove(0))
    }

    #[test]
    fn happy_path_parses_all_fields() {
        let config = parse_one(
            r#"
type = "filesystem"
kinds = ["metrics", "logs"]
retention = "45m"
dir = "/var/lib/coppice-agent/telemetry"
"#,
        )
        .expect("should parse");
        let SinkConfig::Filesystem(fs) = config;
        assert_eq!(fs.kinds, vec![SinkKind::Metrics, SinkKind::Logs]);
        assert_eq!(fs.retention, std::time::Duration::from_secs(45 * 60));
        assert_eq!(
            fs.dir,
            Some(PathBuf::from("/var/lib/coppice-agent/telemetry"))
        );
    }

    #[test]
    fn defaults_apply_for_retention_and_dir() {
        let config = parse_one(
            r#"
type = "filesystem"
kinds = ["logs"]
"#,
        )
        .expect("should parse");
        let SinkConfig::Filesystem(fs) = config;
        assert_eq!(fs.retention, default_retention());
        assert_eq!(fs.retention, std::time::Duration::from_secs(60 * 60));
        assert_eq!(fs.dir, None);
    }

    #[test]
    fn unknown_field_in_entry_is_rejected() {
        // `deny_unknown_fields` on the inner struct must reject a typo'd key
        // even though the enum is internally tagged — the tag is consumed by the
        // enum, so it never leaks into the struct as an unknown field.
        let err = parse_one(
            r#"
type = "filesystem"
kinds = ["metrics"]
retenion = "60m"
"#,
        )
        .expect_err("typo'd field should fail");
        assert!(
            err.to_string().contains("retenion"),
            "error should name the offending key, got: {err}"
        );
    }

    #[test]
    fn unknown_type_is_rejected() {
        let err = parse_one(
            r#"
type = "clickhouse"
kinds = ["metrics"]
"#,
        )
        .expect_err("an unknown sink type should fail");
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn the_type_tag_does_not_leak_into_the_struct() {
        // Proves the tag field is not double-counted: a `deny_unknown_fields`
        // struct that also received `type` would reject the happy path above,
        // and this asserts the parse succeeds and preserves the other fields.
        let config = parse_one(
            r#"
type = "filesystem"
kinds = ["metrics"]
"#,
        )
        .expect("the tag must be consumed by the enum, not seen by the struct");
        assert!(matches!(config, SinkConfig::Filesystem(_)));
    }

    #[test]
    fn empty_kinds_is_rejected_by_validate() {
        let config = parse_one(
            r#"
type = "filesystem"
kinds = []
"#,
        )
        .expect("shape is valid; meaning is not");
        let err = config.validate().expect_err("empty kinds must fail");
        assert!(format!("{err:#}").contains("kinds"));
    }

    #[test]
    fn duplicate_kinds_is_rejected_by_validate() {
        let config = parse_one(
            r#"
type = "filesystem"
kinds = ["logs", "logs"]
"#,
        )
        .expect("shape is valid; meaning is not");
        let err = config.validate().expect_err("duplicate kinds must fail");
        assert!(format!("{err:#}").contains("duplicate"));
    }

    #[test]
    fn zero_retention_is_rejected_by_validate() {
        let config = parse_one(
            r#"
type = "filesystem"
kinds = ["metrics"]
retention = "0s"
"#,
        )
        .expect("shape is valid; meaning is not");
        let err = config.validate().expect_err("zero retention must fail");
        assert!(format!("{err:#}").contains("retention"));
    }

    #[test]
    fn raw_integer_retention_is_rejected() {
        // The humane-duration codec rejects a bare integer, the same stance the
        // agent config takes.
        let err = parse_one(
            r#"
type = "filesystem"
kinds = ["metrics"]
retention = 3600
"#,
        )
        .expect_err("a bare integer duration should fail");
        assert!(!err.to_string().is_empty());
    }
}
