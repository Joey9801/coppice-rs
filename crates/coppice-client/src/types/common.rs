//! Vocabulary shared across several endpoints: resources, lifecycle states,
//! attempts and allocations.

use serde::{Deserialize, Serialize};

use crate::id::{AllocationId, AttemptId, JobId, NodeId};
use crate::time::Timestamp;

/// A quantity of each resource dimension.
///
/// Sizes cross this wire as plain `u64` byte counts rather than humane
/// strings, so they are unambiguous to a machine: `Resources::new(2_000, 8 *
/// 1024 * 1024 * 1024, 0)` asks for two cores and 8 GiB. Rendering those
/// counts for a human is the caller's business, not this crate's.
///
/// This is a **write** shape as well as a read one, so it is exhaustive and
/// the server rejects an unknown field in it: a typo like `"cpu_milis"` must
/// be an error, not a silent zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Resources {
    /// CPU in thousandths of a core.
    pub cpu_millis: u64,
    /// Memory in bytes.
    pub memory_bytes: u64,
    /// Disk in bytes.
    pub disk_bytes: u64,
}

impl Resources {
    /// A resource triple.
    pub fn new(cpu_millis: u64, memory_bytes: u64, disk_bytes: u64) -> Resources {
        Resources {
            cpu_millis,
            memory_bytes,
            disk_bytes,
        }
    }
}

/// How many times a job may be retried, and for what.
///
/// A write shape: absent on a submission means the platform default policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Retries after the first attempt.
    pub max_retries: u32,
    /// Opt in to retrying user-error outcomes (a nonzero exit, a limit
    /// breach). Never applies to an abort.
    pub retry_user_errors: bool,
}

impl RetryPolicy {
    /// A policy allowing `max_retries` retries of platform failures only.
    pub fn new(max_retries: u32) -> RetryPolicy {
        RetryPolicy {
            max_retries,
            retry_user_errors: false,
        }
    }

    /// The same, but user errors are retried too.
    pub fn retrying_user_errors(max_retries: u32) -> RetryPolicy {
        RetryPolicy {
            max_retries,
            retry_user_errors: true,
        }
    }
}

/// A job's raw lifecycle state, as the state machine stores it.
///
/// This is the state on a job record. The *displayed* status most surfaces
/// want is [`JobPhase`], the read-time join of this with the current
/// attempt's own state.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum JobStateKind {
    /// Accepted by the API, not yet admitted.
    Submitted,
    /// Admitted, not yet queued.
    Accepted,
    /// Waiting for capacity, with no attempt yet.
    Queued,
    /// Pursuing an attempt.
    Attempting,
    /// Finished successfully.
    Succeeded,
    /// Finished unsuccessfully.
    Failed,
    /// Stopped on request.
    Aborted,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl JobStateKind {
    /// Whether the job has finished and will not change again.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStateKind::Succeeded | JobStateKind::Failed | JobStateKind::Aborted
        )
    }

    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, JobStateKind::Unknown(_))
    }
}

/// A job's flat display **phase**: the read-time join of its state with
/// the state of the attempt it carries.
///
/// The lifecycle vocabulary, in order:
///
/// - `Queued` — waiting, unpinned; no attempt exists yet.
/// - `Accruing` — the current attempt holds a partially funded
///   allocation: it has a node but has not started. A **subset of queue
///   depth**, which is why `QueueStats::depth` counts `Queued + Accruing`.
/// - `Preparing` — placed and fully funded, awaiting the agent's start
///   report. Distinct from `Accruing` so "where is my job" never folds two
///   different waits into one label.
/// - `Running` — observed running.
/// - `Finalizing` — the attempt is finishing or has just finished.
/// - `Submitted`/`Accepted` are pre-admission; `Succeeded`/`Failed`/
///   `Aborted` are terminal.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum JobPhase {
    /// Accepted by the API, not yet admitted.
    Submitted,
    /// Admitted, not yet queued.
    Accepted,
    /// Waiting for capacity, with no attempt yet.
    Queued,
    /// Has a node and a partial allocation; still waiting.
    Accruing,
    /// Placed and fully funded, awaiting the start report.
    Preparing,
    /// Observed running.
    Running,
    /// Finishing.
    Finalizing,
    /// Finished successfully.
    Succeeded,
    /// Finished unsuccessfully.
    Failed,
    /// Stopped on request.
    Aborted,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. `JobPhase` is a **map key** in
    /// `by_state`: carrying the spelling here (rather than a bare unit
    /// variant) is what keeps two future phases from folding into one entry
    /// and losing a count. See [`super`] for the general rationale.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl JobPhase {
    /// Every phase, in lifecycle order — the keys `QueueStats::by_state`
    /// reports a count for, zeros included.
    ///
    /// `Ord` follows this order, so the map iterates in it.
    pub const ALL: [JobPhase; 10] = [
        JobPhase::Submitted,
        JobPhase::Accepted,
        JobPhase::Queued,
        JobPhase::Accruing,
        JobPhase::Preparing,
        JobPhase::Running,
        JobPhase::Finalizing,
        JobPhase::Succeeded,
        JobPhase::Failed,
        JobPhase::Aborted,
    ];

    /// Whether the job has finished and will not change again.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobPhase::Succeeded | JobPhase::Failed | JobPhase::Aborted
        )
    }

    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, JobPhase::Unknown(_))
    }
}

/// An attempt's state. The `Terminal` outcome payload travels separately
/// as [`AttemptView::outcome`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum AttemptState {
    /// Holding a partially funded allocation on a node.
    Accruing,
    /// Fully funded and placed, not yet dispatched.
    Ready,
    /// Handed to the node's agent.
    Dispatching,
    /// Observed running.
    Running,
    /// Stopping, with its charge not yet settled.
    Finalizing,
    /// Finished; see the outcome.
    Terminal,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl AttemptState {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, AttemptState::Unknown(_))
    }
}

/// Why an attempt reached its terminal state.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum AttemptOutcomeKind {
    /// The container exited; `exit_code` says with what.
    Exited,
    /// Killed for exceeding its memory limit.
    MemoryLimitExceeded,
    /// Killed for exceeding its runtime limit.
    RuntimeLimitExceeded,
    /// Killed for exceeding its disk limit.
    DiskLimitExceeded,
    /// Stopped because the job was aborted.
    Aborted,
    /// Its allocation was revoked.
    Revoked,
    /// The image could not be pulled.
    PullFailed,
    /// The container could not be started.
    StartFailed,
    /// The node it ran on was declared lost.
    NodeLost,
    /// The agent failed in a way it could not attribute.
    AgentError,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl AttemptOutcomeKind {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, AttemptOutcomeKind::Unknown(_))
    }
}

/// Who "owns" an outcome — which is what drives the retry policy.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum OutcomeClass {
    /// The work succeeded.
    Success,
    /// The workload's own fault: a nonzero exit, a limit breach.
    UserError,
    /// Asked for: an abort.
    UserRequest,
    /// Coppice's fault: a lost node, a failed pull, an agent error.
    Platform,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl OutcomeClass {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, OutcomeClass::Unknown(_))
    }
}

/// An attempt's terminal outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AttemptOutcome {
    /// What happened.
    pub kind: AttemptOutcomeKind,
    /// The process exit status. Present — and *omitted* rather than null —
    /// only when `kind` is `exited`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub exit_code: Option<i32>,
    /// Whose fault it was.
    pub class: OutcomeClass,
}

/// An allocation's funding state.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum AllocationState {
    /// Partially funded; still accruing capacity.
    Accruing,
    /// Fully funded, not yet active.
    Funded,
    /// Backing a running attempt.
    Active,
    /// Returned to the node.
    Released,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl AllocationState {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, AllocationState::Unknown(_))
    }
}

/// One execution attempt, with its charge metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AttemptView {
    /// The attempt's id.
    pub id: AttemptId,
    /// The job it is an attempt of.
    pub job: JobId,
    /// The node it was placed on.
    pub node: NodeId,
    /// The allocation backing it.
    pub allocation: AllocationId,
    /// Where it is in its lifecycle.
    pub state: AttemptState,
    /// Why it finished; present exactly while `state` is `terminal`.
    pub outcome: Option<AttemptOutcome>,
    /// When it started running, if it did.
    pub started_at: Option<Timestamp>,
    /// When it finished, if it has.
    pub ended_at: Option<Timestamp>,
    /// µCU per second while running (cost weights × requested resources).
    pub rate_ucu_per_second: u64,
    /// The upfront charge for this attempt, trued up at finalization.
    pub charged_ucu: u64,
}

/// One placement of a job onto a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AllocationView {
    /// The allocation's id.
    pub id: AllocationId,
    /// The job it serves.
    pub job: JobId,
    /// The attempt it backs.
    pub attempt: AttemptId,
    /// The node it is on.
    pub node: NodeId,
    /// What the job asked for.
    pub requested: Resources,
    /// What has been funded so far.
    pub funded: Resources,
    /// Where it is in its lifecycle.
    pub state: AllocationState,
    /// Commit order, which drives funding priority within a node.
    pub seq: u64,
}

/// Per-dimension funding progress, 0..1.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FundedFraction {
    /// CPU progress.
    pub cpu: f64,
    /// Memory progress.
    pub memory: f64,
    /// Disk progress.
    pub disk: f64,
}

/// An accruing allocation, with how far along its funding is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AccrualView {
    /// The allocation itself.
    pub allocation: AllocationView,
    /// How much of each dimension is funded.
    pub funded_fraction: FundedFraction,
    /// The earliest guaranteed full-funding time; `null` means unbounded.
    pub projected_start: Option<Timestamp>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_phase_keeps_its_spelling() {
        let phase: JobPhase = serde_json::from_value(serde_json::json!("paused")).unwrap();
        assert_eq!(phase, JobPhase::Unknown("paused".to_string()));
        assert_eq!(
            serde_json::to_value(&phase).unwrap(),
            serde_json::json!("paused")
        );
    }

    /// `by_state` is a map keyed by phase, so an unknown key must survive as
    /// its own entry rather than collapsing into a shared bucket and losing a
    /// count.
    #[test]
    fn unknown_phases_stay_distinct_as_map_keys() {
        let json = serde_json::json!({ "queued": 1, "paused": 2, "draining": 3 });
        let map: std::collections::BTreeMap<JobPhase, u32> =
            serde_json::from_value(json.clone()).unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(map[&JobPhase::Unknown("paused".to_string())], 2);
        assert_eq!(serde_json::to_value(&map).unwrap(), json);
    }

    #[test]
    fn an_outcome_omits_the_exit_code_when_there_is_none() {
        let outcome = AttemptOutcome {
            kind: AttemptOutcomeKind::NodeLost,
            exit_code: None,
            class: OutcomeClass::Platform,
        };
        assert_eq!(
            serde_json::to_value(&outcome).unwrap(),
            serde_json::json!({ "kind": "node_lost", "class": "platform" })
        );
    }

    #[test]
    fn terminal_states_agree_between_the_two_vocabularies() {
        assert!(JobStateKind::Succeeded.is_terminal());
        assert!(!JobStateKind::Attempting.is_terminal());
        assert!(JobPhase::Aborted.is_terminal());
        assert!(!JobPhase::Running.is_terminal());
    }

    /// `Display`/`FromStr` must round-trip both a known value and an unknown
    /// one, with the unknown case printing back exactly the spelling it was
    /// parsed from (never the literal string `"Unknown"`).
    #[test]
    fn display_and_from_str_round_trip_known_and_unknown_values() {
        use std::str::FromStr;

        let known = JobPhase::from_str("running").unwrap();
        assert_eq!(known, JobPhase::Running);
        assert_eq!(known.to_string(), "running");

        let unknown = JobPhase::from_str("paused").unwrap();
        assert_eq!(unknown, JobPhase::Unknown("paused".to_string()));
        assert_eq!(unknown.to_string(), "paused");
        assert!(unknown.is_unknown());
    }

    /// A non-string JSON value must still be a decode error — the untagged
    /// `Unknown(String)` catch-all only widens the accepted *strings*, it
    /// must not accept a number or `null` in their place.
    #[test]
    fn a_non_string_json_value_is_still_a_decode_error() {
        assert!(serde_json::from_value::<JobPhase>(serde_json::json!(42)).is_err());
        assert!(serde_json::from_value::<JobPhase>(serde_json::json!(null)).is_err());
        assert!(serde_json::from_value::<JobPhase>(serde_json::json!({})).is_err());
    }
}
