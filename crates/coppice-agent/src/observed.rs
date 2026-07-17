//! Building the ObservedSet from the recovered journal and the container
//! runtime (ADR 0009 restart reconciliation). Pure: no I/O, no async — the
//! crash sweep asserts these rules directly.
//!
//! The one law the agent lives by: **never trust memory over journal +
//! runtime, and never claim `running = true` without runtime evidence.** The
//! precedence below encodes exactly that.

use coppice_core::attempt::AttemptOutcome;
use coppice_core::id::{AllocationId, AttemptId, JobId};
use coppice_core::time::Duration;

use crate::executor::{classify_exit, ContainerState, ObservedContainer};
use crate::journal::JournalState;

/// One entry of the ObservedSet. `outcome` is `None` while `running`; when the
/// container has ended, `outcome` records how (ADR 0013 taxonomy).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedAllocation {
    pub allocation: AllocationId,
    pub attempt: AttemptId,
    pub job: JobId,
    pub running: bool,
    pub outcome: Option<AttemptOutcome>,
    pub runtime: Duration,
}

/// Reconcile the recovered `journal` against the live `runtime` into the full
/// ObservedSet, following ADR 0009's truth rules with runtime-beats-journal
/// precedence:
///
/// 1. Every runtime container appears, with its true state — running →
///    `running = true`; exited with no journaled exit → `running = false`
///    with the *runtime-observed* classification (OOM vs. exit code). Runtime
///    evidence wins even if the journal intent is missing entirely (a
///    survivor is never forgotten). Ids come from the container labels.
/// 2. A journaled exit → `running = false` with the *journaled* outcome and
///    runtime, whether or not the exited container survives. When both
///    sources agree the container exited, the journal's classification is
///    the truth: it carries kill attribution (`Aborted`,
///    `RuntimeLimitExceeded`) that the runtime's bare exit code cannot show,
///    and re-classifying from the container would let a crash between
///    journal and reap flip an aborted attempt into `Exited { code }`.
///    Runtime evidence overrides a journaled exit only by showing the
///    container still *running* (rule 1 — never claim or deny `running`
///    without runtime evidence).
/// 3. A journaled intent with neither a runtime container nor a journaled exit
///    → `running = false, outcome = AgentError, runtime = 0`: the honest "I
///    lost it". The agent never restarts a pending intent after a crash — the
///    re-registration epoch bump has already fenced it, so it reports the
///    doubt and lets the coordinator re-plan (ADR 0009).
/// 4. A tombstone with no intent, exit, or container contributes nothing.
///
/// Output is deterministic (allocation-id order).
pub fn build_observed_set(
    journal: &JournalState,
    runtime: &[ObservedContainer],
) -> Vec<ObservedAllocation> {
    use std::collections::{BTreeMap, BTreeSet};

    // Index the runtime by allocation for precedence lookups.
    let runtime_by_alloc: BTreeMap<AllocationId, &ObservedContainer> =
        runtime.iter().map(|c| (c.allocation, c)).collect();

    // Every allocation we might report: runtime containers, journaled exits,
    // journaled intents. Tombstones alone contribute nothing (rule 4).
    let mut allocations: BTreeSet<AllocationId> = BTreeSet::new();
    allocations.extend(runtime_by_alloc.keys().copied());
    allocations.extend(journal.exits.keys().copied());
    allocations.extend(journal.intents.keys().copied());

    let mut out = Vec::with_capacity(allocations.len());
    for allocation in allocations {
        // Rule 1: runtime evidence wins — a running container, or an exited
        // one whose exit never reached the journal.
        if let Some(container) = runtime_by_alloc.get(&allocation) {
            let journaled_exit = journal.exits.get(&allocation);
            if matches!(container.state, ContainerState::Running { .. }) || journaled_exit.is_none()
            {
                out.push(from_runtime(container));
                continue;
            }
            // Both sources say exited: fall through to rule 2 — the journaled
            // classification is the truth (see the precedence doc above).
        }
        // Rule 2: journaled exit.
        if let Some(exit) = journal.exits.get(&allocation) {
            out.push(ObservedAllocation {
                allocation,
                attempt: exit.attempt,
                job: exit.job,
                running: false,
                outcome: Some(exit.outcome.clone()),
                runtime: exit.runtime,
            });
            continue;
        }
        // Rule 3: journaled intent with no evidence of its fate.
        if let Some(intent) = journal.intents.get(&allocation) {
            out.push(ObservedAllocation {
                allocation,
                attempt: intent.attempt,
                job: intent.job,
                running: false,
                outcome: Some(AttemptOutcome::AgentError),
                runtime: Duration::ZERO,
            });
            continue;
        }
        // Only a tombstone: nothing to report (rule 4).
    }
    out
}

fn from_runtime(container: &ObservedContainer) -> ObservedAllocation {
    match container.state {
        ContainerState::Running { runtime } => ObservedAllocation {
            allocation: container.allocation,
            attempt: container.attempt,
            job: container.job,
            running: true,
            outcome: None,
            runtime,
        },
        ContainerState::Exited(exit) => ObservedAllocation {
            allocation: container.allocation,
            attempt: container.attempt,
            job: container.job,
            running: false,
            outcome: Some(classify_exit(&exit)),
            runtime: exit.runtime,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{ExitCause, ExitInfo};
    use crate::journal::{ExitRec, IntentRec, JournalState};
    use coppice_core::time::Timestamp;

    fn ids() -> (AllocationId, AttemptId, JobId) {
        (AllocationId::new(), AttemptId::new(), JobId::new())
    }

    #[test]
    fn runtime_running_container_is_reported_running() {
        let (a, at, j) = ids();
        let runtime = vec![ObservedContainer {
            allocation: a,
            attempt: at,
            job: j,
            state: ContainerState::Running {
                runtime: Duration::from_micros(100),
            },
        }];
        let set = build_observed_set(&JournalState::default(), &runtime);
        assert_eq!(set.len(), 1);
        assert!(set[0].running);
        assert_eq!(set[0].outcome, None);
        assert_eq!(set[0].runtime, Duration::from_micros(100));
    }

    #[test]
    fn runtime_beats_journal_even_with_missing_intent() {
        // Survivor with no journal intent (can't happen given fsync-before-start,
        // but must never be forgotten).
        let (a, at, j) = ids();
        let runtime = vec![ObservedContainer {
            allocation: a,
            attempt: at,
            job: j,
            state: ContainerState::Exited(ExitInfo {
                code: 0,
                cause: ExitCause::OomKilled,
                runtime: Duration::from_micros(9),
                finished_at: Timestamp::UNIX_EPOCH,
            }),
        }];
        let set = build_observed_set(&JournalState::default(), &runtime);
        assert_eq!(set[0].outcome, Some(AttemptOutcome::MemoryLimitExceeded));
        assert!(!set[0].running);
    }

    #[test]
    fn journaled_outcome_beats_runtime_classification_when_both_exited() {
        // A stop journaled `Aborted`, then the crash hit before reap: the
        // surviving container's bare 137 must not flip the outcome.
        let (a, at, j) = ids();
        let mut state = JournalState::default();
        state.exits.insert(
            a,
            ExitRec {
                allocation: a,
                attempt: at,
                job: j,
                outcome: AttemptOutcome::Aborted,
                runtime: Duration::from_micros(7),
            },
        );
        let runtime = vec![ObservedContainer {
            allocation: a,
            attempt: at,
            job: j,
            state: ContainerState::Exited(ExitInfo {
                code: 137,
                cause: ExitCause::Natural,
                runtime: Duration::from_micros(9),
                finished_at: Timestamp::UNIX_EPOCH,
            }),
        }];
        let set = build_observed_set(&state, &runtime);
        assert_eq!(set.len(), 1);
        assert!(!set[0].running);
        assert_eq!(set[0].outcome, Some(AttemptOutcome::Aborted));
        assert_eq!(set[0].runtime, Duration::from_micros(7));
    }

    #[test]
    fn journaled_exit_without_container_reports_journaled_outcome() {
        let (a, at, j) = ids();
        let mut state = JournalState::default();
        state.exits.insert(
            a,
            ExitRec {
                allocation: a,
                attempt: at,
                job: j,
                outcome: AttemptOutcome::Aborted,
                runtime: Duration::from_micros(7),
            },
        );
        let set = build_observed_set(&state, &[]);
        assert_eq!(set[0].outcome, Some(AttemptOutcome::Aborted));
        assert_eq!(set[0].runtime, Duration::from_micros(7));
    }

    #[test]
    fn lost_intent_is_reported_as_agent_error() {
        let (a, at, j) = ids();
        let mut state = JournalState::default();
        state.intents.insert(
            a,
            IntentRec {
                allocation: a,
                attempt: at,
                job: j,
                node_epoch: 1,
            },
        );
        let set = build_observed_set(&state, &[]);
        assert_eq!(set[0].outcome, Some(AttemptOutcome::AgentError));
        assert!(!set[0].running);
    }

    #[test]
    fn tombstone_alone_reports_nothing() {
        let (a, _, _) = ids();
        let mut state = JournalState::default();
        state.tombstones.insert(a);
        assert!(build_observed_set(&state, &[]).is_empty());
    }

    #[test]
    fn runtime_beats_a_journaled_exit_for_the_same_allocation() {
        let (a, at, j) = ids();
        let mut state = JournalState::default();
        state.exits.insert(
            a,
            ExitRec {
                allocation: a,
                attempt: at,
                job: j,
                outcome: AttemptOutcome::Aborted,
                runtime: Duration::from_micros(1),
            },
        );
        let runtime = vec![ObservedContainer {
            allocation: a,
            attempt: at,
            job: j,
            state: ContainerState::Running {
                runtime: Duration::from_micros(55),
            },
        }];
        let set = build_observed_set(&state, &runtime);
        assert_eq!(set.len(), 1);
        assert!(
            set[0].running,
            "a live container wins over a journaled exit"
        );
    }
}
