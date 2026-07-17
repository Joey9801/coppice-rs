//! Docker's raw container state → the agent's runtime view (docker-executor.md
//! §3). One total mapping table, in one place.
//!
//! Docker's state machine (`created`, `running`, `paused`, `restarting`,
//! `removing`, `exited`, `dead`) is wider than the two states the agent
//! reports (`Running` / `Exited`). This module is the sole translation, total
//! over Docker's enum and defensive over bollard's `EMPTY`/`None`. Pure: no
//! Docker client, no I/O — the §12 principle is that the whole table is
//! unit-tested without a daemon.
//!
//! Two arms deliberately *remove and report nothing* rather than surface a
//! state: `created` (start-sequence debris) and `dead`-without-usable-evidence.
//! Both are safe because the journaled `StartIntent` with no runtime evidence
//! already reports `AgentError` through observed.rs rule 3 — reporting a
//! half-state here would double-count or fabricate an outcome the session
//! cannot classify. The lifecycle layer owns the removal; this function only
//! names the disposition.

use coppice_core::time::{Duration, Timestamp};

use super::classify;
use crate::executor::ContainerState;

/// The disposition of one observed container after applying the §3 table.
///
/// Only [`Mapped::Report`] surfaces to the session; the other three are
/// executor-internal instructions to the lifecycle layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mapped {
    /// Report to the session as running or exited.
    Report(ContainerState),
    /// `created`: start-sequence debris from a crashed start — remove, don't
    /// report (the journaled intent with no runtime evidence already reports
    /// `AgentError` via observed.rs rule 3). The lifecycle layer must NOT
    /// remove a `created` container whose start is still in flight in this
    /// process.
    StartDebris,
    /// `removing`: a reap in flight; terminal evidence was already captured.
    ReapInFlight,
    /// `dead` (or `exited`) with no usable evidence: remove the container and
    /// report nothing — same `AgentError` channel as [`Mapped::StartDebris`].
    DeadUnusable,
}

/// Map one container's `inspect` state onto its disposition (docker-executor.md
/// §3). `now` stamps the running-container runtime (`now − StartedAt`).
///
/// Total over Docker's seven documented states and defensive over bollard's
/// `EMPTY`/`None`.
pub(crate) fn map_container(state: &bollard::models::ContainerState, now: Timestamp) -> Mapped {
    use bollard::models::ContainerStateStatusEnum as S;
    match state.status {
        // Created but never started: a crash inside the start sequence. The
        // intent-without-evidence path already reports AgentError (§3).
        Some(S::CREATED) => Mapped::StartDebris,

        // Running is running. Paused holds resources and we never pause, so we
        // treat it as running (reconcile stops it if unowned, §3). Restarting
        // is unreachable — restart policy is always `no` — but mapped
        // defensively to running rather than dropped.
        Some(S::RUNNING) | Some(S::PAUSED) | Some(S::RESTARTING) => {
            Mapped::Report(ContainerState::Running {
                runtime: running_runtime(state, now),
            })
        }

        // A reap already in flight; terminal evidence was captured before it.
        Some(S::REMOVING) => Mapped::ReapInFlight,

        // Exited: evidence from inspect. exit_info returning None shouldn't
        // happen for a genuinely exited container, so treat it defensively as
        // unusable (same AgentError channel).
        Some(S::EXITED) => match classify::exit_info(state) {
            Some(info) => Mapped::Report(ContainerState::Exited(info)),
            None => Mapped::DeadUnusable,
        },

        // Dead: a daemon-side failure. Usable evidence is reported, but a
        // "clean 0" that comes with a non-empty error field is daemon debris,
        // not a real success — treat that, and any unusable evidence, as
        // DeadUnusable so the intent-without-evidence path reports AgentError.
        Some(S::DEAD) => map_dead(state),

        // EMPTY or absent status: no state to trust (defensive). Remove and
        // report nothing, same as dead-unusable.
        Some(S::EMPTY) | None => Mapped::DeadUnusable,
    }
}

/// `now − StartedAt`, clamped to ≥ 0; an unset `StartedAt` yields zero.
fn running_runtime(state: &bollard::models::ContainerState, now: Timestamp) -> Duration {
    match state
        .started_at
        .as_deref()
        .and_then(classify::parse_docker_time)
    {
        Some(started_at) => (now - started_at).max(Duration::ZERO),
        None => Duration::ZERO,
    }
}

fn map_dead(state: &bollard::models::ContainerState) -> Mapped {
    match classify::exit_info(state) {
        Some(info) if !dead_is_daemon_debris(state, info.code) => {
            Mapped::Report(ContainerState::Exited(info))
        }
        _ => Mapped::DeadUnusable,
    }
}

/// A `dead` container reporting exit code 0 *with* a non-empty `Error` field is
/// daemon failure debris — the "0" is not a genuine success and must not be
/// reported as one (§3).
fn dead_is_daemon_debris(state: &bollard::models::ContainerState, code: i32) -> bool {
    code == 0 && state.error.as_deref().is_some_and(|e| !e.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExitCause;
    use bollard::models::ContainerStateStatusEnum as S;

    fn now() -> Timestamp {
        // A fixed "now" after the timestamps used below.
        classify::parse_docker_time("2026-07-17T10:00:10Z").unwrap()
    }

    /// A minimal exited/dead state carrying usable evidence.
    fn exited_state(code: i64) -> bollard::models::ContainerState {
        bollard::models::ContainerState {
            exit_code: Some(code),
            started_at: Some("2026-07-17T10:00:00Z".into()),
            finished_at: Some("2026-07-17T10:00:05Z".into()),
            ..Default::default()
        }
    }

    fn with_status(
        mut s: bollard::models::ContainerState,
        status: S,
    ) -> bollard::models::ContainerState {
        s.status = Some(status);
        s
    }

    #[test]
    fn created_is_start_debris() {
        let s = with_status(bollard::models::ContainerState::default(), S::CREATED);
        assert_eq!(map_container(&s, now()), Mapped::StartDebris);
    }

    #[test]
    fn running_reports_running_with_clamped_runtime() {
        let s = bollard::models::ContainerState {
            status: Some(S::RUNNING),
            started_at: Some("2026-07-17T10:00:00Z".into()),
            ..Default::default()
        };
        assert_eq!(
            map_container(&s, now()),
            Mapped::Report(ContainerState::Running {
                runtime: Duration::from_secs(10),
            })
        );
    }

    #[test]
    fn running_with_unset_started_is_zero_runtime() {
        let s = bollard::models::ContainerState {
            status: Some(S::RUNNING),
            started_at: Some("0001-01-01T00:00:00Z".into()),
            ..Default::default()
        };
        assert_eq!(
            map_container(&s, now()),
            Mapped::Report(ContainerState::Running {
                runtime: Duration::ZERO,
            })
        );
    }

    #[test]
    fn running_clamps_when_started_after_now() {
        let s = bollard::models::ContainerState {
            status: Some(S::RUNNING),
            started_at: Some("2026-07-17T10:00:30Z".into()),
            ..Default::default()
        };
        assert_eq!(
            map_container(&s, now()),
            Mapped::Report(ContainerState::Running {
                runtime: Duration::ZERO,
            })
        );
    }

    #[test]
    fn paused_reports_running() {
        let s = bollard::models::ContainerState {
            status: Some(S::PAUSED),
            started_at: Some("2026-07-17T10:00:00Z".into()),
            ..Default::default()
        };
        assert_eq!(
            map_container(&s, now()),
            Mapped::Report(ContainerState::Running {
                runtime: Duration::from_secs(10),
            })
        );
    }

    #[test]
    fn restarting_reports_running() {
        let s = bollard::models::ContainerState {
            status: Some(S::RESTARTING),
            started_at: Some("2026-07-17T10:00:00Z".into()),
            ..Default::default()
        };
        assert!(matches!(
            map_container(&s, now()),
            Mapped::Report(ContainerState::Running { .. })
        ));
    }

    #[test]
    fn removing_is_reap_in_flight() {
        let s = with_status(bollard::models::ContainerState::default(), S::REMOVING);
        assert_eq!(map_container(&s, now()), Mapped::ReapInFlight);
    }

    #[test]
    fn exited_reports_exit_info() {
        let s = with_status(exited_state(0), S::EXITED);
        let Mapped::Report(ContainerState::Exited(info)) = map_container(&s, now()) else {
            panic!("expected Exited");
        };
        assert_eq!(info.code, 0);
        assert_eq!(info.cause, ExitCause::Natural);
        assert_eq!(info.runtime, Duration::from_secs(5));
    }

    #[test]
    fn exited_without_usable_evidence_is_dead_unusable() {
        // No finished_at ⇒ no usable evidence even in the `exited` state.
        let s = bollard::models::ContainerState {
            status: Some(S::EXITED),
            exit_code: Some(0),
            finished_at: None,
            ..Default::default()
        };
        assert_eq!(map_container(&s, now()), Mapped::DeadUnusable);
    }

    #[test]
    fn dead_with_usable_evidence_reports_exited() {
        let s = with_status(exited_state(1), S::DEAD);
        let Mapped::Report(ContainerState::Exited(info)) = map_container(&s, now()) else {
            panic!("expected Exited");
        };
        assert_eq!(info.code, 1);
    }

    #[test]
    fn dead_code_zero_with_error_is_dead_unusable() {
        // Daemon failure debris: a "0" that comes with an error field is not a
        // genuine success.
        let s = bollard::models::ContainerState {
            error: Some("cgroup setup failed".into()),
            ..with_status(exited_state(0), S::DEAD)
        };
        assert_eq!(map_container(&s, now()), Mapped::DeadUnusable);
    }

    #[test]
    fn dead_code_zero_with_empty_error_is_reported() {
        // An empty error string is not debris — evidence stands.
        let s = bollard::models::ContainerState {
            error: Some(String::new()),
            ..with_status(exited_state(0), S::DEAD)
        };
        assert!(matches!(
            map_container(&s, now()),
            Mapped::Report(ContainerState::Exited(_))
        ));
    }

    #[test]
    fn dead_nonzero_with_error_is_reported() {
        // A non-zero code with an error is a real (bad) exit, still evidence.
        let s = bollard::models::ContainerState {
            error: Some("oom".into()),
            ..with_status(exited_state(137), S::DEAD)
        };
        assert!(matches!(
            map_container(&s, now()),
            Mapped::Report(ContainerState::Exited(_))
        ));
    }

    #[test]
    fn dead_without_usable_evidence_is_dead_unusable() {
        let s = bollard::models::ContainerState {
            status: Some(S::DEAD),
            exit_code: None,
            ..Default::default()
        };
        assert_eq!(map_container(&s, now()), Mapped::DeadUnusable);
    }

    #[test]
    fn empty_and_none_status_are_dead_unusable() {
        let empty = with_status(bollard::models::ContainerState::default(), S::EMPTY);
        assert_eq!(map_container(&empty, now()), Mapped::DeadUnusable);

        let none = bollard::models::ContainerState::default();
        assert_eq!(map_container(&none, now()), Mapped::DeadUnusable);
    }
}
