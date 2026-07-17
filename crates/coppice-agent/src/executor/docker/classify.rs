//! Exit-evidence extraction and start-error classification
//! (docker-executor.md §4).
//!
//! Pure translation: Docker's `inspect` view and bollard error variants in,
//! the executor's evidence types out. No Docker client, no I/O, no async — the
//! §12 principle is that everything correctness-bearing here is unit-tested
//! without a daemon. Classification proper (evidence → outcome) stays *above*
//! the trait (ADR 0013); this module only produces the evidence.

use chrono::{DateTime, Utc};

use coppice_core::time::{Duration, Timestamp};

use crate::executor::{ExitCause, ExitInfo, StartError};

/// Parse a Docker `inspect` timestamp (RFC3339 with nanosecond precision) into
/// a µs-quantised [`Timestamp`].
///
/// Returns `None` for the empty string, an unparseable value, or Docker's
/// "unset" sentinel `0001-01-01T00:00:00Z`. The sentinel rule is generalised:
/// anything strictly before the Unix epoch is treated as unset, because no
/// real container ran before 1970 — a `StartedAt`/`FinishedAt` that predates
/// the epoch is Docker saying "this never happened", not runtime evidence.
pub(crate) fn parse_docker_time(s: &str) -> Option<Timestamp> {
    if s.is_empty() {
        return None;
    }
    let datetime = DateTime::parse_from_rfc3339(s).ok()?;
    let timestamp = Timestamp::from_datetime(datetime.with_timezone(&Utc));
    // The zero sentinel (0001-01-01T00:00:00Z) and any pre-epoch instant mean
    // "unset" — never usable evidence.
    if timestamp < Timestamp::UNIX_EPOCH {
        return None;
    }
    Some(timestamp)
}

/// Extract exit evidence from the `inspect` state of an exited/dead container
/// (docker-executor.md §4).
///
/// Returns `None` when there is no usable evidence — no exit code, or no
/// parseable `FinishedAt` — so the caller can route the allocation onto the
/// same `AgentError` channel a lost intent uses (observed.rs rule 3, via
/// [`super::state::Mapped::DeadUnusable`]).
///
/// - `code`: `ExitCode` narrowed `i64 → i32`, saturating.
/// - `cause`: [`ExitCause::OomKilled`] iff `OOMKilled == Some(true)`, else
///   [`ExitCause::Natural`]. [`ExitCause::DiskKilled`] is produced only by the
///   S4 disk enforcer — never synthesised from inspect here.
/// - `finished_at`: `parse_docker_time(FinishedAt)`; an unusable value yields
///   `None` (no evidence).
/// - `runtime`: `FinishedAt − StartedAt`, clamped to ≥ 0; an unset `StartedAt`
///   yields a zero runtime rather than a bogus span from the epoch.
pub(crate) fn exit_info(state: &bollard::models::ContainerState) -> Option<ExitInfo> {
    // No exit code ⇒ no usable evidence.
    let code_i64 = state.exit_code?;
    let code = code_i64.clamp(i32::MIN as i64, i32::MAX as i64) as i32;

    let cause = if state.oom_killed == Some(true) {
        ExitCause::OomKilled
    } else {
        ExitCause::Natural
    };

    // An unusable FinishedAt means no evidence to report at all.
    let finished_at = parse_docker_time(state.finished_at.as_deref()?)?;

    // runtime = finished_at − started_at, clamped ≥ 0; unset started_at ⇒ zero.
    let runtime = match state.started_at.as_deref().and_then(parse_docker_time) {
        Some(started_at) => (finished_at - started_at).max(Duration::ZERO),
        None => Duration::ZERO,
    };

    Some(ExitInfo {
        code,
        cause,
        runtime,
        finished_at,
    })
}

/// Classify a failure from the image-pull phase (docker-executor.md §4 table).
///
/// Always the [`StartError::Pull`] variant; the message names the image
/// reference and the underlying error. `user_error` is `true` for a bad image
/// reference the coordinator should not retry (manifest unknown, unauthorized,
/// bad reference syntax) and `false` for registry/platform trouble that a
/// retry might clear (5xx, transport/hyper errors, timeouts, local IO).
pub(crate) fn classify_pull_error(err: &bollard::errors::Error, image: &str) -> StartError {
    // 404 manifest unknown, 401/403 unauthorized, 400 bad reference syntax.
    let status_is_user = matches!(
        err,
        bollard::errors::Error::DockerResponseServerError { status_code, .. }
            if matches!(status_code, 400 | 401 | 403 | 404)
    );
    // A malformed reference can also surface through non-server-error variants
    // (client-side rejection); catch it by message regardless of shape.
    let user_error = status_is_user
        || err
            .to_string()
            .to_ascii_lowercase()
            .contains("invalid reference format");

    StartError::Pull {
        user_error,
        message: format!("pulling image {image}: {err}"),
    }
}

/// Classify a failure from the container create/start phase
/// (docker-executor.md §4 table).
///
/// Always the [`StartError::Start`] variant. `user_error` is `true` only for a
/// daemon 400 whose message names a workload fault (a bad command/entrypoint:
/// missing executable, wrong architecture, empty command, otherwise invalid);
/// everything else is platform (`false`), including daemon errors and cgroup
/// failures. A 409 name conflict is resolved by the lifecycle layer *before*
/// classification (adopt-on-conflict, §5); one reaching here anyway is a
/// conflict "we can't resolve" and maps platform (§4).
pub(crate) fn classify_start_error(err: &bollard::errors::Error) -> StartError {
    let user_error = match err {
        bollard::errors::Error::DockerResponseServerError {
            status_code: 400,
            message,
        } => {
            let m = message.to_ascii_lowercase();
            [
                "executable file not found",
                "exec format error",
                "no command specified",
                "invalid",
            ]
            .iter()
            .any(|needle| m.contains(needle))
        }
        _ => false,
    };

    StartError::Start {
        user_error,
        message: format!("starting container: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_err(status_code: u16, message: &str) -> bollard::errors::Error {
        bollard::errors::Error::DockerResponseServerError {
            status_code,
            message: message.to_string(),
        }
    }

    /// A non-server-error, transport-ish variant (registry/platform trouble).
    fn transport_err() -> bollard::errors::Error {
        bollard::errors::Error::IOError {
            err: std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
        }
    }

    fn state() -> bollard::models::ContainerState {
        bollard::models::ContainerState::default()
    }

    // ---- parse_docker_time -------------------------------------------------

    #[test]
    fn parse_time_truncates_nanos_to_micros() {
        // 1.5 µs past a whole second truncates down to 1 µs (floor, per Timestamp).
        let ts = parse_docker_time("2026-07-17T10:00:00.000001500Z").expect("parses");
        assert_eq!(ts.to_rfc3339(), "2026-07-17T10:00:00.000001Z");
    }

    #[test]
    fn parse_time_accepts_offset_and_normalises_to_utc() {
        let ts = parse_docker_time("2026-07-17T11:00:00+01:00").expect("parses");
        assert_eq!(ts.to_rfc3339(), "2026-07-17T10:00:00.000000Z");
    }

    #[test]
    fn parse_time_rejects_zero_sentinel_empty_and_garbage() {
        assert_eq!(parse_docker_time("0001-01-01T00:00:00Z"), None);
        assert_eq!(parse_docker_time(""), None);
        assert_eq!(parse_docker_time("not a timestamp"), None);
        // Any pre-epoch instant is "unset".
        assert_eq!(parse_docker_time("1969-12-31T23:59:59Z"), None);
    }

    // ---- exit_info ---------------------------------------------------------

    #[test]
    fn exit_info_natural_exit() {
        let s = bollard::models::ContainerState {
            exit_code: Some(0),
            oom_killed: Some(false),
            started_at: Some("2026-07-17T10:00:00Z".into()),
            finished_at: Some("2026-07-17T10:00:05Z".into()),
            ..state()
        };
        let info = exit_info(&s).expect("usable evidence");
        assert_eq!(info.code, 0);
        assert_eq!(info.cause, ExitCause::Natural);
        assert_eq!(info.runtime, Duration::from_secs(5));
        assert_eq!(
            info.finished_at,
            parse_docker_time("2026-07-17T10:00:05Z").unwrap()
        );
    }

    #[test]
    fn exit_info_oom() {
        let s = bollard::models::ContainerState {
            exit_code: Some(137),
            oom_killed: Some(true),
            started_at: Some("2026-07-17T10:00:00Z".into()),
            finished_at: Some("2026-07-17T10:00:01Z".into()),
            ..state()
        };
        let info = exit_info(&s).expect("usable evidence");
        assert_eq!(info.code, 137);
        assert_eq!(info.cause, ExitCause::OomKilled);
    }

    #[test]
    fn exit_info_none_when_finished_at_missing_or_unset() {
        let missing = bollard::models::ContainerState {
            exit_code: Some(0),
            finished_at: None,
            ..state()
        };
        assert_eq!(exit_info(&missing), None);

        let sentinel = bollard::models::ContainerState {
            exit_code: Some(0),
            finished_at: Some("0001-01-01T00:00:00Z".into()),
            ..state()
        };
        assert_eq!(exit_info(&sentinel), None);
    }

    #[test]
    fn exit_info_none_when_code_missing() {
        let s = bollard::models::ContainerState {
            exit_code: None,
            finished_at: Some("2026-07-17T10:00:05Z".into()),
            ..state()
        };
        assert_eq!(exit_info(&s), None);
    }

    #[test]
    fn exit_info_runtime_clamped_when_finished_before_started() {
        let s = bollard::models::ContainerState {
            exit_code: Some(1),
            started_at: Some("2026-07-17T10:00:05Z".into()),
            finished_at: Some("2026-07-17T10:00:00Z".into()),
            ..state()
        };
        let info = exit_info(&s).expect("usable evidence");
        assert_eq!(info.runtime, Duration::ZERO);
    }

    #[test]
    fn exit_info_zero_runtime_when_started_unset() {
        let s = bollard::models::ContainerState {
            exit_code: Some(2),
            started_at: Some("0001-01-01T00:00:00Z".into()),
            finished_at: Some("2026-07-17T10:00:05Z".into()),
            ..state()
        };
        let info = exit_info(&s).expect("usable evidence");
        assert_eq!(info.runtime, Duration::ZERO);
    }

    #[test]
    fn exit_info_saturates_out_of_range_code() {
        let s = bollard::models::ContainerState {
            exit_code: Some(i64::MAX),
            finished_at: Some("2026-07-17T10:00:05Z".into()),
            ..state()
        };
        assert_eq!(exit_info(&s).expect("usable").code, i32::MAX);
    }

    // ---- classify_pull_error (§4 table) ------------------------------------

    #[test]
    fn pull_user_errors() {
        for status in [404, 401, 403, 400] {
            let e = classify_pull_error(&server_err(status, "boom"), "img:tag");
            assert!(
                matches!(
                    e,
                    StartError::Pull {
                        user_error: true,
                        ..
                    }
                ),
                "status {status} should be a pull user error"
            );
        }
    }

    #[test]
    fn pull_bad_reference_by_message() {
        // A malformed reference surfaced through a non-server-error variant.
        let e = classify_pull_error(
            &bollard::errors::Error::DockerStreamError {
                error: "invalid reference format".into(),
            },
            "BAD IMAGE",
        );
        assert!(matches!(
            e,
            StartError::Pull {
                user_error: true,
                ..
            }
        ));
    }

    #[test]
    fn pull_platform_errors() {
        for status in [500, 502, 503, 429] {
            let e = classify_pull_error(&server_err(status, "registry down"), "img:tag");
            assert!(
                matches!(
                    e,
                    StartError::Pull {
                        user_error: false,
                        ..
                    }
                ),
                "status {status} should be a pull platform error"
            );
        }
        let e = classify_pull_error(&transport_err(), "img:tag");
        assert!(matches!(
            e,
            StartError::Pull {
                user_error: false,
                ..
            }
        ));
    }

    #[test]
    fn pull_message_names_image() {
        let e = classify_pull_error(&server_err(404, "manifest unknown"), "ghcr.io/x/y:1");
        let StartError::Pull { message, .. } = e else {
            panic!("expected Pull");
        };
        assert!(message.contains("ghcr.io/x/y:1"));
    }

    // ---- classify_start_error (§4 table) -----------------------------------

    #[test]
    fn start_user_errors_on_workload_fault_400s() {
        for msg in [
            "OCI runtime create failed: executable file not found in $PATH",
            "exec format error",
            "no command specified",
            "invalid mount config",
        ] {
            let e = classify_start_error(&server_err(400, msg));
            assert!(
                matches!(
                    e,
                    StartError::Start {
                        user_error: true,
                        ..
                    }
                ),
                "{msg:?} should be a start user error"
            );
        }
    }

    #[test]
    fn start_platform_errors() {
        // A 400 with no workload-fault marker is still platform.
        let e = classify_start_error(&server_err(400, "cgroup: something the daemon botched"));
        assert!(matches!(
            e,
            StartError::Start {
                user_error: false,
                ..
            }
        ));

        // Non-400 statuses, incl. an unresolved 409 name conflict, are platform.
        for status in [409, 500, 503] {
            let e = classify_start_error(&server_err(status, "conflict / daemon error"));
            assert!(
                matches!(
                    e,
                    StartError::Start {
                        user_error: false,
                        ..
                    }
                ),
                "status {status} should be a start platform error"
            );
        }

        // Transport-ish errors are platform.
        let e = classify_start_error(&transport_err());
        assert!(matches!(
            e,
            StartError::Start {
                user_error: false,
                ..
            }
        ));
    }
}
