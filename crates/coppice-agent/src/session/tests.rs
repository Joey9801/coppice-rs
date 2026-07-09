//! Pre-transport unit tests for the session core: fencing acceptance, seq
//! dedup, StartJob idempotency + the tombstone rule, truth-wins-the-race, and
//! outcome classification. No live server — every method under test is a plain
//! `async fn` over the journal and the fake executor.

use coppice_consensus::fs::RealFs;
use coppice_core::attempt::{AttemptOutcome, AttemptState};
use coppice_core::id::{AllocationId, AttemptId, JobId, NodeId};
use coppice_core::resource::Resources;
use coppice_proto::pb::agent::v1 as pb;

use crate::executor::{ExitInfo, FakeExecutor, StartError};
use crate::journal::Journal;

use super::Session;

type TestSession = Session<RealFs, FakeExecutor>;

fn session() -> (tempfile::TempDir, TestSession, FakeExecutor) {
    let dir = tempfile::tempdir().unwrap();
    let (journal, state) = Journal::open(RealFs::new(dir.path())).unwrap();
    let exec = FakeExecutor::new();
    let session = Session::new(
        NodeId::new(),
        Resources::ZERO,
        Vec::new(),
        journal,
        state,
        exec.clone(),
    );
    (dir, session, exec)
}

fn header(term: u64, epoch: u64, seq: u64) -> pb::CommandHeader {
    pb::CommandHeader {
        token: Some(pb::FencingToken {
            leader_term: term,
            node_epoch: epoch,
        }),
        command_seq: seq,
    }
}

fn command(term: u64, epoch: u64, seq: u64, body: pb::agent_command::Body) -> pb::AgentCommand {
    pb::AgentCommand {
        header: Some(header(term, epoch, seq)),
        body: Some(body),
    }
}

fn start_job(
    alloc: AllocationId,
    attempt: AttemptId,
    job: JobId,
    max_runtime_us: Option<u64>,
) -> pb::agent_command::Body {
    pb::agent_command::Body::StartJob(pb::StartJob {
        allocation: Some(alloc.into()),
        attempt: Some(attempt.into()),
        job: Some(job.into()),
        image: "img".into(),
        limits: None,
        max_runtime_us,
    })
}

fn stop_job(alloc: AllocationId) -> pb::agent_command::Body {
    pb::agent_command::Body::StopJob(pb::StopJob {
        allocation: Some(alloc.into()),
        grace_us: 0,
    })
}

/// Extract the single AttemptStatus outcome from a report list (terminal) or
/// `None` if the observed state is non-terminal.
fn terminal_outcome(reports: &[pb::AgentReport]) -> Option<AttemptOutcome> {
    let pb::agent_report::Body::AttemptStatus(status) = reports.first()?.body.as_ref()? else {
        return None;
    };
    let state: AttemptState = status.observed?.try_into().ok()?;
    match state {
        AttemptState::Terminal(o) => Some(o),
        _ => None,
    }
}

fn is_running_report(reports: &[pb::AgentReport]) -> bool {
    let Some(pb::agent_report::Body::AttemptStatus(status)) =
        reports.first().and_then(|r| r.body.as_ref())
    else {
        return false;
    };
    let Ok(state): Result<AttemptState, _> = status.observed.unwrap().try_into() else {
        return false;
    };
    matches!(state, AttemptState::Running)
}

async fn register(session: &mut TestSession, term: u64, epoch: u64, seq: u64) {
    let reports = session
        .handle_command(command(
            term,
            epoch,
            seq,
            pb::agent_command::Body::RegisterAccepted(pb::RegisterAccepted {}),
        ))
        .await
        .unwrap();
    assert!(session.is_registered());
    assert_eq!(session.epoch(), epoch);
    assert!(matches!(
        reports.first().and_then(|r| r.body.as_ref()),
        Some(pb::agent_report::Body::ObservedSet(_))
    ));
}

#[tokio::test]
async fn command_before_registration_is_dropped() {
    let (_dir, mut session, _exec) = session();
    let alloc = AllocationId::new();
    // No RegisterAccepted yet: a StartJob is dropped (fail closed).
    let reports = session
        .handle_command(command(
            1,
            1,
            1,
            start_job(alloc, AttemptId::new(), JobId::new(), None),
        ))
        .await
        .unwrap();
    assert!(reports.is_empty());
    assert!(session.state().intents.is_empty());
}

#[tokio::test]
async fn fencing_rejects_lower_term_and_epoch() {
    let (_dir, mut session, _exec) = session();
    register(&mut session, 5, 3, 1).await;

    // Lower term → rejected: drain flag stays false.
    session
        .handle_command(command(
            4,
            3,
            2,
            pb::agent_command::Body::Drain(pb::Drain {}),
        ))
        .await
        .unwrap();
    assert!(
        !session.is_drained(),
        "a lower-term command must be rejected"
    );

    // Lower epoch → rejected.
    session
        .handle_command(command(
            5,
            2,
            2,
            pb::agent_command::Body::Drain(pb::Drain {}),
        ))
        .await
        .unwrap();
    assert!(
        !session.is_drained(),
        "a lower-epoch command must be rejected"
    );

    // Equal token, fresh seq → accepted.
    session
        .handle_command(command(
            5,
            3,
            2,
            pb::agent_command::Body::Drain(pb::Drain {}),
        ))
        .await
        .unwrap();
    assert!(session.is_drained(), "an in-band command must be accepted");
}

#[tokio::test]
async fn raising_token_resets_the_sequence_space() {
    let (_dir, mut session, _exec) = session();
    register(&mut session, 1, 1, 5).await; // last_seq = 5

    let alloc = AllocationId::new();
    // A StopJob at a seq below the watermark is a duplicate (no tombstone).
    session
        .handle_command(command(1, 1, 3, stop_job(alloc)))
        .await
        .unwrap();
    assert!(
        !session.state().tombstones.contains(&alloc),
        "seq 3 <= 5 must be a duplicate"
    );

    // Raising the epoch resets the seq space, so seq 1 is fresh again.
    session
        .handle_command(command(1, 2, 1, stop_job(alloc)))
        .await
        .unwrap();
    assert_eq!(session.epoch(), 2);
    assert!(
        session.state().tombstones.contains(&alloc),
        "a raising token must reset the seq space"
    );
}

#[tokio::test]
async fn start_job_is_idempotent_and_dedups() {
    let (_dir, mut session, exec) = session();
    register(&mut session, 1, 1, 1).await;
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());

    // Fresh start.
    let first = session
        .handle_command(command(1, 1, 2, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    assert!(is_running_report(&first));
    assert!(exec.is_running(alloc));

    // Same seq → duplicate: re-reports running, does not re-execute.
    let dup = session
        .handle_command(command(1, 1, 2, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    assert!(is_running_report(&dup));

    // A fresh seq for the same allocation → idempotent: still no re-execute.
    let again = session
        .handle_command(command(1, 1, 3, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    assert!(is_running_report(&again));
    assert_eq!(
        session.state().intents.len(),
        1,
        "the allocation was journaled exactly once"
    );
}

#[tokio::test]
async fn tombstone_refuses_a_later_start() {
    let (_dir, mut session, _exec) = session();
    register(&mut session, 1, 1, 1).await;
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());

    session
        .handle_command(command(1, 1, 2, stop_job(alloc)))
        .await
        .unwrap();
    let reports = session
        .handle_command(command(1, 1, 3, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    assert_eq!(terminal_outcome(&reports), Some(AttemptOutcome::Aborted));
}

#[tokio::test]
async fn truth_wins_journaled_exit_beats_tombstone_abort() {
    let (_dir, mut session, exec) = session();
    register(&mut session, 1, 1, 1).await;
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());

    // Start, then a natural exit is observed and journaled.
    session
        .handle_command(command(1, 1, 2, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    exec.finish(
        alloc,
        ExitInfo {
            code: 0,
            oom_killed: false,
            runtime_us: 9,
        },
    );
    let exit_reports = session
        .handle_observed_exit(
            alloc,
            ExitInfo {
                code: 0,
                oom_killed: false,
                runtime_us: 9,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        terminal_outcome(&exit_reports),
        Some(AttemptOutcome::Exited { code: 0 })
    );

    // Now a StopJob arrives: the tombstone is journaled, but the honest exit
    // wins over an abort.
    let stop_reports = session
        .handle_command(command(1, 1, 3, stop_job(alloc)))
        .await
        .unwrap();
    assert_eq!(
        terminal_outcome(&stop_reports),
        Some(AttemptOutcome::Exited { code: 0 })
    );

    // And a racing StartJob after the tombstone reports the honest outcome too.
    let start_reports = session
        .handle_command(command(1, 1, 4, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    assert_eq!(
        terminal_outcome(&start_reports),
        Some(AttemptOutcome::Exited { code: 0 })
    );
}

#[tokio::test]
async fn start_failure_is_classified_and_journaled() {
    let (_dir, mut session, exec) = session();
    register(&mut session, 1, 1, 1).await;
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());

    exec.fail_next_start(
        alloc,
        StartError::Pull {
            user_error: true,
            message: "bad ref".into(),
        },
    );
    let reports = session
        .handle_command(command(1, 1, 2, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::PullFailed { user_error: true })
    );
    assert!(
        session.state().exits.contains_key(&alloc),
        "the classified failure is journaled"
    );
}

#[tokio::test]
async fn observed_exit_classifies_oom_and_nonzero() {
    let (_dir, mut session, _exec) = session();
    register(&mut session, 1, 1, 1).await;

    for (exit, expected) in [
        (
            ExitInfo {
                code: 137,
                oom_killed: true,
                runtime_us: 1,
            },
            AttemptOutcome::OomKilled,
        ),
        (
            ExitInfo {
                code: 3,
                oom_killed: false,
                runtime_us: 1,
            },
            AttemptOutcome::Exited { code: 3 },
        ),
    ] {
        let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());
        let seq = 100 + expected_seq(&expected);
        session
            .handle_command(command(1, 1, seq, start_job(alloc, attempt, job, None)))
            .await
            .unwrap();
        let reports = session.handle_observed_exit(alloc, exit).await.unwrap();
        assert_eq!(terminal_outcome(&reports), Some(expected));
    }
}

fn expected_seq(o: &AttemptOutcome) -> u64 {
    match o {
        AttemptOutcome::OomKilled => 1,
        _ => 2,
    }
}

#[tokio::test]
async fn stop_of_running_container_is_aborted() {
    let (_dir, mut session, _exec) = session();
    register(&mut session, 1, 1, 1).await;
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());
    session
        .handle_command(command(1, 1, 2, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();

    let reports = session
        .handle_command(command(1, 1, 3, stop_job(alloc)))
        .await
        .unwrap();
    assert_eq!(terminal_outcome(&reports), Some(AttemptOutcome::Aborted));
}

#[tokio::test]
async fn max_runtime_watchdog_classifies_exceeded() {
    let (_dir, mut session, _exec) = session();
    register(&mut session, 1, 1, 1).await;
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());
    session
        .handle_command(command(
            1,
            1,
            2,
            start_job(alloc, attempt, job, Some(1_000)),
        ))
        .await
        .unwrap();
    assert_eq!(session.take_armed_watchdogs(), vec![(alloc, 1_000)]);

    let reports = session.trigger_max_runtime(alloc).await.unwrap();
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::MaxRuntimeExceeded)
    );
}

#[tokio::test]
async fn max_runtime_after_natural_exit_is_a_noop() {
    let (_dir, mut session, exec) = session();
    register(&mut session, 1, 1, 1).await;
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());
    session
        .handle_command(command(
            1,
            1,
            2,
            start_job(alloc, attempt, job, Some(1_000)),
        ))
        .await
        .unwrap();
    exec.finish(
        alloc,
        ExitInfo {
            code: 0,
            oom_killed: false,
            runtime_us: 5,
        },
    );
    session
        .handle_observed_exit(
            alloc,
            ExitInfo {
                code: 0,
                oom_killed: false,
                runtime_us: 5,
            },
        )
        .await
        .unwrap();

    // The container already exited: the watchdog must not fabricate an outcome.
    let reports = session.trigger_max_runtime(alloc).await.unwrap();
    assert!(reports.is_empty());
}
