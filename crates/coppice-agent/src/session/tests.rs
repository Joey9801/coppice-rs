//! Pre-transport unit tests for the session core: fencing acceptance, seq
//! dedup, StartJob idempotency + the tombstone rule, truth-wins-the-race, and
//! outcome classification. No live server — every method under test is a plain
//! `async fn` over the journal and the fake executor.

use coppice_consensus::fs::RealFs;
use coppice_core::attempt::{AttemptOutcome, AttemptState};
use coppice_core::id::{AllocationId, AttemptId, JobId, NodeId};
use coppice_core::resource::Resources;
use coppice_core::time::Duration;
use coppice_proto::pb::agent::v1 as pb;

use coppice_core::time::Timestamp;

use crate::executor::{Executor, ExitCause, ExitInfo, FakeExecutor, StartError};
use crate::journal::Journal;
use crate::session::ArmedWatchdog;

use super::Session;

/// Whether the fake executor still shows a container for `alloc` (running or
/// exited) — the witness that a reap did or didn't happen.
async fn observes(exec: &FakeExecutor, alloc: AllocationId) -> bool {
    exec.observe()
        .await
        .unwrap()
        .iter()
        .any(|c| c.allocation == alloc)
}

/// A natural exit stamped at the fake executor's current clock — the value the
/// stop path would synthesize, so tests and the fake agree on `finished_at`.
fn natural_exit(code: i32, runtime: Duration, at: Timestamp) -> ExitInfo {
    ExitInfo {
        code,
        cause: ExitCause::Natural,
        runtime,
        finished_at: at,
    }
}

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
        command: vec!["run".into()],
        entrypoint: None,
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
    let exit = natural_exit(0, Duration::from_micros(9), exec.now());
    exec.finish(alloc, exit);
    let exit_reports = session.handle_observed_exit(alloc, exit).await.unwrap();
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
                cause: ExitCause::OomKilled,
                runtime: Duration::from_micros(1),
                finished_at: Timestamp::UNIX_EPOCH,
            },
            AttemptOutcome::MemoryLimitExceeded,
        ),
        (
            ExitInfo {
                code: 3,
                cause: ExitCause::Natural,
                runtime: Duration::from_micros(1),
                finished_at: Timestamp::UNIX_EPOCH,
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
        AttemptOutcome::MemoryLimitExceeded => 1,
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

/// The §4 carve-out (docker-executor.md): a stop whose evidence shows a limit
/// kill landed as it took effect must record the limit breach, never claim the
/// stop (abort / max-runtime) terminated the container.
#[tokio::test]
async fn stop_racing_a_limit_kill_records_the_breach() {
    let (_dir, mut session, exec) = session();
    register(&mut session, 1, 1, 1).await;
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());
    session
        .handle_command(command(1, 1, 2, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    exec.plan_stop_cause(alloc, ExitCause::OomKilled);

    let reports = session
        .handle_command(command(1, 1, 3, stop_job(alloc)))
        .await
        .unwrap();
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::MemoryLimitExceeded)
    );
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
    assert_eq!(
        session.take_armed_watchdogs(),
        vec![ArmedWatchdog {
            allocation: alloc,
            max_runtime: Duration::from_micros(1_000)
        }]
    );

    let reports = session.trigger_max_runtime(alloc).await.unwrap();
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::RuntimeLimitExceeded)
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
    let exit = natural_exit(0, Duration::from_micros(5), exec.now());
    exec.finish(alloc, exit);
    session.handle_observed_exit(alloc, exit).await.unwrap();

    // The container already exited: the watchdog must not fabricate an outcome.
    let reports = session.trigger_max_runtime(alloc).await.unwrap();
    assert!(reports.is_empty());
}

// ---- reaping (docker-executor.md §5) ----

#[tokio::test]
async fn exit_path_journals_then_reaps() {
    let (_dir, mut session, exec) = session();
    register(&mut session, 1, 1, 1).await;

    // Natural-exit path: start, observe the exit → journaled and reaped.
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());
    session
        .handle_command(command(1, 1, 2, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    let exit = natural_exit(0, Duration::from_micros(9), exec.now());
    exec.finish(alloc, exit);
    session.handle_observed_exit(alloc, exit).await.unwrap();
    assert!(
        session.state().exits.contains_key(&alloc),
        "the observed exit is journaled"
    );
    assert!(
        !observes(&exec, alloc).await,
        "a journaled natural exit is reaped from the runtime"
    );

    // Stop path: a container that already exited, resolved through StopJob, is
    // journaled (truth-wins) and likewise reaped.
    let (alloc2, attempt2, job2) = (AllocationId::new(), AttemptId::new(), JobId::new());
    session
        .handle_command(command(1, 1, 3, start_job(alloc2, attempt2, job2, None)))
        .await
        .unwrap();
    exec.finish(
        alloc2,
        natural_exit(0, Duration::from_micros(4), exec.now()),
    );
    session
        .handle_command(command(1, 1, 4, stop_job(alloc2)))
        .await
        .unwrap();
    assert!(
        session.state().exits.contains_key(&alloc2),
        "the stop-resolved exit is journaled"
    );
    assert!(
        !observes(&exec, alloc2).await,
        "a journaled stop-resolved exit is reaped from the runtime"
    );
}

/// Start `alloc` (journaling intent), finish it in the fake with a controlled
/// `finished_at`, and optionally journal the exit *without reaping* (via the
/// private `record_exit`, accessible from this child module) — modelling a reap
/// lost to a crash, so the container stays visible in the runtime.
async fn arrange_exited(
    session: &mut TestSession,
    exec: &FakeExecutor,
    seq: u64,
    finished_at: Timestamp,
    journaled: bool,
) -> AllocationId {
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());
    session
        .handle_command(command(1, 1, seq, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    exec.finish(
        alloc,
        ExitInfo {
            code: 0,
            cause: ExitCause::Natural,
            runtime: Duration::from_micros(1),
            finished_at,
        },
    );
    if journaled {
        session
            .record_exit(
                alloc,
                attempt,
                job,
                AttemptOutcome::Exited { code: 0 },
                Duration::ZERO,
            )
            .unwrap();
    }
    alloc
}

#[tokio::test]
async fn janitor_sweeps_only_old_journaled_exits() {
    let (_dir, mut session, exec) = session();
    register(&mut session, 1, 1, 1).await;

    let bound = Duration::from_secs(100);
    let now = Timestamp::UNIX_EPOCH + Duration::from_secs(10_000);

    // (a) journaled + older than the bound → swept.
    let old = arrange_exited(&mut session, &exec, 2, now - Duration::from_secs(200), true).await;
    // (b) journaled + younger than the bound → kept.
    let young = arrange_exited(&mut session, &exec, 3, now - Duration::from_secs(50), true).await;
    // (c) exited but not journaled → kept (evidence still needed).
    let unjournaled = arrange_exited(
        &mut session,
        &exec,
        4,
        now - Duration::from_secs(200),
        false,
    )
    .await;

    session.janitor_sweep(now, bound).await.unwrap();

    assert!(
        !observes(&exec, old).await,
        "an old journaled exit is swept"
    );
    assert!(
        observes(&exec, young).await,
        "a young journaled exit is kept"
    );
    assert!(
        observes(&exec, unjournaled).await,
        "an unjournaled exit is kept even when old"
    );
}

#[tokio::test]
async fn recovery_reaps_journaled_survivors() {
    // Session 1: start, observe the exit *durably journaled* but crash before
    // reaping — the container survives in the runtime.
    let dir = tempfile::tempdir().unwrap();
    let (journal, state) = Journal::open(RealFs::new(dir.path())).unwrap();
    let exec = FakeExecutor::new();
    let mut s1 = Session::new(
        NodeId::new(),
        Resources::ZERO,
        Vec::new(),
        journal,
        state,
        exec.clone(),
    );
    register(&mut s1, 1, 1, 1).await;
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());
    s1.handle_command(command(1, 1, 2, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    exec.finish(alloc, natural_exit(0, Duration::from_micros(7), exec.now()));
    // Journal the exit WITHOUT reaping (models a crash between the two).
    s1.record_exit(
        alloc,
        attempt,
        job,
        AttemptOutcome::Exited { code: 0 },
        Duration::ZERO,
    )
    .unwrap();
    assert!(
        observes(&exec, alloc).await,
        "the container survives the crash"
    );

    // Session 2: fork the executor (the container is still visible), then drop
    // session 1 to release its journal LOCK and reopen the journal (recovering
    // the exit). Registration reaps the journaled survivor.
    let forked = exec.fork();
    drop(s1);
    let (journal2, state2) = Journal::open(RealFs::new(dir.path())).unwrap();
    assert!(
        state2.exits.contains_key(&alloc),
        "the exit was recovered from the journal"
    );
    let mut s2 = Session::new(
        NodeId::new(),
        Resources::ZERO,
        Vec::new(),
        journal2,
        state2,
        forked.clone(),
    );
    register(&mut s2, 1, 1, 1).await;
    assert!(
        !observes(&forked, alloc).await,
        "recovery reaps a container whose exit is already journaled"
    );
}

/// Crash window between a stop's journaled `Aborted` outcome and its reap: on
/// recovery the surviving container's bare 137 must not reclassify the attempt
/// as `Exited { code: 137 }` — the ObservedSet reports the journaled outcome.
#[tokio::test]
async fn recovery_reports_journaled_stop_outcome_over_runtime_code() {
    let dir = tempfile::tempdir().unwrap();
    let (journal, state) = Journal::open(RealFs::new(dir.path())).unwrap();
    let exec = FakeExecutor::new();
    let mut s1 = Session::new(
        NodeId::new(),
        Resources::ZERO,
        Vec::new(),
        journal,
        state,
        exec.clone(),
    );
    register(&mut s1, 1, 1, 1).await;
    let (alloc, attempt, job) = (AllocationId::new(), AttemptId::new(), JobId::new());
    s1.handle_command(command(1, 1, 2, start_job(alloc, attempt, job, None)))
        .await
        .unwrap();
    // The stop's SIGKILL surfaces as a bare 137 in the runtime…
    exec.finish(
        alloc,
        natural_exit(137, Duration::from_micros(7), exec.now()),
    );
    // …and the session journals the kill attribution, then crashes before reap.
    s1.record_exit(alloc, attempt, job, AttemptOutcome::Aborted, Duration::ZERO)
        .unwrap();

    let forked = exec.fork();
    drop(s1);
    let (journal2, state2) = Journal::open(RealFs::new(dir.path())).unwrap();
    let mut s2 = Session::new(
        NodeId::new(),
        Resources::ZERO,
        Vec::new(),
        journal2,
        state2,
        forked.clone(),
    );
    let reports = s2
        .handle_command(command(
            1,
            1,
            1,
            pb::agent_command::Body::RegisterAccepted(pb::RegisterAccepted {}),
        ))
        .await
        .unwrap();
    let Some(pb::agent_report::Body::ObservedSet(set)) =
        reports.first().and_then(|r| r.body.clone())
    else {
        panic!("registration must report an ObservedSet");
    };
    let entry = set
        .allocations
        .iter()
        .find(|a| a.allocation == Some(alloc.into()))
        .expect("the surviving allocation is reported");
    assert!(!entry.running);
    let outcome = AttemptOutcome::try_from(entry.outcome.unwrap()).unwrap();
    assert_eq!(
        outcome,
        AttemptOutcome::Aborted,
        "the journaled stop outcome wins over the runtime's bare exit code"
    );
    assert!(
        !observes(&forked, alloc).await,
        "recovery still reaps the journaled survivor"
    );
}
