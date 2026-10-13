//! The termination mechanism (ADR 0044 §3) and the interruption plan (§5):
//! plan execution, classification, recovery, replacement marks, and StartJob
//! under a plan. Plans are injected directly through
//! [`Session::on_interruption`] — the interruption source is the live loop's.

use coppice_core::attempt::{AttemptOutcome, PreemptReason};
use coppice_core::id::{AllocationId, AttemptId, JobId};
use coppice_core::preemption::NoticeSignal;
use coppice_core::time::{Duration, Timestamp};
use coppice_proto::pb::agent::v1 as pb;
use coppice_proto::pb::core::v1 as pbcore;

use super::{command, register, restarted, session, terminal_outcome, TestSession};
use crate::executor::{Executor, ExitCause, ExitInfo, FakeExecutor};
use crate::session::DRAIN_CLEANUP_MARGIN;

const NOTICE: Duration = Duration::from_secs(60);
const GRACE: Duration = Duration::from_secs(10);
const INTERRUPTED: PreemptReason = PreemptReason::Interruption;
const PREEMPTED: AttemptOutcome = AttemptOutcome::Preempted {
    reason: PreemptReason::Interruption,
};

/// A StartJob declaring a SIGUSR1 notice of [`NOTICE`] and the stop grace
/// [`GRACE`], started and running.
async fn start_noticed(session: &mut TestSession, seq: u64) -> (AllocationId, AttemptId, JobId) {
    let ids = (AllocationId::new(), AttemptId::new(), JobId::new());
    let reports = session
        .handle_command_settled(command(1, 1, seq, start_body(ids, None)))
        .await
        .unwrap();
    assert!(super::is_running_report(&reports));
    ids
}

fn start_body(
    (alloc, attempt, job): (AllocationId, AttemptId, JobId),
    max_runtime_us: Option<u64>,
) -> pb::agent_command::Body {
    pb::agent_command::Body::StartJob(pb::StartJob {
        allocation: Some(alloc.into()),
        attempt: Some(attempt.into()),
        job: Some(job.into()),
        image: "img".into(),
        command: vec!["run".into()],
        max_runtime_us,
        notice_signal: pbcore::NoticeSignal::Sigusr1 as i32,
        notice_us: NOTICE.as_micros() as u64,
        abort_grace_us: GRACE.as_micros() as u64,
        ..Default::default()
    })
}

/// A registered session, ready for commands.
async fn registered() -> (tempfile::TempDir, TestSession, FakeExecutor) {
    let (dir, mut session, exec) = session();
    register(&mut session, 1, 1, 1).await;
    (dir, session, exec)
}

/// Every `attempt_notice` in `reports`, as `(allocation, remaining, shortfall)`.
fn notices(reports: &[pb::AgentReport]) -> Vec<(AllocationId, Duration, Duration)> {
    reports
        .iter()
        .filter_map(|r| match r.body.as_ref()? {
            pb::agent_report::Body::AttemptNotice(n) => {
                assert_eq!(
                    PreemptReason::try_from(n.reason.clone().unwrap()).unwrap(),
                    INTERRUPTED
                );
                Some((
                    n.allocation.clone().unwrap().try_into().unwrap(),
                    Duration::from_micros(n.remaining_us as i64),
                    Duration::from_micros(n.shortfall_us as i64),
                ))
            }
            _ => None,
        })
        .collect()
}

/// The stop time an attempt with [`GRACE`] gets under a plan with deadline `d`.
fn t_stop(d: Timestamp) -> Timestamp {
    d - DRAIN_CLEANUP_MARGIN - GRACE
}

/// Run every stop due at `now` to completion through the fake, as the live
/// loop's spawned tasks would, collecting the reports.
async fn run_due_stops(
    session: &mut TestSession,
    exec: &FakeExecutor,
    now: Timestamp,
) -> Vec<pb::AgentReport> {
    let stops = session.begin_due_stops(now).unwrap();
    let mut reports = Vec::new();
    for stop in stops {
        let result = exec.stop(stop.allocation, stop.grace).await;
        reports.extend(session.finish_stop(stop.allocation, result, now).unwrap());
    }
    reports
}

fn exit(code: i32, cause: ExitCause, exec: &FakeExecutor) -> ExitInfo {
    ExitInfo {
        code,
        cause,
        runtime: Duration::from_secs(1),
        finished_at: exec.now(),
    }
}

// ---- plan execution ----

#[tokio::test]
async fn an_interruption_notifies_every_attempt_then_stops_it_at_its_stop_time() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let (b, _, _) = start_noticed(&mut session, 3).await;

    let now = Timestamp::now();
    let d = now + Duration::from_secs(120);
    let reports = session.on_interruption(d, now).await.unwrap();

    // Plan journaled, both attempts notified: intent, signal, mark, report.
    assert_eq!(session.active_plan().unwrap().deadline, d);
    let window = t_stop(d).duration_since(now); // 80 s: more than the 60 s notice
    let mut reported = notices(&reports);
    reported.sort();
    let mut expected = vec![(a, window, Duration::ZERO), (b, window, Duration::ZERO)];
    expected.sort();
    assert_eq!(reported, expected);
    for alloc in [a, b] {
        assert_eq!(exec.signals_delivered(alloc), vec![NoticeSignal::Sigusr1]);
        assert_eq!(session.state().notice_intents[&alloc], INTERRUPTED);
        assert_eq!(session.state().marks[&alloc].stop_at, t_stop(d));
    }

    // Announced on both report shapes, with the provider deadline.
    let pb::agent_report::Body::Heartbeat(hb) = session.heartbeat_report().await.body.unwrap()
    else {
        panic!("heartbeat");
    };
    assert!(hb.draining);
    assert_eq!(hb.interruption_deadline_us, Some(d.as_micros()));
    let pb::agent_report::Body::Register(reg) = session.register_report().body.unwrap() else {
        panic!("register");
    };
    assert!(reg.draining);
    assert_eq!(reg.interruption_deadline_us, Some(d.as_micros()));

    // Nothing is due before the stop time; at it, both stop.
    assert_eq!(session.next_stop_at(), Some(t_stop(d)));
    assert!(session
        .begin_due_stops(t_stop(d) - Duration::from_secs(1))
        .unwrap()
        .is_empty());
    let reports = run_due_stops(&mut session, &exec, t_stop(d)).await;
    assert_eq!(reports.len(), 2);
    for report in reports.chunks(1) {
        assert_eq!(terminal_outcome(report), Some(PREEMPTED));
    }
    for alloc in [a, b] {
        assert_eq!(
            session.state().tombstones[&alloc],
            crate::journal::TombstoneReason::Preempt {
                reason: INTERRUPTED
            }
        );
        assert_eq!(session.state().exits[&alloc].outcome, PREEMPTED);
    }
    assert!(session.next_stop_at().is_none());
    assert!(session.outstanding_live_work().is_empty());
}

#[tokio::test]
async fn a_stop_in_flight_is_never_handed_out_twice() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    session
        .on_interruption(now + Duration::from_secs(41), now)
        .await
        .unwrap();
    let due = session
        .begin_due_stops(now + Duration::from_secs(1))
        .unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].grace, GRACE);
    assert!(session.next_stop_at().is_none(), "in flight, not due again");
    assert!(session
        .begin_due_stops(now + Duration::from_secs(2))
        .unwrap()
        .is_empty());

    // A failed stop is retried after a backoff, not at once.
    session
        .finish_stop(
            a,
            Err(crate::executor::ExecutorError::Other(
                "daemon hiccup".into(),
            )),
            now,
        )
        .unwrap();
    assert!(session.begin_due_stops(now).unwrap().is_empty());
    let retry = session.next_stop_at().expect("retried later");
    assert!(retry > now);
    let reports = run_due_stops(&mut session, &exec, retry).await;
    assert_eq!(terminal_outcome(&reports), Some(PREEMPTED));
}

// ---- classification (ADR 0044 §3) ----

#[tokio::test]
async fn a_workload_that_exits_non_zero_on_the_signal_is_preempted() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    exec.exit_on_signal(a, NoticeSignal::Sigusr1, 75);
    let now = Timestamp::now();
    session
        .on_interruption(now + Duration::from_secs(120), now)
        .await
        .unwrap();
    assert!(!exec.is_running(a), "it trapped the signal and exited");
    let reports = session
        .handle_observed_exit(a, exit(75, ExitCause::Natural, &exec))
        .await
        .unwrap();
    assert_eq!(terminal_outcome(&reports), Some(PREEMPTED));
    // The stop path then finds nothing to do.
    assert!(session.next_stop_at().is_none());
}

#[tokio::test]
async fn a_non_zero_exit_without_a_notice_is_the_jobs_own() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    exec.finish(a, exit(3, ExitCause::Natural, &exec));
    let reports = session
        .handle_observed_exit(a, exit(3, ExitCause::Natural, &exec))
        .await
        .unwrap();
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::Exited { code: 3 })
    );
}

#[tokio::test]
async fn exit_zero_during_the_notice_window_is_completion() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    session
        .on_interruption(now + Duration::from_secs(120), now)
        .await
        .unwrap();
    exec.finish(a, exit(0, ExitCause::Natural, &exec));
    let reports = session
        .handle_observed_exit(a, exit(0, ExitCause::Natural, &exec))
        .await
        .unwrap();
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::Exited { code: 0 })
    );
}

#[tokio::test]
async fn an_oom_during_the_notice_window_is_the_limit() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let (b, _, _) = start_noticed(&mut session, 3).await;
    let now = Timestamp::now();
    let d = now + Duration::from_secs(120);
    session.on_interruption(d, now).await.unwrap();

    // `a` is OOM-killed on its own during the window.
    exec.finish(a, exit(137, ExitCause::OomKilled, &exec));
    let reports = session
        .handle_observed_exit(a, exit(137, ExitCause::OomKilled, &exec))
        .await
        .unwrap();
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::MemoryLimitExceeded)
    );

    // `b`'s OOM lands as the stop path's stop takes effect: the limit wins.
    exec.plan_stop_cause(b, ExitCause::OomKilled);
    let reports = run_due_stops(&mut session, &exec, t_stop(d)).await;
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::MemoryLimitExceeded)
    );
}

#[tokio::test]
async fn a_max_runtime_watchdog_that_fires_first_wins() {
    let (_dir, mut session, exec) = registered().await;
    let ids = (AllocationId::new(), AttemptId::new(), JobId::new());
    session
        .handle_command_settled(command(1, 1, 2, start_body(ids, Some(5_000_000))))
        .await
        .unwrap();
    let a = ids.0;
    let now = Timestamp::now();
    session
        .on_interruption(now + Duration::from_secs(120), now)
        .await
        .unwrap();
    let reports = session.trigger_max_runtime(a).await.unwrap();
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::RuntimeLimitExceeded)
    );
    assert!(session.next_stop_at().is_none());

    // ...but once the preemption stop is in flight, the watchdog stands down.
    let ids = (AllocationId::new(), AttemptId::new(), JobId::new());
    session
        .handle_command_settled(command(1, 1, 3, start_body(ids, Some(5_000_000))))
        .await
        .unwrap();
    let stop_at = session.state().marks[&ids.0].stop_at;
    let stops = session.begin_due_stops(stop_at).unwrap();
    assert_eq!(stops.len(), 1);
    assert!(session.trigger_max_runtime(ids.0).await.unwrap().is_empty());
    let result = exec.stop(ids.0, GRACE).await;
    let reports = session.finish_stop(ids.0, result, stop_at).unwrap();
    assert_eq!(terminal_outcome(&reports), Some(PREEMPTED));
}

#[tokio::test]
async fn an_unattributable_kill_after_the_notice_is_preempted() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    session
        .on_interruption(now + Duration::from_secs(120), now)
        .await
        .unwrap();
    exec.finish(a, exit(137, ExitCause::Killed, &exec));
    let reports = session
        .handle_observed_exit(a, exit(137, ExitCause::Killed, &exec))
        .await
        .unwrap();
    assert_eq!(terminal_outcome(&reports), Some(PREEMPTED));
}

// ---- tombstones ----

#[tokio::test]
async fn a_start_refused_by_a_preemption_tombstone_reports_preempted() {
    let (dir, mut session, exec) = registered().await;
    let ids = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    let d = now + Duration::from_secs(120);
    session.on_interruption(d, now).await.unwrap();
    // The stop's tombstone is journaled, then the agent dies mid-stop.
    assert_eq!(session.begin_due_stops(t_stop(d)).unwrap().len(), 1);
    drop(session);

    let mut session = restarted(&dir, &exec);
    register(&mut session, 1, 2, 1).await;
    // A re-delivered StartJob (no exit journaled yet) resolves as the stop.
    let reports = session
        .handle_command_settled(command(1, 2, 2, start_body(ids, None)))
        .await
        .unwrap();
    assert_eq!(terminal_outcome(&reports), Some(PREEMPTED));
    // And the interrupted stop is due again at once.
    assert!(session.next_stop_at().unwrap() <= now);
    let reports = run_due_stops(&mut session, &exec, now).await;
    assert_eq!(terminal_outcome(&reports), Some(PREEMPTED));
}

// ---- recovery ----

#[tokio::test]
async fn recovery_resumes_a_journaled_mark_without_signalling_again() {
    let (dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    let d = now + Duration::from_secs(120);
    session.on_interruption(d, now).await.unwrap();
    drop(session);

    let mut session = restarted(&dir, &exec);
    assert!(session.resume_notices(now).await.unwrap().is_empty());
    assert_eq!(exec.signals_delivered(a).len(), 1, "no second signal");
    assert_eq!(session.next_stop_at(), Some(t_stop(d)));
    assert_eq!(session.active_plan().unwrap().deadline, d);

    // The ObservedSet repeats the live mark, remaining recomputed.
    let reports = session
        .handle_command_settled(command(
            1,
            2,
            1,
            pb::agent_command::Body::RegisterAccepted(pb::RegisterAccepted {}),
        ))
        .await
        .unwrap();
    let Some(pb::agent_report::Body::ObservedSet(set)) = reports[0].body.clone() else {
        panic!("ObservedSet");
    };
    let entry = &set.allocations[0];
    assert!(entry.running);
    let notice = entry.notice.clone().expect("the live mark is repeated");
    assert!(notice.remaining_us > 0 && notice.remaining_us <= 80_000_000);
    assert_eq!(notice.shortfall_us, 0);
    assert_eq!(
        PreemptReason::try_from(notice.reason.unwrap()).unwrap(),
        INTERRUPTED
    );
}

#[tokio::test]
async fn recovery_of_an_intent_without_a_mark_signals_again_under_the_plan() {
    let (dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    // A tight plan, journaled with its notice intent — then the crash, before
    // the signal and the mark.
    let d = now + Duration::from_secs(70);
    session
        .journal
        .journal_drain_plan(&crate::journal::DrainPlanRec {
            deadline: d,
            reason: INTERRUPTED,
            accepted_target: None,
        })
        .unwrap();
    session
        .journal
        .journal_notice_intent(a, INTERRUPTED)
        .unwrap();
    drop(session);

    let mut session = restarted(&dir, &exec);
    let reports = session.resume_notices(now).await.unwrap();
    assert_eq!(exec.signals_delivered(a), vec![NoticeSignal::Sigusr1]);
    // Stop = the plan's T_stop (30 s from now): a 60 s notice is 30 s short.
    let mark = session.state().marks[&a];
    assert_eq!(mark.stop_at, t_stop(d));
    assert_eq!(mark.shortfall, Duration::from_secs(30));
    assert_eq!(
        notices(&reports),
        vec![(a, Duration::from_secs(30), Duration::from_secs(30))]
    );
}

#[tokio::test]
async fn recovery_of_an_intent_without_a_mark_and_no_plan_counts_down_the_notice() {
    let (dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    session
        .journal
        .journal_notice_intent(a, INTERRUPTED)
        .unwrap();
    drop(session);

    let mut session = restarted(&dir, &exec);
    let now = Timestamp::now();
    session.resume_notices(now).await.unwrap();
    assert_eq!(exec.signals_delivered(a).len(), 1);
    assert_eq!(session.state().marks[&a].stop_at, now + NOTICE);
    assert_eq!(session.state().marks[&a].shortfall, Duration::ZERO);
}

#[tokio::test]
async fn a_plan_whose_deadline_passed_stops_its_work_at_once_and_retires() {
    let (dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let (b, _, _) = start_noticed(&mut session, 3).await;
    let then = Timestamp::now();
    let d = then + Duration::from_secs(120);
    // `a` was notified; `b` was not yet (the crash came between plan and
    // notices for it).
    session
        .journal
        .journal_drain_plan(&crate::journal::DrainPlanRec {
            deadline: d,
            reason: INTERRUPTED,
            accepted_target: None,
        })
        .unwrap();
    session
        .journal
        .journal_notice_intent(a, INTERRUPTED)
        .unwrap();
    session
        .journal
        .journal_notice_mark(
            a,
            &crate::journal::NoticeMarkRec {
                stop_at: t_stop(d),
                shortfall: Duration::ZERO,
                notified_at: then,
            },
        )
        .unwrap();
    drop(session);

    // The host came back after D.
    let later = d + Duration::from_secs(600);
    let mut session = restarted(&dir, &exec);
    session.resume_notices(later).await.unwrap();
    assert!(session.active_plan().is_none(), "retired");
    assert!(!super::draining_of(&session.register_report()));
    assert_eq!(session.state().notice_intents[&b], INTERRUPTED);
    let reports = run_due_stops(&mut session, &exec, later).await;
    assert_eq!(reports.len(), 2);
    assert!(session.outstanding_live_work().is_empty());

    // The retirement is durable.
    drop(session);
    let session = restarted(&dir, &exec);
    assert!(session.active_plan().is_none());
}

// ---- replacement marks ----

#[tokio::test]
async fn an_earlier_deadline_shortens_marks_and_a_later_one_changes_nothing() {
    let (dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    let d1 = now + Duration::from_secs(120);
    session.on_interruption(d1, now).await.unwrap();

    // A second notice 40 s earlier: the stop moves 40 s earlier, and the
    // 80 s window (20 s of slack over the 60 s notice) shrinks to 40 s — 20 s
    // short of the declared notice.
    let d2 = d1 - Duration::from_secs(40);
    let reports = session.on_interruption(d2, now).await.unwrap();
    assert_eq!(session.state().marks[&a].stop_at, t_stop(d2));
    assert_eq!(
        notices(&reports),
        vec![(a, Duration::from_secs(40), Duration::from_secs(20))],
        "the shortfall is measured against the window the signal opened"
    );
    assert_eq!(
        exec.signals_delivered(a).len(),
        1,
        "a replacement re-signals nothing"
    );

    // A later or equal deadline is a no-op.
    assert!(session.on_interruption(d1, now).await.unwrap().is_empty());
    assert!(session.on_interruption(d2, now).await.unwrap().is_empty());
    assert_eq!(session.active_plan().unwrap().deadline, d2);

    // The replacement is what a restart restores.
    drop(session);
    let session = restarted(&dir, &exec);
    assert_eq!(session.state().marks[&a].stop_at, t_stop(d2));
    assert_eq!(session.active_plan().unwrap().deadline, d2);
}

#[tokio::test]
async fn a_stale_notice_starts_no_plan() {
    let (_dir, mut session, _exec) = registered().await;
    let _ = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    assert!(session
        .on_interruption(now - Duration::from_secs(1), now)
        .await
        .unwrap()
        .is_empty());
    assert!(session.active_plan().is_none());
}

// ---- StartJob under a plan ----

#[tokio::test]
async fn a_start_under_an_active_plan_is_notified_at_once() {
    let (_dir, mut session, exec) = registered().await;
    let now = Timestamp::now();
    let d = now + Duration::from_secs(300);
    session.on_interruption(d, now).await.unwrap();

    let ids = (AllocationId::new(), AttemptId::new(), JobId::new());
    let reports = session
        .handle_command_settled(command(1, 1, 2, start_body(ids, None)))
        .await
        .unwrap();
    assert!(super::is_running_report(&reports[..1]));
    let reported = notices(&reports);
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].0, ids.0);
    assert_eq!(exec.signals_delivered(ids.0), vec![NoticeSignal::Sigusr1]);
    assert_eq!(session.state().marks[&ids.0].stop_at, t_stop(d));
}

#[tokio::test]
async fn a_start_whose_stop_time_has_passed_is_stopped_at_once() {
    let (_dir, mut session, exec) = registered().await;
    let now = Timestamp::now();
    // D is 35 s out: T_stop = D − 30 s − 10 s is already 5 s past.
    session
        .on_interruption(now + Duration::from_secs(35), now)
        .await
        .unwrap();
    let ids = (AllocationId::new(), AttemptId::new(), JobId::new());
    let reports = session
        .handle_command_settled(command(1, 1, 2, start_body(ids, None)))
        .await
        .unwrap();
    let reported = notices(&reports);
    assert_eq!(reported[0].1, Duration::ZERO, "no notice left");
    assert_eq!(reported[0].2, NOTICE, "the whole notice is short");
    assert!(session.next_stop_at().unwrap() <= Timestamp::now());
    let reports = run_due_stops(&mut session, &exec, Timestamp::now()).await;
    assert_eq!(terminal_outcome(&reports), Some(PREEMPTED));
}

// ---- readiness ----

#[tokio::test]
async fn an_active_plan_reads_interrupted() {
    let dir = tempfile::tempdir().unwrap();
    let (journal, state) =
        crate::journal::Journal::open(coppice_consensus::fs::RealFs::new(dir.path())).unwrap();
    let health = crate::health::AgentHealth::new(coppice_core::id::NodeId::new());
    let mut session = crate::session::Session::new(
        coppice_core::id::NodeId::new(),
        coppice_core::resource::Resources::ZERO,
        Vec::new(),
        journal,
        state,
        FakeExecutor::new(),
    )
    .with_health(health.clone());
    register(&mut session, 1, 1, 1).await;
    assert_eq!(health.readiness().phase, crate::health::Phase::Ready);
    let now = Timestamp::now();
    session
        .on_interruption(now + Duration::from_secs(120), now)
        .await
        .unwrap();
    assert_eq!(health.readiness().phase, crate::health::Phase::Interrupted);
}

// ---- exits racing a spawned stop (ADR 0013: an exit during our stop) ----

#[tokio::test]
async fn an_exit_zero_on_the_preemption_stops_term_is_still_preempted() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    let d = now + Duration::from_secs(120);
    session.on_interruption(d, now).await.unwrap();
    let stops = session.begin_due_stops(t_stop(d)).unwrap();
    assert_eq!(stops.len(), 1);

    // The workload exits 0 on the stop's SIGTERM, and the die event reaches
    // the session before the stop task's post-inspect does.
    exec.finish(a, exit(0, ExitCause::Natural, &exec));
    let early = session
        .handle_observed_exit(a, exit(0, ExitCause::Natural, &exec))
        .await
        .unwrap();
    assert!(early.is_empty(), "the stop in flight decides, not the exit");
    assert!(!session.state().exits.contains_key(&a));

    // The stop's own answer: it terminated the container.
    let stopped = crate::executor::StopOutcome::Stopped(exit(0, ExitCause::Natural, &exec));
    let reports = session.finish_stop(a, Ok(stopped), t_stop(d)).unwrap();
    assert_eq!(terminal_outcome(&reports), Some(PREEMPTED));
}

#[tokio::test]
async fn a_failed_stop_falls_back_to_the_exit_it_held() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    let d = now + Duration::from_secs(120);
    session.on_interruption(d, now).await.unwrap();
    session.begin_due_stops(t_stop(d)).unwrap();
    exec.finish(a, exit(0, ExitCause::Natural, &exec));
    session
        .handle_observed_exit(a, exit(0, ExitCause::Natural, &exec))
        .await
        .unwrap();

    // The stop could not say what happened: the held exit is classified on
    // its own evidence, as it would have been with no stop at all.
    let reports = session
        .finish_stop(
            a,
            Err(crate::executor::ExecutorError::Other(
                "daemon hiccup".into(),
            )),
            t_stop(d),
        )
        .unwrap();
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::Exited { code: 0 })
    );
    assert!(session.next_stop_at().is_none(), "nothing left to retry");
}

#[tokio::test]
async fn an_abort_stop_in_flight_holds_an_early_exit_too() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    // Unsettled: the stop is queued for the loop, and the command returns at
    // once — nothing waits out `abort_grace` here.
    let reports = session
        .handle_command(command(1, 1, 3, super::stop_job(a)))
        .await
        .unwrap();
    assert!(reports.is_empty());
    let stops = session.take_pending_stops();
    assert_eq!(stops.len(), 1);

    // Meanwhile the session keeps serving: another start goes through.
    let (b, _, _) = start_noticed(&mut session, 4).await;
    assert!(exec.is_running(b));

    exec.finish(a, exit(0, ExitCause::Natural, &exec));
    assert!(session
        .handle_observed_exit(a, exit(0, ExitCause::Natural, &exec))
        .await
        .unwrap()
        .is_empty());
    let result = exec.stop(a, stops[0].grace).await;
    let reports = session.finish_stop(a, result, Timestamp::now()).unwrap();
    // The fake answers AlreadyExited (exit 0): truth wins over the abort for
    // a container that ended before the stop took effect.
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::Exited { code: 0 })
    );

    // And with the daemon reporting our stop terminated it, the abort wins.
    let (c, _, _) = start_noticed(&mut session, 5).await;
    session
        .handle_command(command(1, 1, 6, super::stop_job(c)))
        .await
        .unwrap();
    session.take_pending_stops();
    session
        .handle_observed_exit(c, exit(0, ExitCause::Natural, &exec))
        .await
        .unwrap();
    let stopped = crate::executor::StopOutcome::Stopped(exit(0, ExitCause::Natural, &exec));
    let reports = session
        .finish_stop(c, Ok(stopped), Timestamp::now())
        .unwrap();
    assert_eq!(terminal_outcome(&reports), Some(AttemptOutcome::Aborted));
}

// ---- starts in flight ----

#[tokio::test]
async fn a_notice_while_a_start_is_in_flight_is_delivered_once_it_starts() {
    let (_dir, mut session, exec) = registered().await;
    let ids = (AllocationId::new(), AttemptId::new(), JobId::new());
    let a = ids.0;
    // Unsettled: the intent is journaled, the start queued.
    let reports = session
        .handle_command(command(1, 1, 2, start_body(ids, None)))
        .await
        .unwrap();
    assert!(
        reports.is_empty(),
        "Running is reported when the start finishes"
    );
    assert!(
        session.state().intents.contains_key(&a),
        "fsync-before-start"
    );
    let starts = session.take_pending_starts();
    assert_eq!(starts.len(), 1);
    // A duplicate while it is in flight says nothing yet.
    assert!(session
        .handle_command(command(1, 1, 2, start_body(ids, None)))
        .await
        .unwrap()
        .is_empty());

    // The interruption lands mid-pull: the attempt is accountable work (the
    // drain waits for it) but has no container to signal.
    let now = Timestamp::now();
    let d = now + Duration::from_secs(120);
    assert!(session.on_interruption(d, now).await.unwrap().is_empty());
    assert_eq!(session.outstanding_live_work(), vec![a]);
    assert!(session.next_stop_at().is_none());

    // The start completes: Running, then the notice under the plan.
    let result = exec.start(starts.into_iter().next().unwrap()).await;
    let reports = session.finish_start(a, result, now).await.unwrap();
    assert!(super::is_running_report(&reports[..1]));
    assert_eq!(notices(&reports).len(), 1);
    assert_eq!(exec.signals_delivered(a), vec![NoticeSignal::Sigusr1]);
    assert_eq!(session.next_stop_at(), Some(t_stop(d)));
}

#[tokio::test]
async fn a_stop_job_racing_a_start_in_flight_stops_it_once_started() {
    let (_dir, mut session, exec) = registered().await;
    let ids = (AllocationId::new(), AttemptId::new(), JobId::new());
    let a = ids.0;
    session
        .handle_command(command(1, 1, 2, start_body(ids, None)))
        .await
        .unwrap();
    let starts = session.take_pending_starts();
    assert!(session
        .handle_command(command(1, 1, 3, super::stop_job(a)))
        .await
        .unwrap()
        .is_empty());
    assert!(
        session.take_pending_stops().is_empty(),
        "nothing to stop yet"
    );

    let result = exec.start(starts.into_iter().next().unwrap()).await;
    let mut reports = session
        .finish_start(a, result, Timestamp::now())
        .await
        .unwrap();
    assert!(super::is_running_report(&reports));
    reports = session.settle().await.unwrap();
    assert_eq!(terminal_outcome(&reports), Some(AttemptOutcome::Aborted));
    assert!(!exec.is_running(a));
}

// ---- recovery of a half-applied replacement plan ----

#[tokio::test]
async fn recovery_shortens_marks_a_crash_left_behind_an_earlier_plan() {
    let (dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    let d1 = now + Duration::from_secs(120);
    session.on_interruption(d1, now).await.unwrap();
    assert_eq!(session.state().marks[&a].stop_at, t_stop(d1));
    // An earlier notice: the replacement plan is journaled, then the crash,
    // before the replacement mark.
    let d2 = d1 - Duration::from_secs(40);
    session
        .journal
        .journal_drain_plan(&crate::journal::DrainPlanRec {
            deadline: d2,
            reason: INTERRUPTED,
            accepted_target: None,
        })
        .unwrap();
    drop(session);

    let mut session = restarted(&dir, &exec);
    assert_eq!(
        session.state().marks[&a].stop_at,
        t_stop(d1),
        "the stale mark"
    );
    let reports = session.resume_notices(now).await.unwrap();
    let mark = session.state().marks[&a];
    assert_eq!(mark.stop_at, t_stop(d2));
    assert_eq!(mark.shortfall, Duration::from_secs(20));
    assert_eq!(
        mark.notified_at, now,
        "the window still opened at the signal"
    );
    assert_eq!(
        notices(&reports),
        vec![(a, Duration::from_secs(40), Duration::from_secs(20))]
    );
    assert_eq!(exec.signals_delivered(a).len(), 1, "no second signal");
}

// ---- reconnects while a start or stop is in flight ----

/// Reconnect: the stream broke and a fresh registration was accepted.
async fn reconnect(session: &mut TestSession, epoch: u64) -> Vec<pb::ObservedAllocation> {
    session.reset_session();
    let reports = session
        .handle_command(command(
            1,
            epoch,
            1,
            pb::agent_command::Body::RegisterAccepted(pb::RegisterAccepted {}),
        ))
        .await
        .unwrap();
    let Some(pb::agent_report::Body::ObservedSet(set)) = reports[0].body.clone() else {
        panic!("ObservedSet");
    };
    set.allocations
}

#[tokio::test]
async fn a_reconnect_mid_start_reports_the_start_unresolved_not_lost() {
    let (_dir, mut session, exec) = registered().await;
    let ids = (AllocationId::new(), AttemptId::new(), JobId::new());
    let a = ids.0;
    session
        .handle_command(command(1, 1, 2, start_body(ids, None)))
        .await
        .unwrap();
    let starts = session.take_pending_starts();

    // The pull is still running when the session reconnects: the intent has
    // no container yet, but this is not the abandoned intent of a crash.
    let set = reconnect(&mut session, 2).await;
    assert_eq!(set.len(), 1);
    assert!(!set[0].running);
    assert_eq!(set[0].outcome, None, "unresolved, not AgentError");

    let result = exec.start(starts.into_iter().next().unwrap()).await;
    let reports = session
        .finish_start(a, result, Timestamp::now())
        .await
        .unwrap();
    assert!(super::is_running_report(&reports));

    // Once started, a reconnect reports it running as usual.
    let set = reconnect(&mut session, 3).await;
    assert!(set[0].running);
}

#[tokio::test]
async fn a_reconnect_while_a_stop_holds_an_exit_reports_it_unresolved() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    let d = now + Duration::from_secs(120);
    session.on_interruption(d, now).await.unwrap();
    session.begin_due_stops(t_stop(d)).unwrap();
    // Exit 0 on the stop's SIGTERM, held for the stop's answer.
    exec.finish(a, exit(0, ExitCause::Natural, &exec));
    session
        .handle_observed_exit(a, exit(0, ExitCause::Natural, &exec))
        .await
        .unwrap();

    // The reconnect must not let the runtime's exit 0 through as terminal.
    let set = reconnect(&mut session, 2).await;
    assert_eq!(set.len(), 1);
    assert!(!set[0].running);
    assert_eq!(set[0].outcome, None, "unresolved, not Exited{{0}}");

    let stopped = crate::executor::StopOutcome::Stopped(exit(0, ExitCause::Natural, &exec));
    let reports = session.finish_stop(a, Ok(stopped), t_stop(d)).unwrap();
    assert_eq!(terminal_outcome(&reports), Some(PREEMPTED));
    // Resolved: the next reconnect reports the journaled outcome.
    let set = reconnect(&mut session, 3).await;
    assert_eq!(
        set[0].outcome,
        Some(pbcore::AttemptOutcome::from(&PREEMPTED))
    );
}

// ---- PreemptJob: the scheduler's preemption (ADR 0044 §3, §4) ----

fn priority_for(job: JobId) -> PreemptReason {
    PreemptReason::Priority { for_job: job }
}

fn preempt_body(alloc: AllocationId, reason: PreemptReason) -> pb::agent_command::Body {
    pb::agent_command::Body::PreemptJob(pb::PreemptJob {
        allocation: Some(alloc.into()),
        notice_us: NOTICE.as_micros() as u64,
        reason: Some(reason.into()),
    })
}

/// Every `attempt_notice` in `reports`, as `(allocation, reason)`.
fn notice_reasons(reports: &[pb::AgentReport]) -> Vec<(AllocationId, PreemptReason)> {
    reports
        .iter()
        .filter_map(|r| match r.body.as_ref()? {
            pb::agent_report::Body::AttemptNotice(n) => Some((
                n.allocation.clone().unwrap().try_into().unwrap(),
                PreemptReason::try_from(n.reason.clone().unwrap()).unwrap(),
            )),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn preempt_job_notifies_then_stops_the_attempt_preempted_for_priority() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let reason = priority_for(JobId::new());

    let before = Timestamp::now();
    let reports = session
        .handle_command_settled(command(1, 1, 3, preempt_body(a, reason)))
        .await
        .unwrap();
    let after = Timestamp::now();

    // Intent journaled, signal delivered, mark journaled, notice reported.
    assert_eq!(notice_reasons(&reports), vec![(a, reason)]);
    assert_eq!(exec.signals_delivered(a), vec![NoticeSignal::Sigusr1]);
    assert_eq!(session.state().notice_intents[&a], reason);
    let mark = session.state().marks[&a];
    assert!(mark.stop_at >= before + NOTICE && mark.stop_at <= after + NOTICE);
    assert_eq!(mark.shortfall, Duration::ZERO);

    // Still running through the window; stopped and classified at its end.
    assert!(exec.is_running(a));
    assert_eq!(session.next_stop_at(), Some(mark.stop_at));
    let reports = run_due_stops(&mut session, &exec, mark.stop_at).await;
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::Preempted { reason })
    );
    assert_eq!(
        session.state().tombstones[&a],
        crate::journal::TombstoneReason::Preempt { reason }
    );
}

#[tokio::test]
async fn a_repeated_preempt_job_keeps_the_one_stop_time_and_re_reports_the_mark() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let reason = priority_for(JobId::new());
    session
        .handle_command_settled(command(1, 1, 3, preempt_body(a, reason)))
        .await
        .unwrap();
    let mark = session.state().marks[&a];

    // The coordinator's re-send (a fresh sequence number, after a reconnect
    // or a leadership change): no second signal, no new stop time, and the
    // mark is reported again so an acknowledgement lost in transit lands.
    let reports = session
        .handle_command_settled(command(1, 1, 4, preempt_body(a, reason)))
        .await
        .unwrap();
    assert_eq!(notice_reasons(&reports), vec![(a, reason)]);
    assert_eq!(exec.signals_delivered(a).len(), 1);
    assert_eq!(session.state().marks[&a], mark);

    // A duplicate sequence number is not acted on at all.
    let reports = session
        .handle_command_settled(command(1, 1, 4, preempt_body(a, reason)))
        .await
        .unwrap();
    assert!(reports.is_empty());
}

#[tokio::test]
async fn preempt_job_for_an_attempt_already_under_an_interruption_has_no_effect() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let now = Timestamp::now();
    session
        .on_interruption(now + Duration::from_secs(120), now)
        .await
        .unwrap();
    let mark = session.state().marks[&a];

    let reports = session
        .handle_command_settled(command(
            1,
            1,
            3,
            preempt_body(a, priority_for(JobId::new())),
        ))
        .await
        .unwrap();
    // One record per attempt: the interruption's notice stands, reason and
    // stop alike.
    assert_eq!(notice_reasons(&reports), vec![(a, INTERRUPTED)]);
    assert_eq!(exec.signals_delivered(a).len(), 1);
    assert_eq!(session.state().notice_intents[&a], INTERRUPTED);
    assert_eq!(session.state().marks[&a], mark);
}

#[tokio::test]
async fn preempt_job_for_an_ended_or_aborting_attempt_does_nothing() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let (b, _, _) = start_noticed(&mut session, 3).await;
    let reason = priority_for(JobId::new());

    // `a` already exited on its own.
    exec.finish(a, exit(0, ExitCause::Natural, &exec));
    session
        .handle_observed_exit(a, exit(0, ExitCause::Natural, &exec))
        .await
        .unwrap();
    // `b` is being aborted.
    session
        .handle_command_settled(command(1, 1, 4, super::stop_job(b)))
        .await
        .unwrap();

    for (seq, alloc) in [(5, a), (6, b)] {
        let reports = session
            .handle_command_settled(command(1, 1, seq, preempt_body(alloc, reason)))
            .await
            .unwrap();
        assert!(notice_reasons(&reports).is_empty());
        assert!(exec.signals_delivered(alloc).is_empty());
        assert!(!session.state().notice_intents.contains_key(&alloc));
    }
}

#[tokio::test]
async fn a_workload_exiting_non_zero_on_a_priority_notice_is_preempted_for_priority() {
    let (_dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    exec.exit_on_signal(a, NoticeSignal::Sigusr1, 75);
    let reason = priority_for(JobId::new());
    session
        .handle_command_settled(command(1, 1, 3, preempt_body(a, reason)))
        .await
        .unwrap();
    assert!(!exec.is_running(a), "it trapped the signal and exited");
    let reports = session
        .handle_observed_exit(a, exit(75, ExitCause::Natural, &exec))
        .await
        .unwrap();
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::Preempted { reason })
    );
}

#[tokio::test]
async fn recovery_of_a_preempt_job_intent_without_a_mark_counts_down_afresh() {
    let (dir, mut session, exec) = registered().await;
    let (a, _, _) = start_noticed(&mut session, 2).await;
    let reason = priority_for(JobId::new());
    // The crash lands between the intent and the signal.
    session.journal.journal_notice_intent(a, reason).unwrap();
    drop(session);

    let mut session = restarted(&dir, &exec);
    let now = Timestamp::now();
    let reports = session.resume_notices(now).await.unwrap();
    assert_eq!(notice_reasons(&reports), vec![(a, reason)]);
    assert_eq!(exec.signals_delivered(a), vec![NoticeSignal::Sigusr1]);
    let mark = session.state().marks[&a];
    assert_eq!(mark.stop_at, now + NOTICE);
    assert_eq!(mark.shortfall, Duration::ZERO);
    let reports = run_due_stops(&mut session, &exec, mark.stop_at).await;
    assert_eq!(
        terminal_outcome(&reports),
        Some(AttemptOutcome::Preempted { reason })
    );
}
