//! The publishing apply task (coordinator-runtime.md, task 2).
//!
//! This is the canonical single-writer apply loop, wrapped with view and event
//! publication. It is the sole owner of the mutable [`StateMachine`], held by
//! value with no lock: nothing else can name the value, so `&mut` across an
//! `.await` is impossible by construction. Committed entries arrive from the
//! openraft state-machine adapter over the bounded [`ApplyRequest`] channel; the
//! task applies each command, emits its events, publishes a view, and replies —
//! in exactly that order (the doc's `apply → emit events → maybe_publish →
//! reply`). It never awaits a full channel: the event tap `try_send`s, the view
//! watch overwrites, and replies are oneshots.
//!
//! Events are emitted as **one batch per command**, tagged with that command's
//! own log index. How openraft grouped entries into an [`ApplyRequest`] is a
//! local runtime detail; the emitted stream must be a pure function of the
//! committed log so every replica derives identical batches and cursor
//! positions (ADR 0008, KOI-3).

use std::time::Instant;

use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use coppice_state::StateMachine;

use crate::adapter::ApplyRequest;
use crate::events::{EventBatch, EventTap};
use crate::view::ViewPublisher;

/// Apply-stall measurement (coordinator-runtime.md § clone-cost analysis):
/// how long the sole apply task is occupied per request, i.e. the pause every
/// other apply, publish, and strong read waits out.
const APPLY_BATCH_SECONDS: &str = "coordinator_apply_batch_seconds";
const SNAPSHOT_CAPTURE_SECONDS: &str = "coordinator_snapshot_capture_seconds";

pub(crate) fn describe_metrics() {
    metrics::describe_histogram!(
        APPLY_BATCH_SECONDS,
        metrics::Unit::Seconds,
        "Time the apply task spends on one apply batch (apply, emit, publish, reply)."
    );
    metrics::describe_histogram!(
        SNAPSHOT_CAPTURE_SECONDS,
        metrics::Unit::Seconds,
        "Time the apply task spends capturing state for a snapshot."
    );
}

pub(crate) fn gather_metrics() {
    // Both histograms are pushed as the loop runs; nothing needs sampling.
}

/// Run the apply loop until the request channel closes.
///
/// `initial_applied_index` is the log index the recovered `state` reflects —
/// the same index the publisher's seed view carries (view.rs). The loop
/// republishes it once up front, which seeds the startup metrics gauges and
/// the cadence clock before entering its `select!`.
///
/// Ends when every [`ApplyRequest`] sender is dropped — the adapter drops the
/// channel at openraft shutdown (coordinator-runtime.md shutdown step 5).
pub(crate) async fn run(
    mut state: StateMachine,
    initial_applied_index: u64,
    mut rx: mpsc::Receiver<ApplyRequest>,
    mut publisher: ViewPublisher,
    mut tap: EventTap,
) {
    let mut applied_index = initial_applied_index;
    // The seed view already carries this index (view.rs); republishing seeds
    // the startup metrics gauges and starts the cadence clock.
    publisher.publish_now(&state, applied_index);

    // The cadence tick lives here (not in the publisher), so a strong-read
    // barrier resolves even on an idle log; skip missed ticks so a stall does
    // not produce a burst of catch-up publishes.
    let mut cadence = tokio::time::interval(publisher.cadence());
    cadence.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(request) = maybe else {
                    // Adapter dropped: openraft has shut down.
                    break;
                };
                match request {
                    ApplyRequest::Apply { entries, reply } => {
                        let batch_started = Instant::now();
                        let mut outcomes = Vec::with_capacity(entries.len());
                        for (index, command) in &entries {
                            let outcome = state.apply(command);
                            if let Ok(applied) = &outcome {
                                // One batch per command at the command's own
                                // index: the stream is a function of the log,
                                // never of the request's batching (KOI-3).
                                // `emit` skips an empty batch and never blocks.
                                tap.emit(EventBatch {
                                    applied_index: *index,
                                    events: applied.events.clone(),
                                });
                            }
                            outcomes.push(outcome);
                            applied_index = *index;
                        }
                        // Order is mandated: apply -> emit events -> publish ->
                        // reply.
                        publisher.maybe_publish(&state, applied_index);
                        // A dropped receiver just means the proposer went away.
                        let _ = reply.send(outcomes);
                        metrics::histogram!(APPLY_BATCH_SECONDS)
                            .record(batch_started.elapsed().as_secs_f64());
                    }
                    ApplyRequest::Advance { applied_index: idx, reply } => {
                        // A Raft no-op or membership entry that touches neither
                        // state nor the event stream. Move the cursor forward
                        // (never back) and let the normal publish machinery —
                        // demand wakeup and cadence tick — carry the view up to
                        // it, so a strong read at this index resolves.
                        applied_index = applied_index.max(idx);
                        publisher.maybe_publish(&state, applied_index);
                        let _ = reply.send(());
                    }
                    ApplyRequest::Snapshot { reply } => {
                        // Share one clone with the view watch instead of
                        // taking a second: reuse the published view when it
                        // is current, publish (and reset the cadence clock)
                        // when it is not.
                        let capture_started = Instant::now();
                        let _ = reply.send((publisher.state_at(&state, applied_index), applied_index));
                        metrics::histogram!(SNAPSHOT_CAPTURE_SECONDS)
                            .record(capture_started.elapsed().as_secs_f64());
                    }
                    ApplyRequest::Install { state: new_state, applied_index: idx, reply } => {
                        state = *new_state;
                        applied_index = idx;
                        // The applied index jumped forward over a range the
                        // derived stream never emitted events for. Force a
                        // discontinuity so a subscriber resyncs from strong
                        // state instead of replaying silently across the
                        // snapshot boundary (KOI-3).
                        tap.force_gap(applied_index);
                        // Snapshot handoff: the reader must see the exact index.
                        publisher.publish_now(&state, applied_index);
                        let _ = reply.send(());
                    }
                }
            }
            // A strong read is waiting; publish early for outstanding demand.
            _ = publisher.idle_wakeup() => {
                publisher.maybe_publish(&state, applied_index);
            }
            // Routine cadence so a barrier resolves on an idle log.
            _ = cadence.tick() => {
                publisher.maybe_publish(&state, applied_index);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use tokio::sync::oneshot;

    use crate::events::TapItem;
    use crate::view::{ViewPublisher, ViewPublisherConfig};

    use super::*;

    fn bump_command(to: u32) -> coppice_state::Command {
        use coppice_state::command::BumpClusterVersion;
        // Accepted from a default state (cluster_version 0) and emits an event.
        coppice_state::Command::BumpClusterVersion(BumpClusterVersion {
            to,
            bumped_at_us: 0,
        })
    }

    #[tokio::test]
    async fn apply_publishes_views_and_emits_events_in_order() {
        let (publisher, views) =
            ViewPublisher::new(StateMachine::default(), 0, ViewPublisherConfig::default());
        let (tap, mut tap_rx) = EventTap::channel(8);
        let (tx, rx) = mpsc::channel(8);

        let handle = tokio::spawn(run(StateMachine::default(), 0, rx, publisher, tap));

        // The first publish reflects the recovered index (0).
        assert_eq!(views.latest().applied_index(), 0);

        let (reply, reply_rx) = oneshot::channel();
        tx.send(ApplyRequest::Apply {
            entries: vec![(7, bump_command(1))],
            reply,
        })
        .await
        .unwrap();
        let outcomes = reply_rx.await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].is_ok(), "bump should be accepted");

        // A strong read at index 7 must resolve — the demand wakeup drives an
        // early publish.
        let view = views.at_least(7).await.unwrap();
        assert_eq!(view.applied_index(), 7);
        assert_eq!(view.state().cluster_version, 1);

        // The batch's events were emitted at index 7.
        match tap_rx.recv().await {
            Some(TapItem::Batch(batch)) => {
                assert_eq!(batch.applied_index, 7);
                assert!(!batch.events.is_empty());
            }
            other => panic!("expected a batch, got {:?}", other.map(|_| ())),
        }

        drop(tx);
        handle.await.unwrap();
    }

    /// Run the loop over `batchings` (each inner vec is one `ApplyRequest`)
    /// and return the full tapped `(index, events)` stream.
    async fn tapped_stream(
        batchings: Vec<Vec<(u64, coppice_state::Command)>>,
    ) -> Vec<(u64, Vec<coppice_state::Event>)> {
        let (publisher, views) =
            ViewPublisher::new(StateMachine::default(), 0, ViewPublisherConfig::default());
        let (tap, mut tap_rx) = EventTap::channel(64);
        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(run(StateMachine::default(), 0, rx, publisher, tap));

        for entries in batchings {
            let (reply, reply_rx) = oneshot::channel();
            tx.send(ApplyRequest::Apply { entries, reply })
                .await
                .unwrap();
            reply_rx.await.unwrap();
        }
        drop(tx);
        handle.await.unwrap();
        drop(views);

        let mut stream = Vec::new();
        while let Some(item) = tap_rx.recv().await {
            match item {
                TapItem::Batch(batch) => stream.push((batch.applied_index, batch.events)),
                TapItem::Gap { .. } => panic!("nothing was dropped; no gap expected"),
            }
        }
        stream
    }

    #[tokio::test]
    async fn event_stream_is_invariant_under_apply_batching() {
        // The same committed log — including a rejected command mid-sequence
        // (the second bump to 2) — grouped into apply requests three different
        // ways. KOI-3: the tapped stream must be identical in every case, and
        // each batch must carry its own command's log index.
        let log = || {
            vec![
                (3, bump_command(1)),
                (4, bump_command(2)),
                (5, bump_command(2)), // rejected: not monotonic
                (6, bump_command(3)),
            ]
        };

        let one_request = tapped_stream(vec![log()]).await;
        let singletons = tapped_stream(log().into_iter().map(|entry| vec![entry]).collect()).await;
        let mut split = log();
        let tail = split.split_off(2);
        let pairs = tapped_stream(vec![split, tail]).await;

        let expected: Vec<(u64, Vec<coppice_state::Event>)> = vec![
            (
                3,
                vec![coppice_state::Event::ClusterVersionBumped { to: 1 }],
            ),
            (
                4,
                vec![coppice_state::Event::ClusterVersionBumped { to: 2 }],
            ),
            // Index 5 was rejected: no batch, and index 6 keeps its own index.
            (
                6,
                vec![coppice_state::Event::ClusterVersionBumped { to: 3 }],
            ),
        ];
        assert_eq!(one_request, expected);
        assert_eq!(singletons, expected);
        assert_eq!(pairs, expected);
    }

    #[tokio::test]
    async fn snapshot_capture_shares_the_published_view() {
        let (publisher, views) =
            ViewPublisher::new(StateMachine::default(), 0, ViewPublisherConfig::default());
        let (tap, _tap_rx) = EventTap::channel(8);
        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(run(StateMachine::default(), 0, rx, publisher, tap));

        let (reply, reply_rx) = oneshot::channel();
        tx.send(ApplyRequest::Apply {
            entries: vec![(7, bump_command(1))],
            reply,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap();

        let (reply, reply_rx) = oneshot::channel();
        tx.send(ApplyRequest::Snapshot { reply }).await.unwrap();
        let (snapshot, index) = reply_rx.await.unwrap();
        assert_eq!(index, 7);
        assert_eq!(snapshot.cluster_version, 1);

        // Capturing published (or reused) the index-7 view; the snapshot and
        // the read path must be the same allocation, not two clones.
        let view = views.at_least(7).await.unwrap();
        assert!(std::ptr::eq(view.state(), snapshot.as_ref()));

        drop(tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cadence_tick_uses_the_configured_cadence() {
        let config = ViewPublisherConfig {
            cadence: Duration::from_millis(20),
            demand_spacing: Duration::from_millis(1),
        };
        let (publisher, views) = ViewPublisher::new(StateMachine::default(), 0, config);
        let (tap, _tap_rx) = EventTap::channel(8);
        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(run(StateMachine::default(), 0, rx, publisher, tap));

        // Apply right after the post-recovery publish: the batch publish is
        // suppressed (cadence not yet elapsed, no demand registered), so the
        // view can only advance via the routine tick.
        let start = Instant::now();
        let (reply, reply_rx) = oneshot::channel();
        tx.send(ApplyRequest::Apply {
            entries: vec![(3, bump_command(1))],
            reply,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap();

        // Poll `latest` only — `at_least` would register demand and publish
        // early, hiding the cadence. A tick from the configured 20 ms cadence
        // lands well before the 100 ms default the loop once hardcoded.
        while views.latest().applied_index() < 3 {
            assert!(
                start.elapsed() < Duration::from_millis(90),
                "routine publish did not use the configured cadence"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        drop(tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn advance_lets_a_strong_read_at_a_noop_index_resolve() {
        // Regression: a Raft no-op / membership entry never reaches the apply
        // task, so before the fix the published cursor stalled at the last
        // normal command and a strong read whose barrier landed on the no-op
        // index (`read_index` returns the full Raft index) waited forever.
        let (publisher, views) =
            ViewPublisher::new(StateMachine::default(), 0, ViewPublisherConfig::default());
        let (tap, _tap_rx) = EventTap::channel(8);
        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(run(StateMachine::default(), 0, rx, publisher, tap));

        // A normal command at index 5 moves the cursor to 5.
        let (reply, reply_rx) = oneshot::channel();
        tx.send(ApplyRequest::Apply {
            entries: vec![(5, bump_command(1))],
            reply,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap();

        // A strong read whose barrier sits past the last normal command, at a
        // trailing no-op index (6), must not resolve yet.
        let waiter = tokio::spawn({
            let views = views.clone();
            async move { views.at_least(6).await }
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "read at the no-op index resolved early"
        );

        // The no-op at index 6 arrives as an Advance; the cursor moves to 6 and
        // the barrier resolves without any further normal command.
        let (reply, reply_rx) = oneshot::channel();
        tx.send(ApplyRequest::Advance {
            applied_index: 6,
            reply,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap();

        let view = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("read at the no-op index must resolve")
            .expect("join")
            .expect("view");
        assert_eq!(view.applied_index(), 6);
        // State is untouched by the no-op: the bump at index 5 still stands.
        assert_eq!(view.state().cluster_version, 1);

        drop(tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn advance_never_moves_the_cursor_backward() {
        // A stale Advance (index below the current cursor) must be a no-op, not
        // a regression of the published index.
        let (publisher, views) =
            ViewPublisher::new(StateMachine::default(), 0, ViewPublisherConfig::default());
        let (tap, _tap_rx) = EventTap::channel(8);
        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(run(StateMachine::default(), 0, rx, publisher, tap));

        let (reply, reply_rx) = oneshot::channel();
        tx.send(ApplyRequest::Apply {
            entries: vec![(9, bump_command(1))],
            reply,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap();
        assert_eq!(views.at_least(9).await.unwrap().applied_index(), 9);

        let (reply, reply_rx) = oneshot::channel();
        tx.send(ApplyRequest::Advance {
            applied_index: 4,
            reply,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap();
        assert_eq!(views.latest().applied_index(), 9);

        // The internal cursor (not just the published view) must still be 9.
        // A regressed cursor hides behind `maybe_publish` — a publish at 4 is
        // suppressed as stale — but a snapshot capture trusts the cursor, and
        // `state_at` would force-publish at 4 and move the watch backward.
        let (reply, reply_rx) = oneshot::channel();
        tx.send(ApplyRequest::Snapshot { reply }).await.unwrap();
        let (_state, index) = reply_rx.await.unwrap();
        assert_eq!(index, 9);
        assert_eq!(views.latest().applied_index(), 9);

        drop(tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn install_republishes_at_the_new_index() {
        let (publisher, views) =
            ViewPublisher::new(StateMachine::default(), 0, ViewPublisherConfig::default());
        let (tap, _tap_rx) = EventTap::channel(8);
        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(run(StateMachine::default(), 0, rx, publisher, tap));

        let installed = StateMachine {
            version: 55,
            ..StateMachine::default()
        };
        let (reply, reply_rx) = oneshot::channel();
        tx.send(ApplyRequest::Install {
            state: Box::new(installed),
            applied_index: 40,
            reply,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap();

        let view = views.at_least(40).await.unwrap();
        assert_eq!(view.applied_index(), 40);
        assert_eq!(view.version(), 55);

        drop(tx);
        handle.await.unwrap();
    }
}
