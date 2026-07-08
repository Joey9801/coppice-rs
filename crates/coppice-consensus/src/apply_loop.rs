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

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use coppice_state::StateMachine;

use crate::adapter::ApplyRequest;
use crate::events::{EventBatch, EventTap};
use crate::view::{ViewPublisher, ViewPublisherConfig};

/// Run the apply loop until the request channel closes.
///
/// `initial_applied_index` is the log index the recovered `state` reflects;
/// the loop republishes it exactly once up front (view.rs requires the true
/// post-recovery index) before entering its `select!`.
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
    // First post-recovery publish at the true index (view.rs contract).
    publisher.publish_now(&state, applied_index);

    // The cadence tick lives here (not in the publisher), so a strong-read
    // barrier resolves even on an idle log; skip missed ticks so a stall does
    // not produce a burst of catch-up publishes.
    let mut cadence = tokio::time::interval(ViewPublisherConfig::default().cadence);
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
                        let mut outcomes = Vec::with_capacity(entries.len());
                        let mut events = Vec::new();
                        for (index, command) in &entries {
                            let outcome = state.apply(command);
                            if let Ok(applied) = &outcome {
                                events.extend(applied.events.iter().cloned());
                            }
                            outcomes.push(outcome);
                            applied_index = *index;
                        }
                        // Order is mandated: apply -> emit events -> publish ->
                        // reply. `emit` skips an empty batch and never blocks.
                        tap.emit(EventBatch { applied_index, events });
                        publisher.maybe_publish(&state, applied_index);
                        // A dropped receiver just means the proposer went away.
                        let _ = reply.send(outcomes);
                    }
                    ApplyRequest::Snapshot { reply } => {
                        let _ = reply.send((Arc::new(state.clone()), applied_index));
                    }
                    ApplyRequest::Install { state: new_state, applied_index: idx, reply } => {
                        state = *new_state;
                        applied_index = idx;
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
    use tokio::sync::oneshot;

    use crate::events::TapItem;
    use crate::view::ViewPublisher;

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
            ViewPublisher::new(StateMachine::default(), ViewPublisherConfig::default());
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

    #[tokio::test]
    async fn install_republishes_at_the_new_index() {
        let (publisher, views) =
            ViewPublisher::new(StateMachine::default(), ViewPublisherConfig::default());
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
