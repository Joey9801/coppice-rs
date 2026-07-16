//! The derived event tap (ADR 0008).
//!
//! Events are *derived, not authoritative*: apply produces them as a side
//! output, keyed by the Raft applied index that is their global sequence
//! number. They are not replicated — every replica derives the identical
//! stream deterministically. The apply task feeds batches into an [`EventTap`],
//! the fanout drains an [`EventTapReceiver`].
//!
//! Emission must never block apply. The tap is a bounded channel and emit uses
//! `try_send`; on overflow the batch is **dropped** and the receiver
//! synthesizes a [`TapItem::Gap`] so downstream clients resync (ADR 0008
//! drop-and-gap). Backpressure from a slow consumer therefore turns into
//! dropped events and a resync, never a stalled state machine.
//!
//! A dropped batch is normally exposed when a *later* batch arrives carrying a
//! jumped `seq`. That alone is not enough (KOI-3): if the dropped batch is the
//! last one before an idle period, no later batch ever arrives, and a `Ready`
//! or `StopRequested` event lost that way would wedge dispatch forever. So the
//! sender also raises an out-of-band [`DropSignal`] — a monotonic count of
//! emitted batches plus a `Notify` — and the receiver surfaces a *trailing*
//! gap the moment the channel goes idle with drops outstanding. The same
//! machinery lets [`EventTap::force_gap`] inject the discontinuity a snapshot
//! install creates, where the applied index jumps forward with no events.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio::sync::Notify;

use coppice_core::time::Timestamp;
use coppice_state::Event;

/// The events emitted by applying **one** committed command, in emission
/// order.
///
/// One batch per command is a load-bearing invariant (ADR 0008, KOI-3): the
/// index is the command's own log index, so every replica derives the same
/// batches and cursor positions regardless of how openraft grouped entries
/// into apply requests, and a cursor resume can never split or skip a
/// command's events.
#[derive(Debug, Clone)]
pub struct EventBatch {
    /// The Raft log index of the command that produced these events — the
    /// global sequence cursor of ADR 0008.
    pub applied_index: u64,
    /// The producing command's proposer stamp (`Command::stamped_at`,
    /// ADR 0032): when the proposer asserted the facts these events record.
    /// Advisory only — copied on by the apply loop, never read back by
    /// apply, and never an ordering key (order is `applied_index` plus the
    /// event's position in `events`). Stamps come from different replicas'
    /// clocks, so `at` regressing as the index advances is normal.
    pub at: Timestamp,
    pub events: Vec<Event>,
}

/// Internal channel message: a batch tagged with a dense per-tap sequence so
/// the receiver can detect a dropped batch as a gap in `seq`.
struct Tagged {
    seq: u64,
    batch: EventBatch,
}

/// Out-of-band drop notification shared by sender and receiver.
///
/// The bounded channel cannot carry a "you dropped one" marker when it is
/// exactly full, and a drop that is the *last* thing emitted has no later
/// batch to expose its `seq` jump. `emitted` (the sender's running total of
/// emitted batches, advanced even on a drop) plus `notify` let the receiver
/// detect and surface such a trailing drop instead of blocking forever.
struct DropSignal {
    /// Count of batches the sender has emitted — sent, dropped, or
    /// forced-gap. When the channel is idle and this sits ahead of the
    /// receiver's `expected_seq`, the difference is dropped batches that must
    /// surface as a gap.
    emitted: AtomicU64,
    /// Highest applied index the sender has emitted (sent *or* dropped) or
    /// forced a gap over. A trailing gap must report this as its floor: the
    /// dropped batches reach up to it, and cursors are portable across
    /// replicas (ADR 0008), so any lower floor would admit a cursor inside
    /// the dropped range and replay silently across it.
    last_emitted_index: AtomicU64,
    notify: Notify,
}

/// Apply-side sender.
///
/// Single owner (the apply task); [`EventTap::emit`] is synchronous and
/// never blocks.
pub struct EventTap {
    tx: mpsc::Sender<Tagged>,
    /// Dense counter of *emitted* batches, advanced even when a batch is
    /// dropped so the receiver observes the drop as a `seq` jump.
    seq: u64,
    signal: Arc<DropSignal>,
}

/// Consumer-side receiver, yielding [`TapItem`]s in order with gap markers
/// where batches were dropped.
pub struct EventTapReceiver {
    rx: mpsc::Receiver<Tagged>,
    /// The `seq` the next received batch should carry if none were dropped.
    expected_seq: u64,
    /// Applied index of the last batch actually delivered — the resume point a
    /// synthesized *trailing* gap reports.
    last_index: u64,
    /// A batch received out of sequence, held back so the gap marker is
    /// delivered before it.
    held: Option<EventBatch>,
    signal: Arc<DropSignal>,
}

/// One item drained from the tap.
#[derive(Debug)]
pub enum TapItem {
    /// A batch of events, in order.
    Batch(EventBatch),
    /// One or more batches were dropped, or the applied index jumped on a
    /// snapshot install. The consumer must re-query authoritative state; a
    /// replay is complete only from a cursor at or after `earliest_replayable`
    /// (ADR 0007/0008).
    Gap { earliest_replayable: u64 },
}

impl EventTap {
    /// Create a bounded tap of `capacity` batches and its receiver.
    pub fn channel(capacity: usize) -> (EventTap, EventTapReceiver) {
        let (tx, rx) = mpsc::channel(capacity);
        let signal = Arc::new(DropSignal {
            emitted: AtomicU64::new(0),
            last_emitted_index: AtomicU64::new(0),
            notify: Notify::new(),
        });
        let tap = EventTap {
            tx,
            seq: 0,
            signal: Arc::clone(&signal),
        };
        let receiver = EventTapReceiver {
            rx,
            expected_seq: 0,
            last_index: 0,
            held: None,
            signal,
        };
        (tap, receiver)
    }

    /// Emit a batch.
    ///
    /// Empty batches are skipped. Never blocks: on a full channel the batch
    /// is dropped (the receiver synthesizes a gap); if the receiver is gone
    /// the batch is dropped silently. The sequence counter advances in every
    /// case, which is precisely what lets a drop show up as a gap.
    pub fn emit(&mut self, batch: EventBatch) {
        if batch.events.is_empty() {
            return;
        }
        let applied_index = batch.applied_index;
        let tagged = Tagged {
            seq: self.seq,
            batch,
        };
        self.seq += 1;
        let dropped = matches!(self.tx.try_send(tagged), Err(TrySendError::Full(_)));
        // Publish the new total *after* the send attempt, so a receiver that
        // observes the higher `emitted` (Acquire) also observes the queued item
        // and the index below (the store is Release-ordered behind both).
        self.signal
            .last_emitted_index
            .store(applied_index, Ordering::Relaxed);
        self.signal.emitted.store(self.seq, Ordering::Release);
        if dropped {
            // A drop with no later batch to expose it: wake a parked receiver
            // so it can surface the trailing gap.
            self.signal.notify.notify_one();
        }
    }

    /// Force a discontinuity into the stream without carrying events.
    ///
    /// Used when the state machine jumps forward on a snapshot install: its
    /// applied index skips to `applied_index` over a range for which the
    /// derived stream emitted nothing, so a consumer replaying across the
    /// boundary would do so silently. Implemented as a phantom drop — the
    /// consumed `seq` makes the next batch expose a gap, and if the stream
    /// then idles the trailing-drop path surfaces it, with `applied_index` as
    /// the replay floor.
    pub fn force_gap(&mut self, applied_index: u64) {
        self.seq += 1;
        self.signal
            .last_emitted_index
            .store(applied_index, Ordering::Relaxed);
        self.signal.emitted.store(self.seq, Ordering::Release);
        self.signal.notify.notify_one();
    }
}

impl EventTapReceiver {
    /// Receive the next item.
    ///
    /// Yields a [`TapItem::Gap`] before the first batch that follows a drop,
    /// then the batch itself on the following call; or a standalone gap when a
    /// drop trails an idle period. Returns `None` once the tap is dropped and
    /// fully drained.
    pub async fn recv(&mut self) -> Option<TapItem> {
        if let Some(batch) = self.held.take() {
            self.last_index = batch.applied_index;
            return Some(TapItem::Batch(batch));
        }

        loop {
            // Surface any trailing drop the channel will never expose on its
            // own. Level-triggered (`emitted` vs `expected_seq`), so a notify
            // permit lost to cancellation cannot hide a drop: the next call
            // re-checks here.
            if let Some(item) = self.take_trailing_gap() {
                return Some(item);
            }

            tokio::select! {
                biased;
                maybe = self.rx.recv() => {
                    match maybe {
                        Some(tagged) => return Some(self.deliver(tagged)),
                        // Sender gone. One last look for a trailing drop (the
                        // final batch may have been dropped) before ending.
                        None => return self.take_trailing_gap(),
                    }
                }
                _ = self.signal.notify.notified() => {
                    // Woken by a drop or force_gap; loop to surface it — or to
                    // drain a batch that raced in, whose `seq` jump exposes the
                    // drop precisely.
                }
            }
        }
    }

    /// Turn one received batch into a [`TapItem`], detecting a `seq` jump as a
    /// gap and holding the batch back until the gap is delivered.
    fn deliver(&mut self, tagged: Tagged) -> TapItem {
        let gap_before = tagged.seq != self.expected_seq;
        self.expected_seq = tagged.seq + 1;
        if gap_before {
            // This batch is the first survivor after the hole, so it is the
            // earliest index a replay can resume from without loss.
            let earliest_replayable = tagged.batch.applied_index;
            self.held = Some(tagged.batch);
            TapItem::Gap {
                earliest_replayable,
            }
        } else {
            self.last_index = tagged.batch.applied_index;
            TapItem::Batch(tagged.batch)
        }
    }

    /// If the sender has emitted past what we have accounted for and nothing is
    /// queued, those batches were dropped with no later batch to expose them —
    /// synthesize the trailing gap.
    fn take_trailing_gap(&mut self) -> Option<TapItem> {
        let emitted = self.signal.emitted.load(Ordering::Acquire);
        if emitted <= self.expected_seq {
            return None;
        }
        // A batch may have raced into the channel after the load; deliver it
        // instead, so we never advance `expected_seq` past a queued `seq`.
        match self.rx.try_recv() {
            Ok(tagged) => Some(self.deliver(tagged)),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                // The dropped batches reach up to the sender's last emitted
                // index (visible: the Acquire load above pairs with the
                // Release store behind it), so only a cursor at or past that
                // index has seen everything the drop lost.
                let earliest_replayable = self.signal.last_emitted_index.load(Ordering::Relaxed);
                // Account the drops so this gap does not re-fire until a further
                // drop advances `emitted` again.
                self.expected_seq = emitted;
                Some(TapItem::Gap {
                    earliest_replayable,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(applied_index: u64) -> EventBatch {
        EventBatch {
            applied_index,
            at: Timestamp::UNIX_EPOCH,
            events: vec![Event::PolicyUpdated],
        }
    }

    fn gap_index(item: Option<TapItem>) -> u64 {
        match item {
            Some(TapItem::Gap {
                earliest_replayable,
            }) => earliest_replayable,
            other => panic!("expected gap, got {other:?}"),
        }
    }

    fn batch_index(item: Option<TapItem>) -> u64 {
        match item {
            Some(TapItem::Batch(b)) => b.applied_index,
            other => panic!("expected batch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn overflow_synthesizes_a_gap() {
        let (mut tap, mut rx) = EventTap::channel(1);

        // First batch fills the single slot.
        tap.emit(batch(10));
        // Second batch cannot be queued and is dropped.
        tap.emit(batch(20));
        // Drain the first so there is room again.
        assert_eq!(batch_index(rx.recv().await), 10);
        // Third batch queues, but its seq jumped past the dropped one.
        tap.emit(batch(30));

        // The gap's floor is the first survivor after the hole (batch 30).
        assert_eq!(gap_index(rx.recv().await), 30);
        assert_eq!(batch_index(rx.recv().await), 30);
    }

    /// KOI-3: a drop that trails an idle period must still surface a gap, even
    /// though no later batch ever arrives to expose its `seq` jump.
    #[tokio::test]
    async fn trailing_drop_surfaces_a_gap_without_a_later_batch() {
        let (mut tap, mut rx) = EventTap::channel(1);

        // Deliver one batch so `last_index` advances to 10.
        tap.emit(batch(10));
        assert_eq!(batch_index(rx.recv().await), 10);

        // The slot is empty again; fill it, then drop the next as the *last*
        // thing emitted.
        tap.emit(batch(20));
        tap.emit(batch(30)); // dropped: channel full

        // The queued batch 20 comes first...
        assert_eq!(batch_index(rx.recv().await), 20);
        // ...then the trailing gap surfaces on its own — recv must not block.
        // Its floor is the dropped batch's own index: a cursor anywhere below
        // 30 has not seen batch 30 and must resync.
        assert_eq!(gap_index(rx.recv().await), 30);
    }

    /// A forced gap (snapshot install) surfaces even on an otherwise idle tap.
    #[tokio::test]
    async fn force_gap_surfaces_on_an_idle_tap() {
        let (mut tap, mut rx) = EventTap::channel(4);

        tap.emit(batch(10));
        assert_eq!(batch_index(rx.recv().await), 10);

        tap.force_gap(40); // snapshot install jumped the applied index to 40
                           // No further batch is emitted; the gap must still arrive, with the
                           // install index as its floor.
        assert_eq!(gap_index(rx.recv().await), 40);
    }

    /// A forced gap exposed by the next batch reports that batch as the floor.
    #[tokio::test]
    async fn force_gap_exposed_by_next_batch() {
        let (mut tap, mut rx) = EventTap::channel(4);

        tap.emit(batch(10));
        assert_eq!(batch_index(rx.recv().await), 10);

        tap.force_gap(40);
        tap.emit(batch(50)); // e.g. first command applied after the install

        assert_eq!(gap_index(rx.recv().await), 50);
        assert_eq!(batch_index(rx.recv().await), 50);
    }

    #[tokio::test]
    async fn empty_batches_are_skipped() {
        let (mut tap, mut rx) = EventTap::channel(4);
        tap.emit(EventBatch {
            applied_index: 1,
            at: Timestamp::UNIX_EPOCH,
            events: vec![],
        });
        tap.emit(batch(2));

        assert_eq!(batch_index(rx.recv().await), 2);
    }

    #[tokio::test]
    async fn closes_when_sender_dropped_and_drained() {
        let (mut tap, mut rx) = EventTap::channel(4);
        tap.emit(batch(1));
        drop(tap);

        assert_eq!(batch_index(rx.recv().await), 1);
        assert!(rx.recv().await.is_none());
    }
}
