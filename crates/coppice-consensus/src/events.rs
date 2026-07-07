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

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use coppice_state::Event;

/// The events emitted while applying the commands committed up to
/// `applied_index`, in commit order.
#[derive(Debug, Clone)]
pub struct EventBatch {
    /// The Raft applied log index this batch was produced at — the global
    /// sequence cursor of ADR 0008.
    pub applied_index: u64,
    pub events: Vec<Event>,
}

/// Internal channel message: a batch tagged with a dense per-tap sequence so
/// the receiver can detect a dropped batch as a gap in `seq`.
struct Tagged {
    seq: u64,
    batch: EventBatch,
}

/// Apply-side sender. Single owner (the apply task); [`EventTap::emit`] is
/// synchronous and never blocks.
pub struct EventTap {
    tx: mpsc::Sender<Tagged>,
    /// Dense counter of *emitted* batches, advanced even when a batch is
    /// dropped so the receiver observes the drop as a `seq` jump.
    seq: u64,
}

/// Consumer-side receiver, yielding [`TapItem`]s in order with gap markers
/// where batches were dropped.
pub struct EventTapReceiver {
    rx: mpsc::Receiver<Tagged>,
    /// The `seq` the next received batch should carry if none were dropped.
    expected_seq: u64,
    /// Applied index of the last batch actually delivered — the resume point a
    /// synthesized gap reports.
    last_index: u64,
    /// A batch received out of sequence, held back so the gap marker is
    /// delivered before it.
    held: Option<EventBatch>,
}

/// One item drained from the tap.
pub enum TapItem {
    /// A batch of events, in order.
    Batch(EventBatch),
    /// One or more batches were dropped; the consumer must re-query
    /// authoritative state and resubscribe from `resume_after_index` (ADR
    /// 0007/0008).
    Gap { resume_after_index: u64 },
}

impl EventTap {
    /// Create a bounded tap of `capacity` batches and its receiver.
    pub fn channel(capacity: usize) -> (EventTap, EventTapReceiver) {
        let (tx, rx) = mpsc::channel(capacity);
        let tap = EventTap { tx, seq: 0 };
        let receiver = EventTapReceiver {
            rx,
            expected_seq: 0,
            last_index: 0,
            held: None,
        };
        (tap, receiver)
    }

    /// Emit a batch. Empty batches are skipped. Never blocks: on a full channel
    /// the batch is dropped (the receiver synthesizes a gap); if the receiver
    /// is gone the batch is dropped silently. The sequence counter advances in
    /// every case, which is precisely what lets a drop show up as a gap.
    pub fn emit(&mut self, batch: EventBatch) {
        if batch.events.is_empty() {
            return;
        }
        let tagged = Tagged {
            seq: self.seq,
            batch,
        };
        self.seq += 1;
        match self.tx.try_send(tagged) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                // Dropped: the seq gap becomes a synthesized Gap downstream.
            }
            Err(TrySendError::Closed(_)) => {
                // Receiver gone; nothing to deliver to.
            }
        }
    }
}

impl EventTapReceiver {
    /// Receive the next item. Yields a [`TapItem::Gap`] before the first batch
    /// that follows a drop, then the batch itself on the following call.
    /// Returns `None` once the tap is dropped and fully drained.
    pub async fn recv(&mut self) -> Option<TapItem> {
        if let Some(batch) = self.held.take() {
            self.last_index = batch.applied_index;
            return Some(TapItem::Batch(batch));
        }

        let tagged = self.rx.recv().await?;
        let is_gap = tagged.seq != self.expected_seq;
        self.expected_seq = tagged.seq + 1;

        if is_gap {
            let resume_after_index = self.last_index;
            self.held = Some(tagged.batch);
            Some(TapItem::Gap { resume_after_index })
        } else {
            self.last_index = tagged.batch.applied_index;
            Some(TapItem::Batch(tagged.batch))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(applied_index: u64) -> EventBatch {
        EventBatch {
            applied_index,
            events: vec![Event::PolicyUpdated],
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
        match rx.recv().await {
            Some(TapItem::Batch(b)) => assert_eq!(b.applied_index, 10),
            other => panic!("expected batch, got {:?}", other.map(|_| ())),
        }
        // Third batch queues, but its seq jumped past the dropped one.
        tap.emit(batch(30));

        match rx.recv().await {
            Some(TapItem::Gap { resume_after_index }) => assert_eq!(resume_after_index, 10),
            other => panic!("expected gap, got {:?}", other.map(|_| ())),
        }
        match rx.recv().await {
            Some(TapItem::Batch(b)) => assert_eq!(b.applied_index, 30),
            other => panic!("expected batch, got {:?}", other.map(|_| ())),
        }
    }

    #[tokio::test]
    async fn empty_batches_are_skipped() {
        let (mut tap, mut rx) = EventTap::channel(4);
        tap.emit(EventBatch {
            applied_index: 1,
            events: vec![],
        });
        tap.emit(batch(2));

        match rx.recv().await {
            Some(TapItem::Batch(b)) => assert_eq!(b.applied_index, 2),
            other => panic!("expected batch, got {:?}", other.map(|_| ())),
        }
    }
}
