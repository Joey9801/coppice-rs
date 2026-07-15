//! Derived queue stats (every replica) — ADR 0032's tier 3.
//!
//! Counts queue transitions from the event stream into 30 s rolling buckets
//! ([`crate::limits::QUEUE_BUCKET_INTERVAL`] / [`crate::limits::QUEUE_WINDOW_MAX_BUCKETS`])
//! and publishes the closed-bucket window over a `watch` for the overview's
//! queue rates and `history`. Task-local state only: nothing here touches
//! the `StateMachine`, the log, or a snapshot, and a restarted replica
//! serves a partially covered window (honestly absent buckets) until it
//! refills.
//!
//! Coverage is all-or-nothing per window: an event-stream gap (tap
//! overflow, subscriber overflow) means an unknown number of transitions
//! were never counted, so the whole window is dropped and coverage restarts
//! from the gap — a bucket's presence is the claim that its counts are
//! complete. Bucketing counts into the currently open bucket, which is the
//! consumer-side clamp ADR 0032 permits (the `proposer_skew` metric watches
//! how far that clamp reaches).

use std::collections::VecDeque;

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use coppice_api::{QueueBucket, QueueWindow};
use coppice_consensus::StateViews;
use coppice_core::job::JobState;
use coppice_state::Event;

use crate::limits::{QUEUE_BUCKET_INTERVAL, QUEUE_WINDOW_MAX_BUCKETS};
use crate::tasks::event_fanout::{EventFilter, FanoutHandle, SubscriptionItem};

/// The rolling window plus the bucket currently being filled.
///
/// Pure accounting, separated from the async loop so tests drive it with
/// explicit clocks and events.
struct WindowState {
    /// Closed buckets, oldest first, bounded by [`QUEUE_WINDOW_MAX_BUCKETS`].
    closed: VecDeque<QueueBucket>,
    /// Start of the open bucket, Unix µs.
    open_start_us: i64,
    open_arrivals: u32,
    open_drains: u32,
}

impl WindowState {
    fn new(now_us: i64) -> Self {
        WindowState {
            closed: VecDeque::new(),
            open_start_us: now_us,
            open_arrivals: 0,
            open_drains: 0,
        }
    }

    /// Count one event's queue transitions into the open bucket.
    fn observe(&mut self, event: &Event) {
        if let Event::JobStateChanged { from, to, .. } = event {
            if matches!(to, JobState::Queued) {
                self.open_arrivals += 1;
            }
            if matches!(from, JobState::Queued) {
                self.open_drains += 1;
            }
        }
    }

    /// Close the open bucket with the depth sampled at close time and open
    /// the next one at `now_us`.
    fn close_bucket(&mut self, depth: u32, now_us: i64) {
        self.closed.push_back(QueueBucket {
            start_us: self.open_start_us,
            depth,
            arrivals: self.open_arrivals,
            drains: self.open_drains,
        });
        while self.closed.len() > QUEUE_WINDOW_MAX_BUCKETS {
            self.closed.pop_front();
        }
        self.open_start_us = now_us;
        self.open_arrivals = 0;
        self.open_drains = 0;
    }

    /// Drop everything: coverage was lost, and a partially counted window
    /// would be indistinguishable from a complete one. Restart from `now_us`.
    fn reset(&mut self, now_us: i64) {
        self.closed.clear();
        self.open_start_us = now_us;
        self.open_arrivals = 0;
        self.open_drains = 0;
    }

    fn window(&self) -> QueueWindow {
        QueueWindow {
            bucket_us: QUEUE_BUCKET_INTERVAL.as_micros() as i64,
            buckets: self.closed.iter().copied().collect(),
        }
    }
}

/// Jobs currently in `Queued`, from the latest published view.
fn sample_depth(views: &StateViews) -> u32 {
    let view = views.latest();
    view.state()
        .jobs
        .iter()
        .filter(|(_, record)| matches!(record.state, JobState::Queued))
        .count()
        .try_into()
        .unwrap_or(u32::MAX)
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Spawn the derived-stats task.
///
/// Returns the watch the API's `queue_window` read serves from (seeded
/// empty: no closed bucket exists until the first interval elapses) and the
/// task's `JoinHandle`.
pub fn spawn(
    fanout: FanoutHandle,
    views: StateViews,
    shutdown: watch::Receiver<bool>,
) -> (watch::Receiver<QueueWindow>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = watch::channel(QueueWindow {
        bucket_us: QUEUE_BUCKET_INTERVAL.as_micros() as i64,
        buckets: Vec::new(),
    });
    let join = tokio::spawn(run(fanout, views, tx, shutdown));
    (rx, join)
}

async fn run(
    fanout: FanoutHandle,
    views: StateViews,
    tx: watch::Sender<QueueWindow>,
    mut shutdown: watch::Receiver<bool>,
) {
    // No cursor: counting starts from "now". The window's coverage is
    // whatever this subscription actually delivers, which is exactly what
    // the bucket presence/absence vocabulary reports.
    let Ok(mut subscription) = fanout.subscribe(EventFilter::All, None).await else {
        // Fanout is gone — shutdown in disguise; the watch stays empty.
        return;
    };

    let mut state = WindowState::new(now_us());

    // Skip missed ticks: a stalled loop should close one (long) bucket, not
    // burst out a run of empty ones with fabricated timestamps.
    let mut tick = tokio::time::interval(QUEUE_BUCKET_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick.reset(); // the first tick fires after one full interval, not at once

    loop {
        tokio::select! {
            biased;
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            item = subscription.items.recv() => {
                match item {
                    Some(SubscriptionItem::Events(batch)) => {
                        for (_ordinal, event) in &batch.events {
                            state.observe(event);
                        }
                    }
                    Some(SubscriptionItem::Gap { earliest_available }) => {
                        // An unknown span of transitions was never counted;
                        // every bucket's completeness claim is void.
                        tracing::info!(
                            earliest_available,
                            "derived stats: event stream gap, dropping the queue window"
                        );
                        state.reset(now_us());
                        let _ = tx.send(state.window());
                    }
                    // Fanout shut down; nothing further will arrive.
                    None => break,
                }
            }
            _ = tick.tick() => {
                state.close_bucket(sample_depth(&views), now_us());
                let _ = tx.send(state.window());
            }
        }
    }
    tracing::debug!("derived stats shutting down");
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::id::JobId;

    fn queued_transition(to_queued: bool) -> Event {
        let job = JobId::new();
        if to_queued {
            Event::JobStateChanged {
                job,
                from: JobState::Accepted,
                to: JobState::Queued,
            }
        } else {
            Event::JobStateChanged {
                job,
                from: JobState::Queued,
                to: JobState::Succeeded,
            }
        }
    }

    #[test]
    fn counts_arrivals_and_drains_into_the_open_bucket() {
        let mut state = WindowState::new(1_000);
        state.observe(&queued_transition(true));
        state.observe(&queued_transition(true));
        state.observe(&queued_transition(false));
        // A non-queue transition counts as neither.
        state.observe(&Event::JobStateChanged {
            job: JobId::new(),
            from: JobState::Submitted,
            to: JobState::Accepted,
        });

        state.close_bucket(7, 31_000);
        let window = state.window();
        assert_eq!(window.buckets.len(), 1);
        let bucket = window.buckets[0];
        assert_eq!(bucket.start_us, 1_000);
        assert_eq!(bucket.arrivals, 2);
        assert_eq!(bucket.drains, 1);
        assert_eq!(bucket.depth, 7);

        // The next bucket opens at the close time with fresh counts.
        state.observe(&queued_transition(true));
        state.close_bucket(8, 61_000);
        let window = state.window();
        assert_eq!(window.buckets[1].start_us, 31_000);
        assert_eq!(window.buckets[1].arrivals, 1);
        assert_eq!(window.buckets[1].drains, 0);
    }

    #[test]
    fn window_is_bounded_by_evicting_the_oldest_bucket() {
        let mut state = WindowState::new(0);
        for i in 0..(QUEUE_WINDOW_MAX_BUCKETS + 3) {
            state.close_bucket(0, (i as i64 + 1) * 30_000_000);
        }
        let window = state.window();
        assert_eq!(window.buckets.len(), QUEUE_WINDOW_MAX_BUCKETS);
        // Oldest three were evicted: the window starts at the fourth bucket.
        assert_eq!(window.buckets[0].start_us, 3 * 30_000_000);
    }

    /// The task end to end under virtual time: events counted off a real
    /// fanout subscription, depth sampled from the published view, and the
    /// closed bucket published on the watch after one interval.
    #[tokio::test(start_paused = true)]
    async fn task_publishes_closed_buckets_from_the_event_stream() {
        use coppice_consensus::{EventBatch, EventTap, ViewPublisher, ViewPublisherConfig};
        use coppice_core::resource::Resources;

        let (mut tap, tap_rx) = EventTap::channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, _fanout_join) =
            crate::tasks::event_fanout::spawn(tap_rx, 0, shutdown_rx.clone());

        // One queued job in the published view — the depth sample at close.
        let job = JobId::new();
        let mut state = coppice_state::StateMachine::default();
        state.jobs.insert(
            job,
            crate::test_support::job_record(job, "busybox", Resources::ZERO, None),
        );
        let (_publisher, views) = ViewPublisher::new(state, 1, ViewPublisherConfig::default());

        let (mut window_rx, _join) = spawn(fanout, views, shutdown_rx);
        // Let the task's subscription register before events flow (a fresh
        // subscription only covers from registration).
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        tap.emit(EventBatch {
            applied_index: 2,
            at_us: 1_000,
            events: vec![queued_transition(true), queued_transition(false)],
        });

        // Virtual time: the first interval elapses, the bucket closes, and
        // the window publishes.
        window_rx.changed().await.expect("task publishes a window");
        let window = window_rx.borrow().clone();
        assert_eq!(window.bucket_us, QUEUE_BUCKET_INTERVAL.as_micros() as i64);
        assert_eq!(window.buckets.len(), 1);
        assert_eq!(window.buckets[0].arrivals, 1);
        assert_eq!(window.buckets[0].drains, 1);
        assert_eq!(window.buckets[0].depth, 1);
    }

    /// ADR 0032's honest-gap rule for tier 3: a gap voids every bucket's
    /// completeness claim, so the window empties rather than serving counts
    /// that silently miss a span.
    #[test]
    fn a_gap_drops_the_whole_window() {
        let mut state = WindowState::new(0);
        state.observe(&queued_transition(true));
        state.close_bucket(1, 30_000_000);
        assert_eq!(state.window().buckets.len(), 1);

        state.reset(45_000_000);
        assert!(state.window().buckets.is_empty());
        // Counting restarts cleanly from the gap.
        state.observe(&queued_transition(true));
        state.close_bucket(1, 75_000_000);
        let window = state.window();
        assert_eq!(window.buckets.len(), 1);
        assert_eq!(window.buckets[0].start_us, 45_000_000);
        assert_eq!(window.buckets[0].arrivals, 1);
    }
}
