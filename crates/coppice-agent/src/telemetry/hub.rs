//! The telemetry hub: fan-out from the collectors to the configured sinks
//! (docker-executor.md §8.3).
//!
//! The [`TelemetryHub`] sits between the Docker collectors (which append whole
//! batches) and the [`MetricsSink`]/[`LogSink`] instances (which persist them).
//! Each configured sink instance gets its **own** bounded queue and one
//! dedicated drain task, so a slow sink can never backpressure container
//! execution or another sink (§8.3). A batch is the queue's unit — the queue
//! holds `queue_depth` *batches* (§8.3 "1024 batches", config default), not rows
//! — and a full queue **drops its oldest** entry: that is a failure mode, not a
//! policy, so every drop bumps an error-level counter and the first drop of a
//! streak warns (steady-state loss is a defect signal; loss is sanctioned only
//! in a crash, §8.4).
//!
//! **Delivery is at-most-once, hub→sink, in process** (§8.3): a batch a drain
//! task has popped but not yet persisted is lost if the process dies, and the
//! hub never retries — the sink's `append` is already infallible at its own
//! boundary, and end-to-end at-least-once log delivery is reconstructed on
//! restart from the filesystem sink, the local source of truth (§8.2, §8.4).
//!
//! [`flush`](TelemetryHub::flush) resolves once every queue is empty *and* no
//! delivery is in flight; the container-reap barrier awaits it (bounded by a
//! timeout) to order "hub drained" before the attempt-ended marker (§8.4).
//!
//! Like the filesystem sink and the image cache, [`TelemetryHub`] is a cheap
//! `Clone` handle over a shared `Arc<HubInner>`; the drain tasks capture only
//! per-queue `Arc`s, so dropping the **last** hub handle aborts every drain task
//! (the `Inner`-drop-aborts pattern from
//! [`executor::docker`](crate::executor::docker)).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

use super::sink::{LogSink, MetricsSink, SinkKind};
use super::{LogChunk, MetricSample, AGENT_TELEMETRY_SINK_DROPPED_BATCHES};

// ---- public surface (docker-executor.md §8.3) ---------------------------

/// One sink instance the hub fans out to (docker-executor.md §8.3). A
/// compile-time registry: future sink types (`clickhouse`, `loki`, …) are new
/// arms plus their trait impls, mirroring the [`SinkConfig`](super::SinkConfig)
/// enum they are built from.
pub enum SinkInstance {
    /// The v1 [`FilesystemSink`](super::FilesystemSink): the local source of
    /// truth (§8.4).
    Filesystem(super::FilesystemSink),
    /// A test double that records delivered batches and can gate delivery, so
    /// the queue mechanics (drop-oldest, order, flush) are testable
    /// deterministically without a real sink. A `#[cfg(test)]` enum arm is a
    /// precedented Rust test seam; it compiles out of every real build.
    #[cfg(test)]
    Test(TestSink),
}

impl SinkInstance {
    /// A short human name for the drop warning (docker-executor.md §8.3).
    fn type_name(&self) -> &'static str {
        match self {
            SinkInstance::Filesystem(_) => "filesystem",
            #[cfg(test)]
            SinkInstance::Test(_) => "test",
        }
    }
}

/// One configured sink instance plus which streams it consumes
/// (docker-executor.md §8.3). Built from a [`SinkConfig`](super::SinkConfig)
/// entry; `kinds` is that entry's validated, non-empty stream set.
pub struct HubSink {
    /// The sink instance itself.
    pub sink: SinkInstance,
    /// The streams routed to it. A batch of a kind not listed here is never
    /// enqueued to this instance in the first place (routing happens at append).
    pub kinds: Vec<SinkKind>,
}

/// The fan-out hub (docker-executor.md §8.3). A cheap `Clone` handle; clones
/// share one `Arc<HubInner>`, and dropping the **last** handle aborts every
/// drain task.
#[derive(Clone)]
pub struct TelemetryHub {
    inner: Arc<HubInner>,
}

impl TelemetryHub {
    /// Build the hub (docker-executor.md §8.3): give each sink instance a bounded
    /// queue of `queue_depth` batches (config default 1024) and spawn one
    /// dedicated drain task per instance. The tasks capture only per-queue
    /// `Arc`s, never the hub, so [`Drop for HubInner`](HubInner) is what stops
    /// them.
    pub fn new(sinks: Vec<HubSink>, queue_depth: usize) -> TelemetryHub {
        let mut queues = Vec::with_capacity(sinks.len());
        let mut tasks = Vec::with_capacity(sinks.len());
        for (index, hub_sink) in sinks.into_iter().enumerate() {
            // Initialised to 0 pending (empty queue, nothing in flight), so a
            // `flush` on an idle hub resolves at once.
            let (pending, _) = watch::channel(0u64);
            let queue = Arc::new(SinkQueue {
                sink: hub_sink.sink,
                kinds: hub_sink.kinds,
                index,
                depth: queue_depth,
                state: Mutex::new(QueueState {
                    queue: VecDeque::new(),
                    in_flight: false,
                }),
                notify: Notify::new(),
                pending,
                drop_warned: AtomicBool::new(false),
            });
            tasks.push(tokio::spawn(drain(Arc::clone(&queue))));
            queues.push(queue);
        }
        TelemetryHub {
            inner: Arc::new(HubInner { queues, tasks }),
        }
    }

    /// Whether **any** configured sink instance consumes `kind` (docker-executor.md
    /// §8.3). The collectors consult this so a config with no logs consumer spawns
    /// no follower, and one with no metrics consumer spawns no sampler — a stream
    /// nobody stores is never collected in the first place, rather than followed
    /// and polled only for the hub to discard every batch.
    pub fn consumes(&self, kind: SinkKind) -> bool {
        self.inner
            .queues
            .iter()
            .any(|queue| queue.kinds.contains(&kind))
    }

    /// Enqueue one metrics batch to every sink instance whose `kinds` include
    /// [`SinkKind::Metrics`] (docker-executor.md §8.3). Non-blocking; a full queue
    /// drops its oldest entry (error-level counter + rate-limited warn). An empty
    /// batch or an empty routing set is a cheap no-op — no clone is made.
    pub fn append_metrics(&self, batch: Vec<MetricSample>) {
        if batch.is_empty() {
            return;
        }
        self.dispatch(SinkKind::Metrics, Batch::Metrics(batch));
    }

    /// Enqueue one logs batch to every sink instance whose `kinds` include
    /// [`SinkKind::Logs`] (docker-executor.md §8.3). The logs mirror of
    /// [`append_metrics`](Self::append_metrics).
    pub fn append_logs(&self, batch: Vec<LogChunk>) {
        if batch.is_empty() {
            return;
        }
        self.dispatch(SinkKind::Logs, Batch::Logs(batch));
    }

    /// Route one batch to every consuming queue, cloning into all but the last
    /// and moving into it — so the common single-consumer case makes zero clones,
    /// and no consumers makes none at all (the batch is simply dropped).
    fn dispatch(&self, kind: SinkKind, batch: Batch) {
        let mut targets = self
            .inner
            .queues
            .iter()
            .filter(|queue| queue.kinds.contains(&kind));
        let Some(mut current) = targets.next() else {
            return;
        };
        for next in targets {
            enqueue(current, batch.clone());
            current = next;
        }
        enqueue(current, batch);
    }

    /// Resolve once every queue is empty **and** no delivery is in flight
    /// (docker-executor.md §8.3, §8.4). The reap barrier awaits this to order
    /// "hub drained" before the attempt-ended marker; callers bound it with
    /// [`tokio::time::timeout`], because a wedged sink would otherwise never
    /// let it complete.
    pub async fn flush(&self) {
        for queue in &self.inner.queues {
            let mut pending = queue.pending.subscribe();
            loop {
                if *pending.borrow_and_update() == 0 {
                    break;
                }
                // The sender lives as long as the hub; an `Err` means the hub is
                // being torn down, so there is nothing left to drain.
                if pending.changed().await.is_err() {
                    break;
                }
            }
        }
    }
}

// ---- internals ----------------------------------------------------------

/// One enqueued unit (docker-executor.md §8.3): a whole batch of one stream. The
/// queue counts these, never rows, so drop-oldest sheds exactly one batch.
#[derive(Clone)]
enum Batch {
    Metrics(Vec<MetricSample>),
    Logs(Vec<LogChunk>),
}

/// The mutable per-queue state behind the std mutex. `pending == queue.len() +
/// in_flight`; both fields change only under the lock, and the lock is never held
/// across an await, so the critical sections stay tiny.
struct QueueState {
    queue: VecDeque<Batch>,
    /// Whether the drain task currently holds a popped batch it has not finished
    /// delivering — the "in flight" a `flush` must also wait out.
    in_flight: bool,
}

/// One sink instance's queue and the coordination its drain task needs
/// (docker-executor.md §8.3). Shared behind an `Arc` between the hub and its
/// drain task.
struct SinkQueue {
    sink: SinkInstance,
    kinds: Vec<SinkKind>,
    /// The instance's position in the configured list, named in the drop warning.
    index: usize,
    /// Capacity in batches (§8.3 default 1024). A push into a full queue drops the
    /// oldest.
    depth: usize,
    state: Mutex<QueueState>,
    /// Wakes the drain task when a batch is pushed onto an empty queue.
    notify: Notify,
    /// Publishes `queue.len() + in_flight` so [`flush`](TelemetryHub::flush) can
    /// await it reaching 0.
    pending: watch::Sender<u64>,
    /// Warn-once latch for the drop path (the fs sink's `write_error_logged`
    /// idiom): the first drop of a streak warns, the rest only bump the counter,
    /// and a non-dropping push resets it — so a wedged queue is metered, not a
    /// log flood.
    drop_warned: AtomicBool,
}

/// The shared guts behind every [`TelemetryHub`] clone. Owns the per-sink queues
/// and their drain-task handles.
struct HubInner {
    queues: Vec<Arc<SinkQueue>>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for HubInner {
    fn drop(&mut self) {
        // The drain tasks hold only per-queue `Arc`s, so these aborts are the sole
        // thing keeping them alive — dropping the last hub handle stops them (the
        // executor/docker `Inner`-drop pattern).
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Publish the current pending count (`queue.len() + in_flight`) for `flush`.
/// `send_replace` because there may be no active `flush` subscriber, and a
/// same-value republish only costs a harmless spurious wakeup that re-checks the
/// count.
fn publish(pending: &watch::Sender<u64>, state: &QueueState) {
    pending.send_replace(state.queue.len() as u64 + u64::from(state.in_flight));
}

/// Push one batch, dropping the oldest first if the queue is full
/// (docker-executor.md §8.3). Every drop bumps the error-level counter; the first
/// drop of a streak also warns, and any non-dropping push resets the latch.
fn enqueue(queue: &SinkQueue, batch: Batch) {
    let dropped = {
        let mut state = queue.state.lock().expect("hub queue mutex poisoned");
        let dropped = state.queue.len() >= queue.depth;
        if dropped {
            state.queue.pop_front();
        }
        state.queue.push_back(batch);
        publish(&queue.pending, &state);
        dropped
    };
    if dropped {
        metrics::counter!(AGENT_TELEMETRY_SINK_DROPPED_BATCHES).increment(1);
        if !queue.drop_warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                sink = queue.sink.type_name(),
                index = queue.index,
                depth = queue.depth,
                "telemetry hub queue full; dropping oldest batch (§8.3)"
            );
        }
    } else {
        queue.drop_warned.store(false, Ordering::Relaxed);
    }
    // A batch is now waiting; wake the drain task if it is parked. `notify_one`
    // stores a permit when no waiter is parked, so a wake is never missed.
    queue.notify.notify_one();
}

/// One sink instance's drain loop (docker-executor.md §8.3): pop the oldest
/// batch and deliver it, or park on the notify when the queue is empty. Setting
/// `in_flight` under the same lock as the pop keeps the pending count from ever
/// dipping to 0 while a batch is still being delivered, so `flush` cannot resolve
/// early. The loop ends only when the task is aborted on hub drop.
async fn drain(queue: Arc<SinkQueue>) {
    loop {
        let next = {
            let mut state = queue.state.lock().expect("hub queue mutex poisoned");
            match state.queue.pop_front() {
                Some(batch) => {
                    state.in_flight = true;
                    publish(&queue.pending, &state);
                    Some(batch)
                }
                None => None,
            }
        };
        match next {
            Some(batch) => {
                dispatch_to_sink(&queue.sink, batch).await;
                let mut state = queue.state.lock().expect("hub queue mutex poisoned");
                state.in_flight = false;
                publish(&queue.pending, &state);
            }
            // Empty: park until the next push. If a push raced between the pop
            // check and here, `notify_one` already stored a permit, so this
            // returns immediately and the loop drains it.
            None => queue.notify.notified().await,
        }
    }
}

/// Deliver one batch to one sink instance (docker-executor.md §8.3). The sink's
/// `append` is infallible at its boundary, so there is nothing to retry and no
/// error to surface — at-most-once, hub→sink, in process.
async fn dispatch_to_sink(sink: &SinkInstance, batch: Batch) {
    match sink {
        SinkInstance::Filesystem(fs) => match batch {
            Batch::Metrics(samples) => MetricsSink::append(fs, &samples).await,
            Batch::Logs(chunks) => LogSink::append(fs, &chunks).await,
        },
        #[cfg(test)]
        SinkInstance::Test(test) => test.deliver(batch).await,
    }
}

// ---- test double --------------------------------------------------------

/// A test sink (docker-executor.md §8.3): records every delivered batch to an
/// unbounded channel and gates delivery on a `watch<bool>`, so a test can hold
/// delivery shut, drive the queue into its drop-oldest path deterministically,
/// then open the gate and observe exactly what survived, in order.
#[cfg(test)]
#[derive(Clone)]
pub struct TestSink {
    delivered: tokio::sync::mpsc::UnboundedSender<Batch>,
    gate: watch::Receiver<bool>,
}

#[cfg(test)]
impl TestSink {
    /// Wait for the gate to open, then record the batch. When the last hub handle
    /// drops, the drain task is aborted and this sink (and its sender) drops with
    /// it, closing the receiver — how a test observes the drain task ending.
    async fn deliver(&self, batch: Batch) {
        let mut gate = self.gate.clone();
        while !*gate.borrow_and_update() {
            if gate.changed().await.is_err() {
                return;
            }
        }
        let _ = self.delivered.send(batch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{
        FilesystemSink, FilesystemSinkOptions, LogQuery, LogStream, StoredLogChunk,
    };
    use coppice_core::id::{AllocationId, AttemptId, JobId};
    use coppice_core::time::{Duration as CoreDuration, Timestamp};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use std::path::PathBuf;
    use std::time::Duration as StdDuration;
    use tempfile::TempDir;
    use tokio::sync::mpsc::{self, UnboundedReceiver};

    // Hand-built timestamps the fs_sink/pressure way: no clock, just
    // `UNIX_EPOCH + Duration`. Doubles as a batch tag in the delivery-order and
    // drop-oldest tests (one sample per batch, keyed by `at`).
    fn at(secs: i64) -> Timestamp {
        Timestamp::UNIX_EPOCH + CoreDuration::from_secs(secs)
    }

    fn metric(at: Timestamp) -> MetricSample {
        MetricSample {
            allocation: AllocationId::new(),
            attempt: AttemptId::new(),
            job: JobId::new(),
            at,
            cpu_usage_total: CoreDuration::from_secs(1),
            cpu_throttled_total: CoreDuration::ZERO,
            memory_used_bytes: 100,
            memory_peak_bytes: 200,
            disk_writable_bytes: 10,
            disk_image_bytes: 20,
            net_rx_bytes_total: 1,
            net_tx_bytes_total: 2,
            blkio_read_bytes_total: 3,
            blkio_write_bytes_total: 4,
        }
    }

    fn log_chunk(at: Timestamp, bytes: &[u8]) -> LogChunk {
        LogChunk {
            allocation: AllocationId::new(),
            attempt: AttemptId::new(),
            job: JobId::new(),
            at,
            stream: LogStream::Stdout,
            bytes: bytes::Bytes::copy_from_slice(bytes),
        }
    }

    /// A test sink plus its gate opener and delivery receiver. `open` gates
    /// delivery in (`true`) or shut (`false`); a shut gate lets a test fill the
    /// queue synchronously before any batch is consumed.
    fn test_sink(open: bool) -> (TestSink, watch::Sender<bool>, UnboundedReceiver<Batch>) {
        let (delivered, rx) = mpsc::unbounded_channel();
        let (gate_tx, gate) = watch::channel(open);
        (TestSink { delivered, gate }, gate_tx, rx)
    }

    /// The `at` tag of a delivered metrics batch's sole sample.
    fn metric_tag(batch: &Batch) -> i64 {
        match batch {
            Batch::Metrics(samples) => samples[0].at.as_micros(),
            Batch::Logs(_) => panic!("expected a metrics batch"),
        }
    }

    /// The `at` tag of a delivered logs batch's sole chunk.
    fn log_tag(batch: &Batch) -> i64 {
        match batch {
            Batch::Logs(chunks) => chunks[0].at.as_micros(),
            Batch::Metrics(_) => panic!("expected a logs batch"),
        }
    }

    /// Drain everything currently buffered in a delivery receiver.
    fn collect(rx: &mut UnboundedReceiver<Batch>) -> Vec<Batch> {
        let mut out = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            out.push(batch);
        }
        out
    }

    /// The value of a counter in a debugging snapshot, or 0 if never touched.
    fn counter_value(snapshotter: &Snapshotter, name: &str) -> u64 {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(key, _unit, _desc, value)| (key.key().name() == name).then_some(value))
            .map(|value| match value {
                DebugValue::Counter(n) => n,
                other => panic!("expected a counter, got {other:?}"),
            })
            .unwrap_or(0)
    }

    async fn fs_sink(root: PathBuf) -> FilesystemSink {
        FilesystemSink::new(FilesystemSinkOptions::new(root))
            .await
            .expect("build filesystem sink")
    }

    // ---- 1. routing by kind (docker-executor.md §8.3) ----------------------

    #[tokio::test]
    async fn batches_route_only_to_instances_whose_kinds_match() {
        let (metrics_sink, _mg, mut metrics_rx) = test_sink(true);
        let (logs_sink, _lg, mut logs_rx) = test_sink(true);
        let hub = TelemetryHub::new(
            vec![
                HubSink {
                    sink: SinkInstance::Test(metrics_sink),
                    kinds: vec![SinkKind::Metrics],
                },
                HubSink {
                    sink: SinkInstance::Test(logs_sink),
                    kinds: vec![SinkKind::Logs],
                },
            ],
            16,
        );

        hub.append_metrics(vec![metric(at(1))]);
        hub.append_logs(vec![log_chunk(at(2), b"x")]);
        tokio::time::timeout(StdDuration::from_secs(5), hub.flush())
            .await
            .expect("flush resolves");

        let to_metrics = collect(&mut metrics_rx);
        assert_eq!(
            to_metrics.len(),
            1,
            "metrics sink gets exactly its one batch"
        );
        assert!(matches!(to_metrics[0], Batch::Metrics(_)));

        let to_logs = collect(&mut logs_rx);
        assert_eq!(to_logs.len(), 1, "logs sink gets exactly its one batch");
        assert!(
            matches!(to_logs[0], Batch::Logs(_)),
            "a logs-only sink never sees a metrics batch"
        );
    }

    // ---- 2. per-sink ordering (docker-executor.md §8.3) --------------------

    #[tokio::test]
    async fn delivery_preserves_enqueue_order() {
        let (sink, _gate, mut rx) = test_sink(true);
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Test(sink),
                kinds: vec![SinkKind::Metrics],
            }],
            16,
        );
        for i in 0..8 {
            hub.append_metrics(vec![metric(at(i))]);
        }
        tokio::time::timeout(StdDuration::from_secs(5), hub.flush())
            .await
            .expect("flush resolves");

        let tags: Vec<i64> = collect(&mut rx).iter().map(metric_tag).collect();
        let expected: Vec<i64> = (0..8).map(|i| at(i).as_micros()).collect();
        assert_eq!(tags, expected, "batches deliver in enqueue order");
    }

    // ---- 3. drop-oldest under a full queue (docker-executor.md §8.3) --------

    #[tokio::test]
    async fn full_queue_drops_oldest_keeping_the_newest_in_order() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // Gate delivery shut so the drain task consumes nothing while we push:
        // `append_metrics` is synchronous and never yields, so on the
        // current-thread runtime all six pushes land before any drain runs.
        let (sink, gate, mut rx) = test_sink(false);
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Test(sink),
                kinds: vec![SinkKind::Metrics],
            }],
            4,
        );

        metrics::with_local_recorder(&recorder, || {
            for i in 0..6 {
                hub.append_metrics(vec![metric(at(i))]);
            }
        });
        assert_eq!(
            counter_value(&snapshotter, AGENT_TELEMETRY_SINK_DROPPED_BATCHES),
            2,
            "6 batches into a depth-4 queue drops the 2 oldest"
        );

        // Open the gate and let the survivors drain: the newest 4, in order.
        gate.send(true).unwrap();
        tokio::time::timeout(StdDuration::from_secs(5), hub.flush())
            .await
            .expect("flush resolves once the gate opens");
        let tags: Vec<i64> = collect(&mut rx).iter().map(metric_tag).collect();
        let expected: Vec<i64> = (2..6).map(|i| at(i).as_micros()).collect();
        assert_eq!(tags, expected, "survivors are the newest 4, in order");
    }

    // ---- 4. flush semantics (docker-executor.md §8.3, §8.4) ----------------

    #[tokio::test]
    async fn flush_is_immediate_when_idle_and_blocks_until_drained() {
        // Idle hub: flush resolves at once.
        let (idle_sink, _g, _rx) = test_sink(true);
        let idle = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Test(idle_sink),
                kinds: vec![SinkKind::Metrics],
            }],
            16,
        );
        tokio::time::timeout(StdDuration::from_millis(500), idle.flush())
            .await
            .expect("an idle flush resolves immediately");

        // Gated-shut hub: flush stays pending until the gate opens and drains.
        let (sink, gate, _rx) = test_sink(false);
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Test(sink),
                kinds: vec![SinkKind::Metrics],
            }],
            16,
        );
        hub.append_metrics(vec![metric(at(1))]);
        assert!(
            tokio::time::timeout(StdDuration::from_millis(300), hub.flush())
                .await
                .is_err(),
            "flush must not resolve while a batch is undelivered"
        );
        gate.send(true).unwrap();
        tokio::time::timeout(StdDuration::from_secs(5), hub.flush())
            .await
            .expect("flush resolves once the gate opens and the queue drains");
    }

    // ---- 5a. end-to-end into a real filesystem sink (docker-executor.md §8.4) -

    #[tokio::test]
    async fn appended_batches_land_in_a_real_filesystem_store() {
        let root = TempDir::new().unwrap();
        let sink = fs_sink(root.path().join("tel")).await;
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Filesystem(sink.clone()),
                kinds: vec![SinkKind::Metrics, SinkKind::Logs],
            }],
            16,
        );
        let (job, attempt, alloc) = (JobId::new(), AttemptId::new(), AllocationId::new());
        let sample = MetricSample {
            job,
            attempt,
            allocation: alloc,
            ..metric(at(1))
        };
        let chunk = LogChunk {
            job,
            attempt,
            allocation: alloc,
            ..log_chunk(at(2), b"hello")
        };
        hub.append_metrics(vec![sample]);
        hub.append_logs(vec![chunk]);
        tokio::time::timeout(StdDuration::from_secs(5), hub.flush())
            .await
            .expect("flush resolves");

        let metrics = sink
            .metric_samples(
                &job,
                &attempt,
                Timestamp::UNIX_EPOCH,
                Timestamp::max_value(),
            )
            .await
            .unwrap();
        assert_eq!(metrics.len(), 1, "the metrics batch persisted");
        let logs: Vec<StoredLogChunk> = sink
            .log_chunks(&job, &attempt, None, LogQuery::Tail { n: 8 })
            .await
            .unwrap();
        assert_eq!(logs.len(), 1, "the logs batch persisted");
        assert_eq!(logs[0].bytes.as_ref(), b"hello");
    }

    // ---- 5b. dropping the last handle ends the drain tasks (§8.3) -----------

    #[tokio::test]
    async fn dropping_the_last_handle_ends_the_drain_task() {
        let (sink, _gate, mut rx) = test_sink(true);
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Test(sink),
                kinds: vec![SinkKind::Metrics],
            }],
            16,
        );
        // Idle, so no buffered deliveries precede the close.
        drop(hub);
        // The abort drops the task, its `Arc<SinkQueue>`, the test sink, and its
        // sender — closing the receiver (the fs-sink janitor's shutdown check).
        let closed = tokio::time::timeout(StdDuration::from_secs(5), rx.recv())
            .await
            .expect("the drain task ends promptly once the hub drops");
        assert!(
            closed.is_none(),
            "the sink's sender dropped, so the drain task is gone"
        );
    }

    // ---- 6. the drop latch resets between full-queue streaks (§8.3) --------

    #[tokio::test]
    async fn a_new_full_queue_episode_counts_again_after_the_latch_resets() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let (sink, gate, _rx) = test_sink(false);
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Test(sink),
                kinds: vec![SinkKind::Metrics],
            }],
            2,
        );

        metrics::with_local_recorder(&recorder, || {
            // First streak: depth 2, push 4 → 2 drops, latch set.
            for i in 0..4 {
                hub.append_metrics(vec![metric(at(i))]);
            }
        });
        assert_eq!(
            counter_value(&snapshotter, AGENT_TELEMETRY_SINK_DROPPED_BATCHES),
            2,
            "the first full-queue streak drops 2"
        );

        // Open the gate and drain to empty: the drain task parks again.
        gate.send(true).unwrap();
        tokio::time::timeout(StdDuration::from_secs(5), hub.flush())
            .await
            .expect("flush resolves");
        // Shut the gate again for a fresh synchronous fill.
        gate.send(false).unwrap();

        metrics::with_local_recorder(&recorder, || {
            // A non-dropping push with room resets the latch...
            hub.append_metrics(vec![metric(at(100))]);
            // ...and this fills to depth 2, so the *next* push drops and the new
            // streak's first drop counts again.
            hub.append_metrics(vec![metric(at(101))]);
            hub.append_metrics(vec![metric(at(102))]);
        });
        assert_eq!(
            counter_value(&snapshotter, AGENT_TELEMETRY_SINK_DROPPED_BATCHES),
            3,
            "the second full-queue episode counts one more drop after the reset"
        );
    }

    // ---- 7. `consumes` reflects configured kinds (docker-executor.md §8.3) ---

    #[tokio::test]
    async fn consumes_reflects_configured_kinds() {
        let (m, _mg, _mr) = test_sink(true);
        let (l, _lg, _lr) = test_sink(true);
        let both = TelemetryHub::new(
            vec![
                HubSink {
                    sink: SinkInstance::Test(m),
                    kinds: vec![SinkKind::Metrics],
                },
                HubSink {
                    sink: SinkInstance::Test(l),
                    kinds: vec![SinkKind::Logs],
                },
            ],
            16,
        );
        assert!(both.consumes(SinkKind::Metrics));
        assert!(both.consumes(SinkKind::Logs));

        let (only, _g, _r) = test_sink(true);
        let logs_only = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Test(only),
                kinds: vec![SinkKind::Logs],
            }],
            16,
        );
        assert!(
            !logs_only.consumes(SinkKind::Metrics),
            "no metrics consumer"
        );
        assert!(logs_only.consumes(SinkKind::Logs));

        let none = TelemetryHub::new(vec![], 16);
        assert!(!none.consumes(SinkKind::Metrics));
        assert!(
            !none.consumes(SinkKind::Logs),
            "an empty hub consumes nothing"
        );
    }

    // ---- 8. same-kind fan-out (docker-executor.md §8.3) --------------------

    #[tokio::test]
    async fn same_kind_fan_out_delivers_every_batch_to_both_sinks_in_order() {
        let (a, _ag, mut a_rx) = test_sink(true);
        let (b, _bg, mut b_rx) = test_sink(true);
        let hub = TelemetryHub::new(
            vec![
                HubSink {
                    sink: SinkInstance::Test(a),
                    kinds: vec![SinkKind::Logs],
                },
                HubSink {
                    sink: SinkInstance::Test(b),
                    kinds: vec![SinkKind::Logs],
                },
            ],
            16,
        );
        for i in 0..5 {
            hub.append_logs(vec![log_chunk(at(i), b"x")]);
        }
        tokio::time::timeout(StdDuration::from_secs(5), hub.flush())
            .await
            .expect("flush resolves");

        let expected: Vec<i64> = (0..5).map(|i| at(i).as_micros()).collect();
        let a_tags: Vec<i64> = collect(&mut a_rx).iter().map(log_tag).collect();
        let b_tags: Vec<i64> = collect(&mut b_rx).iter().map(log_tag).collect();
        assert_eq!(a_tags, expected, "sink A receives every batch, in order");
        assert_eq!(b_tags, expected, "sink B receives every batch, in order");
    }

    // ---- 9. slow-sink isolation (docker-executor.md §8.3) ------------------

    #[tokio::test]
    async fn a_slow_sink_never_starves_a_fast_one() {
        // A gated shut, B open: B must receive every batch while A's queue merely
        // holds them — proven WITHOUT flushing (a flush would block on A).
        let (a, a_gate, mut a_rx) = test_sink(false);
        let (b, _bg, mut b_rx) = test_sink(true);
        let hub = TelemetryHub::new(
            vec![
                HubSink {
                    sink: SinkInstance::Test(a),
                    kinds: vec![SinkKind::Logs],
                },
                HubSink {
                    sink: SinkInstance::Test(b),
                    kinds: vec![SinkKind::Logs],
                },
            ],
            64,
        );
        for i in 0..8 {
            hub.append_logs(vec![log_chunk(at(i), b"x")]);
        }

        // Await B's deliveries directly; A is still shut, so no flush is possible.
        let mut b_tags = Vec::new();
        while b_tags.len() < 8 {
            let batch = tokio::time::timeout(StdDuration::from_secs(5), b_rx.recv())
                .await
                .expect("B keeps delivering while A is stuck")
                .expect("B's sender is live");
            b_tags.push(log_tag(&batch));
        }
        let expected: Vec<i64> = (0..8).map(|i| at(i).as_micros()).collect();
        assert_eq!(
            b_tags, expected,
            "the fast sink drained all 8 while A was shut"
        );
        assert!(
            collect(&mut a_rx).is_empty(),
            "the shut sink delivered nothing; its queue held every batch"
        );

        // Open A and flush: its held batches drain in order.
        a_gate.send(true).unwrap();
        tokio::time::timeout(StdDuration::from_secs(5), hub.flush())
            .await
            .expect("flush resolves once A opens");
        let a_tags: Vec<i64> = collect(&mut a_rx).iter().map(log_tag).collect();
        assert_eq!(
            a_tags, expected,
            "A eventually drained the same 8, in order"
        );
    }

    // ---- 10. flush races concurrent enqueue (docker-executor.md §8.3) -------

    /// `flush` runs **concurrently with** `append_metrics`, not after it: a
    /// flusher task loops `flush()` for the entire lifetime of the producers
    /// (they signal completion through a shared flag), so enqueue and flush
    /// genuinely overlap — the earlier join-then-flush version never exercised
    /// that race. What this proves: (a) no deadlock or panic while a flush races
    /// live enqueues — every racing flush is bounded by a timeout and must
    /// resolve; and (b) once every producer is joined and a FINAL flush completes
    /// on the now-quiescent hub, every appended batch has been delivered exactly
    /// once (queue depth is far above the total, so nothing is dropped). Ordering
    /// is not asserted — the prior test asserted only completeness, and
    /// interleaved producers have no cross-producer order to preserve.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn flush_races_concurrent_enqueue_and_still_delivers_all() {
        let (sink, _gate, mut rx) = test_sink(true);
        // Depth well above the total so nothing is dropped: this asserts delivery
        // completeness under concurrent producers, not the drop path.
        let hub = TelemetryHub::new(
            vec![HubSink {
                sink: SinkInstance::Test(sink),
                kinds: vec![SinkKind::Metrics],
            }],
            4096,
        );
        let producers = 8usize;
        let per_producer = 50usize;
        let running = Arc::new(AtomicBool::new(true));

        // Producers append with `yield_now` interleavings so their appends
        // overlap each other and the racing flusher rather than running to
        // completion in one uninterrupted burst.
        let mut handles = Vec::new();
        for _ in 0..producers {
            let hub = hub.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..per_producer {
                    hub.append_metrics(vec![metric(at(i as i64))]);
                    tokio::task::yield_now().await;
                }
            }));
        }

        // A flusher racing the producers: loop `flush()` while any producer is
        // still appending. Each flush must resolve (no deadlock) even though
        // enqueue is happening underneath it; the timeout turns a hang into a
        // loud failure instead of blocking the whole suite.
        let flusher = {
            let hub = hub.clone();
            let running = Arc::clone(&running);
            tokio::spawn(async move {
                while running.load(Ordering::Relaxed) {
                    tokio::time::timeout(StdDuration::from_secs(5), hub.flush())
                        .await
                        .expect("a flush racing live enqueues must resolve, not deadlock");
                    tokio::task::yield_now().await;
                }
            })
        };

        for handle in handles {
            handle.await.expect("producer task");
        }
        running.store(false, Ordering::Relaxed);
        flusher.await.expect("flusher task");

        // FINAL flush on the now-quiescent hub: only now is the delivered count
        // stable, because a racing flush can return between two producers' appends.
        tokio::time::timeout(StdDuration::from_secs(5), hub.flush())
            .await
            .expect("final flush resolves");
        assert_eq!(
            collect(&mut rx).len(),
            producers * per_producer,
            "every concurrently-enqueued batch is delivered exactly once, even with flush racing enqueue"
        );
    }
}
