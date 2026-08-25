//! `StateMachineStore`: the openraft [`RaftStateMachine`] over the shared
//! engine plus the single-writer apply task (ADR 0002, coordinator-runtime).
//!
//! # Reconciling with the apply-task protocol
//!
//! `adapter.rs` fixes the ownership scheme: one apply task owns the mutable
//! [`coppice_state::StateMachine`] and is reached only over the bounded
//! [`ApplyRequest`] channel. This store does **not** own state; it is the
//! protocol's client:
//!
//! - `apply` forwards the batch's normal commands as one
//!   [`ApplyRequest::Apply`] and awaits the replies, so backpressure lands on
//!   openraft's replication. Blank and membership entries never reach the
//!   task — they do not touch state; the store records them (last applied,
//!   membership) and answers `Ok(Applied::default())`. When such an entry is
//!   the batch's last, the store sends one [`ApplyRequest::Advance`] so the
//!   apply task's *published* cursor moves past it too; otherwise a strong
//!   read or event resync whose `read_index` barrier lands on a no-op or
//!   membership index would block forever (the published cursor only advances
//!   on normal commands, but `read_index` returns the full Raft index).
//! - Snapshot builds ask the task for its state via
//!   [`ApplyRequest::Snapshot`]; serialization then happens off the apply
//!   task, on the blocking pool.
//! - Snapshot installs swap state wholesale via [`ApplyRequest::Install`].
//!   The container itself moves as a [`SnapshotFile`] — decoded, validated,
//!   and adopted from disk in bounded memory, never as one in-memory buffer
//!   (ADR 0018).
//!
//! The canonical apply loop lives here as [`run_apply_task`]; the
//! coordinator runtime spawns the same loop (wrapping it with view/status
//! publication), tests spawn it bare — one loop, not two ownership schemes.
//!
//! # Raft bookkeeping the state machine does not carry
//!
//! `coppice_state::StateMachine` is deliberately Raft-agnostic: it counts
//! applied commands (`version`) but knows nothing of log ids or membership.
//! The store tracks `(last_applied, last_membership)` beside the channel,
//! under an async mutex held across each apply round-trip, so a concurrent
//! snapshot build always pairs a state with exactly the log id it reflects.
//! Durable recovery of that pair is the snapshot's meta record plus
//! openraft's own startup replay from the manifest's best-effort committed
//! index (`StorageHelper::get_initial_state` re-applies committed entries
//! through this very `apply` path — the "log replay from the snapshot index"
//! of ADR 0016 runs through one code path, not two).

use std::fmt;
use std::io;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use openraft::storage::{RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{
    BasicNode, LogId, RaftSnapshotBuilder, StorageError, StorageIOError, StoredMembership,
};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

use coppice_proto::convert::state_from_records;
use coppice_proto::pb::storage::v1 as pbstorage;

use coppice_state::StateMachine;

use crate::adapter::{ApplyRequest, ApplyResult, TypeConfig};
use crate::fs::{Fs, FsFile, RealFs};
use crate::CoordinatorId;

use super::engine::StorageCore;
use super::raftpb;
use super::snapshot;

/// Off-apply-task snapshot build cost: encode + write + validate + adopt of
/// one container, on the blocking pool.
const SNAPSHOT_BUILD_SECONDS: &str = "coordinator_snapshot_build_seconds";
/// The `last_applied` log index covered by the newest snapshot this replica
/// has adopted — the coordinate the purge floor follows (ADR 0017). Read
/// against `coordinator_view_applied_index` to see how far the log has run
/// past the last snapshot.
const SNAPSHOT_LAST_INDEX: &str = "coordinator_snapshot_last_index";
/// How long ago this replica last adopted a snapshot. Sampled, not pushed:
/// age advances with the clock, not with events.
const SNAPSHOT_AGE_SECONDS: &str = "coordinator_snapshot_age_seconds";
/// Snapshot adoptions that failed, by [`PHASE_BUILD`]/[`PHASE_INSTALL`].
///
/// A failure here is survivable by construction — nothing durable changes
/// until a container validates and the manifest pointer flips (ADR 0017) —
/// so this counter rising without `coordinator_snapshot_age_seconds`
/// flattening is the signal worth alerting on.
const SNAPSHOT_FAILURES_TOTAL: &str = "coordinator_snapshot_failures_total";

/// `phase` label values for [`SNAPSHOT_FAILURES_TOTAL`].
const PHASE_BUILD: &str = "build";
const PHASE_INSTALL: &str = "install";

/// When this process last adopted a snapshot, behind [`SNAPSHOT_AGE_SECONDS`].
///
/// Process-local on purpose: no durable stamp carries a snapshot's wall
/// clock, so after a restart the gauge is simply **absent** until this
/// replica adopts one. Absent beats a fabricated zero — an age alert must not
/// read "just snapshotted" merely because the coordinator bounced.
static LAST_SNAPSHOT_AT: StdMutex<Option<Instant>> = StdMutex::new(None);

pub(crate) fn describe_metrics() {
    metrics::describe_histogram!(
        SNAPSHOT_BUILD_SECONDS,
        metrics::Unit::Seconds,
        "Time to encode, write, validate, and adopt one snapshot container (off the apply task)."
    );
    metrics::describe_gauge!(
        SNAPSHOT_LAST_INDEX,
        metrics::Unit::Count,
        "Raft log index covered by the newest snapshot this replica has adopted."
    );
    metrics::describe_gauge!(
        SNAPSHOT_AGE_SECONDS,
        metrics::Unit::Seconds,
        "Seconds since this replica last adopted a snapshot (absent until it adopts one)."
    );
    metrics::describe_counter!(
        SNAPSHOT_FAILURES_TOTAL,
        metrics::Unit::Count,
        "Snapshot adoptions that failed, by phase (build, install)."
    );
}

pub(crate) fn gather_metrics() {
    // Build duration and last-index are pushed as snapshots are adopted; age
    // is a function of the clock alone, so it is sampled here.
    if let Some(at) = *LAST_SNAPSHOT_AT.lock().unwrap_or_else(|e| e.into_inner()) {
        metrics::gauge!(SNAPSHOT_AGE_SECONDS).set(at.elapsed().as_secs_f64());
    }
}

/// Note one adopted snapshot — built here or installed from the leader.
///
/// Called only once the manifest pointer has flipped, so the gauges describe
/// what is durably on this disk, never what was merely attempted.
fn record_snapshot_adopted(last_index: u64) {
    *LAST_SNAPSHOT_AT.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
    metrics::gauge!(SNAPSHOT_LAST_INDEX).set(last_index as f64);
}

/// Note one failed snapshot adoption.
fn record_snapshot_failure(phase: &'static str) {
    metrics::counter!(SNAPSHOT_FAILURES_TOTAL, "phase" => phase).increment(1);
}

/// The openraft `SnapshotData` binding: a file-backed handle to one ADR 0018
/// snapshot container. (openraft's `generic-snapshot-data` feature lifts the
/// tokio IO bounds, so this is a plain positioned-IO handle over the [`FsFile`]
/// seam, not an async stream.)
///
/// It leads two lives:
///
/// - **Reading / sending**: a read handle to a store's durable
///   `snap/<id>.snap`; the network layer streams it in wire-sized chunks.
/// - **Receiving**: the engine's receive spool (`snap/receiving.tmp`),
///   appended one wire frame at a time. The spool is never claimed by the
///   manifest, so a crash mid-receive leaves an orphan the recovery sweep
///   deletes (ADR 0017).
///
/// Bounded memory is the point of the binding: neither side of an
/// install-snapshot ever materializes the container (ADR 0018's 1M-job
/// snapshots are tens of MB to GB).
// `len` is a fallible size query on a file, not a collection length.
#[allow(clippy::len_without_is_empty)]
pub struct SnapshotFile {
    file: Box<dyn FsFile>,
}

impl SnapshotFile {
    pub(crate) fn new(file: Box<dyn FsFile>) -> SnapshotFile {
        SnapshotFile { file }
    }

    /// Append one received wire chunk (receive spool only). Visible, not
    /// durable — durability happens at adoption (ADR 0017).
    pub fn append(&mut self, data: &[u8]) -> io::Result<()> {
        self.file.append(data)
    }

    /// Current length of the underlying file.
    pub fn len(&self) -> io::Result<u64> {
        self.file.len()
    }

    /// Read exactly `buf.len()` bytes at `offset`.
    pub fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.file.read_exact_at(offset, buf)
    }

    /// The underlying seam file, for the validate/decode/adopt paths.
    pub(crate) fn as_file(&self) -> &dyn FsFile {
        &*self.file
    }
}

impl fmt::Debug for SnapshotFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotFile")
            .field("len", &self.file.len())
            .finish()
    }
}

/// The Raft coordinates of the applied state, tracked beside the apply
/// channel (see module docs).
#[derive(Debug, Clone, Default)]
struct AppliedState {
    last_applied: Option<LogId<CoordinatorId>>,
    membership: StoredMembership<CoordinatorId, BasicNode>,
}

fn sm_read_err(e: &io::Error) -> StorageError<CoordinatorId> {
    StorageIOError::read_state_machine(&io::Error::new(e.kind(), e.to_string())).into()
}

fn sm_write_err(e: &io::Error) -> StorageError<CoordinatorId> {
    StorageIOError::write_state_machine(&io::Error::new(e.kind(), e.to_string())).into()
}

fn channel_closed() -> StorageError<CoordinatorId> {
    StorageIOError::write_state_machine(&io::Error::other("apply task is gone (shutdown)")).into()
}

/// The state-machine store (ADR 0002/0016/0018).
///
/// Constructed by [`super::open`]'s [`super::Recovered::into_stores`].
pub struct StateMachineStore<F: Fs = RealFs> {
    core: Arc<StdMutex<StorageCore<F>>>,
    apply_tx: mpsc::Sender<ApplyRequest>,
    /// Held across every apply round-trip and every snapshot-state capture,
    /// so `(state, applied)` pairs are always coherent.
    applied: Arc<AsyncMutex<AppliedState>>,
    /// Snapshot sharding degree and the cluster identity stamped into every
    /// built snapshot (ADR 0016/0018).
    shards: u32,
    history_id: [u8; 16],
}

/// Snapshot builder: captures a coherent `(state, log id, membership)` and
/// serializes it off the apply task.
pub struct SegmentSnapshotBuilder<F: Fs = RealFs> {
    core: Arc<StdMutex<StorageCore<F>>>,
    apply_tx: mpsc::Sender<ApplyRequest>,
    applied: Arc<AsyncMutex<AppliedState>>,
    shards: u32,
    history_id: [u8; 16],
}

impl<F: Fs> StateMachineStore<F> {
    pub(super) fn new(
        core: Arc<StdMutex<StorageCore<F>>>,
        apply_tx: mpsc::Sender<ApplyRequest>,
        last_applied: Option<LogId<CoordinatorId>>,
        membership: StoredMembership<CoordinatorId, BasicNode>,
        shards: u32,
        history_id: [u8; 16],
    ) -> Self {
        StateMachineStore {
            core,
            apply_tx,
            applied: Arc::new(AsyncMutex::new(AppliedState {
                last_applied,
                membership,
            })),
            shards,
            history_id,
        }
    }

    async fn install_snapshot_inner(
        &mut self,
        meta: &SnapshotMeta<CoordinatorId, BasicNode>,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<CoordinatorId>> {
        // Decode first, streaming from the file: every section CRC is
        // validated and every record must convert before anything durable
        // changes (ADR 0016 — a snapshot that cannot rebuild state is never
        // adopted). Only per-section buffers are ever in memory (ADR 0018).
        let expect_id = meta.snapshot_id.clone();
        let (snapshot, state) = tokio::task::spawn_blocking(
            move || -> io::Result<(Box<SnapshotFile>, StateMachine)> {
                let path = std::path::Path::new("install-snapshot");
                let (embedded, records) = snapshot::decode_state_file(path, snapshot.as_file())?;
                if embedded.snapshot_id != expect_id {
                    return Err(io::Error::other(format!(
                        "snapshot stream claims id {expect_id:?} but carries {:?}",
                        embedded.snapshot_id
                    )));
                }
                let state = state_from_records(records).map_err(|e| {
                    io::Error::other(format!("snapshot records do not rebuild: {e}"))
                })?;
                Ok((snapshot, state))
            },
        )
        .await
        .map_err(|e| sm_write_err(&io::Error::other(format!("storage task panicked: {e}"))))?
        .map_err(|e| sm_write_err(&e))?;

        let mut applied = self.applied.lock().await;

        // Durable adoption: stream the container into this store (copy,
        // fsync, rename), flip the manifest pointer, and advance the purge
        // floor past everything the snapshot covers, in one manifest swap
        // (ADR 0016 learner rebuild; ADR 0017 ordering).
        let core = Arc::clone(&self.core);
        tokio::task::spawn_blocking(move || {
            let mut core = core.lock().expect("storage engine poisoned");
            core.install_snapshot_from(snapshot.as_file(), true)?;
            // If this snapshot arrived over the wire, `snapshot` is the
            // receive spool — adopted now, so drop the spool file.
            core.remove_snapshot_spool()
        })
        .await
        .map_err(|e| sm_write_err(&io::Error::other(format!("storage task panicked: {e}"))))?
        .map_err(|e| sm_write_err(&e))?;

        // State adoption, through the single-writer protocol.
        let applied_index = meta.last_log_id.map(|id| id.index).unwrap_or(0);
        let (reply, rx) = oneshot::channel();
        self.apply_tx
            .send(ApplyRequest::Install {
                state: Box::new(state),
                applied_index,
                reply,
            })
            .await
            .map_err(|_| channel_closed())?;
        rx.await.map_err(|_| channel_closed())?;

        applied.last_applied = meta.last_log_id;
        applied.membership = meta.last_membership.clone();
        Ok(())
    }
}

impl<F: Fs> RaftStateMachine<TypeConfig> for StateMachineStore<F> {
    type SnapshotBuilder = SegmentSnapshotBuilder<F>;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<CoordinatorId>>,
            StoredMembership<CoordinatorId, BasicNode>,
        ),
        StorageError<CoordinatorId>,
    > {
        let applied = self.applied.lock().await;
        Ok((applied.last_applied, applied.membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ApplyResult>, StorageError<CoordinatorId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        use openraft::EntryPayload;

        // Coherence lock: held across the whole round-trip so a snapshot
        // build never observes state and tracker out of step.
        let mut applied = self.applied.lock().await;

        let entries: Vec<_> = entries.into_iter().collect();
        let mut responses: Vec<Option<ApplyResult>> = Vec::with_capacity(entries.len());
        let mut normals: Vec<(u64, coppice_state::Command)> = Vec::new();
        let mut normal_slots: Vec<usize> = Vec::new();

        for entry in &entries {
            applied.last_applied = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Blank => responses.push(Some(Ok(coppice_state::Applied::default()))),
                EntryPayload::Membership(membership) => {
                    applied.membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    responses.push(Some(Ok(coppice_state::Applied::default())));
                }
                EntryPayload::Normal(command) => {
                    normals.push((entry.log_id.index, command.clone()));
                    normal_slots.push(responses.len());
                    responses.push(None);
                }
            }
        }

        if !normals.is_empty() {
            let (reply, rx) = oneshot::channel();
            self.apply_tx
                .send(ApplyRequest::Apply {
                    entries: normals,
                    reply,
                })
                .await
                .map_err(|_| channel_closed())?;
            let outcomes = rx.await.map_err(|_| channel_closed())?;
            if outcomes.len() != normal_slots.len() {
                return Err(StorageIOError::write_state_machine(&io::Error::other(
                    "apply task returned a mismatched outcome count",
                ))
                .into());
            }
            for (slot, outcome) in normal_slots.into_iter().zip(outcomes) {
                responses[slot] = Some(outcome);
            }
        }

        // The apply task advanced its published cursor to the last *normal*
        // command it applied. If the batch ends on a blank (Raft no-op) or
        // membership entry — which never reach the task — that cursor now lags
        // the batch's true last-applied index, and a strong read there would
        // hang. Nudge the cursor forward to the batch's final index. (When the
        // last entry is a normal command, the task already advanced to it.)
        if let Some(last) = entries.last() {
            if !matches!(last.payload, EntryPayload::Normal(_)) {
                let (reply, rx) = oneshot::channel();
                self.apply_tx
                    .send(ApplyRequest::Advance {
                        applied_index: last.log_id.index,
                        reply,
                    })
                    .await
                    .map_err(|_| channel_closed())?;
                rx.await.map_err(|_| channel_closed())?;
            }
        }

        Ok(responses
            .into_iter()
            .map(|r| r.expect("every entry produced a response"))
            .collect())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SegmentSnapshotBuilder {
            core: Arc::clone(&self.core),
            apply_tx: self.apply_tx.clone(),
            applied: Arc::clone(&self.applied),
            shards: self.shards,
            history_id: self.history_id,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotFile>, StorageError<CoordinatorId>> {
        // The spool lives in this store's own snap/ directory, through the
        // Fs seam, so the crash suite's recovery sweep sees (and deletes) a
        // torn receive like any other orphan.
        let core = Arc::clone(&self.core);
        let file = tokio::task::spawn_blocking(move || {
            core.lock()
                .expect("storage engine poisoned")
                .begin_snapshot_receive()
        })
        .await
        .map_err(|e| sm_write_err(&io::Error::other(format!("storage task panicked: {e}"))))?
        .map_err(|e| sm_write_err(&e))?;
        Ok(Box::new(SnapshotFile::new(file)))
    }

    /// The metrics skin over [`StateMachineStore::install_snapshot_inner`].
    ///
    /// A rejected container (bad CRC, wrong snapshot id, records that will
    /// not rebuild) is discarded before anything durable changes, so this
    /// only ever counts the failure.
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<CoordinatorId, BasicNode>,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<CoordinatorId>> {
        match self.install_snapshot_inner(meta, snapshot).await {
            Ok(()) => {
                record_snapshot_adopted(meta.last_log_id.as_ref().map_or(0, |id| id.index));
                Ok(())
            }
            Err(e) => {
                record_snapshot_failure(PHASE_INSTALL);
                Err(e)
            }
        }
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<CoordinatorId>> {
        let core = Arc::clone(&self.core);
        let current = tokio::task::spawn_blocking(move || {
            core.lock()
                .expect("storage engine poisoned")
                .current_snapshot_reader()
        })
        .await
        .map_err(|e| sm_read_err(&io::Error::other(format!("storage task panicked: {e}"))))?
        .map_err(|e| sm_read_err(&e))?;
        let Some((meta, _index, file)) = current else {
            return Ok(None);
        };
        let meta = openraft_meta(&meta).map_err(|e| sm_read_err(&e))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(SnapshotFile::new(file)),
        }))
    }
}

impl<F: Fs> RaftSnapshotBuilder<TypeConfig> for SegmentSnapshotBuilder<F> {
    /// The metrics skin over [`SegmentSnapshotBuilder::build_snapshot_inner`].
    ///
    /// Success is recorded from the returned meta, i.e. after the manifest
    /// pointer has flipped; a failure leaves storage untouched (ADR 0017) and
    /// openraft retries on its own cadence, so it is counted, not escalated.
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, StorageError<CoordinatorId>> {
        match self.build_snapshot_inner().await {
            Ok(snapshot) => {
                record_snapshot_adopted(
                    snapshot.meta.last_log_id.as_ref().map_or(0, |id| id.index),
                );
                Ok(snapshot)
            }
            Err(e) => {
                record_snapshot_failure(PHASE_BUILD);
                Err(e)
            }
        }
    }
}

impl<F: Fs> SegmentSnapshotBuilder<F> {
    async fn build_snapshot_inner(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, StorageError<CoordinatorId>> {
        // Capture a coherent (state, log id, membership) triple under the
        // same lock `apply` holds across its round-trips.
        let (state, last_applied, membership) = {
            let applied = self.applied.lock().await;
            let (reply, rx) = oneshot::channel();
            self.apply_tx
                .send(ApplyRequest::Snapshot { reply })
                .await
                .map_err(|_| channel_closed())?;
            let (state, _task_applied) = rx.await.map_err(|_| channel_closed())?;
            (state, applied.last_applied, applied.membership.clone())
        };

        let snapshot_id = self
            .core
            .lock()
            .expect("storage engine poisoned")
            .mint_snapshot_id();
        let meta = pbstorage::SnapshotMeta {
            history_id: self.history_id.to_vec(),
            snapshot_id: snapshot_id.clone(),
            last_applied: last_applied.as_ref().map(raftpb::log_id_to_pb),
            membership: Some(raftpb::stored_membership_to_pb(&membership)),
            cluster_version: state.cluster_version,
            shard_count: self.shards,
        };

        // Serialize + write + pointer flip on the blocking pool; the apply
        // task is free the whole time (ADR 0018). Sections are encoded
        // straight from the captured state — each worker converts only its
        // own shard's window of records to protobuf and drops them before the
        // next (KOI-5) — and streamed section by section into the engine's
        // temp file, never held in memory whole. No whole-state protobuf copy
        // exists at any point; peak is the live state, this cloned `Arc`, and
        // a bounded set of in-flight section buffers. The engine is locked
        // only to create the temp file and to adopt it, so appends continue
        // while the encode runs. What openraft holds (and the network later
        // streams) is a read handle to the adopted file, never the bytes.
        let core = Arc::clone(&self.core);
        let shards = self.shards;
        let file = tokio::task::spawn_blocking(move || -> io::Result<Box<dyn FsFile>> {
            let build_started = Instant::now();
            let mut spool = core
                .lock()
                .expect("storage engine poisoned")
                .begin_snapshot_build(&meta.snapshot_id)?;
            snapshot::write_state_direct(&mut *spool, &meta, &state, shards)?;
            let mut core = core.lock().expect("storage engine poisoned");
            core.finish_snapshot_build(spool)?;
            metrics::histogram!(SNAPSHOT_BUILD_SECONDS)
                .record(build_started.elapsed().as_secs_f64());
            let (_, _, file) = core
                .current_snapshot_reader()?
                .ok_or_else(|| io::Error::other("freshly built snapshot is not the current one"))?;
            Ok(file)
        })
        .await
        .map_err(|e| sm_write_err(&io::Error::other(format!("storage task panicked: {e}"))))?
        .map_err(|e| sm_write_err(&e))?;

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                last_membership: membership,
                snapshot_id,
            },
            snapshot: Box::new(SnapshotFile::new(file)),
        })
    }
}

/// Convert a durable snapshot meta record into openraft's.
pub(super) fn openraft_meta(
    meta: &pbstorage::SnapshotMeta,
) -> io::Result<SnapshotMeta<CoordinatorId, BasicNode>> {
    let path = std::path::Path::new("snap");
    let last_log_id = meta
        .last_applied
        .as_ref()
        .map(|id| raftpb::log_id_from_pb(path, id))
        .transpose()?;
    let last_membership = meta
        .membership
        .as_ref()
        .map(|m| raftpb::stored_membership_from_pb(path, m))
        .transpose()?
        .unwrap_or_default();
    Ok(SnapshotMeta {
        last_log_id,
        last_membership,
        snapshot_id: meta.snapshot_id.clone(),
    })
}

/// The canonical single-writer apply loop (coordinator-runtime.md): sole
/// owner of the mutable [`StateMachine`], fed only by [`ApplyRequest`]s.
///
/// Ends when every sender is dropped.
pub async fn run_apply_task(mut state: StateMachine, mut rx: mpsc::Receiver<ApplyRequest>) {
    let mut applied_index: u64 = 0;
    while let Some(request) = rx.recv().await {
        match request {
            ApplyRequest::Apply { entries, reply } => {
                let mut outcomes = Vec::with_capacity(entries.len());
                for (index, command) in &entries {
                    outcomes.push(state.apply(command));
                    applied_index = *index;
                }
                // A dropped receiver just means the proposer went away.
                let _ = reply.send(outcomes);
            }
            ApplyRequest::Advance {
                applied_index: index,
                reply,
            } => {
                applied_index = applied_index.max(index);
                let _ = reply.send(());
            }
            ApplyRequest::Snapshot { reply } => {
                let _ = reply.send((Arc::new(state.clone()), applied_index));
            }
            ApplyRequest::Install {
                state: new_state,
                applied_index: index,
                reply,
            } => {
                state = *new_state;
                applied_index = index;
                let _ = reply.send(());
            }
        }
    }
}

#[cfg(test)]
mod metrics_tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

    /// One metric's value by name, or `None` if it was never touched.
    fn value(snapshotter: &Snapshotter, name: &str) -> Option<DebugValue> {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(key, _unit, _desc, value)| (key.key().name() == name).then_some(value))
    }

    /// Adoption pushes the index; age is only ever *sampled*, so a gather is
    /// what materializes it — and it stays absent until something is adopted,
    /// because a zero would read as "just snapshotted" on a replica that has
    /// never snapshotted at all.
    #[test]
    fn adoption_pushes_the_index_and_gather_samples_the_age() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            // The static is process-wide, so this test cannot assert the
            // never-adopted case against it; it asserts the ordering instead:
            // no gather, no age, even after an adoption.
            record_snapshot_adopted(4_211);
            assert_eq!(
                value(&snapshotter, SNAPSHOT_LAST_INDEX),
                Some(DebugValue::Gauge(4_211.0.into())),
                "the last-index gauge is pushed at adoption"
            );
            assert!(
                value(&snapshotter, SNAPSHOT_AGE_SECONDS).is_none(),
                "age is sampled, never pushed: nothing should exist before a gather"
            );

            gather_metrics();
            let DebugValue::Gauge(age) = value(&snapshotter, SNAPSHOT_AGE_SECONDS)
                .expect("age is emitted once a snapshot has been adopted and gathered")
            else {
                panic!("snapshot age must be a gauge");
            };
            assert!(
                age.into_inner() < 60.0,
                "age measures from the adoption we just recorded, got {age:?}"
            );
        });
    }

    /// Failures are counted per phase, so a build that keeps failing is
    /// distinguishable from a peer that keeps sending containers this replica
    /// rejects.
    #[test]
    fn failures_are_counted_per_phase() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            record_snapshot_failure(PHASE_BUILD);
            record_snapshot_failure(PHASE_INSTALL);
            record_snapshot_failure(PHASE_INSTALL);
        });

        let mut by_phase: Vec<(String, u64)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == SNAPSHOT_FAILURES_TOTAL)
            .map(|(key, _, _, value)| {
                let phase = key
                    .key()
                    .labels()
                    .find(|l| l.key() == "phase")
                    .expect("every failure carries a phase label")
                    .value()
                    .to_string();
                let DebugValue::Counter(n) = value else {
                    panic!("snapshot failures must be a counter");
                };
                (phase, n)
            })
            .collect();
        by_phase.sort();
        assert_eq!(
            by_phase,
            vec![("build".to_string(), 1), ("install".to_string(), 2)]
        );
    }
}
