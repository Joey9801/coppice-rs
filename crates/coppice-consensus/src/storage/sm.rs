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
//!   membership) and answers `Ok(Applied::default())`.
//! - Snapshot builds ask the task for its state via
//!   [`ApplyRequest::Snapshot`]; serialization then happens off the apply
//!   task, on the blocking pool.
//! - Snapshot installs swap state wholesale via [`ApplyRequest::Install`].
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

use std::io::{self, Cursor};
use std::sync::{Arc, Mutex as StdMutex};

use openraft::storage::{RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{
    BasicNode, LogId, RaftSnapshotBuilder, StorageError, StorageIOError, StoredMembership,
};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

use coppice_proto::convert::{state_from_records, state_to_records};
use coppice_proto::pb::storage::v1 as pbstorage;

use coppice_state::StateMachine;

use crate::adapter::{ApplyRequest, ApplyResult, TypeConfig};
use crate::fs::{Fs, RealFs};
use crate::CoordinatorId;

use super::engine::StorageCore;
use super::raftpb;
use super::snapshot;

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
    cluster_uuid: [u8; 16],
}

/// Snapshot builder: captures a coherent `(state, log id, membership)` and
/// serializes it off the apply task.
pub struct SegmentSnapshotBuilder<F: Fs = RealFs> {
    core: Arc<StdMutex<StorageCore<F>>>,
    apply_tx: mpsc::Sender<ApplyRequest>,
    applied: Arc<AsyncMutex<AppliedState>>,
    shards: u32,
    cluster_uuid: [u8; 16],
}

impl<F: Fs> StateMachineStore<F> {
    pub(super) fn new(
        core: Arc<StdMutex<StorageCore<F>>>,
        apply_tx: mpsc::Sender<ApplyRequest>,
        last_applied: Option<LogId<CoordinatorId>>,
        membership: StoredMembership<CoordinatorId, BasicNode>,
        shards: u32,
        cluster_uuid: [u8; 16],
    ) -> Self {
        StateMachineStore {
            core,
            apply_tx,
            applied: Arc::new(AsyncMutex::new(AppliedState {
                last_applied,
                membership,
            })),
            shards,
            cluster_uuid,
        }
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
            cluster_uuid: self.cluster_uuid,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<CoordinatorId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<CoordinatorId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<CoordinatorId>> {
        let bytes = snapshot.into_inner();
        let path = std::path::Path::new("install-snapshot");

        // Decode first: every section CRC is validated and every record must
        // convert before anything durable changes (ADR 0016 — a snapshot
        // that cannot rebuild state is never adopted).
        let (embedded, records) =
            snapshot::decode_state(path, &bytes).map_err(|e| sm_write_err(&e))?;
        if embedded.snapshot_id != meta.snapshot_id {
            return Err(sm_write_err(&io::Error::other(format!(
                "snapshot stream claims id {:?} but carries {:?}",
                meta.snapshot_id, embedded.snapshot_id
            ))));
        }
        let state = state_from_records(records).map_err(|e| {
            sm_write_err(&io::Error::other(format!(
                "snapshot records do not rebuild: {e}"
            )))
        })?;

        let mut applied = self.applied.lock().await;

        // Durable adoption: write the file, flip the manifest pointer, and
        // advance the purge floor past everything the snapshot covers, in
        // one manifest swap (ADR 0016 learner rebuild; ADR 0017 ordering).
        let core = Arc::clone(&self.core);
        tokio::task::spawn_blocking(move || {
            core.lock()
                .expect("storage engine poisoned")
                .install_snapshot(&bytes, true)
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

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<CoordinatorId>> {
        let core = Arc::clone(&self.core);
        let current = tokio::task::spawn_blocking(move || {
            core.lock()
                .expect("storage engine poisoned")
                .current_snapshot()
        })
        .await
        .map_err(|e| sm_read_err(&io::Error::other(format!("storage task panicked: {e}"))))?
        .map_err(|e| sm_read_err(&e))?;
        let Some((meta, bytes)) = current else {
            return Ok(None);
        };
        let meta = openraft_meta(&meta).map_err(|e| sm_read_err(&e))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        }))
    }
}

impl<F: Fs> RaftSnapshotBuilder<TypeConfig> for SegmentSnapshotBuilder<F> {
    async fn build_snapshot(
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
            cluster_uuid: self.cluster_uuid.to_vec(),
            snapshot_id: snapshot_id.clone(),
            last_applied: last_applied.as_ref().map(raftpb::log_id_to_pb),
            membership: Some(raftpb::stored_membership_to_pb(&membership)),
            cluster_version: state.cluster_version,
            shard_count: self.shards,
        };

        // Serialize + write + pointer flip on the blocking pool; the apply
        // task is free the whole time (ADR 0018).
        let core = Arc::clone(&self.core);
        let shards = self.shards;
        let bytes = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            let records = state_to_records(&state);
            let bytes = snapshot::encode_state(&meta, &records, shards);
            core.lock()
                .expect("storage engine poisoned")
                .install_snapshot(&bytes, false)?;
            Ok(bytes)
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
            snapshot: Box::new(Cursor::new(bytes)),
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
