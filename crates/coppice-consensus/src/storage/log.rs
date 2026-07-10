//! `SegmentLogStorage`: the openraft [`RaftLogStorage`] over [`StorageCore`]
//! (ADR 0002).
//!
//! This layer only converts and bridges: openraft types become durable
//! protobuf shapes (`raftpb`), and openraft's async trait calls become
//! `spawn_blocking` sections over the shared synchronous core. openraft 0.9
//! serializes all storage write IO through its core loop, so a mutex around
//! the engine adds no contention on the write path; see `engine.rs` for why a
//! dedicated writer thread was deliberately not built.

use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{LogId, RaftLogReader, StorageError, StorageIOError, Vote};
use tokio::sync::watch;

use crate::adapter::TypeConfig;
use crate::fs::{Fs, RealFs};
use crate::CoordinatorId;

use super::engine::{EncodedEntry, StorageCore};
use super::raftpb;

/// Map an engine error onto openraft's error surface.
///
/// The engine's fail-stop messages (file, offset, `coordinator replace`)
/// ride through as the source.
fn storage_err(err: &io::Error) -> StorageError<CoordinatorId> {
    StorageIOError::write_logs(&io::Error::new(err.kind(), err.to_string())).into()
}

fn read_err(err: &io::Error) -> StorageError<CoordinatorId> {
    StorageIOError::read_logs(&io::Error::new(err.kind(), err.to_string())).into()
}

/// Run one engine operation on the blocking pool.
async fn blocking<F: Fs, T: Send + 'static>(
    core: &Arc<Mutex<StorageCore<F>>>,
    op: impl FnOnce(&mut StorageCore<F>) -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let core = Arc::clone(core);
    tokio::task::spawn_blocking(move || op(&mut core.lock().expect("storage engine poisoned")))
        .await
        .map_err(|e| io::Error::other(format!("storage task panicked: {e}")))?
}

/// The segment log store (ADR 0002/0017).
///
/// Constructed by [`super::open`]; shares its engine with the
/// [`super::StateMachineStore`] because the manifest is the single durable
/// home of both the segment list and the snapshot pointer.
pub struct SegmentLogStorage<F: Fs = RealFs> {
    core: Arc<Mutex<StorageCore<F>>>,
    /// Publishes the committed index openraft reports through `save_committed`.
    /// The coordinator runtime's status task folds this into
    /// [`ConsensusStatus::known_committed`](crate::ConsensusStatus): openraft's
    /// `save_committed` can briefly lead the applied index (the commit is known
    /// before the entry is applied), which is precisely the follower-read
    /// staleness bound of ADR 0007.
    committed_tx: watch::Sender<u64>,
}

/// Read-only handle handed to openraft's replication tasks.
pub struct SegmentLogReader<F: Fs = RealFs> {
    core: Arc<Mutex<StorageCore<F>>>,
}

impl<F: Fs> SegmentLogStorage<F> {
    pub(super) fn new(core: Arc<Mutex<StorageCore<F>>>) -> Self {
        // Seed the watch with the best-effort committed index recovered from
        // the manifest (ADR 0017), so a status reader observes a correct value
        // before openraft's first `save_committed`.
        let initial = core
            .lock()
            .expect("storage engine poisoned")
            .committed()
            .map(|id| id.index)
            .unwrap_or(0);
        let (committed_tx, _) = watch::channel(initial);
        SegmentLogStorage { core, committed_tx }
    }

    /// A latest-value watch of the committed index openraft reports.
    ///
    /// Taken by the coordinator runtime *before* the store moves into
    /// `Raft::new`; the sender lives inside the store for the process lifetime,
    /// so the receiver stays live for the status task.
    pub fn committed_watch(&self) -> watch::Receiver<u64> {
        self.committed_tx.subscribe()
    }
}

// The `Err` variant is openraft's `StorageError` (~224 bytes), which trips
// `clippy::result_large_err`. Its size is not ours to control, and this helper
// exists only to back `RaftLogReader::try_get_log_entries` below, whose
// signature the trait fixes — boxing here would just mean unboxing at the trait
// boundary. Clippy exempts the trait impl itself for exactly this reason but
// does not extend that to free helpers.
#[allow(clippy::result_large_err)]
async fn get_entries<F: Fs, RB: RangeBounds<u64> + Debug>(
    core: &Arc<Mutex<StorageCore<F>>>,
    range: RB,
) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<CoordinatorId>> {
    let lo = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n + 1,
        Bound::Unbounded => 0,
    };
    let hi = match range.end_bound() {
        Bound::Included(&n) => n + 1,
        Bound::Excluded(&n) => n,
        Bound::Unbounded => u64::MAX,
    };
    let payloads = blocking(core, move |core| core.read_payloads(lo, hi))
        .await
        .map_err(|e| read_err(&e))?;
    let path = std::path::Path::new("log");
    payloads
        .iter()
        .map(|(index, bytes)| {
            raftpb::entry_from_bytes(path, *index, bytes).map_err(|e| read_err(&e))
        })
        .collect()
}

impl<F: Fs> RaftLogReader<TypeConfig> for SegmentLogStorage<F> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<CoordinatorId>> {
        get_entries(&self.core, range).await
    }
}

impl<F: Fs> RaftLogReader<TypeConfig> for SegmentLogReader<F> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<CoordinatorId>> {
        get_entries(&self.core, range).await
    }
}

impl<F: Fs> RaftLogStorage<TypeConfig> for SegmentLogStorage<F> {
    type LogReader = SegmentLogReader<F>;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<CoordinatorId>> {
        let (purged, last) = self
            .core
            .lock()
            .expect("storage engine poisoned")
            .log_state();
        let path = std::path::Path::new("manifest");
        let convert = |id: Option<coppice_proto::pb::raft::v1::LogId>| {
            id.map(|id| raftpb::log_id_from_pb(path, &id)).transpose()
        };
        let last_purged_log_id = convert(purged).map_err(|e| read_err(&e))?;
        let last_log_id = convert(last)
            .map_err(|e| read_err(&e))?
            .or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        SegmentLogReader {
            core: Arc::clone(&self.core),
        }
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<CoordinatorId>,
    ) -> Result<(), StorageError<CoordinatorId>> {
        let vote = raftpb::vote_to_pb(vote);
        blocking(&self.core, move |core| core.save_vote(&vote))
            .await
            .map_err(|e| StorageError::from(StorageIOError::write_vote(&e)))
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<CoordinatorId>>, StorageError<CoordinatorId>> {
        let vote = self
            .core
            .lock()
            .expect("storage engine poisoned")
            .vote()
            .cloned();
        vote.map(|v| raftpb::vote_from_pb(std::path::Path::new("vote"), &v))
            .transpose()
            .map_err(|e| StorageIOError::read_vote(&e).into())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<CoordinatorId>>,
    ) -> Result<(), StorageError<CoordinatorId>> {
        // In memory now; persisted opportunistically with the next structural
        // manifest write (rotation, snapshot) — never its own fsync, the
        // committed index is best-effort by design (ADR 0017).
        let index = committed.as_ref().map(|id| id.index).unwrap_or(0);
        let committed = committed.as_ref().map(raftpb::log_id_to_pb);
        self.core
            .lock()
            .expect("storage engine poisoned")
            .set_committed(committed)
            .map_err(|e| storage_err(&e))?;
        // Publish for the status task; overwrite semantics, never blocks apply.
        self.committed_tx.send_replace(index);
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<CoordinatorId>>, StorageError<CoordinatorId>> {
        let committed = self
            .core
            .lock()
            .expect("storage engine poisoned")
            .committed();
        committed
            .map(|id| raftpb::log_id_from_pb(std::path::Path::new("manifest"), &id))
            .transpose()
            .map_err(|e| read_err(&e))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<CoordinatorId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // Encode outside the engine lock; the hot path inside it is frame +
        // append + one fsync (ADR 0002 group commit).
        let encoded: Vec<EncodedEntry> = entries
            .into_iter()
            .map(|entry| EncodedEntry {
                id: super::container::FrameLogId {
                    index: entry.log_id.index,
                    term: entry.log_id.leader_id.term,
                    node_id: entry.log_id.leader_id.node_id,
                },
                payload: raftpb::entry_to_bytes(&entry),
            })
            .collect();
        let result = blocking(&self.core, move |core| core.append_batch(&encoded)).await;
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                callback.log_io_completed(Err(io::Error::new(e.kind(), e.to_string())));
                Err(storage_err(&e))
            }
        }
    }

    async fn truncate(
        &mut self,
        log_id: LogId<CoordinatorId>,
    ) -> Result<(), StorageError<CoordinatorId>> {
        blocking(&self.core, move |core| core.truncate(log_id.index))
            .await
            .map_err(|e| storage_err(&e))
    }

    async fn purge(
        &mut self,
        log_id: LogId<CoordinatorId>,
    ) -> Result<(), StorageError<CoordinatorId>> {
        let upto = raftpb::log_id_to_pb(&log_id);
        blocking(&self.core, move |core| core.purge(upto))
            .await
            .map_err(|e| storage_err(&e))
    }
}
