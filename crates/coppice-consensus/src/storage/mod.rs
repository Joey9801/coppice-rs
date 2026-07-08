//! The segment storage engine: openraft's durable log and state-machine
//! stores over append-only segment files (ADRs 0002, 0015, 0016, 0017, 0018).
//!
//! # Layout (ADR 0017)
//!
//! ```text
//! <data-dir>/
//!   LOCK                    # advisory lock, held for the process lifetime
//!   manifest                # atomic-swap, the pessimistic structural truth
//!   vote                    # atomic-swap Raft vote
//!   log/<start-index>.seg   # append-only segments, framed LogEntry records
//!   snap/<snapshot-id>.snap # ADR 0018 sharded-section snapshot containers
//! ```
//!
//! # Shape of the module
//!
//! - [`engine`]: the synchronous engine — every byte of durable state, every
//!   ordering decision, all of it through the `fs` seam so the crash suite
//!   sees everything. `docs/architecture/storage-engine.md` documents the
//!   formats and the recovery algorithm.
//! - [`container`] / [`snapshot`]: the ADR 0015 header + record framing and
//!   the ADR 0018 snapshot container codec.
//! - [`log`] / [`sm`]: thin async adapters implementing
//!   [`openraft::storage::RaftLogStorage`] and
//!   [`openraft::storage::RaftStateMachine`] over one shared engine — shared
//!   because the manifest is the single durable home of both the segment
//!   list and the snapshot pointer.
//!
//! The lower layers (`core`, `container`, `snapshot`) are exported for the
//! crash suite and the ADR 0018 benches, which drive the engine through the
//! synchronous surface deliberately (deterministic crash points need no
//! executor in the loop).
//!
//! # Opening a store
//!
//! [`init`] stamps an empty directory (ADR 0016); [`open`] runs recovery and
//! returns a [`Recovered`], which splits into the two openraft stores once
//! the caller has an apply task: either its own loop speaking the
//! [`ApplyRequest`](crate::ApplyRequest) protocol (the coordinator runtime),
//! or [`Recovered::into_stores_with_local_apply_task`] which spawns the
//! canonical [`run_apply_task`] loop (tests, tools).

mod container;
mod engine;
mod log;
pub(crate) mod raftpb;
mod sm;
mod snapshot;

pub use container::{FrameLogId, CONTAINER_VERSION};
pub use engine::{EncodedEntry, StorageCore, StorageOptions};
pub use log::{SegmentLogReader, SegmentLogStorage};
pub use sm::{run_apply_task, SegmentSnapshotBuilder, StateMachineStore};

/// Container/framing internals, exported for the storage test suites and
/// the ADR 0018 benches.
///
/// Not a stable API.
pub mod raw {
    pub use super::container::{
        check_header, fail_stop, frame_entry, frame_record, header, parse_entry, read_record,
        FrameStep, ENTRY_OVERHEAD, HEADER_LEN, MANIFEST_MAGIC, RECORD_OVERHEAD, SEGMENT_MAGIC,
        SNAPSHOT_FOOTER_MAGIC, SNAPSHOT_MAGIC, VOTE_MAGIC,
    };
    pub use super::snapshot::{
        assemble_container, decode_state, encode_state, section_bytes, validate_container,
        RawSection, ENCODING_PROTOBUF_LD,
    };
}

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use openraft::{BasicNode, LogId, StoredMembership};
use tokio::sync::mpsc;

use coppice_proto::convert::state_from_records;
use coppice_state::StateMachine;

use crate::adapter::{ApplyRequest, APPLY_CHANNEL_CAPACITY};
use crate::fs::Fs;
use crate::CoordinatorId;

/// Initialize an empty data directory: `log/`, `snap/`, and an
/// identity-stamped manifest (ADR 0016).
///
/// The instance UUID is minted here — a new one for every directory life, so
/// "same node id, different life" is distinguishable in forensics.
pub fn init<F: Fs>(fs: &F, options: &StorageOptions) -> io::Result<()> {
    StorageCore::init(fs, options, *uuid::Uuid::new_v4().as_bytes())
}

/// Open a data directory through full recovery (ADR 0017), rebuilding the
/// applied state from the current snapshot (ADR 0016).
///
/// Log replay from the snapshot index happens through openraft's startup
/// path, driven by the manifest's best-effort committed index — one apply
/// path, not two.
pub fn open<F: Fs>(fs: F, options: StorageOptions) -> io::Result<Recovered<F>> {
    let shards = options.snapshot_shards;
    let cluster_uuid = options.cluster_uuid;
    let core = StorageCore::open(fs, options)?;

    let (state, last_applied, membership) = match core.current_snapshot()? {
        Some((meta, bytes)) => {
            let path = Path::new("snap");
            let (_, records) = snapshot::decode_state(path, &bytes)?;
            let state = state_from_records(records).map_err(|e| {
                container::fail_stop_file(path, format!("snapshot records do not rebuild: {e}"))
            })?;
            let meta = sm::openraft_meta(&meta)?;
            (state, meta.last_log_id, meta.last_membership)
        }
        None => (StateMachine::default(), None, StoredMembership::default()),
    };

    Ok(Recovered {
        core: Arc::new(Mutex::new(core)),
        state,
        last_applied,
        membership,
        shards,
        cluster_uuid,
    })
}

/// A recovered store, one step short of the openraft pair: the caller
/// decides who runs the apply task that will own [`Recovered::state`].
pub struct Recovered<F: Fs> {
    core: Arc<Mutex<StorageCore<F>>>,
    /// The state machine rebuilt from the current snapshot; hand it to the
    /// apply task.
    pub state: StateMachine,
    /// Raft coordinates of `state` (from the snapshot meta).
    pub last_applied: Option<LogId<CoordinatorId>>,
    /// Membership as of `last_applied`.
    pub membership: StoredMembership<CoordinatorId, BasicNode>,
    shards: u32,
    cluster_uuid: [u8; 16],
}

impl<F: Fs> Recovered<F> {
    /// Split into the openraft stores, wiring the state-machine store to an
    /// apply task the caller owns.
    ///
    /// The caller must have seeded that task with [`Recovered::state`] (and
    /// applied-index `last_applied.map(|l| l.index)`).
    pub fn into_stores(
        self,
        apply_tx: mpsc::Sender<ApplyRequest>,
    ) -> (SegmentLogStorage<F>, StateMachineStore<F>) {
        let log = SegmentLogStorage::new(Arc::clone(&self.core));
        let sm = StateMachineStore::new(
            self.core,
            apply_tx,
            self.last_applied,
            self.membership,
            self.shards,
            self.cluster_uuid,
        );
        (log, sm)
    }

    /// Split into the openraft stores, spawning the canonical
    /// [`run_apply_task`] loop on the current tokio runtime.
    ///
    /// For tests and tools; the coordinator runtime wraps the same loop with
    /// view and status publication.
    pub fn into_stores_with_local_apply_task(self) -> (SegmentLogStorage<F>, StateMachineStore<F>) {
        let (tx, rx) = mpsc::channel(APPLY_CHANNEL_CAPACITY);
        let state = self.state.clone();
        tokio::spawn(run_apply_task(state, rx));
        self.into_stores(tx)
    }
}
