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
pub use engine::{
    read_formation_marks, read_manifest_stamp, EncodedEntry, FormationMarks, ManifestStamp,
    StorageCore, StorageOptions,
};
pub use log::{SegmentLogReader, SegmentLogStorage};
pub use sm::{run_apply_task, SegmentSnapshotBuilder, SnapshotFile, StateMachineStore};

pub(crate) fn describe_metrics() {
    sm::describe_metrics();
}

pub(crate) fn gather_metrics() {
    sm::gather_metrics();
}

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
        assemble_container, decode_state, decode_state_file, encode_state, section_bytes,
        validate_container, validate_container_file, write_state, write_state_direct,
        ContainerWriter, RawSection, ENCODING_PROTOBUF_LD,
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
/// Both halves of the node's identity are minted here (ADR 0025): the
/// allocate-once Raft node id this directory will carry for its whole life
/// (returned, so the caller can surface it to the operator), and the
/// instance UUID — a new one for every directory life, so "same node id,
/// different life" is distinguishable in forensics.
pub fn init<F: Fs>(fs: &F, options: &StorageOptions) -> io::Result<CoordinatorId> {
    init_with_formation(fs, options, FormationMarks::default())
}

/// Initialize an empty data directory *and* record a formation intent
/// (ADR 0037 §3 step 2).
///
/// The intent is the first durable act of `coppice coordinator init` and is
/// what makes every later crash identifiable: until step 7 stamps the
/// completion marker, this directory restarts into the `formation-failed`
/// phase rather than into a plausible-looking cluster missing its day-0
/// state. `at_us` is the wall-clock stamp the failed phase reports.
pub fn init_forming<F: Fs>(
    fs: &F,
    options: &StorageOptions,
    at_us: i64,
) -> io::Result<CoordinatorId> {
    init_with_formation(
        fs,
        options,
        FormationMarks {
            intent_at_us: Some(at_us),
            complete_at_us: None,
        },
    )
}

/// Initialize an empty data directory for a replica **joining** an already-formed
/// cluster (ADR 0037 §6), recording both formation markers at once.
///
/// A joiner's history id came from the cluster it probed, not from its own
/// config, so the ADR 0016 config-vs-stamp wrong-volume check cannot apply to
/// this directory on any later start — and the `formation_complete` marker is
/// exactly the signal that says so ([`crate::storage::FormationMarks`],
/// consulted by the coordinator's `resumed_history`).
///
/// Both markers land in the *same* manifest write rather than intent-now,
/// complete-later, because there is no window here worth reporting: formation
/// happened on another node, and a joiner that dies mid-start has nothing
/// half-done to diagnose — it simply has an initialized directory that will
/// resume. Stamping only the intent would instead leave it fail-stopped in
/// `formation-failed`, which would be a lie about what went wrong.
pub fn init_joined<F: Fs>(
    fs: &F,
    options: &StorageOptions,
    at_us: i64,
) -> io::Result<CoordinatorId> {
    init_with_formation(
        fs,
        options,
        FormationMarks {
            intent_at_us: Some(at_us),
            complete_at_us: Some(at_us),
        },
    )
}

fn init_with_formation<F: Fs>(
    fs: &F,
    options: &StorageOptions,
    formation: FormationMarks,
) -> io::Result<CoordinatorId> {
    let node_id = mint_node_id();
    StorageCore::init(
        fs,
        options,
        node_id,
        *uuid::Uuid::new_v4().as_bytes(),
        formation,
    )?;
    Ok(node_id)
}

/// Stamps the ADR 0037 §3 `formation_complete` marker through the open
/// store.
///
/// Handed out by [`Recovered::formation_stamp`] before the stores move into
/// openraft, so formation's last step reaches the same engine instance that
/// owns the manifest — see
/// [`StorageCore::mark_formation_complete`](engine::StorageCore::mark_formation_complete).
pub trait FormationStamp: Send + Sync {
    /// Stamp the marker; idempotent, and refused on a directory recording no
    /// formation intent.
    fn mark_formation_complete(&self, at_us: i64) -> io::Result<()>;
}

impl<F: Fs + Send + 'static> FormationStamp for Mutex<StorageCore<F>> {
    fn mark_formation_complete(&self, at_us: i64) -> io::Result<()> {
        let mut core = self
            .lock()
            .map_err(|_| io::Error::other("storage engine mutex poisoned"))?;
        core.mark_formation_complete(at_us)
    }
}

/// Mint a random allocate-once Raft identity (ADR 0025).
///
/// 64 uniform bits (the XOR of a v4 uuid's two halves — no extra RNG
/// dependency). Uniqueness rests on collision improbability across a
/// cluster's whole membership history, exactly like entity ids; ids are
/// never reused even after the node leaves (ADR 0016).
fn mint_node_id() -> CoordinatorId {
    let (hi, lo) = uuid::Uuid::new_v4().as_u64_pair();
    hi ^ lo
}

/// Open a data directory through full recovery (ADR 0017), rebuilding the
/// applied state from the current snapshot (ADR 0016).
///
/// Log replay from the snapshot index happens through openraft's startup
/// path, driven by the manifest's best-effort committed index — one apply
/// path, not two.
pub fn open<F: Fs>(fs: F, options: StorageOptions) -> io::Result<Recovered<F>> {
    let shards = options.snapshot_shards;
    let history_id = options.history_id;
    let core = StorageCore::open(fs, options)?;
    let node_id = core.node_id();
    let instance_uuid = core.instance_uuid();

    // Rebuild from the snapshot file in bounded memory: streaming validation
    // already ran inside `current_snapshot_reader`, so only the per-section
    // record decode remains (ADR 0018).
    let (state, last_applied, membership) = match core.current_snapshot_reader()? {
        Some((meta, index, file)) => {
            let path = Path::new("snap");
            let records = snapshot::decode_records_file(path, &*file, &index)?;
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
        history_id,
        node_id,
        instance_uuid,
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
    /// The allocate-once Raft identity stamped in the manifest (ADR 0025):
    /// the directory, not config, is the authority on which replica this is.
    pub node_id: CoordinatorId,
    /// The instance UUID stamped when this directory was initialized (ADR
    /// 0025); surfaced through `/readyz` (ADR 0037 §9).
    pub instance_uuid: [u8; 16],
    shards: u32,
    history_id: [u8; 16],
}

impl<F: Fs> Recovered<F> {
    /// A handle that stamps the `formation_complete` marker through this
    /// open engine (ADR 0037 §3 step 7).
    ///
    /// Taken before [`into_stores`](Recovered::into_stores) consumes the
    /// recovered store, so formation's last step still reaches the engine
    /// once openraft owns it.
    pub fn formation_stamp(&self) -> Arc<dyn FormationStamp>
    where
        F: Send + 'static,
    {
        Arc::clone(&self.core) as Arc<dyn FormationStamp>
    }

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
            self.history_id,
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
