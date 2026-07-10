//! The synchronous storage engine: segments, manifest, vote, snapshot files
//! (ADRs 0002, 0015–0018).
//!
//! Everything durable happens here, through the [`Fs`] seam and nothing else,
//! so the crash suite observes every durability decision. The engine is
//! synchronous on purpose (see `fs.rs`): openraft's async traits are bridged
//! one layer up (`log.rs` / `sm.rs`) via `spawn_blocking` over a shared
//! `Mutex<StorageCore>`. A dedicated writer thread was considered and
//! deliberately not built: openraft 0.9 serializes all storage write IO
//! through its core loop, so cross-call group commit never materializes — the
//! group in "group commit" is the batch openraft hands to one `append` call,
//! and a mutex plus `spawn_blocking` gives the same fsync schedule with less
//! machinery.
//!
//! # Ordering rules (ADR 0017)
//!
//! The manifest is the pessimistic truth: it may claim less log than
//! physically exists, never more.
//!
//! - **Append**: segment writes + one `sync_data` acknowledge the batch; the
//!   manifest is never touched on this path.
//! - **Segment create (rotation / first append)**: the segment file and its
//!   directory entry are made durable *before* the manifest claims it.
//! - **Suffix truncation / purge**: the manifest is written and fsynced
//!   *before* any segment file is deleted.
//! - **Snapshot install**: the snapshot file is made durable *before* the
//!   manifest pointer flips; the previous snapshot is deleted only *after*
//!   the flip is durable.
//! - **Recovery**: deletes whatever the manifest does not claim; self-heals a
//!   torn tail only past the manifest's claims; fail-stops on everything
//!   else.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use prost::Message;

use coppice_proto::pb::raft::v1 as pbraft;
use coppice_proto::pb::storage::v1 as pbstorage;

use crate::fs::{read_to_vec, write_atomic, Fs, FsFile};

use super::container::{
    self, check_header, fail_stop, fail_stop_file, fail_stop_on_shape, frame_entry, frame_record,
    header, parse_entry, read_record, FrameLogId, FrameStep, HEADER_LEN, MANIFEST_MAGIC,
    SEGMENT_MAGIC, VOTE_MAGIC,
};
use super::snapshot;

/// Node-local configuration of the engine plus the cluster identity the
/// directory must carry (ADR 0016).
///
/// A cluster-stamp mismatch fail-stops at open. The *node* id is not an
/// input: it is minted at init, stamped into the manifest, and read back at
/// open — the directory alone is the authority on which replica it is
/// (ADR 0025).
#[derive(Debug, Clone)]
pub struct StorageOptions {
    /// The cluster this replica belongs to; stamped at `cluster init`.
    pub cluster_uuid: [u8; 16],
    /// Size threshold past which the active segment is sealed and the next
    /// append opens a fresh one.
    pub segment_max_bytes: u64,
    /// Hash-shards per snapshot section kind (ADR 0018).
    pub snapshot_shards: u32,
}

impl StorageOptions {
    pub fn new(cluster_uuid: [u8; 16]) -> StorageOptions {
        StorageOptions {
            cluster_uuid,
            segment_max_bytes: 64 << 20,
            snapshot_shards: 4,
        }
    }
}

/// One log entry already encoded as its durable `LogEntry` payload, plus the
/// frame-level log id.
///
/// The core never decodes payloads (ADR 0018).
#[derive(Debug, Clone)]
pub struct EncodedEntry {
    pub id: FrameLogId,
    pub payload: Vec<u8>,
}

/// What the manifest claims (the domain view of `coppice.storage.v1.Manifest`).
#[derive(Debug, Clone)]
struct ManifestState {
    cluster_uuid: [u8; 16],
    node_id: u64,
    instance_uuid: [u8; 16],
    /// Ascending start indices; `log/<start>.seg`.
    segments: Vec<u64>,
    purge_floor: Option<FrameLogId>,
    logical_end: Option<FrameLogId>,
    snapshot_id: Option<String>,
    committed_index: Option<u64>,
}

/// entry index -> (byte offset of frame, total frame length).
type OffsetTable = BTreeMap<u64, (u64, u32)>;

/// Per-segment in-memory state: the entry offset table, built by scanning
/// (eagerly for the tail at open, lazily for sealed segments on first read).
#[derive(Debug, Default)]
struct SegmentInfo {
    entries: Option<OffsetTable>,
}

/// The open active segment appends go to.
struct ActiveSegment<File> {
    start: u64,
    file: File,
    len: u64,
}

/// The engine.
///
/// One per data directory; the `LOCK` file is held for its lifetime, so a
/// second opener is refused (ADR 0017).
pub struct StorageCore<F: Fs> {
    fs: F,
    options: StorageOptions,
    manifest: ManifestState,
    segments: BTreeMap<u64, SegmentInfo>,
    active: Option<ActiveSegment<F::File>>,
    /// The log id of the last *live* entry; equals the purge floor when the
    /// log is empty but purged, `None` when nothing was ever appended.
    last_log_id: Option<FrameLogId>,
    vote: Option<pbraft::Vote>,
    committed: Option<FrameLogId>,
    snapshot_seq: u64,
    frame_buf: Vec<u8>,
    _lock: F::Lock,
}

/// Chunk size of the streaming snapshot copy in
/// [`StorageCore::install_snapshot_from`]: the only per-install allocation,
/// however large the container (ADR 0018).
const SNAPSHOT_COPY_CHUNK: usize = 1 << 20;

/// The install-snapshot receive spool: where a streamed container lands
/// frame by frame before adoption. Never claimed by the manifest, so a crash
/// mid-receive leaves an orphan the recovery sweep deletes (ADR 0017).
const RECEIVE_SPOOL: &str = "snap/receiving.tmp";

fn seg_path(start: u64) -> PathBuf {
    PathBuf::from(format!("log/{start}.seg"))
}

fn seg_name(start: u64) -> String {
    format!("{start}.seg")
}

fn snap_path(id: &str) -> PathBuf {
    PathBuf::from(format!("snap/{id}.snap"))
}

fn lid_to_pb(id: FrameLogId) -> pbraft::LogId {
    pbraft::LogId {
        leader_id: Some(pbraft::LeaderId {
            term: id.term,
            node_id: id.node_id,
        }),
        index: id.index,
    }
}

fn lid_from_pb(path: &Path, id: &pbraft::LogId) -> io::Result<FrameLogId> {
    let leader = id
        .leader_id
        .as_ref()
        .ok_or_else(|| fail_stop_file(path, "LogId missing leader_id"))?;
    Ok(FrameLogId {
        index: id.index,
        term: leader.term,
        node_id: leader.node_id,
    })
}

/// A snapshot id becomes a file name; refuse anything that could escape
/// `snap/` or collide with the `.tmp` convention.
fn check_snapshot_id(id: &str) -> io::Result<()> {
    let ok = !id.is_empty()
        && id.len() <= 128
        && !id.ends_with(".tmp")
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
        && !id.starts_with('.');
    if ok {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("snapshot id {id:?} is not a safe file name"),
        ))
    }
}

impl ManifestState {
    fn to_pb(&self) -> pbstorage::Manifest {
        pbstorage::Manifest {
            cluster_uuid: self.cluster_uuid.to_vec(),
            node_id: self.node_id,
            instance_uuid: self.instance_uuid.to_vec(),
            segments: self
                .segments
                .iter()
                .map(|&start| pbstorage::Segment { start_index: start })
                .collect(),
            purge_floor: self.purge_floor.map(lid_to_pb),
            logical_end: self.logical_end.map(lid_to_pb),
            snapshot: self
                .snapshot_id
                .clone()
                .map(|snapshot_id| pbstorage::SnapshotPointer { snapshot_id }),
            committed_index: self.committed_index,
        }
    }

    fn from_pb(path: &Path, manifest: pbstorage::Manifest) -> io::Result<ManifestState> {
        let uuid = |bytes: &[u8], what: &str| -> io::Result<[u8; 16]> {
            bytes
                .try_into()
                .map_err(|_| fail_stop_file(path, format!("{what} is not 16 raw UUID bytes")))
        };
        let segments: Vec<u64> = manifest.segments.iter().map(|s| s.start_index).collect();
        if !segments.windows(2).all(|w| w[0] < w[1]) {
            return Err(fail_stop_file(
                path,
                "segment starts are not strictly ascending",
            ));
        }
        Ok(ManifestState {
            cluster_uuid: uuid(&manifest.cluster_uuid, "cluster_uuid")?,
            node_id: manifest.node_id,
            instance_uuid: uuid(&manifest.instance_uuid, "instance_uuid")?,
            segments,
            purge_floor: manifest
                .purge_floor
                .as_ref()
                .map(|id| lid_from_pb(path, id))
                .transpose()?,
            logical_end: manifest
                .logical_end
                .as_ref()
                .map(|id| lid_from_pb(path, id))
                .transpose()?,
            snapshot_id: manifest.snapshot.map(|p| p.snapshot_id),
            committed_index: manifest.committed_index,
        })
    }
}

impl<F: Fs> StorageCore<F> {
    /// Initialize an empty data directory: `log/`, `snap/`, and an
    /// identity-stamped manifest claiming nothing (ADR 0016 / 0017).
    ///
    /// `node_id` is the freshly-minted allocate-once identity this directory
    /// will carry for its whole life (ADR 0025).
    ///
    /// Refuses a directory that already has a manifest. Not crash-armed by
    /// the crash suite (initialization precedes any acknowledged state).
    pub fn init(
        fs: &F,
        options: &StorageOptions,
        node_id: u64,
        instance_uuid: [u8; 16],
    ) -> io::Result<()> {
        if fs.exists(Path::new("manifest"))? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "data directory is already initialized (manifest exists)",
            ));
        }
        fs.create_dir_all(Path::new("log"))?;
        fs.create_dir_all(Path::new("snap"))?;
        fs.sync_dir(Path::new(""))?;
        let manifest = ManifestState {
            cluster_uuid: options.cluster_uuid,
            node_id,
            instance_uuid,
            segments: Vec::new(),
            purge_floor: None,
            logical_end: None,
            snapshot_id: None,
            committed_index: None,
        };
        write_manifest(fs, &manifest)
    }

    /// Open a data directory through the full ADR 0017 recovery procedure:
    ///
    /// 1. Take the `LOCK`; read and validate the manifest (header, CRC,
    ///    identity stamps).
    /// 2. Delete orphan segment/snapshot/temp files the manifest does not
    ///    claim.
    /// 3. Open claimed segments; verify headers and the chain of start
    ///    indices.
    /// 4. Scan the tail segment (up to the logical end, if set); self-heal a
    ///    torn final record only past the manifest's claims; fail-stop on any
    ///    other damage.
    /// 5. Derive vote, purge floor, log range, and snapshot for openraft.
    pub fn open(fs: F, options: StorageOptions) -> io::Result<StorageCore<F>> {
        // Step 1: lock, then the manifest.
        let lock = fs.lock(Path::new("LOCK"))?;
        let manifest_path = Path::new("manifest");
        if !fs.exists(manifest_path)? {
            return Err(fail_stop_file(
                manifest_path,
                "no manifest: refusing to start on an uninitialized directory (run init)",
            ));
        }
        let bytes = read_to_vec(&fs, manifest_path)?;
        check_header(manifest_path, &bytes, MANIFEST_MAGIC)?;
        let (payload, _) = read_record(manifest_path, &bytes, HEADER_LEN)?;
        let manifest = pbstorage::Manifest::decode(payload)
            .map_err(|e| fail_stop_file(manifest_path, format!("manifest does not decode: {e}")))?;
        let manifest = ManifestState::from_pb(manifest_path, manifest)?;
        if manifest.cluster_uuid != options.cluster_uuid {
            return Err(fail_stop_file(
                manifest_path,
                format!(
                    "identity stamp mismatch: directory is stamped for cluster {:02x?}, this \
                     process is configured for cluster {:02x?} (wrong volume or cross-cluster \
                     mixup, ADR 0016)",
                    manifest.cluster_uuid, options.cluster_uuid
                ),
            ));
        }

        // Step 2: orphan sweep. Idempotent — an un-synced delete may have
        // resurrected files a previous recovery already removed.
        sweep_dir(&fs, Path::new("log"), |name| {
            manifest.segments.iter().any(|&s| name == seg_name(s))
        })?;
        sweep_dir(&fs, Path::new("snap"), |name| {
            manifest
                .snapshot_id
                .as_deref()
                .is_some_and(|id| name == format!("{id}.snap"))
        })?;
        sweep_dir(&fs, Path::new(""), |name| !name.ends_with(".tmp"))?;

        // Step 3: claimed segments exist, carry valid headers, and chain.
        // (Ascending starts were validated in the manifest decode.)
        let mut segments: BTreeMap<u64, SegmentInfo> = BTreeMap::new();
        for (i, &start) in manifest.segments.iter().enumerate() {
            let path = seg_path(start);
            let file = fs
                .open_read(&path)
                .map_err(|e| fail_stop_on_shape(&path, 0, "claimed segment missing", e))?;
            let is_tail = i + 1 == manifest.segments.len();
            if !is_tail {
                let mut hdr = [0u8; HEADER_LEN];
                file.read_exact_at(0, &mut hdr)
                    .map_err(|e| fail_stop_on_shape(&path, 0, "segment header unreadable", e))?;
                check_header(&path, &hdr, SEGMENT_MAGIC)?;
            }
            segments.insert(start, SegmentInfo::default());
        }

        let mut core = StorageCore {
            fs,
            options,
            manifest,
            segments,
            active: None,
            last_log_id: None,
            vote: None,
            committed: None,
            snapshot_seq: 1,
            frame_buf: Vec::new(),
            _lock: lock,
        };

        // Step 4: scan the tail segment, healing only past the claims.
        let mut tail_last: Option<FrameLogId> = None;
        if let Some(&tail_start) = core.manifest.segments.last() {
            let (entries, last_id) = core.scan_tail(tail_start)?;
            tail_last = last_id;
            core.segments
                .get_mut(&tail_start)
                .expect("tail is claimed")
                .entries = Some(entries);
        }

        // Step 5: derive the reported state.
        core.last_log_id = match (core.manifest.logical_end, tail_last) {
            (Some(le), _) => Some(le),
            (None, Some(last)) => Some(last),
            (None, None) => {
                // Tail empty or no segments. With more than one claimed
                // segment the previous one holds the last entry (rotation
                // names segments contiguously); otherwise fall back to the
                // purge floor.
                let n = core.manifest.segments.len();
                if n >= 2 {
                    let prev = core.manifest.segments[n - 2];
                    let tail = core.manifest.segments[n - 1];
                    core.ensure_scanned(prev)?;
                    core.frame_id_at(prev, tail - 1)?
                } else {
                    core.manifest.purge_floor
                }
            }
        };
        if let Some(floor) = core.manifest.purge_floor {
            if core.last_log_id.map(|l| l.index) < Some(floor.index) {
                core.last_log_id = Some(floor);
            }
        }

        // Vote (atomic-swap file; absent on a fresh directory).
        let vote_path = Path::new("vote");
        if core.fs.exists(vote_path)? {
            let bytes = read_to_vec(&core.fs, vote_path)?;
            check_header(vote_path, &bytes, VOTE_MAGIC)?;
            let (payload, _) = read_record(vote_path, &bytes, HEADER_LEN)?;
            let vote = pbraft::Vote::decode(payload)
                .map_err(|e| fail_stop_file(vote_path, format!("vote does not decode: {e}")))?;
            core.vote = Some(vote);
        }

        // Best-effort committed index (ADR 0017): resolve to a full log id
        // when the entry is still readable; correctness never depends on it.
        if let Some(ci) = core.manifest.committed_index {
            core.committed = core.resolve_index(ci)?;
        }

        // Reopen the tail for appending unless a truncation sealed it.
        if core.manifest.logical_end.is_none() {
            if let Some(&tail_start) = core.manifest.segments.last() {
                let file = core.fs.open_append(&seg_path(tail_start))?;
                let len = file.len()?;
                core.active = Some(ActiveSegment {
                    start: tail_start,
                    file,
                    len,
                });
            }
        }

        // Continue snapshot-id minting past the current pointer.
        if let Some(id) = &core.manifest.snapshot_id {
            if let Ok(seq) = u64::from_str_radix(id, 16) {
                core.snapshot_seq = seq + 1;
            }
        }

        Ok(core)
    }

    /// The allocate-once Raft identity stamped into this directory's
    /// manifest at init (ADR 0025).
    pub fn node_id(&self) -> u64 {
        self.manifest.node_id
    }

    // ---- log reads ----------------------------------------------------

    /// The `(last_purged, last)` pair openraft's `get_log_state` reports.
    pub fn log_state(&self) -> (Option<pbraft::LogId>, Option<pbraft::LogId>) {
        (
            self.manifest.purge_floor.map(lid_to_pb),
            self.last_log_id.map(lid_to_pb),
        )
    }

    pub fn vote(&self) -> Option<&pbraft::Vote> {
        self.vote.as_ref()
    }

    pub fn committed(&self) -> Option<pbraft::LogId> {
        self.committed.map(lid_to_pb)
    }

    /// Record the committed index in memory.
    ///
    /// Persisted opportunistically at the next structural manifest write
    /// (rotation, snapshot) per ADR 0017 — never on its own, the append path
    /// stays manifest-free.
    pub fn set_committed(&mut self, committed: Option<pbraft::LogId>) -> io::Result<()> {
        self.committed = committed
            .as_ref()
            .map(|id| lid_from_pb(Path::new("manifest"), id))
            .transpose()?;
        Ok(())
    }

    /// First live index, if any entry is live: just above the purge floor,
    /// but never below the first claimed segment (a gap append restarts the
    /// log above the floor).
    fn first_live(&self) -> Option<u64> {
        let last = self.last_log_id?;
        let first_start = *self.manifest.segments.first()?;
        let lo = match self.manifest.purge_floor {
            Some(floor) => (floor.index + 1).max(first_start),
            None => first_start,
        };
        (lo <= last.index).then_some(lo)
    }

    /// Read the framed payload bytes of the live entries in `[lo, hi)`.
    pub fn read_payloads(&mut self, lo: u64, hi: u64) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let Some(first) = self.first_live() else {
            return Ok(Vec::new());
        };
        let last = self
            .last_log_id
            .expect("live entries imply a last id")
            .index;
        let lo = lo.max(first);
        let hi = hi.min(last + 1);
        let mut out = Vec::new();
        let mut idx = lo;
        while idx < hi {
            let (&start, _) = self.segments.range(..=idx).next_back().ok_or_else(|| {
                fail_stop_file(Path::new("log"), format!("no segment holds entry {idx}"))
            })?;
            self.ensure_scanned(start)?;
            let path = seg_path(start);
            let file = self.fs.open_read(&path)?;
            let info = self.segments.get(&start).expect("segment exists");
            let table = info.entries.as_ref().expect("scanned above");
            let before = idx;
            while idx < hi {
                let Some(&(offset, frame_len)) = table.get(&idx) else {
                    break;
                };
                let mut frame = vec![0u8; frame_len as usize];
                file.read_exact_at(offset, &mut frame).map_err(|e| {
                    fail_stop_on_shape(&path, offset, format!("entry {idx} unreadable"), e)
                })?;
                match parse_entry(&frame, 0) {
                    FrameStep::Entry { id, payload, .. } if id.index == idx => {
                        out.push((idx, payload.to_vec()));
                    }
                    _ => {
                        return Err(fail_stop(
                            &path,
                            offset,
                            format!("entry {idx} failed its CRC32C on read"),
                        ))
                    }
                }
                idx += 1;
            }
            if idx == before {
                return Err(fail_stop_file(
                    &path,
                    format!("segment {start} does not hold live entry {idx}"),
                ));
            }
        }
        Ok(out)
    }

    /// The frame-level log id of one live entry.
    pub fn frame_id(&mut self, index: u64) -> io::Result<Option<FrameLogId>> {
        Ok(self.resolve_index(index)?.filter(|id| id.index == index))
    }

    /// Resolve `index` to a log id: the purge floor if at/below it, the
    /// framed id if live, `None` if past the end or before any log.
    fn resolve_index(&mut self, index: u64) -> io::Result<Option<FrameLogId>> {
        if let Some(floor) = self.manifest.purge_floor {
            if index <= floor.index {
                return Ok(Some(floor));
            }
        }
        let Some(last) = self.last_log_id else {
            return Ok(None);
        };
        if index > last.index {
            return Ok(None);
        }
        if index == last.index {
            return Ok(Some(last));
        }
        let Some((&start, _)) = self.segments.range(..=index).next_back() else {
            return Ok(None);
        };
        self.ensure_scanned(start)?;
        self.frame_id_at(start, index)
    }

    /// Read the framed log id at `index` inside an already-scanned segment.
    fn frame_id_at(&self, start: u64, index: u64) -> io::Result<Option<FrameLogId>> {
        let Some(info) = self.segments.get(&start) else {
            return Ok(None);
        };
        let Some(table) = info.entries.as_ref() else {
            return Ok(None);
        };
        let Some(&(offset, frame_len)) = table.get(&index) else {
            return Ok(None);
        };
        let path = seg_path(start);
        let file = self.fs.open_read(&path)?;
        let mut frame = vec![0u8; frame_len as usize];
        file.read_exact_at(offset, &mut frame).map_err(|e| {
            fail_stop_on_shape(&path, offset, format!("entry {index} unreadable"), e)
        })?;
        match parse_entry(&frame, 0) {
            FrameStep::Entry { id, .. } if id.index == index => Ok(Some(id)),
            _ => Err(fail_stop(
                &path,
                offset,
                format!("entry {index} failed its CRC32C on read"),
            )),
        }
    }

    // ---- append path ----------------------------------------------------

    /// Append a batch and fsync once; the batch is acknowledged (and visible
    /// in memory) only after the fsync returns (ADR 0002 group commit).
    pub fn append_batch(&mut self, entries: &[EncodedEntry]) -> io::Result<()> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        for (i, entry) in entries.iter().enumerate() {
            if entry.id.index != first.id.index + i as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "append batch indices are not contiguous",
                ));
            }
        }
        if let Some(last) = self.last_log_id {
            // A batch starting at or below the current end replaces the
            // conflicting suffix: openraft's append contract is
            // overwrite-from-batch-start (its reference stores insert by
            // index), which maps exactly onto the ADR 0017 suffix
            // truncation — manifest first, fresh segment for the new bytes.
            if first.id.index <= last.index {
                self.truncate(first.id.index)?;
            } else if first.id.index != last.index + 1 {
                // A gap append. openraft's raft core never produces one (its
                // own invariant forbids holes), but its storage test suite
                // does, with overwrite semantics inherited from its
                // BTreeMap-backed reference store. Segment files cannot hold
                // a hole without poisoning every contiguity invariant, so a
                // gap append replaces the log wholesale — the same manifest-
                // first protocol as a full suffix truncation.
                self.clear_log()?;
            }
        }

        // Seal-and-rotate on batch boundaries once the active segment is
        // over the size threshold.
        if self
            .active
            .as_ref()
            .is_some_and(|a| a.len >= self.options.segment_max_bytes)
        {
            self.active = None;
        }
        if self.active.is_none() {
            self.open_fresh_segment(first.id.index)?;
        }

        let active = self.active.as_mut().expect("opened above");
        self.frame_buf.clear();
        let mut offsets = Vec::with_capacity(entries.len());
        for entry in entries {
            let at = active.len + self.frame_buf.len() as u64;
            let before = self.frame_buf.len();
            frame_entry(entry.id, &entry.payload, &mut self.frame_buf);
            offsets.push((entry.id.index, at, (self.frame_buf.len() - before) as u32));
        }
        active.file.append(&self.frame_buf)?;
        active.file.sync_data()?;

        // Acknowledged: now (and only now) update in-memory state.
        active.len += self.frame_buf.len() as u64;
        let start = active.start;
        let table = self
            .segments
            .get_mut(&start)
            .expect("active segment is claimed")
            .entries
            .get_or_insert_with(BTreeMap::new);
        for (index, offset, frame_len) in offsets {
            table.insert(index, (offset, frame_len));
        }
        self.last_log_id = Some(entries.last().expect("nonempty").id);
        Ok(())
    }

    /// Drop every log claim (manifest first), keeping the purge floor.
    ///
    /// Used only for openraft's gap-append overwrite semantics.
    fn clear_log(&mut self) -> io::Result<()> {
        let dropped = std::mem::take(&mut self.manifest.segments);
        self.manifest.logical_end = None;
        write_manifest(&self.fs, &self.manifest)?;
        self.active = None;
        self.last_log_id = self.manifest.purge_floor;
        self.segments.clear();
        for start in &dropped {
            self.fs.remove_file(&seg_path(*start))?;
        }
        if !dropped.is_empty() {
            self.fs.sync_dir(Path::new("log"))?;
        }
        Ok(())
    }

    /// Seal the active segment; the next append opens a fresh one (and takes
    /// the structural manifest write with it).
    ///
    /// A never-written active segment is left alone so rotation cannot mint
    /// two segments at one index.
    pub fn rotate(&mut self) -> io::Result<()> {
        if let Some(active) = &self.active {
            let has_entries = self
                .last_log_id
                .is_some_and(|last| last.index >= active.start);
            if has_entries {
                self.active = None;
            }
        }
        Ok(())
    }

    /// Create, make durable, and *then* claim `log/<start>.seg` (ADR 0017:
    /// recovery either sees an orphan or a claimed segment, never an
    /// acknowledged entry in an unclaimed file).
    ///
    /// This is the structural event that clears a logical end and records
    /// the best-effort committed index.
    fn open_fresh_segment(&mut self, start: u64) -> io::Result<()> {
        let path = seg_path(start);
        // A leftover file here is always unclaimed (truncation deletes claim
        // first, then file) — replace it.
        if self.fs.exists(&path)? {
            self.fs.remove_file(&path)?;
        }
        let mut file = self.fs.create_new(&path)?;
        file.append(&header(SEGMENT_MAGIC))?;
        file.sync_data()?;
        self.fs.sync_dir(Path::new("log"))?;

        self.manifest.segments.push(start);
        debug_assert!(self.manifest.segments.windows(2).all(|w| w[0] < w[1]));
        self.manifest.logical_end = None;
        self.manifest.committed_index = self.committed.map(|c| c.index);
        write_manifest(&self.fs, &self.manifest)?;

        self.segments.insert(
            start,
            SegmentInfo {
                entries: Some(BTreeMap::new()),
            },
        );
        self.active = Some(ActiveSegment {
            start,
            file,
            len: HEADER_LEN as u64,
        });
        Ok(())
    }

    // ---- vote ----------------------------------------------------------

    /// Persist the vote with the atomic-swap discipline; durable before
    /// returning (Raft correctness depends on votes never rolling back).
    pub fn save_vote(&mut self, vote: &pbraft::Vote) -> io::Result<()> {
        let mut bytes = header(VOTE_MAGIC).to_vec();
        frame_record(&vote.encode_to_vec(), &mut bytes);
        write_atomic(&self.fs, Path::new("vote"), Path::new("vote.tmp"), &bytes)?;
        self.vote = Some(*vote);
        Ok(())
    }

    // ---- structural transitions -----------------------------------------

    /// Suffix truncation (ADR 0017): everything at and above `from` is
    /// discarded.
    ///
    /// Manifest first (logical end + dropped claims, fsynced), physical
    /// deletion strictly after; sealed bytes are never rewritten, and the
    /// next append opens a fresh segment.
    pub fn truncate(&mut self, from: u64) -> io::Result<()> {
        let Some(last) = self.last_log_id else {
            return Ok(());
        };
        if from > last.index {
            return Ok(());
        }

        let floor = self.manifest.purge_floor;
        let empties_log = match floor {
            Some(f) => from <= f.index + 1,
            None => self.manifest.segments.first().map_or(true, |&s| from <= s),
        };
        let (new_last, logical_end, dropped): (Option<FrameLogId>, Option<FrameLogId>, Vec<u64>) =
            if empties_log {
                // Nothing live remains; drop every claim (a segment holding
                // only sub-floor entries carries nothing the log still
                // promises).
                (floor, None, self.manifest.segments.clone())
            } else {
                let new_last = self.resolve_index(from - 1)?.ok_or_else(|| {
                    fail_stop_file(
                        Path::new("log"),
                        format!("entry {} vanished while truncating to it", from - 1),
                    )
                })?;
                let dropped: Vec<u64> = self
                    .manifest
                    .segments
                    .iter()
                    .copied()
                    .filter(|&s| s >= from)
                    .collect();
                (Some(new_last), Some(new_last), dropped)
            };

        // Step 1 (durable before anything else): the manifest stops claiming.
        self.manifest.segments.retain(|s| !dropped.contains(s));
        self.manifest.logical_end = logical_end;
        write_manifest(&self.fs, &self.manifest)?;

        // Sealed-in-place: appends never continue into stale bytes.
        self.active = None;
        self.last_log_id = new_last;
        for start in &dropped {
            self.segments.remove(start);
        }
        if let Some(info) = self
            .manifest
            .segments
            .last()
            .and_then(|s| self.segments.get_mut(s))
        {
            if let Some(table) = info.entries.as_mut() {
                table.retain(|&idx, _| idx < from);
            }
        }

        // Step 2: physical deletion, orphan-safe if we crash mid-way.
        for start in &dropped {
            self.fs.remove_file(&seg_path(*start))?;
        }
        if !dropped.is_empty() {
            self.fs.sync_dir(Path::new("log"))?;
        }
        Ok(())
    }

    /// The segments whose every live entry is at or below `upto` — the set a
    /// purge (or a floor-advancing snapshot install) may delete.
    ///
    /// A segment straddling the floor stays: it still holds live entries
    /// above it.
    fn covered_segments(&self, upto: u64, purges_everything: bool) -> Vec<u64> {
        if purges_everything {
            return self.manifest.segments.clone();
        }
        let starts = &self.manifest.segments;
        starts
            .iter()
            .enumerate()
            .filter(|&(i, _)| {
                let live_end = starts
                    .get(i + 1)
                    .map(|next| next - 1)
                    .or(self.last_log_id.map(|l| l.index))
                    .expect("non-final branch implies a last id");
                live_end <= upto
            })
            .map(|(_, &s)| s)
            .collect()
    }

    /// Purge (ADR 0017): advance the purge floor to `upto` (inclusive),
    /// manifest first, then delete the segments a snapshot fully covers.
    pub fn purge(&mut self, upto: pbraft::LogId) -> io::Result<()> {
        let upto = lid_from_pb(Path::new("manifest"), &upto)?;
        if self
            .manifest
            .purge_floor
            .is_some_and(|f| f.index >= upto.index)
        {
            return Ok(());
        }

        let purges_everything = self.last_log_id.map_or(true, |l| upto.index >= l.index);
        let dropped = self.covered_segments(upto.index, purges_everything);

        // Step 1: the manifest stops claiming, floor advances, fsync.
        self.manifest.purge_floor = Some(upto);
        self.manifest.segments.retain(|s| !dropped.contains(s));
        if purges_everything {
            self.manifest.logical_end = None;
        }
        write_manifest(&self.fs, &self.manifest)?;

        if purges_everything {
            self.last_log_id = Some(upto);
            self.active = None;
        }
        for start in &dropped {
            self.segments.remove(start);
            if self.active.as_ref().is_some_and(|a| a.start == *start) {
                self.active = None;
            }
        }

        // Step 2: physical deletion.
        for start in &dropped {
            self.fs.remove_file(&seg_path(*start))?;
        }
        if !dropped.is_empty() {
            self.fs.sync_dir(Path::new("log"))?;
        }
        Ok(())
    }

    // ---- snapshots -------------------------------------------------------

    /// Mint an id for a locally built snapshot.
    pub fn mint_snapshot_id(&mut self) -> String {
        let id = format!("{:016x}", self.snapshot_seq);
        self.snapshot_seq += 1;
        id
    }

    /// Open a fresh receive spool for a streamed install-snapshot, replacing
    /// any stale one. The caller appends wire chunks to it, then adopts via
    /// [`StorageCore::install_snapshot_from`].
    pub fn begin_snapshot_receive(&mut self) -> io::Result<Box<dyn FsFile>> {
        let path = Path::new(RECEIVE_SPOOL);
        if self.fs.exists(path)? {
            self.fs.remove_file(path)?;
        }
        Ok(Box::new(self.fs.create_new(path)?))
    }

    /// Delete the receive spool, if present. Called after a streamed install
    /// is adopted; a crash before this point leaves an orphan the recovery
    /// sweep removes, so best-effort ordering is fine here.
    pub fn remove_snapshot_spool(&mut self) -> io::Result<()> {
        let path = Path::new(RECEIVE_SPOOL);
        if self.fs.exists(path)? {
            self.fs.remove_file(path)?;
        }
        Ok(())
    }

    /// Open the temp file a locally built snapshot streams into
    /// (`snap/<id>.snap.tmp`), replacing any stale one. The builder writes
    /// the container into it section by section (without holding the engine
    /// lock), then adopts via [`StorageCore::finish_snapshot_build`]. The
    /// temp file is never claimed by the manifest, so a crash mid-build
    /// leaves an orphan the recovery sweep deletes (ADR 0017).
    pub fn begin_snapshot_build(&mut self, id: &str) -> io::Result<Box<dyn FsFile>> {
        check_snapshot_id(id)?;
        let tmp = PathBuf::from(format!("snap/{id}.snap.tmp"));
        if self.fs.exists(&tmp)? {
            self.fs.remove_file(&tmp)?;
        }
        Ok(Box::new(self.fs.create_new(&tmp)?))
    }

    /// Adopt the container streamed into
    /// [`StorageCore::begin_snapshot_build`]'s temp file: streaming
    /// validation of every section CRC (ADR 0016 — nothing durable points at
    /// unvalidated bytes), then fsync, rename, and the same one-swap manifest
    /// flip as the other install entrypoints (ADR 0017). The container is
    /// never materialized in memory.
    ///
    /// A locally built snapshot never advances the purge floor; purging is
    /// openraft's own, separate call.
    pub fn finish_snapshot_build(
        &mut self,
        file: Box<dyn FsFile>,
    ) -> io::Result<pbstorage::SnapshotMeta> {
        let snap_dir_path = Path::new("snap/container");
        let (meta, _) = snapshot::validate_container_file(snap_dir_path, &*file)?;
        let (id, path, tmp) = self.checked_snapshot_paths(snap_dir_path, &meta)?;

        file.sync_data()?;
        drop(file);
        self.fs.rename(&tmp, &path)?;
        self.fs.sync_dir(Path::new("snap"))?;

        self.adopt_snapshot(meta, id, path, false)
    }

    /// Adopt a complete snapshot container held in memory: validate it, make
    /// the file durable, then atomically flip the manifest pointer (the
    /// commit point); the previous snapshot is retained until the flip is
    /// durable and deleted after (ADR 0002/0018).
    ///
    /// With `advance_floor` (the ADR 0016 learner-rebuild install path) the
    /// same manifest write advances the purge floor past everything the
    /// snapshot covers.
    ///
    /// This is the slice entrypoint the crash suite drives with opaque
    /// containers; production never materializes one — a local build streams
    /// through [`StorageCore::begin_snapshot_build`] /
    /// [`StorageCore::finish_snapshot_build`], and a snapshot received over
    /// the wire is adopted from its spool file by
    /// [`StorageCore::install_snapshot_from`].
    pub fn install_snapshot(
        &mut self,
        container: &[u8],
        advance_floor: bool,
    ) -> io::Result<pbstorage::SnapshotMeta> {
        let snap_dir_path = Path::new("snap/container");
        let (meta, _) = snapshot::validate_container(snap_dir_path, container)?;
        let (id, path, tmp) = self.checked_snapshot_paths(snap_dir_path, &meta)?;

        // The snapshot file becomes durable before anything points at it.
        if self.fs.exists(&tmp)? {
            self.fs.remove_file(&tmp)?;
        }
        let mut file = self.fs.create_new(&tmp)?;
        file.append(container)?;
        file.sync_data()?;
        drop(file);
        self.fs.rename(&tmp, &path)?;
        self.fs.sync_dir(Path::new("snap"))?;

        self.adopt_snapshot(meta, id, path, advance_floor)
    }

    /// [`StorageCore::install_snapshot`] from a readable file instead of a
    /// byte slice, in bounded memory: streaming validation of every section
    /// CRC on the source, then a chunked copy into this store's temp file,
    /// fsync, rename, and the same one-swap manifest flip (ADR 0016/0017).
    ///
    /// The source is any [`FsFile`] — the receive spool of a streamed
    /// install, or another store's snapshot file — and is left untouched.
    pub fn install_snapshot_from(
        &mut self,
        source: &dyn FsFile,
        advance_floor: bool,
    ) -> io::Result<pbstorage::SnapshotMeta> {
        let snap_dir_path = Path::new("snap/container");
        let (meta, _) = snapshot::validate_container_file(snap_dir_path, source)?;
        let (id, path, tmp) = self.checked_snapshot_paths(snap_dir_path, &meta)?;

        if self.fs.exists(&tmp)? {
            self.fs.remove_file(&tmp)?;
        }
        let mut file = self.fs.create_new(&tmp)?;
        let len = source.len()?;
        let mut buf = vec![0u8; SNAPSHOT_COPY_CHUNK.min(len.max(1) as usize)];
        let mut at = 0u64;
        while at < len {
            let n = ((len - at) as usize).min(buf.len());
            source.read_exact_at(at, &mut buf[..n])?;
            file.append(&buf[..n])?;
            at += n as u64;
        }
        file.sync_data()?;
        drop(file);
        self.fs.rename(&tmp, &path)?;
        self.fs.sync_dir(Path::new("snap"))?;

        self.adopt_snapshot(meta, id, path, advance_floor)
    }

    /// Identity and file-name checks shared by both install entrypoints:
    /// refuse a foreign cluster's snapshot (ADR 0016), then derive the final
    /// and temp paths for its id.
    fn checked_snapshot_paths(
        &self,
        label: &Path,
        meta: &pbstorage::SnapshotMeta,
    ) -> io::Result<(String, PathBuf, PathBuf)> {
        if meta.cluster_uuid != self.options.cluster_uuid {
            return Err(fail_stop_file(
                label,
                "snapshot carries another cluster's uuid; refusing to adopt it (ADR 0016)",
            ));
        }
        let id = meta.snapshot_id.clone();
        check_snapshot_id(&id)?;
        let path = snap_path(&id);
        let tmp = PathBuf::from(format!("snap/{id}.snap.tmp"));
        Ok((id, path, tmp))
    }

    /// The adoption tail shared by both install entrypoints. Runs once the
    /// snapshot file is durably renamed into place: one atomic manifest swap
    /// flips the pointer (and, with `advance_floor`, the purge floor), then
    /// covered segments and the previous snapshot are deleted (ADR 0017:
    /// delete only after the flip is durable).
    fn adopt_snapshot(
        &mut self,
        meta: pbstorage::SnapshotMeta,
        id: String,
        path: PathBuf,
        advance_floor: bool,
    ) -> io::Result<pbstorage::SnapshotMeta> {
        let previous = self.manifest.snapshot_id.replace(id.clone());
        let mut dropped: Vec<u64> = Vec::new();
        if advance_floor {
            if let Some(last_applied) = meta.last_applied.as_ref() {
                let upto = lid_from_pb(&path, last_applied)?;
                if self
                    .manifest
                    .purge_floor
                    .map_or(true, |f| f.index < upto.index)
                {
                    let purges_everything =
                        self.last_log_id.map_or(true, |l| upto.index >= l.index);
                    dropped = self.covered_segments(upto.index, purges_everything);
                    self.manifest.purge_floor = Some(upto);
                    self.manifest.segments.retain(|s| !dropped.contains(s));
                    if purges_everything {
                        self.manifest.logical_end = None;
                        self.last_log_id = Some(upto);
                        self.active = None;
                    }
                }
            }
        }
        // Best-effort committed index rides along (ADR 0017: recorded at
        // snapshot events).
        self.manifest.committed_index = self.committed.map(|c| c.index);

        // The commit point: one atomic manifest swap.
        write_manifest(&self.fs, &self.manifest)?;

        for start in &dropped {
            self.segments.remove(start);
            if self.active.as_ref().is_some_and(|a| a.start == *start) {
                self.active = None;
            }
            self.fs.remove_file(&seg_path(*start))?;
        }
        if !dropped.is_empty() {
            self.fs.sync_dir(Path::new("log"))?;
        }

        // Delete-after-commit: the previous snapshot outlived the flip.
        if let Some(previous) = previous.filter(|p| *p != id) {
            self.fs.remove_file(&snap_path(&previous))?;
            self.fs.sync_dir(Path::new("snap"))?;
        }

        if let Ok(seq) = u64::from_str_radix(&id, 16) {
            self.snapshot_seq = self.snapshot_seq.max(seq + 1);
        }
        Ok(meta)
    }

    /// Open and fully validate the current snapshot, if the manifest points
    /// at one, returning a read handle — the container is CRC-checked in
    /// streaming reads and never materialized (ADR 0018).
    ///
    /// The manifest only ever points at a durably renamed file, so any
    /// damage here is fail-stop corruption, never a torn write.
    #[allow(clippy::type_complexity)]
    pub fn current_snapshot_reader(
        &self,
    ) -> io::Result<Option<(pbstorage::SnapshotMeta, pbstorage::SectionIndex, Box<dyn FsFile>)>>
    {
        let Some(id) = &self.manifest.snapshot_id else {
            return Ok(None);
        };
        let path = snap_path(id);
        let file = self
            .fs
            .open_read(&path)
            .map_err(|e| fail_stop_on_shape(&path, 0, "claimed snapshot unreadable", e))?;
        let (meta, index) = snapshot::validate_container_file(&path, &file)?;
        Ok(Some((meta, index, Box::new(file))))
    }

    /// Read and fully validate the current snapshot into memory.
    ///
    /// Test/tooling convenience over [`StorageCore::current_snapshot_reader`]
    /// (the crash suite's observer slurps deliberately); production paths
    /// stream through the reader instead.
    pub fn current_snapshot(&self) -> io::Result<Option<(pbstorage::SnapshotMeta, Vec<u8>)>> {
        let Some(id) = &self.manifest.snapshot_id else {
            return Ok(None);
        };
        let path = snap_path(id);
        let bytes = read_to_vec(&self.fs, &path)
            .map_err(|e| fail_stop_on_shape(&path, 0, "claimed snapshot unreadable", e))?;
        let (meta, _) = snapshot::validate_container(&path, &bytes)?;
        Ok(Some((meta, bytes)))
    }

    // ---- recovery internals ---------------------------------------------

    /// Scan the tail segment per recovery step 4.
    ///
    /// Frames at or below the manifest's claims (the logical end, when set)
    /// fail-stop on any damage; a torn or short frame past the claims is the
    /// un-acknowledged tail and is physically truncated (the one place
    /// sealed-bytes-never-rewritten does not apply, because these bytes were
    /// never acknowledged).
    fn scan_tail(&mut self, start: u64) -> io::Result<(OffsetTable, Option<FrameLogId>)> {
        let path = seg_path(start);
        let bytes = read_to_vec(&self.fs, &path)?;
        check_header(&path, &bytes, SEGMENT_MAGIC)?;

        let claim_end = self.manifest.logical_end.map(|le| le.index);
        let mut table = BTreeMap::new();
        let mut last_id = None;
        let mut offset = HEADER_LEN;
        let mut expected = start;
        loop {
            if claim_end.is_some_and(|ce| expected > ce) {
                // Stale truncated-suffix bytes beyond the logical end:
                // durable, sealed-in-place, deliberately not read and never
                // rewritten (ADR 0017).
                break;
            }
            let claimed = claim_end.is_some_and(|ce| expected <= ce);
            match parse_entry(&bytes, offset) {
                FrameStep::Entry { id, next, .. } if id.index == expected => {
                    table.insert(expected, (offset as u64, (next - offset) as u32));
                    last_id = Some(id);
                    offset = next;
                    expected += 1;
                }
                FrameStep::End if claimed => {
                    return Err(fail_stop(
                        &path,
                        offset as u64,
                        format!("segment ends before entry {expected}, which the manifest claims"),
                    ));
                }
                FrameStep::End => break,
                _ if claimed => {
                    return Err(fail_stop(
                        &path,
                        offset as u64,
                        format!("unreadable at entry {expected} inside the committed range"),
                    ));
                }
                _ => {
                    // Torn tail past every claim: self-heal by truncating to
                    // the last good frame boundary, durably.
                    let mut file = self.fs.open_append(&path)?;
                    file.truncate(offset as u64)?;
                    file.sync_data()?;
                    break;
                }
            }
        }
        Ok((table, last_id))
    }

    /// Build the offset table for a sealed segment on first use (recovery
    /// step 3 verified its header already; the full scan is lazy because
    /// recovery only ever scans the tail — ADR 0018 keeps cold starts
    /// replay-bound, not scan-bound).
    fn ensure_scanned(&mut self, start: u64) -> io::Result<()> {
        let is_scanned = self
            .segments
            .get(&start)
            .is_some_and(|info| info.entries.is_some());
        if is_scanned {
            return Ok(());
        }
        let seg_index = self
            .manifest
            .segments
            .iter()
            .position(|&s| s == start)
            .ok_or_else(|| {
                fail_stop_file(Path::new("log"), format!("segment {start} not claimed"))
            })?;
        let next_start = self.manifest.segments.get(seg_index + 1).copied();

        let path = seg_path(start);
        let bytes = read_to_vec(&self.fs, &path)?;
        check_header(&path, &bytes, SEGMENT_MAGIC)?;

        // A sealed segment is fully claimed up to the next segment's start
        // (rotation names segments contiguously); any damage inside that
        // range is corruption of possibly-committed state.
        let live_end = next_start
            .map(|n| n - 1)
            .or(self.manifest.logical_end.map(|le| le.index))
            .or(self.last_log_id.map(|l| l.index))
            .unwrap_or(start);
        let mut table = BTreeMap::new();
        let mut offset = HEADER_LEN;
        let mut expected = start;
        while expected <= live_end {
            match parse_entry(&bytes, offset) {
                FrameStep::Entry { id, next, .. } if id.index == expected => {
                    table.insert(expected, (offset as u64, (next - offset) as u32));
                    offset = next;
                    expected += 1;
                }
                _ => {
                    return Err(fail_stop(
                        &path,
                        offset as u64,
                        format!(
                            "sealed segment unreadable at entry {expected}; sealed segments \
                             are never truncated"
                        ),
                    ));
                }
            }
        }
        self.segments
            .get_mut(&start)
            .expect("claimed segment present")
            .entries = Some(table);
        Ok(())
    }
}

/// One atomic manifest swap: encode, then `write_atomic` (write-new + fsync +
/// rename + parent fsync).
///
/// Every structural fact changes through here and nowhere else (ADR 0017).
fn write_manifest<F: Fs>(fs: &F, manifest: &ManifestState) -> io::Result<()> {
    let mut bytes = header(MANIFEST_MAGIC).to_vec();
    frame_record(&manifest.to_pb().encode_to_vec(), &mut bytes);
    write_atomic(fs, Path::new("manifest"), Path::new("manifest.tmp"), &bytes)
}

/// Recovery's orphan sweep over one directory: delete everything `keep`
/// rejects, syncing the directory if anything died.
///
/// Idempotent by construction.
fn sweep_dir<F: Fs>(fs: &F, dir: &Path, keep: impl Fn(&str) -> bool) -> io::Result<()> {
    let mut deleted = false;
    for name in fs.list_dir(dir)? {
        // Only files are swept; the fixed subdirectories are structural.
        if dir == Path::new("") && (name == "log" || name == "snap") {
            continue;
        }
        if !keep(&name) {
            fs.remove_file(&dir.join(&name))?;
            deleted = true;
        }
    }
    if deleted {
        fs.sync_dir(dir)?;
    }
    Ok(())
}

// Container-version note (ADR 0015): `container::CONTAINER_VERSION` is what
// this module writes; the readable floor is also 1. Both move only by ADR.
#[allow(unused)]
const READABLE_CONTAINER_FLOOR: u32 = container::CONTAINER_VERSION;
