//! The toy reference storage engine: ADR 0017's protocol in miniature.
//!
//! This engine exists for two reasons. First, the crash harness must itself be
//! proven — a harness that passes everything is worse than no harness — and
//! that takes a subject implementing the *correct* orderings so that the sweep
//! is green, and breaks loudly when an ordering is deliberately broken. Second,
//! it is executable documentation: the manifest-first orderings of ADR 0017's
//! truncation and purge, the footer-last snapshot of ADR 0018, and the
//! five-step recovery procedure appear here literally, stripped of protobuf
//! and openraft so the shape is legible. The real segment engine replaces this
//! as the harness subject when it lands; it must pass the identical sweep.
//!
//! Simplifications relative to the real engine (deliberate, none affecting
//! crash orderings): fixed-width little-endian encodings instead of protobuf,
//! whole-segment reads at recovery instead of streaming, entries mirrored in
//! memory, no group-commit *batching across callers* (each `AppendBatch` is
//! one group), and a fresh segment after every recovery instead of reopening
//! the tail segment.
//!
//! Every durable file carries the ADR 0015 header — 8-byte magic, `u32`
//! container version, `u32` header CRC — and validation fail-stops on any
//! mismatch. Corruption policy is ADR 0017's: a torn tail self-heals by
//! physical truncation; anything else refuses to open.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use coppice_consensus::fs::{read_to_vec, write_atomic, Fs, FsFile};

use crate::harness::{CrashSubject, Observed, StorageOp};
use crate::simfs::SimFs;

const SEG_MAGIC: [u8; 8] = *b"CPTYSEG\0";
const VOTE_MAGIC: [u8; 8] = *b"CPTYVOT\0";
const MANIFEST_MAGIC: [u8; 8] = *b"CPTYMAN\0";
const SNAP_MAGIC: [u8; 8] = *b"CPTYSNP\0";
/// Closing magic of the snapshot footer. Written last: its presence is what
/// makes a truncated snapshot detectable by construction (ADR 0018).
const SNAP_FOOT_MAGIC: [u8; 8] = *b"CPTYSNF\0";

const CONTAINER_VERSION: u32 = 1;
/// magic (8) + version (4) + header CRC (4).
const HEADER_LEN: u64 = 16;
/// upto_index (8) + payload CRC (4) + closing magic (8).
const SNAP_FOOTER_LEN: u64 = 20;

/// The toy engine's configuration: identity stamps (ADR 0016) plus the
/// rotation threshold. Doubles as the harness [`CrashSubject`].
#[derive(Debug, Clone)]
pub struct ToyConfig {
    pub cluster_uuid: u128,
    pub node_id: u64,
    pub instance_uuid: u128,
    /// Appending past this many payload bytes in the active segment triggers
    /// a rotation before the next batch. Tests shrink it to force rotations
    /// mid-workload.
    pub rotation_threshold: u64,
}

impl Default for ToyConfig {
    fn default() -> ToyConfig {
        ToyConfig {
            cluster_uuid: 0x_1111_2222_3333_4444_5555_6666_7777_8888,
            node_id: 1,
            instance_uuid: 0x_9999_AAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000,
            rotation_threshold: 64 * 1024,
        }
    }
}

/// The durable facts the manifest holds (ADR 0017): identity, the claimed
/// segment list, the purge floor, the optional logical end of log, and the
/// snapshot pointer. `committed` is carried but unused, as in the ADR
/// (best-effort replay shortcut, never correctness).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Manifest {
    cluster_uuid: u128,
    node_id: u64,
    instance_uuid: u128,
    purge_floor: u64,
    /// `Some(n)`: the log logically ends at n even if the last claimed
    /// segment physically holds more (suffix truncation). Cleared when a new
    /// segment supersedes it as the chain bound.
    logical_end: Option<u64>,
    /// `(file number, covered-through index)` of the current snapshot.
    snapshot: Option<(u64, u64)>,
    committed: u64,
    /// Start indices of the claimed segments, ascending.
    segments: Vec<u64>,
}

/// An open toy store: the in-memory mirror plus the active-segment handle.
/// Holding `_lock` for the store's lifetime is the ADR 0017 second-opener
/// refusal.
pub struct ToyStore<F: Fs> {
    fs: F,
    config: ToyConfig,
    manifest: Manifest,
    entries: BTreeMap<u64, Vec<u8>>,
    vote: Option<(u64, u64)>,
    snapshot_payload: Option<Vec<u8>>,
    /// `(start index, handle, payload bytes appended)` of the active segment.
    active: Option<(u64, F::File, u64)>,
    /// The index the next appended entry takes.
    next_index: u64,
    _lock: F::Lock,
}

fn fail_stop(what: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("toy storage fail stop: {what}"))
}

// ---- Little-endian encode/decode helpers ----

fn put_u32(v: &mut Vec<u8>, x: u32) {
    v.extend_from_slice(&x.to_le_bytes());
}
fn put_u64(v: &mut Vec<u8>, x: u64) {
    v.extend_from_slice(&x.to_le_bytes());
}
fn put_u128(v: &mut Vec<u8>, x: u128) {
    v.extend_from_slice(&x.to_le_bytes());
}

/// Cursor over a decoded file body; every read fail-stops on truncation
/// because the callers validate a CRC first — a short body after a good CRC
/// is a decoder bug, not disk damage.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf, pos: 0 }
    }
    fn bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.buf.len());
        match end {
            Some(end) => {
                let s = &self.buf[self.pos..end];
                self.pos = end;
                Ok(s)
            }
            None => Err(fail_stop("short read inside a CRC-validated body")),
        }
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> io::Result<u128> {
        Ok(u128::from_le_bytes(self.bytes(16)?.try_into().unwrap()))
    }
}

// ---- The ADR 0015 container header ----

fn header_bytes(magic: &[u8; 8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(HEADER_LEN as usize);
    v.extend_from_slice(magic);
    put_u32(&mut v, CONTAINER_VERSION);
    let crc = crc32c::crc32c(&v);
    put_u32(&mut v, crc);
    v
}

/// Validate a file's leading header, returning the body that follows.
fn check_header<'a>(bytes: &'a [u8], magic: &[u8; 8], what: &str) -> io::Result<&'a [u8]> {
    if bytes.len() < HEADER_LEN as usize {
        return Err(fail_stop(format!("{what}: shorter than a container header")));
    }
    let (head, body) = bytes.split_at(HEADER_LEN as usize);
    if &head[0..8] != magic {
        return Err(fail_stop(format!("{what}: wrong magic")));
    }
    let version = u32::from_le_bytes(head[8..12].try_into().unwrap());
    if version != CONTAINER_VERSION {
        // ADR 0015: unknown or above-range container versions refuse to
        // start; there is no best-effort parse.
        return Err(fail_stop(format!("{what}: unsupported container version {version}")));
    }
    let crc = u32::from_le_bytes(head[12..16].try_into().unwrap());
    if crc != crc32c::crc32c(&head[0..12]) {
        return Err(fail_stop(format!("{what}: header CRC mismatch")));
    }
    Ok(body)
}

// ---- Paths ----

fn seg_path(start: u64) -> PathBuf {
    Path::new("log").join(format!("{start}.seg"))
}
fn snap_path(no: u64) -> PathBuf {
    Path::new("snap").join(format!("{no}.snap"))
}
fn snap_tmp_path(no: u64) -> PathBuf {
    Path::new("snap").join(format!("{no}.snap.tmp"))
}

// ---- Manifest encode/decode ----

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(96 + 8 * self.segments.len());
        put_u128(&mut body, self.cluster_uuid);
        put_u64(&mut body, self.node_id);
        put_u128(&mut body, self.instance_uuid);
        put_u64(&mut body, self.purge_floor);
        body.push(self.logical_end.is_some() as u8);
        put_u64(&mut body, self.logical_end.unwrap_or(0));
        body.push(self.snapshot.is_some() as u8);
        let (sno, supto) = self.snapshot.unwrap_or((0, 0));
        put_u64(&mut body, sno);
        put_u64(&mut body, supto);
        put_u64(&mut body, self.committed);
        put_u32(&mut body, self.segments.len() as u32);
        for s in &self.segments {
            put_u64(&mut body, *s);
        }

        let mut file = header_bytes(&MANIFEST_MAGIC);
        let crc = crc32c::crc32c(&body);
        file.extend_from_slice(&body);
        put_u32(&mut file, crc);
        file
    }

    fn decode(bytes: &[u8]) -> io::Result<Manifest> {
        let body_and_crc = check_header(bytes, &MANIFEST_MAGIC, "manifest")?;
        if body_and_crc.len() < 4 {
            return Err(fail_stop("manifest: missing body CRC"));
        }
        let (body, crc_bytes) = body_and_crc.split_at(body_and_crc.len() - 4);
        let crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc != crc32c::crc32c(body) {
            return Err(fail_stop("manifest: body CRC mismatch"));
        }
        let mut r = Reader::new(body);
        let cluster_uuid = r.u128()?;
        let node_id = r.u64()?;
        let instance_uuid = r.u128()?;
        let purge_floor = r.u64()?;
        let has_logical = r.bytes(1)?[0] != 0;
        let logical = r.u64()?;
        let has_snap = r.bytes(1)?[0] != 0;
        let sno = r.u64()?;
        let supto = r.u64()?;
        let committed = r.u64()?;
        let n = r.u32()? as usize;
        let mut segments = Vec::with_capacity(n);
        for _ in 0..n {
            segments.push(r.u64()?);
        }
        Ok(Manifest {
            cluster_uuid,
            node_id,
            instance_uuid,
            purge_floor,
            logical_end: has_logical.then_some(logical),
            snapshot: has_snap.then_some((sno, supto)),
            committed,
            segments,
        })
    }
}

/// The one way any structural fact changes: atomic-swap the manifest
/// (ADR 0017). Durable on return.
fn swap_manifest<F: Fs>(fs: &F, m: &Manifest) -> io::Result<()> {
    write_atomic(fs, Path::new("manifest"), Path::new("manifest.tmp"), &m.encode())
}

// ---- Vote file ----

fn encode_vote(term: u64, voted_for: u64) -> Vec<u8> {
    let mut body = Vec::with_capacity(16);
    put_u64(&mut body, term);
    put_u64(&mut body, voted_for);
    let mut file = header_bytes(&VOTE_MAGIC);
    let crc = crc32c::crc32c(&body);
    file.extend_from_slice(&body);
    put_u32(&mut file, crc);
    file
}

fn decode_vote(bytes: &[u8]) -> io::Result<(u64, u64)> {
    let body_and_crc = check_header(bytes, &VOTE_MAGIC, "vote")?;
    if body_and_crc.len() != 20 {
        return Err(fail_stop("vote: wrong body length"));
    }
    let (body, crc_bytes) = body_and_crc.split_at(16);
    if u32::from_le_bytes(crc_bytes.try_into().unwrap()) != crc32c::crc32c(body) {
        // The vote file is written by atomic swap, so a crash leaves the old
        // file or the new one, never a torn one; a bad CRC here is real
        // corruption of possibly-committed voting state. Fail stop.
        return Err(fail_stop("vote: body CRC mismatch"));
    }
    let mut r = Reader::new(body);
    Ok((r.u64()?, r.u64()?))
}

// ---- Segment frames ----

/// Frame layout: u32 payload length, u64 entry index, u32 CRC32C over
/// (index LE bytes ++ payload), payload.
fn frame(index: u64, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(16 + payload.len());
    put_u32(&mut v, payload.len() as u32);
    put_u64(&mut v, index);
    let mut crc = crc32c::crc32c(&index.to_le_bytes());
    crc = crc32c::crc32c_append(crc, payload);
    put_u32(&mut v, crc);
    v.extend_from_slice(payload);
    v
}

/// One parsed frame, or where and why parsing stopped.
enum FrameScan {
    Frame { index: u64, payload: Vec<u8>, next_offset: u64 },
    /// The bytes from `offset` do not form a whole, CRC-valid frame. Whether
    /// this is a self-healing torn tail or fail-stop corruption is the
    /// caller's decision — it depends on position, which the caller knows.
    Bad { offset: u64 },
    End,
}

fn scan_frame(bytes: &[u8], offset: u64) -> FrameScan {
    let buf = &bytes[offset as usize..];
    if buf.is_empty() {
        return FrameScan::End;
    }
    if buf.len() < 16 {
        return FrameScan::Bad { offset };
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let index = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let crc = u32::from_le_bytes(buf[12..16].try_into().unwrap());
    if buf.len() < 16 + len {
        return FrameScan::Bad { offset };
    }
    let payload = &buf[16..16 + len];
    let mut want = crc32c::crc32c(&index.to_le_bytes());
    want = crc32c::crc32c_append(want, payload);
    if want != crc {
        return FrameScan::Bad { offset };
    }
    FrameScan::Frame { index, payload: payload.to_vec(), next_offset: offset + 16 + len as u64 }
}

// ---- Snapshot container ----

fn encode_snapshot(upto_index: u64, payload: &[u8]) -> Vec<u8> {
    let mut v = header_bytes(&SNAP_MAGIC);
    v.extend_from_slice(payload);
    // Footer last: a crash mid-write leaves a file without a closing magic,
    // never a file that validates.
    put_u64(&mut v, upto_index);
    put_u32(&mut v, crc32c::crc32c(payload));
    v.extend_from_slice(&SNAP_FOOT_MAGIC);
    v
}

fn decode_snapshot(bytes: &[u8], want_upto: u64) -> io::Result<Vec<u8>> {
    let rest = check_header(bytes, &SNAP_MAGIC, "snapshot")?;
    if (rest.len() as u64) < SNAP_FOOTER_LEN {
        return Err(fail_stop("snapshot: no footer"));
    }
    let (payload, footer) = rest.split_at(rest.len() - SNAP_FOOTER_LEN as usize);
    let mut r = Reader::new(footer);
    let upto = r.u64()?;
    let crc = r.u32()?;
    if footer[12..20] != SNAP_FOOT_MAGIC {
        return Err(fail_stop("snapshot: missing closing magic (truncated write adopted?)"));
    }
    if crc != crc32c::crc32c(payload) {
        return Err(fail_stop("snapshot: payload CRC mismatch"));
    }
    if upto != want_upto {
        return Err(fail_stop("snapshot: footer index disagrees with the manifest pointer"));
    }
    Ok(payload.to_vec())
}

// ---- The engine ----

impl ToyConfig {
    /// Initialize a fresh data directory: directory skeleton plus the stamped
    /// manifest. This is the explicit-intent path of ADR 0016 (`--bootstrap`
    /// / `--join`); [`ToyConfig::open`] refuses an uninitialized directory.
    pub fn init<F: Fs>(&self, fs: &F) -> io::Result<()> {
        fs.create_dir_all(Path::new("log"))?;
        fs.create_dir_all(Path::new("snap"))?;
        fs.sync_dir(Path::new(""))?;
        swap_manifest(
            fs,
            &Manifest {
                cluster_uuid: self.cluster_uuid,
                node_id: self.node_id,
                instance_uuid: self.instance_uuid,
                purge_floor: 0,
                logical_end: None,
                snapshot: None,
                committed: 0,
                segments: Vec::new(),
            },
        )
    }

    /// Open through the full recovery procedure — the five steps of ADR 0017
    /// §"Recovery procedure", in order.
    pub fn open<F: Fs + Clone>(&self, fs: &F) -> io::Result<ToyStore<F>> {
        // Step 1: take the LOCK, then read and validate the manifest.
        let lock = fs.lock(Path::new("LOCK"))?;
        if !fs.exists(Path::new("manifest"))? {
            // ADR 0016: an unexpectedly empty directory is a failed mount,
            // never permission to start clean.
            return Err(fail_stop(
                "no manifest: refusing to start on an uninitialized directory (run init)",
            ));
        }
        let manifest = Manifest::decode(&read_to_vec(fs, Path::new("manifest"))?)?;
        if manifest.cluster_uuid != self.cluster_uuid
            || manifest.node_id != self.node_id
            || manifest.instance_uuid != self.instance_uuid
        {
            return Err(fail_stop("identity stamp mismatch: wrong volume for this node"));
        }

        // Step 2: delete orphans the manifest does not claim. An un-synced
        // delete can resurrect a file, so this must tolerate re-finding work
        // recovery already did (it does: it is a pure sweep).
        let claimed_segs: std::collections::BTreeSet<String> = manifest
            .segments
            .iter()
            .map(|s| format!("{s}.seg"))
            .collect();
        for name in fs.list_dir(Path::new("log"))? {
            if !claimed_segs.contains(&name) {
                fs.remove_file(&Path::new("log").join(&name))?;
            }
        }
        fs.sync_dir(Path::new("log"))?;
        let claimed_snap = manifest.snapshot.map(|(no, _)| format!("{no}.snap"));
        for name in fs.list_dir(Path::new("snap"))? {
            if Some(&name) != claimed_snap.as_ref() {
                fs.remove_file(&Path::new("snap").join(&name))?;
            }
        }
        fs.sync_dir(Path::new("snap"))?;
        for name in fs.list_dir(Path::new(""))? {
            if name.ends_with(".tmp") {
                fs.remove_file(Path::new(&name))?;
            }
        }
        fs.sync_dir(Path::new(""))?;

        // Steps 3 + 4: open claimed segments, verify headers and the start
        // chain, read sealed segments fully, scan the tail segment.
        let mut entries = BTreeMap::new();
        let starts = &manifest.segments;
        if starts.windows(2).any(|w| w[0] >= w[1]) {
            return Err(fail_stop("manifest: segment starts not strictly ascending"));
        }
        for (i, &start) in starts.iter().enumerate() {
            let bound = starts.get(i + 1).copied();
            let is_last = bound.is_none();
            read_segment(
                fs,
                start,
                bound,
                if is_last { manifest.logical_end } else { None },
                &mut entries,
            )?;
        }
        // Entries at or below the purge floor may physically survive in a
        // partially covered segment; they are logically gone.
        entries.retain(|&i, _| i > manifest.purge_floor);

        // Contiguity of what recovery reports is an invariant the harness
        // checks; verifying it here too makes the toy fail stop at the source.
        {
            let idxs: Vec<u64> = entries.keys().copied().collect();
            if idxs.windows(2).any(|w| w[1] != w[0] + 1) {
                return Err(fail_stop("recovered log is not contiguous"));
            }
        }

        // Snapshot pointer: the manifest only ever points at a snapshot whose
        // rename was durable, so a missing or invalid file here is an
        // ordering bug upstream — fail stop, never adopt-and-hope.
        let snapshot_payload = match manifest.snapshot {
            None => None,
            Some((no, upto)) => {
                Some(decode_snapshot(&read_to_vec(fs, &snap_path(no))?, upto)?)
            }
        };

        // Step 5 ends with reporting vote, floor, log range, snapshot.
        let vote = if fs.exists(Path::new("vote"))? {
            Some(decode_vote(&read_to_vec(fs, Path::new("vote"))?)?)
        } else {
            None
        };

        let last_entry = entries.keys().next_back().copied().unwrap_or(0);
        let next_index = last_entry
            .max(manifest.logical_end.unwrap_or(0))
            .max(manifest.purge_floor)
            + 1;

        Ok(ToyStore {
            fs: fs.clone(),
            config: self.clone(),
            manifest,
            entries,
            vote,
            snapshot_payload,
            // Recovery never reopens the tail segment for appending; the
            // first post-recovery append claims a fresh segment. Simpler than
            // the real engine, with identical crash semantics.
            active: None,
            next_index,
            _lock: lock,
        })
    }
}

/// Read one claimed segment. `bound` is the next segment's start (the chain
/// bound superseding any logical end); `logical_end` applies only to the last
/// segment. Frames at or beyond the bound are stale truncated-suffix bytes
/// and are ignored without validation.
fn read_segment<F: Fs>(
    fs: &F,
    start: u64,
    bound: Option<u64>,
    logical_end: Option<u64>,
    entries: &mut BTreeMap<u64, Vec<u8>>,
) -> io::Result<()> {
    let path = seg_path(start);
    if !fs.exists(&path)? {
        // Segments are made durable (file + dir fsync) before the manifest
        // claims them, so a claimed-but-missing segment means acknowledged
        // entries are gone.
        return Err(fail_stop(format!("claimed segment {} is missing", path.display())));
    }
    let bytes = read_to_vec(fs, &path)?;
    check_header(&bytes, &SEG_MAGIC, "segment")?;

    let mut expected = start;
    let mut offset = HEADER_LEN;
    loop {
        // The live range ends where the next segment takes over or where the
        // logical end (suffix truncation) says it does.
        let live_end = match (bound, logical_end) {
            (Some(b), _) => Some(b - 1),
            (None, Some(le)) => Some(le),
            (None, None) => None,
        };
        if let Some(le) = live_end {
            if expected > le {
                return Ok(());
            }
        }
        match scan_frame(&bytes, offset) {
            FrameScan::Frame { index, payload, next_offset } => {
                if index != expected {
                    return Err(fail_stop(format!(
                        "segment {start}: frame index {index} where {expected} was expected"
                    )));
                }
                entries.insert(index, payload);
                expected += 1;
                offset = next_offset;
            }
            FrameScan::End | FrameScan::Bad { .. }
                if live_end.is_some_and(|le| expected <= le) =>
            {
                // The chain or logical end promises entries we cannot read:
                // acknowledged state is damaged. Never truncate here.
                return Err(fail_stop(format!(
                    "segment {start}: unreadable at entry {expected} inside the committed range"
                )));
            }
            FrameScan::End => return Ok(()),
            FrameScan::Bad { offset } => {
                // The torn tail of ADR 0002: an un-acknowledged trailing
                // frame. Self-heal by physical truncation — the one place
                // sealed-bytes-never-rewritten does not apply, because these
                // bytes were never acknowledged.
                let mut f = fs.open_append(&path)?;
                f.truncate(offset)?;
                f.sync_data()?;
                return Ok(());
            }
        }
    }
}

impl<F: Fs + Clone> ToyStore<F> {
    /// Apply one abstract storage operation; `Ok(())` means acknowledged
    /// durable per the op's ADR ordering.
    pub fn apply(&mut self, op: &StorageOp) -> io::Result<()> {
        match op {
            StorageOp::AppendBatch { payloads } => self.append_batch(payloads),
            StorageOp::SetVote { term, voted_for } => self.set_vote(*term, *voted_for),
            StorageOp::TruncateSuffix { from } => self.truncate_suffix(*from),
            StorageOp::Purge { upto } => self.purge(*upto),
            StorageOp::InstallSnapshot { upto_index, payload } => {
                self.install_snapshot(*upto_index, payload)
            }
            StorageOp::Rotate => self.rotate(),
        }
    }

    /// What this store would report to openraft; the harness compares it
    /// against the acknowledged model. Purely in-memory.
    pub fn observe(&self) -> Observed {
        Observed {
            entries: self.entries.clone(),
            vote: self.vote,
            snapshot: self
                .manifest
                .snapshot
                .map(|(_, upto)| (upto, self.snapshot_payload.clone().unwrap_or_default())),
            purge_floor: self.manifest.purge_floor,
        }
    }

    /// Ensure an active segment exists to append into, creating and *claiming*
    /// one if not. Ordering: the segment file is durable (file fsync + dir
    /// fsync) before the manifest claims it, and the manifest claims it
    /// before any entry in it is acknowledged — so recovery either sees an
    /// orphan (deleted) or a claimed, empty-or-longer segment, never an
    /// acknowledged entry in an unclaimed file.
    fn ensure_active(&mut self) -> io::Result<()> {
        if self.active.is_some() {
            return Ok(());
        }
        let start = self.next_index;
        let path = seg_path(start);
        let mut file = fs_create_replacing(&self.fs, &path)?;
        file.append(&header_bytes(&SEG_MAGIC))?;
        file.sync_data()?;
        self.fs.sync_dir(Path::new("log"))?;

        let mut m = self.manifest.clone();
        m.segments.push(start);
        // A new segment is the chain bound for its predecessor; the logical
        // end override has done its job (ADR 0017: rotation clears it).
        m.logical_end = None;
        swap_manifest(&self.fs, &m)?;
        self.manifest = m;
        self.active = Some((start, file, 0));
        Ok(())
    }

    fn append_batch(&mut self, payloads: &[Vec<u8>]) -> io::Result<()> {
        // Rotate on the batch boundary once the active segment is oversize.
        if self
            .active
            .as_ref()
            .is_some_and(|(_, _, bytes)| *bytes >= self.config.rotation_threshold)
        {
            self.rotate()?;
        }
        self.ensure_active()?;
        let (_, file, appended) = self.active.as_mut().expect("ensure_active");
        let mut batch_bytes = 0u64;
        let mut index = self.next_index;
        for p in payloads {
            let f = frame(index, p);
            batch_bytes += f.len() as u64;
            file.append(&f)?;
            index += 1;
        }
        // Group commit: one fsync acknowledges the whole batch (ADR 0002).
        file.sync_data()?;
        *appended += batch_bytes;
        for p in payloads {
            self.entries.insert(self.next_index, p.clone());
            self.next_index += 1;
        }
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        // Rotating an absent or empty active segment would mint a second
        // segment with the same start index; it is a no-op instead.
        match &self.active {
            Some((start, _, _)) if self.next_index > *start => {
                self.active = None;
                self.ensure_active()
            }
            _ => Ok(()),
        }
    }

    fn set_vote(&mut self, term: u64, voted_for: u64) -> io::Result<()> {
        // Atomic swap, exactly like the manifest: Raft correctness depends on
        // the vote being durable before this returns (ADR 0002).
        write_atomic(
            &self.fs,
            Path::new("vote"),
            Path::new("vote.tmp"),
            &encode_vote(term, voted_for),
        )?;
        self.vote = Some((term, voted_for));
        Ok(())
    }

    fn truncate_suffix(&mut self, from: u64) -> io::Result<()> {
        if from >= self.next_index {
            return Ok(());
        }
        // Step 1 (ADR 0017): the manifest records the logical end and drops
        // wholly-truncated segments, and is durable BEFORE the truncation is
        // acknowledged.
        let mut m = self.manifest.clone();
        m.logical_end = Some(from - 1);
        let dropped: Vec<u64> = m.segments.iter().copied().filter(|&s| s >= from).collect();
        m.segments.retain(|&s| s < from);
        swap_manifest(&self.fs, &m)?;
        self.manifest = m;

        // The active segment is either dropped or now sealed-with-stale-tail;
        // either way appends never touch it again (sealed bytes are never
        // rewritten — the next append opens a fresh segment at `from`).
        self.active = None;

        // Step 2: physical deletion, order irrelevant once the manifest is
        // durable — a crash here leaves orphans for recovery to sweep.
        for s in dropped {
            self.fs.remove_file(&seg_path(s))?;
        }
        self.fs.sync_dir(Path::new("log"))?;

        self.entries.retain(|&i, _| i < from);
        self.next_index = from.max(self.manifest.purge_floor + 1);
        Ok(())
    }

    fn purge(&mut self, upto: u64) -> io::Result<()> {
        let upto = upto.max(self.manifest.purge_floor);
        // A segment is covered once every entry it can contribute is <= upto;
        // its live range ends where the next segment starts, or at the last
        // in-memory entry for the tail segment.
        let mut m = self.manifest.clone();
        let last_live = self.entries.keys().next_back().copied().unwrap_or(m.purge_floor);
        let starts = m.segments.clone();
        let covered: Vec<u64> = starts
            .iter()
            .enumerate()
            .filter(|&(i, &s)| {
                let end = starts.get(i + 1).map(|n| n - 1).unwrap_or(last_live);
                end <= upto && end >= s
            })
            .map(|(_, &s)| s)
            .collect();

        // Step 1 (ADR 0017): floor advances and covered segments leave the
        // manifest before any file dies.
        m.purge_floor = upto;
        m.segments.retain(|s| !covered.contains(s));
        swap_manifest(&self.fs, &m)?;
        self.manifest = m;

        if let Some((start, _, _)) = &self.active {
            if covered.contains(start) {
                self.active = None;
            }
        }

        // Step 2: deletion.
        for s in covered {
            self.fs.remove_file(&seg_path(s))?;
        }
        self.fs.sync_dir(Path::new("log"))?;

        self.entries.retain(|&i, _| i > upto);
        self.next_index = self.next_index.max(upto + 1);
        Ok(())
    }

    fn install_snapshot(&mut self, upto_index: u64, payload: &[u8]) -> io::Result<()> {
        let no = self.manifest.snapshot.map(|(n, _)| n + 1).unwrap_or(1);
        // Temp path + rename + dir fsync (ADR 0018), footer already last in
        // the encoding: at no crash point can a partial snapshot carry a
        // valid closing magic AND be pointed at by a durable manifest.
        let tmp = snap_tmp_path(no);
        let mut f = fs_create_replacing(&self.fs, &tmp)?;
        f.append(&encode_snapshot(upto_index, payload))?;
        f.sync_data()?;
        drop(f);
        self.fs.rename(&tmp, &snap_path(no))?;
        self.fs.sync_dir(Path::new("snap"))?;

        // The manifest pointer flip is the commit point; the previous
        // snapshot is retained until then (ADR 0002) and deleted after.
        let prev = self.manifest.snapshot;
        let mut m = self.manifest.clone();
        m.snapshot = Some((no, upto_index));
        swap_manifest(&self.fs, &m)?;
        self.manifest = m;
        self.snapshot_payload = Some(payload.to_vec());

        if let Some((prev_no, _)) = prev {
            self.fs.remove_file(&snap_path(prev_no))?;
            self.fs.sync_dir(Path::new("snap"))?;
        }
        Ok(())
    }
}

/// `create_new`, deleting any visible leftover first. Leftovers are normal:
/// recovery deletes orphans, but a file created, deleted, and recreated
/// within one process life never went through recovery.
fn fs_create_replacing<F: Fs>(fs: &F, path: &Path) -> io::Result<F::File> {
    if fs.exists(path)? {
        fs.remove_file(path)?;
    }
    fs.create_new(path)
}

impl CrashSubject for ToyConfig {
    type Store = ToyStore<SimFs>;

    fn init(&self, fs: &SimFs) -> io::Result<()> {
        ToyConfig::init(self, fs)
    }

    fn open(&self, fs: &SimFs) -> io::Result<Self::Store> {
        ToyConfig::open(self, fs)
    }

    fn apply(&self, store: &mut Self::Store, op: &StorageOp) -> io::Result<()> {
        store.apply(op)
    }

    fn observe(&self, store: &Self::Store) -> Observed {
        store.observe()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use coppice_consensus::fs::Fs;

    use super::ToyConfig;
    use crate::harness::StorageOp;
    use crate::simfs::{SimConfig, SimFs};

    fn store_with(fs: &SimFs, ops: &[StorageOp]) -> super::ToyStore<SimFs> {
        let cfg = ToyConfig::default();
        cfg.init(fs).unwrap();
        let mut store = cfg.open(fs).unwrap();
        for op in ops {
            store.apply(op).unwrap();
        }
        store
    }

    fn payloads(ns: &[usize]) -> StorageOp {
        StorageOp::AppendBatch {
            payloads: ns.iter().map(|&n| vec![0xAB; n]).collect(),
        }
    }

    #[test]
    fn roundtrip_through_reopen() {
        let fs = SimFs::new(SimConfig::default());
        let ops = [
            payloads(&[10, 5000, 100]),
            StorageOp::SetVote { term: 3, voted_for: 2 },
            StorageOp::Rotate,
            payloads(&[64]),
            StorageOp::InstallSnapshot { upto_index: 2, payload: vec![7; 40] },
            StorageOp::Purge { upto: 2 },
        ];
        let store = store_with(&fs, &ops);
        let before = store.observe();
        assert_eq!(before.entries.keys().copied().collect::<Vec<_>>(), vec![3, 4]);
        assert_eq!(before.vote, Some((3, 2)));
        assert_eq!(before.purge_floor, 2);
        assert_eq!(before.snapshot, Some((2, vec![7; 40])));
        drop(store);

        let after = ToyConfig::default().open(&fs).unwrap().observe();
        assert_eq!(before, after);
    }

    #[test]
    fn truncate_never_rewrites_sealed_bytes_and_reopens_cleanly() {
        let fs = SimFs::new(SimConfig::default());
        let store = store_with(
            &fs,
            &[
                payloads(&[100, 100, 100, 100]),
                StorageOp::TruncateSuffix { from: 3 },
                payloads(&[9, 9]),
            ],
        );
        let obs = store.observe();
        assert_eq!(obs.entries.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
        // The truncated segment keeps its stale physical bytes; the new
        // entries live in a fresh segment starting at 3.
        assert!(fs.exists(Path::new("log/1.seg")).unwrap());
        assert!(fs.exists(Path::new("log/3.seg")).unwrap());
        drop(store);
        assert_eq!(ToyConfig::default().open(&fs).unwrap().observe(), obs);
    }

    #[test]
    fn uninitialized_directory_is_refused() {
        let fs = SimFs::new(SimConfig::default());
        let err = ToyConfig::default().open(&fs).map(|_| ()).unwrap_err();
        assert!(err.to_string().contains("uninitialized"), "{err}");
    }

    #[test]
    fn wrong_identity_is_refused() {
        let fs = SimFs::new(SimConfig::default());
        ToyConfig::default().init(&fs).unwrap();
        let other = ToyConfig { node_id: 99, ..ToyConfig::default() };
        let err = other.open(&fs).map(|_| ()).unwrap_err();
        assert!(err.to_string().contains("identity"), "{err}");
    }
}
