//! The agent's local durable journal (ADR 0009).
//!
//! A single append-only file, `journal`, in the agent's data directory,
//! guarded by a `LOCK` (second-opener refusal, ADR 0017). It is written
//! through the [`coppice_consensus::fs`] seam with the same crash-safety care
//! as the coordinator's storage — never through `std::fs` — so the crash
//! suite can hammer it.
//!
//! # Framing
//!
//! Each record is `u32 LE length ++ u32 LE crc32c(payload) ++ payload`, where
//! `payload` is a prost-encoded [`pb::JournalRecord`]. Recovery scans from
//! offset 0; a torn, short, or CRC-failing tail ends the scan (the last
//! partial write of a crashed process), and everything after the last good
//! frame is discarded by the compaction rewrite below.
//!
//! # Recovery and compaction
//!
//! [`Journal::open`] scans the existing file into a [`JournalState`], then
//! rewrites the live set atomically ([`write_atomic`], ADR 0017) and reopens
//! for append. "Compaction" in v1 means only dropping duplicate `FencingUpdate`
//! records down to the single current watermark; attempt records (intents,
//! tombstones, exits) are all kept — a retention policy is future work. The
//! rewrite is atomic, so a crash inside recovery leaves the previous journal
//! intact (same live set), and recovery is idempotent: opening twice yields an
//! identical [`JournalState`].
//!
//! # Durability
//!
//! Every append fsyncs (`sync_data`) before returning — this is the
//! fsync-before-container-start barrier of ADR 0009: `StartIntent` is durable
//! before [`crate::executor::Executor::start`] is ever called.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

use coppice_consensus::fs::{write_atomic, Fs, FsFile};
use coppice_core::attempt::AttemptOutcome;
use coppice_core::id::{AllocationId, AttemptId, JobId};
use coppice_proto::pb::agent::v1 as pb;
use prost::Message;

const JOURNAL: &str = "journal";
const JOURNAL_TMP: &str = "journal.tmp";
const LOCK: &str = "LOCK";

/// The highest fencing token the agent has accepted (ADR 0009). Componentwise:
/// a command is rejected if either coordinate is below the watermark.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Watermark {
    pub leader_term: u64,
    pub node_epoch: u64,
}

/// A journaled start intent (ADR 0009).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntentRec {
    pub allocation: AllocationId,
    pub attempt: AttemptId,
    pub job: JobId,
    pub node_epoch: u64,
}

/// A journaled, classified exit (ADR 0013).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitRec {
    pub allocation: AllocationId,
    pub attempt: AttemptId,
    pub job: JobId,
    pub outcome: AttemptOutcome,
    pub runtime_us: u64,
}

/// The pure recovered state of the journal: the fencing watermark plus the
/// per-allocation intents, tombstones, and exits. Reconstructed identically by
/// replaying the same records in any run (recovery idempotence).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JournalState {
    pub watermark: Watermark,
    pub intents: BTreeMap<AllocationId, IntentRec>,
    pub tombstones: BTreeSet<AllocationId>,
    pub exits: BTreeMap<AllocationId, ExitRec>,
}

impl JournalState {
    /// Fold one decoded record into the state. Fencing raises the watermark
    /// componentwise (records are only ever written strictly-raising, but
    /// componentwise max is robust to any interleaving). A CRC-valid frame
    /// that fails domain conversion is genuine corruption, surfaced as an
    /// error — the crash adversary never produces one (CRCs catch tears).
    fn apply_record(&mut self, rec: &pb::JournalRecord) -> io::Result<()> {
        use pb::journal_record::Body;
        let Some(body) = &rec.body else {
            // An empty record carries no information; ignore it.
            return Ok(());
        };
        match body {
            Body::Fencing(f) => {
                self.watermark.leader_term = self.watermark.leader_term.max(f.leader_term);
                self.watermark.node_epoch = self.watermark.node_epoch.max(f.node_epoch);
            }
            Body::StartIntent(si) => {
                let allocation = try_id(si.allocation.clone(), "StartIntent.allocation")?;
                let attempt = try_id(si.attempt.clone(), "StartIntent.attempt")?;
                let job = try_id(si.job.clone(), "StartIntent.job")?;
                self.intents.insert(
                    allocation,
                    IntentRec {
                        allocation,
                        attempt,
                        job,
                        node_epoch: si.node_epoch,
                    },
                );
            }
            Body::Tombstone(t) => {
                let allocation = try_id(t.allocation.clone(), "AllocationTombstone.allocation")?;
                self.tombstones.insert(allocation);
            }
            Body::ObservedExit(e) => {
                let allocation = try_id(e.allocation.clone(), "ObservedExit.allocation")?;
                let attempt = try_id(e.attempt.clone(), "ObservedExit.attempt")?;
                let job = try_id(e.job.clone(), "ObservedExit.job")?;
                let outcome = e
                    .outcome
                    .ok_or_else(|| corrupt("ObservedExit.outcome missing"))?
                    .try_into()
                    .map_err(|_| corrupt("ObservedExit.outcome invalid"))?;
                self.exits.insert(
                    allocation,
                    ExitRec {
                        allocation,
                        attempt,
                        job,
                        outcome,
                        runtime_us: e.runtime_us,
                    },
                );
            }
        }
        Ok(())
    }

    /// The canonical live set as records, in a deterministic order: the single
    /// current watermark, then intents, tombstones, and exits by allocation
    /// id. Scanning this back reproduces `self` exactly.
    fn canonical_records(&self) -> Vec<pb::JournalRecord> {
        use pb::journal_record::Body;
        let mut records = Vec::new();
        records.push(pb::JournalRecord {
            body: Some(Body::Fencing(pb::FencingUpdate {
                leader_term: self.watermark.leader_term,
                node_epoch: self.watermark.node_epoch,
            })),
        });
        for intent in self.intents.values() {
            records.push(record_intent(intent));
        }
        for allocation in &self.tombstones {
            records.push(record_tombstone(*allocation));
        }
        for exit in self.exits.values() {
            records.push(record_exit(exit));
        }
        records
    }
}

/// The append-only journal file plus its lock. Generic over the filesystem
/// seam so the crash suite can run it over `SimFs`.
pub struct Journal<F: Fs> {
    fs: F,
    file: F::File,
    // Held for the life of the journal; dropping it releases the lock.
    _lock: F::Lock,
}

impl<F: Fs> Journal<F> {
    /// Open (recovering) the journal at the data directory `fs` is anchored
    /// at. Takes the `LOCK`, scans and truncates any torn tail by rewriting
    /// the compacted live set atomically, and reopens for append. Returns the
    /// journal handle and the recovered [`JournalState`].
    pub fn open(fs: F) -> io::Result<(Journal<F>, JournalState)> {
        let lock = fs.lock(Path::new(LOCK))?;

        let mut state = JournalState::default();
        if fs.exists(Path::new(JOURNAL))? {
            let reader = fs.open_read(Path::new(JOURNAL))?;
            scan_frames(&reader, &mut state)?;
        }

        // Compact: rewrite the canonical live set atomically. This both drops
        // duplicate fencing records and discards any torn tail (it is not part
        // of `state`). Crash-safe: `write_atomic` leaves either the old file
        // or the new one, both representing the same live set.
        let bytes = encode_records(&state.canonical_records());
        write_atomic(&fs, Path::new(JOURNAL), Path::new(JOURNAL_TMP), &bytes)?;

        let file = fs.open_append(Path::new(JOURNAL))?;
        Ok((
            Journal {
                fs,
                file,
                _lock: lock,
            },
            state,
        ))
    }

    /// Append one record and fsync before returning (ADR 0009's
    /// fsync-before-start barrier). The caller updates its in-memory
    /// [`JournalState`] via the record it built.
    fn append(&mut self, rec: &pb::JournalRecord) -> io::Result<()> {
        let frame = encode_frame(rec);
        self.file.append(&frame)?;
        self.file.sync_data()
    }

    /// Journal a raised fencing watermark (fsynced) before acting on the
    /// command that raised it (ADR 0009).
    pub fn journal_fencing(&mut self, watermark: Watermark) -> io::Result<()> {
        self.append(&pb::JournalRecord {
            body: Some(pb::journal_record::Body::Fencing(pb::FencingUpdate {
                leader_term: watermark.leader_term,
                node_epoch: watermark.node_epoch,
            })),
        })
    }

    /// Journal a start intent (fsynced) *before* starting the container.
    pub fn journal_intent(&mut self, intent: &IntentRec) -> io::Result<()> {
        self.append(&record_intent(intent))
    }

    /// Journal a stop tombstone (fsynced) *before* acting on the stop.
    pub fn journal_tombstone(&mut self, allocation: AllocationId) -> io::Result<()> {
        self.append(&record_tombstone(allocation))
    }

    /// Journal a classified exit (fsynced) *before* reporting it.
    pub fn journal_exit(&mut self, exit: &ExitRec) -> io::Result<()> {
        self.append(&record_exit(exit))
    }

    /// The filesystem the journal is written through (test/inspection use).
    pub fn fs(&self) -> &F {
        &self.fs
    }
}

// ---- record constructors ------------------------------------------------

fn record_intent(intent: &IntentRec) -> pb::JournalRecord {
    pb::JournalRecord {
        body: Some(pb::journal_record::Body::StartIntent(pb::StartIntent {
            allocation: Some(intent.allocation.into()),
            attempt: Some(intent.attempt.into()),
            job: Some(intent.job.into()),
            node_epoch: intent.node_epoch,
        })),
    }
}

fn record_tombstone(allocation: AllocationId) -> pb::JournalRecord {
    pb::JournalRecord {
        body: Some(pb::journal_record::Body::Tombstone(
            pb::AllocationTombstone {
                allocation: Some(allocation.into()),
            },
        )),
    }
}

fn record_exit(exit: &ExitRec) -> pb::JournalRecord {
    pb::JournalRecord {
        body: Some(pb::journal_record::Body::ObservedExit(pb::ObservedExit {
            allocation: Some(exit.allocation.into()),
            attempt: Some(exit.attempt.into()),
            job: Some(exit.job.into()),
            outcome: Some((&exit.outcome).into()),
            runtime_us: exit.runtime_us,
        })),
    }
}

// ---- framing ------------------------------------------------------------

fn encode_frame(rec: &pb::JournalRecord) -> Vec<u8> {
    let payload = rec.encode_to_vec();
    let crc = crc32c::crc32c(&payload);
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);
    frame
}

fn encode_records(records: &[pb::JournalRecord]) -> Vec<u8> {
    let mut out = Vec::new();
    for rec in records {
        out.extend_from_slice(&encode_frame(rec));
    }
    out
}

/// Scan every intact frame into `state`, stopping at the first torn / short /
/// CRC-failing frame (the crashed process's partial tail).
fn scan_frames<File: FsFile>(reader: &File, state: &mut JournalState) -> io::Result<()> {
    let len = reader.len()?;
    let mut off = 0u64;
    loop {
        if off + 8 > len {
            break; // no room for a header: end of the intact log
        }
        let mut header = [0u8; 8];
        reader.read_exact_at(off, &mut header)?;
        let plen = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as u64;
        let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        if off + 8 + plen > len {
            break; // truncated payload: torn tail
        }
        let mut payload = vec![0u8; plen as usize];
        reader.read_exact_at(off + 8, &mut payload)?;
        if crc32c::crc32c(&payload) != crc {
            break; // corrupt tail
        }
        let rec = pb::JournalRecord::decode(payload.as_slice())
            .map_err(|e| corrupt(&format!("undecodable CRC-valid frame: {e}")))?;
        state.apply_record(&rec)?;
        off += 8 + plen;
    }
    Ok(())
}

fn try_id<P, D>(value: Option<P>, field: &'static str) -> io::Result<D>
where
    D: TryFrom<P>,
{
    let value = value.ok_or_else(|| corrupt(field))?;
    D::try_from(value).map_err(|_| corrupt(field))
}

fn corrupt(msg: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("corrupt journal: {msg}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_consensus::fs::RealFs;

    fn ids() -> (AllocationId, AttemptId, JobId) {
        (AllocationId::new(), AttemptId::new(), JobId::new())
    }

    #[test]
    fn roundtrip_through_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let (alloc, attempt, job) = ids();

        {
            let fs = RealFs::new(dir.path());
            let (mut journal, mut state) = Journal::open(fs).unwrap();
            journal
                .journal_fencing(Watermark {
                    leader_term: 3,
                    node_epoch: 7,
                })
                .unwrap();
            state.watermark = Watermark {
                leader_term: 3,
                node_epoch: 7,
            };
            let intent = IntentRec {
                allocation: alloc,
                attempt,
                job,
                node_epoch: 7,
            };
            journal.journal_intent(&intent).unwrap();
            state.intents.insert(alloc, intent);
            let exit = ExitRec {
                allocation: alloc,
                attempt,
                job,
                outcome: AttemptOutcome::Exited { code: 0 },
                runtime_us: 42,
            };
            journal.journal_exit(&exit).unwrap();
            state.exits.insert(alloc, exit);
        }

        let fs = RealFs::new(dir.path());
        let (_journal, recovered) = Journal::open(fs).unwrap();
        assert_eq!(
            recovered.watermark,
            Watermark {
                leader_term: 3,
                node_epoch: 7
            }
        );
        assert_eq!(recovered.intents.get(&alloc).unwrap().node_epoch, 7);
        assert_eq!(recovered.exits.get(&alloc).unwrap().runtime_us, 42);
    }

    #[test]
    fn recovery_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let (alloc, attempt, job) = ids();
        {
            let fs = RealFs::new(dir.path());
            let (mut journal, _) = Journal::open(fs).unwrap();
            journal.journal_tombstone(alloc).unwrap();
            journal
                .journal_intent(&IntentRec {
                    allocation: alloc,
                    attempt,
                    job,
                    node_epoch: 1,
                })
                .unwrap();
        }
        let first = { Journal::open(RealFs::new(dir.path())).unwrap().1 };
        let second = { Journal::open(RealFs::new(dir.path())).unwrap().1 };
        assert_eq!(first, second);
        assert!(first.tombstones.contains(&alloc));
        assert!(first.intents.contains_key(&alloc));
    }

    #[test]
    fn torn_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let (alloc, attempt, job) = ids();
        {
            let fs = RealFs::new(dir.path());
            let (mut journal, _) = Journal::open(fs).unwrap();
            journal
                .journal_intent(&IntentRec {
                    allocation: alloc,
                    attempt,
                    job,
                    node_epoch: 1,
                })
                .unwrap();
        }
        // Append a torn frame directly: a length header promising more bytes
        // than follow.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.path().join("journal"))
                .unwrap();
            f.write_all(&999u32.to_le_bytes()).unwrap();
            f.write_all(&0u32.to_le_bytes()).unwrap();
            f.write_all(b"short").unwrap();
        }
        let recovered = { Journal::open(RealFs::new(dir.path())).unwrap().1 };
        // The good intent survived; the torn tail was dropped.
        assert!(recovered.intents.contains_key(&alloc));
        // After recovery the file is the compacted (clean) form: reopening is
        // stable.
        let again = { Journal::open(RealFs::new(dir.path())).unwrap().1 };
        assert_eq!(recovered, again);
    }

    #[test]
    fn second_opener_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (held, _) = Journal::open(RealFs::new(dir.path())).unwrap();
        assert!(Journal::open(RealFs::new(dir.path())).is_err());
        drop(held);
        Journal::open(RealFs::new(dir.path())).unwrap();
    }
}
