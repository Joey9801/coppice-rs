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

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use coppice_consensus::fs::{write_atomic, Fs, FsFile};
use coppice_core::attempt::{AttemptOutcome, PreemptReason};
use coppice_core::id::{AllocationId, AttemptId, JobId};
use coppice_core::preemption::NoticeSignal;
use coppice_core::time::{Duration, Timestamp};
use coppice_proto::pb::agent::v1 as pb;
use prost::Message;

/// The journal file itself, relative to the data directory. `pub(crate)` so
/// [`crate::identity`] can probe for prior agent state without restating the
/// name (deployment-story A1).
pub(crate) const JOURNAL: &str = "journal";
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
    /// The enforced runtime bound from `StartJob`, so recovery can re-arm the
    /// watchdog. `None` = unbounded.
    pub max_runtime: Option<Duration>,
    /// The job's declared notice contract from `StartJob` (ADR 0044 §3), so
    /// recovery re-signals with the declared signal.
    pub notice_signal: NoticeSignal,
    pub notice: Duration,
    /// The stop-path grace dispatched with `StartJob` (the policy's
    /// `abort_grace`), for a stop the agent runs on its own.
    pub abort_grace: Duration,
    /// The policy's `drain_cleanup_margin` dispatched with `StartJob` (ADR
    /// 0044 §5): a drain plan stops this attempt at `deadline −
    /// cleanup_margin − abort_grace`.
    pub cleanup_margin: Duration,
}

/// Which command wrote a tombstone (ADR 0044 §3), so a tombstone found on
/// recovery resolves as that command would have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TombstoneReason {
    /// A `StopJob` from the coordinator: the abort mechanism.
    Abort,
    /// The agent's own stop path for a platform termination.
    Preempt { reason: PreemptReason },
}

impl TombstoneReason {
    /// The outcome of the command that wrote the tombstone: what a refused
    /// `StartJob`, or an otherwise unattributable kill, resolves as.
    pub fn outcome(self) -> AttemptOutcome {
        match self {
            TombstoneReason::Abort => AttemptOutcome::Aborted,
            TombstoneReason::Preempt { reason } => AttemptOutcome::Preempted { reason },
        }
    }
}

/// A journaled notice mark (ADR 0044 §3): the notice signal was delivered, and
/// this is the attempt's stop time. The latest mark per allocation is in force;
/// a replacement mark is only ever written earlier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoticeMarkRec {
    /// When the agent's stop path runs, agent wall-clock.
    pub stop_at: Timestamp,
    /// Declared notice minus notice actually given; zero when none was lost.
    pub shortfall: Duration,
    /// When the signal was delivered, agent wall-clock: the start of the
    /// workload's window, so a replacement mark can recompute the shortfall.
    pub notified_at: Timestamp,
}

/// The operator's component of the node's drain plan (ADR 0044 §5): the
/// deadline from a `Drain` command and the notice window that came with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatorDrain {
    /// The operator's deadline `D`, wall-clock: the accepted target the
    /// heartbeat echoes.
    pub deadline: Timestamp,
    /// The common notice window (the policy's `preemption_notice_cap`): an
    /// attempt is notified at its `T_stop − notice_window`.
    pub notice_window: Duration,
}

/// The node's whole journaled drain plan (ADR 0044 §5): an operator deadline
/// drain and an external interruption, each independently present. The
/// latest record is in force; with neither component there is no plan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DrainPlanRec {
    pub operator: Option<OperatorDrain>,
    /// The provider's reclaim time, agent wall-clock.
    pub interruption: Option<Timestamp>,
}

impl DrainPlanRec {
    /// Neither component: no plan at all.
    pub fn is_empty(&self) -> bool {
        self.operator.is_none() && self.interruption.is_none()
    }
}

/// A journaled, classified exit (ADR 0013).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitRec {
    pub allocation: AllocationId,
    pub attempt: AttemptId,
    pub job: JobId,
    pub outcome: AttemptOutcome,
    pub runtime: Duration,
}

/// The pure recovered state of the journal: the fencing watermark plus the
/// per-allocation intents, tombstones, and exits. Reconstructed identically by
/// replaying the same records in any run (recovery idempotence).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JournalState {
    pub watermark: Watermark,
    pub intents: BTreeMap<AllocationId, IntentRec>,
    pub tombstones: BTreeMap<AllocationId, TombstoneReason>,
    pub exits: BTreeMap<AllocationId, ExitRec>,
    /// Notice intents (ADR 0044 §3): the platform asked this attempt to stop.
    /// The classifier — a non-zero exit of an allocation listed here is
    /// `Preempted { reason }`.
    pub notice_intents: BTreeMap<AllocationId, PreemptReason>,
    /// The notice mark in force per allocation (the latest written).
    pub marks: BTreeMap<AllocationId, NoticeMarkRec>,
    /// The drain plan in force; empty when there is none.
    pub plan: DrainPlanRec,
}

impl JournalState {
    /// The durable attribution for `allocation`'s exit (ADR 0044 §3): its
    /// tombstone's command and its notice intent.
    pub fn attribution(&self, allocation: &AllocationId) -> crate::executor::Attribution {
        crate::executor::Attribution {
            kill: self.tombstones.get(allocation).map(|t| t.outcome()),
            notice: self.notice_intents.get(allocation).copied(),
        }
    }

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
                let notice_signal = coppice_proto::convert::notice_signal_from_pb(
                    si.notice_signal,
                    "StartIntent.notice_signal",
                )
                .map_err(|_| corrupt("StartIntent.notice_signal"))?;
                self.intents.insert(
                    allocation,
                    IntentRec {
                        allocation,
                        attempt,
                        job,
                        node_epoch: si.node_epoch,
                        max_runtime: si
                            .max_runtime_us
                            .map(|us| Duration::from_micros(i64::try_from(us).unwrap_or(i64::MAX))),
                        notice_signal,
                        notice: try_duration(si.notice_us, "StartIntent.notice_us")?,
                        abort_grace: try_duration(si.abort_grace_us, "StartIntent.abort_grace_us")?,
                        cleanup_margin: try_duration(
                            si.drain_cleanup_margin_us,
                            "StartIntent.drain_cleanup_margin_us",
                        )?,
                    },
                );
            }
            Body::Tombstone(t) => {
                let allocation = try_id(t.allocation.clone(), "AllocationTombstone.allocation")?;
                let reason = tombstone_reason_from_pb(t.reason.clone())?;
                self.tombstones.insert(allocation, reason);
            }
            Body::ObservedExit(e) => {
                let allocation = try_id(e.allocation.clone(), "ObservedExit.allocation")?;
                let attempt = try_id(e.attempt.clone(), "ObservedExit.attempt")?;
                let job = try_id(e.job.clone(), "ObservedExit.job")?;
                let outcome = e
                    .outcome
                    .clone()
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
                        runtime: try_duration(e.runtime_us, "ObservedExit.runtime_us")?,
                    },
                );
            }
            Body::NoticeIntent(n) => {
                let allocation = try_id(n.allocation.clone(), "NoticeIntent.allocation")?;
                let reason = try_reason(n.reason.clone(), "NoticeIntent.reason")?;
                self.notice_intents.insert(allocation, reason);
            }
            Body::NoticeMark(m) => {
                let allocation = try_id(m.allocation.clone(), "NoticeMark.allocation")?;
                // Latest wins: the writer only ever replaces a mark with an
                // earlier one (ADR 0044 §3), so the last record is in force.
                self.marks.insert(
                    allocation,
                    NoticeMarkRec {
                        stop_at: try_timestamp(m.stop_at_us, "NoticeMark.stop_at_us")?,
                        shortfall: try_duration(m.shortfall_us, "NoticeMark.shortfall_us")?,
                        notified_at: try_timestamp(m.notified_at_us, "NoticeMark.notified_at_us")?,
                    },
                );
            }
            Body::DrainPlan(p) => {
                // The whole plan: the latest record replaces it outright.
                self.plan = DrainPlanRec {
                    operator: p
                        .operator_deadline_us
                        .map(|us| -> io::Result<OperatorDrain> {
                            Ok(OperatorDrain {
                                deadline: try_timestamp(us, "DrainPlan.operator_deadline_us")?,
                                notice_window: try_duration(
                                    p.notice_window_us,
                                    "DrainPlan.notice_window_us",
                                )?,
                            })
                        })
                        .transpose()?,
                    interruption: p
                        .interruption_deadline_us
                        .map(|us| try_timestamp(us, "DrainPlan.interruption_deadline_us"))
                        .transpose()?,
                };
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
        for (allocation, reason) in &self.tombstones {
            records.push(record_tombstone(*allocation, *reason));
        }
        for (allocation, reason) in &self.notice_intents {
            records.push(record_notice_intent(*allocation, *reason));
        }
        for (allocation, mark) in &self.marks {
            records.push(record_notice_mark(*allocation, mark));
        }
        // An empty plan is simply absent: there is nothing to restore.
        if !self.plan.is_empty() {
            records.push(record_drain_plan(&self.plan));
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

/// Acquire the data directory's singleton `LOCK` (second-opener refusal,
/// ADR 0017) ahead of journal recovery.
///
/// Split out of [`Journal::open`] so the daemon can take sole ownership of the
/// directory *before* first-boot work that must not race a second process:
/// identity minting writes `node-identity` and enrollment then issues a
/// certificate *for* the minted id ([`crate::identity`], deployment-story A1),
/// so an accidental overlapping start — a service manager plus a manual
/// invocation — must be cut down to one process before either begins, not
/// when the journal finally opens. The returned guard is handed back to
/// [`Journal::open_locked`]; dropping it releases the lock.
pub fn lock_data_dir<F: Fs>(fs: &F) -> io::Result<F::Lock> {
    fs.lock(Path::new(LOCK))
}

impl<F: Fs> Journal<F> {
    /// Open (recovering) the journal at the data directory `fs` is anchored
    /// at. Takes the `LOCK`, scans and truncates any torn tail by rewriting
    /// the compacted live set atomically, and reopens for append. Returns the
    /// journal handle and the recovered [`JournalState`].
    pub fn open(fs: F) -> io::Result<(Journal<F>, JournalState)> {
        let lock = lock_data_dir(&fs)?;
        Self::open_locked(fs, lock)
    }

    /// [`Journal::open`] for a caller that already holds the directory lock
    /// from [`lock_data_dir`] — the daemon, which locks before minting its
    /// identity. The lock's ownership moves into the journal and lives as
    /// long as it does.
    pub fn open_locked(fs: F, lock: F::Lock) -> io::Result<(Journal<F>, JournalState)> {
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
    pub fn journal_tombstone(
        &mut self,
        allocation: AllocationId,
        reason: TombstoneReason,
    ) -> io::Result<()> {
        self.append(&record_tombstone(allocation, reason))
    }

    /// Journal a notice intent (fsynced) *before* the notice signal is
    /// delivered (ADR 0044 §3).
    pub fn journal_notice_intent(
        &mut self,
        allocation: AllocationId,
        reason: PreemptReason,
    ) -> io::Result<()> {
        self.append(&record_notice_intent(allocation, reason))
    }

    /// Journal a notice mark (fsynced) *after* the signal and before it is
    /// reported (ADR 0044 §3) — including a replacement mark, which must be
    /// durable before the earlier stop it carries is re-reported.
    pub fn journal_notice_mark(
        &mut self,
        allocation: AllocationId,
        mark: &NoticeMarkRec,
    ) -> io::Result<()> {
        self.append(&record_notice_mark(allocation, mark))
    }

    /// Journal the node's whole drain plan (fsynced) before acting on it. An
    /// empty plan is how a plan ends.
    pub fn journal_drain_plan(&mut self, plan: &DrainPlanRec) -> io::Result<()> {
        self.append(&record_drain_plan(plan))
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
            max_runtime_us: intent
                .max_runtime
                .map(|d| u64::try_from(d.as_micros()).unwrap_or(0)),
            notice_signal: coppice_proto::convert::notice_signal_to_pb(intent.notice_signal) as i32,
            notice_us: u64::try_from(intent.notice.as_micros()).unwrap_or(0),
            abort_grace_us: u64::try_from(intent.abort_grace.as_micros()).unwrap_or(0),
            drain_cleanup_margin_us: u64::try_from(intent.cleanup_margin.as_micros()).unwrap_or(0),
        })),
    }
}

fn record_tombstone(allocation: AllocationId, reason: TombstoneReason) -> pb::JournalRecord {
    use pb::tombstone_reason as r;
    let reason = match reason {
        TombstoneReason::Abort => r::Reason::Abort(r::Abort {}),
        TombstoneReason::Preempt { reason } => r::Reason::Preempt(r::Preempt {
            reason: Some(reason.into()),
        }),
    };
    pb::JournalRecord {
        body: Some(pb::journal_record::Body::Tombstone(
            pb::AllocationTombstone {
                allocation: Some(allocation.into()),
                reason: Some(pb::TombstoneReason {
                    reason: Some(reason),
                }),
            },
        )),
    }
}

/// A tombstone's reason. Every tombstone is written with one, so a missing
/// reason is corruption like any other missing field.
fn tombstone_reason_from_pb(reason: Option<pb::TombstoneReason>) -> io::Result<TombstoneReason> {
    use pb::tombstone_reason as r;
    match reason.and_then(|r| r.reason) {
        None => Err(corrupt("AllocationTombstone.reason missing")),
        Some(r::Reason::Abort(_)) => Ok(TombstoneReason::Abort),
        Some(r::Reason::Preempt(p)) => Ok(TombstoneReason::Preempt {
            reason: p
                .reason
                .ok_or_else(|| corrupt("TombstoneReason.preempt.reason missing"))?
                .try_into()
                .map_err(|_| corrupt("TombstoneReason.preempt.reason invalid"))?,
        }),
    }
}

fn record_notice_intent(allocation: AllocationId, reason: PreemptReason) -> pb::JournalRecord {
    pb::JournalRecord {
        body: Some(pb::journal_record::Body::NoticeIntent(pb::NoticeIntent {
            allocation: Some(allocation.into()),
            reason: Some(reason.into()),
        })),
    }
}

fn record_notice_mark(allocation: AllocationId, mark: &NoticeMarkRec) -> pb::JournalRecord {
    pb::JournalRecord {
        body: Some(pb::journal_record::Body::NoticeMark(pb::NoticeMark {
            allocation: Some(allocation.into()),
            stop_at_us: mark.stop_at.as_micros(),
            shortfall_us: u64::try_from(mark.shortfall.as_micros()).unwrap_or(0),
            notified_at_us: mark.notified_at.as_micros(),
        })),
    }
}

fn record_drain_plan(plan: &DrainPlanRec) -> pb::JournalRecord {
    pb::JournalRecord {
        body: Some(pb::journal_record::Body::DrainPlan(pb::DrainPlan {
            operator_deadline_us: plan.operator.map(|o| o.deadline.as_micros()),
            notice_window_us: plan.operator.map_or(0, |o| {
                u64::try_from(o.notice_window.as_micros()).unwrap_or(0)
            }),
            interruption_deadline_us: plan.interruption.map(Timestamp::as_micros),
        })),
    }
}

fn record_exit(exit: &ExitRec) -> pb::JournalRecord {
    pb::JournalRecord {
        body: Some(pb::journal_record::Body::ObservedExit(pb::ObservedExit {
            allocation: Some(exit.allocation.into()),
            attempt: Some(exit.attempt.into()),
            job: Some(exit.job.into()),
            outcome: Some((&exit.outcome).into()),
            runtime_us: exit.runtime.as_micros() as u64,
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

/// A journaled span, which the wire encodes as *unsigned* microseconds. Only a
/// corrupt frame can carry one past `i64`, so this reports rather than clamps —
/// same rule as [`try_id`].
fn try_duration(micros: u64, field: &'static str) -> io::Result<Duration> {
    i64::try_from(micros)
        .map(Duration::from_micros)
        .map_err(|_| corrupt(field))
}

/// A journaled wall-clock instant; out of [`Timestamp`]'s range only when
/// corrupt.
fn try_timestamp(micros: i64, field: &'static str) -> io::Result<Timestamp> {
    Timestamp::from_micros(micros).ok_or_else(|| corrupt(field))
}

/// A journaled preemption reason; every writer sets one.
fn try_reason(
    reason: Option<coppice_proto::pb::core::v1::PreemptReason>,
    field: &'static str,
) -> io::Result<PreemptReason> {
    reason
        .ok_or_else(|| corrupt(field))?
        .try_into()
        .map_err(|_| corrupt(field))
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
                max_runtime: Some(Duration::from_secs(30)),
                notice_signal: Default::default(),
                notice: Default::default(),
                abort_grace: Default::default(),
                cleanup_margin: Default::default(),
            };
            journal.journal_intent(&intent).unwrap();
            state.intents.insert(alloc, intent);
            let exit = ExitRec {
                allocation: alloc,
                attempt,
                job,
                outcome: AttemptOutcome::Exited { code: 0 },
                runtime: Duration::from_micros(42),
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
        assert_eq!(
            recovered.exits.get(&alloc).unwrap().runtime,
            Duration::from_micros(42)
        );
    }

    #[test]
    fn recovery_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let (alloc, attempt, job) = ids();
        {
            let fs = RealFs::new(dir.path());
            let (mut journal, _) = Journal::open(fs).unwrap();
            journal
                .journal_tombstone(alloc, TombstoneReason::Abort)
                .unwrap();
            journal
                .journal_intent(&IntentRec {
                    allocation: alloc,
                    attempt,
                    job,
                    node_epoch: 1,
                    max_runtime: None,
                    notice_signal: Default::default(),
                    notice: Default::default(),
                    abort_grace: Default::default(),
                    cleanup_margin: Default::default(),
                })
                .unwrap();
        }
        let first = { Journal::open(RealFs::new(dir.path())).unwrap().1 };
        let second = { Journal::open(RealFs::new(dir.path())).unwrap().1 };
        assert_eq!(first, second);
        assert!(first.tombstones.contains_key(&alloc));
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
                    max_runtime: None,
                    notice_signal: Default::default(),
                    notice: Default::default(),
                    abort_grace: Default::default(),
                    cleanup_margin: Default::default(),
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
