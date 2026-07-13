//! `coppice.storage.v1` snapshot records ↔ `coppice_state` replicated
//! records, and whole-state assembly/disassembly for the snapshot path.
//!
//! The storage layer owns sharding, framing, compression, and CRCs
//! (ADR 0018); this module owns only the payloads. The per-kind record
//! streams ([`job_records`] and friends) are the single mapping from
//! `StateMachine` fields to snapshot record kinds: [`state_to_records`]
//! collects them into per-entity lists (each record individually decodable,
//! carrying its own key) for the slice encoder, while the storage layer's
//! streaming build shards and converts them one window at a time so it never
//! holds a whole-state record copy. [`state_from_records`] rebuilds —
//! including the accrual queue, which is *derived* from the Accruing
//! allocations rather than snapshotted, so there is no second copy to
//! disagree with the allocation records.

use coppice_core::allocation::AllocationState;
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_state::{
    AllocationRecord, AttemptRecord, JobRecord, NodeRecord, QuotaEntity, StateMachine,
};

use super::{req, ConvertError};
use crate::pb::storage::v1 as pb;

// ---- Per-record conversions ----

impl From<&JobRecord> for pb::JobRecord {
    fn from(r: &JobRecord) -> Self {
        pb::JobRecord {
            spec: Some((&r.spec).into()),
            state: Some(r.state.into()),
            multiplier_q32_32: r.multiplier.0,
            submitted_at_us: r.submitted_at_us,
            terminal_at_us: r.terminal_at_us,
            retries_used: r.retries_used,
            attempts: r.attempts.iter().map(|id| (*id).into()).collect(),
        }
    }
}

impl TryFrom<pb::JobRecord> for JobRecord {
    type Error = ConvertError;

    fn try_from(r: pb::JobRecord) -> Result<Self, ConvertError> {
        Ok(JobRecord {
            spec: req(r.spec, "JobRecord.spec")?.try_into()?,
            state: req(r.state, "JobRecord.state")?.try_into()?,
            multiplier: PriorityMultiplier(r.multiplier_q32_32),
            submitted_at_us: r.submitted_at_us,
            terminal_at_us: r.terminal_at_us,
            retries_used: r.retries_used,
            attempts: r
                .attempts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl From<&AttemptRecord> for pb::AttemptRecord {
    fn from(r: &AttemptRecord) -> Self {
        pb::AttemptRecord {
            attempt: Some((&r.attempt).into()),
            group: Some(r.group.into()),
            charge: Some(r.charge.into()),
            rate_ucu_per_second: r.rate_ucu_per_second,
            multiplier_q32_32: r.multiplier.0,
            started_at_us: r.started_at_us,
        }
    }
}

impl TryFrom<pb::AttemptRecord> for AttemptRecord {
    type Error = ConvertError;

    fn try_from(r: pb::AttemptRecord) -> Result<Self, ConvertError> {
        Ok(AttemptRecord {
            attempt: req(r.attempt, "AttemptRecord.attempt")?.try_into()?,
            group: req(r.group, "AttemptRecord.group")?.try_into()?,
            charge: req(r.charge, "AttemptRecord.charge")?.into(),
            rate_ucu_per_second: r.rate_ucu_per_second,
            multiplier: PriorityMultiplier(r.multiplier_q32_32),
            started_at_us: r.started_at_us,
        })
    }
}

impl From<&AllocationRecord> for pb::AllocationRecord {
    fn from(r: &AllocationRecord) -> Self {
        pb::AllocationRecord {
            allocation: Some((&r.allocation).into()),
            seq: r.seq,
        }
    }
}

impl TryFrom<pb::AllocationRecord> for AllocationRecord {
    type Error = ConvertError;

    fn try_from(r: pb::AllocationRecord) -> Result<Self, ConvertError> {
        Ok(AllocationRecord {
            allocation: req(r.allocation, "AllocationRecord.allocation")?.try_into()?,
            seq: r.seq,
        })
    }
}

impl From<&NodeRecord> for pb::NodeRecord {
    fn from(r: &NodeRecord) -> Self {
        pb::NodeRecord {
            node: Some((&r.node).into()),
            epoch: r.epoch,
        }
    }
}

impl TryFrom<pb::NodeRecord> for NodeRecord {
    type Error = ConvertError;

    fn try_from(r: pb::NodeRecord) -> Result<Self, ConvertError> {
        Ok(NodeRecord {
            node: req(r.node, "NodeRecord.node")?.try_into()?,
            epoch: r.epoch,
        })
    }
}

// Quota entities are keyed externally (`StateMachine.quota_entities`), so
// the record carries the key and converts as a (key, entity) pair.

impl From<(&coppice_core::id::QuotaEntityId, &QuotaEntity)> for pb::QuotaEntityRecord {
    fn from((entity, e): (&coppice_core::id::QuotaEntityId, &QuotaEntity)) -> Self {
        pb::QuotaEntityRecord {
            entity: Some((*entity).into()),
            parent: e.parent.map(Into::into),
            name: e.name.clone(),
            quota_ucu: e.quota.0,
            usage: Some(e.usage.into()),
        }
    }
}

impl TryFrom<pb::QuotaEntityRecord> for (coppice_core::id::QuotaEntityId, QuotaEntity) {
    type Error = ConvertError;

    fn try_from(r: pb::QuotaEntityRecord) -> Result<Self, ConvertError> {
        Ok((
            req(r.entity, "QuotaEntityRecord.entity")?.try_into()?,
            QuotaEntity {
                parent: r.parent.map(TryInto::try_into).transpose()?,
                name: r.name,
                quota: CostUnits(r.quota_ucu),
                usage: req(r.usage, "QuotaEntityRecord.usage")?.into(),
            },
        ))
    }
}

// ---- Whole-state assembly ----

/// A `StateMachine` flattened into snapshot records, grouped per entity
/// type — the unit the storage layer shards into sections.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StateRecords {
    pub jobs: Vec<pb::JobRecord>,
    pub attempts: Vec<pb::AttemptRecord>,
    pub allocations: Vec<pb::AllocationRecord>,
    pub nodes: Vec<pb::NodeRecord>,
    pub quota_entities: Vec<pb::QuotaEntityRecord>,
    pub cluster: Option<pb::ClusterStateRecord>,
}

/// Per-kind record counts of a state, in section order.
///
/// The shard planner on the streaming build path sizes each section from
/// these without materializing a single record (the whole point of that
/// path — see the storage layer's `write_state_direct`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecordCounts {
    pub jobs: usize,
    pub attempts: usize,
    pub allocations: usize,
    pub nodes: usize,
    pub quota_entities: usize,
}

/// Count each entity kind without touching a record.
pub fn record_counts(state: &StateMachine) -> RecordCounts {
    RecordCounts {
        jobs: state.jobs.len(),
        attempts: state.attempts.len(),
        allocations: state.allocations.len(),
        nodes: state.nodes.len(),
        quota_entities: state.quota_entities.len(),
    }
}

// ---- Per-kind record streams: the single field→kind mapping ----
//
// These are the one place that maps a `StateMachine` field to a snapshot
// record stream. Both the eager [`state_to_records`] and the storage
// layer's streaming build (which shards each stream and encodes one
// `[start, count)` window at a time) draw from them, so neither the
// field→kind wiring nor the per-record conversion is duplicated.
//
// Each `skip`s on the *unconverted* ordered iterator and only then converts,
// so a sharded build converts just the window it is about to encode — never
// the whole entity list at once. Iteration is in map order
// (`values`/`iter`), so identical states flatten identically; rebuild does
// not depend on the order. The pattern is generic over `Iterator`, so it is
// unaffected by whether a field is a `BTreeMap` or an ordered `imbl::OrdMap`.

/// Job records for the window `[start, start + count)` of the ordered map.
pub fn job_records(
    state: &StateMachine,
    start: usize,
    count: usize,
) -> impl Iterator<Item = pb::JobRecord> + '_ {
    state.jobs.values().skip(start).take(count).map(Into::into)
}

/// Attempt records for the window `[start, start + count)`.
pub fn attempt_records(
    state: &StateMachine,
    start: usize,
    count: usize,
) -> impl Iterator<Item = pb::AttemptRecord> + '_ {
    state
        .attempts
        .values()
        .skip(start)
        .take(count)
        .map(Into::into)
}

/// Allocation records for the window `[start, start + count)`.
pub fn allocation_records(
    state: &StateMachine,
    start: usize,
    count: usize,
) -> impl Iterator<Item = pb::AllocationRecord> + '_ {
    state
        .allocations
        .values()
        .skip(start)
        .take(count)
        .map(Into::into)
}

/// Node records for the window `[start, start + count)`.
pub fn node_records(
    state: &StateMachine,
    start: usize,
    count: usize,
) -> impl Iterator<Item = pb::NodeRecord> + '_ {
    state.nodes.values().skip(start).take(count).map(Into::into)
}

/// Quota-entity records for the window `[start, start + count)`. Each record
/// carries its own key, so the stream is `(&id, &entity)` pairs.
pub fn quota_entity_records(
    state: &StateMachine,
    start: usize,
    count: usize,
) -> impl Iterator<Item = pb::QuotaEntityRecord> + '_ {
    state
        .quota_entities
        .iter()
        .skip(start)
        .take(count)
        .map(Into::into)
}

/// The single `ClusterStateRecord` — the state's scalar tail (policy,
/// versions, allocation sequence). Not sharded: exactly one per snapshot.
pub fn cluster_record(state: &StateMachine) -> pb::ClusterStateRecord {
    pb::ClusterStateRecord {
        policy: Some((&state.policy).into()),
        cluster_version: state.cluster_version,
        version: state.version,
        next_allocation_seq: state.next_allocation_seq,
    }
}

/// Flatten replicated state into snapshot records.
///
/// Iteration is key order, so identical states flatten identically; rebuild
/// does not depend on this order. Built from the per-kind record streams
/// above, the single source of the field→kind mapping.
pub fn state_to_records(state: &StateMachine) -> StateRecords {
    let counts = record_counts(state);
    StateRecords {
        jobs: job_records(state, 0, counts.jobs).collect(),
        attempts: attempt_records(state, 0, counts.attempts).collect(),
        allocations: allocation_records(state, 0, counts.allocations).collect(),
        nodes: node_records(state, 0, counts.nodes).collect(),
        quota_entities: quota_entity_records(state, 0, counts.quota_entities).collect(),
        cluster: Some(cluster_record(state)),
    }
}

/// Rebuild replicated state from snapshot records, in any record order.
///
/// Map keys come from the records themselves (a record decodes alone);
/// duplicates are corruption. The accrual queue is rebuilt from the
/// Accruing allocations — (node, seq) → id, exactly the ADR 0014 funding
/// order.
pub fn state_from_records(records: StateRecords) -> Result<StateMachine, ConvertError> {
    let mut state = StateMachine::default();

    for r in records.jobs {
        let record: JobRecord = r.try_into()?;
        if state.jobs.insert(record.spec.id, record).is_some() {
            return Err(ConvertError::DuplicateEntry("StateRecords.jobs"));
        }
    }
    for r in records.attempts {
        let record: AttemptRecord = r.try_into()?;
        if state.attempts.insert(record.attempt.id, record).is_some() {
            return Err(ConvertError::DuplicateEntry("StateRecords.attempts"));
        }
    }
    for r in records.allocations {
        let record: AllocationRecord = r.try_into()?;
        if record.allocation.state == AllocationState::Accruing {
            state
                .accrual_queue
                .insert((record.allocation.node, record.seq), record.allocation.id);
        }
        if state
            .allocations
            .insert(record.allocation.id, record)
            .is_some()
        {
            return Err(ConvertError::DuplicateEntry("StateRecords.allocations"));
        }
    }
    for r in records.nodes {
        let record: NodeRecord = r.try_into()?;
        if state.nodes.insert(record.node.id, record).is_some() {
            return Err(ConvertError::DuplicateEntry("StateRecords.nodes"));
        }
    }
    for r in records.quota_entities {
        let (entity, record) = r.try_into()?;
        if state.quota_entities.insert(entity, record).is_some() {
            return Err(ConvertError::DuplicateEntry("StateRecords.quota_entities"));
        }
    }

    let cluster = req(records.cluster, "StateRecords.cluster")?;
    state.policy = req(cluster.policy, "ClusterStateRecord.policy")?.try_into()?;
    state.cluster_version = cluster.cluster_version;
    state.version = cluster.version;
    state.next_allocation_seq = cluster.next_allocation_seq;

    Ok(state)
}
