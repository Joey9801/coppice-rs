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
use coppice_core::id::{EnrollTokenId, MachineId};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::time::Timestamp;
use coppice_state::{
    AllocationRecord, AttemptRecord, CaCertBundle, CaCertificate, EnrollToken, JobRecord,
    MachineBinding, NodeRecord, QuotaEntity, RevokedIdentity, StateMachine,
};

use super::command::{enroll_role_from_pb, enroll_role_to_pb};
use super::{req, timestamp, ConvertError};
use crate::pb::core::v1 as pbcore;
use crate::pb::storage::v1 as pb;

// ---- Per-record conversions ----

impl From<&JobRecord> for pb::JobRecord {
    fn from(r: &JobRecord) -> Self {
        pb::JobRecord {
            spec: Some((&r.spec).into()),
            state: Some(r.state.into()),
            multiplier_q32_32: r.multiplier.0,
            submitted_at_us: r.submitted_at.as_micros(),
            terminal_at_us: r.terminal_at.map(|t| t.as_micros()),
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
            submitted_at: timestamp(r.submitted_at_us, "JobRecord.submitted_at_us")?,
            terminal_at: r
                .terminal_at_us
                .map(|us| timestamp(us, "JobRecord.terminal_at_us"))
                .transpose()?,
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
            started_at_us: r.started_at.map(|t| t.as_micros()),
        }
    }
}

impl TryFrom<pb::AttemptRecord> for AttemptRecord {
    type Error = ConvertError;

    fn try_from(r: pb::AttemptRecord) -> Result<Self, ConvertError> {
        Ok(AttemptRecord {
            attempt: req(r.attempt, "AttemptRecord.attempt")?.try_into()?,
            group: req(r.group, "AttemptRecord.group")?.try_into()?,
            charge: req(r.charge, "AttemptRecord.charge")?.try_into()?,
            rate_ucu_per_second: r.rate_ucu_per_second,
            multiplier: PriorityMultiplier(r.multiplier_q32_32),
            started_at: r
                .started_at_us
                .map(|us| timestamp(us, "AttemptRecord.started_at_us"))
                .transpose()?,
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
            created_at_us: e.created_at.as_micros(),
            updated_at_us: e.updated_at.as_micros(),
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
                usage: req(r.usage, "QuotaEntityRecord.usage")?.try_into()?,
                // Pre-timestamp snapshots decode these as 0 (epoch) — an
                // accepted loss of a display-only instant.
                created_at: timestamp(r.created_at_us, "QuotaEntityRecord.created_at_us")?,
                updated_at: timestamp(r.updated_at_us, "QuotaEntityRecord.updated_at_us")?,
            },
        ))
    }
}

// ---- Cluster PKI / identity records (ADR 0037) ----

impl From<&CaCertificate> for pbcore::CaCertificate {
    fn from(ca: &CaCertificate) -> Self {
        pbcore::CaCertificate {
            cert_pem: ca.bundle.pem().to_string(),
            recorded_at_us: ca.recorded_at.as_micros(),
        }
    }
}

impl TryFrom<pbcore::CaCertificate> for CaCertificate {
    type Error = ConvertError;

    fn try_from(ca: pbcore::CaCertificate) -> Result<Self, ConvertError> {
        Ok(CaCertificate {
            // Same gate as the command path: a snapshot section is another
            // door into replicated state, and it gets the same lock.
            bundle: CaCertBundle::parse(ca.cert_pem).map_err(|_| ConvertError::Invalid {
                field: "CaCertificate.cert_pem",
                reason: "not a sequence of X.509 CA certificate PEM blocks",
            })?,
            recorded_at: timestamp(ca.recorded_at_us, "CaCertificate.recorded_at_us")?,
        })
    }
}

// Bindings are keyed externally (`StateMachine.machine_bindings`), so the
// record carries the key and converts as a (key, binding) pair.

impl From<(&MachineId, &MachineBinding)> for pb::MachineBindingRecord {
    fn from((machine, b): (&MachineId, &MachineBinding)) -> Self {
        pb::MachineBindingRecord {
            machine: Some((*machine).into()),
            raft_node_id: b.raft_node_id,
            address: b.address.clone(),
            bound_at_us: b.bound_at.as_micros(),
        }
    }
}

impl TryFrom<pb::MachineBindingRecord> for (MachineId, MachineBinding) {
    type Error = ConvertError;

    fn try_from(r: pb::MachineBindingRecord) -> Result<Self, ConvertError> {
        Ok((
            req(r.machine, "MachineBindingRecord.machine")?.try_into()?,
            MachineBinding {
                raft_node_id: r.raft_node_id,
                address: r.address,
                bound_at: timestamp(r.bound_at_us, "MachineBindingRecord.bound_at_us")?,
            },
        ))
    }
}

impl From<(&EnrollTokenId, &EnrollToken)> for pb::EnrollTokenRecord {
    fn from((token, t): (&EnrollTokenId, &EnrollToken)) -> Self {
        pb::EnrollTokenRecord {
            token: Some((*token).into()),
            hash: t.hash.clone(),
            role: enroll_role_to_pb(t.role) as i32,
            label: t.label.clone(),
            expires_at_us: t.expires_at.map(|e| e.as_micros()),
            minted_at_us: t.minted_at.as_micros(),
            revoked: t.revoked,
        }
    }
}

impl TryFrom<pb::EnrollTokenRecord> for (EnrollTokenId, EnrollToken) {
    type Error = ConvertError;

    fn try_from(r: pb::EnrollTokenRecord) -> Result<Self, ConvertError> {
        Ok((
            req(r.token, "EnrollTokenRecord.token")?.try_into()?,
            EnrollToken {
                hash: r.hash,
                role: enroll_role_from_pb(r.role, "EnrollTokenRecord.role")?,
                label: r.label,
                expires_at: r
                    .expires_at_us
                    .map(|us| timestamp(us, "EnrollTokenRecord.expires_at_us"))
                    .transpose()?,
                minted_at: timestamp(r.minted_at_us, "EnrollTokenRecord.minted_at_us")?,
                revoked: r.revoked,
            },
        ))
    }
}

impl From<&RevokedIdentity> for pb::RevokedIdentityRecord {
    fn from(identity: &RevokedIdentity) -> Self {
        pb::RevokedIdentityRecord {
            identity: Some(identity.into()),
        }
    }
}

impl TryFrom<pb::RevokedIdentityRecord> for RevokedIdentity {
    type Error = ConvertError;

    fn try_from(r: pb::RevokedIdentityRecord) -> Result<Self, ConvertError> {
        req(r.identity, "RevokedIdentityRecord.identity")?.try_into()
    }
}

// Key confirmations are keyed by the raft node id, so the record carries the
// key and converts as a (raft_node_id, confirmed_at) pair.

impl From<(&u64, &Timestamp)> for pb::KeyConfirmationRecord {
    fn from((raft_node_id, confirmed_at): (&u64, &Timestamp)) -> Self {
        pb::KeyConfirmationRecord {
            raft_node_id: *raft_node_id,
            confirmed_at_us: confirmed_at.as_micros(),
        }
    }
}

impl TryFrom<pb::KeyConfirmationRecord> for (u64, Timestamp) {
    type Error = ConvertError;

    fn try_from(r: pb::KeyConfirmationRecord) -> Result<Self, ConvertError> {
        Ok((
            r.raft_node_id,
            timestamp(r.confirmed_at_us, "KeyConfirmationRecord.confirmed_at_us")?,
        ))
    }
}

// Enrolled identities are keyed by the machine id, so the record carries the
// key and converts as a (machine, recorded_at) pair.

impl From<(&MachineId, &Timestamp)> for pb::EnrolledIdentityRecord {
    fn from((machine, recorded_at): (&MachineId, &Timestamp)) -> Self {
        pb::EnrolledIdentityRecord {
            machine: Some((*machine).into()),
            recorded_at_us: recorded_at.as_micros(),
        }
    }
}

impl TryFrom<pb::EnrolledIdentityRecord> for (MachineId, Timestamp) {
    type Error = ConvertError;

    fn try_from(r: pb::EnrolledIdentityRecord) -> Result<Self, ConvertError> {
        Ok((
            req(r.machine, "EnrolledIdentityRecord.machine")?.try_into()?,
            timestamp(r.recorded_at_us, "EnrolledIdentityRecord.recorded_at_us")?,
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
    pub machine_bindings: Vec<pb::MachineBindingRecord>,
    pub enroll_tokens: Vec<pb::EnrollTokenRecord>,
    pub revoked_identities: Vec<pb::RevokedIdentityRecord>,
    pub key_confirmations: Vec<pb::KeyConfirmationRecord>,
    pub enrolled_identities: Vec<pb::EnrolledIdentityRecord>,
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
    pub machine_bindings: usize,
    pub enroll_tokens: usize,
    pub revoked_identities: usize,
    pub key_confirmations: usize,
    pub enrolled_identities: usize,
}

/// Count each entity kind without touching a record.
pub fn record_counts(state: &StateMachine) -> RecordCounts {
    RecordCounts {
        jobs: state.jobs.len(),
        attempts: state.attempts.len(),
        allocations: state.allocations.len(),
        nodes: state.nodes.len(),
        quota_entities: state.quota_entities.len(),
        machine_bindings: state.machine_bindings.len(),
        enroll_tokens: state.enroll_tokens.len(),
        revoked_identities: state.revoked_identities.len(),
        key_confirmations: state.key_confirmations.len(),
        enrolled_identities: state.enrolled_identities.len(),
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

/// Machine-binding records for the window `[start, start + count)`. Each
/// record carries its own key, so the stream is `(&id, &binding)` pairs.
pub fn machine_binding_records(
    state: &StateMachine,
    start: usize,
    count: usize,
) -> impl Iterator<Item = pb::MachineBindingRecord> + '_ {
    state
        .machine_bindings
        .iter()
        .skip(start)
        .take(count)
        .map(Into::into)
}

/// Enroll-token records for the window `[start, start + count)`, `(&id, &token)`
/// pairs.
pub fn enroll_token_records(
    state: &StateMachine,
    start: usize,
    count: usize,
) -> impl Iterator<Item = pb::EnrollTokenRecord> + '_ {
    state
        .enroll_tokens
        .iter()
        .skip(start)
        .take(count)
        .map(Into::into)
}

/// Revoked-identity records for the window `[start, start + count)`. The record
/// *is* the identity, so the stream is `&RevokedIdentity`.
pub fn revoked_identity_records(
    state: &StateMachine,
    start: usize,
    count: usize,
) -> impl Iterator<Item = pb::RevokedIdentityRecord> + '_ {
    state
        .revoked_identities
        .iter()
        .skip(start)
        .take(count)
        .map(Into::into)
}

/// Key-confirmation records for the window `[start, start + count)`,
/// `(&raft_node_id, &confirmed_at)` pairs.
pub fn key_confirmation_records(
    state: &StateMachine,
    start: usize,
    count: usize,
) -> impl Iterator<Item = pb::KeyConfirmationRecord> + '_ {
    state
        .key_confirmations
        .iter()
        .skip(start)
        .take(count)
        .map(Into::into)
}

/// Enrolled-identity records for the window `[start, start + count)`,
/// `(&machine, &recorded_at)` pairs.
pub fn enrolled_identity_records(
    state: &StateMachine,
    start: usize,
    count: usize,
) -> impl Iterator<Item = pb::EnrolledIdentityRecord> + '_ {
    state
        .enrolled_identities
        .iter()
        .skip(start)
        .take(count)
        .map(Into::into)
}

/// The single `ClusterStateRecord` — the state's scalar tail (policy,
/// versions, allocation sequence, and the singleton CA bundle). Not sharded:
/// exactly one per snapshot.
pub fn cluster_record(state: &StateMachine) -> pb::ClusterStateRecord {
    pb::ClusterStateRecord {
        policy: Some((&state.policy).into()),
        cluster_version: state.cluster_version,
        version: state.version,
        next_allocation_seq: state.next_allocation_seq,
        ca: state.ca.as_ref().map(Into::into),
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
        machine_bindings: machine_binding_records(state, 0, counts.machine_bindings).collect(),
        enroll_tokens: enroll_token_records(state, 0, counts.enroll_tokens).collect(),
        revoked_identities: revoked_identity_records(state, 0, counts.revoked_identities).collect(),
        key_confirmations: key_confirmation_records(state, 0, counts.key_confirmations).collect(),
        enrolled_identities: enrolled_identity_records(state, 0, counts.enrolled_identities)
            .collect(),
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
    for r in records.machine_bindings {
        let (machine, binding) = r.try_into()?;
        if state.machine_bindings.insert(machine, binding).is_some() {
            return Err(ConvertError::DuplicateEntry(
                "StateRecords.machine_bindings",
            ));
        }
    }
    for r in records.enroll_tokens {
        let (token, record) = r.try_into()?;
        if state.enroll_tokens.insert(token, record).is_some() {
            return Err(ConvertError::DuplicateEntry("StateRecords.enroll_tokens"));
        }
    }
    for r in records.revoked_identities {
        let identity: RevokedIdentity = r.try_into()?;
        if !state.revoked_identities.insert(identity) {
            return Err(ConvertError::DuplicateEntry(
                "StateRecords.revoked_identities",
            ));
        }
    }
    for r in records.key_confirmations {
        let (raft_node_id, confirmed_at) = r.try_into()?;
        if state
            .key_confirmations
            .insert(raft_node_id, confirmed_at)
            .is_some()
        {
            return Err(ConvertError::DuplicateEntry(
                "StateRecords.key_confirmations",
            ));
        }
    }
    for r in records.enrolled_identities {
        let (machine, recorded_at) = r.try_into()?;
        if state
            .enrolled_identities
            .insert(machine, recorded_at)
            .is_some()
        {
            return Err(ConvertError::DuplicateEntry(
                "StateRecords.enrolled_identities",
            ));
        }
    }

    let cluster = req(records.cluster, "StateRecords.cluster")?;
    state.policy = req(cluster.policy, "ClusterStateRecord.policy")?.try_into()?;
    state.cluster_version = cluster.cluster_version;
    state.version = cluster.version;
    state.next_allocation_seq = cluster.next_allocation_seq;
    state.ca = cluster.ca.map(TryInto::try_into).transpose()?;

    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::id::QuotaEntityId;
    use coppice_core::quota::{CostUnits, UsageState};
    use coppice_core::time::Timestamp;

    fn ts(micros: i64) -> Timestamp {
        Timestamp::from_micros(micros).expect("fixture timestamps are in range")
    }

    #[test]
    fn quota_entity_record_roundtrips_created_and_updated_timestamps() {
        let id = QuotaEntityId(uuid::Uuid::from_u128(0xE1));
        let entity = QuotaEntity {
            parent: Some(QuotaEntityId(uuid::Uuid::from_u128(0xEE))),
            name: "team".to_string(),
            quota: CostUnits(1_000_000),
            usage: UsageState::new(ts(5_000_000)),
            created_at: ts(1_000_000),
            updated_at: ts(9_000_000),
        };

        let record: pb::QuotaEntityRecord = (&id, &entity).into();
        assert_eq!(record.created_at_us, 1_000_000);
        assert_eq!(record.updated_at_us, 9_000_000);

        let (decoded_id, decoded): (QuotaEntityId, QuotaEntity) = record.try_into().unwrap();
        assert_eq!(decoded_id, id);
        assert_eq!(decoded, entity);
    }

    #[test]
    fn quota_entity_record_without_timestamps_decodes_to_epoch() {
        // A snapshot written before the timestamp fields existed leaves both
        // int64s at their proto3 default of 0, which must decode to epoch-0
        // instants rather than fail — an accepted loss of a display-only value.
        let record = pb::QuotaEntityRecord {
            entity: Some(QuotaEntityId(uuid::Uuid::from_u128(0xE2)).into()),
            parent: None,
            name: "legacy".to_string(),
            quota_ucu: 42,
            usage: Some(UsageState::new(ts(0)).into()),
            created_at_us: 0,
            updated_at_us: 0,
        };

        let (_, decoded): (QuotaEntityId, QuotaEntity) = record.try_into().unwrap();
        assert_eq!(decoded.created_at, ts(0));
        assert_eq!(decoded.updated_at, ts(0));
    }
}
