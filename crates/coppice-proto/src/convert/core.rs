//! `coppice.core.v1` ↔ `coppice_core` conversions: ids, resources, labels,
//! job/attempt/allocation/node, and the quota/policy vocabulary.

use std::collections::BTreeMap;

use coppice_core::allocation::{Allocation, AllocationState};
use coppice_core::attempt::{Attempt, AttemptOutcome, AttemptState};
use coppice_core::bytes::ByteSize;
use coppice_core::job::{AbortRequest, Job, JobState, RetryPolicy};
use coppice_core::node::Node;
use coppice_core::quota::{
    ChargeRecord, CostUnits, CostWeights, DecayPolicy, PriorityMultiplier, UsageState,
};
use coppice_core::resource::Resources;

use coppice_core::time::Duration;

use super::{positive_duration, req, timestamp, ConvertError};
use crate::pb::core::v1 as pb;

// ---- Ids ----

macro_rules! convert_id {
    ($name:ident) => {
        impl From<coppice_core::id::$name> for pb::$name {
            fn from(id: coppice_core::id::$name) -> Self {
                pb::$name {
                    value: id.to_string(),
                }
            }
        }

        impl TryFrom<pb::$name> for coppice_core::id::$name {
            type Error = ConvertError;

            fn try_from(id: pb::$name) -> Result<Self, ConvertError> {
                // The typed `<prefix>-<uuid>` form is validated here, so a
                // JobId payload smuggled into a NodeId field fails loudly.
                id.value
                    .parse()
                    .map_err(|_| ConvertError::InvalidId(stringify!($name)))
            }
        }
    };
}

convert_id!(JobId);
convert_id!(NodeId);
convert_id!(AllocationId);
convert_id!(AttemptId);
convert_id!(GroupId);
convert_id!(QuotaEntityId);

// ---- Resources ----

impl From<&Resources> for pb::Resources {
    /// Canonical form by construction: ascending kind, zeros omitted.
    fn from(r: &Resources) -> Self {
        let mut quantities = Vec::new();
        let mut push = |kind: pb::ResourceKind, amount: u64| {
            if amount != 0 {
                quantities.push(pb::ResourceQuantity {
                    kind: kind as i32,
                    amount,
                });
            }
        };
        push(pb::ResourceKind::CpuMillis, r.cpu_millis);
        push(pb::ResourceKind::MemoryBytes, r.memory.as_u64());
        push(pb::ResourceKind::DiskBytes, r.disk.as_u64());
        pb::Resources { quantities }
    }
}

impl TryFrom<pb::Resources> for Resources {
    type Error = ConvertError;

    /// Accepts any entry order (the domain type re-canonicalizes) but
    /// rejects duplicate and unknown kinds — an unknown kind in a committed
    /// payload means the ClusterVersion write gate was violated, and
    /// silently dropping a priced dimension would corrupt accounting.
    fn try_from(r: pb::Resources) -> Result<Self, ConvertError> {
        let mut out = Resources::ZERO;
        let mut seen = [false; 3];
        for q in r.quantities {
            let slot = match pb::ResourceKind::try_from(q.kind) {
                Ok(pb::ResourceKind::CpuMillis) => 0,
                Ok(pb::ResourceKind::MemoryBytes) => 1,
                Ok(pb::ResourceKind::DiskBytes) => 2,
                _ => {
                    return Err(ConvertError::UnknownEnum {
                        field: "ResourceQuantity.kind",
                        value: q.kind,
                    })
                }
            };
            if seen[slot] {
                return Err(ConvertError::DuplicateEntry("Resources.quantities"));
            }
            seen[slot] = true;
            // The wire carries a bare `uint64` of bytes (ADR 0019 freezes the
            // encoding); this is where it becomes a typed size.
            match slot {
                0 => out.cpu_millis = q.amount,
                1 => out.memory = ByteSize::from_bytes(q.amount),
                _ => out.disk = ByteSize::from_bytes(q.amount),
            }
        }
        Ok(out)
    }
}

// ---- Labels ----

/// Canonical form by construction: `BTreeMap` iteration is ascending by key.
pub(crate) fn labels_to_pb(labels: &BTreeMap<String, String>) -> Vec<pb::Label> {
    labels
        .iter()
        .map(|(key, value)| pb::Label {
            key: key.clone(),
            value: value.clone(),
        })
        .collect()
}

pub(crate) fn labels_from_pb(
    labels: Vec<pb::Label>,
) -> Result<BTreeMap<String, String>, ConvertError> {
    let mut out = BTreeMap::new();
    for label in labels {
        if out.insert(label.key, label.value).is_some() {
            return Err(ConvertError::DuplicateEntry("labels"));
        }
    }
    Ok(out)
}

// ---- Job ----

impl From<&Job> for pb::Job {
    fn from(job: &Job) -> Self {
        pb::Job {
            id: Some(job.id.into()),
            image: job.image.clone(),
            command: job.command.clone(),
            entrypoint: job
                .entrypoint
                .as_ref()
                .map(|argv| pb::Entrypoint { argv: argv.clone() }),
            requests: Some((&job.requests).into()),
            priority: job.priority,
            max_runtime_us: job.max_runtime.map(|d| d.as_micros() as u64),
            quota_entity: Some(job.quota_entity.into()),
            retry: Some(job.retry.into()),
            abort_requested: job.abort_requested.as_ref().map(Into::into),
        }
    }
}

impl TryFrom<pb::Job> for Job {
    type Error = ConvertError;

    fn try_from(job: pb::Job) -> Result<Self, ConvertError> {
        // `command` is required, but an empty repeated field decodes the
        // same as an absent one — so emptiness *is* the missing-field check.
        if job.command.is_empty() {
            return Err(ConvertError::MissingField("Job.command"));
        }
        // "No override" is encoded only by absence (see job.proto).
        let entrypoint = match job.entrypoint {
            None => None,
            Some(pb::Entrypoint { argv }) if argv.is_empty() => {
                return Err(ConvertError::Invalid {
                    field: "Job.entrypoint",
                    reason: "override argv must be non-empty",
                });
            }
            Some(pb::Entrypoint { argv }) => Some(argv),
        };
        Ok(Job {
            id: req(job.id, "Job.id")?.try_into()?,
            image: job.image,
            command: job.command,
            entrypoint,
            requests: req(job.requests, "Job.requests")?.try_into()?,
            priority: job.priority,
            max_runtime: job
                .max_runtime_us
                .map(|us| positive_duration(us, "Job.max_runtime_us"))
                .transpose()?,
            quota_entity: req(job.quota_entity, "Job.quota_entity")?.try_into()?,
            retry: req(job.retry, "Job.retry")?.into(),
            abort_requested: job.abort_requested.map(TryInto::try_into).transpose()?,
        })
    }
}

impl From<RetryPolicy> for pb::RetryPolicy {
    fn from(retry: RetryPolicy) -> Self {
        pb::RetryPolicy {
            max_retries: retry.max_retries,
            retry_user_errors: retry.retry_user_errors,
        }
    }
}

impl From<pb::RetryPolicy> for RetryPolicy {
    fn from(retry: pb::RetryPolicy) -> Self {
        RetryPolicy {
            max_retries: retry.max_retries,
            retry_user_errors: retry.retry_user_errors,
        }
    }
}

impl From<&AbortRequest> for pb::AbortRequest {
    fn from(abort: &AbortRequest) -> Self {
        pb::AbortRequest {
            reason: abort.reason.clone(),
            requested_at_us: abort.requested_at.as_micros(),
        }
    }
}

impl TryFrom<pb::AbortRequest> for AbortRequest {
    type Error = ConvertError;

    fn try_from(abort: pb::AbortRequest) -> Result<Self, ConvertError> {
        Ok(AbortRequest {
            reason: abort.reason,
            requested_at: timestamp(abort.requested_at_us, "AbortRequest.requested_at_us")?,
        })
    }
}

impl From<JobState> for pb::JobState {
    fn from(state: JobState) -> Self {
        use pb::job_state as s;
        let state = match state {
            JobState::Submitted => s::State::Submitted(s::Submitted {}),
            JobState::Accepted => s::State::Accepted(s::Accepted {}),
            JobState::Queued => s::State::Queued(s::Queued {}),
            JobState::Attempting(attempt) => s::State::Attempting(s::Attempting {
                attempt: Some(attempt.into()),
            }),
            JobState::Succeeded => s::State::Succeeded(s::Succeeded {}),
            JobState::Failed => s::State::Failed(s::Failed {}),
            JobState::Aborted => s::State::Aborted(s::Aborted {}),
        };
        pb::JobState { state: Some(state) }
    }
}

impl TryFrom<pb::JobState> for JobState {
    type Error = ConvertError;

    fn try_from(state: pb::JobState) -> Result<Self, ConvertError> {
        use pb::job_state as s;
        Ok(match req(state.state, "JobState.state")? {
            s::State::Submitted(_) => JobState::Submitted,
            s::State::Accepted(_) => JobState::Accepted,
            s::State::Queued(_) => JobState::Queued,
            s::State::Attempting(a) => {
                JobState::Attempting(req(a.attempt, "JobState.attempting.attempt")?.try_into()?)
            }
            s::State::Succeeded(_) => JobState::Succeeded,
            s::State::Failed(_) => JobState::Failed,
            s::State::Aborted(_) => JobState::Aborted,
        })
    }
}

// ---- Attempt ----

impl From<&AttemptOutcome> for pb::AttemptOutcome {
    fn from(outcome: &AttemptOutcome) -> Self {
        use pb::attempt_outcome as o;
        let outcome = match outcome {
            AttemptOutcome::Exited { code } => o::Outcome::Exited(o::Exited { code: *code }),
            AttemptOutcome::MemoryLimitExceeded => {
                o::Outcome::MemoryLimitExceeded(o::MemoryLimitExceeded {})
            }
            AttemptOutcome::RuntimeLimitExceeded => {
                o::Outcome::RuntimeLimitExceeded(o::RuntimeLimitExceeded {})
            }
            AttemptOutcome::DiskLimitExceeded => {
                o::Outcome::DiskLimitExceeded(o::DiskLimitExceeded {})
            }
            AttemptOutcome::Aborted => o::Outcome::Aborted(o::Aborted {}),
            AttemptOutcome::Revoked => o::Outcome::Revoked(o::Revoked {}),
            AttemptOutcome::PullFailed { user_error } => o::Outcome::PullFailed(o::PullFailed {
                user_error: *user_error,
            }),
            AttemptOutcome::StartFailed { user_error } => o::Outcome::StartFailed(o::StartFailed {
                user_error: *user_error,
            }),
            AttemptOutcome::NodeLost => o::Outcome::NodeLost(o::NodeLost {}),
            AttemptOutcome::AgentError => o::Outcome::AgentError(o::AgentError {}),
        };
        pb::AttemptOutcome {
            outcome: Some(outcome),
        }
    }
}

impl TryFrom<pb::AttemptOutcome> for AttemptOutcome {
    type Error = ConvertError;

    fn try_from(outcome: pb::AttemptOutcome) -> Result<Self, ConvertError> {
        use pb::attempt_outcome as o;
        Ok(match req(outcome.outcome, "AttemptOutcome.outcome")? {
            o::Outcome::Exited(e) => AttemptOutcome::Exited { code: e.code },
            o::Outcome::MemoryLimitExceeded(_) => AttemptOutcome::MemoryLimitExceeded,
            o::Outcome::RuntimeLimitExceeded(_) => AttemptOutcome::RuntimeLimitExceeded,
            o::Outcome::DiskLimitExceeded(_) => AttemptOutcome::DiskLimitExceeded,
            o::Outcome::Aborted(_) => AttemptOutcome::Aborted,
            o::Outcome::Revoked(_) => AttemptOutcome::Revoked,
            o::Outcome::PullFailed(p) => AttemptOutcome::PullFailed {
                user_error: p.user_error,
            },
            o::Outcome::StartFailed(s) => AttemptOutcome::StartFailed {
                user_error: s.user_error,
            },
            o::Outcome::NodeLost(_) => AttemptOutcome::NodeLost,
            o::Outcome::AgentError(_) => AttemptOutcome::AgentError,
        })
    }
}

impl From<&AttemptState> for pb::AttemptState {
    fn from(state: &AttemptState) -> Self {
        let (phase, outcome) = match state {
            AttemptState::Accruing => (pb::AttemptPhase::Accruing, None),
            AttemptState::Ready => (pb::AttemptPhase::Ready, None),
            AttemptState::Dispatching => (pb::AttemptPhase::Dispatching, None),
            AttemptState::Running => (pb::AttemptPhase::Running, None),
            AttemptState::Finalizing => (pb::AttemptPhase::Finalizing, None),
            AttemptState::Terminal(outcome) => (pb::AttemptPhase::Terminal, Some(outcome.into())),
        };
        pb::AttemptState {
            phase: phase as i32,
            outcome,
        }
    }
}

impl TryFrom<pb::AttemptState> for AttemptState {
    type Error = ConvertError;

    fn try_from(state: pb::AttemptState) -> Result<Self, ConvertError> {
        let phase =
            pb::AttemptPhase::try_from(state.phase).map_err(|_| ConvertError::UnknownEnum {
                field: "AttemptState.phase",
                value: state.phase,
            })?;
        // Strict both ways: an outcome on a non-terminal phase is as
        // malformed as a terminal phase without one.
        if !matches!(phase, pb::AttemptPhase::Terminal) && state.outcome.is_some() {
            return Err(ConvertError::Invalid {
                field: "AttemptState.outcome",
                reason: "outcome set on a non-terminal phase",
            });
        }
        Ok(match phase {
            pb::AttemptPhase::Accruing => AttemptState::Accruing,
            pb::AttemptPhase::Ready => AttemptState::Ready,
            pb::AttemptPhase::Dispatching => AttemptState::Dispatching,
            pb::AttemptPhase::Running => AttemptState::Running,
            pb::AttemptPhase::Finalizing => AttemptState::Finalizing,
            pb::AttemptPhase::Terminal => {
                AttemptState::Terminal(req(state.outcome, "AttemptState.outcome")?.try_into()?)
            }
            pb::AttemptPhase::Unspecified => {
                return Err(ConvertError::UnknownEnum {
                    field: "AttemptState.phase",
                    value: state.phase,
                })
            }
        })
    }
}

impl From<&Attempt> for pb::Attempt {
    fn from(attempt: &Attempt) -> Self {
        pb::Attempt {
            id: Some(attempt.id.into()),
            job: Some(attempt.job.into()),
            allocation: Some(attempt.allocation.into()),
            node: Some(attempt.node.into()),
            state: Some((&attempt.state).into()),
        }
    }
}

impl TryFrom<pb::Attempt> for Attempt {
    type Error = ConvertError;

    fn try_from(attempt: pb::Attempt) -> Result<Self, ConvertError> {
        Ok(Attempt {
            id: req(attempt.id, "Attempt.id")?.try_into()?,
            job: req(attempt.job, "Attempt.job")?.try_into()?,
            allocation: req(attempt.allocation, "Attempt.allocation")?.try_into()?,
            node: req(attempt.node, "Attempt.node")?.try_into()?,
            state: req(attempt.state, "Attempt.state")?.try_into()?,
        })
    }
}

// ---- Allocation ----

impl From<AllocationState> for pb::AllocationState {
    fn from(state: AllocationState) -> Self {
        match state {
            AllocationState::Accruing => pb::AllocationState::Accruing,
            AllocationState::Funded => pb::AllocationState::Funded,
            AllocationState::Active => pb::AllocationState::Active,
            AllocationState::Released => pb::AllocationState::Released,
        }
    }
}

pub(crate) fn allocation_state_from_pb(value: i32) -> Result<AllocationState, ConvertError> {
    match pb::AllocationState::try_from(value) {
        Ok(pb::AllocationState::Accruing) => Ok(AllocationState::Accruing),
        Ok(pb::AllocationState::Funded) => Ok(AllocationState::Funded),
        Ok(pb::AllocationState::Active) => Ok(AllocationState::Active),
        Ok(pb::AllocationState::Released) => Ok(AllocationState::Released),
        _ => Err(ConvertError::UnknownEnum {
            field: "AllocationState",
            value,
        }),
    }
}

impl From<&Allocation> for pb::Allocation {
    fn from(allocation: &Allocation) -> Self {
        pb::Allocation {
            id: Some(allocation.id.into()),
            job: Some(allocation.job.into()),
            attempt: Some(allocation.attempt.into()),
            node: Some(allocation.node.into()),
            requested: Some((&allocation.requested).into()),
            funded: Some((&allocation.funded).into()),
            state: pb::AllocationState::from(allocation.state) as i32,
        }
    }
}

impl TryFrom<pb::Allocation> for Allocation {
    type Error = ConvertError;

    fn try_from(allocation: pb::Allocation) -> Result<Self, ConvertError> {
        Ok(Allocation {
            id: req(allocation.id, "Allocation.id")?.try_into()?,
            job: req(allocation.job, "Allocation.job")?.try_into()?,
            attempt: req(allocation.attempt, "Allocation.attempt")?.try_into()?,
            node: req(allocation.node, "Allocation.node")?.try_into()?,
            requested: req(allocation.requested, "Allocation.requested")?.try_into()?,
            funded: req(allocation.funded, "Allocation.funded")?.try_into()?,
            state: allocation_state_from_pb(allocation.state)?,
        })
    }
}

// ---- Node ----

impl From<&Node> for pb::Node {
    fn from(node: &Node) -> Self {
        pb::Node {
            id: Some(node.id.into()),
            capacity: Some((&node.capacity).into()),
            labels: labels_to_pb(&node.labels),
            schedulable: node.schedulable,
            service_addr: node.service_addr.clone(),
        }
    }
}

impl TryFrom<pb::Node> for Node {
    type Error = ConvertError;

    fn try_from(node: pb::Node) -> Result<Self, ConvertError> {
        Ok(Node {
            id: req(node.id, "Node.id")?.try_into()?,
            capacity: req(node.capacity, "Node.capacity")?.try_into()?,
            labels: labels_from_pb(node.labels)?,
            schedulable: node.schedulable,
            // Empty string is a second spelling of "no service"; canonicalize
            // it to None so absent and present-but-empty read identically.
            service_addr: node.service_addr.filter(|s| !s.is_empty()),
        })
    }
}

// ---- Quota / policy vocabulary ----

impl From<&CostWeights> for pb::CostWeights {
    /// Canonical form by construction: ascending kind, zeros omitted.
    fn from(weights: &CostWeights) -> Self {
        let mut out = Vec::new();
        let mut push = |kind: pb::ResourceKind, weight_q32_32: u64| {
            if weight_q32_32 != 0 {
                out.push(pb::CostWeight {
                    kind: kind as i32,
                    weight_q32_32,
                });
            }
        };
        push(pb::ResourceKind::CpuMillis, weights.per_cpu_milli_second);
        push(
            pb::ResourceKind::MemoryBytes,
            weights.per_memory_byte_second,
        );
        push(pb::ResourceKind::DiskBytes, weights.per_disk_byte_second);
        pb::CostWeights { weights: out }
    }
}

impl TryFrom<pb::CostWeights> for CostWeights {
    type Error = ConvertError;

    fn try_from(weights: pb::CostWeights) -> Result<Self, ConvertError> {
        let mut out = CostWeights::default();
        let mut seen = [false; 3];
        for w in weights.weights {
            let (slot, target) = match pb::ResourceKind::try_from(w.kind) {
                Ok(pb::ResourceKind::CpuMillis) => (0, &mut out.per_cpu_milli_second),
                Ok(pb::ResourceKind::MemoryBytes) => (1, &mut out.per_memory_byte_second),
                Ok(pb::ResourceKind::DiskBytes) => (2, &mut out.per_disk_byte_second),
                _ => {
                    return Err(ConvertError::UnknownEnum {
                        field: "CostWeight.kind",
                        value: w.kind,
                    })
                }
            };
            if seen[slot] {
                return Err(ConvertError::DuplicateEntry("CostWeights.weights"));
            }
            seen[slot] = true;
            *target = w.weight_q32_32;
        }
        Ok(out)
    }
}

impl From<DecayPolicy> for pb::DecayPolicy {
    fn from(decay: DecayPolicy) -> Self {
        pb::DecayPolicy {
            tick_us: decay.tick.as_micros(),
            decay_per_tick_q0_64: decay.decay_per_tick,
        }
    }
}

impl From<pb::DecayPolicy> for DecayPolicy {
    fn from(decay: pb::DecayPolicy) -> Self {
        // A non-positive tick is not rejected here: `DecayPolicy::validate` is
        // the gate for replicated policy, and it reports the whole policy's
        // problems together rather than one field's at a time.
        DecayPolicy {
            tick: Duration::from_micros(decay.tick_us),
            decay_per_tick: decay.decay_per_tick_q0_64,
        }
    }
}

impl From<UsageState> for pb::UsageState {
    fn from(usage: UsageState) -> Self {
        pb::UsageState {
            usage_ucu: usage.usage.0,
            last_update_us: usage.last_update.as_micros(),
        }
    }
}

impl TryFrom<pb::UsageState> for UsageState {
    type Error = ConvertError;

    fn try_from(usage: pb::UsageState) -> Result<Self, ConvertError> {
        Ok(UsageState {
            usage: CostUnits(usage.usage_ucu),
            last_update: timestamp(usage.last_update_us, "UsageState.last_update_us")?,
        })
    }
}

impl From<ChargeRecord> for pb::ChargeRecord {
    fn from(charge: ChargeRecord) -> Self {
        pb::ChargeRecord {
            amount_ucu: charge.amount.0,
            charged_at_us: charge.charged_at.as_micros(),
            refund_fraction_milli: Some(charge.refund_fraction_milli),
        }
    }
}

impl TryFrom<pb::ChargeRecord> for ChargeRecord {
    type Error = ConvertError;

    fn try_from(charge: pb::ChargeRecord) -> Result<Self, ConvertError> {
        Ok(ChargeRecord {
            amount: CostUnits(charge.amount_ucu),
            charged_at: timestamp(charge.charged_at_us, "ChargeRecord.charged_at_us")?,
            // Absent (a charge recorded before ADR 0029) trues up at full
            // refund, exactly as it did then.
            refund_fraction_milli: charge.refund_fraction_milli.unwrap_or(1000),
        })
    }
}

/// Canonical form by construction: `BTreeMap` iteration is ascending by key.
pub(crate) fn multipliers_to_pb(
    multipliers: &BTreeMap<i32, PriorityMultiplier>,
) -> Vec<pb::PriorityMultiplierEntry> {
    multipliers
        .iter()
        .map(|(priority, multiplier)| pb::PriorityMultiplierEntry {
            priority: *priority,
            multiplier_q32_32: multiplier.0,
        })
        .collect()
}

pub(crate) fn multipliers_from_pb(
    entries: Vec<pb::PriorityMultiplierEntry>,
) -> Result<BTreeMap<i32, PriorityMultiplier>, ConvertError> {
    let mut out = BTreeMap::new();
    for entry in entries {
        if out
            .insert(entry.priority, PriorityMultiplier(entry.multiplier_q32_32))
            .is_some()
        {
            return Err(ConvertError::DuplicateEntry(
                "PolicyConfig.priority_multipliers",
            ));
        }
    }
    Ok(out)
}
