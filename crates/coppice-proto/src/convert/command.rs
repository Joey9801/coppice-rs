//! `coppice.command.v1` ↔ `coppice_state::command` conversions: the
//! versioned envelope and every command arm, plus the replicated
//! `PolicyConfig`.
//!
//! Decode failures here become deterministic `InvalidCommand` rejections at
//! apply (see the module doc on [`super`]); catalog-level shape rules (v1
//! single-allocation placements) pass through untouched so apply can reject
//! them itself.

use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::time::Duration;
use coppice_state::command::{
    AbortJob, AllocationSpec, BumpClusterVersion, Command, CommitPlacements, ConfigureQuotaEntity,
    DeclareNodeLost, DispatchAttempt, EvictTerminalJobs, LostAttempt, Placement, ReconcileNode,
    RecordAttemptExited, RecordAttemptOutcome, RecordAttemptStarted, RegisterNode,
    SetNodeSchedulable, SubmitJob, UpdatePolicy,
};
use coppice_state::PolicyConfig;

use super::core::{labels_from_pb, labels_to_pb, multipliers_from_pb, multipliers_to_pb};
use super::{nonnegative_duration, req, timestamp, ConvertError};
use crate::pb::command::v1 as pb;
use crate::pb::command::v1::command::Body;
use crate::pb::core::v1 as pbcore;

/// Wrap a command in the versioned envelope (ADR 0003).
///
/// `cluster_version` is the replicated `ClusterVersion` the proposer is
/// writing under.
pub fn command_to_pb(command: &Command, cluster_version: u32) -> pb::Command {
    let body = match command {
        Command::SubmitJob(c) => Body::SubmitJob(c.into()),
        Command::AbortJob(c) => Body::AbortJob(c.into()),
        Command::CommitPlacements(c) => Body::CommitPlacements(c.into()),
        Command::DispatchAttempt(c) => Body::DispatchAttempt(c.into()),
        Command::RecordAttemptStarted(c) => Body::RecordAttemptStarted(c.into()),
        Command::RecordAttemptExited(c) => Body::RecordAttemptExited(c.into()),
        Command::RecordAttemptOutcome(c) => Body::RecordAttemptOutcome(c.into()),
        Command::ReconcileNode(c) => Body::ReconcileNode(c.into()),
        Command::RegisterNode(c) => Body::RegisterNode(c.into()),
        Command::DeclareNodeLost(c) => Body::DeclareNodeLost(c.into()),
        Command::SetNodeSchedulable(c) => Body::SetNodeSchedulable(c.into()),
        Command::EvictTerminalJobs(c) => Body::EvictTerminalJobs(c.into()),
        Command::ConfigureQuotaEntity(c) => Body::ConfigureQuotaEntity(c.into()),
        Command::UpdatePolicy(c) => Body::UpdatePolicy(c.into()),
        Command::BumpClusterVersion(c) => Body::BumpClusterVersion(c.into()),
    };
    pb::Command {
        version: cluster_version,
        body: Some(body),
    }
}

/// Unwrap the envelope, returning the version it was written under.
///
/// An absent body means the arm was written by a binary this one does not
/// know — unreachable while the ClusterVersion write gate holds, and an
/// error (never a silent skip) when it does not.
pub fn command_from_pb(command: pb::Command) -> Result<(u32, Command), ConvertError> {
    let command_version = command.version;
    let body = match req(command.body, "Command.body")? {
        Body::SubmitJob(c) => Command::SubmitJob(c.try_into()?),
        Body::AbortJob(c) => Command::AbortJob(c.try_into()?),
        Body::CommitPlacements(c) => Command::CommitPlacements(c.try_into()?),
        Body::DispatchAttempt(c) => Command::DispatchAttempt(c.try_into()?),
        Body::RecordAttemptStarted(c) => Command::RecordAttemptStarted(c.try_into()?),
        Body::RecordAttemptExited(c) => Command::RecordAttemptExited(c.try_into()?),
        Body::RecordAttemptOutcome(c) => Command::RecordAttemptOutcome(c.try_into()?),
        Body::ReconcileNode(c) => Command::ReconcileNode(c.try_into()?),
        Body::RegisterNode(c) => Command::RegisterNode(c.try_into()?),
        Body::DeclareNodeLost(c) => Command::DeclareNodeLost(c.try_into()?),
        Body::SetNodeSchedulable(c) => Command::SetNodeSchedulable(c.try_into()?),
        Body::EvictTerminalJobs(c) => Command::EvictTerminalJobs(c.try_into()?),
        Body::ConfigureQuotaEntity(c) => Command::ConfigureQuotaEntity(c.try_into()?),
        Body::UpdatePolicy(c) => Command::UpdatePolicy(c.try_into()?),
        Body::BumpClusterVersion(c) => Command::BumpClusterVersion(c.try_into()?),
    };
    Ok((command_version, body))
}

// ---- API-proposed ----

impl From<&SubmitJob> for pb::SubmitJob {
    fn from(c: &SubmitJob) -> Self {
        pb::SubmitJob {
            job: Some((&c.job).into()),
            multiplier_q32_32: c.multiplier.0,
            submitted_at_us: c.submitted_at.as_micros(),
        }
    }
}

impl TryFrom<pb::SubmitJob> for SubmitJob {
    type Error = ConvertError;

    fn try_from(c: pb::SubmitJob) -> Result<Self, ConvertError> {
        Ok(SubmitJob {
            job: req(c.job, "SubmitJob.job")?.try_into()?,
            multiplier: PriorityMultiplier(c.multiplier_q32_32),
            submitted_at: timestamp(c.submitted_at_us, "SubmitJob.submitted_at_us")?,
        })
    }
}

impl From<&AbortJob> for pb::AbortJob {
    fn from(c: &AbortJob) -> Self {
        pb::AbortJob {
            job: Some(c.job.into()),
            reason: c.reason.clone(),
            requested_at_us: c.requested_at.as_micros(),
        }
    }
}

impl TryFrom<pb::AbortJob> for AbortJob {
    type Error = ConvertError;

    fn try_from(c: pb::AbortJob) -> Result<Self, ConvertError> {
        Ok(AbortJob {
            job: req(c.job, "AbortJob.job")?.try_into()?,
            reason: c.reason,
            requested_at: timestamp(c.requested_at_us, "AbortJob.requested_at_us")?,
        })
    }
}

// ---- Scheduler-proposed ----

impl From<&CommitPlacements> for pb::CommitPlacements {
    fn from(c: &CommitPlacements) -> Self {
        pb::CommitPlacements {
            expected_version: c.expected_version,
            revocations: c.revocations.iter().map(|id| (*id).into()).collect(),
            placements: c.placements.iter().map(Into::into).collect(),
            proposed_at_us: c.proposed_at.as_micros(),
        }
    }
}

impl TryFrom<pb::CommitPlacements> for CommitPlacements {
    type Error = ConvertError;

    fn try_from(c: pb::CommitPlacements) -> Result<Self, ConvertError> {
        Ok(CommitPlacements {
            expected_version: c.expected_version,
            revocations: c
                .revocations
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            placements: c
                .placements
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            proposed_at: timestamp(c.proposed_at_us, "CommitPlacements.proposed_at_us")?,
        })
    }
}

impl From<&Placement> for pb::Placement {
    fn from(p: &Placement) -> Self {
        pb::Placement {
            job: Some(p.job.into()),
            attempt: Some(p.attempt.into()),
            group: Some(p.group.into()),
            allocations: p.allocations.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<pb::Placement> for Placement {
    type Error = ConvertError;

    fn try_from(p: pb::Placement) -> Result<Self, ConvertError> {
        Ok(Placement {
            job: req(p.job, "Placement.job")?.try_into()?,
            attempt: req(p.attempt, "Placement.attempt")?.try_into()?,
            group: req(p.group, "Placement.group")?.try_into()?,
            allocations: p
                .allocations
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl From<&AllocationSpec> for pb::AllocationSpec {
    fn from(spec: &AllocationSpec) -> Self {
        pb::AllocationSpec {
            id: Some(spec.id.into()),
            node: Some(spec.node.into()),
            requested: Some((&spec.requested).into()),
        }
    }
}

impl TryFrom<pb::AllocationSpec> for AllocationSpec {
    type Error = ConvertError;

    fn try_from(spec: pb::AllocationSpec) -> Result<Self, ConvertError> {
        Ok(AllocationSpec {
            id: req(spec.id, "AllocationSpec.id")?.try_into()?,
            node: req(spec.node, "AllocationSpec.node")?.try_into()?,
            requested: req(spec.requested, "AllocationSpec.requested")?.try_into()?,
        })
    }
}

impl From<&DispatchAttempt> for pb::DispatchAttempt {
    fn from(c: &DispatchAttempt) -> Self {
        pb::DispatchAttempt {
            attempt: Some(c.attempt.into()),
            dispatched_at_us: c.dispatched_at.as_micros(),
        }
    }
}

impl TryFrom<pb::DispatchAttempt> for DispatchAttempt {
    type Error = ConvertError;

    fn try_from(c: pb::DispatchAttempt) -> Result<Self, ConvertError> {
        Ok(DispatchAttempt {
            attempt: req(c.attempt, "DispatchAttempt.attempt")?.try_into()?,
            dispatched_at: timestamp(c.dispatched_at_us, "DispatchAttempt.dispatched_at_us")?,
        })
    }
}

// ---- Agent ingestion ----

impl From<&RecordAttemptStarted> for pb::RecordAttemptStarted {
    fn from(c: &RecordAttemptStarted) -> Self {
        pb::RecordAttemptStarted {
            attempt: Some(c.attempt.into()),
            observed_at_us: c.observed_at.as_micros(),
        }
    }
}

impl TryFrom<pb::RecordAttemptStarted> for RecordAttemptStarted {
    type Error = ConvertError;

    fn try_from(c: pb::RecordAttemptStarted) -> Result<Self, ConvertError> {
        Ok(RecordAttemptStarted {
            attempt: req(c.attempt, "RecordAttemptStarted.attempt")?.try_into()?,
            observed_at: timestamp(c.observed_at_us, "RecordAttemptStarted.observed_at_us")?,
        })
    }
}

impl From<&RecordAttemptExited> for pb::RecordAttemptExited {
    fn from(c: &RecordAttemptExited) -> Self {
        pb::RecordAttemptExited {
            attempt: Some(c.attempt.into()),
            observed_at_us: c.observed_at.as_micros(),
        }
    }
}

impl TryFrom<pb::RecordAttemptExited> for RecordAttemptExited {
    type Error = ConvertError;

    fn try_from(c: pb::RecordAttemptExited) -> Result<Self, ConvertError> {
        Ok(RecordAttemptExited {
            attempt: req(c.attempt, "RecordAttemptExited.attempt")?.try_into()?,
            observed_at: timestamp(c.observed_at_us, "RecordAttemptExited.observed_at_us")?,
        })
    }
}

impl From<&RecordAttemptOutcome> for pb::RecordAttemptOutcome {
    fn from(c: &RecordAttemptOutcome) -> Self {
        pb::RecordAttemptOutcome {
            attempt: Some(c.attempt.into()),
            outcome: Some((&c.outcome).into()),
            actual_runtime_us: c.actual_runtime.as_micros() as u64,
            observed_at_us: c.observed_at.as_micros(),
        }
    }
}

impl TryFrom<pb::RecordAttemptOutcome> for RecordAttemptOutcome {
    type Error = ConvertError;

    fn try_from(c: pb::RecordAttemptOutcome) -> Result<Self, ConvertError> {
        Ok(RecordAttemptOutcome {
            attempt: req(c.attempt, "RecordAttemptOutcome.attempt")?.try_into()?,
            outcome: req(c.outcome, "RecordAttemptOutcome.outcome")?.try_into()?,
            actual_runtime: nonnegative_duration(
                c.actual_runtime_us,
                "RecordAttemptOutcome.actual_runtime_us",
            )?,
            observed_at: timestamp(c.observed_at_us, "RecordAttemptOutcome.observed_at_us")?,
        })
    }
}

impl From<&ReconcileNode> for pb::ReconcileNode {
    fn from(c: &ReconcileNode) -> Self {
        pb::ReconcileNode {
            node: Some(c.node.into()),
            node_epoch: c.node_epoch,
            adopted: c.adopted.iter().map(|id| (*id).into()).collect(),
            lost: c.lost.iter().map(Into::into).collect(),
            observed_at_us: c.observed_at.as_micros(),
        }
    }
}

impl TryFrom<pb::ReconcileNode> for ReconcileNode {
    type Error = ConvertError;

    fn try_from(c: pb::ReconcileNode) -> Result<Self, ConvertError> {
        Ok(ReconcileNode {
            node: req(c.node, "ReconcileNode.node")?.try_into()?,
            node_epoch: c.node_epoch,
            adopted: c
                .adopted
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            lost: c
                .lost
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            observed_at: timestamp(c.observed_at_us, "ReconcileNode.observed_at_us")?,
        })
    }
}

impl From<&LostAttempt> for pb::LostAttempt {
    fn from(l: &LostAttempt) -> Self {
        pb::LostAttempt {
            attempt: Some(l.attempt.into()),
            outcome: Some((&l.outcome).into()),
            actual_runtime_us: l.actual_runtime.as_micros() as u64,
        }
    }
}

impl TryFrom<pb::LostAttempt> for LostAttempt {
    type Error = ConvertError;

    fn try_from(l: pb::LostAttempt) -> Result<Self, ConvertError> {
        Ok(LostAttempt {
            attempt: req(l.attempt, "LostAttempt.attempt")?.try_into()?,
            outcome: req(l.outcome, "LostAttempt.outcome")?.try_into()?,
            actual_runtime: nonnegative_duration(
                l.actual_runtime_us,
                "LostAttempt.actual_runtime_us",
            )?,
        })
    }
}

// ---- Node lifecycle ----

impl From<&RegisterNode> for pb::RegisterNode {
    fn from(c: &RegisterNode) -> Self {
        pb::RegisterNode {
            node: Some(c.node.into()),
            capacity: Some((&c.capacity).into()),
            labels: labels_to_pb(&c.labels),
            registered_at_us: c.registered_at.as_micros(),
            service_addr: c.service_addr.clone(),
        }
    }
}

impl TryFrom<pb::RegisterNode> for RegisterNode {
    type Error = ConvertError;

    fn try_from(c: pb::RegisterNode) -> Result<Self, ConvertError> {
        Ok(RegisterNode {
            node: req(c.node, "RegisterNode.node")?.try_into()?,
            capacity: req(c.capacity, "RegisterNode.capacity")?.try_into()?,
            labels: labels_from_pb(c.labels)?,
            registered_at: timestamp(c.registered_at_us, "RegisterNode.registered_at_us")?,
            // Empty string canonicalizes to None (see Node conversion).
            service_addr: c.service_addr.filter(|s| !s.is_empty()),
        })
    }
}

impl From<&DeclareNodeLost> for pb::DeclareNodeLost {
    fn from(c: &DeclareNodeLost) -> Self {
        pb::DeclareNodeLost {
            node: Some(c.node.into()),
            declared_at_us: c.declared_at.as_micros(),
        }
    }
}

impl TryFrom<pb::DeclareNodeLost> for DeclareNodeLost {
    type Error = ConvertError;

    fn try_from(c: pb::DeclareNodeLost) -> Result<Self, ConvertError> {
        Ok(DeclareNodeLost {
            node: req(c.node, "DeclareNodeLost.node")?.try_into()?,
            declared_at: timestamp(c.declared_at_us, "DeclareNodeLost.declared_at_us")?,
        })
    }
}

impl From<&SetNodeSchedulable> for pb::SetNodeSchedulable {
    fn from(c: &SetNodeSchedulable) -> Self {
        pb::SetNodeSchedulable {
            node: Some(c.node.into()),
            schedulable: c.schedulable,
            updated_at_us: c.updated_at.as_micros(),
        }
    }
}

impl TryFrom<pb::SetNodeSchedulable> for SetNodeSchedulable {
    type Error = ConvertError;

    fn try_from(c: pb::SetNodeSchedulable) -> Result<Self, ConvertError> {
        Ok(SetNodeSchedulable {
            node: req(c.node, "SetNodeSchedulable.node")?.try_into()?,
            schedulable: c.schedulable,
            updated_at: timestamp(c.updated_at_us, "SetNodeSchedulable.updated_at_us")?,
        })
    }
}

// ---- Housekeeping ----

impl From<&EvictTerminalJobs> for pb::EvictTerminalJobs {
    fn from(c: &EvictTerminalJobs) -> Self {
        pb::EvictTerminalJobs {
            jobs: c.jobs.iter().map(|id| (*id).into()).collect(),
            evicted_at_us: c.evicted_at.as_micros(),
        }
    }
}

impl TryFrom<pb::EvictTerminalJobs> for EvictTerminalJobs {
    type Error = ConvertError;

    fn try_from(c: pb::EvictTerminalJobs) -> Result<Self, ConvertError> {
        Ok(EvictTerminalJobs {
            jobs: c
                .jobs
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            evicted_at: timestamp(c.evicted_at_us, "EvictTerminalJobs.evicted_at_us")?,
        })
    }
}

// ---- Admin / policy ----

impl From<&ConfigureQuotaEntity> for pb::ConfigureQuotaEntity {
    fn from(c: &ConfigureQuotaEntity) -> Self {
        pb::ConfigureQuotaEntity {
            entity: Some(c.entity.into()),
            parent: c.parent.map(Into::into),
            name: c.name.clone(),
            quota_ucu: c.quota.0,
            updated_at_us: c.updated_at.as_micros(),
        }
    }
}

impl TryFrom<pb::ConfigureQuotaEntity> for ConfigureQuotaEntity {
    type Error = ConvertError;

    fn try_from(c: pb::ConfigureQuotaEntity) -> Result<Self, ConvertError> {
        Ok(ConfigureQuotaEntity {
            entity: req(c.entity, "ConfigureQuotaEntity.entity")?.try_into()?,
            parent: c.parent.map(TryInto::try_into).transpose()?,
            name: c.name,
            quota: CostUnits(c.quota_ucu),
            updated_at: timestamp(c.updated_at_us, "ConfigureQuotaEntity.updated_at_us")?,
        })
    }
}

impl From<&UpdatePolicy> for pb::UpdatePolicy {
    fn from(c: &UpdatePolicy) -> Self {
        pb::UpdatePolicy {
            policy: Some((&c.policy).into()),
            updated_at_us: c.updated_at.as_micros(),
        }
    }
}

impl TryFrom<pb::UpdatePolicy> for UpdatePolicy {
    type Error = ConvertError;

    fn try_from(c: pb::UpdatePolicy) -> Result<Self, ConvertError> {
        Ok(UpdatePolicy {
            policy: req(c.policy, "UpdatePolicy.policy")?.try_into()?,
            updated_at: timestamp(c.updated_at_us, "UpdatePolicy.updated_at_us")?,
        })
    }
}

impl From<&BumpClusterVersion> for pb::BumpClusterVersion {
    fn from(c: &BumpClusterVersion) -> Self {
        pb::BumpClusterVersion {
            to: c.to,
            bumped_at_us: c.bumped_at.as_micros(),
        }
    }
}

impl TryFrom<pb::BumpClusterVersion> for BumpClusterVersion {
    type Error = ConvertError;

    fn try_from(c: pb::BumpClusterVersion) -> Result<Self, ConvertError> {
        Ok(BumpClusterVersion {
            to: c.to,
            bumped_at: timestamp(c.bumped_at_us, "BumpClusterVersion.bumped_at_us")?,
        })
    }
}

// ---- Replicated policy (shared with the snapshot's ClusterStateRecord) ----

impl From<&PolicyConfig> for pbcore::PolicyConfig {
    fn from(policy: &PolicyConfig) -> Self {
        pbcore::PolicyConfig {
            cost_weights: Some((&policy.cost_weights).into()),
            decay: Some(policy.decay.into()),
            penalty_exponent_milli: policy.penalty_exponent_milli,
            priority_multipliers: multipliers_to_pb(&policy.priority_multipliers),
            accrual_limit: policy.accrual_limit,
            default_charge_runtime_s: policy.default_charge_runtime_s,
            terminal_retention_us: policy.terminal_retention.as_micros(),
            abort_grace_us: policy.abort_grace.as_micros(),
            unbounded_runtime_multiplier_q32_32: Some(policy.unbounded_runtime_multiplier.0),
            refund_fraction_milli: Some(policy.refund_fraction_milli),
        }
    }
}

impl TryFrom<pbcore::PolicyConfig> for PolicyConfig {
    type Error = ConvertError;

    fn try_from(policy: pbcore::PolicyConfig) -> Result<Self, ConvertError> {
        Ok(PolicyConfig {
            cost_weights: req(policy.cost_weights, "PolicyConfig.cost_weights")?.try_into()?,
            decay: req(policy.decay, "PolicyConfig.decay")?.into(),
            penalty_exponent_milli: policy.penalty_exponent_milli,
            priority_multipliers: multipliers_from_pb(policy.priority_multipliers)?,
            accrual_limit: policy.accrual_limit,
            default_charge_runtime_s: policy.default_charge_runtime_s,
            // Signed on the wire and signed in the domain, so this is total;
            // a nonsensical (negative) retention is policy's to validate, not
            // the codec's.
            terminal_retention: Duration::from_micros(policy.terminal_retention_us),
            abort_grace: Duration::from_micros(policy.abort_grace_us),
            // Absent (a policy written by a pre-0029 coordinator) decodes to
            // the neutral values, reproducing today's behaviour — not the new
            // PolicyConfig::default() knobs, which only fresh policies get.
            unbounded_runtime_multiplier: policy
                .unbounded_runtime_multiplier_q32_32
                .map_or(PriorityMultiplier::ONE, PriorityMultiplier),
            refund_fraction_milli: policy.refund_fraction_milli.unwrap_or(1000),
        })
    }
}
