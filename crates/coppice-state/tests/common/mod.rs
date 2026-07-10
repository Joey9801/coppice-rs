//! Shared fixtures for the state-machine tests: deterministic ids, command
//! builders, and a canonical bootstrapped cluster.

#![allow(dead_code)]

use std::collections::BTreeMap;

use coppice_core::id::{AllocationId, AttemptId, GroupId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{Job, RetryPolicy};
use coppice_core::quota::{CostUnits, CostWeights, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_state::command::{
    AbortJob, AllocationSpec, CommitPlacements, ConfigureQuotaEntity, DispatchAttempt, Placement,
    RecordAttemptExited, RecordAttemptOutcome, RecordAttemptStarted, RegisterNode, SubmitJob,
    UpdatePolicy,
};
use coppice_state::{Applied, Command, PolicyConfig, StateMachine};
use uuid::Uuid;

/// A plausible base wall-clock instant; tests offset from here.
pub const TS: i64 = 1_760_000_000_000_000;

pub const ROOT: QuotaEntityId = QuotaEntityId(Uuid::from_u128(0xEE));

pub fn jid(n: u128) -> JobId {
    JobId(Uuid::from_u128(n))
}
pub fn nid(n: u128) -> NodeId {
    NodeId(Uuid::from_u128(n))
}
pub fn aid(n: u128) -> AttemptId {
    AttemptId(Uuid::from_u128(n))
}
pub fn alid(n: u128) -> AllocationId {
    AllocationId(Uuid::from_u128(n))
}
pub fn qid(n: u128) -> QuotaEntityId {
    QuotaEntityId(Uuid::from_u128(n))
}

pub fn apply_ok(sm: &mut StateMachine, cmd: Command) -> Applied {
    match sm.apply(&cmd) {
        Ok(applied) => applied,
        Err(reason) => panic!("expected accept, got rejection {reason}: {cmd:?}"),
    }
}

/// The reference calibration from ADR 0019's tests: 1 core-second = 1 CU.
pub fn test_weights() -> CostWeights {
    CostWeights {
        per_cpu_milli_second: 1000 << 32,
        per_memory_byte_second: 1_000_000,
        per_disk_byte_second: 62_500,
    }
}

pub fn test_policy(accrual_limit: u32) -> PolicyConfig {
    PolicyConfig {
        cost_weights: test_weights(),
        accrual_limit,
        ..PolicyConfig::default()
    }
}

pub fn cpu(millis: u64) -> Resources {
    Resources {
        cpu_millis: millis,
        memory_bytes: 0,
        disk_bytes: 0,
    }
}

pub fn update_policy_cmd(policy: PolicyConfig) -> Command {
    Command::UpdatePolicy(UpdatePolicy {
        policy,
        updated_at_us: TS,
    })
}

pub fn configure_entity_cmd(entity: QuotaEntityId, parent: Option<QuotaEntityId>) -> Command {
    Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
        entity,
        parent,
        name: "entity".into(),
        quota: CostUnits(1_000_000_000_000),
        updated_at_us: TS,
    })
}

pub fn register_node_cmd(node: NodeId, capacity: Resources, ts: i64) -> Command {
    Command::RegisterNode(RegisterNode {
        node,
        capacity,
        labels: BTreeMap::new(),
        registered_at_us: ts,
    })
}

pub fn submit_cmd(
    job: JobId,
    requests: Resources,
    max_runtime_s: Option<u64>,
    retry: RetryPolicy,
) -> Command {
    Command::SubmitJob(SubmitJob {
        job: Job {
            id: job,
            image: "registry/img:latest".into(),
            requests,
            priority: 0,
            max_runtime_us: max_runtime_s.map(|s| s * 1_000_000),
            quota_entity: ROOT,
            retry,
            abort_requested: None,
        },
        multiplier: PriorityMultiplier::ONE,
        submitted_at_us: TS,
    })
}

pub fn placement(
    job: JobId,
    attempt: AttemptId,
    alloc: AllocationId,
    node: NodeId,
    requested: Resources,
) -> Placement {
    Placement {
        job,
        attempt,
        group: GroupId(job.0),
        allocations: vec![AllocationSpec {
            id: alloc,
            node,
            requested,
        }],
    }
}

pub fn place_cmd(p: Placement, ts: i64) -> Command {
    Command::CommitPlacements(CommitPlacements {
        expected_version: 0,
        revocations: vec![],
        placements: vec![p],
        proposed_at_us: ts,
    })
}

pub fn dispatch_cmd(attempt: AttemptId, ts: i64) -> Command {
    Command::DispatchAttempt(DispatchAttempt {
        attempt,
        dispatched_at_us: ts,
    })
}

pub fn started_cmd(attempt: AttemptId, ts: i64) -> Command {
    Command::RecordAttemptStarted(RecordAttemptStarted {
        attempt,
        observed_at_us: ts,
    })
}

pub fn exited_cmd(attempt: AttemptId, ts: i64) -> Command {
    Command::RecordAttemptExited(RecordAttemptExited {
        attempt,
        observed_at_us: ts,
    })
}

pub fn outcome_cmd(
    attempt: AttemptId,
    outcome: coppice_core::attempt::AttemptOutcome,
    runtime_s: u64,
    ts: i64,
) -> Command {
    Command::RecordAttemptOutcome(RecordAttemptOutcome {
        attempt,
        outcome,
        actual_runtime_us: runtime_s * 1_000_000,
        observed_at_us: ts,
    })
}

pub fn abort_cmd(job: JobId, ts: i64) -> Command {
    Command::AbortJob(AbortJob {
        job,
        reason: Some("test".into()),
        requested_at_us: ts,
    })
}

/// A bootstrapped cluster: root quota entity, reference cost weights, one
/// 10-core node (`nid(1)`).
pub fn setup() -> StateMachine {
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, configure_entity_cmd(ROOT, None));
    apply_ok(&mut sm, update_policy_cmd(test_policy(4)));
    apply_ok(&mut sm, register_node_cmd(nid(1), cpu(10_000), TS));
    sm
}
