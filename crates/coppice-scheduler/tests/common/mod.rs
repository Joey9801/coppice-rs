//! Shared fixtures for the scheduler integration tests: deterministic ids,
//! command builders, a deterministic id minter for `to_commit_placements`, and
//! a small bootstrapped cluster. Mirrors the house style of
//! `coppice-state/tests/common/mod.rs`.

#![allow(dead_code)]

use std::collections::BTreeMap;

use coppice_core::bytes::ByteSize;
use coppice_core::id::{AllocationId, AttemptId, GroupId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{Job, RetryPolicy};
use coppice_core::quota::{CostUnits, CostWeights, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_state::command::{
    AbortJob, AllocationSpec, CommitPlacements, ConfigureQuotaEntity, DispatchAttempt, Placement,
    RecordAttemptStarted, RegisterNode, SetNodeSchedulable, SubmitJob, UpdatePolicy,
};
use coppice_state::{Applied, Command, PolicyConfig, RejectionReason, StateMachine};
use uuid::Uuid;

/// A plausible base wall-clock instant in µs; tests offset from here and wrap
/// the result in [`ts`]. Kept as raw µs (rather than a `Timestamp` constant)
/// because `Timestamp` has no const constructor — the range check is a
/// runtime one.
pub const TS_US: i64 = 1_760_000_000_000_000;

/// A fixture instant. Test offsets stay within days of [`TS_US`], so the
/// range check cannot fire.
pub fn ts(micros: i64) -> Timestamp {
    Timestamp::from_micros(micros).expect("fixture timestamps are in range")
}

/// The base instant as a [`Timestamp`] — the common case of `ts(TS_US)`.
pub fn base_ts() -> Timestamp {
    ts(TS_US)
}

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
        memory: ByteSize::ZERO,
        disk: ByteSize::ZERO,
    }
}

pub fn res(cpu_millis: u64, memory: ByteSize, disk: ByteSize) -> Resources {
    Resources {
        cpu_millis,
        memory,
        disk,
    }
}

/// A deterministic id minter for [`PlacementProposal::to_commit_placements`]
/// starting at `base`. A test that mints into one state machine across two
/// phases uses two disjoint bases so proposer-minted ids never collide.
pub fn minter_from(base: u128) -> impl FnMut() -> (AttemptId, AllocationId) {
    let mut n = base;
    move || {
        let attempt = AttemptId(Uuid::from_u128(n));
        let allocation = AllocationId(Uuid::from_u128(n + 1));
        n += 2;
        (attempt, allocation)
    }
}

/// The default minter: ids start well past any hand-picked fixture id.
pub fn minter() -> impl FnMut() -> (AttemptId, AllocationId) {
    minter_from(1 << 100)
}

pub fn update_policy_cmd(policy: PolicyConfig) -> Command {
    Command::UpdatePolicy(UpdatePolicy {
        policy,
        updated_at: base_ts(),
    })
}

pub fn configure_entity_cmd(entity: QuotaEntityId, parent: Option<QuotaEntityId>) -> Command {
    Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
        entity,
        parent,
        name: "entity".into(),
        quota: CostUnits(1_000_000_000_000),
        updated_at: base_ts(),
    })
}

pub fn register_node_cmd(node: NodeId, capacity: Resources, at: Timestamp) -> Command {
    Command::RegisterNode(RegisterNode {
        node,
        capacity,
        labels: BTreeMap::new(),
        registered_at: at,
    })
}

pub fn submit_cmd(
    job: JobId,
    requests: Resources,
    max_runtime_s: Option<i64>,
    multiplier: PriorityMultiplier,
    at: Timestamp,
) -> Command {
    Command::SubmitJob(SubmitJob {
        job: Job {
            id: job,
            image: "registry/img:latest".into(),
            command: vec!["run".into()],
            entrypoint: None,
            requests,
            priority: 0,
            max_runtime: max_runtime_s.map(Duration::from_secs),
            quota_entity: ROOT,
            retry: RetryPolicy::default(),
            abort_requested: None,
        },
        multiplier,
        submitted_at: at,
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

pub fn place_cmd(p: Placement, at: Timestamp) -> Command {
    Command::CommitPlacements(CommitPlacements {
        expected_version: 0,
        revocations: vec![],
        placements: vec![p],
        proposed_at: at,
    })
}

pub fn dispatch_cmd(attempt: AttemptId, at: Timestamp) -> Command {
    Command::DispatchAttempt(DispatchAttempt {
        attempt,
        dispatched_at: at,
    })
}

pub fn started_cmd(attempt: AttemptId, at: Timestamp) -> Command {
    Command::RecordAttemptStarted(RecordAttemptStarted {
        attempt,
        observed_at: at,
    })
}

pub fn abort_cmd(job: JobId, at: Timestamp) -> Command {
    Command::AbortJob(AbortJob {
        job,
        reason: Some("test".into()),
        requested_at: at,
    })
}

pub fn drain_cmd(node: NodeId, at: Timestamp) -> Command {
    Command::SetNodeSchedulable(SetNodeSchedulable {
        node,
        schedulable: false,
        updated_at: at,
    })
}

/// Collect the reasons carried by an `InvalidBatch` rejection.
pub fn batch_reasons(reason: &RejectionReason) -> Vec<RejectionReason> {
    match reason {
        RejectionReason::InvalidBatch(items) => {
            items.iter().map(|item| item.reason.clone()).collect()
        }
        other => vec![other.clone()],
    }
}

/// A bootstrapped cluster: root quota entity, reference cost weights, and one
/// node of the given capacity (`nid(1)`).
pub fn setup(capacity: Resources, accrual_limit: u32) -> StateMachine {
    let mut sm = StateMachine::default();
    apply_ok(&mut sm, configure_entity_cmd(ROOT, None));
    apply_ok(&mut sm, update_policy_cmd(test_policy(accrual_limit)));
    apply_ok(&mut sm, register_node_cmd(nid(1), capacity, base_ts()));
    sm
}
