//! Envelope roundtrips: every command arm survives domain → pb → encoded
//! bytes → pb → domain unchanged, and decode failures are the errors the
//! boundary contract promises.

use std::collections::BTreeMap;

use coppice_core::attempt::AttemptOutcome;
use coppice_core::id::{
    AllocationId, AttemptId, GroupId, JobId, NodeId, QuotaEntityId,
};
use coppice_core::job::{Job, RetryPolicy};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_proto::convert::{command_from_pb, command_to_pb, ConvertError};
use coppice_proto::pb;
use coppice_state::command::*;
use coppice_state::PolicyConfig;
use prost::Message;
use uuid::Uuid;

const TS: i64 = 1_760_000_000_000_000;

fn jid(n: u128) -> JobId {
    JobId(Uuid::from_u128(n))
}

fn job(n: u128) -> Job {
    Job {
        id: jid(n),
        image: "registry/img:latest".into(),
        requests: Resources { cpu_millis: 2_000, memory_bytes: 1 << 30, disk_bytes: 0 },
        priority: -2,
        max_runtime_us: Some(3_600_000_000),
        quota_entity: QuotaEntityId(Uuid::from_u128(0xEE)),
        retry: RetryPolicy::default(),
        abort_requested: None,
    }
}

fn every_command() -> Vec<Command> {
    let attempt = AttemptId(Uuid::from_u128(2));
    let alloc = AllocationId(Uuid::from_u128(3));
    let node = NodeId(Uuid::from_u128(4));
    vec![
        Command::SubmitJob(SubmitJob {
            job: job(1),
            multiplier: PriorityMultiplier::from_integer(3),
            submitted_at_us: TS,
        }),
        Command::AbortJob(AbortJob {
            job: jid(1),
            reason: Some("wrong dataset".into()),
            requested_at_us: TS,
        }),
        Command::CommitPlacements(CommitPlacements {
            expected_version: 41,
            revocations: vec![AllocationId(Uuid::from_u128(9))],
            placements: vec![Placement {
                job: jid(1),
                attempt,
                group: GroupId(jid(1).0),
                allocations: vec![AllocationSpec {
                    id: alloc,
                    node,
                    requested: Resources { cpu_millis: 2_000, memory_bytes: 0, disk_bytes: 0 },
                }],
            }],
            proposed_at_us: TS,
        }),
        Command::DispatchAttempt(DispatchAttempt { attempt, dispatched_at_us: TS }),
        Command::RecordAttemptStarted(RecordAttemptStarted { attempt, observed_at_us: TS }),
        Command::RecordAttemptExited(RecordAttemptExited { attempt, observed_at_us: TS }),
        Command::RecordAttemptOutcome(RecordAttemptOutcome {
            attempt,
            outcome: AttemptOutcome::Exited { code: 137 },
            actual_runtime_us: 30_000_000,
            observed_at_us: TS,
        }),
        Command::ReconcileNode(ReconcileNode {
            node,
            node_epoch: 2,
            adopted: vec![attempt],
            lost: vec![LostAttempt {
                attempt: AttemptId(Uuid::from_u128(7)),
                outcome: AttemptOutcome::PullFailed { user_error: false },
                actual_runtime_us: 0,
            }],
            observed_at_us: TS,
        }),
        Command::RegisterNode(RegisterNode {
            node,
            capacity: Resources { cpu_millis: 16_000, memory_bytes: 64 << 30, disk_bytes: 0 },
            labels: BTreeMap::from([("zone".into(), "a".into()), ("gpu".into(), "none".into())]),
            registered_at_us: TS,
        }),
        Command::DeclareNodeLost(DeclareNodeLost { node, declared_at_us: TS }),
        Command::SetNodeSchedulable(SetNodeSchedulable {
            node,
            schedulable: false,
            updated_at_us: TS,
        }),
        Command::EvictTerminalJobs(EvictTerminalJobs { jobs: vec![jid(1), jid(2)], evicted_at_us: TS }),
        Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
            entity: QuotaEntityId(Uuid::from_u128(0xE1)),
            parent: Some(QuotaEntityId(Uuid::from_u128(0xEE))),
            name: "team".into(),
            quota: CostUnits(1_000_000_000),
            updated_at_us: TS,
        }),
        Command::UpdatePolicy(UpdatePolicy {
            policy: PolicyConfig {
                priority_multipliers: BTreeMap::from([
                    (-1, PriorityMultiplier::ONE),
                    (3, PriorityMultiplier::from_integer(3)),
                ]),
                ..PolicyConfig::default()
            },
            updated_at_us: TS,
        }),
        Command::BumpClusterVersion(BumpClusterVersion { to: 2, bumped_at_us: TS }),
    ]
}

#[test]
fn every_command_roundtrips_through_encoded_bytes() {
    for command in every_command() {
        let encoded = command_to_pb(&command, 1).encode_to_vec();
        let decoded = pb::command::v1::Command::decode(encoded.as_slice())
            .expect("envelope must decode");
        let (version, back) = command_from_pb(decoded).expect("conversion must succeed");
        assert_eq!(version, 1);
        assert_eq!(back, command, "roundtrip must be lossless");
    }
}

#[test]
fn abort_requests_roundtrip_inside_job_specs() {
    let mut spec = job(1);
    spec.abort_requested = Some(coppice_core::job::AbortRequest {
        reason: None,
        requested_at_us: TS + 5,
    });
    let submit = Command::SubmitJob(SubmitJob {
        job: spec,
        multiplier: PriorityMultiplier::ONE,
        submitted_at_us: TS,
    });
    let (_, back) = command_from_pb(command_to_pb(&submit, 1)).unwrap();
    assert_eq!(back, submit);
}

#[test]
fn empty_envelope_is_an_error_not_a_skip() {
    let envelope = pb::command::v1::Command { version: 1, body: None };
    assert_eq!(command_from_pb(envelope), Err(ConvertError::MissingField("Command.body")));
}

#[test]
fn malformed_uuids_are_rejected_at_the_boundary() {
    let envelope = pb::command::v1::Command {
        version: 1,
        body: Some(pb::command::v1::command::Body::DispatchAttempt(
            pb::command::v1::DispatchAttempt {
                attempt: Some(pb::core::v1::AttemptId { value: vec![0xAB; 15] }),
                dispatched_at_us: TS,
            },
        )),
    };
    assert_eq!(command_from_pb(envelope), Err(ConvertError::InvalidUuid("AttemptId")));
}

#[test]
fn duplicate_resource_kinds_are_rejected() {
    let quantity = pb::core::v1::ResourceQuantity {
        kind: pb::core::v1::ResourceKind::CpuMillis as i32,
        amount: 1,
    };
    let resources = pb::core::v1::Resources { quantities: vec![quantity, quantity] };
    assert_eq!(
        Resources::try_from(resources),
        Err(ConvertError::DuplicateEntry("Resources.quantities"))
    );
}

#[test]
fn unknown_resource_kinds_fail_loud() {
    // A future kind written past the ClusterVersion gate must error, never
    // silently drop a priced dimension.
    let resources = pb::core::v1::Resources {
        quantities: vec![pb::core::v1::ResourceQuantity { kind: 99, amount: 1 }],
    };
    assert_eq!(
        Resources::try_from(resources),
        Err(ConvertError::UnknownEnum { field: "ResourceQuantity.kind", value: 99 })
    );
}

#[test]
fn resources_encode_canonically() {
    // Ascending kind, zeros omitted — byte-identical encodes for equal values.
    let r = Resources { cpu_millis: 5, memory_bytes: 0, disk_bytes: 7 };
    let encoded = pb::core::v1::Resources::from(&r);
    let kinds: Vec<i32> = encoded.quantities.iter().map(|q| q.kind).collect();
    assert_eq!(kinds, vec![
        pb::core::v1::ResourceKind::CpuMillis as i32,
        pb::core::v1::ResourceKind::DiskBytes as i32,
    ]);
}
