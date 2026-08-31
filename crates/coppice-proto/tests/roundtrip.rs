//! Envelope roundtrips: every command arm survives domain → pb → encoded
//! bytes → pb → domain unchanged, and decode failures are the errors the
//! boundary contract promises.

use std::collections::BTreeMap;

use coppice_core::attempt::AttemptOutcome;
use coppice_core::bytes::ByteSize;
use coppice_core::id::{
    AllocationId, AttemptId, EnrollTokenId, GroupId, JobId, MachineId, NodeId, QuotaEntityId,
};
use coppice_core::job::{Job, JobState, RetryPolicy};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_proto::convert::{command_from_pb, command_to_pb, ConvertError};
use coppice_proto::pb;
use coppice_state::authz::{Actor, Binding, Role, Subject};
use coppice_state::command::*;
use coppice_state::{EnrollRole, PolicyConfig, RevokedIdentity, DEFAULT_GROUPS_CLAIM};
use prost::Message;
use uuid::Uuid;

const TS_US: i64 = 1_760_000_000_000_000;

/// The fixture instant, well inside the representable range.
fn ts() -> Timestamp {
    Timestamp::from_micros(TS_US).expect("fixture timestamp is in range")
}

fn jid(n: u128) -> JobId {
    JobId(Uuid::from_u128(n))
}

fn job(n: u128) -> Job {
    Job {
        id: jid(n),
        image: "registry/img:latest".into(),
        command: vec!["run".into(), "--epochs".into(), "3".into()],
        entrypoint: Some(vec!["/bin/launch".into()]),
        requests: Resources {
            cpu_millis: 2_000,
            memory: ByteSize::from_gib(1),
            disk: ByteSize::ZERO,
        },
        priority: -2,
        max_runtime: Some(Duration::from_hours(1)),
        quota_entity: QuotaEntityId(Uuid::from_u128(0xEE)),
        retry: RetryPolicy::default(),
        abort_requested: None,
        submitted_by: Some("user-42".into()),
    }
}

/// A token-derived actor: a plain principal carrying group membership, no
/// break-glass flags.
fn grouped_actor() -> Actor {
    Actor {
        principal: "user-42".into(),
        groups: vec!["platform".into(), "ml-team".into()],
        operator_cert: false,
        auth_disabled: false,
    }
}

/// The operator-certificate break-glass path (ADR 0022): an implicit
/// unscoped admin outside the bindings list.
fn operator_cert_actor() -> Actor {
    Actor {
        principal: "cert:node-7".into(),
        groups: vec![],
        operator_cert: true,
        auth_disabled: false,
    }
}

/// The open posture (`[auth] insecure_open`): a static anonymous actor with
/// implicit unscoped admin, distinct from an operator certificate.
fn open_posture_actor() -> Actor {
    Actor {
        principal: String::new(),
        groups: vec![],
        operator_cert: false,
        auth_disabled: true,
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
            submitted_at: ts(),
            actor: Some(grouped_actor()),
        }),
        Command::AbortJob(AbortJob {
            job: jid(1),
            reason: Some("wrong dataset".into()),
            requested_at: ts(),
            actor: Some(operator_cert_actor()),
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
                    requested: Resources {
                        cpu_millis: 2_000,
                        memory: ByteSize::ZERO,
                        disk: ByteSize::ZERO,
                    },
                }],
            }],
            proposed_at: ts(),
        }),
        Command::DispatchAttempt(DispatchAttempt {
            attempt,
            dispatched_at: ts(),
        }),
        Command::RecordAttemptStarted(RecordAttemptStarted {
            attempt,
            observed_at: ts(),
        }),
        Command::RecordAttemptExited(RecordAttemptExited {
            attempt,
            observed_at: ts(),
        }),
        Command::RecordAttemptOutcome(RecordAttemptOutcome {
            attempt,
            outcome: AttemptOutcome::Exited { code: 137 },
            actual_runtime: Duration::from_secs(30),
            observed_at: ts(),
        }),
        Command::ReconcileNode(ReconcileNode {
            node,
            node_epoch: 2,
            adopted: vec![attempt],
            lost: vec![LostAttempt {
                attempt: AttemptId(Uuid::from_u128(7)),
                outcome: AttemptOutcome::PullFailed { user_error: false },
                actual_runtime: Duration::ZERO,
            }],
            observed_at: ts(),
        }),
        Command::RegisterNode(RegisterNode {
            node,
            capacity: Resources {
                cpu_millis: 16_000,
                memory: ByteSize::from_gib(64),
                disk: ByteSize::ZERO,
            },
            labels: BTreeMap::from([("zone".into(), "a".into()), ("gpu".into(), "none".into())]),
            registered_at: ts(),
            service_addr: Some("10.0.0.7:9443".into()),
        }),
        Command::DeclareNodeLost(DeclareNodeLost {
            node,
            declared_at: ts(),
        }),
        Command::SetNodeSchedulable(SetNodeSchedulable {
            node,
            schedulable: false,
            updated_at: ts(),
            actor: Some(open_posture_actor()),
        }),
        Command::EvictTerminalJobs(EvictTerminalJobs {
            jobs: vec![jid(1), jid(2)],
            evicted_at: ts(),
        }),
        Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
            entity: QuotaEntityId(Uuid::from_u128(0xE1)),
            parent: Some(QuotaEntityId(Uuid::from_u128(0xEE))),
            name: "team".into(),
            quota: CostUnits(1_000_000_000),
            updated_at: ts(),
            actor: None,
        }),
        Command::UpdatePolicy(UpdatePolicy {
            policy: PolicyConfig {
                priority_multipliers: BTreeMap::from([
                    (-1, PriorityMultiplier::ONE),
                    (3, PriorityMultiplier::from_integer(3)),
                ]),
                ..PolicyConfig::default()
            },
            updated_at: ts(),
            actor: Some(grouped_actor()),
        }),
        Command::UpdateAuthorization(UpdateAuthorization {
            bindings: vec![
                Binding {
                    subject: Subject::Group("platform".into()),
                    role: Role::Submitter,
                    scope: Some(QuotaEntityId(Uuid::from_u128(0xE1))),
                },
                Binding {
                    subject: Subject::Principal("user-42".into()),
                    role: Role::Operator,
                    scope: None,
                },
                Binding {
                    subject: Subject::Group("sre".into()),
                    role: Role::Admin,
                    scope: Some(QuotaEntityId(Uuid::from_u128(0xEE))),
                },
                Binding {
                    subject: Subject::Principal("root@cluster".into()),
                    role: Role::Admin,
                    scope: None,
                },
            ],
            actor: Some(open_posture_actor()),
            updated_at: ts(),
            // The optional claim rename that rides this command — carried so
            // the corpus exercises the `Some` arm of the conversion.
            groups_claim: Some("entitlements".into()),
        }),
        Command::BumpClusterVersion(BumpClusterVersion {
            to: 2,
            bumped_at: ts(),
            actor: None,
        }),
        // ---- Cluster PKI / identity (ADR 0037) ----
        Command::RecordCaCertificate(RecordCaCertificate {
            bundle: {
                // parse DER-validates, so the fixture must be a real CA cert.
                use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
                let key = KeyPair::generate().unwrap();
                let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
                params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
                coppice_state::CaCertBundle::parse(params.self_signed(&key).unwrap().pem()).unwrap()
            },
            staged_root_serial: None,
            recorded_at: ts(),
        }),
        // A staged re-root: the recorded bundle carries the still-active old
        // root at position 0 and the pending new root at position 1
        // (ADR 0037 §4).
        {
            use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
            let old_key = KeyPair::generate().unwrap();
            let mut old_params = CertificateParams::new(Vec::<String>::new()).unwrap();
            old_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let old_pem = old_params.self_signed(&old_key).unwrap().pem();

            let new_key = KeyPair::generate().unwrap();
            let mut new_params = CertificateParams::new(Vec::<String>::new()).unwrap();
            new_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let new_pem = new_params.self_signed(&new_key).unwrap().pem();

            let bundle = coppice_state::CaCertBundle::parse(format!("{old_pem}{new_pem}")).unwrap();
            let staged_serial = bundle.serials()[1].clone();
            Command::RecordCaCertificate(RecordCaCertificate {
                bundle,
                staged_root_serial: Some(staged_serial),
                recorded_at: ts(),
            })
        },
        Command::BindMachineIdentity(BindMachineIdentity {
            machine: MachineId(Uuid::from_u128(0x11)),
            raft_node_id: 3,
            address: "10.0.0.3:7000".into(),
            bound_at: ts(),
        }),
        // Both an expiring and (below) a never-expiring token, and both roles.
        Command::MintEnrollToken(MintEnrollToken {
            token: EnrollTokenId(Uuid::from_u128(0x22)),
            hash: "$argon2id$v=19$m=19456,t=2,p=1$coord".into(),
            role: EnrollRole::Coordinator,
            label: "coord".into(),
            expires_at: Some(ts() + Duration::from_micros(9)),
            minted_at: ts(),
        }),
        Command::MintEnrollToken(MintEnrollToken {
            token: EnrollTokenId(Uuid::from_u128(0x23)),
            hash: "$argon2id$v=19$m=19456,t=2,p=1$agent".into(),
            role: EnrollRole::Agent,
            label: "agent".into(),
            expires_at: None,
            minted_at: ts(),
        }),
        Command::RevokeEnrollToken(RevokeEnrollToken {
            token: EnrollTokenId(Uuid::from_u128(0x22)),
            revoked_at: ts(),
        }),
        // Both RevokedIdentity variants.
        Command::RevokeIdentity(RevokeIdentity {
            identity: RevokedIdentity::Machine(MachineId(Uuid::from_u128(0x24))),
            revoked_at: ts(),
        }),
        Command::RevokeIdentity(RevokeIdentity {
            identity: RevokedIdentity::Node(node),
            revoked_at: ts(),
        }),
        Command::ConfirmKeyPossession(ConfirmKeyPossession {
            raft_node_id: 3,
            confirmed_at: ts(),
        }),
        Command::RecordEnrolledIdentity(RecordEnrolledIdentity {
            machine: MachineId(Uuid::from_u128(0x26)),
            recorded_at: ts(),
        }),
        Command::RebindMachineAddress(RebindMachineAddress {
            raft_node_id: 3,
            address: "10.0.0.9:7000".into(),
            rebound_at: ts(),
        }),
        Command::RetireMachineBinding(RetireMachineBinding {
            machine: MachineId(Uuid::from_u128(0x11)),
            retired_at: ts(),
        }),
        Command::RecordKeyTransferIntent(RecordKeyTransferIntent {
            raft_node_id: 5,
            intended_at: ts(),
        }),
        Command::RecordStagedKeyTransferIntent(RecordStagedKeyTransferIntent {
            raft_node_id: 5,
            root_serial: "ab12cd34".into(),
            intended_at: ts(),
        }),
        Command::ConfirmStagedKeyPossession(ConfirmStagedKeyPossession {
            raft_node_id: 3,
            root_serial: "ab12cd34".into(),
            confirmed_at: ts(),
        }),
    ]
}

#[test]
fn every_command_roundtrips_through_encoded_bytes() {
    for command in every_command() {
        let encoded = command_to_pb(&command, 1).encode_to_vec();
        let decoded =
            pb::command::v1::Command::decode(encoded.as_slice()).expect("envelope must decode");
        let (version, back) = command_from_pb(decoded).expect("conversion must succeed");
        assert_eq!(version, 1);
        assert_eq!(back, command, "roundtrip must be lossless");
    }
}

fn every_job_state() -> Vec<JobState> {
    vec![
        JobState::Submitted,
        JobState::Accepted,
        JobState::Queued,
        JobState::Attempting(AttemptId(Uuid::from_u128(0xA77))),
        JobState::Succeeded,
        JobState::Failed,
        JobState::Aborted,
    ]
}

#[test]
fn every_job_state_roundtrips_through_encoded_bytes() {
    // The oneof carries the attempt id structurally: Attempting must survive
    // with its real id, and every unit variant must survive its empty message.
    for state in every_job_state() {
        let encoded = pb::core::v1::JobState::from(state).encode_to_vec();
        let decoded =
            pb::core::v1::JobState::decode(encoded.as_slice()).expect("JobState must decode");
        let back = JobState::try_from(decoded).expect("conversion must succeed");
        assert_eq!(back, state, "JobState roundtrip must be lossless");
    }
}

#[test]
fn unset_job_state_oneof_is_an_error() {
    // An unset oneof is malformed, exactly like an empty command envelope.
    let empty = pb::core::v1::JobState { state: None };
    assert_eq!(
        JobState::try_from(empty),
        Err(ConvertError::MissingField("JobState.state"))
    );
}

#[test]
fn abort_requests_roundtrip_inside_job_specs() {
    let mut spec = job(1);
    spec.abort_requested = Some(coppice_core::job::AbortRequest {
        reason: None,
        requested_at: ts() + Duration::from_micros(5),
    });
    let submit = Command::SubmitJob(SubmitJob {
        job: spec,
        multiplier: PriorityMultiplier::ONE,
        submitted_at: ts(),
        actor: None,
    });
    let (_, back) = command_from_pb(command_to_pb(&submit, 1)).unwrap();
    assert_eq!(back, submit);
}

#[test]
fn absent_entrypoints_roundtrip_inside_job_specs() {
    // `job()` covers the Some side; None must also survive, distinct from it.
    let mut spec = job(1);
    spec.entrypoint = None;
    let submit = Command::SubmitJob(SubmitJob {
        job: spec,
        multiplier: PriorityMultiplier::ONE,
        submitted_at: ts(),
        actor: None,
    });
    let (_, back) = command_from_pb(command_to_pb(&submit, 1)).unwrap();
    assert_eq!(back, submit);
}

#[test]
fn empty_commands_are_rejected_at_the_boundary() {
    // `command` is required, and an empty repeated field is the wire's only
    // way to omit it — so emptiness is the missing-field error.
    let mut pb_job = pb::core::v1::Job::from(&job(1));
    pb_job.command.clear();
    assert_eq!(
        Job::try_from(pb_job),
        Err(ConvertError::MissingField("Job.command"))
    );
}

#[test]
fn empty_entrypoint_overrides_are_rejected_at_the_boundary() {
    // "No override" is encoded only by absence; a present-but-empty argv is
    // a second spelling of the same meaning and must not decode.
    let mut pb_job = pb::core::v1::Job::from(&job(1));
    pb_job.entrypoint = Some(pb::core::v1::Entrypoint { argv: vec![] });
    assert_eq!(
        Job::try_from(pb_job),
        Err(ConvertError::Invalid {
            field: "Job.entrypoint",
            reason: "override argv must be non-empty",
        })
    );
}

#[test]
fn empty_envelope_is_an_error_not_a_skip() {
    let envelope = pb::command::v1::Command {
        version: 1,
        body: None,
    };
    assert_eq!(
        command_from_pb(envelope),
        Err(ConvertError::MissingField("Command.body"))
    );
}

#[test]
fn ca_bundle_carrying_key_material_is_rejected_at_the_boundary() {
    // The CA private key must never enter replicated state (ADR 0037 §4).
    // The wire field is a plain string, so the conversion boundary is where
    // the CaCertBundle newtype refuses non-certificate payloads — before the
    // command can be proposed into the Raft log.
    // Private-key DER hidden under a CERTIFICATE label: the label is a claim,
    // the DER parse is the fact.
    let relabeled_key = {
        use base64::Engine as _;
        let key_der = rcgen::KeyPair::generate().unwrap().serialize_der();
        let b64 = base64::engine::general_purpose::STANDARD.encode(key_der);
        format!("-----BEGIN CERTIFICATE-----\n{b64}\n-----END CERTIFICATE-----\n")
    };
    let cases = [
        // A private key alone.
        "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n".to_string(),
        // A certificate with the key appended.
        "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n\
         -----BEGIN EC PRIVATE KEY-----\nBBBB\n-----END EC PRIVATE KEY-----\n"
            .to_string(),
        relabeled_key,
        // Empty.
        String::new(),
    ];
    for cert_pem in cases {
        let envelope = pb::command::v1::Command {
            version: 1,
            body: Some(pb::command::v1::command::Body::RecordCaCertificate(
                pb::command::v1::RecordCaCertificate {
                    cert_pem: cert_pem.clone(),
                    staged_root_serial: None,
                    recorded_at_us: ts().as_micros(),
                },
            )),
        };
        assert_eq!(
            command_from_pb(envelope),
            Err(ConvertError::Invalid {
                field: "RecordCaCertificate.cert_pem",
                reason: "not a sequence of X.509 CA certificate PEM blocks",
            }),
            "payload must not decode: {cert_pem:?}"
        );
    }
}

#[test]
fn unspecified_enroll_role_is_rejected_at_the_boundary() {
    // EnrollRole is a closed enum: the zero (UNSPECIFIED) value and any
    // unknown value must fail to decode, never silently default (ADR 0037 §5).
    for role in [0i32, 99] {
        let envelope = pb::command::v1::Command {
            version: 1,
            body: Some(pb::command::v1::command::Body::MintEnrollToken(
                pb::command::v1::MintEnrollToken {
                    token: Some(pb::core::v1::EnrollTokenId {
                        value: EnrollTokenId(Uuid::from_u128(1)).to_string(),
                    }),
                    hash: "h".into(),
                    role,
                    label: "l".into(),
                    expires_at_us: None,
                    minted_at_us: ts().as_micros(),
                },
            )),
        };
        assert_eq!(
            command_from_pb(envelope),
            Err(ConvertError::UnknownEnum {
                field: "MintEnrollToken.role",
                value: role,
            })
        );
    }
}

#[test]
fn unset_revoked_identity_oneof_is_rejected_at_the_boundary() {
    // An unset RevokedIdentity oneof is malformed, exactly like an empty
    // command envelope.
    let envelope = pb::command::v1::Command {
        version: 1,
        body: Some(pb::command::v1::command::Body::RevokeIdentity(
            pb::command::v1::RevokeIdentity {
                identity: Some(pb::core::v1::RevokedIdentity { identity: None }),
                revoked_at_us: ts().as_micros(),
            },
        )),
    };
    assert_eq!(
        command_from_pb(envelope),
        Err(ConvertError::MissingField("RevokedIdentity.identity"))
    );
}

#[test]
fn malformed_ids_are_rejected_at_the_boundary() {
    // A bare uuid without the `attempt-` type tag must not decode, and
    // neither must a well-formed id carrying the wrong tag.
    for value in [
        "1683852a-993f-4497-a48b-6527b458fbd1",
        "job-1683852a-993f-4497-a48b-6527b458fbd1",
        "attempt-not-a-uuid",
    ] {
        let envelope = pb::command::v1::Command {
            version: 1,
            body: Some(pb::command::v1::command::Body::DispatchAttempt(
                pb::command::v1::DispatchAttempt {
                    attempt: Some(pb::core::v1::AttemptId {
                        value: value.to_string(),
                    }),
                    dispatched_at_us: ts().as_micros(),
                },
            )),
        };
        assert_eq!(
            command_from_pb(envelope),
            Err(ConvertError::InvalidId("AttemptId"))
        );
    }
}

#[test]
fn duplicate_resource_kinds_are_rejected() {
    let quantity = pb::core::v1::ResourceQuantity {
        kind: pb::core::v1::ResourceKind::CpuMillis as i32,
        amount: 1,
    };
    let resources = pb::core::v1::Resources {
        quantities: vec![quantity, quantity],
    };
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
        quantities: vec![pb::core::v1::ResourceQuantity {
            kind: 99,
            amount: 1,
        }],
    };
    assert_eq!(
        Resources::try_from(resources),
        Err(ConvertError::UnknownEnum {
            field: "ResourceQuantity.kind",
            value: 99
        })
    );
}

#[test]
fn charge_record_refund_fraction_roundtrips() {
    let record = coppice_core::quota::ChargeRecord {
        amount: CostUnits(42_000),
        charged_at: ts(),
        refund_fraction_milli: 750,
    };
    let encoded: pb::core::v1::ChargeRecord = record.into();
    assert_eq!(encoded.refund_fraction_milli, Some(750));
    let back: coppice_core::quota::ChargeRecord = encoded.try_into().expect("decodes");
    assert_eq!(back, record, "charge-record roundtrip must be lossless");
}

#[test]
fn charge_record_absent_refund_fraction_is_full_refund() {
    // A charge recorded before ADR 0029 carries no fraction; it must true up
    // at the full-refund neutral (1000), preserving pre-0029 behaviour.
    let encoded = pb::core::v1::ChargeRecord {
        amount_ucu: 42_000,
        charged_at_us: ts().as_micros(),
        refund_fraction_milli: None,
    };
    let back: coppice_core::quota::ChargeRecord = encoded.try_into().expect("decodes");
    assert_eq!(back.refund_fraction_milli, 1000);
}

#[test]
fn policy_config_incentive_knobs_roundtrip() {
    let policy = PolicyConfig {
        unbounded_runtime_multiplier: PriorityMultiplier::from_integer(3),
        refund_fraction_milli: 500,
        ..PolicyConfig::default()
    };
    let encoded: pb::core::v1::PolicyConfig = (&policy).into();
    assert_eq!(
        encoded.unbounded_runtime_multiplier_q32_32,
        Some(PriorityMultiplier::from_integer(3).0)
    );
    assert_eq!(encoded.refund_fraction_milli, Some(500));
    let back: PolicyConfig = encoded.try_into().expect("policy must convert");
    assert_eq!(back, policy, "policy roundtrip must be lossless");
}

#[test]
fn policy_config_absent_incentive_knobs_are_neutral() {
    // A PolicyConfig written by a pre-0029 coordinator omits both knobs. They
    // must decode to the neutral values (1.0, 1000) — today's behaviour — and
    // NOT to PolicyConfig::default()'s new knobs (2.0, 750), so an old policy
    // round-trips to the old arithmetic.
    let mut encoded: pb::core::v1::PolicyConfig = (&PolicyConfig::default()).into();
    encoded.unbounded_runtime_multiplier_q32_32 = None;
    encoded.refund_fraction_milli = None;
    let back: PolicyConfig = encoded.try_into().expect("policy must convert");
    assert_eq!(back.unbounded_runtime_multiplier, PriorityMultiplier::ONE);
    assert_eq!(back.refund_fraction_milli, 1000);
}

#[test]
fn absent_submitted_by_roundtrips_inside_job_specs() {
    // `job()` covers the Some side; None must also survive, distinct from an
    // empty string — a job proposed internally, with no actor, carries no
    // submitter identity at all.
    let mut spec = job(1);
    spec.submitted_by = None;
    let submit = Command::SubmitJob(SubmitJob {
        job: spec,
        multiplier: PriorityMultiplier::ONE,
        submitted_at: ts(),
        actor: None,
    });
    let (_, back) = command_from_pb(command_to_pb(&submit, 1)).unwrap();
    assert_eq!(back, submit);
}

#[test]
fn policy_config_groups_claim_roundtrips() {
    // A non-default claim name (ADR 0022's `groups_claim` knob) must survive
    // unchanged, not just fall back to the default.
    let policy = PolicyConfig {
        groups_claim: "cognito:groups".into(),
        ..PolicyConfig::default()
    };
    let encoded: pb::core::v1::PolicyConfig = (&policy).into();
    assert_eq!(encoded.groups_claim, Some("cognito:groups".to_string()));
    let back: PolicyConfig = encoded.try_into().expect("policy must convert");
    assert_eq!(back, policy, "groups_claim roundtrip must be lossless");
}

#[test]
fn policy_config_absent_groups_claim_is_the_documented_default() {
    // A policy written before ADR 0023 omits the field entirely; it must
    // decode to `DEFAULT_GROUPS_CLAIM` ("groups"), not an empty string.
    let mut encoded: pb::core::v1::PolicyConfig = (&PolicyConfig::default()).into();
    encoded.groups_claim = None;
    let back: PolicyConfig = encoded.try_into().expect("policy must convert");
    assert_eq!(back.groups_claim, DEFAULT_GROUPS_CLAIM);
}

#[test]
fn unspecified_role_is_rejected_at_the_boundary() {
    // Role is a closed enum: the zero (UNSPECIFIED) value and any unknown
    // value must fail to decode, never silently default (ADR 0023).
    for role in [0i32, 99] {
        let envelope = pb::command::v1::Command {
            version: 1,
            body: Some(pb::command::v1::command::Body::UpdateAuthorization(
                pb::command::v1::UpdateAuthorization {
                    bindings: vec![pb::core::v1::Binding {
                        subject: Some(pb::core::v1::binding::Subject::Principal(
                            pb::core::v1::binding::Principal { sub: "u".into() },
                        )),
                        role,
                        scope: None,
                    }],
                    actor: None,
                    updated_at_us: ts().as_micros(),
                    groups_claim: None,
                },
            )),
        };
        assert_eq!(
            command_from_pb(envelope),
            Err(ConvertError::UnknownEnum {
                field: "Binding.role",
                value: role,
            })
        );
    }
}

#[test]
fn unset_binding_subject_oneof_is_rejected_at_the_boundary() {
    // An unset Binding.subject oneof names nobody — there is no "everyone"
    // subject, and inventing one would be a silent grant.
    let envelope = pb::command::v1::Command {
        version: 1,
        body: Some(pb::command::v1::command::Body::UpdateAuthorization(
            pb::command::v1::UpdateAuthorization {
                bindings: vec![pb::core::v1::Binding {
                    subject: None,
                    role: pb::core::v1::Role::Operator as i32,
                    scope: None,
                }],
                actor: None,
                updated_at_us: ts().as_micros(),
                groups_claim: None,
            },
        )),
    };
    assert_eq!(
        command_from_pb(envelope),
        Err(ConvertError::MissingField("Binding.subject"))
    );
}

#[test]
fn resources_encode_canonically() {
    // Ascending kind, zeros omitted — byte-identical encodes for equal values.
    let r = Resources {
        cpu_millis: 5,
        memory: ByteSize::ZERO,
        disk: ByteSize::from_bytes(7),
    };
    let encoded = pb::core::v1::Resources::from(&r);
    let kinds: Vec<i32> = encoded.quantities.iter().map(|q| q.kind).collect();
    assert_eq!(
        kinds,
        vec![
            pb::core::v1::ResourceKind::CpuMillis as i32,
            pb::core::v1::ResourceKind::DiskBytes as i32,
        ]
    );
}
