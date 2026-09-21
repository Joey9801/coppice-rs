//! `coppice-client` publishes on its own, with no `coppice-*` dependency, so
//! its wire types (`crates/coppice-client/src/types/*.rs`) are hand-written
//! copies of the server's own DTOs (`coppice_api::http::dto`). Nothing keeps
//! the two in sync except this test: it is the only place in the workspace
//! that can see both crates at once, and it is the only thing standing
//! between a coordinator release and a client that silently drops or
//! misreads a field.
//!
//! Three directions are checked:
//! - a fully populated server *response* value must serialize to the exact
//!   same JSON the client type produces after a round trip through it
//!   (`round_trip`, used for the bulk of `/api/v1`'s read models);
//! - a client-built *request* must decode into the server's (usually
//!   `deny_unknown_fields`) type, and the two must reserialize identically;
//! - the two crates' closed vocabularies (wire enums, the `JobFilter` AST,
//!   id prefixes, metadata limits) must agree variant
//!   for variant, limit for limit, and — where the client copied one — error
//!   string for error string.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;

use coppice_api::http::dto;
use coppice_api::http::ErrorCode as ServerErrorCode;
use coppice_api::Consistency as ServerConsistency;
use coppice_authn::{AuthMethod as ServerAuthMethod, AuthMode as ServerAuthMode};
use coppice_core::id as core_id;
use coppice_core::metadata as core_metadata;
use coppice_core::time::Timestamp as ServerTimestamp;

use coppice_client as client;

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn jid(n: u128) -> core_id::JobId {
    format!("job-00000000-0000-0000-0000-{n:012}")
        .parse()
        .unwrap()
}
fn nid(n: u128) -> core_id::NodeId {
    format!("node-00000000-0000-0000-0000-{n:012}")
        .parse()
        .unwrap()
}
fn aid(n: u128) -> core_id::AllocationId {
    format!("alloc-00000000-0000-0000-0000-{n:012}")
        .parse()
        .unwrap()
}
fn atid(n: u128) -> core_id::AttemptId {
    format!("attempt-00000000-0000-0000-0000-{n:012}")
        .parse()
        .unwrap()
}
fn qid(n: u128) -> core_id::QuotaEntityId {
    format!("quota-00000000-0000-0000-0000-{n:012}")
        .parse()
        .unwrap()
}
fn cid(n: u128) -> core_id::ClusterId {
    format!("cluster-00000000-0000-0000-0000-{n:012}")
        .parse()
        .unwrap()
}

fn ts(us: i64) -> ServerTimestamp {
    ServerTimestamp::from_micros(us).expect("fixture timestamp is in range")
}

fn resources(cpu: u64, mem: u64, disk: u64) -> dto::Resources {
    dto::Resources {
        cpu_millis: cpu,
        memory_bytes: mem,
        disk_bytes: disk,
    }
}

fn metadata(pairs: &[(&str, &str)]) -> core_metadata::JobMetadata {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn by_state(present: &[dto::JobPhase]) -> BTreeMap<dto::JobPhase, u32> {
    dto::JobPhase::ALL
        .iter()
        .map(|p| (*p, u32::from(present.contains(p))))
        .collect()
}

fn attempt_view(
    n: u128,
    state: dto::AttemptState,
    outcome: Option<dto::AttemptOutcome>,
) -> dto::AttemptView {
    dto::AttemptView {
        id: atid(n),
        job: jid(1),
        node: nid(1),
        allocation: aid(n),
        state,
        outcome,
        started_at: Some(ts(1_000_000)),
        ended_at: Some(ts(2_000_000)),
        rate_ucu_per_second: 42,
        charged_ucu: 4200,
    }
}

fn allocation_view(n: u128, state: dto::AllocationState) -> dto::AllocationView {
    dto::AllocationView {
        id: aid(n),
        job: jid(1),
        attempt: atid(n),
        node: nid(1),
        requested: resources(1000, 2_000_000, 3_000_000),
        funded: resources(500, 1_000_000, 1_500_000),
        state,
        seq: 7,
    }
}

fn accrual_view(n: u128) -> dto::AccrualView {
    dto::AccrualView {
        allocation: allocation_view(n, dto::AllocationState::Accruing),
        funded_fraction: dto::FundedFraction {
            cpu: 0.5,
            memory: 0.25,
            disk: 0.75,
        },
        projected_start: Some(ts(3_000_000)),
    }
}

/// Server value -> JSON -> client type -> JSON. The two JSON documents must
/// be identical: that is the whole contract this test exists to hold.
fn round_trip<S, C>(server: &S)
where
    S: Serialize,
    C: DeserializeOwned + Serialize,
{
    let from_server = serde_json::to_value(server).expect("the server value serializes");
    let client: C = serde_json::from_value(from_server.clone()).unwrap_or_else(|e| {
        panic!("the client type failed to decode the server's JSON: {e}\n{from_server:#}")
    });
    let from_client = serde_json::to_value(&client).expect("the client value serializes");
    assert_eq!(from_server, from_client);
}

// ---------------------------------------------------------------------------
// 1. Response round trips
// ---------------------------------------------------------------------------

/// Every job read model — the list row, the timeline, and the detail view
/// with its cost report — must decode into the client's copies and serialize
/// back to the identical JSON document, including every
/// `AttemptOutcomeKind`/`OutcomeClass` combination and the true-up variants.
#[test]
fn job_read_models_round_trip_through_the_client() {
    round_trip::<dto::Resources, client::Resources>(&resources(1000, 2_000_000, 3_000_000));

    let exited = dto::AttemptOutcome {
        kind: dto::AttemptOutcomeKind::Exited,
        exit_code: Some(1),
        class: dto::OutcomeClass::UserError,
    };
    round_trip::<dto::AttemptOutcome, client::AttemptOutcome>(&exited);
    let node_lost = dto::AttemptOutcome {
        kind: dto::AttemptOutcomeKind::NodeLost,
        exit_code: None,
        class: dto::OutcomeClass::Platform,
    };
    round_trip::<dto::AttemptOutcome, client::AttemptOutcome>(&node_lost);

    round_trip::<dto::AttemptView, client::AttemptView>(&attempt_view(
        1,
        dto::AttemptState::Terminal,
        Some(exited),
    ));
    round_trip::<dto::AllocationView, client::AllocationView>(&allocation_view(
        2,
        dto::AllocationState::Active,
    ));
    round_trip::<dto::FundedFraction, client::FundedFraction>(&dto::FundedFraction {
        cpu: 0.5,
        memory: 0.25,
        disk: 0.75,
    });
    round_trip::<dto::AccrualView, client::AccrualView>(&accrual_view(3));

    let retry = dto::RetryPolicy {
        max_retries: 3,
        retry_user_errors: true,
    };
    round_trip::<dto::RetryPolicy, client::RetryPolicy>(&retry);

    let summary = dto::JobSummary {
        id: jid(1),
        state: dto::JobStateKind::Attempting,
        attempt: Some(atid(1)),
        image: "ubuntu:22.04".to_string(),
        quota_entity: qid(1),
        quota_entity_name: "team-a".to_string(),
        priority: 5,
        submitted_at: ts(1_000_000),
        submitted_by: Some("alice@example.com".to_string()),
        terminal_at: Some(ts(9_000_000)),
        node: Some(nid(1)),
        attempt_state: Some(dto::AttemptState::Running),
        funding_fraction: Some(0.5),
        cost_ucu: 12_345,
        outcome: Some(node_lost),
        metadata: metadata(&[("name", "nightly"), ("ticket", "INC-1")]),
    };
    round_trip::<dto::JobSummary, client::JobSummary>(&summary);

    round_trip::<dto::ListJobsResponse, client::ListJobsResponse>(&dto::ListJobsResponse {
        jobs: vec![summary.clone()],
        next_cursor: Some("v1:job-x".to_string()),
    });
    round_trip::<dto::ListJobsResponse, client::ListJobsResponse>(&dto::ListJobsResponse {
        jobs: vec![],
        next_cursor: None,
    });

    let spec = dto::JobSpecView {
        image: "ubuntu:22.04".to_string(),
        command: vec!["sleep".to_string(), "60".to_string()],
        entrypoint: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
        requests: resources(2000, 4_000_000, 0),
        priority: 3,
        max_runtime_seconds: Some(3600),
        quota_entity: qid(1),
        retry,
        submitted_by: Some("alice@example.com".to_string()),
        env: coppice_core::env::JobEnv::from([
            ("EMPTY".to_string(), String::new()),
            ("RUST_LOG".to_string(), "info".to_string()),
        ]),
    };
    round_trip::<dto::JobSpecView, client::JobSpecView>(&spec);

    let abort_requested = dto::AbortRequestedView {
        reason: Some("operator request".to_string()),
        requested_at: ts(5_000_000),
    };
    round_trip::<dto::AbortRequestedView, client::AbortRequestedView>(&abort_requested);

    let penalty_link_finite = dto::PenaltyLink {
        entity: qid(2),
        name: "team-b".to_string(),
        usage_ucu: 100,
        quota_ucu: 200,
        over_quota_ratio: 0.5,
        penalty: 1.0,
    };
    round_trip::<dto::PenaltyLink, client::PenaltyLink>(&penalty_link_finite);

    let explainer = dto::QueuePositionExplainer {
        multiplier: 2.5,
        penalty_chain: vec![penalty_link_finite.clone()],
        penalty_product: 2.5,
        age_seconds: 30,
    };
    round_trip::<dto::QueuePositionExplainer, client::QueuePositionExplainer>(&explainer);

    let breakdown = dto::RateBreakdown {
        cpu: 60,
        memory: 30,
        disk: 10,
    };
    round_trip::<dto::RateBreakdown, client::RateBreakdown>(&breakdown);

    let true_up_refund = dto::TrueUpView {
        kind: dto::TrueUpKind::Refund,
        amount_ucu: 50,
    };
    round_trip::<dto::TrueUpView, client::TrueUpView>(&true_up_refund);
    let true_up_surcharge = dto::TrueUpView {
        kind: dto::TrueUpKind::Surcharge,
        amount_ucu: 75,
    };
    round_trip::<dto::TrueUpView, client::TrueUpView>(&true_up_surcharge);

    let cost = dto::CostReport {
        rate_ucu_per_second: 100,
        rate_breakdown: breakdown,
        priority_multiplier: 1.5,
        unbounded_multiplier: 1.25,
        effective_rate_ucu_per_second: 187,
        charge_window_seconds: 3600,
        charge_window_is_default: false,
        estimated_ucu: 673_200,
        charged_ucu: 673_200,
        refund_fraction: 0.5,
        actual_ucu: Some(336_600),
        true_up: Some(true_up_refund),
    };
    round_trip::<dto::CostReport, client::CostReport>(&cost);

    let detail = dto::JobDetail {
        id: jid(1),
        state: dto::JobStateKind::Attempting,
        spec,
        submitted_at: ts(1_000_000),
        state_since: ts(1_500_000),
        terminal_at: None,
        retries_used: 1,
        abort_requested: Some(abort_requested),
        entity_chain: vec![
            dto::QuotaEntityView {
                id: qid(0),
                name: "root".to_string(),
                parent: None,
                quota_ucu: 1_000_000,
                usage_ucu: 500_000,
                over_quota_ratio: 0.5,
                penalty: 1.0,
            },
            dto::QuotaEntityView {
                id: qid(1),
                name: "team-a".to_string(),
                parent: Some(qid(0)),
                quota_ucu: 0,
                usage_ucu: 1,
                over_quota_ratio: f64::INFINITY,
                penalty: f64::INFINITY,
            },
        ],
        attempts: vec![
            attempt_view(1, dto::AttemptState::Terminal, Some(exited)),
            attempt_view(2, dto::AttemptState::Accruing, None),
        ],
        queue: Some(explainer),
        accrual: Some(accrual_view(3)),
        cost,
        metadata: metadata(&[("name", "nightly")]),
    };
    round_trip::<dto::JobDetail, client::JobDetail>(&detail);

    round_trip::<dto::SubmitJobResponse, client::SubmitJobResponse>(&dto::SubmitJobResponse {
        job: jid(1),
        log_index: 42,
    });
    round_trip::<dto::AbortJobResponse, client::AbortJobResponse>(&dto::AbortJobResponse {});
    round_trip::<dto::ReplaceJobMetadataResponse, client::ReplaceJobMetadataResponse>(
        &dto::ReplaceJobMetadataResponse {
            job: jid(1),
            log_index: 43,
        },
    );
    round_trip::<dto::UpdateJobMetadataResponse, client::UpdateJobMetadataResponse>(
        &dto::UpdateJobMetadataResponse {
            job: jid(1),
            log_index: 44,
        },
    );

    // Job timeline, one event per `TimelineEventBody` variant.
    let bodies = vec![
        dto::TimelineEventBody::JobSubmitted { job: jid(1) },
        dto::TimelineEventBody::JobStateChanged {
            job: jid(1),
            from: dto::JobStateKind::Queued,
            to: dto::JobStateKind::Attempting,
        },
        dto::TimelineEventBody::AttemptStateChanged {
            attempt: atid(1),
            job: jid(1),
            node: nid(1),
            state: dto::AttemptState::Running,
        },
        dto::TimelineEventBody::AllocationFunded {
            allocation: aid(1),
            job: jid(1),
            node: nid(1),
        },
        dto::TimelineEventBody::StopRequested {
            node: nid(1),
            allocation: aid(1),
            job: jid(1),
        },
        dto::TimelineEventBody::NodeEpochBumped {
            node: nid(1),
            epoch: 3,
        },
        dto::TimelineEventBody::JobEvicted { job: jid(1) },
        dto::TimelineEventBody::JobMetadataUpdated { job: jid(1) },
        dto::TimelineEventBody::QuotaEntityConfigured { entity: qid(1) },
        dto::TimelineEventBody::PolicyUpdated,
        dto::TimelineEventBody::AuthorizationUpdated,
        dto::TimelineEventBody::ClusterVersionBumped { to: 7 },
    ];
    let events: Vec<dto::TimelineEvent> = bodies
        .into_iter()
        .enumerate()
        .map(|(i, body)| dto::TimelineEvent {
            index: i as u64 + 1,
            ordinal: 0,
            at: ts(1_000_000 + i as i64),
            body,
        })
        .collect();
    round_trip::<dto::GetJobTimelineResponse, client::GetJobTimelineResponse>(
        &dto::GetJobTimelineResponse {
            events,
            floor_index: 3,
            next_cursor: Some("v1:10:0".to_string()),
        },
    );
    round_trip::<dto::GetJobTimelineResponse, client::GetJobTimelineResponse>(
        &dto::GetJobTimelineResponse {
            events: vec![],
            floor_index: 0,
            next_cursor: None,
        },
    );
}

/// The three frames of the ADR 0043 job-event subscription. They never
/// arrive as a response body — each is one SSE frame's `data:` payload — so
/// this is the only place their JSON is held to the client's copies.
#[test]
fn event_stream_frames_round_trip_through_the_client() {
    round_trip::<dto::EventBatchFrame, client::EventBatchFrame>(&dto::EventBatchFrame {
        index: 91,
        at: ts(1_700_000_000_000_000),
        events: vec![
            dto::TimelineEvent {
                index: 91,
                ordinal: 0,
                at: ts(1_700_000_000_000_000),
                body: dto::TimelineEventBody::JobSubmitted { job: jid(1) },
            },
            // A gap in the ordinals is legitimate: they are positions in the
            // command's *full* batch, and the filter admitted only some.
            dto::TimelineEvent {
                index: 91,
                ordinal: 4,
                at: ts(1_700_000_000_000_000),
                body: dto::TimelineEventBody::JobStateChanged {
                    job: jid(1),
                    from: dto::JobStateKind::Submitted,
                    to: dto::JobStateKind::Queued,
                },
            },
        ],
    });
    // An admitted command whose every event the filter dropped is still a
    // frame: the index is the point of it.
    round_trip::<dto::EventBatchFrame, client::EventBatchFrame>(&dto::EventBatchFrame {
        index: 92,
        at: ts(1_700_000_000_000_001),
        events: vec![],
    });
    round_trip::<dto::EventProgressFrame, client::EventProgressFrame>(&dto::EventProgressFrame {
        index: 93,
    });
    round_trip::<dto::EventGapFrame, client::EventGapFrame>(&dto::EventGapFrame {
        earliest_available: 40,
    });
}

/// A subscription accepts a strictly smaller set of filter leaves than a list
/// does (ADR 0043), and the client refuses the rest before spending a round
/// trip on them. Both sides must draw that line in the same place and say the
/// same thing about it — otherwise the client either refuses a filter the
/// server would have taken, or sends one it will not.
#[test]
fn the_subscribable_filter_leaves_agree_between_the_two_crates() {
    use coppice_api::events::JobSelector;

    let cases: Vec<client::JobFilter> = vec![
        client::JobFilter::metadata_present("team"),
        client::JobFilter::metadata_equals("team", "platform"),
        client::JobFilter::entity(client::QuotaEntityId::new()),
        client::JobFilter::entity_exact(client::QuotaEntityId::new()),
        client::JobFilter::id_in([client::JobId::new()]),
        client::JobFilter::submitted_by("alice@example.com"),
        client::JobFilter::not(client::JobFilter::metadata_present("archived")),
        client::JobFilter::all([
            client::JobFilter::metadata_present("team"),
            client::JobFilter::any([client::JobFilter::submitted_by("alice")]),
        ]),
        client::JobFilter::phase_in([client::JobPhase::Running]),
        client::JobFilter::node(client::NodeId::new()),
        client::JobFilter::image_equals("alpine:3"),
        client::JobFilter::search("needle"),
        client::JobFilter::submitted_after(client::Timestamp::from_micros(1).unwrap()),
        client::JobFilter::requests_min(client::RequestsResource::CpuMillis, 1),
        // The forbidden leaf is nested, so the whole tree is walked on both
        // sides rather than just its root.
        client::JobFilter::all([
            client::JobFilter::metadata_present("team"),
            client::JobFilter::not(client::JobFilter::phase_in([client::JobPhase::Queued])),
        ]),
    ];

    for filter in cases {
        let json = serde_json::to_value(&filter).unwrap();
        let server = parse_server_filter(json.clone()).unwrap();
        let server_verdict = JobSelector::compile(&server)
            .map(|_| ())
            .map_err(|e| e.to_string());
        assert_eq!(
            filter.validate_subscribable(),
            server_verdict,
            "the two crates disagree about {json}"
        );
    }
}

/// The node list/detail views, including the every-field-`Some` host facts
/// and the utilization history, and the drain/undrain/remove empty bodies.
#[test]
fn node_read_models_round_trip_through_the_client() {
    let summary = dto::NodeSummary {
        id: nid(1),
        capacity: resources(4000, 8_000_000_000, 100_000_000_000),
        allocated: resources(1000, 1_000_000, 2_000_000),
        used: Some(resources(2500, 3_000_000_000, 50_000_000_000)),
        labels: BTreeMap::from([("zone".to_string(), "a".to_string())]),
        schedulable: true,
        draining: true,
        health: dto::NodeHealth::Healthy,
        epoch: 3,
        last_heartbeat: Some(ts(4_000_000)),
        running_count: 2,
        accruing_count: 1,
    };
    round_trip::<dto::NodeSummary, client::NodeSummary>(&summary);
    let lost = dto::NodeSummary {
        health: dto::NodeHealth::Lost,
        used: None,
        last_heartbeat: None,
        ..summary.clone()
    };
    round_trip::<dto::NodeSummary, client::NodeSummary>(&lost);
    let unknown = dto::NodeSummary {
        health: dto::NodeHealth::Unknown,
        ..summary.clone()
    };
    round_trip::<dto::NodeSummary, client::NodeSummary>(&unknown);

    round_trip::<dto::ListNodesResponse, client::ListNodesResponse>(&dto::ListNodesResponse {
        nodes: vec![summary.clone()],
    });

    let host = dto::HostFacts {
        os: "linux".to_string(),
        os_version: "Ubuntu 24.04".to_string(),
        kernel_version: "6.8.0".to_string(),
        arch: "x86_64".to_string(),
        cpu_model: "AMD EPYC".to_string(),
        physical_cores: 8,
        logical_cores: 16,
        total_memory_bytes: 64_000_000_000,
        total_disk_bytes: 1_000_000_000_000,
        agent_version: "0.5.1".to_string(),
    };
    round_trip::<dto::HostFacts, client::HostFacts>(&host);

    round_trip::<dto::GetNodeResponse, client::GetNodeResponse>(&dto::GetNodeResponse {
        summary,
        host: Some(host),
        detected_capacity: Some(resources(4000, 8_100_000_000, 100_000_000_000)),
        active_attempts: vec![attempt_view(1, dto::AttemptState::Running, None)],
        accrual_queue: vec![accrual_view(2)],
    });

    let sample = dto::UtilizationSample {
        t: ts(1_000_000),
        allocated: resources(1000, 1_000_000, 0),
        used: Some(resources(500, 500_000, 0)),
    };
    round_trip::<dto::UtilizationSample, client::UtilizationSample>(&sample);
    let gap = dto::UtilizationSample {
        used: None,
        ..sample
    };
    round_trip::<dto::UtilizationSample, client::UtilizationSample>(&gap);

    round_trip::<dto::GetNodeUtilizationResponse, client::GetNodeUtilizationResponse>(
        &dto::GetNodeUtilizationResponse {
            capacity: resources(4000, 8_000_000_000, 0),
            samples: vec![sample, gap],
        },
    );

    round_trip::<dto::DrainNodeResponse, client::DrainNodeResponse>(&dto::DrainNodeResponse {});
    round_trip::<dto::RemoveNodeResponse, client::RemoveNodeResponse>(&dto::RemoveNodeResponse {});
}

/// Quota entity list/detail, including the infinity convention on ratio and
/// penalty and the (always empty, but still-typed) usage history.
#[test]
fn quota_read_models_round_trip_through_the_client() {
    let node = dto::QuotaEntityNode {
        id: qid(1),
        name: "team-a".to_string(),
        parent: Some(qid(0)),
        origin: dto::QuotaEntityOrigin::Configured,
        principal: Some("sub-123".to_string()),
        quota_ucu: 1_000_000,
        usage_ucu: 500_000,
        over_quota_ratio: 0.5,
        penalty: 1.0,
        created_at: ts(1_000_000),
        updated_at: ts(9_000_000),
        queued_count: 3,
        running_count: 2,
    };
    round_trip::<dto::QuotaEntityNode, client::QuotaEntityNode>(&node);
    let sso = dto::QuotaEntityNode {
        origin: dto::QuotaEntityOrigin::Sso,
        ..node.clone()
    };
    round_trip::<dto::QuotaEntityNode, client::QuotaEntityNode>(&sso);

    round_trip::<dto::ListQuotaEntitiesResponse, client::ListQuotaEntitiesResponse>(
        &dto::ListQuotaEntitiesResponse {
            entities: vec![node.clone()],
        },
    );

    let usage_sample = dto::UsageSample {
        t: ts(1_000_000),
        usage_ucu: 100,
    };
    round_trip::<dto::UsageSample, client::UsageSample>(&usage_sample);

    let stats = dto::QuotaEntityStats {
        by_state: by_state(&[dto::JobPhase::Running]),
        oldest_queued_age_seconds: Some(120),
        burn_rate_ucu_per_second: 60,
        charged_ucu_24h: Some(1_000_000),
        usage_history: vec![usage_sample],
    };
    round_trip::<dto::QuotaEntityStats, client::QuotaEntityStats>(&stats);

    let view = dto::QuotaEntityView {
        id: qid(0),
        name: "root".to_string(),
        parent: None,
        quota_ucu: 0,
        usage_ucu: 1,
        over_quota_ratio: f64::INFINITY,
        penalty: f64::INFINITY,
    };
    round_trip::<dto::QuotaEntityView, client::QuotaEntityView>(&view);

    round_trip::<dto::GetQuotaEntityResponse, client::GetQuotaEntityResponse>(
        &dto::GetQuotaEntityResponse {
            entity: node.clone(),
            chain: vec![view],
            children: vec![sso],
            stats,
        },
    );

    round_trip::<dto::ConfigureQuotaEntityResponse, client::ConfigureQuotaEntityResponse>(
        &dto::ConfigureQuotaEntityResponse {
            entity: qid(1),
            log_index: 7,
        },
    );
}

/// The overview and coordinator-status views, including the raft-id decimal
/// strings and every `CoordinatorRole`.
#[test]
fn cluster_read_models_round_trip_through_the_client() {
    let overview = dto::GetClusterOverviewResponse {
        cluster_id: cid(1),
        queue: dto::QueueStats {
            depth: 5,
            accruing: 2,
            drain_rate_per_minute: Some(1.5),
            arrival_rate_per_minute: Some(2.5),
            oldest_queued_age_seconds: Some(600),
            by_state: by_state(&[dto::JobPhase::Queued, dto::JobPhase::Accruing]),
            history: vec![dto::QueueSample {
                t: ts(1_000_000),
                depth: 4,
                drained_per_minute: 1.5,
                arrived_per_minute: 2.5,
            }],
        },
        capacity: dto::ClusterCapacity {
            nodes: dto::NodeCounts {
                total: 3,
                schedulable: 2,
                lost: 1,
            },
            capacity: resources(12_000, 24_000_000_000, 0),
            allocated: resources(3000, 3_000_000, 0),
            used: Some(resources(1500, 1_500_000, 0)),
            reporting_nodes: 2,
            total_nodes: 3,
            history: vec![dto::CapacitySample {
                t: ts(1_000_000),
                capacity: resources(12_000, 24_000_000_000, 0),
                allocated: resources(3000, 3_000_000, 0),
                used: None,
                reporting_nodes: 0,
                total_nodes: 3,
            }],
        },
    };
    round_trip::<dto::GetClusterOverviewResponse, client::GetClusterOverviewResponse>(&overview);

    let snapshot = dto::CoordinatorSnapshot {
        size_bytes: None,
        last_included_index: 100,
        taken_at: None,
        entries_since_snapshot: 5,
    };
    round_trip::<dto::CoordinatorSnapshot, client::CoordinatorSnapshot>(&snapshot);

    let counts = dto::CoordinatorStateCounts {
        jobs: 10,
        attempts: 12,
        allocations: 12,
        nodes: 3,
        quota_entities: 4,
    };
    round_trip::<dto::CoordinatorStateCounts, client::CoordinatorStateCounts>(&counts);

    let leader = dto::CoordinatorMember {
        id: "18446744073709551615".to_string(),
        addr: "10.0.0.1:7070".to_string(),
        role: dto::CoordinatorRole::Leader,
        voter: true,
        last_applied: Some(105),
        replication_lag_entries: None,
    };
    let follower = dto::CoordinatorMember {
        id: "2".to_string(),
        addr: "10.0.0.2:7070".to_string(),
        role: dto::CoordinatorRole::Follower,
        voter: true,
        last_applied: None,
        replication_lag_entries: Some(3),
    };
    let learner = dto::CoordinatorMember {
        id: "3".to_string(),
        addr: "10.0.0.3:7070".to_string(),
        role: dto::CoordinatorRole::Learner,
        voter: false,
        last_applied: None,
        replication_lag_entries: Some(50),
    };
    round_trip::<dto::CoordinatorMember, client::CoordinatorMember>(&leader);

    round_trip::<dto::GetCoordinatorStatusResponse, client::GetCoordinatorStatusResponse>(
        &dto::GetCoordinatorStatusResponse {
            cluster_id: cid(1),
            leader: Some("18446744073709551615".to_string()),
            term: 9,
            known_committed: 105,
            last_applied: 105,
            state_version: 105,
            snapshot: Some(snapshot),
            state_counts: counts,
            members: vec![leader, follower, learner],
        },
    );

    round_trip::<dto::HealthzResponse, client::HealthzResponse>(&dto::HealthzResponse {
        status: "ok",
    });
}

/// Job logs, every `LogAvailability` verdict, and a live-following body.
#[test]
fn log_read_models_round_trip_through_the_client() {
    let entry = dto::LogEntry {
        id: "segment:1".to_string(),
        attempt: atid(1),
        at: ts(1_000_000),
        stream: dto::LogStreamName::Stdout,
        text: "line one\n".to_string(),
        truncated: true,
    };
    round_trip::<dto::LogEntry, client::LogEntry>(&entry);
    let stderr_entry = dto::LogEntry {
        stream: dto::LogStreamName::Stderr,
        truncated: false,
        ..entry.clone()
    };
    round_trip::<dto::LogEntry, client::LogEntry>(&stderr_entry);

    for (availability, reason) in [
        (dto::LogAvailability::Available, None),
        (dto::LogAvailability::Expired, Some("aged out".to_string())),
        (
            dto::LogAvailability::Unreachable,
            Some("dial failed".to_string()),
        ),
        (dto::LogAvailability::NotStarted, None),
    ] {
        let source = dto::LogSourceRecord {
            attempt: atid(2),
            node: Some(nid(1)),
            availability,
            truncated: true,
            earliest_available_at: Some(ts(500_000)),
            reason,
        };
        round_trip::<dto::LogSourceRecord, client::LogSourceRecord>(&source);
    }
    // A source record with no known node (the attempt record itself is
    // missing from replicated state).
    round_trip::<dto::LogSourceRecord, client::LogSourceRecord>(&dto::LogSourceRecord {
        attempt: atid(3),
        node: None,
        availability: dto::LogAvailability::Unreachable,
        truncated: false,
        earliest_available_at: None,
        reason: Some("node decommissioned".to_string()),
    });

    round_trip::<dto::GetJobLogsResponse, client::GetJobLogsResponse>(&dto::GetJobLogsResponse {
        resume_cursor: Some("v1:desc:attempt-x:100:0".to_string()),
        live: true,
        entries: vec![entry, stderr_entry],
        sources: vec![dto::LogSourceRecord {
            attempt: atid(1),
            node: Some(nid(1)),
            availability: dto::LogAvailability::Available,
            truncated: false,
            earliest_available_at: None,
            reason: None,
        }],
        next_cursor: Some("v1:desc:attempt-x:50:0".to_string()),
    });
    round_trip::<dto::GetJobLogsResponse, client::GetJobLogsResponse>(&dto::GetJobLogsResponse {
        resume_cursor: None,
        live: false,
        entries: vec![],
        sources: vec![],
        next_cursor: None,
    });
}

/// Job usage samples, every `UsageAvailability` verdict.
#[test]
fn usage_read_models_round_trip_through_the_client() {
    let point = dto::UsagePoint {
        attempt: atid(1),
        at: ts(1_000_000),
        cpu_usage_total_us: 1_500_000,
        cpu_throttled_total_us: 5_000,
        memory_used_bytes: 2_000_000,
        memory_peak_bytes: 3_000_000,
        disk_writable_bytes: 100_000,
        disk_image_bytes: 500_000_000,
        net_rx_bytes_total: 1_000,
        net_tx_bytes_total: 2_000,
        blkio_read_bytes_total: 3_000,
        blkio_write_bytes_total: 4_000,
    };
    round_trip::<dto::UsagePoint, client::UsagePoint>(&point);

    for (availability, reason) in [
        (dto::UsageAvailability::Available, None),
        (
            dto::UsageAvailability::Expired,
            Some("aged out".to_string()),
        ),
        (
            dto::UsageAvailability::Unreachable,
            Some("dial failed".to_string()),
        ),
        (dto::UsageAvailability::NotStarted, None),
    ] {
        let source = dto::UsageSourceRecord {
            attempt: atid(2),
            node: Some(nid(1)),
            availability,
            truncated: true,
            earliest_available_at: Some(ts(500_000)),
            reason,
        };
        round_trip::<dto::UsageSourceRecord, client::UsageSourceRecord>(&source);
    }

    round_trip::<dto::GetJobUsageResponse, client::GetJobUsageResponse>(
        &dto::GetJobUsageResponse {
            samples: vec![point],
            sources: vec![dto::UsageSourceRecord {
                attempt: atid(1),
                node: None,
                availability: dto::UsageAvailability::NotStarted,
                truncated: false,
                earliest_available_at: None,
                reason: None,
            }],
            next_cursor: Some("v1:asc:attempt-x:100:0".to_string()),
        },
    );
}

/// Auth config (both open and OIDC shapes — open must round trip with the
/// three OIDC keys *absent*, not null), the session view, and the
/// authorization policy including every `BindingRole`.
#[test]
fn auth_read_models_round_trip_through_the_client() {
    let open = dto::GetAuthConfigResponse {
        mode: "open".to_string(),
        issuer: None,
        client_id: None,
        audience: None,
    };
    let open_json = serde_json::to_value(&open).unwrap();
    assert_eq!(open_json, json!({ "mode": "open" }));
    round_trip::<dto::GetAuthConfigResponse, client::GetAuthConfigResponse>(&open);

    let oidc = dto::GetAuthConfigResponse {
        mode: "oidc".to_string(),
        issuer: Some("https://idp.example".to_string()),
        client_id: Some("coppice".to_string()),
        audience: Some("coppice-api".to_string()),
    };
    round_trip::<dto::GetAuthConfigResponse, client::GetAuthConfigResponse>(&oidc);

    let session = dto::GetSessionResponse {
        principal: "sub-123".to_string(),
        groups: vec!["platform".to_string(), "sre".to_string()],
        auth_method: "bearer".to_string(),
        name: Some("Alice".to_string()),
        email: Some("alice@example.com".to_string()),
        bindings: vec![
            dto::SessionBinding {
                role: dto::BindingRole::Submitter,
                scope: Some(qid(1)),
            },
            dto::SessionBinding {
                role: dto::BindingRole::Operator,
                scope: None,
            },
            dto::SessionBinding {
                role: dto::BindingRole::Admin,
                scope: None,
            },
        ],
        implicit_admin: true,
    };
    round_trip::<dto::GetSessionResponse, client::GetSessionResponse>(&session);

    let bindings = vec![
        dto::BindingDto {
            group: Some("platform-team".to_string()),
            principal: None,
            role: dto::BindingRole::Operator,
            scope: Some(qid(1)),
        },
        dto::BindingDto {
            group: None,
            principal: Some("sub-456".to_string()),
            role: dto::BindingRole::Admin,
            scope: None,
        },
    ];
    round_trip::<dto::GetAuthorizationResponse, client::GetAuthorizationResponse>(
        &dto::GetAuthorizationResponse {
            groups_claim: "groups".to_string(),
            bindings,
        },
    );

    round_trip::<dto::UpdateAuthorizationResponse, client::UpdateAuthorizationResponse>(
        &dto::UpdateAuthorizationResponse { log_index: 99 },
    );
}

/// The infinity convention: `f64::INFINITY` serializes as `null` on the
/// server and the client must read that `null` back as infinity, on every
/// field that carries it.
#[test]
fn infinite_quota_figures_round_trip_as_null_both_ways() {
    let node = dto::QuotaEntityNode {
        id: qid(1),
        name: "root".to_string(),
        parent: None,
        origin: dto::QuotaEntityOrigin::Configured,
        principal: None,
        quota_ucu: 0,
        usage_ucu: 1,
        over_quota_ratio: f64::INFINITY,
        penalty: f64::INFINITY,
        created_at: ts(1_000_000),
        updated_at: ts(2_000_000),
        queued_count: 1,
        running_count: 0,
    };
    let json = serde_json::to_value(&node).unwrap();
    assert_eq!(json["over_quota_ratio"], serde_json::Value::Null);
    assert_eq!(json["penalty"], serde_json::Value::Null);
    let client_node: client::QuotaEntityNode = serde_json::from_value(json).unwrap();
    assert_eq!(client_node.over_quota_ratio, f64::INFINITY);
    assert_eq!(client_node.penalty, f64::INFINITY);

    let view = dto::QuotaEntityView {
        id: qid(1),
        name: "root".to_string(),
        parent: None,
        quota_ucu: 0,
        usage_ucu: 1,
        over_quota_ratio: f64::INFINITY,
        penalty: f64::INFINITY,
    };
    let json = serde_json::to_value(&view).unwrap();
    let client_view: client::QuotaEntityView = serde_json::from_value(json).unwrap();
    assert_eq!(client_view.over_quota_ratio, f64::INFINITY);
    assert_eq!(client_view.penalty, f64::INFINITY);

    let link = dto::PenaltyLink {
        entity: qid(1),
        name: "root".to_string(),
        usage_ucu: 1,
        quota_ucu: 0,
        over_quota_ratio: f64::INFINITY,
        penalty: f64::INFINITY,
    };
    let json = serde_json::to_value(&link).unwrap();
    let client_link: client::PenaltyLink = serde_json::from_value(json).unwrap();
    assert_eq!(client_link.over_quota_ratio, f64::INFINITY);
    assert_eq!(client_link.penalty, f64::INFINITY);

    let explainer = dto::QueuePositionExplainer {
        multiplier: 1.0,
        penalty_chain: vec![link],
        penalty_product: f64::INFINITY,
        age_seconds: 10,
    };
    let json = serde_json::to_value(&explainer).unwrap();
    assert_eq!(json["penalty_product"], serde_json::Value::Null);
    let client_explainer: client::QueuePositionExplainer = serde_json::from_value(json).unwrap();
    assert_eq!(client_explainer.penalty_product, f64::INFINITY);
}

// ---------------------------------------------------------------------------
// 2. Request round trips: client-built request -> JSON -> server type
// ---------------------------------------------------------------------------

/// A client request must decode into the server's (`deny_unknown_fields`)
/// type, catching a stray or misspelled key, and the two must reserialize to
/// identical JSON.
fn request_round_trip<C, S>(client_value: &C)
where
    C: Serialize,
    S: DeserializeOwned + Serialize,
{
    let from_client = serde_json::to_value(client_value).expect("the client value serializes");
    let server: S = serde_json::from_value(from_client.clone()).unwrap_or_else(|e| {
        panic!("the server type failed to decode the client's JSON: {e}\n{from_client:#}")
    });
    let from_server = serde_json::to_value(&server).expect("the server value serializes");
    assert_eq!(from_client, from_server);
}

#[test]
fn submit_job_request_decodes_into_the_server_type() {
    let job = client::JobId::new();
    let entity = client::QuotaEntityId::new();
    let full = client::SubmitJobRequest::new(
        job,
        "ubuntu:22.04",
        ["sleep", "60"],
        client::Resources::new(1000, 512 * 1024 * 1024, 0),
        entity,
    )
    .with_entrypoint(["/bin/sh", "-c"])
    .with_priority(2)
    .with_max_runtime(Duration::from_secs(600))
    .with_retry(client::RetryPolicy::retrying_user_errors(3))
    .with_metadata(client::JobMetadata::from_iter([
        ("name", "nightly"),
        ("owner", "platform"),
    ]))
    .with_env(client::JobEnv::from_iter([
        ("RUST_LOG", "info"),
        ("EMPTY", ""),
    ]));
    assert!(full.validate().is_ok());
    request_round_trip::<client::SubmitJobRequest, dto::SubmitJobRequest>(&full);

    let minimal = client::SubmitJobRequest::new(
        client::JobId::new(),
        "alpine",
        ["true"],
        client::Resources::default(),
        client::QuotaEntityId::new(),
    );
    assert!(minimal.validate().is_ok());
    request_round_trip::<client::SubmitJobRequest, dto::SubmitJobRequest>(&minimal);
}

#[test]
fn abort_job_request_decodes_into_the_server_type() {
    request_round_trip::<client::AbortJobRequest, dto::AbortJobRequest>(
        &client::AbortJobRequest::new(),
    );
    request_round_trip::<client::AbortJobRequest, dto::AbortJobRequest>(
        &client::AbortJobRequest::new().with_reason("operator request"),
    );
    let job = client::JobId::new();
    request_round_trip::<client::AbortJobRequest, dto::AbortJobRequest>(
        &client::AbortJobRequest::new()
            .with_job(job)
            .with_reason("stale"),
    );
}

#[test]
fn job_metadata_write_requests_decode_into_the_server_types() {
    let metadata = client::JobMetadata::from_iter([("name", "nightly"), ("ticket", "INC-1")]);
    request_round_trip::<client::ReplaceJobMetadataRequest, dto::ReplaceJobMetadataRequest>(
        &client::ReplaceJobMetadataRequest::new(metadata),
    );
    request_round_trip::<client::ReplaceJobMetadataRequest, dto::ReplaceJobMetadataRequest>(
        &client::ReplaceJobMetadataRequest::new(client::JobMetadata::new()),
    );

    let patch = client::UpdateJobMetadataRequest::new()
        .set("name", "nightly")
        .unwrap()
        .set("owner", "platform")
        .unwrap()
        .unset("stale-key")
        .unset("another-stale-key");
    assert!(patch.validate().is_ok());
    request_round_trip::<client::UpdateJobMetadataRequest, dto::UpdateJobMetadataRequest>(&patch);
}

#[test]
fn configure_quota_entity_request_decodes_into_the_server_type() {
    let entity = client::QuotaEntityId::new();
    let rooted = client::ConfigureQuotaEntityRequest::new(entity, "team-a", 1_000_000);
    request_round_trip::<client::ConfigureQuotaEntityRequest, dto::ConfigureQuotaEntityRequest>(
        &rooted,
    );

    let parent = client::QuotaEntityId::new();
    let scoped =
        client::ConfigureQuotaEntityRequest::new(entity, "team-a", 1_000_000).with_parent(parent);
    request_round_trip::<client::ConfigureQuotaEntityRequest, dto::ConfigureQuotaEntityRequest>(
        &scoped,
    );
}

#[test]
fn update_authorization_request_decodes_into_the_server_type() {
    let scope = client::QuotaEntityId::new();
    let bindings = vec![
        client::Binding::for_group("platform-team", client::BindingRole::Submitter),
        client::Binding::for_group("sre", client::BindingRole::Operator).with_scope(scope),
        client::Binding::for_principal("sub-123", client::BindingRole::Admin),
        client::Binding::for_principal("sub-456", client::BindingRole::Submitter).with_scope(scope),
    ];
    for binding in &bindings {
        assert!(binding.validate().is_ok());
    }
    let request = client::UpdateAuthorizationRequest::new(bindings).with_groups_claim("groups");
    assert!(request.validate().is_ok());
    request_round_trip::<client::UpdateAuthorizationRequest, dto::UpdateAuthorizationRequest>(
        &request,
    );

    // No groups_claim rename.
    let request = client::UpdateAuthorizationRequest::new([client::Binding::for_group(
        "everyone",
        client::BindingRole::Submitter,
    )]);
    request_round_trip::<client::UpdateAuthorizationRequest, dto::UpdateAuthorizationRequest>(
        &request,
    );
}

// ---------------------------------------------------------------------------
// 3. `JobFilter` contract
// ---------------------------------------------------------------------------

fn parse_server_filter(json: serde_json::Value) -> Result<dto::JobFilter, String> {
    serde_json::from_value(json).map_err(|e| e.to_string())
}

/// Every `JobFilter` leaf built through the client's constructors must
/// deserialize as the server's (`deny_unknown_fields`-on-every-leaf) type,
/// validate on both sides, and match a hand-written JSON literal exactly —
/// the same literals `dto.rs`'s own contract test spells.
#[test]
fn every_job_filter_leaf_matches_the_wire_contract() {
    let job = client::JobId::new();
    let entity = client::QuotaEntityId::new();
    let node = client::NodeId::new();
    let after = client::Timestamp::from_micros(1_700_000_000_000_000).unwrap();
    let before = client::Timestamp::from_micros(1_700_000_100_000_000).unwrap();

    let cases: Vec<(client::JobFilter, serde_json::Value)> = vec![
        (
            client::JobFilter::all([client::JobFilter::phase_in([client::JobPhase::Queued])]),
            json!({"all": [{"phase": {"in": ["queued"]}}]}),
        ),
        (
            client::JobFilter::any([client::JobFilter::search("x")]),
            json!({"any": [{"search": "x"}]}),
        ),
        (
            client::JobFilter::not(client::JobFilter::search("x")),
            json!({"not": {"search": "x"}}),
        ),
        (
            client::JobFilter::phase_in([client::JobPhase::Queued, client::JobPhase::Running]),
            json!({"phase": {"in": ["queued", "running"]}}),
        ),
        (
            client::JobFilter::entity(entity),
            json!({"entity": {"id": entity.to_string(), "scope": "subtree"}}),
        ),
        (
            client::JobFilter::entity_exact(entity),
            json!({"entity": {"id": entity.to_string(), "scope": "exact"}}),
        ),
        (
            client::JobFilter::node(node),
            json!({"node": node.to_string()}),
        ),
        (
            client::JobFilter::image_contains("alpine"),
            json!({"image": {"contains": "alpine"}}),
        ),
        (
            client::JobFilter::image_equals("alpine:3"),
            json!({"image": {"equals": "alpine:3"}}),
        ),
        (
            client::JobFilter::id_in([job]),
            json!({"id": {"in": [job.to_string()]}}),
        ),
        (
            client::JobFilter::search("needle"),
            json!({"search": "needle"}),
        ),
        (
            client::JobFilter::submitted_after(after),
            json!({"submitted": {"after": after.to_rfc3339()}}),
        ),
        (
            client::JobFilter::submitted_before(before),
            json!({"submitted": {"before": before.to_rfc3339()}}),
        ),
        (
            client::JobFilter::submitted_between(after, before),
            json!({"submitted": {"after": after.to_rfc3339(), "before": before.to_rfc3339()}}),
        ),
        (
            client::JobFilter::submitted_by("alice@example.com"),
            json!({"submitted_by": "alice@example.com"}),
        ),
        (
            client::JobFilter::requests_min(client::RequestsResource::CpuMillis, 1000),
            json!({"requests": {"resource": "cpu_millis", "min": 1000}}),
        ),
        (
            client::JobFilter::requests_max(client::RequestsResource::MemoryBytes, 2000),
            json!({"requests": {"resource": "memory_bytes", "max": 2000}}),
        ),
        (
            client::JobFilter::requests_between(client::RequestsResource::DiskBytes, 1, 2),
            json!({"requests": {"resource": "disk_bytes", "min": 1, "max": 2}}),
        ),
        (
            client::JobFilter::metadata_present("name"),
            json!({"metadata": {"key": "name"}}),
        ),
        (
            client::JobFilter::metadata_equals("name", "nightly"),
            json!({"metadata": {"key": "name", "equals": "nightly"}}),
        ),
    ];

    for (filter, expected) in cases {
        let client_json = serde_json::to_value(&filter).unwrap();
        assert_eq!(client_json, expected, "leaf JSON mismatch for {filter:?}");
        assert!(
            filter.validate().is_ok(),
            "client validate failed for {filter:?}"
        );

        let server_filter = parse_server_filter(client_json.clone())
            .unwrap_or_else(|e| panic!("server failed to decode {client_json}: {e}"));
        assert!(
            server_filter.validate().is_ok(),
            "server validate failed for {client_json}"
        );
    }
}

/// The two crates' filter-tree caps must be identical.
#[test]
fn filter_caps_match_between_the_two_crates() {
    assert_eq!(client::MAX_FILTER_DEPTH, dto::MAX_FILTER_DEPTH);
    assert_eq!(client::MAX_FILTER_NODES, dto::MAX_FILTER_NODES);
}

/// Every shape-rule refusal — empty lists, missing bounds, ordered bounds,
/// an unstorable metadata key, the depth cap, and the node cap — must be
/// refused by both crates with byte-identical error text.
#[test]
fn filter_validation_refusals_agree_between_the_two_crates() {
    // Empty `all`/`any`, reachable via the client's own constructors.
    let empty_all = client::JobFilter::all(std::iter::empty());
    let json = serde_json::to_value(&empty_all).unwrap();
    let server = parse_server_filter(json).unwrap();
    assert_eq!(empty_all.validate(), server.validate());
    assert_eq!(
        empty_all.validate(),
        Err("`all` filter list must be non-empty".to_string())
    );

    let empty_any = client::JobFilter::any(std::iter::empty());
    let json = serde_json::to_value(&empty_any).unwrap();
    let server = parse_server_filter(json).unwrap();
    assert_eq!(empty_any.validate(), server.validate());

    // Empty `phase.in`/`id.in`.
    let empty_phase = client::JobFilter::phase_in(std::iter::empty());
    let json = serde_json::to_value(&empty_phase).unwrap();
    let server = parse_server_filter(json).unwrap();
    assert_eq!(empty_phase.validate(), server.validate());
    assert_eq!(
        empty_phase.validate(),
        Err("`phase.in` must be non-empty".to_string())
    );

    let empty_id = client::JobFilter::id_in(std::iter::empty());
    let json = serde_json::to_value(&empty_id).unwrap();
    let server = parse_server_filter(json).unwrap();
    assert_eq!(empty_id.validate(), server.validate());

    // `submitted` with no bounds — no client constructor reaches this, so
    // both sides are built by deserializing the same JSON.
    let json = json!({"submitted": {}});
    let client_filter: client::JobFilter = serde_json::from_value(json.clone()).unwrap();
    let server_filter = parse_server_filter(json).unwrap();
    assert_eq!(client_filter.validate(), server_filter.validate());
    assert_eq!(
        client_filter.validate(),
        Err("`submitted` requires at least one of `after`/`before`".to_string())
    );

    // `submitted.after > before`.
    let after = client::Timestamp::from_micros(2).unwrap();
    let before = client::Timestamp::from_micros(1).unwrap();
    let reversed = client::JobFilter::submitted_between(after, before);
    let json = serde_json::to_value(&reversed).unwrap();
    let server = parse_server_filter(json).unwrap();
    assert_eq!(reversed.validate(), server.validate());
    assert_eq!(
        reversed.validate(),
        Err("`submitted.after` must not be later than `submitted.before`".to_string())
    );

    // `requests` with no bound.
    let json = json!({"requests": {"resource": "cpu_millis"}});
    let client_filter: client::JobFilter = serde_json::from_value(json.clone()).unwrap();
    let server_filter = parse_server_filter(json).unwrap();
    assert_eq!(client_filter.validate(), server_filter.validate());
    assert_eq!(
        client_filter.validate(),
        Err("`requests` requires at least one of `min`/`max`".to_string())
    );

    // `requests.min > max`.
    let reversed = client::JobFilter::requests_between(client::RequestsResource::CpuMillis, 5, 1);
    let json = serde_json::to_value(&reversed).unwrap();
    let server = parse_server_filter(json).unwrap();
    assert_eq!(reversed.validate(), server.validate());
    assert_eq!(
        reversed.validate(),
        Err("`requests.min` must not exceed `requests.max`".to_string())
    );

    // An unstorable metadata key.
    let bad_key = client::JobFilter::metadata_present("has space");
    let json = serde_json::to_value(&bad_key).unwrap();
    let server = parse_server_filter(json).unwrap();
    assert_eq!(bad_key.validate(), server.validate());
    let err = bad_key.validate().unwrap_err();
    assert!(err.contains("`metadata.key` is invalid"), "{err}");
    assert!(err.contains("outside the allowed set"), "{err}");

    // The depth cap: 9 nested `not`s exceeds MAX_FILTER_DEPTH (8).
    let mut nested = client::JobFilter::search("leaf");
    for _ in 0..9 {
        nested = client::JobFilter::not(nested);
    }
    let json = serde_json::to_value(&nested).unwrap();
    let server = parse_server_filter(json).unwrap();
    assert_eq!(nested.validate(), server.validate());
    assert!(nested.validate().is_err());

    // The node cap: 65 leaves exceeds MAX_FILTER_NODES (64).
    let over_cap = client::JobFilter::all((0..65).map(|_| client::JobFilter::search("x")));
    let json = serde_json::to_value(&over_cap).unwrap();
    let server = parse_server_filter(json).unwrap();
    assert_eq!(over_cap.validate(), server.validate());
    assert_eq!(
        over_cap.validate(),
        Err(format!(
            "filter exceeds the maximum of {} nodes",
            dto::MAX_FILTER_NODES
        ))
    );
}

// ---------------------------------------------------------------------------
// 4. Enum vocabulary checks
// ---------------------------------------------------------------------------

/// Every server variant of a forward-compatible client enum (one with an
/// `Unknown(String)` catch-all — see `coppice_client::types`) must decode to
/// a *known* client value (never the `Unknown` catch-all), and re-serialize
/// to the identical wire string.
macro_rules! assert_known_variants {
    ($client_ty:ty, [$($server_value:expr),+ $(,)?]) => {
        $(
            let json = serde_json::to_value($server_value).unwrap();
            let value: $client_ty = serde_json::from_value(json.clone()).unwrap();
            assert!(!value.is_unknown(), "{json} decoded as Unknown for {}", stringify!($client_ty));
            assert_eq!(serde_json::to_value(&value).unwrap(), json);
        )+
    };
}

#[test]
fn every_server_wire_enum_variant_is_known_to_the_client() {
    assert_known_variants!(
        client::NodeHealth,
        [
            dto::NodeHealth::Unknown,
            dto::NodeHealth::Healthy,
            dto::NodeHealth::Lost
        ]
    );
    assert_known_variants!(
        client::AttemptState,
        [
            dto::AttemptState::Accruing,
            dto::AttemptState::Ready,
            dto::AttemptState::Dispatching,
            dto::AttemptState::Running,
            dto::AttemptState::Finalizing,
            dto::AttemptState::Terminal,
        ]
    );
    assert_known_variants!(
        client::AttemptOutcomeKind,
        [
            dto::AttemptOutcomeKind::Exited,
            dto::AttemptOutcomeKind::MemoryLimitExceeded,
            dto::AttemptOutcomeKind::RuntimeLimitExceeded,
            dto::AttemptOutcomeKind::DiskLimitExceeded,
            dto::AttemptOutcomeKind::Aborted,
            dto::AttemptOutcomeKind::Revoked,
            dto::AttemptOutcomeKind::PullFailed,
            dto::AttemptOutcomeKind::StartFailed,
            dto::AttemptOutcomeKind::NodeLost,
            dto::AttemptOutcomeKind::AgentError,
        ]
    );
    assert_known_variants!(
        client::OutcomeClass,
        [
            dto::OutcomeClass::Success,
            dto::OutcomeClass::UserError,
            dto::OutcomeClass::UserRequest,
            dto::OutcomeClass::Platform,
        ]
    );
    assert_known_variants!(
        client::AllocationState,
        [
            dto::AllocationState::Accruing,
            dto::AllocationState::Funded,
            dto::AllocationState::Active,
            dto::AllocationState::Released,
        ]
    );
    for phase in dto::JobPhase::ALL {
        let json = serde_json::to_value(phase).unwrap();
        let value: client::JobPhase = serde_json::from_value(json.clone()).unwrap();
        assert!(
            !value.is_unknown(),
            "{json} decoded as Unknown for JobPhase"
        );
        assert_eq!(serde_json::to_value(&value).unwrap(), json);
    }
    assert_known_variants!(
        client::JobStateKind,
        [
            dto::JobStateKind::Submitted,
            dto::JobStateKind::Accepted,
            dto::JobStateKind::Queued,
            dto::JobStateKind::Attempting,
            dto::JobStateKind::Succeeded,
            dto::JobStateKind::Failed,
            dto::JobStateKind::Aborted,
        ]
    );
    assert_known_variants!(
        client::QuotaEntityOrigin,
        [
            dto::QuotaEntityOrigin::Configured,
            dto::QuotaEntityOrigin::Sso
        ]
    );
    assert_known_variants!(
        client::CoordinatorRole,
        [
            dto::CoordinatorRole::Leader,
            dto::CoordinatorRole::Follower,
            dto::CoordinatorRole::Learner,
        ]
    );
    assert_known_variants!(
        client::TrueUpKind,
        [dto::TrueUpKind::Refund, dto::TrueUpKind::Surcharge]
    );
    assert_known_variants!(
        client::LogStreamName,
        [dto::LogStreamName::Stdout, dto::LogStreamName::Stderr]
    );
    assert_known_variants!(
        client::LogAvailability,
        [
            dto::LogAvailability::Available,
            dto::LogAvailability::Expired,
            dto::LogAvailability::Unreachable,
            dto::LogAvailability::NotStarted,
        ]
    );
    assert_known_variants!(
        client::UsageAvailability,
        [
            dto::UsageAvailability::Available,
            dto::UsageAvailability::Expired,
            dto::UsageAvailability::Unreachable,
            dto::UsageAvailability::NotStarted,
        ]
    );
    assert_known_variants!(
        client::BindingRole,
        [
            dto::BindingRole::Submitter,
            dto::BindingRole::Operator,
            dto::BindingRole::Admin,
        ]
    );
}

/// `LogOrder`, `EntityScope`, and `RequestsResource` are client-authored
/// vocabularies: the server never echoes one back, so there is no `Unknown`
/// catch-all on either side (and the server's `EntityScope`/`RequestsResource`
/// are `Deserialize`-only). Every spelling still has to agree.
#[test]
fn closed_client_authored_enums_agree_on_every_spelling() {
    for wire in ["asc", "desc"] {
        let server: dto::LogOrder = serde_json::from_value(json!(wire)).unwrap();
        assert_eq!(serde_json::to_value(server).unwrap(), json!(wire));
        let client: client::LogOrder = serde_json::from_value(json!(wire)).unwrap();
        assert_eq!(serde_json::to_value(client).unwrap(), json!(wire));
    }
    for wire in ["subtree", "exact"] {
        let _: dto::EntityScope = serde_json::from_value(json!(wire)).unwrap();
        let client: client::EntityScope = serde_json::from_value(json!(wire)).unwrap();
        assert_eq!(serde_json::to_value(client).unwrap(), json!(wire));
    }
    for wire in ["cpu_millis", "memory_bytes", "disk_bytes"] {
        let _: dto::RequestsResource = serde_json::from_value(json!(wire)).unwrap();
        let client: client::RequestsResource = serde_json::from_value(json!(wire)).unwrap();
        assert_eq!(serde_json::to_value(client).unwrap(), json!(wire));
    }
}

/// Every `TimelineEventBody` variant's `kind` tag must decode to a known
/// (non-`Unknown`) client variant.
#[test]
fn every_timeline_event_kind_is_known_to_the_client() {
    let bodies = vec![
        dto::TimelineEventBody::JobSubmitted { job: jid(1) },
        dto::TimelineEventBody::JobStateChanged {
            job: jid(1),
            from: dto::JobStateKind::Queued,
            to: dto::JobStateKind::Attempting,
        },
        dto::TimelineEventBody::AttemptStateChanged {
            attempt: atid(1),
            job: jid(1),
            node: nid(1),
            state: dto::AttemptState::Running,
        },
        dto::TimelineEventBody::AllocationFunded {
            allocation: aid(1),
            job: jid(1),
            node: nid(1),
        },
        dto::TimelineEventBody::StopRequested {
            node: nid(1),
            allocation: aid(1),
            job: jid(1),
        },
        dto::TimelineEventBody::NodeEpochBumped {
            node: nid(1),
            epoch: 1,
        },
        dto::TimelineEventBody::JobEvicted { job: jid(1) },
        dto::TimelineEventBody::JobMetadataUpdated { job: jid(1) },
        dto::TimelineEventBody::QuotaEntityConfigured { entity: qid(1) },
        dto::TimelineEventBody::PolicyUpdated,
        dto::TimelineEventBody::AuthorizationUpdated,
        dto::TimelineEventBody::ClusterVersionBumped { to: 1 },
    ];
    for body in bodies {
        let json = serde_json::to_value(&body).unwrap();
        let event_json = json!({
            "index": 1,
            "ordinal": 0,
            "at": "2026-01-01T00:00:00.000000Z",
        });
        let mut full = event_json.as_object().unwrap().clone();
        full.extend(json.as_object().unwrap().clone());
        let client_event: client::TimelineEvent =
            serde_json::from_value(serde_json::Value::Object(full)).unwrap();
        assert_ne!(
            client_event.body,
            client::TimelineEventBody::Unknown,
            "kind {:?} decoded as Unknown",
            json["kind"]
        );
    }
}

/// Every server `ErrorCode` variant must be known to the client (never
/// `Other`), with an identical wire spelling. New server variant? Add it to
/// `SERVER_ERROR_CODES` below too.
#[test]
fn every_server_error_code_is_known_to_the_client() {
    const SERVER_ERROR_CODES: &[ServerErrorCode] = &[
        ServerErrorCode::InvalidArgument,
        ServerErrorCode::Unauthenticated,
        ServerErrorCode::PermissionDenied,
        ServerErrorCode::NotFound,
        ServerErrorCode::Rejected,
        ServerErrorCode::NotLeader,
        ServerErrorCode::Unavailable,
        ServerErrorCode::Unimplemented,
        ServerErrorCode::Internal,
    ];
    for code in SERVER_ERROR_CODES {
        let client_code: client::ErrorCode = code.as_str().parse().unwrap();
        assert!(
            !matches!(client_code, client::ErrorCode::Other(_)),
            "{} decoded as Other",
            code.as_str()
        );
        assert_eq!(client_code.to_string(), code.as_str());
    }
}

/// The two `Consistency` vocabularies must agree on every spelling: the
/// server's is `Deserialize`-only, so the client's `Display` is fed back
/// through the server's decoder.
#[test]
fn consistency_spellings_agree_between_the_two_crates() {
    for consistency in [
        client::Consistency::Strong,
        client::Consistency::Bounded,
        client::Consistency::Eventual,
    ] {
        let decoded: ServerConsistency =
            serde_json::from_value(json!(consistency.to_string())).unwrap();
        let expected = match consistency {
            client::Consistency::Strong => ServerConsistency::Strong,
            client::Consistency::Bounded => ServerConsistency::Bounded,
            client::Consistency::Eventual => ServerConsistency::Eventual,
            _ => unreachable!("exhaustive over the three known variants"),
        };
        assert_eq!(decoded, expected);
    }
}

/// `AuthMode` (`GetAuthConfigResponse::mode`): every spelling the server's
/// `coppice_authn::AuthMode::as_str()` can produce must decode to a known
/// (non-`Unknown`) client value.
#[test]
fn every_server_auth_mode_is_known_to_the_client() {
    let oidc = ServerAuthMode::Oidc(coppice_authn::OidcConfig {
        issuer: "https://idp.example".to_string(),
        client_id: "coppice".to_string(),
        audience: "coppice-api".to_string(),
    });
    for mode in [&oidc, &ServerAuthMode::Open] {
        let json = json!(mode.as_str());
        let client_mode: client::AuthMode = serde_json::from_value(json.clone()).unwrap();
        assert!(!client_mode.is_unknown(), "{json} decoded as Unknown");
        assert_eq!(serde_json::to_value(&client_mode).unwrap(), json);
    }
}

/// `AuthMethod` (`GetSessionResponse::auth_method`): every spelling the
/// server's `AuthMethod::as_str()` can produce must decode to a known
/// (non-`Unknown`) client value.
#[test]
fn every_server_auth_method_is_known_to_the_client() {
    for method in [
        ServerAuthMethod::Bearer,
        ServerAuthMethod::OperatorCert,
        ServerAuthMethod::Open,
    ] {
        let json = json!(method.as_str());
        let client_method: client::AuthMethod = serde_json::from_value(json.clone()).unwrap();
        assert!(!client_method.is_unknown(), "{json} decoded as Unknown");
        assert_eq!(serde_json::to_value(&client_method).unwrap(), json);
    }
}

/// `HealthStatus` (`HealthzResponse::status`): the server's one value must
/// decode to a known (non-`Unknown`) client value.
#[test]
fn the_healthz_status_is_known_to_the_client() {
    let server = dto::HealthzResponse { status: "ok" };
    let json = serde_json::to_value(server).unwrap();
    let client_status: client::HealthzResponse = serde_json::from_value(json.clone()).unwrap();
    assert!(
        !client_status.status.is_unknown(),
        "{json} decoded as Unknown"
    );
    assert_eq!(serde_json::to_value(&client_status).unwrap(), json);
}

/// `RaftId`: the decimal-string wire form crosses both ways, and a
/// non-decimal string is a decode error rather than a silent truncation.
#[test]
fn raft_id_round_trips_and_rejects_a_non_decimal_string() {
    let huge = 7_234_980_239_847_293_847u64; // > 2^53
    let json = json!(huge.to_string());
    let id: client::RaftId = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(id, client::RaftId(huge));
    assert_eq!(serde_json::to_value(id).unwrap(), json);
    assert!(serde_json::from_value::<client::RaftId>(json!("not-a-number")).is_err());
}

// ---------------------------------------------------------------------------
// 5. `Timestamp`, ids, and metadata parity
// ---------------------------------------------------------------------------

/// The two `Timestamp` types must serialize identically and cross-decode.
#[test]
fn timestamp_serializes_identically_and_cross_decodes() {
    // `ServerTimestamp::max_value()`/`min_value()` are deliberately excluded
    // from this corpus: they render a five-digit, `+`/`-`-prefixed extended
    // year (`"+262142-12-31T23:59:59.999999Z"`), which chrono's own
    // `DateTime::parse_from_rfc3339` refuses to parse back — a pre-existing
    // bug in `coppice_core::time::Timestamp` itself (it cannot round-trip
    // its own extreme values through its own `Serialize`/`Deserialize`,
    // confirmed independently of this client), not a client/server copy
    // drift. See this test's final report for the repro. `9999-12-31` below
    // is the largest instant whose year still renders without the
    // extended-year prefix, so it stays inside the format both parsers
    // accept.
    let micros = [
        0i64,
        1,
        -1,
        9_500_000,
        1_700_000_000_000_000,
        253_402_300_799_999_999, // 9999-12-31T23:59:59.999999Z
        -62_135_596_800_000_000, // 0001-01-01T00:00:00.000000Z
    ];
    for us in micros {
        let server = ServerTimestamp::from_micros(us).expect("fixture in range");
        let client = client::Timestamp::from_micros(us).expect("fixture in range");

        let server_json = serde_json::to_value(server).unwrap();
        let client_json = serde_json::to_value(client).unwrap();
        assert_eq!(server_json, client_json, "rendering diverged for {us}us");

        let client_from_server: client::Timestamp = serde_json::from_value(server_json).unwrap();
        assert_eq!(client_from_server.as_micros(), us);
        let server_from_client: ServerTimestamp = serde_json::from_value(client_json).unwrap();
        assert_eq!(server_from_client.as_micros(), us);
    }
}

/// For every id type present in both crates, the wire prefix must match and
/// each side must decode the other's rendering to the same uuid.
#[test]
fn typed_id_prefixes_and_cross_decoding_agree() {
    macro_rules! check_id {
        ($server_ty:ty, $client_ty:ty, $prefix:literal) => {
            assert_eq!(<$server_ty>::PREFIX, $prefix);
            assert_eq!(<$client_ty>::PREFIX, $prefix);

            let server_id = <$server_ty>::new();
            let text = server_id.to_string();
            let client_id: $client_ty = text.parse().unwrap();
            assert_eq!(client_id.0, server_id.0);

            let client_id2 = <$client_ty>::new();
            let text2 = client_id2.to_string();
            let server_id2: $server_ty = text2.parse().unwrap();
            assert_eq!(server_id2.0, client_id2.0);
        };
    }
    check_id!(core_id::JobId, client::JobId, "job");
    check_id!(core_id::NodeId, client::NodeId, "node");
    check_id!(core_id::AllocationId, client::AllocationId, "alloc");
    check_id!(core_id::AttemptId, client::AttemptId, "attempt");
    check_id!(core_id::QuotaEntityId, client::QuotaEntityId, "quota");
    check_id!(core_id::ClusterId, client::ClusterId, "cluster");
}

/// The metadata limits and key-validation rule must be identical between
/// the two crates, including the rejection text.
///
/// `coppice_client`'s `MAX_KEYS`/`MAX_KEY_BYTES`/`MAX_VALUE_BYTES` constants
/// and its `validate_key` function are declared `pub` inside a private
/// module (`mod metadata;` in `lib.rs`, re-exporting only `JobMetadata` and
/// `MetadataError`), so they are not part of the crate's public API and
/// cannot be named from this test crate. This checks the same limits and
/// rule indirectly, through the public `JobMetadata::insert`/`::validate`,
/// which exercise them internally; the literal `64`/`64`/`1024` below are
/// compared against `coppice_core::metadata`'s constants directly.
#[test]
fn metadata_limits_and_key_validation_agree() {
    assert_eq!(core_metadata::MAX_KEYS, 64);
    assert_eq!(core_metadata::MAX_KEY_BYTES, 64);
    assert_eq!(core_metadata::MAX_VALUE_BYTES, 1024);

    let long_key = "x".repeat(65);
    let keys = [
        "name",
        "a.b_c-d/e:f",
        "",
        "has space",
        "emoji🙂",
        "brace{",
        "under~score",
        long_key.as_str(),
    ];
    for key in keys {
        let server_result = core_metadata::validate_key(key);
        let mut client_metadata = client::JobMetadata::new();
        let client_result = client_metadata.insert(key, "v");
        assert_eq!(
            server_result.is_ok(),
            client_result.is_ok(),
            "agreement diverged for {key:?}"
        );
        if let (Err(s), Err(c)) = (server_result, client_result) {
            assert_eq!(
                s.to_string(),
                c.to_string(),
                "error text diverged for {key:?}"
            );
        }
    }

    // The key-count limit, exercised through `validate` (the whole-map
    // check `insert` deliberately does not run per entry).
    let at_limit: client::JobMetadata = (0..64).map(|i| (format!("k{i}"), String::new())).collect();
    assert!(at_limit.validate().is_ok());
    let over_limit: client::JobMetadata =
        (0..65).map(|i| (format!("k{i}"), String::new())).collect();
    let client_err = over_limit.validate().unwrap_err();
    let mut server_over_limit: core_metadata::JobMetadata = BTreeMap::new();
    for i in 0..65 {
        server_over_limit.insert(format!("k{i}"), String::new());
    }
    let server_err = core_metadata::validate(&server_over_limit).unwrap_err();
    assert_eq!(client_err.to_string(), server_err.to_string());

    // The value-length limit.
    let mut client_metadata = client::JobMetadata::new();
    let client_err = client_metadata.insert("k", "x".repeat(1025)).unwrap_err();
    let mut server_over_value: core_metadata::JobMetadata = BTreeMap::new();
    server_over_value.insert("k".to_string(), "x".repeat(1025));
    let server_err = core_metadata::validate(&server_over_value).unwrap_err();
    assert_eq!(client_err.to_string(), server_err.to_string());

    // The two `JobMetadata` shapes serialize identically and cross-decode.
    let server_metadata: core_metadata::JobMetadata =
        metadata(&[("name", "nightly"), ("owner", "platform")]);
    let client_metadata =
        client::JobMetadata::from_iter([("name", "nightly"), ("owner", "platform")]);
    let server_json = serde_json::to_value(&server_metadata).unwrap();
    let client_json = serde_json::to_value(&client_metadata).unwrap();
    assert_eq!(server_json, client_json);

    let client_from_server: client::JobMetadata = serde_json::from_value(server_json).unwrap();
    assert_eq!(client_from_server, client_metadata);
    let server_from_client: core_metadata::JobMetadata =
        serde_json::from_value(client_json).unwrap();
    assert_eq!(server_from_client, server_metadata);
}

/// The environment limits and the portable-name rule must be identical
/// between `coppice_core::env` and `coppice_client`: same constants, the same
/// accept/refuse verdict for every name and value, and the same error text,
/// so a map refused client-side is refused by the server for the same reason.
#[test]
fn env_limits_and_validation_agree() {
    use coppice_core::env as core_env;

    assert_eq!(core_env::MAX_VARS, client::MAX_ENV_VARS);
    assert_eq!(core_env::MAX_NAME_BYTES, client::MAX_ENV_NAME_BYTES);
    assert_eq!(core_env::MAX_VALUE_BYTES, client::MAX_ENV_VALUE_BYTES);
    assert_eq!(core_env::MAX_TOTAL_BYTES, client::MAX_ENV_TOTAL_BYTES);

    let long_name = "N".repeat(129);
    let max_name = "N".repeat(128);
    let big_value = "x".repeat(4097);
    let max_value = "x".repeat(4096);
    let mut maps: Vec<Vec<(String, String)>> = vec![];
    for (name, value) in [
        ("PATH", "/usr/bin"),
        ("_PRIVATE", ""),
        ("lower_ok", "1"),
        ("", "x"),
        ("1ST", "x"),
        ("A-B", "x"),
        ("A B", "x"),
        ("\u{e9}", "x"),
        (long_name.as_str(), "x"),
        (max_name.as_str(), "x"),
        ("BIG", big_value.as_str()),
        ("MAX", max_value.as_str()),
        ("NUL", "a\0b"),
    ] {
        maps.push(vec![(name.to_string(), value.to_string())]);
    }
    // Whole-map limits: one variable too many, and a total over 32 KiB.
    maps.push((0..=64).map(|i| (format!("V{i}"), String::new())).collect());
    maps.push((0..64).map(|i| (format!("V{i}"), String::new())).collect());
    maps.push(
        (0..9)
            .map(|i| (format!("V{i}"), max_value.clone()))
            .collect(),
    );

    for entries in maps {
        let server: core_env::JobEnv = entries.iter().cloned().collect();
        let client_env = client::JobEnv::from_iter(entries.iter().cloned());
        assert_eq!(
            core_env::validate(&server).map_err(|e| e.to_string()),
            client_env.validate().map_err(|e| e.to_string()),
            "{:?}",
            entries.iter().map(|(n, _)| n).collect::<Vec<_>>()
        );
        assert_eq!(
            serde_json::to_value(&server).unwrap(),
            serde_json::to_value(&client_env).unwrap()
        );
    }
}
