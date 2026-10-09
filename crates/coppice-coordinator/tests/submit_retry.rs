//! KOI-2 end-to-end regression: job submission is idempotent across an
//! unknown outcome (ADR 0026).
//!
//! A client submits through the leader's `ControlPlane`, the response is
//! "lost" (the test simply ignores what it learned), the leader dies, and
//! the client retries the *identical* request through the next coordinator.
//! Exactly one job may exist afterwards, and the retry must return the
//! original client-minted id.
//!
//! The same contract holds for a submission that names its quota entity by
//! **path** (ADR 0045): the path resolves before the proposal, and because an
//! entity's name and parent are fixed at creation, a retry resolves to the
//! same id and reaches the job-id dedup with an identical spec — even after
//! an attempted rename, which is refused.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use tower::ServiceExt;

use coppice_api::http::dto;
use coppice_api::{ApiError, ControlPlane};
use coppice_consensus::Consensus;
use coppice_coordinator::admin;
use coppice_coordinator::CoordinatorControlPlane;
use coppice_core::id::{ClusterId, JobId, QuotaEntityId};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::time::Timestamp;
use coppice_state::command::{ConfigureQuotaEntity, UpdatePolicy};
use coppice_state::{Actor, Command, PolicyConfig};

use common::{poll, Ca, Node};

const DEADLINE: Duration = Duration::from_secs(20);
/// Promotion retry cadence for the admin wrapper — the in-test twin of the
/// fixture's `[pacing] promote_poll_interval`, which the daemons run with.
const POLL: Duration = Duration::from_millis(50);

/// The actor every submission here proposes as.
///
/// The open posture (`auth_disabled`), which is what an unconfigured fleet
/// runs: this test is about idempotent retry across a leader change, and a
/// cluster seeded with no role bindings would otherwise refuse every write at
/// apply for reasons that have nothing to do with what it is testing.
fn actor() -> Actor {
    Actor {
        principal: "anonymous".to_string(),
        groups: Vec::new(),
        operator_cert: false,
        auth_disabled: true,
    }
}

async fn wait_for_leader(nodes: &[Node], candidates: &[usize], deadline: Duration) -> usize {
    let start = Instant::now();
    loop {
        for &i in candidates {
            if nodes[i].is_booted() && nodes[i].is_leader() {
                return i;
            }
        }
        if start.elapsed() >= deadline {
            panic!("no leader emerged among {candidates:?} within {deadline:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The one logical submission, byte-identical on every send.
fn submit_request(job: JobId, quota_entity: QuotaEntityId) -> dto::ResolvedSubmitJobRequest {
    dto::ResolvedSubmitJobRequest {
        image: "registry/img:latest".to_string(),
        requests: dto::Resources {
            cpu_millis: 1000,
            memory_bytes: 0,
            disk_bytes: 0,
        },
        priority: 0,
        max_runtime_seconds: Some(3_600),
        quota_entity,
        retry: None,
        job,
        command: vec!["run".to_string()],
        entrypoint: None,
        metadata: Default::default(),
        env: Default::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retried_submission_across_leader_change_creates_one_job() {
    let ca = Ca::new();
    let admin_leaf = ca.operator_leaf();
    let cluster_id = ClusterId::new();
    let history_id = *cluster_id.0.as_bytes();

    // -- Form a three-voter cluster (bootstrap + learner-join + promote). ---
    let mut nodes: Vec<Node> = (1..=3).map(|id| Node::new(id, cluster_id, &ca)).collect();
    nodes[0].boot().await;
    wait_for_leader(&nodes, &[0], DEADLINE).await;
    for i in [1usize, 2] {
        nodes[i].boot_joining().await;
    }
    {
        let target = nodes[0].advertise.clone();
        let mut client =
            admin::admin_channel(&target, &ca.pem, &admin_leaf.cert_pem, &admin_leaf.key_pem)
                .await
                .expect("dial leader admin surface");
        for i in [1usize, 2] {
            admin::add_learner(
                &mut client,
                history_id,
                nodes[i].raft_id(),
                nodes[i].advertise.clone(),
            )
            .await
            .unwrap_or_else(|e| panic!("add-learner {} failed: {e:#}", nodes[i].id));
        }
        // promote_voter polls the catch-up gate itself.
        for i in [1usize, 2] {
            admin::promote_voter(&mut client, history_id, nodes[i].raft_id(), DEADLINE, POLL)
                .await
                .unwrap_or_else(|e| panic!("promote {} failed: {e:#}", nodes[i].id));
        }
    }

    // -- Seed the state submit_job validates against: a quota entity and a --
    // -- multiplier for priority 0. ------------------------------------------
    let quota_entity = QuotaEntityId::new();
    let leader = wait_for_leader(&nodes, &[0, 1, 2], DEADLINE).await;
    let consensus = nodes[leader].consensus();
    consensus
        .propose(Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
            entity: quota_entity,
            parent: None,
            name: "root".into(),
            quota: CostUnits(1_000_000),
            updated_at: Timestamp::from_micros(1).expect("in range"),
            actor: None,
        }))
        .await
        .expect("configure quota entity")
        .outcome
        .expect("quota entity accepted");
    let mut policy = PolicyConfig::default();
    policy
        .priority_multipliers
        .insert(0, PriorityMultiplier::ONE);
    consensus
        .propose(Command::UpdatePolicy(UpdatePolicy {
            policy,
            updated_at: Timestamp::from_micros(2).expect("in range"),
            actor: None,
        }))
        .await
        .expect("update policy")
        .outcome
        .expect("policy accepted");

    // Every replica must see the seeded policy before it can serve
    // submit_job's synchronous multiplier resolution.
    for node in &nodes {
        let views = node.views();
        poll(DEADLINE, "replica sees seeded policy", move || {
            let views = views.clone();
            async move {
                let view = views.latest();
                view.state().policy.priority_multipliers.contains_key(&0)
            }
        })
        .await;
    }

    // -- First submission through the leader; the response is "lost". -------
    let job = JobId::new();
    let request = submit_request(job, quota_entity);
    {
        let cp = CoordinatorControlPlane::new(
            nodes[leader].consensus(),
            nodes[leader].views(),
            cluster_id,
        );
        let first = cp
            .submit_job(request.clone(), actor())
            .await
            .expect("first submission accepted");
        assert_eq!(first.job, job);
        // ... and here the client never receives `first`.
    }

    // -- The leader dies before the client learns the outcome. --------------
    let survivors: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
    nodes[leader].kill().await;
    let new_leader = wait_for_leader(&nodes, &survivors, DEADLINE).await;

    // A retry that lands on the remaining follower is redirected, exactly
    // like any other write — dedup does not depend on hitting one replica.
    let follower = *survivors.iter().find(|&&i| i != new_leader).unwrap();
    {
        let cp = CoordinatorControlPlane::new(
            nodes[follower].consensus(),
            nodes[follower].views(),
            cluster_id,
        );
        let redirected = cp.submit_job(request.clone(), actor()).await;
        assert!(
            matches!(redirected, Err(ApiError::NotLeader { .. })),
            "follower must redirect, got {redirected:?}"
        );
    }

    // -- Identical retry through the new leader. -----------------------------
    let cp = Arc::new(CoordinatorControlPlane::new(
        nodes[new_leader].consensus(),
        nodes[new_leader].views(),
        cluster_id,
    ));
    let retried = cp
        .submit_job(request.clone(), actor())
        .await
        .expect("retry after unknown outcome must succeed");
    assert_eq!(
        retried.job, job,
        "the retry must resolve to the original client-minted job id"
    );
    assert!(retried.log_index > 0);

    // Reusing the id with a different payload is a distinct intent: rejected.
    let mut mutated = request.clone();
    mutated.image = "registry/other:latest".into();
    let mismatch = cp.submit_job(mutated, actor()).await;
    assert!(
        matches!(mismatch, Err(ApiError::Rejected(_))),
        "id reuse with a different spec must reject, got {mismatch:?}"
    );

    // -- Exactly one job exists on every surviving replica. -----------------
    for &i in &survivors {
        let views = nodes[i].views();
        let min_index = retried.log_index;
        poll(DEADLINE, "survivor applied the retry", move || {
            let views = views.clone();
            async move { views.latest().applied_index() >= min_index }
        })
        .await;
        let view = nodes[i].views().latest();
        let jobs = &view.state().jobs;
        assert_eq!(jobs.len(), 1, "exactly one job must exist");
        assert!(jobs.contains_key(&job));
    }

    for &i in &survivors {
        // Explicit teardown keeps the tempdirs alive until the end.
        nodes[i].graceful_stop().await;
    }
}

/// POST `body` to `uri` through the real router, returning the status and
/// the decoded JSON body.
async fn post(router: &axum::Router, uri: &str, body: &str) -> (StatusCode, serde_json::Value) {
    let response = router
        .clone()
        .oneshot(
            Request::post(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("router response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// ADR 0045 + ADR 0026 through the real HTTP edge and a real single-node
/// cluster: a submission by path, committed and then retried byte-for-byte,
/// is the dedup success (one job, the original id) — and stays so after an
/// attempted rename or move of the entity, because both are refused and the
/// path keeps resolving to the same id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retried_submission_by_path_dedups_even_after_an_attempted_rename() {
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let mut nodes = vec![Node::new(1, cluster_id, &ca)];
    nodes[0].boot().await;
    wait_for_leader(&nodes, &[0], DEADLINE).await;

    let mut policy = PolicyConfig::default();
    policy
        .priority_multipliers
        .insert(0, PriorityMultiplier::ONE);
    nodes[0]
        .consensus()
        .propose(Command::UpdatePolicy(UpdatePolicy {
            policy,
            updated_at: Timestamp::from_micros(1).expect("in range"),
            actor: None,
        }))
        .await
        .expect("update policy")
        .outcome
        .expect("policy accepted");

    let router = coppice_api::http::router(
        Arc::new(CoordinatorControlPlane::new(
            nodes[0].consensus(),
            nodes[0].views(),
            cluster_id,
        )),
        coppice_api::http::MetricsEndpoint::detached_for_tests(),
        coppice_api::http::ReadyzEndpoint::detached_for_tests(),
        coppice_api::http::EnrollEndpoint::detached_for_tests(),
        // Open mode: this is about path resolution and dedup, not authn.
        Arc::new(coppice_authn::AuthnChain::open(coppice_authn::no_ca())),
    );

    // -- `acme/eng`, created by path through the configure route. ----------
    let (acme, eng) = (QuotaEntityId::new(), QuotaEntityId::new());
    let configure = |entity: QuotaEntityId, parent: &str, name: &str, quota: u64| {
        format!(
            r#"{{ "entity": "{entity}", "parent": {parent}, "name": "{name}", "quota_ucu": {quota} }}"#
        )
    };
    let (status, body) = post(
        &router,
        "/api/v1/quota-entities",
        &configure(acme, "null", "acme", 1_000_000),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["path"], "acme");
    let (status, body) = post(
        &router,
        "/api/v1/quota-entities",
        &configure(eng, r#""acme""#, "eng", 1_000_000),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["path"], "acme/eng");
    // Path resolution reads this replica's view, which publishes the write
    // shortly after the propose resolves: wait for it, as a client pairing
    // the write's `log_index` with its next read would.
    let views = nodes[0].views();
    let min_index = body["log_index"].as_u64().expect("log_index");
    poll(DEADLINE, "the view holds acme/eng", move || {
        let views = views.clone();
        async move { views.latest().applied_index() >= min_index }
    })
    .await;

    // -- (a) Submit by path, then retry the identical bytes. ----------------
    let job = JobId::new();
    let submission = format!(
        r#"{{
            "image": "registry/img:latest",
            "command": ["run"],
            "requests": {{ "cpu_millis": 1000, "memory_bytes": 0, "disk_bytes": 0 }},
            "max_runtime_seconds": 3600,
            "job": "{job}",
            "quota_entity": "acme/eng"
        }}"#
    );
    let (status, first) = post(&router, "/api/v1/jobs", &submission).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["job"], job.to_string());
    let (status, retried) = post(&router, "/api/v1/jobs", &submission).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the retry is the dedup success: {retried}"
    );
    assert_eq!(retried["job"], job.to_string());

    // -- (b) An attempted rename, and an attempted move, are refused... -----
    for (parent, name) in [(r#""acme""#, "platform"), ("null", "eng")] {
        let (status, body) = post(
            &router,
            "/api/v1/quota-entities",
            &configure(eng, parent, name, 1_000_000),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "REJECTED");
    }
    // ...while a quota-only update of the same entity is fine.
    let (status, body) = post(
        &router,
        "/api/v1/quota-entities",
        &configure(eng, r#""acme""#, "eng", 2_000_000),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["path"], "acme/eng");

    // ...so the path still names the same entity, and the byte-identical
    // retry still resolves to it and dedups.
    let (status, again) = post(&router, "/api/v1/jobs", &submission).await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(again["job"], job.to_string());

    let views = nodes[0].views();
    let min_index = again["log_index"].as_u64().expect("log_index");
    poll(DEADLINE, "the view holds the retry", move || {
        let views = views.clone();
        async move { views.latest().applied_index() >= min_index }
    })
    .await;
    let view = nodes[0].views().latest();
    let state = view.state();
    assert_eq!(state.jobs.len(), 1, "exactly one job must exist");
    assert_eq!(state.jobs[&job].spec.quota_entity, eng);
    assert_eq!(state.quota_entity_path(eng).as_deref(), Some("acme/eng"));
    assert_eq!(state.quota_entities[&eng].quota, CostUnits(2_000_000));

    nodes[0].graceful_stop().await;
}
