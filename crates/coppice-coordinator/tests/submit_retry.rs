//! KOI-2 end-to-end regression: job submission is idempotent across an
//! unknown outcome (ADR 0026).
//!
//! A client submits through the leader's `ControlPlane`, the response is
//! "lost" (the test simply ignores what it learned), the leader dies, and
//! the client retries the *identical* request through the next coordinator.
//! Exactly one job may exist afterwards, and the retry must return the
//! original client-minted id.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use coppice_api::http::dto;
use coppice_api::{ApiError, ControlPlane};
use coppice_consensus::Consensus;
use coppice_coordinator::admin;
use coppice_coordinator::CoordinatorControlPlane;
use coppice_core::id::{ClusterId, JobId, QuotaEntityId};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::time::Timestamp;
use coppice_state::command::{ConfigureQuotaEntity, UpdatePolicy};
use coppice_state::{Command, PolicyConfig};

use common::{poll, Ca, Node};

const DEADLINE: Duration = Duration::from_secs(20);

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
fn submit_request(job: JobId, quota_entity: QuotaEntityId) -> dto::SubmitJobRequest {
    dto::SubmitJobRequest {
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
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retried_submission_across_leader_change_creates_one_job() {
    let ca = Ca::new();
    let admin_leaf = ca.operator_leaf();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    // -- Form a three-voter cluster (form + learner-join + promote, ADR 0037). -
    let mut nodes: Vec<Node> = (1..=3).map(|id| Node::new(id, cluster_id, &ca)).collect();
    nodes[0].boot().await;
    nodes[0].form("submit-retry-formation").await;
    wait_for_leader(&nodes, &[0], DEADLINE).await;
    for i in [1usize, 2] {
        nodes[i].boot().await;
    }
    {
        let target = nodes[0].advertise.clone();
        // Each replica self-joins under its own machine identity (ADR 0037 §6).
        for i in [1usize, 2] {
            let leaf = ca.machine_leaf(&nodes[i].machine);
            let mut client = admin::admin_channel(&target, &ca.pem, &leaf.cert_pem, &leaf.key_pem)
                .await
                .expect("dial leader admin surface");
            admin::add_learner(
                &mut client,
                cluster_uuid,
                nodes[i].raft_id(),
                nodes[i].advertise.clone(),
            )
            .await
            .unwrap_or_else(|e| panic!("add-learner {} failed: {e:#}", nodes[i].id));
        }
        // Promote through the operator client; promote_voter polls catch-up.
        let mut client =
            admin::admin_channel(&target, &ca.pem, &admin_leaf.cert_pem, &admin_leaf.key_pem)
                .await
                .expect("dial leader admin surface");
        for i in [1usize, 2] {
            admin::promote_voter(
                &mut client,
                cluster_uuid,
                nodes[i].raft_id(),
                None,
                DEADLINE,
            )
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
            .submit_job(request.clone())
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
        let redirected = cp.submit_job(request.clone()).await;
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
        .submit_job(request.clone())
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
    let mismatch = cp.submit_job(mutated).await;
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
