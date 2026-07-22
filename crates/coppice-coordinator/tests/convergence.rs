//! The heart of ADR 0037: self-converging membership, explicit formation, and
//! the machine self-service authorization matrix — all over the real mTLS admin
//! surface, driven through the same `bootstrap` path the daemon uses.
//!
//! - `self_converging_formation`: the ADR's central claim — three coordinator
//!   processes started from identical-in-shape `file`-discovery configs sharing
//!   one directory, plus ONE `cluster init`, converge to a full three-voter
//!   cluster with no hand-driven `AddLearner`/`PromoteVoter` anywhere. Asserts
//!   `Phase::Voter` on all three, three voters each bound to a distinct machine
//!   identity, real HTTP `/readyz` 200 (plain and `?require=formed`) on all
//!   three, and a 503 `waiting` on every replica pre-init (ADR 0037 §1/§4/§7).
//! - `double_init_against_second_parked_is_refused`: `InitializeCluster` against
//!   a second parked daemon that discovers the already-formed cluster is refused
//!   by the probe guard (ADR 0037 §3 case c).
//! - `formation_state_machine`: the durable, idempotent formation token
//!   (same token re-reports; a different token conflicts, naming the recorded
//!   one — ADR 0037 §3).
//! - `authz_matrix`: the refusal matrix (ADR 0037 §6) — agent certs refused,
//!   machine certs refused on operator verbs and foreign ids, operators allowed.

mod common;

use std::time::{Duration, Instant};

use prost::Message as _;
use serde_json::Value;
use tonic::Code;

use coppice_coordinator::admin;
use coppice_coordinator::convergence::Phase;
use coppice_core::id::ClusterId;
use coppice_proto::pb::raft::v1 as pb;

use common::{poll, Ca, Node};

const DEADLINE: Duration = Duration::from_secs(20);

/// GET `<base><path>` and return the status code plus parsed JSON body.
async fn get_readyz(base: &str, path: &str) -> (reqwest::StatusCode, Value) {
    let resp = reqwest::Client::new()
        .get(format!("{base}{path}"))
        .send()
        .await
        .expect("GET /readyz");
    let status = resp.status();
    let body: Value = resp.json().await.expect("/readyz body is JSON");
    (status, body)
}

/// Poll `/readyz` until `pred` holds or the deadline elapses.
async fn poll_readyz(
    base: &str,
    path: &str,
    pred: impl Fn(reqwest::StatusCode, &Value) -> bool,
) -> (reqwest::StatusCode, Value) {
    let start = Instant::now();
    loop {
        let (status, body) = get_readyz(base, path).await;
        if pred(status, &body) {
            return (status, body);
        }
        if start.elapsed() >= DEADLINE {
            panic!("timed out waiting for /readyz{path}; last: {status} {body}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// The ADR's central claim (ADR 0037 §1/§4/§7): three coordinator processes
/// started from identical-in-shape `file`-discovery configs — one shared
/// registration directory, per-process data dir and port the harness generates —
/// plus ONE `cluster init` converge to a full three-voter cluster with no
/// hand-driven `AddLearner`/`PromoteVoter` anywhere. Every join and promotion is
/// each replica's own convergence loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn self_converging_formation() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // One shared file-discovery directory: the only thing the three "identical"
    // configs have in common besides cluster_id. Each process registers itself
    // here on boot and discovers the others by enumerating it.
    let discovery_dir = tempfile::tempdir().expect("discovery dir");

    let mut nodes: Vec<Node> = (1..=3)
        .map(|id| Node::new_file_discovery(id, cluster_id, &ca, discovery_dir.path().to_path_buf()))
        .collect();
    for node in &mut nodes {
        node.boot().await;
    }

    // Each replica serves the real HTTP `/readyz` route (ADR 0037 §7).
    let mut bases = Vec::new();
    for node in &mut nodes {
        bases.push(node.serve_readyz().await);
    }

    // Pre-init: every replica is parked (nothing initialized in discovery), so
    // plain `/readyz` is 503 with phase `waiting` — alive but deliberately not
    // ready (ADR 0037 §1/§7).
    for base in &bases {
        let (status, body) = poll_readyz(base, "/readyz", |status, body| {
            status == reqwest::StatusCode::SERVICE_UNAVAILABLE && body["phase"] == "waiting"
        })
        .await;
        assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["phase"], "waiting");
        assert_eq!(body["formed"], false);
    }

    // ONE formation act against any one replica (ADR 0037 §3). The other two
    // discover the formed cluster on their next probe and self-join.
    nodes[0].form("self-converging-formation").await;

    // All three reach the terminal `Voter` phase entirely on their own.
    for (i, node) in nodes.iter().enumerate() {
        poll(DEADLINE, "replica converges to Voter", || async {
            node.convergence_phase() == Phase::Voter && node.is_voter()
        })
        .await;
        assert_eq!(node.convergence_phase(), Phase::Voter, "replica {i} phase");
    }

    // Membership is exactly three voters, each bound to its own distinct machine
    // identity (ADR 0037 §6). Assert from every replica's own view (raft
    // membership is replicated, not a leader-only fact).
    for node in &nodes {
        let voters: Vec<_> = node
            .summary()
            .members
            .into_iter()
            .filter(|m| m.voter)
            .collect();
        assert_eq!(voters.len(), 3, "three voters: {voters:?}");
        let mut identities: Vec<String> =
            voters.iter().map(|m| m.machine_identity.clone()).collect();
        identities.sort();
        assert_eq!(
            identities,
            vec!["coord-1", "coord-2", "coord-3"],
            "each voter bound to a distinct machine identity"
        );
    }

    // Real HTTP `/readyz`: plain 200 (caught-up voter) and `?require=formed` 200
    // (membership cardinality met, 3 ≥ cluster_size 3) on all three.
    for base in &bases {
        let (status, body) = poll_readyz(base, "/readyz", |status, body| {
            status == reqwest::StatusCode::OK && body["phase"] == "voter"
        })
        .await;
        assert_eq!(status, reqwest::StatusCode::OK, "plain /readyz 200: {body}");
        assert_eq!(body["voters"], 3);
        assert_eq!(body["formed"], true);

        let (status, body) = get_readyz(base, "/readyz?require=formed").await;
        assert_eq!(
            status,
            reqwest::StatusCode::OK,
            "?require=formed 200: {body}"
        );
        assert_eq!(body["formed"], true);
    }

    for mut node in nodes {
        node.graceful_stop().await;
    }
}

/// The formation probe guard (ADR 0037 §3 case c): once one daemon has formed
/// the cluster, `InitializeCluster` against a *second* parked daemon that can
/// discover the formed one is refused — a guard against accidental double-init
/// forking formation into two histories under one cluster id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn double_init_against_second_parked_is_refused() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    // Node 1 forms the cluster.
    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("double-init-formation").await;
    poll(DEADLINE, "node 1 is a formed leader", || async {
        node1.is_leader() && node1.is_voter()
    })
    .await;

    // Node 2 parks, but its discovery seed points at node 1, so its probe guard
    // will see the already-initialized cluster.
    let mut node2 = Node::new_with_seeds(2, cluster_id, &ca, &[node1.advertise.clone()]);
    node2.boot().await;
    let target = node2.advertise.clone();

    // An operator tries to initialize node 2 as a *second* founding voter.
    let op = ca.operator_leaf();
    let mut client = admin::admin_channel(&target, &ca.pem, &op.cert_pem, &op.key_pem)
        .await
        .expect("dial node 2 admin surface");
    let status = client
        .initialize_cluster(pb::InitializeClusterRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            formation_token: "second-formation".into(),
            policy_toml: None,
        })
        .await
        .expect_err("double-init against a discoverable formed cluster is refused");
    assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "{}",
        status.message()
    );
    assert!(
        status.message().contains("initialize") || status.message().contains("fork"),
        "probe-guard refusal names the double-init hazard: {}",
        status.message()
    );

    // Node 2 was never formed on its own; it remains free to *join* node 1
    // instead (its convergence loop does so).
    poll(
        DEADLINE,
        "node 2 joins the existing cluster instead",
        || async { node2.is_voter() },
    )
    .await;

    node2.graceful_stop().await;
    node1.graceful_stop().await;
}

/// The durable formation token is a resumable, idempotent state machine
/// (ADR 0037 §3): the same token re-reports success; a different token is
/// refused, naming the recorded token in a decodable refusal detail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn formation_state_machine() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    let mut node = Node::new(1, cluster_id, &ca);
    node.boot().await; // parked, uninitialized
    let target = node.advertise.clone();

    // Operators form; machines cannot (§6). Dial with an operator leaf.
    let op = ca.operator_leaf();
    let mut client = admin::admin_channel(&target, &ca.pem, &op.cert_pem, &op.key_pem)
        .await
        .expect("dial admin surface");

    // First init with token "alpha": success, not already-initialized.
    let first = client
        .initialize_cluster(pb::InitializeClusterRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            formation_token: "alpha".into(),
            policy_toml: None,
        })
        .await
        .expect("first init succeeds")
        .into_inner();
    assert_eq!(first.node_id, node.raft_id());
    assert!(!first.already_initialized);

    // Re-init with the SAME token: idempotent success, already-initialized.
    let again = client
        .initialize_cluster(pb::InitializeClusterRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            formation_token: "alpha".into(),
            policy_toml: None,
        })
        .await
        .expect("re-init with same token succeeds")
        .into_inner();
    assert!(again.already_initialized, "same token resumes/re-reports");

    // Init with a DIFFERENT token: refused, naming the recorded token.
    let status = client
        .initialize_cluster(pb::InitializeClusterRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            formation_token: "beta".into(),
            policy_toml: None,
        })
        .await
        .expect_err("a different token conflicts");
    assert_eq!(status.code(), Code::FailedPrecondition);
    let refusal = pb::MembershipRefusal::decode(status.details()).expect("decodable refusal");
    match refusal.reason {
        Some(pb::membership_refusal::Reason::ConflictingFormationToken(c)) => {
            assert_eq!(
                c.recorded_token, "alpha",
                "the refusal names the recorded token"
            );
        }
        other => panic!("expected ConflictingFormationToken, got {other:?}"),
    }

    node.graceful_stop().await;
}

/// `cluster init --policy` applies the bootstrap policy as part of formation,
/// and a same-token re-init re-applies it idempotently — no duplicate effect
/// (ADR 0037 §3).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_applies_policy_idempotently() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    let mut node = Node::new(1, cluster_id, &ca);
    node.boot().await; // parked, uninitialized
    let target = node.advertise.clone();
    let views = node.views();

    // A minimal bootstrap policy: one priority multiplier and one quota entity.
    const POLICY: &str = r#"
[[priority_multiplier]]
index = 0
multiplier = 1.0

[[quota_entity]]
id = "quota-00000000-0000-0000-0000-0000000000aa"
name = "prod"
quota = 500000
"#;
    let entity: coppice_core::id::QuotaEntityId = "quota-00000000-0000-0000-0000-0000000000aa"
        .parse()
        .expect("quota entity id");

    let op = ca.operator_leaf();
    let mut client = admin::admin_channel(&target, &ca.pem, &op.cert_pem, &op.key_pem)
        .await
        .expect("dial admin surface");

    // First init WITH policy: forms and applies the policy.
    let first = client
        .initialize_cluster(pb::InitializeClusterRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            formation_token: "policy-token".into(),
            policy_toml: Some(POLICY.as_bytes().to_vec()),
        })
        .await
        .expect("init with policy succeeds")
        .into_inner();
    assert!(!first.already_initialized);

    // The policy is observable in applied state.
    poll(DEADLINE, "policy applied to state", || async {
        let view = views.latest();
        let state = view.state();
        !state.policy.priority_multipliers.is_empty() && state.quota_entities.contains_key(&entity)
    })
    .await;
    {
        let view = views.latest();
        let state = view.state();
        assert_eq!(
            state.policy.priority_multipliers.get(&0),
            Some(&coppice_core::quota::PriorityMultiplier::ONE),
            "priority index 0 maps to 1.0×"
        );
        assert_eq!(
            state.quota_entities.get(&entity).map(|e| e.quota.0),
            Some(500_000),
            "the quota entity was created with the configured stock"
        );
    }

    // Re-init with the SAME token and policy: idempotent, already-initialized,
    // no duplicate effect (the quota entity is not re-created).
    let again = client
        .initialize_cluster(pb::InitializeClusterRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            formation_token: "policy-token".into(),
            policy_toml: Some(POLICY.as_bytes().to_vec()),
        })
        .await
        .expect("re-init with same token succeeds")
        .into_inner();
    assert!(again.already_initialized, "same token re-reports formation");
    {
        let view = views.latest();
        let state = view.state();
        assert_eq!(
            state.quota_entities.len(),
            1,
            "exactly one quota entity — re-init created no duplicate"
        );
        assert!(state.quota_entities.contains_key(&entity));
    }

    // A DIFFERENT token is still refused, naming the recorded one.
    let status = client
        .initialize_cluster(pb::InitializeClusterRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            formation_token: "other-token".into(),
            policy_toml: Some(POLICY.as_bytes().to_vec()),
        })
        .await
        .expect_err("a different token conflicts");
    assert_eq!(status.code(), Code::FailedPrecondition);
    let refusal = pb::MembershipRefusal::decode(status.details()).expect("decodable refusal");
    assert!(matches!(
        refusal.reason,
        Some(pb::membership_refusal::Reason::ConflictingFormationToken(c)) if c.recorded_token == "policy-token"
    ));

    node.graceful_stop().await;
}

/// The ADR 0037 §6 authorization matrix over the real admin surface.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authz_matrix() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    let mut node = Node::new(1, cluster_id, &ca);
    node.boot().await;
    node.form("authz-formation").await;
    poll(DEADLINE, "leader elected", || async { node.is_leader() }).await;
    let target = node.advertise.clone();
    let node_id = node.raft_id();

    // (1) An AGENT cert (no OU) can call none of the surface — not even Probe.
    {
        let agent = ca.leaf(); // no OU → agent profile
        let mut client = admin::admin_channel(&target, &ca.pem, &agent.cert_pem, &agent.key_pem)
            .await
            .expect("dial");
        let err = client
            .probe_cluster(pb::ProbeClusterRequest {})
            .await
            .expect_err("agent cert refused from ProbeCluster");
        assert_eq!(err.code(), Code::PermissionDenied, "{}", err.message());
    }

    // (2) A MACHINE cert is refused RemoveNode, InitializeCluster, and promotion
    //     of a node bound to another machine identity (a foreign id).
    {
        let foreign = ca.machine_leaf("coord-99"); // not bound to any seat
        let mut client =
            admin::admin_channel(&target, &ca.pem, &foreign.cert_pem, &foreign.key_pem)
                .await
                .expect("dial");

        let err = client
            .remove_node(pb::RemoveNodeRequest {
                cluster_uuid: cluster_uuid.to_vec(),
                node_id,
            })
            .await
            .expect_err("machine cert refused RemoveNode");
        assert_eq!(
            err.code(),
            Code::PermissionDenied,
            "RemoveNode: {}",
            err.message()
        );

        let err = client
            .initialize_cluster(pb::InitializeClusterRequest {
                cluster_uuid: cluster_uuid.to_vec(),
                formation_token: "x".into(),
                policy_toml: None,
            })
            .await
            .expect_err("machine cert refused InitializeCluster");
        assert_eq!(
            err.code(),
            Code::PermissionDenied,
            "Init: {}",
            err.message()
        );

        // Promotion of node 1 (bound to coord-1) by the coord-99 machine cert is
        // a foreign-id promotion → refused.
        let err = client
            .promote_voter(pb::PromoteVoterRequest {
                cluster_uuid: cluster_uuid.to_vec(),
                promote_node_id: node_id,
                remove_node_id: None,
            })
            .await
            .expect_err("machine cert refused promotion of a foreign id");
        assert_eq!(
            err.code(),
            Code::PermissionDenied,
            "Promote: {}",
            err.message()
        );
    }

    // (3) An OPERATOR cert may call the operator-only verbs. RemoveNode of an
    //     absent id is an idempotent no-op success (proving authorization, not a
    //     real removal).
    {
        let op = ca.operator_leaf();
        let mut client = admin::admin_channel(&target, &ca.pem, &op.cert_pem, &op.key_pem)
            .await
            .expect("dial");
        admin::remove_node(&mut client, cluster_uuid, 123_456_789)
            .await
            .expect("operator RemoveNode of an absent id is an allowed no-op");
    }

    node.graceful_stop().await;
}
