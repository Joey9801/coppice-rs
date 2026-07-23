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

use common::{poll, Ca, DiscoverySpec, Node, NodeSpec};

const DEADLINE: Duration = Duration::from_secs(20);

/// For waits that include a deliberate grace period (e.g. a killed leader
/// aging past `removal_grace` before an overflow removal can fold it out).
const LONG_DEADLINE: Duration = Duration::from_secs(40);

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

        // set-address is operator-only too (ADR 0037 §4/§6): a machine cert can
        // never repoint a voter — refused before any endpoint dial.
        let err = client
            .set_node_address(pb::SetNodeAddressRequest {
                cluster_uuid: cluster_uuid.to_vec(),
                node_id,
                address: "localhost:1".into(),
            })
            .await
            .expect_err("machine cert refused SetNodeAddress");
        assert_eq!(
            err.code(),
            Code::PermissionDenied,
            "SetNodeAddress: {}",
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

/// ADR 0037 §6 (finding #10): a coordinator whose serving leaf carries the
/// coordinator profile marker but NO Common Name must FAIL to boot, not warn.
/// An empty CN would be persisted as the founding voter's machine identity —
/// an unbound seat, which the ADR forbids ("no seat is ever unbound") and which
/// could never pass endpoint verification. The refusal names the cert path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boot_refuses_coordinator_leaf_without_cn() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // Lay down a normal node, then replace its serving leaf with a
    // coordinator-profile leaf that has no CN before booting.
    let node = Node::new(1, cluster_id, &ca);
    node.overwrite_leaf(&ca.machine_leaf_without_cn());

    let err = node
        .try_boot()
        .await
        .expect_err("a coordinator leaf without a CN must be refused at boot");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("Common Name") || msg.contains("CN"),
        "the refusal names the missing CN: {msg}"
    );
    assert!(
        msg.contains("node.crt"),
        "the refusal names the offending cert path: {msg}"
    );
}

/// The single registration file's first line in a `file`-discovery directory,
/// asserting there is exactly one (call before any second process registers).
fn only_registration(dir: &std::path::Path) -> String {
    let files: Vec<_> = std::fs::read_dir(dir)
        .expect("read discovery dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .collect();
    assert_eq!(
        files.len(),
        1,
        "expected exactly one registration file, found {}",
        files.len()
    );
    let contents = std::fs::read_to_string(files[0].path()).expect("read registration");
    contents
        .lines()
        .next()
        .expect("first line")
        .trim()
        .to_string()
}

/// Remove every registration file from a `file`-discovery directory, so a later
/// `candidates()` yields nothing — the way a test proves the convergence loop is
/// NOT leaning on discovery.
fn clear_discovery_dir(dir: &std::path::Path) {
    for entry in std::fs::read_dir(dir)
        .expect("read discovery dir")
        .flatten()
    {
        if entry.path().is_file() {
            std::fs::remove_file(entry.path()).expect("remove registration");
        }
    }
}

/// ADR 0037 §2 port-0 dev case (finding #6): a coordinator booted with
/// `raft_addr` port 0 must resolve its real bound port BEFORE publishing any
/// address, so its `file`-discovery registration carries `host:<real-port>`
/// (never `host:0`) and a second node can self-join through it. Before the fix,
/// registration happened ahead of the bind and permanently published `host:0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn port_zero_registration_publishes_real_port() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let discovery_dir = tempfile::tempdir().expect("discovery dir");

    // Node 1 binds raft_addr :0 and registers in the shared file-discovery dir.
    let mut n1 = Node::with_spec(
        NodeSpec::new(
            1,
            cluster_id,
            DiscoverySpec::File(discovery_dir.path().to_path_buf()),
        )
        .with_raft_port_zero(),
        &ca,
    );
    n1.boot().await;
    n1.form("port-zero-formation").await;
    poll(DEADLINE, "node 1 forms as a voter", || async {
        n1.is_leader() && n1.is_voter()
    })
    .await;

    // The registration file names the REAL bound port, not :0.
    let advertised = only_registration(discovery_dir.path());
    let port: u16 = advertised
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .unwrap_or_else(|| panic!("registration {advertised:?} has no parseable port"));
    assert_ne!(
        port, 0,
        "registration must carry the real bound port, got {advertised:?}"
    );

    // A second node self-joins purely by discovering node 1 through that file —
    // proving the published address is actually dialable.
    let mut n2 = Node::new_file_discovery(2, cluster_id, &ca, discovery_dir.path().to_path_buf());
    n2.boot().await;
    poll(
        DEADLINE,
        "node 2 joins via the port-0 node's real-port registration",
        || async { n2.is_voter() },
    )
    .await;

    n2.graceful_stop().await;
    n1.graceful_stop().await;
}

/// ADR 0037 §2/§4 (finding #5) + issue #47 leader-change-mid-join: once a
/// joiner's `AddLearner` has committed, the admitted learner carries replicated
/// membership + leader info, so it must keep converging by routing from its
/// LOCAL membership view — even if discovery goes empty AND the leader it joined
/// through dies. Before the fix, the loop still required discovery to yield a
/// reachable leader, so an admitted learner wedged forever when discovery went
/// empty or named only the dead leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn leader_change_mid_join_converges_via_local_membership() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let discovery_dir = tempfile::tempdir().expect("discovery dir");

    // cluster_size = 3 with three live voters: the fourth identity is ADMITTED
    // as a learner but its promotion is refused (voter set full, no removable
    // peer while all voters are alive) — a semantic, race-free hold in the
    // Learner phase. removal_grace is shortened so that once the leader is
    // killed, its corpse qualifies for overflow removal quickly and the held
    // promotion can complete against the successor.
    let grace = Duration::from_millis(1500);
    let mk = |id: u64| {
        Node::with_spec(
            NodeSpec {
                removal_grace: Some(grace),
                ..NodeSpec::new(
                    id,
                    cluster_id,
                    DiscoverySpec::File(discovery_dir.path().to_path_buf()),
                )
            },
            &ca,
        )
    };
    let mut n1 = mk(1);
    let mut n2 = mk(2);
    let mut n3 = mk(3);
    for n in [&mut n1, &mut n2, &mut n3] {
        n.boot().await;
    }

    // One formation act; the other two self-join. n1 is the founding voter and
    // the stable leader (it initialized as the single voter).
    n1.form("leader-change-formation").await;
    for (i, n) in [&n1, &n2, &n3].iter().enumerate() {
        poll(DEADLINE, "initial three voters", || async { n.is_voter() }).await;
        assert!(n.is_voter(), "node {} is a voter", i + 1);
    }
    poll(DEADLINE, "n1 is the leader", || async { n1.is_leader() }).await;

    // The joiner boots and self-joins THROUGH n1 (found via discovery,
    // pre-admission). Its AddLearner commits, but promotion is refused while
    // all three seats are held by live voters, so it is deterministically
    // parked in the Learner phase.
    let mut d = mk(4);
    d.boot().await;
    let d_id = d.raft_id();
    poll(DEADLINE, "joiner committed as a held learner", || async {
        n1.summary()
            .members
            .iter()
            .any(|m| m.id == d_id && !m.voter)
            && d.convergence_phase() == Phase::Learner
    })
    .await;
    assert!(
        !d.is_voter(),
        "promotion is refused while the voter set is full"
    );

    // Sabotage discovery AND kill the join-leader together: clearing the
    // file-discovery directory proves the joiner cannot lean on discovery, and
    // killing n1 forces a leader change while the joiner is mid-join.
    clear_discovery_dir(discovery_dir.path());
    let n1_id = n1.raft_id();
    n1.kill().await;

    // A live successor is elected among the survivors (n1 is gone).
    poll(DEADLINE, "a live successor leads", || async {
        n2.is_leader() || n3.is_leader()
    })
    .await;

    // The joiner — routing purely from its LOCAL membership view now that
    // discovery is empty and the leader it joined through is dead — finds the
    // successor and keeps re-issuing its idempotent promotion. Once the dead
    // leader has been unreachable for removal_grace, the promotion folds its
    // removal into the same joint change and the joiner becomes a voter.
    poll(
        LONG_DEADLINE,
        "joiner converges to voter via the new leader",
        || async { d.convergence_phase() == Phase::Voter && d.is_voter() },
    )
    .await;
    let survivor = if n2.is_leader() { &n2 } else { &n3 };
    let members = survivor.summary().members;
    assert!(
        members.iter().all(|m| m.id != n1_id),
        "the dead join-leader was retired in the promotion's joint change"
    );
    assert_eq!(
        members.iter().filter(|m| m.voter).count(),
        3,
        "cluster back at its intended size with the joiner seated"
    );

    for mut n in [n2, n3, d] {
        n.graceful_stop().await;
    }
}

/// Finding #3 (formation token on joined replicas). A normally-JOINED replica
/// holds no founding formation record, so `is_initialized()` is true while its
/// recorded token is `None`. `InitializeCluster` against it must be refused as
/// a plain "already initialized" for ANY supplied token — never the idempotent
/// `AlreadyFormed`, and never applying the supplied bootstrap policy (ADR 0037
/// §3). Both paths are asserted: the FOUNDER re-reports already-initialized for
/// its recorded token; the JOINED replica refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joined_replica_without_formation_record_refuses_init() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    // Node 1 forms; node 2 self-joins and becomes a voter with no formation
    // record of its own.
    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("founder-token").await;
    poll(DEADLINE, "node 1 formed", || async {
        node1.is_leader() && node1.is_voter()
    })
    .await;

    let mut node2 = Node::new_with_seeds(2, cluster_id, &ca, &[node1.advertise.clone()]);
    node2.boot().await;
    poll(DEADLINE, "node 2 joins as a voter", || async {
        node2.is_voter()
    })
    .await;

    let op = ca.operator_leaf();

    // Path A — the founder holds the recorded token: its exact token re-reports
    // already-initialized success (idempotent formation).
    let mut c1 = admin::admin_channel(&node1.advertise, &ca.pem, &op.cert_pem, &op.key_pem)
        .await
        .expect("dial node 1 admin surface");
    let founder = c1
        .initialize_cluster(pb::InitializeClusterRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            formation_token: "founder-token".into(),
            policy_toml: None,
        })
        .await
        .expect("founder re-init with its recorded token succeeds")
        .into_inner();
    assert!(
        founder.already_initialized,
        "the founder re-reports already-initialized for its recorded token"
    );

    // Path B — the joined replica holds no record: refused for ANY token,
    // never AlreadyFormed, no policy applied.
    let mut c2 = admin::admin_channel(&node2.advertise, &ca.pem, &op.cert_pem, &op.key_pem)
        .await
        .expect("dial node 2 admin surface");
    let status = c2
        .initialize_cluster(pb::InitializeClusterRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            formation_token: "any-token-at-all".into(),
            policy_toml: None,
        })
        .await
        .expect_err("a joined replica with no formation record refuses InitializeCluster");
    assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "plain already-initialized refusal, not a server error: {}",
        status.message()
    );
    assert!(
        status.message().contains("already initialized"),
        "refusal names the already-initialized condition: {}",
        status.message()
    );

    node2.graceful_stop().await;
    node1.graceful_stop().await;
}

/// Finding #4 (concurrent InitializeCluster). Two requests with DIFFERENT
/// tokens race against one parked daemon. The formation lock plus the storage
/// layer's "unset or equal" token guard let exactly one win; the loser is
/// refused `ConflictingFormationToken` naming the winner's token, never a false
/// idempotent success from the loser's `raft.initialize` `NotAllowed` (ADR 0037
/// §3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_init_different_tokens_one_wins() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    // A parked node with NO discovery seeds: the probe guard finds nothing, so
    // both requests reach `form` and race there.
    let mut node = Node::new(1, cluster_id, &ca);
    node.boot().await;
    let target = node.advertise.clone();

    let op = ca.operator_leaf();
    let mut c_alpha = admin::admin_channel(&target, &ca.pem, &op.cert_pem, &op.key_pem)
        .await
        .expect("dial admin surface (alpha)");
    let mut c_beta = admin::admin_channel(&target, &ca.pem, &op.cert_pem, &op.key_pem)
        .await
        .expect("dial admin surface (beta)");

    let req_alpha = pb::InitializeClusterRequest {
        cluster_uuid: cluster_uuid.to_vec(),
        formation_token: "alpha".into(),
        policy_toml: None,
    };
    let req_beta = pb::InitializeClusterRequest {
        cluster_uuid: cluster_uuid.to_vec(),
        formation_token: "beta".into(),
        policy_toml: None,
    };

    let (r_alpha, r_beta) = tokio::join!(
        c_alpha.initialize_cluster(req_alpha),
        c_beta.initialize_cluster(req_beta),
    );

    // Exactly one wins; the loser is refused, naming the winner's token.
    let (winner_token, loser_status) = match (r_alpha, r_beta) {
        (Ok(_), Err(status)) => ("alpha", status),
        (Err(status), Ok(_)) => ("beta", status),
        (Ok(_), Ok(_)) => panic!("both init calls succeeded — formation forked into two histories"),
        (Err(a), Err(b)) => panic!("both init calls failed — none formed: {a:?} / {b:?}"),
    };
    assert_eq!(
        loser_status.code(),
        Code::FailedPrecondition,
        "the loser gets a conflict, not a server error: {}",
        loser_status.message()
    );
    let refusal =
        pb::MembershipRefusal::decode(loser_status.details()).expect("decodable refusal detail");
    match refusal.reason {
        Some(pb::membership_refusal::Reason::ConflictingFormationToken(c)) => {
            assert_eq!(
                c.recorded_token, winner_token,
                "the refusal names the winning token"
            );
        }
        other => panic!("expected ConflictingFormationToken, got {other:?}"),
    }

    node.graceful_stop().await;
}
