//! The ADR 0037 §4 operator break-glass `admin set-address` verified repoint,
//! driven through the real mTLS admin surface.
//!
//! The contract under test (ADR 0037 §4, final paragraph, and §6):
//! - operator-credential ONLY — a machine or agent leaf is refused before any
//!   dial (`set_address_refuses_machine_and_agent_certs`);
//! - the leader commits only after VERIFYING the new endpoint: its serving-cert
//!   subject must equal the machine identity already bound to the target, and
//!   `ProbeCluster` there must report the target's stamped node id — a claimed
//!   id behind the wrong subject is refused
//!   (`set_address_refuses_endpoint_with_wrong_machine_identity`);
//! - unknown id → refused, no silent creation (`set_address_unknown_id_refused`);
//! - idempotent: new addr == current addr → Ok no-op (`set_address_same_addr_noop`);
//! - the verified repoint updates only the target's address and replication
//!   resumes at the new address (`set_address_repoints_and_replication_resumes`).

mod common;

use std::time::Duration;

use tonic::Code;

use coppice_consensus::Consensus;
use coppice_coordinator::admin;
use coppice_core::id::ClusterId;
use coppice_core::time::Timestamp;
use coppice_proto::pb::raft::v1 as pb;
use coppice_state::command::BumpClusterVersion;
use coppice_state::Command;

use common::{poll, Ca, Node};

const DEADLINE: Duration = Duration::from_secs(30);

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Dial `target`'s admin surface presenting `leaf` as the client certificate.
async fn dial(
    ca: &Ca,
    leaf: &common::Leaf,
    target: &str,
) -> coppice_net::admin::Client<tonic::transport::Channel> {
    admin::admin_channel(target, &ca.pem, &leaf.cert_pem, &leaf.key_pem)
        .await
        .expect("dial admin surface")
}

/// Operator-only (ADR 0037 §4/§6): a coordinator *machine* leaf and a plain
/// (agent) leaf are both refused `SetNodeAddress` with PERMISSION_DENIED, before
/// any endpoint dial. Only the operator profile may drive the repoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_address_refuses_machine_and_agent_certs() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("set-address-authz-formation").await;
    poll(DEADLINE, "node 1 formed", || async {
        node1.is_leader() && node1.is_voter()
    })
    .await;
    let node1_id = node1.raft_id();
    let target = node1.advertise.clone();

    // A machine leaf (OU=coppice-coordinator) may drive self-join verbs but NOT
    // set-address — repointing a voter can split-brain, so it is operator-only.
    let machine = ca.machine_leaf("coord-1");
    let mut client = dial(&ca, &machine, &target).await;
    let status = client
        .set_node_address(pb::SetNodeAddressRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            node_id: node1_id,
            address: "localhost:1".to_string(),
        })
        .await
        .expect_err("machine cert must be refused set-address");
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "machine cert refused: {}",
        status.message()
    );

    // A plain leaf (no OU marker → agent) is likewise refused.
    let agent = ca.leaf();
    let mut client = dial(&ca, &agent, &target).await;
    let status = client
        .set_node_address(pb::SetNodeAddressRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            node_id: node1_id,
            address: "localhost:1".to_string(),
        })
        .await
        .expect_err("agent cert must be refused set-address");
    assert_eq!(status.code(), Code::PermissionDenied);

    node1.graceful_stop().await;
}

/// Unknown id → refused (ADR 0037 §4): set-address never silently creates a
/// node, even for the operator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_address_unknown_id_refused() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("set-address-unknown-formation").await;
    poll(DEADLINE, "node 1 formed", || async { node1.is_leader() }).await;

    let operator = ca.operator_leaf();
    let mut client = dial(&ca, &operator, &node1.advertise).await;
    let status = client
        .set_node_address(pb::SetNodeAddressRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            node_id: 999_999,
            address: "localhost:2".to_string(),
        })
        .await
        .expect_err("unknown id must be refused");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("not in membership"),
        "names the missing node: {}",
        status.message()
    );

    node1.graceful_stop().await;
}

/// Idempotent no-op (ADR 0037 §4): repointing a node to the address it already
/// holds returns Ok with no dial and no committed change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_address_same_addr_noop() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("set-address-noop-formation").await;
    poll(DEADLINE, "node 1 formed", || async {
        node1.is_leader() && node1.is_voter()
    })
    .await;
    let node1_id = node1.raft_id();

    // node 1's own membership address is its advertised addr; repointing to it
    // is an idempotent success.
    let current = node1
        .summary()
        .members
        .into_iter()
        .find(|m| m.id == node1_id)
        .expect("node 1 in its own membership")
        .addr;

    let operator = ca.operator_leaf();
    let mut client = dial(&ca, &operator, &node1.advertise).await;
    client
        .set_node_address(pb::SetNodeAddressRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            node_id: node1_id,
            address: current.clone(),
        })
        .await
        .expect("same-addr repoint is a no-op success");

    // Address unchanged.
    let after = node1
        .summary()
        .members
        .into_iter()
        .find(|m| m.id == node1_id)
        .expect("node 1 still in membership")
        .addr;
    assert_eq!(after, current, "no-op left the address unchanged");

    node1.graceful_stop().await;
}

/// Endpoint verification refusal (ADR 0037 §4/§6): repointing a target to an
/// address whose serving certificate presents a DIFFERENT machine identity than
/// the one bound to the target is refused. Here the operator points node 2
/// (bound `coord-2`) at node 1's endpoint, which serves `coord-1` — the subject
/// does not match the target's binding, so the leader refuses rather than
/// committing an unverified `SetNodes`.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn set_address_refuses_endpoint_with_wrong_machine_identity() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("set-address-wrong-id-formation").await;
    poll(DEADLINE, "node 1 formed", || async { node1.is_leader() }).await;
    let seeds = [node1.advertise.clone()];
    let mut node2 = Node::new_with_seeds(2, cluster_id, &ca, &seeds);
    node2.boot().await;
    poll(DEADLINE, "two voters", || async {
        node1.summary().members.iter().filter(|m| m.voter).count() == 2
    })
    .await;
    let node2_id = node2.raft_id();

    // Point node 2 (bound coord-2) at node 1's endpoint (serves coord-1).
    let operator = ca.operator_leaf();
    let mut client = dial(&ca, &operator, &node1.advertise).await;
    let status = client
        .set_node_address(pb::SetNodeAddressRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            node_id: node2_id,
            address: node1.advertise.clone(),
        })
        .await
        .expect_err("wrong-machine-identity endpoint must be refused");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("endpoint verification"),
        "refusal names endpoint verification: {}",
        status.message()
    );

    // node 2's membership address was not rewritten.
    let addr = node1
        .summary()
        .members
        .into_iter()
        .find(|m| m.id == node2_id)
        .expect("node 2 in membership")
        .addr;
    assert_eq!(addr, node2.advertise, "address unchanged after refusal");

    node2.graceful_stop().await;
    node1.graceful_stop().await;
}

/// Happy path (ADR 0037 §4): a three-voter cluster keeps quorum while one voter
/// moves. The moved instance is re-bound to a new port (same disk → same stamped
/// identity and machine leaf), the operator repoints it, the leader verifies the
/// new endpoint and commits the `SetNodes`, and replication resumes at the new
/// address — the moved node applies a command proposed after the repoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn set_address_repoints_and_replication_resumes() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    // Form a three-voter cluster so node 1 + node 2 keep quorum when node 3 moves.
    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("set-address-repoint-formation").await;
    poll(DEADLINE, "node 1 formed", || async { node1.is_leader() }).await;
    let seeds = [node1.advertise.clone()];
    let mut node2 = Node::new_with_seeds(2, cluster_id, &ca, &seeds);
    let mut node3 = Node::new_with_seeds(3, cluster_id, &ca, &seeds);
    node2.boot().await;
    node3.boot().await;
    poll(DEADLINE, "three voters", || async {
        node1.summary().members.iter().filter(|m| m.voter).count() == 3
    })
    .await;
    let node3_id = node3.raft_id();
    let old_addr = node3.advertise.clone();

    // node 3 moves: stop it, rebind to a new port, and re-boot from its own disk.
    // The convergence loop no-ops for an existing voter (ADR 0037 §1), so the
    // resumed process just serves its identity at the new address; the leader
    // still points at the OLD address and cannot reach it.
    node3.graceful_stop().await;
    node3.rebind();
    node3.boot().await;
    let new_addr = node3.advertise.clone();
    assert_ne!(new_addr, old_addr, "node 3 moved to a new address");
    assert_eq!(
        node3.raft_id(),
        node3_id,
        "same stamped identity after move"
    );
    poll(DEADLINE, "leader still holds quorum", || async {
        node1.is_leader()
    })
    .await;

    // Operator repoints node 3 to its new address. The leader dials it, matches
    // its serving-cert subject (coord-3) to node 3's binding, confirms the
    // stamped node id by probe, and commits the `SetNodes`.
    let operator = ca.operator_leaf();
    let mut client = dial(&ca, &operator, &node1.advertise).await;
    admin::set_node_address(&mut client, cluster_uuid, node3_id, new_addr.clone())
        .await
        .expect("verified repoint succeeds");

    // The membership record now carries the new address, and the binding/marking
    // are preserved.
    let rec = node1
        .summary()
        .members
        .into_iter()
        .find(|m| m.id == node3_id)
        .expect("node 3 in membership");
    assert_eq!(rec.addr, new_addr, "address repointed");
    assert_eq!(rec.machine_identity, "coord-3", "binding preserved");
    assert!(!rec.superseded, "superseded marking preserved");

    // Replication resumes at the new address: a command proposed on the leader
    // after the repoint reaches node 3.
    node1
        .consensus()
        .propose(Command::BumpClusterVersion(BumpClusterVersion {
            to: 42,
            bumped_at: Timestamp::from_micros(42).expect("in range"),
        }))
        .await
        .expect("propose after repoint");
    let node3_views = node3.views();
    poll(
        DEADLINE,
        "node 3 applies the post-repoint command at its new address",
        || {
            let views = node3_views.clone();
            async move { views.latest().state().cluster_version == 42 }
        },
    )
    .await;

    node3.graceful_stop().await;
    node2.graceful_stop().await;
    node1.graceful_stop().await;
}
