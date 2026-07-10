//! End-to-end multi-node coordinator test: real mTLS Raft transport, real
//! bootstrap/join, real admin gRPC surface, on localhost.
//!
//! The scenario walks a cluster through its whole membership lifecycle —
//! bootstrap, learner-join + promotion (ADR 0016), converged commits, a leader
//! kill with re-election, a follower restart-from-disk, and finally a full
//! replace of a dead voter by a fresh learner that resyncs via install-snapshot
//! (ADR 0016 end to end). A second, fast test pins the ADR 0016 identity matrix.
//!
//! Everything is driven through the same code paths the daemon uses
//! (`config::load` + `bootstrap::bootstrap`, the `admin` client helpers, the
//! `Consensus` seam). The harness lives in `common/`.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use coppice_consensus::{Consensus, OpenraftConsensus};
use coppice_coordinator::admin;
use coppice_coordinator::config::CliOverrides;
use coppice_state::command::BumpClusterVersion;
use coppice_state::Command;
use coppice_core::id::ClusterId;

use common::{poll, wait_converged, Ca, Leaf, Node};

/// Generous per-wait deadline. Well above the 300ms election timeout, small
/// enough that a genuine hang fails the test rather than the 2-minute harness
/// timeout.
const DEADLINE: Duration = Duration::from_secs(20);

fn uuid_bytes(u: ClusterId) -> [u8; 16] {
    *u.0.as_bytes()
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

/// Propose one `BumpClusterVersion` through a node's consensus seam and assert
/// it applied Ok; returns the Raft log index it committed at.
async fn propose_bump(node: &Node, to: u32) -> u64 {
    let applied = node
        .consensus()
        .propose(Command::BumpClusterVersion(BumpClusterVersion {
            to,
            bumped_at_us: to as i64,
        }))
        .await
        .unwrap_or_else(|e| panic!("propose bump to={to} failed: {e:?}"));
    assert!(
        applied.outcome.is_ok(),
        "bump to={to} was rejected: {:?}",
        applied.outcome
    );
    applied.log_index
}

/// Wait until one of `candidates` reports itself leader; return its index.
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

/// Poll the leader's admin `cluster_status` RPC until every id in `expect` is a
/// member AND its leader-observed replication has caught up (near-zero lag).
async fn wait_learners_caught_up(
    admin_ca: &[u8],
    admin_cert: &[u8],
    admin_key: &[u8],
    leader_target: &str,
    cluster_uuid: [u8; 16],
    expect: &[u64],
    deadline: Duration,
) {
    let mut client = admin::admin_channel(leader_target, admin_ca, admin_cert, admin_key)
        .await
        .expect("dial admin surface");
    let start = Instant::now();
    loop {
        let status = admin::cluster_status(&mut client, cluster_uuid)
            .await
            .expect("cluster_status RPC");

        let members: BTreeSet<u64> = status
            .membership
            .as_ref()
            .map(|m| m.members.iter().map(|x| x.node_id).collect())
            .unwrap_or_default();
        let all_present = expect.iter().all(|id| members.contains(id));
        let all_matched = expect.iter().all(|id| {
            status
                .replication
                .iter()
                .find(|r| r.node_id == *id)
                .map(|r| status.last_applied_index.saturating_sub(r.matched_index) <= 4)
                .unwrap_or(false)
        });

        if all_present && all_matched {
            return;
        }
        if start.elapsed() >= deadline {
            panic!("learners {expect:?} did not appear+catch up in {deadline:?}: {status:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Current voter-id set from a node's in-process cluster summary.
fn voter_ids(node: &Node) -> BTreeSet<u64> {
    node.summary()
        .members
        .iter()
        .filter(|m| m.voter)
        .map(|m| m.id)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_lifecycle() {
    init_tracing();

    let ca = Ca::new();
    // A dedicated admin-client identity signed by the same CA (ADR 0011): the
    // test acts as an operator presenting a valid client cert.
    let admin_leaf: Leaf = ca.leaf();
    let cluster_id = ClusterId::new();
    let cluster_uuid = uuid_bytes(cluster_id);

    // Three replicas, ids 1..=3, each its own tempdir/port/cert.
    let mut nodes: Vec<Node> = (1..=3).map(|id| Node::new(id, cluster_id, &ca)).collect();

    // -- Step 1: bootstrap node 1, wait until it is leader. -----------------
    nodes[0]
        .boot(CliOverrides {
            bootstrap: true,
            join: false,
        })
        .await;
    let leader0 = wait_for_leader(&nodes, &[0], DEADLINE).await;
    assert_eq!(leader0, 0, "the bootstrap node must be the initial leader");

    // -- Step 2: join nodes 2 and 3 as learners, then promote to voters. ----
    for i in [1usize, 2] {
        nodes[i]
            .boot(CliOverrides {
                bootstrap: false,
                join: true,
            })
            .await;
    }

    let target = nodes[0].advertise.clone();
    {
        let mut client =
            admin::admin_channel(&target, &ca.pem, &admin_leaf.cert_pem, &admin_leaf.key_pem)
                .await
                .expect("dial leader admin surface");
        for i in [1usize, 2] {
            admin::add_learner(
                &mut client,
                cluster_uuid,
                nodes[i].id,
                nodes[i].advertise.clone(),
            )
            .await
            .unwrap_or_else(|e| panic!("add-learner {} failed: {e:#}", nodes[i].id));
        }
    }

    wait_learners_caught_up(
        &ca.pem,
        &admin_leaf.cert_pem,
        &admin_leaf.key_pem,
        &target,
        cluster_uuid,
        &[nodes[1].id, nodes[2].id],
        DEADLINE,
    )
    .await;

    {
        let mut client =
            admin::admin_channel(&target, &ca.pem, &admin_leaf.cert_pem, &admin_leaf.key_pem)
                .await
                .expect("dial leader admin surface");
        // No removal: pure promotions. The helper polls the catch-up gate.
        for i in [1usize, 2] {
            admin::promote_voter(&mut client, cluster_uuid, nodes[i].id, None, DEADLINE)
                .await
                .unwrap_or_else(|e| panic!("promote {} failed: {e:#}", nodes[i].id));
        }
    }

    poll(DEADLINE, "three voters in membership", || async {
        voter_ids(&nodes[0]).len() == 3
    })
    .await;

    // -- Step 3: converged commits across all three replicas. ---------------
    let mut last_index = 0;
    for to in 1..=20u32 {
        last_index = propose_bump(&nodes[0], to).await;
    }
    for node in &nodes {
        wait_converged(
            node.views(),
            last_index,
            20,
            DEADLINE,
            &format!("node {} converges to cv=20", node.id),
        )
        .await;
    }

    // -- Step 4: kill the leader, re-elect, keep committing. ----------------
    let dead_idx = wait_for_leader(&nodes, &[0, 1, 2], DEADLINE).await;
    let survivors: Vec<usize> = (0..3).filter(|&i| i != dead_idx).collect();
    nodes[dead_idx].kill().await;

    let new_leader = wait_for_leader(&nodes, &survivors, DEADLINE).await;
    for to in 21..=25u32 {
        last_index = propose_bump(&nodes[new_leader], to).await;
    }
    for &i in &survivors {
        wait_converged(
            nodes[i].views(),
            last_index,
            25,
            DEADLINE,
            &format!("survivor {} converges to cv=25", nodes[i].id),
        )
        .await;
    }

    // -- Step 5: gracefully stop a follower, keep proposing, restart it. ----
    let follower = *survivors
        .iter()
        .find(|&&i| i != new_leader)
        .expect("a surviving follower");
    nodes[follower].graceful_stop().await;

    // With one voter already dead (step 4) and this follower down, only the
    // leader remains of three voters — below quorum. openraft 0.9 does NOT step
    // a leader down on quorum loss (it only steps down on a membership change
    // that removes it), so these proposals are appended by the still-leader and
    // pend, uncommitted, until quorum is restored. We issue them off-task so
    // they can resolve after the follower rejoins.
    let leader_consensus: Arc<OpenraftConsensus> = nodes[new_leader].consensus();
    let proposer = tokio::spawn(async move {
        let mut idx = 0;
        for to in 26..=27u32 {
            let applied = leader_consensus
                .propose(Command::BumpClusterVersion(BumpClusterVersion {
                    to,
                    bumped_at_us: to as i64,
                }))
                .await
                .unwrap_or_else(|e| panic!("offline-window bump to={to} failed: {e:?}"));
            assert!(
                applied.outcome.is_ok(),
                "offline-window bump to={to} rejected"
            );
            idx = applied.log_index;
        }
        idx
    });

    // Restart the follower from its own disk (Restart intent: neither flag).
    nodes[follower]
        .boot(CliOverrides {
            bootstrap: false,
            join: false,
        })
        .await;

    last_index = proposer.await.expect("offline-window proposer joins");
    for &i in &survivors {
        wait_converged(
            nodes[i].views(),
            last_index,
            27,
            DEADLINE,
            &format!("node {} converges to cv=27 after restart", nodes[i].id),
        )
        .await;
    }

    // -- Step 6: replace the dead voter with a fresh learner (install-snapshot).
    // Force the snapshot resync path: with snapshot_keep_log_entries = 0, a
    // triggered snapshot purges the log behind it, so a brand-new learner
    // CANNOT catch up by replaying from index 1 — it must install the snapshot.
    // A fresh node 4 converging therefore proves install-snapshot ran end to end.
    let leader_idx = wait_for_leader(&nodes, &survivors, DEADLINE).await;
    nodes[leader_idx]
        .consensus()
        .trigger_snapshot()
        .await
        .expect("trigger snapshot");
    for to in 28..=30u32 {
        last_index = propose_bump(&nodes[leader_idx], to).await;
    }
    // A second snapshot after the new entries guarantees the purge window has
    // advanced past what a fresh learner could replay from scratch.
    nodes[leader_idx]
        .consensus()
        .trigger_snapshot()
        .await
        .expect("re-trigger snapshot");

    let mut node4 = Node::new(4, cluster_id, &ca);
    node4
        .boot(CliOverrides {
            bootstrap: false,
            join: true,
        })
        .await;
    let dead_id = nodes[dead_idx].id;

    let leader_target = nodes[leader_idx].advertise.clone();
    {
        let mut client = admin::admin_channel(
            &leader_target,
            &ca.pem,
            &admin_leaf.cert_pem,
            &admin_leaf.key_pem,
        )
        .await
        .expect("dial leader admin surface");
        admin::add_learner(&mut client, cluster_uuid, node4.id, node4.advertise.clone())
            .await
            .expect("add-learner node 4");
    }

    wait_learners_caught_up(
        &ca.pem,
        &admin_leaf.cert_pem,
        &admin_leaf.key_pem,
        &leader_target,
        cluster_uuid,
        &[node4.id],
        DEADLINE,
    )
    .await;

    {
        let mut client = admin::admin_channel(
            &leader_target,
            &ca.pem,
            &admin_leaf.cert_pem,
            &admin_leaf.key_pem,
        )
        .await
        .expect("dial leader admin surface");
        // Promote node 4 and drop the dead node in one joint change (ADR 0016 step 3).
        admin::promote_voter(&mut client, cluster_uuid, node4.id, Some(dead_id), DEADLINE)
            .await
            .expect("promote node 4, remove dead node");
    }

    poll(
        DEADLINE,
        "membership = {leader, follower, node4}, no dead node",
        || async {
            let voters = voter_ids(&nodes[leader_idx]);
            voters.contains(&node4.id) && !voters.contains(&dead_id) && voters.len() == 3
        },
    )
    .await;

    wait_converged(
        node4.views(),
        last_index,
        30,
        DEADLINE,
        "fresh node 4 converges via install-snapshot",
    )
    .await;

    // Final bump: node 4, now a voter, must apply it too.
    let final_index = propose_bump(&nodes[leader_idx], 31).await;
    wait_converged(
        node4.views(),
        final_index,
        31,
        DEADLINE,
        "node 4 applies the final bump",
    )
    .await;

    // -- Step 6b: the resync's durable artifact. Install-snapshot streams the
    // ADR 0018 container disk-to-disk (the `SnapshotData` binding is a
    // file-backed handle; neither side holds the container in memory), so the
    // learner must have adopted the leader-built file itself: same snapshot id,
    // byte-identical content, a complete footer-valid container behind its
    // manifest pointer — and no leftover receive spool, which is deleted once
    // the copy is adopted (a crash mid-receive would leave it for the
    // recovery sweep instead).
    {
        let snap_files = |dir: &std::path::Path| -> Vec<std::path::PathBuf> {
            let mut files: Vec<_> = std::fs::read_dir(dir)
                .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
                .map(|entry| entry.expect("snap dir entry").path())
                .filter(|p| p.extension().is_some_and(|ext| ext == "snap"))
                .collect();
            files.sort();
            files
        };
        let leader_snaps = snap_files(&nodes[leader_idx].data_dir().join("snap"));
        let node4_snap_dir = node4.data_dir().join("snap");
        let node4_snaps = snap_files(&node4_snap_dir);
        assert_eq!(leader_snaps.len(), 1, "leader holds one current snapshot");
        assert_eq!(node4_snaps.len(), 1, "node 4 holds one current snapshot");
        assert_eq!(
            node4_snaps[0].file_name(),
            leader_snaps[0].file_name(),
            "node 4 must have adopted the leader-built snapshot (same id)"
        );

        let leader_bytes = std::fs::read(&leader_snaps[0]).expect("read leader snapshot");
        let node4_bytes = std::fs::read(&node4_snaps[0]).expect("read node 4 snapshot");
        assert_eq!(
            leader_bytes, node4_bytes,
            "the container must arrive disk-to-disk unchanged"
        );

        // Container-level validity: header, every section CRC, total CRC,
        // closing magic. The manifest may only ever point at a complete,
        // durably renamed container (ADR 0017/0018).
        coppice_consensus::storage::raw::validate_container(&node4_snaps[0], &node4_bytes)
            .expect("node 4's adopted snapshot must validate end to end");

        assert!(
            !node4_snap_dir.join("receiving.tmp").exists(),
            "the receive spool must be deleted once the snapshot is adopted"
        );
    }

    // -- Step 7: graceful shutdown of all remaining nodes. ------------------
    node4.graceful_stop().await;
    for &i in &survivors {
        nodes[i].graceful_stop().await;
    }
}

/// The ADR 0016 startup identity matrix — fast, no cluster needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identity_matrix() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // (a) Restart intent on an empty directory must fail-stop, pointing the
    //     operator at the intent flags.
    {
        let node = Node::new(10, cluster_id, &ca);
        let err = node
            .try_boot(CliOverrides::default())
            .await
            .expect_err("Restart on an empty dir must refuse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--bootstrap") && msg.contains("--join"),
            "error should point at the intent flags, got: {msg}"
        );
    }

    // (b) Bootstrap intent on an already-initialized directory must fail-stop.
    {
        let mut node = Node::new(11, cluster_id, &ca);
        node.boot(CliOverrides {
            bootstrap: true,
            join: false,
        })
        .await;
        node.graceful_stop().await;

        let err = node
            .try_boot(CliOverrides {
                bootstrap: true,
                join: false,
            })
            .await
            .expect_err("Bootstrap on an initialized dir must refuse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("already initialized"),
            "error should mention the directory is already initialized, got: {msg}"
        );
    }

    // (c) Restart with a DIFFERENT cluster_id than the disk was stamped with
    //     must fail-stop on the identity mismatch.
    {
        let mut node = Node::new(12, cluster_id, &ca);
        node.boot(CliOverrides {
            bootstrap: true,
            join: false,
        })
        .await;
        node.graceful_stop().await;

        node.rewrite_cluster_id(ClusterId::new());
        let err = node
            .try_boot(CliOverrides::default())
            .await
            .expect_err("Restart with a different cluster_id must refuse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("identity") || msg.contains("cluster"),
            "error should mention the identity/cluster mismatch, got: {msg}"
        );
    }
}
