//! The ADR 0037 "new failure modes to test explicitly" (Consequences), driven
//! through the real mTLS admin surface and the self-converging membership loop.
//!
//! Each test is deterministic: it synchronizes with the harness `poll` helpers
//! (no bare sleeps), and where a grace period is load-bearing it configures a
//! short one so overflow removal and stale-learner eviction are reachable inside
//! the test deadline. The map from ADR failure mode to test:
//!
//! - interrupted join (kill mid-join, resume from disk, no duplicate seat)
//!   → `interrupted_join_converges_after_respawn`
//! - replacement in a FULL cluster (predecessor retired, no second overflow
//!   removal) → `replacement_in_full_cluster_retires_predecessor`
//! - replacement in an UNDERFILLED cluster (predecessor retired even though
//!   voters < cluster_size) → `replacement_in_underfilled_cluster_retires_predecessor`
//! - overflow removal folds a dead voter → `overflow_removal_folds_dead_voter`
//! - overflow refused, voter set full / no removable peer
//!   → `overflow_refused_when_all_voters_alive`
//! - two concurrent replacements for one machine identity: one admitted, the
//!   loser enters seat-conflict and converges once the incumbent is gone
//!   → `seat_conflict_loser_converges_after_incumbent_gone`
//! - `formed` true with unreachable voters must fail `?require=healthy`
//!   → `formed_with_unreachable_voters_fails_healthy_gate`
//! - a parked fleet resumes when its cluster reappears in discovery
//!   → `parked_replica_joins_when_discovery_entry_appears`
//!
//! Double-init (probe guard + same/different token) lives in `convergence.rs`
//! (`double_init_against_second_parked_is_refused`, `formation_state_machine`).

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use prost::Message as _;
use tonic::Code;

use coppice_consensus::{Consensus, MemberSummary};
use coppice_coordinator::admin;
use coppice_coordinator::convergence::Phase;
use coppice_coordinator::readyz::{evaluate, ReadyzInputs, Require};
use coppice_core::id::ClusterId;
use coppice_core::time::Timestamp;
use coppice_proto::pb::raft::v1 as pb;
use coppice_state::command::BumpClusterVersion;
use coppice_state::Command;

use common::{poll, Ca, DiscoverySpec, Node, NodeSpec};

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

/// The voter members of a node's own replicated membership view.
fn voters(node: &Node) -> Vec<MemberSummary> {
    node.summary()
        .members
        .into_iter()
        .filter(|m| m.voter)
        .collect()
}

/// The one-seat-per-installation invariant (ADR 0037 §6): no two *active*
/// (non-`superseded`) voters are ever bound to the same machine identity, and
/// the active voter count never exceeds `max`.
///
/// The membership view is sampled repeatedly during a replacement. A retirement
/// is one atomic `ReplaceAllVoters`, but openraft applies it through a *joint*
/// configuration, so the summary transiently unions the old and new voter sets —
/// the retiring predecessor appears alongside its replacement, marked
/// `superseded: true`. That is not a violation: the predecessor's vote is on its
/// way out and is exactly what the `superseded` marking records. The invariant
/// that must never break is that at most one *non-superseded* voter is bound to
/// any machine identity (ADR 0037 §5: "only this joint change actually retires
/// the old vote").
fn assert_seat_invariant(node: &Node, max: usize) {
    let active: Vec<MemberSummary> = voters(node).into_iter().filter(|m| !m.superseded).collect();
    assert!(
        active.len() <= max,
        "active voter count {} exceeded {max}: {active:?}",
        active.len()
    );
    let mut seen = BTreeSet::new();
    for m in &active {
        assert!(
            seen.insert(m.machine_identity.clone()),
            "two active voters bound to one machine identity {:?}: {active:?}",
            m.machine_identity
        );
    }
}

/// A [`NodeSpec`] with static discovery seeded at `seeds` and both grace periods
/// shortened to `grace` (ADR 0037 §5/§6) so overflow/eviction is reachable fast.
fn spec_with_grace(id: u64, cluster_id: ClusterId, seeds: &[String], grace: Duration) -> NodeSpec {
    NodeSpec {
        id,
        cluster_id,
        discovery: DiscoverySpec::Static(seeds.to_vec()),
        removal_grace: Some(grace),
        replacement_grace: Some(grace),
    }
}

/// (a) Interrupted join: a converging replica killed mid-join resumes from its
/// own disk and converges to a voter, with no duplicate seat — the identity is
/// stamped and reused, and `AddLearner` is idempotent (ADR 0037 §4).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_join_converges_after_respawn() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("interrupted-join-formation").await;
    poll(DEADLINE, "node 1 formed", || async {
        node1.is_leader() && node1.is_voter()
    })
    .await;

    // Node 2 begins converging; wait until it has been admitted (its id appears
    // in membership — i.e. AddLearner committed) then KILL it mid-join.
    let mut node2 = Node::new_with_seeds(2, cluster_id, &ca, &[node1.advertise.clone()]);
    node2.boot().await;
    let node2_id = node2.raft_id();
    poll(DEADLINE, "node 2 admitted as a learner", || async {
        node1.summary().members.iter().any(|m| m.id == node2_id)
    })
    .await;
    node2.kill().await;

    // Respawn from the same disk: intent is derived (manifest present → resume),
    // and the SAME identity re-enters the convergence loop.
    node2.boot().await;
    assert_eq!(
        node2.raft_id(),
        node2_id,
        "respawn resumes the same stamped identity, not a fresh mint"
    );
    poll(
        DEADLINE,
        "node 2 converges to voter after respawn",
        || async { node2.is_voter() },
    )
    .await;

    // Exactly two voters, and exactly one seat bound to coord-2 — no duplicate.
    let vs = voters(&node1);
    assert_eq!(vs.len(), 2, "two voters after respawn: {vs:?}");
    assert_eq!(
        vs.iter()
            .filter(|m| m.machine_identity == "coord-2")
            .count(),
        1,
        "exactly one seat for the resumed machine identity: {vs:?}"
    );

    node2.graceful_stop().await;
    node1.graceful_stop().await;
}

/// (b) Replacement in a FULL cluster: three voters, kill one, boot a fresh
/// instance with the SAME machine identity. It is admitted as a replacement and
/// its promotion retires the predecessor in the same joint change — exactly
/// three voters after, the dead id gone, and at no point four voters or two
/// voters bound to one machine (ADR 0037 §5).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replacement_in_full_cluster_retires_predecessor() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // Form a three-voter cluster via self-convergence.
    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("full-replacement-formation").await;
    poll(DEADLINE, "node 1 formed", || async { node1.is_leader() }).await;
    let seeds = [node1.advertise.clone()];
    let mut node2 = Node::new_with_seeds(2, cluster_id, &ca, &seeds);
    let mut node3 = Node::new_with_seeds(3, cluster_id, &ca, &seeds);
    node2.boot().await;
    node3.boot().await;
    poll(DEADLINE, "three voters", || async {
        voters(&node1).len() == 3
    })
    .await;

    // Kill node 3 (its coord-3 seat becomes a dead voter) and boot a fresh
    // instance under the SAME machine identity (a new raft id, empty disk).
    let dead_id = node3.raft_id();
    node3.kill().await;
    let mut node3b = Node::new_with_seeds(3, cluster_id, &ca, &seeds);
    node3b.boot().await;
    let repl_id = node3b.raft_id();
    assert_ne!(repl_id, dead_id, "the replacement is a fresh raft identity");

    // Converge; assert the seat invariant on every sample until it settles.
    poll(
        DEADLINE,
        "replacement is a voter, predecessor retired",
        || async {
            assert_seat_invariant(&node1, 3);
            let vs: BTreeSet<u64> = voters(&node1).iter().map(|m| m.id).collect();
            vs.contains(&repl_id) && !vs.contains(&dead_id) && vs.len() == 3
        },
    )
    .await;

    let vs = voters(&node1);
    assert_eq!(vs.len(), 3);
    assert!(!vs.iter().any(|m| m.id == dead_id), "dead id gone: {vs:?}");
    assert_eq!(
        vs.iter()
            .filter(|m| m.machine_identity == "coord-3")
            .count(),
        1,
        "one coord-3 seat: {vs:?}"
    );

    node3b.graceful_stop().await;
    node2.graceful_stop().await;
    node1.graceful_stop().await;
}

/// (c) Replacement in an UNDERFILLED cluster: two voters of `cluster_size` 3,
/// replace one by a same-machine-identity join. The promotion retires the
/// predecessor even though voters < cluster_size — the one-seat-per-machine
/// invariant is enforced by the promotion itself, not by cardinality
/// (ADR 0037 §5).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replacement_in_underfilled_cluster_retires_predecessor() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // Form a two-voter cluster (cluster_size stays the default 3).
    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("underfilled-replacement-formation").await;
    poll(DEADLINE, "node 1 formed", || async { node1.is_leader() }).await;
    let seeds = [node1.advertise.clone()];
    let mut node2 = Node::new_with_seeds(2, cluster_id, &ca, &seeds);
    node2.boot().await;
    poll(DEADLINE, "two voters", || async {
        voters(&node1).len() == 2
    })
    .await;

    // The predecessor stays alive (both voters are needed for quorum in a
    // two-voter cluster), but stops converging so it will not war for the seat
    // once the leader retires it — modelling a decommissioned instance.
    let dead_id = node2.raft_id();
    node2.stop_convergence();

    // A fresh coord-2 instance self-joins as a replacement.
    let mut node2b = Node::new_with_seeds(2, cluster_id, &ca, &seeds);
    node2b.boot().await;
    let repl_id = node2b.raft_id();

    poll(
        DEADLINE,
        "replacement retires predecessor, still 2 voters",
        || async {
            assert_seat_invariant(&node1, 3);
            let vs: BTreeSet<u64> = voters(&node1).iter().map(|m| m.id).collect();
            vs.contains(&repl_id) && !vs.contains(&dead_id) && vs.len() == 2
        },
    )
    .await;

    let vs = voters(&node1);
    assert_eq!(vs.len(), 2, "still underfilled, not grown: {vs:?}");
    assert_eq!(
        vs.iter()
            .filter(|m| m.machine_identity == "coord-2")
            .count(),
        1,
        "one coord-2 seat, predecessor retired: {vs:?}"
    );

    node2b.graceful_stop().await;
    node2.graceful_stop().await;
    node1.graceful_stop().await;
}

/// (d) Overflow removal — the qualifying case. Three voters, one killed dead; a
/// NEW machine identity joins, and with a short `removal_grace` the dead voter
/// qualifies, so its removal is folded into the promotion (ADR 0037 §5).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn overflow_removal_folds_dead_voter() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let grace = Duration::from_millis(800);

    // The LEADER evaluates overflow removal against its own `removal_grace`, so
    // every replica carries the short grace (the config is identical in shape).
    let mut node1 = Node::with_spec(spec_with_grace(1, cluster_id, &[], grace), &ca);
    node1.boot().await;
    node1.form("overflow-formation").await;
    poll(DEADLINE, "node 1 formed", || async { node1.is_leader() }).await;
    let seeds = [node1.advertise.clone()];
    let mut node2 = Node::with_spec(spec_with_grace(2, cluster_id, &seeds, grace), &ca);
    let mut node3 = Node::with_spec(spec_with_grace(3, cluster_id, &seeds, grace), &ca);
    node2.boot().await;
    node3.boot().await;
    poll(DEADLINE, "three voters", || async {
        voters(&node1).len() == 3
    })
    .await;

    // Kill node 3 dead; a genuinely NEW machine identity (coord-4) joins.
    let dead_id = node3.raft_id();
    node3.kill().await;
    let mut node4 = Node::with_spec(spec_with_grace(4, cluster_id, &seeds, grace), &ca);
    node4.boot().await;
    let new_id = node4.raft_id();

    // Keep the cluster non-idle while node 4 converges. `removal_grace` is
    // "no replication PROGRESS for the grace" — the leader's own observation —
    // and `matched` only advances when there is new log traffic. A real
    // coordinator applies a steady stream of commands (agent heartbeats, job
    // events), so a live follower's progress keeps advancing while a dead one's
    // freezes; an idle test cluster would instead freeze *every* follower's
    // progress and make a live-but-idle voter indistinguishable from a dead one.
    // A background proposer reproduces that steady traffic so only the genuinely
    // dead node 3 qualifies for overflow removal.
    let proposer_consensus = node1.consensus();
    let proposer = tokio::spawn(async move {
        for to in 1.. {
            let _ = proposer_consensus
                .propose(Command::BumpClusterVersion(BumpClusterVersion {
                    to,
                    bumped_at: Timestamp::from_micros(to as i64).expect("in range"),
                }))
                .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Its promotion is refused (voter set would be 4) until `removal_grace`
    // elapses and the dead voter qualifies; then the removal folds in and node 4
    // becomes the third voter. The convergence loop retries across that window.
    poll(DEADLINE, "overflow folds the dead voter", || async {
        let vs: BTreeSet<u64> = voters(&node1).iter().map(|m| m.id).collect();
        vs.contains(&new_id) && !vs.contains(&dead_id) && vs.len() == 3
    })
    .await;
    proposer.abort();

    let vs = voters(&node1);
    assert_eq!(vs.len(), 3, "exactly three voters: {vs:?}");
    assert!(
        !vs.iter().any(|m| m.id == dead_id),
        "dead voter removed: {vs:?}"
    );

    node4.graceful_stop().await;
    node2.graceful_stop().await;
    node1.graceful_stop().await;
}

/// (d) Overflow removal — the refusal case. Three live voters and a fourth NEW
/// identity joins: no voter qualifies as dead, so promotion is refused with the
/// machine-readable `no removable peer` reason, and the cluster never grows to
/// four voters. The joiner keeps polling as a learner (ADR 0037 §5).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn overflow_refused_when_all_voters_alive() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let cluster_uuid = *cluster_id.0.as_bytes();

    let mut node1 = Node::new(1, cluster_id, &ca);
    node1.boot().await;
    node1.form("overflow-refusal-formation").await;
    poll(DEADLINE, "node 1 formed", || async { node1.is_leader() }).await;
    let seeds = [node1.advertise.clone()];
    let mut node2 = Node::new_with_seeds(2, cluster_id, &ca, &seeds);
    let mut node3 = Node::new_with_seeds(3, cluster_id, &ca, &seeds);
    node2.boot().await;
    node3.boot().await;
    poll(DEADLINE, "three live voters", || async {
        voters(&node1).len() == 3
    })
    .await;

    // A fourth, brand-new identity self-joins as a learner and catches up, but
    // can never be promoted (no seat is free, no voter is dead).
    let mut node4 = Node::new_with_seeds(4, cluster_id, &ca, &seeds);
    node4.boot().await;
    let new_id = node4.raft_id();
    poll(DEADLINE, "node 4 admitted as a learner", || async {
        node1.summary().members.iter().any(|m| m.id == new_id)
    })
    .await;

    // Drive the promotion directly under node 4's own machine cert to observe
    // the machine-readable refusal (the convergence loop gets the same refusal
    // and simply keeps polling).
    let leaf = ca.machine_leaf(&node4.machine);
    let target = node1.advertise.clone();
    let mut client = admin::admin_channel(&target, &ca.pem, &leaf.cert_pem, &leaf.key_pem)
        .await
        .expect("dial leader admin surface");
    // The learner may still be catching up; retry until the promotion is decided
    // (either the catch-up "behind" retryable status or the terminal refusal).
    let refusal = poll_promotion_refusal(&mut client, cluster_uuid, new_id).await;
    let decoded = pb::MembershipRefusal::decode(refusal.details()).expect("decodable refusal");
    match decoded.reason {
        Some(pb::membership_refusal::Reason::PromotionRefused(pr)) => {
            assert_eq!(
                pr.reason(),
                pb::promotion_refused::Reason::NoRemovablePeer,
                "no dead voter qualifies for overflow removal"
            );
        }
        other => panic!("expected PromotionRefused/NoRemovablePeer, got {other:?}"),
    }

    // The cluster never grew to four voters, and node 4 is not a voter.
    assert_eq!(voters(&node1).len(), 3, "voter set stayed at three");
    assert!(!node4.is_voter(), "the refused joiner never became a voter");

    node4.graceful_stop().await;
    node3.graceful_stop().await;
    node2.graceful_stop().await;
    node1.graceful_stop().await;
}

/// Poll `PromoteVoter` under a machine cert until it returns a terminal refusal
/// (not the retryable "learner behind" catch-up status), and return that status.
async fn poll_promotion_refusal(
    client: &mut coppice_net::admin::Client<tonic::transport::Channel>,
    cluster_uuid: [u8; 16],
    promote: u64,
) -> tonic::Status {
    let start = std::time::Instant::now();
    loop {
        let status = client
            .promote_voter(pb::PromoteVoterRequest {
                cluster_uuid: cluster_uuid.to_vec(),
                promote_node_id: promote,
                remove_node_id: None,
            })
            .await
            .expect_err("promotion must be refused, never succeed");
        // "behind" is the retryable catch-up status (ADR 0016); anything else is
        // the terminal decision we are waiting for.
        if !(status.code() == Code::FailedPrecondition && status.message().contains("behind")) {
            return status;
        }
        if start.elapsed() >= DEADLINE {
            panic!(
                "promotion never reached a terminal refusal: {}",
                status.message()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// (e) Seat conflict: two concurrent fresh learners with the SAME machine
/// identity race for one seat. One is admitted; the other is refused
/// `MachineSeatPending` and enters the `SeatConflict` phase. After the incumbent
/// is killed and `replacement_grace` passes, the loser converges (ADR 0037 §6).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn seat_conflict_loser_converges_after_incumbent_gone() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let grace = Duration::from_millis(1000);

    // A two-voter base {coord-1, coord-3} so that killing the coord-2 incumbent
    // later still leaves a quorum.
    let mut node1 = Node::with_spec(spec_with_grace(1, cluster_id, &[], grace), &ca);
    node1.boot().await;
    node1.form("seat-conflict-formation").await;
    poll(DEADLINE, "node 1 formed", || async { node1.is_leader() }).await;
    let seeds = [node1.advertise.clone()];
    let mut node3 = Node::with_spec(spec_with_grace(3, cluster_id, &seeds, grace), &ca);
    node3.boot().await;
    poll(DEADLINE, "two voters", || async {
        voters(&node1).len() == 2
    })
    .await;

    // Two fresh instances under the SAME machine identity coord-2 race.
    let mut a = Node::with_spec(spec_with_grace(2, cluster_id, &seeds, grace), &ca);
    let mut b = Node::with_spec(spec_with_grace(2, cluster_id, &seeds, grace), &ca);
    a.boot().await;
    b.boot().await;

    // Exactly one wins the seat (becomes a voter); the other is in SeatConflict.
    poll(
        DEADLINE,
        "one wins the seat, the other is in seat-conflict",
        || async {
            (a.is_voter() && b.convergence_phase() == Phase::SeatConflict)
                || (b.is_voter() && a.convergence_phase() == Phase::SeatConflict)
        },
    )
    .await;
    let (winner, loser): (&mut Node, &mut Node) = if a.is_voter() {
        (&mut a, &mut b)
    } else {
        (&mut b, &mut a)
    };
    let winner_id = winner.raft_id();
    let loser_id = loser.raft_id();
    assert_eq!(
        loser.convergence_phase(),
        Phase::SeatConflict,
        "the loser watches without resubmitting while the incumbent is live"
    );

    // The incumbent disappears; after `replacement_grace` the loser retries and
    // converges as the replacement, retiring the dead incumbent's seat in the
    // same joint change — wait for that change to fully settle (loser in, dead
    // incumbent out, back to three voters), not just for the loser's vote to
    // appear mid-joint.
    winner.kill().await;
    poll(
        DEADLINE,
        "loser converges once the incumbent is gone",
        || async {
            assert_seat_invariant(&node1, 3);
            let vs: BTreeSet<u64> = voters(&node1).iter().map(|m| m.id).collect();
            vs.contains(&loser_id) && !vs.contains(&winner_id) && vs.len() == 3
        },
    )
    .await;

    let vs: BTreeSet<u64> = voters(&node1).iter().map(|m| m.id).collect();
    assert!(vs.contains(&loser_id), "loser is a voter: {vs:?}");
    assert!(
        !vs.contains(&winner_id),
        "the dead incumbent seat is gone: {vs:?}"
    );
    assert_eq!(vs.len(), 3, "back to three voters: {vs:?}");
    assert_seat_invariant(&node1, 3);

    // The winner was killed above (its `booted` is already taken), so stop only
    // the loser and the base cluster; the winner's Node drops with the fn.
    loser.graceful_stop().await;
    node3.graceful_stop().await;
    node1.graceful_stop().await;
}

/// (g) `formed` true with unreachable voters must fail `?require=healthy` while
/// still passing `?require=formed` (ADR 0037 §7). Asserted through the pure
/// `evaluate` gate the HTTP handler uses, with the exact inputs a leader of a
/// fully-enumerated three-voter cluster produces when one voter is unreachable:
/// membership cardinality is met (3 voters) but live redundancy is not
/// (`voters_live` 2 < 3). The 10s health-stability window is deliberately
/// side-stepped by evaluating the pure path rather than a live cluster over HTTP.
#[test]
fn formed_with_unreachable_voters_fails_healthy_gate() {
    // A leader that sees all three seats in membership but can only reach two of
    // them: formed (cardinality) but not healthy (redundancy).
    let inputs = ReadyzInputs {
        cluster_uuid: [1u8; 16],
        node_id: Some(1),
        instance_uuid: [2u8; 16],
        phase: Phase::Voter,
        leader: Some(1),
        is_leader: true,
        applied_index: 100,
        replication_lag: None,
        voters: 3,
        voters_live: Some(2),
        cluster_size: 3,
        node_ready: true,
        // The leader knows its health (it is the leader), so this is a definite
        // "not sustained", never `health_unknown`.
        healthy_sustained: Some(false),
    };

    // Plain node readiness: the leader is a caught-up voter → 200.
    assert_eq!(
        evaluate(&inputs, Require::Node).0,
        axum::http::StatusCode::OK
    );

    // `?require=formed`: membership cardinality met (3 ≥ 3) → 200.
    let (code, body) = evaluate(&inputs, Require::Formed);
    assert_eq!(
        code,
        axum::http::StatusCode::OK,
        "formed despite an unreachable voter"
    );
    assert!(body.formed);

    // `?require=healthy`: live redundancy not met (2 < 3) → 503, and because the
    // leader knows its own health this is a plain 503, not `health_unknown`.
    let (code, body) = evaluate(&inputs, Require::Healthy);
    assert_eq!(code, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body.reason.is_none(),
        "leader health is known, not health_unknown: {:?}",
        body.reason
    );
}

/// (h) A parked replica whose discovery view is empty joins WITHOUT a restart
/// the moment a registration for a formed cluster appears in its `file`
/// discovery directory (ADR 0037 §1/§2 — "a parked fleet resumes when its
/// cluster reappears").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parked_replica_joins_when_discovery_entry_appears() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // Node 1 forms, registered in its OWN (separate) discovery directory.
    let dir1 = tempfile::tempdir().expect("dir1");
    let mut node1 = Node::new_file_discovery(1, cluster_id, &ca, dir1.path().to_path_buf());
    node1.boot().await;
    node1.form("parked-resume-formation").await;
    poll(DEADLINE, "node 1 formed", || async { node1.is_leader() }).await;

    // Node 2 uses a DIFFERENT, initially-empty discovery directory, so it parks:
    // it can register itself but sees no other candidate.
    let dir2 = tempfile::tempdir().expect("dir2");
    let mut node2 = Node::new_file_discovery(2, cluster_id, &ca, dir2.path().to_path_buf());
    node2.boot().await;
    poll(
        DEADLINE,
        "node 2 parks (waiting, no candidates)",
        || async { node2.convergence_phase() == Phase::Waiting },
    )
    .await;
    assert!(!node2.is_voter(), "a parked replica holds no seat");

    // Drop a registration file naming node 1 into node 2's discovery directory —
    // exactly what a `file`-backend registration looks like (first line = addr).
    std::fs::write(
        dir2.path().join("node1-registration"),
        format!("{}\n", node1.advertise),
    )
    .expect("write discovery registration");

    // Node 2 joins on its next probe — no restart, no config change.
    poll(
        DEADLINE,
        "node 2 joins once its cluster appears in discovery",
        || async { node2.is_voter() },
    )
    .await;
    assert_eq!(
        voters(&node1).len(),
        2,
        "node 2 joined the existing cluster"
    );

    node2.graceful_stop().await;
    node1.graceful_stop().await;
}
