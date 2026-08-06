//! End-to-end multi-node coordinator tests: real mTLS Raft transport, real
//! join, real admin gRPC surface, on localhost.
//!
//! Two scenarios, deliberately at different altitudes.
//!
//! **The lifecycle** is what ADR 0037 §1 promises and is asserted the way an
//! operator would experience it: N shape-identical configs, one `init`, three
//! voters. No test here names a node id, an address, or a peer — if it had to,
//! the promise would already be broken.
//!
//! **The raft mechanics** underneath it still need exercising at a level the
//! convergence loop deliberately hides: converged commits, a leader kill with
//! re-election, a follower restart-from-disk, and a dead voter replaced by a
//! fresh learner that resyncs via install-snapshot. That test drives membership
//! through the operator verbs directly — which is not a workaround but the
//! genuine §7 operator path, and the only way to stage a *dead* voter whose
//! seat must be surgically replaced.
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
use coppice_core::id::{ClusterId, MachineId};
use coppice_core::time::Timestamp;
use coppice_state::command::BumpClusterVersion;
use coppice_state::Command;
use coppice_tls::pki;

use common::{poll, wait_converged, Ca, Daemon, Fleet, Leaf, Node};

/// ADR 0037 §1, whole: N identical configs plus one `init` become an N-voter
/// cluster, with nobody told anything about anybody.
///
/// The only inputs that differ between these three daemons are the ports a
/// single test process forces apart. There is no seed list (the `file` backend
/// enumerates a shared directory), no node id anywhere, no `add-learner`, no
/// `promote`, and no second operator action after `init` — the enrollment token
/// was baked into all three configs before the cluster existed, exactly as a
/// launch template would bake it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fleet_of_identical_configs_converges_to_three_voters() {
    init_tracing();
    let ca = Ca::new();
    let mut fleet = Fleet::new(3, &ca);

    // All three park: none may self-bootstrap, and there is no cluster to find.
    fleet.start_all();
    for member in &fleet.members {
        member.await_phase("waiting").await;
    }

    // The one operator act in the cluster's whole lifetime (ADR 0037 §3).
    let operator = fleet.init().await;
    assert!(operator.cert_pem.contains("BEGIN CERTIFICATE"));

    // Everything after this is the loop's: the other two discover the cluster
    // through the registration directory, enrol for a leaf against the seeded
    // token, join as learners, catch up, and promote themselves.
    fleet.await_voters(3).await;

    // One leader, and every member agrees who it is.
    let mut leaders: Vec<u64> = Vec::new();
    for member in &fleet.members {
        let body = member.readyz().await.1;
        leaders.push(
            body["leader"]
                .as_u64()
                .unwrap_or_else(|| panic!("a converged member must know the leader: {body}")),
        );
    }
    leaders.dedup();
    assert_eq!(
        leaders.len(),
        1,
        "members disagree about the leader: {leaders:?}"
    );

    // Each member enrolled into the cluster CA rather than the fixture's, and
    // holds a leaf that chains to it. A certless fleet had nothing else.
    for (i, member) in fleet.members.iter().enumerate() {
        let (cluster_ca, cert, _key) = member.tls_material();
        assert_ne!(
            cluster_ca,
            member.bootstrap_ca_pem(),
            "member {i} is still serving under the bootstrap CA"
        );
        coppice_tls::pki::verify_leaf(&cluster_ca, &cert)
            .unwrap_or_else(|e| panic!("member {i}'s leaf must chain to the cluster CA: {e}"));
    }

    fleet.stop_all().await;
}

/// A fleet that starts before its cluster exists must sit parked indefinitely
/// and then converge the moment one appears — the ADR 0037 §1 property that
/// makes boot ordering a non-question for deployment automation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_fleet_converges_when_a_cluster_finally_appears() {
    init_tracing();
    let ca = Ca::new();
    let mut fleet = Fleet::new(3, &ca);

    fleet.start_all();
    for member in &fleet.members {
        member.await_phase("waiting").await;
    }

    // Long enough to cross several convergence rounds and let the parked
    // backoff grow: the daemons must still be parked, not wedged, and not
    // have talked each other into forming anything.
    tokio::time::sleep(Duration::from_secs(3)).await;
    for (i, member) in fleet.members.iter().enumerate() {
        let body = member.readyz().await.1;
        assert_eq!(
            body["phase"], "waiting",
            "member {i} left park unprompted: {body}"
        );
        assert_eq!(body["formed"], false, "member {i} formed itself: {body}");
    }

    // Now give them a cluster. The backed-off loops must still find it.
    fleet.init().await;
    fleet.await_voters(3).await;

    fleet.stop_all().await;
}

/// Wait until `member` is a voter in an `expected`-voter set.
///
/// The per-member half of [`Fleet::await_voters`], for tests that start only
/// part of a fleet (the rest of the members have no listener to poll).
async fn await_member_voters(member: &Daemon, expected: usize) {
    poll(
        Duration::from_secs(60),
        &format!("member reaches a {expected}-voter set"),
        || async {
            let (_, body) = member.readyz().await;
            body["phase"] == "voter" && body["voters"].as_array().map(|v| v.len()) == Some(expected)
        },
    )
    .await;
}

/// The index of the member currently reporting itself leader, among the first
/// `candidates` members. Polled: a converged cluster has a leader, but the
/// observation and the question are on different replicas.
async fn fleet_leader_index(fleet: &Fleet, candidates: usize) -> usize {
    // A plain loop rather than the `poll` helper only because the answer is a
    // value, not a condition; the deadline discipline is the same.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        for (i, member) in fleet.members[..candidates].iter().enumerate() {
            if member.readyz().await.1["is_leader"] == true {
                return i;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no member among the first {candidates} reported leadership"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A newcomer that is mid-join when the leader goes away must still converge:
/// its dial targets re-derive from local membership each tick (ADR 0037
/// §2/§6), so the new leader is found without discovery ever being told
/// anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mid_join_newcomer_converges_via_the_new_leader_after_the_old_one_stops() {
    init_tracing();
    let ca = Ca::new();
    // Shape-identical configs sized for four voters, with only three started:
    // the fourth is the newcomer whose join the leader change interrupts.
    let mut fleet = Fleet::new(4, &ca);
    for member in &mut fleet.members[..3] {
        member.start();
    }
    fleet.init().await;
    for member in &fleet.members[..3] {
        await_member_voters(member, 3).await;
    }
    let leader_idx = fleet_leader_index(&fleet, 3).await;

    // Pre-provision the newcomer's cluster material instead of letting it
    // enroll organically: certless enrollment is not this test's subject (the
    // fleet lifecycle and convergence suites own it), and on a starved CI
    // runner the enroll round can keep landing inside election churn for
    // longer than any reasonable staging deadline — the park backoff caps at
    // 15s, so every failed round costs most of the budget. With a usable leaf
    // on disk the first successful probe round joins.
    let machine = MachineId::new();
    {
        let (ca_pem, _, _) = fleet.members[0].tls_material();
        let ca_key = pki::load_ca_key(&fleet.members[0].data_dir(), &ca_pem)
            .expect("the forming voter's data dir holds the CA key");
        let signer = pki::CaSigner::load(&ca_pem, &ca_key).expect("load the cluster CA signer");
        let (key_pem, csr_pem) = pki::generate_key_and_csr().expect("newcomer keypair");
        let cert_pem = pki::issue_coordinator(
            &signer,
            &csr_pem,
            &machine,
            &["localhost".to_string(), "127.0.0.1".to_string()],
        )
        .expect("issue the newcomer's machine leaf");
        fleet.members[3].install_tls_material(&ca_pem, &cert_pem, &key_pem);
        std::fs::create_dir_all(fleet.members[3].data_dir()).expect("create data dir");
        pki::persist_machine_identity(&fleet.members[3].data_dir(), &machine)
            .expect("persist the newcomer's machine identity");
    }

    // Start the newcomer and catch it mid-join: `joining` (driving admission)
    // or `learner` (admitted, catching up). The 10ms poll against the loop's
    // 300ms tick usually observes one of the two, but under full-suite load
    // the whole join can outrun the observer, so `voter` is accepted too and
    // the phase actually caught travels into the final assertion message —
    // the invariant under test (a leader change never strands the newcomer,
    // because dial targets re-derive from local membership) holds from any
    // of the three points; only how much of the join the change interrupts
    // varies.
    fleet.members[3].start();
    let staged = fleet.members[3]
        .await_phase_in(&["joining", "learner", "voter"])
        .await;
    let phase_at_stop = staged["phase"].as_str().expect("a phase").to_string();

    // The leader leaves mid-join. Quorum survives (2 of 3 voters), a new
    // leader is elected, and nobody tells the newcomer anything.
    fleet.members[leader_idx]
        .stop()
        .await
        .expect("the old leader stops cleanly");

    let body = fleet.members[3].await_phase("voter").await;
    assert_eq!(
        body["voters"].as_array().expect("voters").len(),
        4,
        "the newcomer (leader stopped while it was {phase_at_stop}) must converge \
         via the new leader: {body}"
    );

    fleet.stop_all().await;
}

/// `formed` answers shape, `?require=healthy` answers redundancy, and killing
/// a voter must split them (ADR 0037 §9): the leader keeps reporting
/// `formed: true` and its own plain `/readyz` stays 200, while the
/// cluster-redundancy gate goes 503.
///
/// Deliberately **no workload traffic** after formation: liveness is the
/// leader's per-voter contact observation (a dead voter stops answering
/// heartbeats within the staleness bound), never inference from log
/// positions — on an idle log a dead voter's matched index stays current
/// forever, so a gate that needed the log to move would call a dead-quiet
/// cluster healthy indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn formed_stays_true_but_require_healthy_degrades_once_a_voter_dies() {
    init_tracing();
    let ca = Ca::new();
    let mut fleet = Fleet::new(3, &ca);
    // Shrink the sustain window (default 10s) so the healthy 200 arrives
    // inside a test-sized wait; degradation itself needs only the staleness
    // bound (2× the 300ms election timeout) plus a sampler tick either way.
    for member in &fleet.members {
        member.set_health_stability("1s");
    }
    fleet.start_all();
    fleet.init().await;
    fleet.await_voters(3).await;

    let leader_idx = fleet_leader_index(&fleet, 3).await;
    let client = reqwest::Client::new();
    let healthy_url = fleet.members[leader_idx].api("/readyz?require=healthy");
    poll(
        Duration::from_secs(30),
        "the leader sustains full redundancy and answers require=healthy with 200",
        || async {
            client
                .get(&healthy_url)
                .send()
                .await
                .is_ok_and(|r| r.status().as_u16() == 200)
        },
    )
    .await;

    // Kill (not stop) a non-leader voter: disk untouched, no goodbye — and
    // not one entry written from here on.
    let victim = (leader_idx + 1) % 3;
    fleet.members[victim].kill().await;

    // The leader's own gate and the shape field are untouched by the death:
    // membership still holds three voters, and 2-of-3 quorum keeps acking.
    let (status, body) = fleet.members[leader_idx].readyz().await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["formed"], true, "{body}");
    assert_eq!(body["voters"].as_array().expect("voters").len(), 3);

    // The idle-cluster requirement itself: the dead voter stops answering the
    // leader's heartbeats, its contact goes stale (2s: twice the fixture's 1s
    // election timeout), the sampler
    // observes the shortfall on its next tick — 503 well inside this budget,
    // with the log never moving.
    poll(
        Duration::from_secs(10),
        "require=healthy degrades to 503 from lost contact alone (no writes)",
        || async {
            client
                .get(&healthy_url)
                .send()
                .await
                .is_ok_and(|r| r.status().as_u16() == 503)
        },
    )
    .await;
    let resp = client.get(&healthy_url).send().await.expect("readyz");
    assert_eq!(resp.status().as_u16(), 503);
    let degraded: serde_json::Value = resp.json().await.expect("readyz json");
    assert_eq!(degraded["live_voters"], 2, "{degraded}");
    assert_eq!(
        degraded["formed"], true,
        "shape must keep answering shape: {degraded}"
    );

    // And the split holds: the plain node gate on the same leader is still 200.
    let (status, body) = fleet.members[leader_idx].readyz().await;
    assert_eq!(status, 200, "{body}");

    fleet.stop_all().await;
}

/// A voter that dies and recovers entirely *between* two health requests must
/// still reset the stability window (ADR 0037 §9): the window is maintained
/// by the leader's background sampler, not by whoever happens to ask, so a
/// flap nobody polled through is still a flap.
///
/// Shape: observe a healthy 200, kill a voter, restart it with **no health
/// request in between**, wait until the leader again observes all three
/// voters live — and the gate must answer 503 at that instant (the unobserved
/// flap reset the window), turning 200 only after a fresh stability interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flap_between_health_requests_still_resets_the_stability_window() {
    init_tracing();
    let ca = Ca::new();
    let mut fleet = Fleet::new(3, &ca);
    // Long enough that "recovered but not yet re-sustained" is a wide, safely
    // assertable state; short enough to keep the test bounded.
    const STABILITY: Duration = Duration::from_secs(5);
    for member in &fleet.members {
        member.set_health_stability("5s");
    }
    fleet.start_all();
    fleet.init().await;
    fleet.await_voters(3).await;

    let leader_idx = fleet_leader_index(&fleet, 3).await;
    let client = reqwest::Client::new();
    let healthy_url = fleet.members[leader_idx].api("/readyz?require=healthy");
    poll(
        Duration::from_secs(30),
        "the leader sustains full redundancy and answers require=healthy with 200",
        || async {
            client
                .get(&healthy_url)
                .send()
                .await
                .is_ok_and(|r| r.status().as_u16() == 200)
        },
    )
    .await;

    // The flap: kill a non-leader voter and bring it straight back, touching
    // no health endpoint in between. The sleep only guarantees the outage
    // outlives the contact-staleness bound (2× the fixture's 1s election
    // timeout = 2s) plus a couple of sampler ticks (staleness/4 = 500ms), so
    // the lapse is observable *by the sampler* — nothing else is watching,
    // which is the point.
    let victim = (leader_idx + 1) % 3;
    fleet.members[victim].kill().await;
    tokio::time::sleep(Duration::from_millis(3500)).await;
    fleet.members[victim].await_released().await;
    fleet.members[victim].start();

    // Wait until a leader again observes all three voters live — read off the
    // plain report body, which never touches the health verdict. Re-derived
    // rather than assumed to be the same member: under CPU contention the
    // flap can coincide with an election, and the property under test holds
    // either way — the incumbent's sampler saw the lapse and reset, and a new
    // leader starts a fresh window under a new term (ADR 0037 §9).
    let deadline = Instant::now() + Duration::from_secs(30);
    let leader_idx = loop {
        let mut found = None;
        for (i, member) in fleet.members.iter().enumerate() {
            let (status, body) = member.readyz().await;
            if status == 200 && body["is_leader"] == true && body["live_voters"] == 3 {
                found = Some(i);
                break;
            }
        }
        if let Some(i) = found {
            break i;
        }
        assert!(
            Instant::now() < deadline,
            "no leader ever observed all three voters live again"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let healthy_url = fleet.members[leader_idx].api("/readyz?require=healthy");
    let recovered_at = Instant::now();

    // Full replication is back, and the gate must still say no: the window
    // restarted when the sampler saw the flap, and a fresh stability interval
    // has not elapsed. The two facts — 503, and all three voters live — must
    // be observed in ONE response: a freshly reconnected voter's contact can
    // transiently lapse again (stream re-establishment under load), so a
    // live_voters read from a moment other than the 503's proves nothing.
    // Any 200 inside the fresh interval is the actual bug this test exists to
    // catch, and fails immediately.
    let joint_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let resp = client.get(&healthy_url).send().await.expect("readyz");
        let status = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.expect("readyz json");
        if status == 200 {
            panic!(
                "an unobserved flap must reset the stability window, but the gate \
                 said 200 only {:?} after recovery (interval {STABILITY:?}): {body}",
                recovered_at.elapsed(),
            );
        }
        if status == 503 && body["live_voters"] == 3 {
            break;
        }
        assert!(
            Instant::now() < joint_deadline,
            "never observed 503-with-full-replication together (the 503 must be \
             about the window, not about replication); last: {status} {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // And it re-sustains only by serving out a fresh interval.
    poll(
        Duration::from_secs(15),
        "require=healthy returns 200 after a fresh stability interval",
        || async {
            client
                .get(&healthy_url)
                .send()
                .await
                .is_ok_and(|r| r.status().as_u16() == 200)
        },
    )
    .await;
    assert!(
        recovered_at.elapsed() >= STABILITY - Duration::from_millis(500),
        "the 200 must not arrive before a fresh stability interval has been \
         served out (recovered {:?} ago, interval {STABILITY:?})",
        recovered_at.elapsed(),
    );

    fleet.stop_all().await;
}

/// `?require=healthy` on a non-leader is a plain refusal to guess (ADR 0037
/// §9): 503, machine-readable `health_unknown`, and the leader hint in the
/// body — never a cached snapshot of what the leader last said.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn require_healthy_on_a_follower_is_health_unknown_with_a_leader_hint() {
    init_tracing();
    let ca = Ca::new();
    let mut fleet = Fleet::new(2, &ca);
    fleet.start_all();
    fleet.init().await;
    fleet.await_voters(2).await;

    let leader_idx = fleet_leader_index(&fleet, 2).await;
    let follower_idx = 1 - leader_idx;
    let leader_node = fleet.members[leader_idx].readyz().await.1["node_id"]
        .as_u64()
        .expect("the leader reports its node id");

    let resp = reqwest::get(fleet.members[follower_idx].api("/readyz?require=healthy"))
        .await
        .expect("follower readyz");
    assert_eq!(resp.status().as_u16(), 503);
    let body: serde_json::Value = resp.json().await.expect("readyz json");
    assert_eq!(body["reason_code"], "health_unknown", "{body}");
    assert_eq!(
        body["leader"], leader_node,
        "the refusal must carry the leader hint: {body}"
    );

    fleet.stop_all().await;
}

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
            bumped_at: Timestamp::from_micros(to as i64).expect("in range"),
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
    history_id: [u8; 16],
    expect: &[u64],
    deadline: Duration,
) {
    let mut client = admin::admin_channel(leader_target, admin_ca, admin_cert, admin_key)
        .await
        .expect("dial admin surface");
    let start = Instant::now();
    loop {
        let status = admin::cluster_status(&mut client, history_id)
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
    let admin_leaf: Leaf = ca.operator_leaf();
    let cluster_id = ClusterId::new();
    let history_id = uuid_bytes(cluster_id);

    // Three replicas, ids 1..=3, each its own tempdir/port/cert.
    let mut nodes: Vec<Node> = (1..=3).map(|id| Node::new(id, cluster_id, &ca)).collect();

    // -- Step 1: bootstrap node 1, wait until it is leader. -----------------
    nodes[0].boot().await;
    let leader0 = wait_for_leader(&nodes, &[0], DEADLINE).await;
    assert_eq!(leader0, 0, "the bootstrap node must be the initial leader");

    // -- Step 2: join nodes 2 and 3 as learners, then promote to voters. ----
    for i in [1usize, 2] {
        nodes[i].boot_joining().await;
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
                history_id,
                nodes[i].raft_id(),
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
        history_id,
        &[nodes[1].raft_id(), nodes[2].raft_id()],
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
            admin::promote_voter(&mut client, history_id, nodes[i].raft_id(), DEADLINE)
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
                    bumped_at: Timestamp::from_micros(to as i64).expect("in range"),
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
    nodes[follower].boot().await;

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
    node4.boot_joining().await;
    let dead_id = nodes[dead_idx].raft_id();

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
        admin::add_learner(
            &mut client,
            history_id,
            node4.raft_id(),
            node4.advertise.clone(),
        )
        .await
        .expect("add-learner node 4");
    }

    wait_learners_caught_up(
        &ca.pem,
        &admin_leaf.cert_pem,
        &admin_leaf.key_pem,
        &leader_target,
        history_id,
        &[node4.raft_id()],
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
        // Promote node 4 and drop the dead node in ONE joint change
        // (ADR 0037 §7): a caller-named pair is `ReplaceVoter`, the
        // operator-authenticated verb — `PromoteVoter` never names a removal
        // any more, and the leader's own evidence-gated fold-in is the other
        // path to the same shape.
        admin::replace_voter(&mut client, history_id, dead_id, node4.raft_id())
            .await
            .expect("replace the dead voter with node 4");
    }

    poll(
        DEADLINE,
        "membership = {leader, follower, node4}, no dead node",
        || async {
            let voters = voter_ids(&nodes[leader_idx]);
            voters.contains(&node4.raft_id()) && !voters.contains(&dead_id) && voters.len() == 3
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

/// Regression: a strong read whose `read_index` barrier lands on a Raft no-op
/// (the blank entry openraft appends on becoming leader) or the bootstrap
/// membership entry must resolve — with no normal command ever proposed.
///
/// Those entries never reach the publishing apply task, but `read_index`
/// returns the full Raft index, so the published view cursor has to advance
/// past them anyway. Before the fix the cursor stalled at the last normal
/// command (index 0 on a fresh leader), so `at_least(read_index)` blocked
/// forever; a regression here hangs until the timeout instead of returning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strong_read_resolves_at_a_leader_noop() {
    init_tracing();

    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let mut node = Node::new(1, cluster_id, &ca);
    node.boot().await;
    wait_for_leader(std::slice::from_ref(&node), &[0], DEADLINE).await;

    // The strong-read barrier — deliberately taken with no normal command in
    // the log, so it can only sit on the bootstrap membership entry or the
    // leader's no-op, both of which bypass the apply task.
    let read_index = tokio::time::timeout(DEADLINE, node.consensus().read_index())
        .await
        .expect("read_index returned within the deadline")
        .expect("read_index");
    assert!(
        read_index >= 1,
        "the barrier must land on a non-normal entry (membership/no-op), got {read_index}"
    );

    // The read side of the strong read: this is what used to hang.
    let view = tokio::time::timeout(DEADLINE, node.views().at_least(read_index))
        .await
        .expect("strong read at a no-op/membership index must resolve, not hang")
        .expect("view");
    assert!(
        view.applied_index() >= read_index,
        "published view must have advanced past the no-op barrier"
    );

    node.graceful_stop().await;
}

/// The ADR 0016 identity rules as ADR 0037 §1 reaches them: startup intent is
/// *derived* from the data directory, so what used to be a flag matrix is now
/// a question about disk state — fast, no cluster needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn derived_startup_intent() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // (a) An empty directory forms: there is no flag to omit and nothing to
    //     refuse, so the first boot mints this replica's allocate-once raft
    //     identity (ADR 0025) and stamps the directory.
    {
        let mut node = Node::new(10, cluster_id, &ca);
        node.boot().await;
        assert!(
            node.raft_id() != 0,
            "forming an empty directory must mint a raft identity"
        );
        node.graceful_stop().await;
    }

    // (b) A directory that already carries a manifest resumes under the
    //     identity it was stamped with, rather than fail-stopping the way the
    //     old `--bootstrap`-on-an-initialized-directory case did.
    {
        let mut node = Node::new(11, cluster_id, &ca);
        node.boot().await;
        let first = node.raft_id();
        node.graceful_stop().await;

        node.boot().await;
        assert_eq!(
            node.raft_id(),
            first,
            "resuming must reuse the stamped raft identity, not mint a new one"
        );
        node.graceful_stop().await;
    }

    // (c) Restart with a DIFFERENT cluster_id than the disk was stamped with
    //     must fail-stop on the identity mismatch.
    {
        let mut node = Node::new(12, cluster_id, &ca);
        node.boot().await;
        node.graceful_stop().await;

        node.rewrite_cluster_id(ClusterId::new());
        let err = node
            .try_boot()
            .await
            .expect_err("Restart with a different cluster_id must refuse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("identity") || msg.contains("cluster"),
            "error should mention the identity/cluster mismatch, got: {msg}"
        );
    }
}
