//! Self-converging membership, end to end (ADR 0037 §1/§6).
//!
//! The claim under test is the one that makes ADR 0037's deployment story
//! work: **a coordinator joins a cluster by itself**. There is no
//! `add-learner`, no `promote`, no node id read out of a log and typed into an
//! operator's terminal anywhere in this file — the only human acts are the two
//! the ADR keeps: one `coppice coordinator init` on the first node, and one
//! minted enrollment token that the fleet-wide config artifact carries.
//!
//! Each test therefore drives whole daemons through `bootstrap::run_with`, the
//! same code the binary runs, and asserts on `/readyz` — which is exactly what
//! a real bringup would wait on. What a daemon *does* between parking and
//! reaching `voter` is the loop's business; that it gets there without being
//! told to is the property.

mod common;

use std::time::Duration;

use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::id::ClusterId;
use coppice_proto::pb::core::v1 as pbcore;
use coppice_proto::pb::raft::v1 as pb;

use common::{poll, Ca, Daemon};

/// Per-binary tracing, so `RUST_LOG=coppice_coordinator=debug` shows what the
/// loop is doing. `run_with` does not install a subscriber — the binary's
/// `run` does — so the harness supplies one.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Form a cluster on `daemon` the only way one can be formed: park, then
/// `init` over the local admin socket.
async fn form(daemon: &mut Daemon) -> OperatorPem {
    daemon.start();
    daemon.await_phase("waiting").await;
    let reply = daemon
        .admin(AdminCall::Init {
            policy: None,
            operator_csr: None,
            operator_cn: None,
        })
        .await;
    let AdminReply::Formed { operator, .. } = reply else {
        panic!("expected the cluster to form, got {reply:?}");
    };
    daemon.await_phase("voter").await;
    operator
}

/// Mint the coordinator enrollment token a fleet's config artifact carries
/// (ADR 0037 §5), using the operator credential `init` printed.
async fn coordinator_token(daemon: &Daemon, operator: &OperatorPem) -> String {
    let ca = operator.ca_pem.as_bytes().to_vec();
    let cert = operator.cert_pem.as_bytes().to_vec();
    let key = operator
        .key_pem
        .as_ref()
        .expect("no CSR was supplied, so the cluster minted the keypair")
        .as_bytes()
        .to_vec();
    let mut client =
        coppice_coordinator::admin::admin_channel(&daemon.raft_target(), &ca, &cert, &key)
            .await
            .expect("dial the admin surface");
    let history_id = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe")
        .into_inner()
        .history_id;
    client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id,
            role: pbcore::EnrollRole::Coordinator as i32,
            label: "coordinators".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("mint a coordinator enrollment token")
        .into_inner()
        .secret
}

/// A second coordinator, configured exactly as a fleet's identical artifact
/// would configure it: the cluster's enrollment endpoint, one discovery seed,
/// and nothing else.
fn newcomer(cluster_id: ClusterId, ca: &Ca, leader: &Daemon, token: &str, size: usize) -> Daemon {
    let daemon = Daemon::new_certless(cluster_id, ca);
    daemon.set_cluster_size(size);
    daemon.set_static_discovery(&[leader.raft_target()]);
    daemon.set_enrollment(&leader.api(""), token);
    daemon
}

// ---------------------------------------------------------------------------

/// The whole of ADR 0037 §1 in one test: a brand-new, certless daemon with a
/// fleet-wide config joins an existing cluster and becomes a voter, with no
/// membership command issued by anyone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_certless_newcomer_enrolls_discovers_and_promotes_itself_to_voter() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    let mut leader = Daemon::new_certless(cluster_id, &ca);
    leader.set_cluster_size(2);
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    let mut newcomer = newcomer(cluster_id, &ca, &leader, &token, 2);
    newcomer.start();

    // Everything the loop does is on the other side of this one await: enroll
    // for a machine leaf, probe the seed, start under `Join` stamped with the
    // history the probe reported, ask to be admitted, catch up, and promote.
    let body = newcomer.await_phase("voter").await;
    assert_eq!(body["voters"].as_array().expect("voters").len(), 2);
    assert_eq!(body["is_leader"], false);
    assert!(body["last_admission_refusal"].is_null(), "{body}");

    // The newcomer enrolled: it holds a leaf, and it is the *cluster's* leaf,
    // not the fixture CA's — a certless daemon had nothing else to serve with.
    let (newcomer_ca, cert, _key) = newcomer.tls_material();
    assert_ne!(
        newcomer_ca,
        newcomer.bootstrap_ca_pem(),
        "the newcomer must be serving under the cluster CA it enrolled into"
    );
    coppice_tls::pki::verify_leaf(&newcomer_ca, &cert)
        .expect("the enrolled leaf chains to the cluster CA");

    // And the leader agrees: two voters, from its side of the membership.
    let leader_body = leader.readyz().await.1;
    assert_eq!(leader_body["voters"].as_array().expect("voters").len(), 2);

    newcomer.stop().await.expect("newcomer stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}

/// Convergence is re-entered from the top on every restart (ADR 0037 §6), so a
/// converged replica that is killed and restarted resumes as a voter without
/// re-enrolling, re-joining, or being told anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_converged_replica_restarts_straight_back_into_its_voter_seat() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    let mut leader = Daemon::new_certless(cluster_id, &ca);
    leader.set_cluster_size(2);
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    let mut newcomer = newcomer(cluster_id, &ca, &leader, &token, 2);
    newcomer.start();
    let first = newcomer.await_phase("voter").await;
    let node_id = first["node_id"].clone();

    newcomer.stop().await.expect("converged replica stops");
    newcomer.start();

    // Same seat, same identity: the restart is a resume, and the post-start
    // loop's first tick finds the work already done.
    let again = newcomer.await_phase("voter").await;
    assert_eq!(
        again["node_id"], node_id,
        "a restart must not mint a new id"
    );
    assert_eq!(again["voters"].as_array().expect("voters").len(), 2);

    newcomer.stop().await.expect("newcomer stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}

/// This installation's persisted machine identity file (ADR 0037 §7): minted
/// once at first enrollment, presented forever after.
fn machine_identity_path(daemon: &Daemon) -> std::path::PathBuf {
    daemon
        .data_dir()
        .join(coppice_tls::pki::machine::MACHINE_IDENTITY_FILE)
}

/// Interrupted join, kill point (a): killed **after enrollment, before any
/// join** — staged deterministically rather than by timing. Enrollment needs
/// only the `[enrollment]` endpoint, while joining also needs discovery, so a
/// newcomer with an empty seed list enrolls (leaf + machine identity land on
/// disk) and can go no further: at the kill it is provably still parked. A
/// restart must resume the SAME identity — re-presenting, never re-minting —
/// and converge with no cleanup (ADR 0037 §6).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_newcomer_killed_after_enrolling_but_before_joining_resumes_the_same_identity() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    let mut leader = Daemon::new_certless(cluster_id, &ca);
    leader.set_cluster_size(2);
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    // Enrollment endpoint but NO discovery: the loop's enroll step runs every
    // round regardless of candidates, so the leaf arrives while the join
    // cannot.
    let mut newcomer = Daemon::new_certless(cluster_id, &ca);
    newcomer.set_cluster_size(2);
    newcomer.set_enrollment(&leader.api(""), &token);
    newcomer.start();

    let root = newcomer
        .data_dir()
        .parent()
        .expect("data dir has a parent")
        .to_path_buf();
    let identity_path = machine_identity_path(&newcomer);
    poll(
        Duration::from_secs(20),
        "the newcomer enrolls: leaf and machine identity on disk",
        || {
            let root = root.clone();
            let identity_path = identity_path.clone();
            async move {
                ["node.crt", "node.key", "ca.crt"]
                    .iter()
                    .all(|f| root.join(f).exists())
                    && identity_path.exists()
            }
        },
    )
    .await;
    // Still parked: with no discovery there is nothing to join, so this kill
    // is genuinely between enrollment and admission.
    assert_eq!(newcomer.readyz().await.1["phase"], "waiting");
    let identity_before = std::fs::read(&identity_path).expect("read machine identity");
    let leaf_before = std::fs::read(root.join("node.crt")).expect("read enrolled leaf");
    newcomer.kill().await;
    newcomer.await_released().await;

    // Give the restart what the kill-window installation lacked — a seed —
    // and nothing else. No cleanup of any kind.
    newcomer.set_static_discovery(&[leader.raft_target()]);
    newcomer.start();
    let body = newcomer.await_phase("voter").await;
    assert_eq!(body["voters"].as_array().expect("voters").len(), 2);

    assert_eq!(
        std::fs::read(&identity_path).expect("read machine identity"),
        identity_before,
        "the restart must re-present the persisted machine identity, not mint a new one"
    );
    assert_eq!(
        std::fs::read(root.join("node.crt")).expect("read leaf"),
        leaf_before,
        "the restart already held a usable leaf and must not have re-enrolled"
    );

    newcomer.stop().await.expect("newcomer stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}

/// Interrupted join, kill points (b)/(c): killed at the first observed
/// post-park phase of a live join. `joining` begins the moment consensus
/// starts under `Join` (stamped, driving `AddLearner`, not yet admitted);
/// `learner` means the leader admitted it (between admission and promotion).
/// Which one the kill lands in is timing-dependent — admission needs at least
/// one 300ms loop tick plus the leader's dial-back, so the 10ms poll almost
/// always catches `joining` first — and the observed phase is carried into
/// the assertion messages rather than assumed. `voter` is in the accepted set
/// only so a loaded test host cannot flake the staging: on a busy machine the
/// whole join can outrun the observer's HTTP polls, and a kill that lands
/// just after convergence still exercises the same contract — restart, no
/// cleanup, SAME node id and machine identity, one seat — merely from the
/// least interesting of the three points.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_newcomer_killed_mid_join_restarts_into_the_same_seat_and_converges() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    let mut leader = Daemon::new_certless(cluster_id, &ca);
    leader.set_cluster_size(2);
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    let mut joiner = newcomer(cluster_id, &ca, &leader, &token, 2);
    joiner.start();

    let staged = joiner
        .await_phase_in(&["joining", "learner", "voter"])
        .await;
    let phase_at_kill = staged["phase"].as_str().expect("a phase").to_string();
    // A non-`waiting` phase means consensus started, so the identity is
    // already stamped and reported.
    let node_id = staged["node_id"]
        .as_u64()
        .unwrap_or_else(|| panic!("a {phase_at_kill} daemon reports its node id: {staged}"));
    let identity_path = machine_identity_path(&joiner);
    let identity_before = std::fs::read(&identity_path).expect("read machine identity");
    joiner.kill().await;
    joiner.await_released().await;

    joiner.start();
    let body = joiner.await_phase("voter").await;
    assert_eq!(
        body["node_id"], node_id,
        "the restart (after a kill at {phase_at_kill}) must resume the stamped node id"
    );
    assert_eq!(
        std::fs::read(&identity_path).expect("read machine identity"),
        identity_before,
        "the machine identity must survive a kill at {phase_at_kill} unchanged"
    );

    // One seat ever existed: the leader's membership holds exactly two
    // voters, and the newcomer's seat is the id from BEFORE the kill.
    let leader_body = leader.readyz().await.1;
    let voters = leader_body["voters"].as_array().expect("voters");
    assert_eq!(voters.len(), 2, "kill at {phase_at_kill}: {leader_body}");
    assert!(
        voters.iter().any(|v| v["node_id"] == node_id),
        "the pre-kill seat must be the one that converged: {leader_body}"
    );

    joiner.stop().await.expect("joiner stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}

/// Two newcomers race to join the same cluster and both must converge with
/// distinct seats. Each names the OTHER joiner *first* in its seed list: an
/// unadmitted joiner answers `ProbeCluster` with `initialized` and an empty
/// voter set, and the old first-match-wins leader search wedged two joiners
/// onto each other in exactly this shape — so this is the regression test for
/// `find_leader` probing the whole round and never settling for a candidate
/// outside the voter set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_racing_newcomers_both_converge_to_distinct_voter_seats() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    let mut leader = Daemon::new_certless(cluster_id, &ca);
    leader.set_cluster_size(3);
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    let mut first = Daemon::new_certless(cluster_id, &ca);
    let mut second = Daemon::new_certless(cluster_id, &ca);
    for joiner in [&first, &second] {
        joiner.set_cluster_size(3);
        joiner.set_enrollment(&leader.api(""), &token);
    }
    first.set_static_discovery(&[second.raft_target(), leader.raft_target()]);
    second.set_static_discovery(&[first.raft_target(), leader.raft_target()]);

    // Started back to back, so both spend the enroll/probe/admission window
    // unadmitted at the same time — the state in which each one's probe of
    // the other must not be mistaken for finding the cluster.
    first.start();
    second.start();

    let first_body = first.await_phase("voter").await;
    let second_body = second.await_phase("voter").await;
    assert_ne!(
        first_body["node_id"], second_body["node_id"],
        "racing joiners must be admitted as distinct seats"
    );
    // Reaching `voter` and seeing the FULL voter set are separate moments —
    // the first promotion's body can still show two voters — so the
    // three-voter agreement is polled, on every replica.
    for daemon in [&leader, &first, &second] {
        poll(
            Duration::from_secs(20),
            "every replica agrees on the three-voter set",
            || async {
                let (_, body) = daemon.readyz().await;
                body["voters"].as_array().map(|v| v.len()) == Some(3)
            },
        )
        .await;
    }

    first.stop().await.expect("first joiner stops cleanly");
    second.stop().await.expect("second joiner stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}

/// Widen a follower's election timeout, so that after its leader dies it goes
/// on believing in — and hinting at — the dead leader for a *known* window
/// instead of the fixture default (1s).
fn stretch_election_timeout(daemon: &Daemon, timeout: &str) {
    let path = daemon.config_path();
    let toml = std::fs::read_to_string(&path).expect("read config");
    let updated = toml.replace(
        "election_timeout = \"1s\"",
        &format!("election_timeout = \"{timeout}\""),
    );
    assert_ne!(toml, updated, "no election_timeout line to rewrite");
    std::fs::write(&path, updated).expect("write config");
}

/// A joiner whose every candidate is a follower still hinting at a DEAD former
/// leader must converge anyway — the regression test for `find_leader`
/// trusting the first syntactically resolvable `leader_hint`. Under the old
/// code the first reachable answer's hint was returned *unvalidated*, so a
/// follower naming a corpse pinned the tick to a dead address; the fix probes
/// the whole round, returns a hinted endpoint only when that endpoint's own
/// answer claims leadership, and never returns an endpoint it has not just
/// heard from.
///
/// Staging: a 3-voter cluster whose two followers carry a stretched (4s)
/// election timeout; a fourth daemon enrolls while the leader is alive and is
/// killed still parked; then the leader is killed and the joiner is restarted
/// with the two surviving followers as its ONLY discovery candidates. For the
/// whole pre-election window both survivors answer probes with
/// `leader_hint = <dead node>`, which is exactly the answer the old code
/// returned and dialed forever-for-the-window.
///
/// Residual nondeterminism, documented rather than hidden: this harness has
/// no network partition primitive, so the survivors' stale hint lasts only
/// until they elect (~4s here), not indefinitely — meaning the *old* code
/// eventually converged too, once the hint changed, and under it this test
/// would have shown up as a stall inside the window rather than a guaranteed
/// timeout. What the test pins deterministically is the invariant the finding
/// asks for: candidates whose hints name a dead node, presented first and
/// alone, must not stop the joiner from converging — a dead hinted endpoint
/// is never worth returning.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_hint_naming_a_dead_leader_does_not_wedge_a_joiner() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // cluster_size 4: three voters now, one seat genuinely free for the
    // joiner, so its convergence cannot be confused with the voter-set-full
    // wait.
    let mut first = Daemon::new_certless(cluster_id, &ca);
    first.set_cluster_size(4);
    let operator = form(&mut first).await;
    let token = coordinator_token(&first, &operator).await;

    let mut second = newcomer(cluster_id, &ca, &first, &token, 4);
    let mut third = newcomer(cluster_id, &ca, &first, &token, 4);
    for follower in [&second, &third] {
        stretch_election_timeout(follower, "4s");
    }
    second.start();
    third.start();
    second.await_phase("voter").await;
    third.await_phase("voter").await;

    // The joiner enrolls while the leader — the enrollment endpoint — is
    // still alive: enrollment needs only `[enrollment]`, so with an empty
    // seed list the leaf lands on disk while the join provably cannot start.
    let mut joiner = Daemon::new_certless(cluster_id, &ca);
    joiner.set_cluster_size(4);
    joiner.set_enrollment(&first.api(""), &token);
    joiner.start();
    let root = joiner
        .data_dir()
        .parent()
        .expect("data dir has a parent")
        .to_path_buf();
    poll(
        Duration::from_secs(20),
        "the joiner enrolls: leaf on disk while still parked",
        || {
            let root = root.clone();
            async move {
                ["node.crt", "node.key", "ca.crt"]
                    .iter()
                    .all(|f| root.join(f).exists())
            }
        },
    )
    .await;
    assert_eq!(joiner.readyz().await.1["phase"], "waiting");
    joiner.kill().await;
    joiner.await_released().await;

    // Kill whichever replica is currently leader (found, not assumed), and
    // collect the two survivors — the followers that will keep hinting at it.
    let mut nodes = vec![first, second, third];
    let mut leader_idx = None;
    for (i, node) in nodes.iter().enumerate() {
        if node.readyz().await.1["is_leader"] == true {
            leader_idx = Some(i);
        }
    }
    let leader_idx = leader_idx.expect("one of the three voters is leader");
    let dead_leader_id = nodes[leader_idx].readyz().await.1["node_id"].clone();
    let survivors: Vec<String> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader_idx)
        .map(|(_, node)| node.raft_target())
        .collect();
    nodes[leader_idx].kill().await;

    // Restart the joiner immediately, inside the survivors' stale window, with
    // the stale-hinting followers as its ONLY candidates. It must converge.
    joiner.set_static_discovery(&survivors);
    joiner.start();
    let body = joiner.await_phase("voter").await;

    // The seat it took is the free fourth one: the dead leader keeps its
    // (now unreachable) voter seat, and the joiner did not displace anyone.
    let voters = body["voters"].as_array().expect("voters");
    assert_eq!(voters.len(), 4, "{body}");
    assert!(
        voters.iter().any(|v| v["node_id"] == dead_leader_id),
        "the dead leader must still hold its seat: {body}"
    );

    joiner.stop().await.expect("joiner stops cleanly");
    for mut node in nodes {
        if node.is_running() {
            node.stop().await.expect("survivor stops cleanly");
        }
    }
}

/// A second installation presenting an already-bound machine identity — a
/// cloned VM image, a copied data volume — must be refused admission
/// (ADR 0037 §7: one identity, one node id, ever) AND the refusal must reach
/// the second daemon's own `/readyz`, because status, not logs, is where an
/// operator learns this node will never be admitted (§9).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_installation_with_a_duplicated_machine_identity_is_refused_and_surfaced() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // cluster_size 3, so the clone's refusal cannot be confused with the
    // voter-set-full wait: a seat is genuinely available to it.
    let mut leader = Daemon::new_certless(cluster_id, &ca);
    leader.set_cluster_size(3);
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    let mut original = newcomer(cluster_id, &ca, &leader, &token, 3);
    original.start();
    original.await_phase("voter").await;
    let identity = std::fs::read(machine_identity_path(&original)).expect("read machine identity");

    // The clone: an otherwise-fresh installation (empty data dir, valid
    // enrollment config) whose machine identity file was copied from the
    // converged member. Its enroll step finds the file, presents the stolen
    // identity, and is issued a leaf — issuance is not admission — so the
    // conflict surfaces exactly where §7 places it: at `AddLearner`.
    let clone = newcomer(cluster_id, &ca, &leader, &token, 3);
    std::fs::create_dir_all(clone.data_dir()).expect("create the clone's data dir");
    std::fs::write(machine_identity_path(&clone), &identity).expect("copy the machine identity");
    let mut clone = clone;
    clone.start();

    poll(
        Duration::from_secs(30),
        "the duplicated-identity refusal reaches the clone's /readyz",
        || async {
            let (_, body) = clone.readyz().await;
            body["last_admission_refusal"]
                .as_str()
                .is_some_and(|m| m.contains(coppice_coordinator::admin::MACHINE_IDENTITY_CONFLICT))
        },
    )
    .await;

    // Refused means refused: never admitted (still `joining`, not `learner`),
    // and the cluster's membership did not grow.
    let body = clone.readyz().await.1;
    assert_eq!(body["phase"], "joining", "{body}");
    let leader_body = leader.readyz().await.1;
    assert_eq!(
        leader_body["voters"].as_array().expect("voters").len(),
        2,
        "the clone must not have taken a seat: {leader_body}"
    );

    clone.stop().await.expect("clone stops cleanly");
    original.stop().await.expect("original stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}

/// The §7 voter ceiling is not terminal (ADR 0037 §6): a newcomer that arrives
/// at a cluster already holding its configured voter count stays a caught-up
/// learner and keeps polling, rather than failing or being refused outright.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_newcomer_at_a_full_voter_set_stays_a_learner() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // A single-voter cluster that says it expects exactly one voter.
    let mut leader = Daemon::new_certless(cluster_id, &ca);
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    let mut newcomer = newcomer(cluster_id, &ca, &leader, &token, 1);
    newcomer.start();

    // It is admitted and catches up — the leader's membership grows — but the
    // ceiling holds, so it never reaches `voter`.
    newcomer.await_phase("learner").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let body = newcomer.readyz().await.1;
    assert_eq!(body["phase"], "learner", "the ceiling must hold: {body}");
    assert_eq!(
        body["voters"].as_array().expect("voters").len(),
        1,
        "the voter set must not have grown past cluster_size"
    );
    // Not a refusal, either: this is a wait, and `/readyz` must not report it
    // as an operator-actionable failure.
    assert!(body["last_admission_refusal"].is_null(), "{body}");
    // But the wait is not silent: §7 requires the leader's machine-readable
    // reason be visible in status output, and it has its own field so the
    // distinction between "waiting" and "refused" survives.
    let hold = body["promotion_hold"]
        .as_str()
        .unwrap_or_else(|| panic!("a held promotion must surface its reason: {body}"));
    assert!(
        hold.starts_with("no-removable-peer") || hold.starts_with("voter-set-full"),
        "the hold must carry the leader's marker: {hold}"
    );

    newcomer.stop().await.expect("newcomer stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}
