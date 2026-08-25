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

use coppice_coordinator::failpoints;
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

/// The distinct points of the join pipeline at which this suite kills a
/// joining daemon (ADR 0037 §6: "interrupted join at every step").
///
/// The pipeline a certless newcomer walks is: enroll → stamp its identity and
/// start consensus under `Join` → `AddLearner` → catch up → `PromoteVoter`.
/// Each variant below names one boundary in that sequence, and **every one of
/// them is reached by construction** — by withholding an input, by the §7
/// voter-set ceiling, or by a failpoint this daemon's own config arms
/// ([`coppice_coordinator::failpoints`]). Nothing here races a poller against
/// the loop, because the two boundaries that matter most — an RPC issued whose
/// outcome was never observed — are not states an external observer can catch
/// against a leader that answers in microseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KillPoint {
    /// After enrollment, before any join. Staged by withholding discovery:
    /// the loop's enroll step runs every round regardless of candidates, so
    /// the leaf and machine identity land on disk while the join provably
    /// cannot start — at the kill the daemon is still `waiting`.
    AfterEnroll,
    /// Stamped, consensus running under `Join`, and the cluster has never
    /// heard of this replica: killed one line before `AddLearner` is issued.
    /// The joiner's own phase is the proof — an admitted learner learns its
    /// membership, so `joining` means the request was never sent.
    ///
    /// Replaces an earlier `AfterStamp` variant that polled for a phase in
    /// `{joining, learner, voter}` and could therefore be satisfied by a
    /// daemon that had already sailed past admission *and* promotion — an
    /// "interrupted join" that interrupted nothing.
    BeforeAddLearner,
    /// The admission interruption: `AddLearner` **issued and answered but
    /// never observed**. The leader may have committed the admission while
    /// this replica holds no record of asking, which is exactly what a crash
    /// on the wire produces and precisely what the idempotency contract is
    /// for — the restart re-issues the same request and must be told yes.
    AddLearnerIssued,
    /// A caught-up learner whose promotion is pending — the near side of the
    /// `PromoteVoter` boundary. Staged deterministically by forming the leader
    /// with `cluster_size = 1`: the §7 ceiling holds the joiner as a learner
    /// indefinitely (see `a_newcomer_at_a_full_voter_set_stays_a_learner`),
    /// and the seat is opened *after* the restart by widening the leader's
    /// ceiling, so this kill lands provably between admission and promotion.
    CaughtUpLearner,
    /// The promotion interruption, and the instant between the two variants
    /// either side of it: `PromoteVoter` issued and answered, its outcome
    /// never observed. The seat may already be this replica's while this
    /// replica still believes it is a learner.
    PromoteVoterIssued,
    /// The far side of the same boundary: killed the instant the promotion
    /// has committed and this replica has observed it. The restart must find
    /// the work already done — the loop's already-a-voter short-circuit — and
    /// re-present, never re-mint, its identity.
    JustPromoted,
}

impl KillPoint {
    const ALL: [KillPoint; 6] = [
        KillPoint::AfterEnroll,
        KillPoint::BeforeAddLearner,
        KillPoint::AddLearnerIssued,
        KillPoint::CaughtUpLearner,
        KillPoint::PromoteVoterIssued,
        KillPoint::JustPromoted,
    ];

    /// The leader's `[discovery] cluster_size` while this point is staged.
    /// `CaughtUpLearner` needs a full voter set (the §7 hold); every other
    /// point needs a seat waiting so the join can complete on its own.
    fn leader_cluster_size(self) -> usize {
        match self {
            KillPoint::CaughtUpLearner => 1,
            _ => 2,
        }
    }

    /// The failpoint that stages this point, for the three that are staged by
    /// one. Armed in the *joiner's* config and nowhere else: every RPC of the
    /// join pipeline is issued by the joiner, so both in-flight boundaries
    /// live on that side, and a leader sharing this test process is untouched.
    fn failpoint(self) -> Option<&'static str> {
        match self {
            KillPoint::BeforeAddLearner => Some(failpoints::JOIN_BEFORE_ADD_LEARNER),
            KillPoint::AddLearnerIssued => Some(failpoints::JOIN_ADD_LEARNER_ISSUED),
            KillPoint::PromoteVoterIssued => Some(failpoints::JOIN_PROMOTE_VOTER_ISSUED),
            _ => None,
        }
    }

    /// The `/readyz` phases a daemon halted here may be in.
    ///
    /// **Not the staging** — the marker file is, and it is written by the very
    /// line the halt lands on. This is the bound that says the line means what
    /// it claims: each set excludes the far side of the boundary the point
    /// brackets, so a failpoint that silently moved would fail here.
    ///
    /// Two of the three sets have two members, and deliberately: only the
    /// convergence loop is parked, so the consensus core keeps taking appends
    /// and the replicated membership goes on updating *underneath* a replica
    /// that has stopped asking. Whether that update has landed by the moment
    /// the harness samples is timing, and the honest thing is to accept both
    /// answers rather than build a second race into the assertion. What is not
    /// timing — because the failpoint fires on the RPC's boundary and not on
    /// its outcome — is that a first `PromoteVoter` may legitimately have been
    /// answered `learner-behind` (the leader wants its own heartbeat
    /// acknowledgement, which the joiner's catch-up check against the leader's
    /// replication view does not wait for), so `learner` there is not a slow
    /// `voter` — it is the other half of the point.
    fn phases_at_halt(self) -> &'static [&'static str] {
        match self {
            KillPoint::BeforeAddLearner => &["joining"],
            KillPoint::AddLearnerIssued => &["joining", "learner"],
            KillPoint::PromoteVoterIssued => &["learner", "voter"],
            _ => &[],
        }
    }
}

/// Interrupted join at every step (ADR 0037 §6), as one parameterized test.
///
/// The property is the same at every kill point and is asserted identically at
/// each: kill the joining daemon, restart it with **no cleanup of any kind**,
/// and it resumes the SAME installation — the same machine identity file byte
/// for byte (re-presented, never re-minted), the same enrolled leaf (no second
/// enrollment), the same stamped node id where one exists yet — and converges
/// to `voter` in a two-voter cluster whose seat is the one it was already
/// heading for. `Restart=always` is the whole recovery story, so a kill that
/// needs an operator to clean up after it is a failed test regardless of where
/// it landed.
///
/// One test rather than six, and sequential rather than parallel, because the
/// staging differs per point but the assertions must not: a per-point test
/// invites a per-point assertion, which is exactly how "every step" quietly
/// becomes "the step someone remembered".
///
/// "Every step" is meant literally, and the two steps that used to be missing
/// are the ones no poller could have caught: the joiner killed with
/// `AddLearner` issued and its answer unread, and the same for `PromoteVoter`.
/// Both are staged by a failpoint carried in the *joiner's own config*
/// ([`coppice_coordinator::failpoints`]), which is what makes them
/// deterministic — and per-daemon, so arming the joiner cannot arm the leader
/// sharing this test process.
///
/// This subsumes and replaces the two earlier single-point tests
/// (`…killed_after_enrolling_but_before_joining…` and
/// `…killed_mid_join…`), which are now the [`KillPoint::AfterEnroll`] and
/// [`KillPoint::BeforeAddLearner`] iterations with the same intent and a
/// superset of their assertions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_interrupted_at_every_step_resumes_the_same_identity_and_converges() {
    init_tracing();
    for point in KillPoint::ALL {
        interrupted_join_at(point).await;
    }
}

/// One kill point, end to end: form a cluster, stage the point, kill, restart,
/// converge, and assert the resume contract.
async fn interrupted_join_at(point: KillPoint) {
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    let mut leader = Daemon::new_certless(cluster_id, &ca);
    if point.leader_cluster_size() != 1 {
        leader.set_cluster_size(point.leader_cluster_size());
    }
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    // The joiner. `AfterEnroll` is the one point staged by withholding the
    // seed list, so it is built by hand; the rest get the ordinary fleet-shaped
    // config.
    let mut joiner = match point {
        KillPoint::AfterEnroll => {
            let joiner = Daemon::new_certless(cluster_id, &ca);
            joiner.set_cluster_size(2);
            joiner.set_enrollment(&leader.api(""), &token);
            joiner
        }
        _ => newcomer(cluster_id, &ca, &leader, &token, 2),
    };
    // Arm the crash, in the joiner's config and nowhere else. Last, because
    // `[test_failpoints]` is written as the file's tail.
    if let Some(name) = point.failpoint() {
        joiner.arm_failpoints(&[name]);
    }
    let root = joiner
        .data_dir()
        .parent()
        .expect("data dir has a parent")
        .to_path_buf();
    let identity_path = machine_identity_path(&joiner);
    joiner.start();

    // ---- stage the kill point ----------------------------------------------
    let staged = stage_kill_point(point, &joiner, &root).await;
    let phase_at_kill = staged["phase"].as_str().unwrap_or("waiting").to_string();
    // A stamped replica reports its node id; a parked one has none yet, and
    // that absence is itself part of what `AfterEnroll` is staging.
    let node_id_at_kill = staged["node_id"].as_u64();
    assert_eq!(
        node_id_at_kill.is_none(),
        point == KillPoint::AfterEnroll,
        "{point:?}: only a still-parked daemon may lack a node id: {staged}"
    );
    let identity_before = std::fs::read(&identity_path)
        .unwrap_or_else(|e| panic!("{point:?}: read machine identity: {e}"));
    let leaf_before = std::fs::read(root.join("node.crt"))
        .unwrap_or_else(|e| panic!("{point:?}: read enrolled leaf: {e}"));

    joiner.kill().await;
    joiner.await_released().await;

    // ---- restart, with nothing cleaned up and nothing added ----------------
    // Nothing on disk is touched: the halt marker a failpoint-staged point
    // wrote is left exactly where it fell, because "no cleanup" has to mean no
    // cleanup. The only edits are to the *crash injection* itself, which is
    // the harness's equipment and not the installation's state — `AfterEnroll`
    // gets back the seed list its staging withheld (a daemon that can never
    // find the cluster demonstrates nothing about resuming), and a
    // failpoint-staged point is disarmed, since a restart that halts again is
    // not a restart.
    if point == KillPoint::AfterEnroll {
        joiner.set_static_discovery(&[leader.raft_target()]);
    }
    if point.failpoint().is_some() {
        joiner.clear_failpoints();
    }
    joiner.start();

    // `CaughtUpLearner` was killed under a ceiling that leaves no seat: it must
    // first come back to exactly where it was — an admitted learner — and only
    // then is the seat opened, which is what makes the kill provably
    // pre-promotion rather than merely early.
    if point == KillPoint::CaughtUpLearner {
        joiner.await_phase("learner").await;
        leader.stop().await.expect("leader stops cleanly");
        leader.set_cluster_size(2);
        leader.start();
        leader.await_phase("voter").await;
    }

    let body = joiner.await_phase("voter").await;

    // ---- the resume contract, identical at every point ---------------------
    assert_eq!(
        std::fs::read(&identity_path).expect("read machine identity"),
        identity_before,
        "{point:?} (killed at {phase_at_kill}): the restart must re-present the persisted \
         machine identity, not mint a new one"
    );
    assert_eq!(
        std::fs::read(root.join("node.crt")).expect("read leaf"),
        leaf_before,
        "{point:?} (killed at {phase_at_kill}): the restart already held a usable leaf and \
         must not have re-enrolled"
    );
    if let Some(node_id) = node_id_at_kill {
        assert_eq!(
            body["node_id"], node_id,
            "{point:?} (killed at {phase_at_kill}): the restart must resume the stamped node id"
        );
    }
    // Converged means converged: whatever refusal or hold the interrupted
    // attempt left behind — a key transfer aimed at a leader that has since
    // moved, a seat that was full when this replica last asked — is cleared
    // once the loop observes the seat, so an operator reading `/readyz`
    // afterwards sees no outstanding problem (ADR 0037 §9). Polled, not read
    // once: `phase` flips the instant membership shows the seat, while the
    // notices are cleared by the loop's next pass — a window one tick wide
    // that a single sample can land inside under CI load.
    poll(
        Duration::from_secs(10),
        "the converged replica clears its refusal and hold on the next loop pass",
        || async {
            let body = joiner.readyz().await.1;
            body["last_admission_refusal"].is_null() && body["promotion_hold"].is_null()
        },
    )
    .await;

    // One seat ever existed, and it is the joiner's: asserted from the
    // leader's side of membership, which is the side that decides.
    let node_id = body["node_id"]
        .as_u64()
        .expect("a converged replica has an id");
    let leader_body = leader.readyz().await.1;
    let voters = leader_body["voters"].as_array().expect("voters");
    assert_eq!(
        voters.len(),
        2,
        "{point:?} (killed at {phase_at_kill}): {leader_body}"
    );
    assert!(
        voters.iter().any(|v| v["node_id"] == node_id),
        "{point:?}: the pre-kill seat must be the one that converged: {leader_body}"
    );

    joiner.stop().await.expect("joiner stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}

/// Drive `joiner` to `point` and return the `/readyz` body observed there.
///
/// Each arm waits on the *evidence* that the point has been reached, never on
/// a sleep: files on disk, a phase, or the leader's own machine-readable
/// promotion hold.
async fn stage_kill_point(
    point: KillPoint,
    joiner: &Daemon,
    root: &std::path::Path,
) -> serde_json::Value {
    match point {
        KillPoint::AfterEnroll => {
            let root = root.to_path_buf();
            let identity_path = joiner
                .data_dir()
                .join(coppice_tls::pki::machine::MACHINE_IDENTITY_FILE);
            poll(
                Duration::from_secs(20),
                "the joiner enrolls: leaf and machine identity on disk",
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
            // With no discovery there is nothing to join, so the kill is
            // provably between enrollment and admission.
            let body = joiner.readyz().await.1;
            assert_eq!(
                body["phase"], "waiting",
                "a joiner with no seed list cannot have started joining: {body}"
            );
            body
        }
        KillPoint::BeforeAddLearner
        | KillPoint::AddLearnerIssued
        | KillPoint::PromoteVoterIssued => {
            let name = point.failpoint().expect("a failpoint-staged point");
            // The halt marker, not a phase: it is durable and it is written by
            // the very line the kill is meant to land on, so there is no race
            // to lose and no doubt about where the daemon stopped. Everything
            // but the convergence loop is still serving, which is why `/readyz`
            // can still be read for the state at the halt.
            joiner.await_halted_at(name).await;
            if point == KillPoint::BeforeAddLearner {
                // The one point whose phase is not merely bounded but *fixed*:
                // nobody was ever asked to admit this replica, so nothing can.
                // Ten probe intervals later it is still `joining` — which is
                // the assertion that the failpoint STOPS the loop rather than
                // delaying it, and the one the old phase-poll staging could
                // never make.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let body = joiner.readyz().await.1;
            let phase = body["phase"].as_str().unwrap_or_default();
            assert!(
                point.phases_at_halt().contains(&phase),
                "{point:?}: halted at {name} in phase {phase}, which is past the boundary \
                 this point brackets: {body}"
            );
            body
        }
        KillPoint::CaughtUpLearner => {
            joiner.await_phase("learner").await;
            // Admitted is not enough: the promotion must be *pending*, which
            // the leader says by refusing it with the §7 machine-readable
            // hold. That refusal is proof this learner caught up and asked.
            poll(
                Duration::from_secs(30),
                "the leader holds the joiner's promotion at the voter-set ceiling",
                || async {
                    joiner.readyz().await.1["promotion_hold"]
                        .as_str()
                        .is_some_and(|hold| {
                            hold.starts_with("voter-set-full")
                                || hold.starts_with("no-removable-peer")
                        })
                },
            )
            .await;
            let body = joiner.readyz().await.1;
            assert_eq!(body["phase"], "learner", "{body}");
            body
        }
        KillPoint::JustPromoted => joiner.await_phase("voter").await,
    }
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
    // Four settled-interval ticks of the newcomer's own convergence loop
    // (250ms under the fixture's `[pacing]`), and twenty probe rounds: it has
    // asked to be promoted repeatedly and been held every time.
    tokio::time::sleep(Duration::from_secs(1)).await;
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

// ---------------------------------------------------------------------------
// Contended seats and moving leaders
// ---------------------------------------------------------------------------

/// Two fresh joiners race for **one** vacancy (ADR 0037 §6/§7).
///
/// `two_racing_newcomers_both_converge_to_distinct_voter_seats` races two
/// joiners for two seats, where the only question is whether they trip over
/// each other's probe answers. This one removes the second seat, which makes
/// the interesting question the opposite one: with both learners caught up and
/// both asking, the leader must promote exactly **one**. The failure modes it
/// excludes are the two that matter — over-promotion (a voter set past
/// `cluster_size`, i.e. a quorum the operator never sized for) and a wedge (a
/// loser that stops converging, or reports an operator-actionable refusal for
/// what is really a wait).
///
/// The loser's state is then shown to be a *hold* and not a dead end: a voter
/// is killed, and once it has been unreachable for longer than `removal_grace`
/// the leader folds it out and promotes the loser into its seat — hands off,
/// with no operator verb and nothing restarted. A cluster that permanently
/// stranded the loser would pass every assertion above this line.
///
/// Staging: `cluster_size = 3` with two voters up, so exactly one seat is
/// open, and a short `removal_grace` so the later vacancy is test-sized.
/// Killing a non-leader voter leaves two of three live, so the leader keeps
/// quorum throughout and the promotion that follows is a real one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_joiners_racing_for_one_vacancy_promote_exactly_one_and_the_loser_waits() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    let mut leader = Daemon::new_certless(cluster_id, &ca);
    leader.set_cluster_size(3);
    leader.set_removal_grace("2s");
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    // The second voter, so the three-seat cluster has exactly one seat left.
    let mut second = newcomer(cluster_id, &ca, &leader, &token, 3);
    second.set_removal_grace("2s");
    second.start();
    second.await_phase("voter").await;

    // The contenders: shape-identical, started back to back, each seeded with
    // both sitting voters. Nothing distinguishes them, which is the point —
    // the seat must be decided by the leader, not by the configs.
    let mut first_racer = Daemon::new_certless(cluster_id, &ca);
    let mut second_racer = Daemon::new_certless(cluster_id, &ca);
    for racer in [&first_racer, &second_racer] {
        racer.set_cluster_size(3);
        racer.set_removal_grace("2s");
        racer.set_enrollment(&leader.api(""), &token);
        racer.set_static_discovery(&[leader.raft_target(), second.raft_target()]);
    }
    first_racer.start();
    second_racer.start();

    // Exactly one reaches `voter`; the other is admitted (`learner`) and held.
    poll(
        Duration::from_secs(120),
        "one racer takes the last seat and the other is held as a learner",
        || async {
            let a = first_racer.readyz().await.1;
            let b = second_racer.readyz().await.1;
            let phases = [a["phase"].clone(), b["phase"].clone()];
            let voters = phases.iter().filter(|p| **p == "voter").count();
            let learners = phases.iter().filter(|p| **p == "learner").count();
            voters == 1 && learners == 1
        },
    )
    .await;

    let a = first_racer.readyz().await.1;
    let (winner, loser) = if a["phase"] == "voter" {
        (&first_racer, &second_racer)
    } else {
        (&second_racer, &first_racer)
    };
    let winner_id = winner.readyz().await.1["node_id"]
        .as_u64()
        .expect("the winner is stamped");
    let loser_id = loser.readyz().await.1["node_id"]
        .as_u64()
        .expect("the loser is stamped: it is in membership as a learner");
    assert_ne!(winner_id, loser_id);

    // Four settled ticks of the loser's loop (250ms each under the fixture's
    // `[pacing]`): it has re-offered itself repeatedly and been held every
    // time, and in that window nothing over-promoted.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let loser_body = loser.readyz().await.1;
    assert_eq!(
        loser_body["phase"], "learner",
        "the ceiling must hold the loser: {loser_body}"
    );
    // A wait, not a dead end: §7 keeps the two fields distinct, because only
    // one of them is something an operator must act on. Losing the race must
    // never leave a *terminal* refusal — those are the ones the loop backs off
    // hard on and an operator has to resolve by hand.
    //
    // Not asserted null, deliberately: the concurrent promotion window really
    // does produce a transient `endpoint-unverified` here (the loser's tick
    // can land its promotion on a peer that is being promoted itself, and the
    // CA-key transfer that only a leader may perform is refused), and
    // `last_admission_refusal` keeps the last such message until convergence
    // clears it. That is retried at the fast cadence by design, and the
    // assertion after the seat opens proves it was transient.
    if let Some(refusal) = loser_body["last_admission_refusal"].as_str() {
        for terminal in [
            coppice_coordinator::admin::MACHINE_IDENTITY_CONFLICT,
            coppice_coordinator::admin::MACHINE_ADDRESS_CONFLICT,
            coppice_coordinator::admin::ADDRESS_CONFLICT,
            coppice_coordinator::admin::UNKNOWN_NODE,
            coppice_coordinator::admin::HISTORY_CONFLICT,
            coppice_coordinator::admin::IDENTITY_RETIRED,
        ] {
            assert!(
                !coppice_coordinator::admin::has_marker(refusal, terminal),
                "losing a seat race must never be a terminal refusal ({terminal}): {refusal}"
            );
        }
    }
    let hold = loser_body["promotion_hold"]
        .as_str()
        .unwrap_or_else(|| panic!("a held promotion must surface its reason: {loser_body}"));
    assert!(
        hold.starts_with("voter-set-full") || hold.starts_with("no-removable-peer"),
        "the hold must carry the leader's machine-readable marker: {hold}"
    );
    // No over-promotion, asserted from the leader's side of membership.
    let leader_body = leader.readyz().await.1;
    let voters = leader_body["voters"].as_array().expect("voters");
    assert_eq!(
        voters.len(),
        3,
        "the voter set must not have grown past cluster_size: {leader_body}"
    );
    assert!(
        voters.iter().any(|v| v["node_id"] == winner_id),
        "the winner holds the contested seat: {leader_body}"
    );
    assert!(
        !voters.iter().any(|v| v["node_id"] == loser_id),
        "the loser must not hold a seat: {leader_body}"
    );

    // ---- a seat opens ------------------------------------------------------
    // The second voter dies. Once it has been out of replication contact for
    // longer than `removal_grace`, the leader folds it out on its own
    // evidence and the loser's standing offer is finally accepted.
    let dead_id = second.readyz().await.1["node_id"]
        .as_u64()
        .expect("the dying voter is stamped");
    second.kill().await;

    loser.await_phase("voter").await;
    // Converged means converged: whatever transient refusal the race left in
    // status is cleared, so an operator reading `/readyz` afterwards sees no
    // outstanding problem (ADR 0037 §9). Polled, not read once: the phase
    // flips the instant membership is observed, while the hold and refusal
    // fields are cleared by the loop's next pass — a window one probe
    // interval wide that a single sample can land inside.
    poll(
        Duration::from_secs(10),
        "the converged loser clears its refusal and hold on the next loop pass",
        || async {
            let body = loser.readyz().await.1;
            body["last_admission_refusal"].is_null() && body["promotion_hold"].is_null()
        },
    )
    .await;
    poll(
        Duration::from_secs(60),
        "the leader's membership shows the loser in the dead voter's seat",
        || async {
            let body = leader.readyz().await.1;
            let voters = body["voters"].as_array().cloned().unwrap_or_default();
            voters.len() == 3
                && voters.iter().any(|v| v["node_id"] == loser_id)
                && !voters.iter().any(|v| v["node_id"] == dead_id)
        },
    )
    .await;

    first_racer.stop().await.expect("first racer stops cleanly");
    second_racer
        .stop()
        .await
        .expect("second racer stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}

/// The leader changes while a join is **in flight** (ADR 0037 §6).
///
/// `a_stale_hint_naming_a_dead_leader_does_not_wedge_a_joiner` kills a leader
/// while the joiner is *parked* — the pre-start half of the loop, holding no
/// consensus state. This test kills one while the joiner is genuinely mid-join:
/// stamped, running consensus under `Join`, admitted as a learner, caught up,
/// and with its promotion already asked for and pending. Everything the loop
/// knows at that moment came from the leader that is about to die.
///
/// The claim is the idempotency contract's: the join is a stateless pass, so
/// the new leader completes it without knowing anything about the old one's
/// half-finished work, and the joiner never re-enrolls, never re-stamps, and
/// never loses its node id.
///
/// `cluster.rs::a_mid_join_newcomer_converges_via_the_new_leader_after_the_old_one_stops`
/// asks a neighbouring question — that dial targets re-derive from local
/// membership when a leader *stops gracefully* — and stages it by racing a poll
/// against the join, accepting whichever of `joining`/`learner`/`voter` it
/// catches. Both are kept: that one is about finding the new leader, this one
/// is about the new leader finishing work it never started, from a mid-join
/// state that is held rather than caught, with the old leader killed rather
/// than drained.
///
/// Staging, and why it is deterministic: `cluster_size` is **node-local**
/// config (ADR 0020 / §2), so a fleet mid-rolling-change genuinely holds
/// different values. The forming node is given a ceiling of 3 and the other two
/// voters a ceiling of 4. While the founder leads, the joiner is admitted,
/// catches up, and is held at the ceiling — a stable, observable mid-join
/// state, not a window to race. Killing the founder hands leadership to a
/// ceiling-4 voter (two of three remain, so quorum survives), and the very next
/// pass of the joiner's loop lands on a leader that can seat it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_change_mid_join_is_completed_by_the_new_leader() {
    init_tracing();
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // The founder: ceiling 3, so it will admit the joiner and hold it.
    let mut founder = Daemon::new_certless(cluster_id, &ca);
    founder.set_cluster_size(3);
    let operator = form(&mut founder).await;
    let token = coordinator_token(&founder, &operator).await;

    // The two successors: ceiling 4, so whichever inherits leadership can seat
    // the joiner.
    let mut second = newcomer(cluster_id, &ca, &founder, &token, 4);
    let mut third = newcomer(cluster_id, &ca, &founder, &token, 4);
    second.start();
    second.await_phase("voter").await;
    third.start();
    third.await_phase("voter").await;

    // The joiner, seeded with all three sitting voters.
    let mut joiner = Daemon::new_certless(cluster_id, &ca);
    joiner.set_cluster_size(4);
    joiner.set_enrollment(&founder.api(""), &token);
    joiner.set_static_discovery(&[
        founder.raft_target(),
        second.raft_target(),
        third.raft_target(),
    ]);
    joiner.start();

    // Mid-join, established rather than assumed: `learner` means the leader
    // admitted it into membership (consensus engaged, ADR 0037 §9), and the
    // hold means it has caught up and asked to be promoted. This is not a
    // parked daemon and not a converged one.
    joiner.await_phase("learner").await;
    poll(
        Duration::from_secs(60),
        "the joiner is a caught-up learner whose promotion the founder is holding",
        || async {
            joiner.readyz().await.1["promotion_hold"]
                .as_str()
                .is_some_and(|hold| {
                    hold.starts_with("voter-set-full") || hold.starts_with("no-removable-peer")
                })
        },
    )
    .await;
    let staged = joiner.readyz().await.1;
    assert_eq!(staged["phase"], "learner", "{staged}");
    let joiner_id = staged["node_id"].as_u64().expect("the joiner is stamped");
    let identity_before =
        std::fs::read(machine_identity_path(&joiner)).expect("read machine identity");

    // The staging depends on the founder being the leader when it dies — the
    // ceiling that is holding the joiner is *its* config — so that is checked,
    // never assumed.
    let founder_body = founder.readyz().await.1;
    assert_eq!(
        founder_body["is_leader"], true,
        "the ceiling holding this join is the founder's, so the founder must be leading it: \
         {founder_body}"
    );
    let founder_id = founder_body["node_id"]
        .as_u64()
        .expect("the founder is stamped");

    // ---- the leader dies mid-join ------------------------------------------
    founder.kill().await;

    let body = joiner.await_phase("voter").await;
    assert_eq!(
        body["node_id"], joiner_id,
        "the join must be completed under the identity it began with, not a new one: {body}"
    );
    assert_eq!(
        std::fs::read(machine_identity_path(&joiner)).expect("read machine identity"),
        identity_before,
        "a leader change is not a reason to re-enroll or re-mint a machine identity"
    );

    // A new leader really did complete it, and the dead founder keeps its seat
    // (nothing removed it: a removal is evidence-gated and the grace is the
    // fixture's 120s default), so the converged set is four.
    poll(
        Duration::from_secs(60),
        "a surviving voter leads, and the joiner sits in the fourth seat",
        || async {
            for survivor in [&second, &third] {
                let body = survivor.readyz().await.1;
                let voters = body["voters"].as_array().cloned().unwrap_or_default();
                if body["is_leader"] == true
                    && body["node_id"].as_u64() != Some(founder_id)
                    && voters.len() == 4
                    && voters.iter().any(|v| v["node_id"] == joiner_id)
                {
                    return true;
                }
            }
            false
        },
    )
    .await;

    joiner.stop().await.expect("joiner stops cleanly");
    second.stop().await.expect("second stops cleanly");
    third.stop().await.expect("third stops cleanly");
}
