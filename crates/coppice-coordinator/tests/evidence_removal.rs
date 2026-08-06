//! Evidence-gated removal and stale-learner GC (ADR 0037 §7).
//!
//! §7's whole point is that voter membership never shrinks on its own: it
//! shrinks only inside `ReplaceVoter`, an evidence-gated promotion, or an
//! explicit `admin remove`. These tests exercise the one background path that
//! *is* automatic — the hands-off replacement of a dead voter by a fresh
//! installation, folded into the ordinary promotion a converging learner
//! already drives — and its two guardrails: a live predecessor never
//! qualifies as evidence-dead, and a promotion that would exceed
//! `cluster_size` with no qualifying candidate is refused machine-readably
//! rather than ever shrinking the wrong way. The stale-learner GC task is the
//! other automatic membership change these tests cover, with its own
//! guardrail: expiry is failed contact, never lack of log advancement.
//!
//! Every fleet here is certless (ADR 0037 §4/§1): the newcomer in each test is
//! a literal fresh installation — its own tempdir, its own machine identity —
//! never a member `Fleet::new` already knew about, because that is exactly
//! the hands-off case §7 describes.

mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use coppice_coordinator::admin::{self, has_marker, IDENTITY_RETIRED, NO_REMOVABLE_PEER};
use coppice_coordinator::localadmin::OperatorPem;
use coppice_proto::pb::raft::v1 as pb;

use common::{poll, Ca, Daemon, Fleet};

type AdminClient = coppice_net::admin::Client<tonic::transport::Channel>;

/// Per-binary tracing, mirroring the other multi-node suites.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Dial `daemon`'s admin surface presenting the operator credential
/// `Fleet::init` minted.
async fn operator_client(daemon: &Daemon, operator: &OperatorPem) -> AdminClient {
    admin::admin_channel(
        &daemon.raft_target(),
        operator.ca_pem.as_bytes(),
        operator.cert_pem.as_bytes(),
        operator
            .key_pem
            .as_ref()
            .expect("no CSR was supplied, so the cluster minted the keypair")
            .as_bytes(),
    )
    .await
    .expect("dial the admin surface")
}

/// This cluster's stamped history id, read off any already-formed member's own
/// `ProbeCluster` answer (which needs no history match to answer).
async fn history_id_of(daemon: &Daemon) -> [u8; 16] {
    let (ca, cert, key) = daemon.tls_material();
    let resp = daemon
        .probe(&ca, &cert, &key)
        .await
        .expect("probe the formed cluster for its history id");
    resp.history_id
        .try_into()
        .unwrap_or_else(|v: Vec<u8>| panic!("history id must be 16 bytes, got {}", v.len()))
}

/// An admin client dialed to whichever fleet member currently reports itself
/// leader — the only replica that can answer `PromoteVoter`/evidence
/// questions authoritatively.
async fn leader_admin_client(fleet: &Fleet, operator: &OperatorPem, hid: [u8; 16]) -> AdminClient {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for member in fleet.members.iter().filter(|m| m.is_running()) {
            let mut client = operator_client(member, operator).await;
            if let Ok(status) = admin::cluster_status(&mut client, hid).await {
                if status.leader_node_id == Some(status.local_node_id) {
                    return client;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "no fleet member ever reported itself leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The voter node ids a `/readyz` body reports.
fn voter_ids(body: &serde_json::Value) -> BTreeSet<u64> {
    body["voters"]
        .as_array()
        .map(|v| v.iter().filter_map(|x| x["node_id"].as_u64()).collect())
        .unwrap_or_default()
}

/// This replica's own stamped raft node id, from its `/readyz` body.
fn node_id(body: &serde_json::Value) -> u64 {
    body["node_id"]
        .as_u64()
        .unwrap_or_else(|| panic!("a started replica must report a node id: {body}"))
}

/// Every node id (voter or learner) a `ClusterStatus` answer's membership
/// carries — unlike `/readyz voters`, this includes learners.
fn membership_ids(status: &pb::ClusterStatusResponse) -> BTreeSet<u64> {
    status
        .membership
        .as_ref()
        .map(|m| m.members.iter().map(|n| n.node_id).collect())
        .unwrap_or_default()
}

/// A 3-voter fleet, formed and converged, with `removal_grace`/`learner_expiry`
/// shrunk to `tune` on every member (including any [`Fleet::add_member`]
/// appends later, which callers must tune themselves).
async fn formed_trio(ca: &Ca, tune: impl Fn(&Daemon)) -> (Fleet, OperatorPem) {
    let mut fleet = Fleet::new(3, ca);
    for member in &fleet.members {
        tune(member);
    }
    fleet.start_all();
    for member in &fleet.members {
        member.await_phase("waiting").await;
    }
    let operator = fleet.init().await;
    fleet.await_voters(3).await;
    (fleet, operator)
}

/// ADR 0037 §7, the hands-off replacement path in full: terminate a voter,
/// launch a fresh installation, and — with no operator call anywhere in
/// between — the cluster converges back to a full voter set with the
/// newcomer in the dead voter's place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_voters_seat_is_retaken_hands_off_by_a_fresh_installation() {
    init_tracing();
    let ca = Ca::new();
    let (mut fleet, _operator) = formed_trio(&ca, |m| m.set_removal_grace("2s")).await;

    // Terminate one voter — process death, no shutdown signal.
    let killed_id = node_id(&fleet.members[1].readyz().await.1);
    fleet.members[1].kill().await;

    // Launch a fresh installation: its own tempdir, its own machine identity,
    // the same shape config every other member carries.
    let newcomer = fleet.add_member(&ca);
    fleet.members[newcomer].set_removal_grace("2s");
    fleet.members[newcomer].start();

    // No operator call follows. The newcomer enrolls, joins as a learner, and
    // the leader's evidence-gated removal folds the corpse out the instant
    // the newcomer's own convergence loop tries to promote past the
    // cluster_size ceiling.
    let survivors = [0usize, 2, newcomer];
    poll(
        Duration::from_secs(60),
        "the fleet retakes the dead voter's seat hands-off, with no operator call",
        || async {
            let mut converged = true;
            for &idx in &survivors {
                let (_, body) = fleet.members[idx].readyz().await;
                let voters = voter_ids(&body);
                if body["phase"] != "voter" || voters.len() != 3 || voters.contains(&killed_id) {
                    converged = false;
                }
            }
            converged
        },
    )
    .await;

    // The corpse's node id is gone from every survivor's voter set; the
    // newcomer's own id has taken its place.
    for &idx in &survivors {
        let (_, body) = fleet.members[idx].readyz().await;
        let voters = voter_ids(&body);
        assert_eq!(voters.len(), 3, "member {idx}: {body}");
        assert!(
            !voters.contains(&killed_id),
            "member {idx} still lists the dead voter: {body}"
        );
    }
    let newcomer_id = node_id(&fleet.members[newcomer].readyz().await.1);
    assert!(voter_ids(&fleet.members[0].readyz().await.1).contains(&newcomer_id));

    fleet.stop_all().await;
}

/// ADR 0037 §7: evidence is the leader's own replication observation, full
/// stop. Three live voters never yield a removal candidate no matter how long
/// a caught-up learner waits, so it stays a learner and the voter set never
/// moves — the refusal is machine-readable (`no-removable-peer`), not a
/// silent stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_predecessor_never_qualifies_as_evidence_dead() {
    init_tracing();
    let ca = Ca::new();
    let (mut fleet, operator) = formed_trio(&ca, |m| m.set_removal_grace("2s")).await;

    let original_voters = voter_ids(&fleet.members[0].readyz().await.1);
    assert_eq!(original_voters.len(), 3);

    let learner = fleet.add_member(&ca);
    fleet.members[learner].set_removal_grace("2s");
    fleet.members[learner].start();
    fleet.members[learner].await_phase("learner").await;

    // Comfortably longer than several `removal_grace` windows: nothing here
    // is a race against the leader's own evidence clock, because no voter
    // ever stops answering.
    tokio::time::sleep(Duration::from_secs(10)).await;

    for member in [&fleet.members[0], &fleet.members[2]] {
        let (_, body) = member.readyz().await;
        assert_eq!(
            voter_ids(&body),
            original_voters,
            "the voter set must not move while every voter is live: {body}"
        );
    }
    let learner_body = fleet.members[learner].readyz().await.1;
    assert_eq!(
        learner_body["phase"], "learner",
        "the caught-up learner must still be a learner: {learner_body}"
    );

    // The refusal is machine-readable, not merely an absence of progress:
    // asking the leader directly to promote the learner is refused with the
    // `no-removable-peer` marker, and asking again still is (it keeps
    // polling, not backing off to a terminal state).
    let hid = history_id_of(&fleet.members[0]).await;
    let learner_id = node_id(&learner_body);
    for _ in 0..2 {
        let mut leader = leader_admin_client(&fleet, &operator, hid).await;
        let status = leader
            .promote_voter(pb::PromoteVoterRequest {
                history_id: hid.to_vec(),
                promote_node_id: learner_id,
            })
            .await
            .expect_err("promotion must be refused: three live voters, no dead candidate");
        assert!(
            has_marker(status.message(), NO_REMOVABLE_PEER),
            "expected the {NO_REMOVABLE_PEER:?} marker, got {:?}",
            status.message()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    fleet.stop_all().await;
}

/// ADR 0037 §7: the same overfull-and-no-dead-candidate refusal as above, but
/// asserted specifically on the machine-readable marker the leader returns,
/// and carried through to its strongest form — the refusal is not permanent:
/// the moment a voter actually dies, the very same learner (still polling)
/// ascends without any other change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_overfull_voter_set_with_no_dead_candidate_refuses_promotion_machine_readably() {
    init_tracing();
    let ca = Ca::new();
    let (mut fleet, operator) = formed_trio(&ca, |m| m.set_removal_grace("2s")).await;

    let learner = fleet.add_member(&ca);
    fleet.members[learner].set_removal_grace("2s");
    fleet.members[learner].start();
    fleet.members[learner].await_phase("learner").await;

    let hid = history_id_of(&fleet.members[0]).await;
    let learner_id = node_id(&fleet.members[learner].readyz().await.1);

    // The refusal window: several `removal_grace` periods with all three
    // voters alive, the marker asserted directly against the leader.
    tokio::time::sleep(Duration::from_secs(6)).await;
    {
        let mut leader = leader_admin_client(&fleet, &operator, hid).await;
        let status = leader
            .promote_voter(pb::PromoteVoterRequest {
                history_id: hid.to_vec(),
                promote_node_id: learner_id,
            })
            .await
            .expect_err("promotion must be refused while the voter set is full and live");
        assert_eq!(
            status.code(),
            tonic::Code::FailedPrecondition,
            "no-removable-peer is a settled-interval refusal, not a hard denial: {status:?}"
        );
        assert!(
            has_marker(status.message(), NO_REMOVABLE_PEER),
            "expected the {NO_REMOVABLE_PEER:?} marker, got {:?}",
            status.message()
        );
    }
    assert_eq!(
        fleet.members[learner].readyz().await.1["phase"],
        "learner",
        "the learner must still be polling, not have given up"
    );

    // Now a voter actually dies. The learner (never told anything new) keeps
    // polling at its own cadence and ascends the moment the leader has
    // evidence to fold the removal into its promotion.
    let killed_id = node_id(&fleet.members[1].readyz().await.1);
    fleet.members[1].kill().await;

    let survivors = [0usize, 2, learner];
    poll(
        Duration::from_secs(60),
        "the previously-refused learner ascends once a voter actually dies",
        || async {
            let mut converged = true;
            for &idx in &survivors {
                let (_, body) = fleet.members[idx].readyz().await;
                let voters = voter_ids(&body);
                if body["phase"] != "voter" || voters.len() != 3 || voters.contains(&killed_id) {
                    converged = false;
                }
            }
            converged
        },
    )
    .await;
    assert!(
        voter_ids(&fleet.members[0].readyz().await.1).contains(&learner_id),
        "the formerly-refused learner must now be a voter"
    );

    fleet.stop_all().await;
}

/// ADR 0037 §7 last paragraph: a learner with no successful replication
/// contact for longer than `learner_expiry` is garbage-collected — its
/// binding retired before its seat is released, so a re-arriving installation
/// carrying that identity is refused rather than silently re-admitted
/// (one-seat-ever).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learner_without_replication_contact_expires_and_its_identity_is_retired() {
    init_tracing();
    let ca = Ca::new();
    let (mut fleet, operator) = formed_trio(&ca, |m| m.set_learner_expiry("4s")).await;

    let learner = fleet.add_member(&ca);
    fleet.members[learner].set_learner_expiry("4s");
    fleet.members[learner].start();
    fleet.members[learner].await_phase("learner").await;
    let learner_id = node_id(&fleet.members[learner].readyz().await.1);

    // No shutdown signal: the leader's contact tracker sees failed
    // heartbeat/append acknowledgement, never a graceful goodbye.
    fleet.members[learner].kill().await;

    let hid = history_id_of(&fleet.members[0]).await;
    // Tick period is min(learner_expiry/4, 60s) = 1s here; a generous deadline
    // covers the expiry window plus several sweeps.
    poll(
        Duration::from_secs(30),
        "learner-gc removes the expired, contactless learner",
        || async {
            let mut client = operator_client(&fleet.members[0], &operator).await;
            match admin::cluster_status(&mut client, hid).await {
                Ok(status) => !membership_ids(&status).contains(&learner_id),
                Err(_) => false,
            }
        },
    )
    .await;

    // The binding is retired (mark, not delete): it is still visible in
    // custody/binding accounting...
    {
        let mut client = operator_client(&fleet.members[0], &operator).await;
        let status = admin::cluster_status(&mut client, hid)
            .await
            .expect("cluster status after gc");
        assert!(
            status.bindings.iter().any(|b| b.node_id == learner_id),
            "a retired binding must be marked, not deleted: {:?}",
            status.bindings
        );
    }

    // ...and §7 one-seat-ever: the identical installation, restarted from the
    // very same data directory, is refused re-admission with the
    // machine-readable `identity-retired` marker (retirement is not surfaced
    // as a wire field, so this is the observable proof).
    fleet.members[learner].start();
    poll(
        Duration::from_secs(30),
        "the retired identity is refused re-admission",
        || async {
            let (_, body) = fleet.members[learner].readyz().await;
            body["last_admission_refusal"]
                .as_str()
                .is_some_and(|m| has_marker(m, IDENTITY_RETIRED))
        },
    )
    .await;
    // Its own local view is a permanently stale artifact of the membership it
    // held before it died (it is no longer a replication target, so it never
    // receives the log entry that removed it) — `phase: learner` here is that
    // stale belief, not a re-admission. The authoritative check is the
    // leader's: the retired identity never re-enters membership no matter how
    // long its convergence loop keeps retrying.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut client = operator_client(&fleet.members[0], &operator).await;
    let status = admin::cluster_status(&mut client, hid)
        .await
        .expect("cluster status after the retired identity's restart");
    assert!(
        !membership_ids(&status).contains(&learner_id),
        "a retired identity must never be re-admitted to membership: {:?}",
        status.membership
    );

    fleet.stop_all().await;
}

/// ADR 0037 §7 last paragraph, the converse case verbatim: an idle
/// caught-up learner that keeps acknowledging heartbeats survives
/// indefinitely, because the GC criterion is failed contact, never lack of
/// log advancement. No command traffic crosses the cluster at all here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_caught_up_learner_survives_on_heartbeats_alone() {
    init_tracing();
    let ca = Ca::new();
    let (mut fleet, operator) = formed_trio(&ca, |m| m.set_learner_expiry("4s")).await;

    let learner = fleet.add_member(&ca);
    fleet.members[learner].set_learner_expiry("4s");
    fleet.members[learner].start();
    fleet.members[learner].await_phase("learner").await;
    let learner_id = node_id(&fleet.members[learner].readyz().await.1);

    // Several expiry periods, fully idle: no job submission, no membership
    // verb, nothing but the raft heartbeat traffic every member already runs.
    tokio::time::sleep(Duration::from_secs(12)).await;

    let body = fleet.members[learner].readyz().await.1;
    assert_eq!(
        body["phase"], "learner",
        "an idle, acking learner must survive on heartbeats alone: {body}"
    );
    assert_eq!(node_id(&body), learner_id);

    let hid = history_id_of(&fleet.members[0]).await;
    let mut client = operator_client(&fleet.members[0], &operator).await;
    let status = admin::cluster_status(&mut client, hid)
        .await
        .expect("cluster status");
    assert!(
        membership_ids(&status).contains(&learner_id),
        "the idle caught-up learner must still be in membership: {:?}",
        status.membership
    );

    fleet.stop_all().await;
}
