//! `ReplaceVoter`, the operator-authenticated launch-before-terminate verb
//! (ADR 0037 §7).
//!
//! Four claims, each staged against a real self-converging fleet:
//!
//! 1. A `new` that has not caught up is refused (`learner-behind`) — the same
//!    catch-up gate promotion uses, so `ReplaceVoter` can never overshoot the
//!    voter count with a candidate that cannot yet serve.
//! 2. A `new`/`old` pair where `old` is perfectly alive succeeds in one joint
//!    change — "launch-before-terminate" is the whole point of the verb — and
//!    the identical call replayed afterwards is a no-op success (§6).
//! 3. A predecessor's crash, raced against nobody: the leader's own
//!    evidence-gated fold-in (§7's hands-off path) completes the replacement
//!    with no operator call at all, and a `ReplaceVoter` that arrives after
//!    the fact still succeeds as the idempotent no-op it always is.
//! 4. The single-voter case: the founder vanishes the instant the change
//!    commits, and the surviving voter must provably hold the CA key it
//!    needs to keep signing — proven here by having it sign an operator
//!    certificate over its own local admin socket.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use coppice_consensus::Consensus;
use coppice_coordinator::admin::{self, has_marker, LEARNER_BEHIND};
use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::id::{ClusterId, EnrollTokenId};
use coppice_core::time::Timestamp;
use coppice_proto::pb::core::v1 as pbcore;
use coppice_proto::pb::raft::v1 as pb;
use coppice_state::command::MintEnrollToken;
use coppice_state::{Command, EnrollRole};

use common::{poll, Ca, Daemon, Fleet, Node};

type AdminClient = coppice_net::admin::Client<tonic::transport::Channel>;

/// Every test in this file drives at least one multi-daemon (often
/// multi-voter) cluster, and one drives tens of thousands of raft proposals;
/// run concurrently (the default under `cargo test`), several at once
/// oversubscribes a modest host badly enough to blow through the shared
/// harness's fixed polling deadlines — a resource problem, not a correctness
/// one, and not this suite's to fix in the harness. Each test acquires this
/// lock for its duration, so `cargo test` still parallelizes across test
/// *binaries* exactly as it always does, but the handful of heavy cases in
/// this one file run one at a time.
static SUITE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

async fn exclusive() -> tokio::sync::MutexGuard<'static, ()> {
    SUITE_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// Per-binary tracing, mirroring the other ADR 0037 suites: `run_with`
/// installs no subscriber (the binary's `run` does), so the harness supplies
/// one.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Form a cluster the only way one can be formed: park, then `init` over the
/// local admin socket (ADR 0037 §3).
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

/// Dial `daemon`'s admin surface presenting the operator credential `init` (or
/// `Fleet::init`) minted.
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
    .expect("dial the admin surface as an operator")
}

/// This cluster's stamped history id, learned the way every membership RPC
/// caller learns it: `ProbeCluster`.
async fn history_id_of(client: &mut AdminClient) -> [u8; 16] {
    client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe")
        .into_inner()
        .history_id
        .try_into()
        .expect("history id is 16 bytes")
}

/// Mint the coordinator enrollment token a fleet's config artifact carries
/// (ADR 0037 §5), using an operator client already dialed. Only safe where
/// leadership cannot move — the single-voter tests; a multi-voter fleet uses
/// [`fleet_coordinator_token`].
async fn coordinator_token(client: &mut AdminClient, history_id: [u8; 16]) -> String {
    client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id: history_id.to_vec(),
            role: pbcore::EnrollRole::Coordinator as i32,
            label: "coordinators".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("mint a coordinator enrollment token")
        .into_inner()
        .secret
}

/// [`coordinator_token`] for a multi-voter fleet: leadership can move between
/// resolving the leader and the mint (observed in CI), so a refused attempt
/// re-resolves the current leader and retries — the label-keyed duplicate a
/// retry could mint is harmless.
async fn fleet_coordinator_token(
    fleet: &Fleet,
    operator: &OperatorPem,
    history_id: [u8; 16],
) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut client = operator_client(find_leader(fleet).await, operator).await;
        match client
            .mint_enroll_token(pb::MintEnrollTokenRequest {
                history_id: history_id.to_vec(),
                role: pbcore::EnrollRole::Coordinator as i32,
                label: "coordinators".to_string(),
                ttl_seconds: None,
            })
            .await
        {
            Ok(resp) => return resp.into_inner().secret,
            Err(status) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "minting the fleet's coordinator token kept failing: {status}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// A fifth wheel: a certless daemon configured exactly as a fleet's identical
/// artifact would configure it, pointed at `leader` for both discovery and
/// enrollment.
fn newcomer(cluster_id: ClusterId, ca: &Ca, leader: &Daemon, token: &str, size: usize) -> Daemon {
    let daemon = Daemon::new_certless(cluster_id, ca);
    daemon.set_cluster_size(size);
    daemon.set_static_discovery(&[leader.raft_target()]);
    daemon.set_enrollment(&leader.api(""), token);
    daemon
}

/// This daemon's currently reported voter set, as node ids (from `/readyz`,
/// which — unlike raft membership at large — reports *voters only*: a
/// caught-up but unpromoted learner never appears here).
async fn voter_ids(daemon: &Daemon) -> BTreeSet<u64> {
    let (_, body) = daemon.readyz().await;
    body["voters"]
        .as_array()
        .expect("voters")
        .iter()
        .map(|v| v["node_id"].as_u64().expect("node_id"))
        .collect()
}

/// This daemon's own allocated raft identity, once it has one.
async fn node_id_of(daemon: &Daemon) -> u64 {
    let (_, body) = daemon.readyz().await;
    body["node_id"].as_u64().unwrap_or_else(|| {
        panic!("daemon has no node id yet: {body}");
    })
}

/// Whether `daemon` currently believes itself the leader.
async fn is_leader(daemon: &Daemon) -> bool {
    daemon.readyz().await.1["is_leader"] == true
}

/// Find the fleet member currently reporting itself leader — required rather
/// than assumed, because `Fleet::init` forms on member 0 but leadership can
/// move. Retried rather than asserted on the first look: the instant between
/// one member stepping down and another completing its election is a normal,
/// momentary gap, not a failure.
async fn find_leader(fleet: &Fleet) -> &Daemon {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        for member in &fleet.members {
            if is_leader(member).await {
                return member;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no fleet member reported itself leader within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Drive a non-settled `ReplaceVoter` to success, re-resolving the current
/// leader on every attempt.
///
/// `ReplaceVoter`'s settled (idempotent) case needs no leader at all — the §6
/// no-op returns straight from local state — but the first, *effecting* call
/// commits a joint change and so bounces off a follower with `not the
/// leader`. In a test process running several multi-voter fleets at once
/// (this file's other cases), an unrelated fleet's CPU load can cost this
/// one's leader an election it would otherwise have kept, so a leader
/// resolved once and cached can go stale between resolving it and issuing the
/// call. Re-resolving on every attempt makes that transient, not a failure.
async fn replace_voter_until_success(
    fleet: &Fleet,
    operator: &OperatorPem,
    history_id: [u8; 16],
    old_id: u64,
    new_id: u64,
) {
    poll(
        Duration::from_secs(30),
        "ReplaceVoter succeeds against the fleet's current leader",
        || async {
            let leader = find_leader(fleet).await;
            let mut client = operator_client(leader, operator).await;
            client
                .replace_voter(pb::ReplaceVoterRequest {
                    history_id: history_id.to_vec(),
                    old_node_id: old_id,
                    new_node_id: new_id,
                })
                .await
                .is_ok()
        },
    )
    .await;
}

// ---------------------------------------------------------------------------
// 1. A `new` that has not caught up is refused
// ---------------------------------------------------------------------------

/// A `ReplaceVoter` naming a `new` that has not yet replicated the log is
/// refused with the same `learner-behind` marker the promotion catch-up gate
/// uses (ADR 0037 §7: "`new_node_id` is a caught-up learner ... that has
/// confirmed durable receipt of the CA key").
///
/// Staged deterministically rather than by timing, and driven at the
/// [`Node`]/[`Command`] level rather than through a whole self-converging
/// daemon, for a reason established empirically against the real fixture: a
/// fresh learner on a fast host catches up an arbitrarily large *static*
/// backlog before an external client sitting behind the full convergence loop
/// (its own enroll/discover/probe/backoff cycle) can win the race to call
/// `ReplaceVoter` first — `SnapshotPolicy::LogsSinceLast` means a fresh
/// learner always installs exactly the *current* compacted snapshot, and only
/// a genuinely large **replicated state** (not merely a deep log) makes that
/// transfer+apply take measurable wall-clock time. So this test builds one
/// directly: tens of thousands of `MintEnrollToken` commands proposed straight
/// through [`Node::consensus`] — bypassing the admin surface's deliberately
/// expensive argon2 hashing entirely (ADR 0037 §5), which this test has no
/// interest in paying for — each landing a real `EnrollToken` record that
/// persists (revoked or not) for the life of the cluster. `AddLearner` is then
/// issued directly, followed **immediately**, with no other await between
/// them, by `ReplaceVoter`: the tightest window an external client can
/// achieve, racing a learner whose replication stream has had no chance to
/// even begin, against a snapshot payload now too large to install in that
/// window.
///
/// This is also the review-mandated "never keyed on refusal" claim (ADR 0037
/// §4: "A `new` that is refused here is never keyed and never appears in the
/// custody accounting"). The rest of this file's cases run against a whole
/// [`Daemon`]/[`Fleet`], which forms its own cluster CA; this one runs at the
/// bare [`Node`]/[`Command`] level with no cluster CA at all, so custody would
/// otherwise be vacuously absent from the claim. Custody is staged by hand
/// before the learner ever joins — a minted CA recorded in replicated state,
/// its key written to the founder's own data directory, and the founder's own
/// key-possession fact confirmed — exactly what a formed voter holds, so the
/// refusal below is checked against a cluster that actually has something to
/// strand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replace_voter_refuses_a_new_that_is_still_catching_up() {
    init_tracing();
    let _guard = exclusive().await;
    let ca = Ca::new();
    let admin_leaf = ca.operator_leaf();
    let cluster_id = ClusterId::new();
    let history_id = *cluster_id.0.as_bytes();

    let mut founder = Node::new(1, cluster_id, &ca);
    founder.boot().await;
    poll(
        Duration::from_secs(10),
        "the founder becomes leader",
        || async { founder.is_leader() },
    )
    .await;

    // Stage cluster custody by hand (ADR 0037 §4), before the learner joins:
    // the fixture's own CA (the one this founder's mTLS material already
    // chains to — recording a DIFFERENT minted CA here would make the
    // admin surface's own authorization plane start classifying every
    // caller, including this test's admin client, against a trust root
    // nothing in the fixture was signed under), its key on the founder's
    // disk, and the founder's own possession confirmed — exactly what
    // `check_change_postconditions`'s key-custody clause reads.
    coppice_tls::pki::write_ca_key(&founder.data_dir(), &ca.key_pem())
        .expect("write the CA key to the founder's data dir");
    let ca_recorded = founder
        .consensus()
        .propose(Command::RecordCaCertificate(
            coppice_state::command::RecordCaCertificate {
                bundle: coppice_state::CaCertBundle::parse(
                    std::str::from_utf8(&ca.pem).expect("CA cert PEM is UTF-8"),
                )
                .expect("the fixture CA parses"),
                recorded_at: Timestamp::now(),
            },
        ))
        .await
        .expect("record the CA");
    ca_recorded.outcome.expect("recording the CA is accepted");
    let confirmed = founder
        .consensus()
        .propose(Command::ConfirmKeyPossession(
            coppice_state::command::ConfirmKeyPossession {
                raft_node_id: founder.raft_id(),
                confirmed_at: Timestamp::now(),
            },
        ))
        .await
        .expect("confirm the founder's key possession");
    confirmed
        .outcome
        .expect("confirming founder key possession is accepted");

    // Tens of thousands of real `MintEnrollToken` commands, proposed directly
    // (no admin RPC, no argon2): each lands a persistent `EnrollToken` record,
    // so the founder's replicated state — and therefore a fresh learner's
    // snapshot payload — grows large fast.
    const BACKLOG: u32 = 10_000;
    for i in 0..BACKLOG {
        let applied = founder
            .consensus()
            .propose(Command::MintEnrollToken(MintEnrollToken {
                token: EnrollTokenId::new(),
                hash: format!("not-a-real-argon2-hash-{i}"),
                role: EnrollRole::Agent,
                label: format!("seed-{i}"),
                expires_at: None,
                minted_at: Timestamp::from_micros(i as i64).expect("in range"),
            }))
            .await
            .unwrap_or_else(|e| panic!("seed mint {i} failed: {e:?}"));
        assert!(
            applied.outcome.is_ok(),
            "seed mint {i} was rejected: {:?}",
            applied.outcome
        );
    }

    let mut learner = Node::new(2, cluster_id, &ca);
    learner.boot_joining().await;

    let mut client = admin::admin_channel(
        &founder.advertise,
        &ca.pem,
        &admin_leaf.cert_pem,
        &admin_leaf.key_pem,
    )
    .await
    .expect("dial the founder's admin surface");
    admin::add_learner(
        &mut client,
        history_id,
        learner.raft_id(),
        learner.advertise.clone(),
    )
    .await
    .expect("add the learner");

    // No other await between admission and this call: the tightest window
    // this test can achieve.
    let status = client
        .replace_voter(pb::ReplaceVoterRequest {
            history_id: history_id.to_vec(),
            old_node_id: founder.raft_id(),
            new_node_id: learner.raft_id(),
        })
        .await
        .expect_err(
            "a ReplaceVoter naming a learner that has not caught up must be refused, not \
             succeed — the catch-up gate exists precisely so a replacement cannot promote a \
             candidate unable to serve",
        );
    assert!(
        has_marker(status.message(), LEARNER_BEHIND),
        "expected the {LEARNER_BEHIND:?} marker, got ({:?}) {:?}",
        status.code(),
        status.message()
    );

    // Never keyed on refusal (ADR 0037 §4): the gate above ran before any key
    // transfer, so the refused learner's disk must hold no CA key at all.
    let learner_key_path = learner.data_dir().join(coppice_tls::pki::CA_KEY_FILE);
    assert!(
        !learner_key_path.exists(),
        "a refused ReplaceVoter must never have written a CA key to the learner's disk: {}",
        learner_key_path.display()
    );

    // ...and never appears in the custody accounting: `key_holders` names the
    // founder alone.
    let status_after = admin::cluster_status(&mut client, history_id)
        .await
        .expect("read cluster status after the refusal");
    let holder_ids: std::collections::BTreeSet<u64> =
        status_after.key_holders.iter().map(|h| h.node_id).collect();
    assert_eq!(
        holder_ids,
        std::collections::BTreeSet::from([founder.raft_id()]),
        "a refused new must never appear in key_holders: {status_after:?}"
    );

    learner.graceful_stop().await;
    founder.graceful_stop().await;
}

// ---------------------------------------------------------------------------
// 2. A live `old` succeeds, and the replay is a no-op
// ---------------------------------------------------------------------------

/// `ReplaceVoter` against a perfectly alive `old` succeeds in one joint
/// change — "launch-before-terminate" is the verb's whole purpose (ADR 0037
/// §7) — and the identical call, issued again afterwards, is a no-op success
/// (§6's idempotency contract: "`new` already voter AND `old` absent →
/// no-op").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replace_voter_with_a_live_old_succeeds_and_is_idempotent() {
    init_tracing();
    let _guard = exclusive().await;
    let ca = Ca::new();
    let mut fleet = Fleet::new(3, &ca);
    fleet.start_all();
    for member in &fleet.members {
        member.await_phase("waiting").await;
    }
    let operator = fleet.init().await;
    fleet.await_voters(3).await;

    let leader = find_leader(&fleet).await;
    let mut leader_client = operator_client(leader, &operator).await;
    let history_id = history_id_of(&mut leader_client).await;
    let token = fleet_coordinator_token(&fleet, &operator, history_id).await;

    let mut fourth = newcomer(fleet.cluster_id, &ca, leader, &token, 3);
    fourth.start();
    // `_in` rather than the plain form: a generous (60s) budget, since this
    // process runs several multi-voter fleets — this file's other cases —
    // and unrelated CPU load can slow any one of them well past the plain
    // form's fixed 20s.
    fourth.await_phase_in(&["learner", "voter"]).await;

    // Genuinely caught up: the light single-mint backlog here replicates in
    // well under the deadline, and a learner's own `/readyz` reports its own
    // lag against what it knows committed regardless of promotion (ADR 0037
    // §9).
    poll(
        Duration::from_secs(30),
        "the fourth daemon's learner catches up",
        || async {
            let (_, body) = fourth.readyz().await;
            body["replication_lag"].as_u64() == Some(0) && body["leader_contact_stale"] == false
        },
    )
    .await;

    let new_id = node_id_of(&fourth).await;
    let old_id = node_id_of(leader).await;

    replace_voter_until_success(&fleet, &operator, history_id, old_id, new_id).await;

    // Polled from `fourth`, the replacement itself, not from `leader`: once
    // replaced, `leader`'s own daemon is cut from membership entirely and may
    // no longer receive replication at all, so its own view is not the one to
    // trust here. A generous deadline, because this process runs several
    // multi-voter fleets at once (this file's other cases) and unrelated CPU
    // load can slow any one of them down.
    poll(
        Duration::from_secs(60),
        "the fleet's voter set becomes {survivors, new}, without old",
        || async {
            let voters = voter_ids(&fourth).await;
            voters.len() == 3 && voters.contains(&new_id) && !voters.contains(&old_id)
        },
    )
    .await;

    // The identical call again: §6's no-op, because `new` is already a voter
    // and `old` is gone from membership entirely. Driven the same
    // leader-resilient way — `old`'s own daemon is no longer a member and so
    // is no longer guaranteed to be the leader (nor anything close to it).
    replace_voter_until_success(&fleet, &operator, history_id, old_id, new_id).await;

    let voters_after_replay = voter_ids(&fourth).await;
    assert_eq!(
        voters_after_replay.len(),
        3,
        "the replay must not have changed the voter count: {voters_after_replay:?}"
    );

    fourth.stop().await.expect("fourth daemon stops cleanly");
    fleet.stop_all().await;
}

// ---------------------------------------------------------------------------
// 3. Terminate-before-launch: the hands-off path, then a late replay
// ---------------------------------------------------------------------------

/// A dead predecessor is folded out by the leader's own evidence-gated
/// promotion — no operator call at all (ADR 0037 §7's hands-off path) — and a
/// `ReplaceVoter` that arrives after the fact still succeeds, because it was
/// always going to be the idempotent §6 no-op.
///
/// `removal_grace` is shortened to 2s so the whole scenario runs in seconds:
/// the caught-up learner's own convergence loop keeps retrying `PromoteVoter`
/// (ADR 0016 step 3/ADR 0037 §7), which the leader keeps refusing with
/// `no-removable-peer` until the killed voter has been unreachable for longer
/// than the grace window, at which point the very next retry folds the dead
/// voter's removal into the promotion's joint change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_raced_by_the_predecessors_crash_converges_hands_off() {
    init_tracing();
    let _guard = exclusive().await;
    let ca = Ca::new();
    let mut fleet = Fleet::new(3, &ca);
    for member in &fleet.members {
        member.set_removal_grace("2s");
    }
    fleet.start_all();
    for member in &fleet.members {
        member.await_phase("waiting").await;
    }
    let operator = fleet.init().await;
    fleet.await_voters(3).await;

    let leader = find_leader(&fleet).await;
    let mut leader_client = operator_client(leader, &operator).await;
    let history_id = history_id_of(&mut leader_client).await;
    let token = fleet_coordinator_token(&fleet, &operator, history_id).await;

    let mut fifth = newcomer(fleet.cluster_id, &ca, leader, &token, 3);
    fifth.set_removal_grace("2s");
    fifth.start();
    // `_in` rather than the plain form: a generous (60s) budget, since this
    // process runs several multi-voter fleets — this file's other cases —
    // and unrelated CPU load can slow any one of them well past the plain
    // form's fixed 20s.
    fifth.await_phase_in(&["learner", "voter"]).await;
    poll(
        Duration::from_secs(30),
        "the caught-up learner's own lag settles",
        || async {
            let (_, body) = fifth.readyz().await;
            body["replication_lag"].as_u64() == Some(0) && body["leader_contact_stale"] == false
        },
    )
    .await;
    let new_id = node_id_of(&fifth).await;

    // Kill a live, non-leader voter: the leader's own contact tracker then
    // records continuous failed heartbeats/appends against it, which is
    // exactly the evidence the hands-off path consults. Killing the leader
    // instead would hand a *new* leader a contact tracker with no prior
    // attempts against the corpse, resetting the grace clock the ADR
    // documents (EPOCH_GAP) — a real but different scenario from the one
    // under test here.
    let leader_id = node_id_of(leader).await;
    let mut dead_idx = None;
    for (i, member) in fleet.members.iter().enumerate() {
        let id = node_id_of(member).await;
        if id != leader_id {
            dead_idx = Some(i);
            break;
        }
    }
    let dead_idx = dead_idx.expect("a non-leader voter exists in a 3-voter fleet");
    let dead_id = node_id_of(&fleet.members[dead_idx]).await;
    fleet.members[dead_idx].kill().await;

    // No operator call whatsoever between here and the poll below: the
    // property under test is that convergence alone reaches the replacement.
    let survivor_idx = (0..3).find(|&i| i != dead_idx).expect("a survivor exists");
    poll(
        Duration::from_secs(30),
        "the learner is folded in and the dead voter is folded out, hands-off",
        || async {
            let voters = voter_ids(&fleet.members[survivor_idx]).await;
            voters.len() == 3 && voters.contains(&new_id) && !voters.contains(&dead_id)
        },
    )
    .await;

    // The late arrival: an operator issues the very `ReplaceVoter` that would
    // have driven this if nobody had waited it out. `new` is already a voter
    // and `old` is already gone, so this must be the §6 no-op, not an error.
    let mut survivor_client = operator_client(&fleet.members[survivor_idx], &operator).await;
    admin::replace_voter(&mut survivor_client, history_id, dead_id, new_id)
        .await
        .expect(
            "a ReplaceVoter arriving after the hands-off path already completed it must be a \
             no-op success",
        );

    fifth.stop().await.expect("fifth daemon stops cleanly");
    fleet.stop_all().await;
}

// ---------------------------------------------------------------------------
// 4. Single-voter replacement: the survivor provably holds the key
// ---------------------------------------------------------------------------

/// The single-voter case (ADR 0037 §7: `ReplaceVoter` "works ... in the
/// single-voter case"): the founder is killed the instant the joint change
/// commits, and the surviving voter must not merely *be* the sole voter but
/// *provably hold the CA key* — proven here by driving its own local admin
/// socket's `issue-operator-cert` (ADR 0037 §3), which signs from the key on
/// that node's disk, and checking the resulting certificate chains to the
/// cluster CA. Key transfer with confirmed durable receipt is a precondition
/// of the joint change (ADR 0037 §4), so the key is already on the new
/// voter's disk before the founder is touched at all — the founder's death
/// afterwards proves nothing was borrowed from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_voter_replacement_leaves_a_new_voter_that_provably_holds_the_key() {
    init_tracing();
    let _guard = exclusive().await;
    let ca = Ca::new();

    let mut founder = Daemon::new_certless(ClusterId::new(), &ca);
    let operator = form(&mut founder).await;

    let mut founder_client = operator_client(&founder, &operator).await;
    let history_id = history_id_of(&mut founder_client).await;
    let token = coordinator_token(&mut founder_client, history_id).await;

    // cluster_size stays at the fixture default of 1: the continuing voter
    // set after the replacement is `{new}`, a single seat, which is exactly
    // what the ceiling allows.
    let mut learner = newcomer(founder.cluster_id, &ca, &founder, &token, 1);
    learner.start();
    // `_in` rather than the plain form: a generous (60s) budget, since this
    // process runs several multi-voter fleets — this file's other cases —
    // and unrelated CPU load can slow any one of them well past the plain
    // form's fixed 20s.
    learner.await_phase_in(&["learner", "voter"]).await;
    poll(
        Duration::from_secs(30),
        "the learner catches up on the founder's light log",
        || async {
            let (_, body) = learner.readyz().await;
            body["replication_lag"].as_u64() == Some(0) && body["leader_contact_stale"] == false
        },
    )
    .await;

    let founder_id = node_id_of(&founder).await;
    let learner_id = node_id_of(&learner).await;

    admin::replace_voter(&mut founder_client, history_id, founder_id, learner_id)
        .await
        .expect("single-voter replacement must succeed");

    // The founder vanishes the instant the change has committed — before it
    // is even asked whether it agrees.
    founder.kill().await;

    poll(
        Duration::from_secs(20),
        "the learner becomes the sole voter",
        || async {
            let (_, body) = learner.readyz().await;
            body["phase"] == "voter"
                && body["voters"].as_array().map(|v| v.len()) == Some(1)
                && body["voters"][0]["node_id"].as_u64() == Some(learner_id)
        },
    )
    .await;

    // Proof of key possession: sign a fresh operator certificate over the
    // survivor's own local admin socket (ADR 0037 §3) — the recovery path
    // that only works because the CA key is durably on THIS node's disk —
    // and check the result actually chains to the cluster CA.
    let reply = learner
        .admin(AdminCall::IssueOperatorCert {
            operator_csr: None,
            operator_cn: Some("post-replacement-proof".to_string()),
        })
        .await;
    let AdminReply::Issued { operator: issued } = reply else {
        panic!("expected the sole surviving voter to issue an operator cert, got {reply:?}");
    };
    coppice_tls::pki::verify_leaf(issued.ca_pem.as_bytes(), issued.cert_pem.as_bytes())
        .expect("the certificate signed by the survivor's key must chain to the cluster CA");

    // And it is the SAME cluster CA the survivor serves under — not some
    // other root the survivor happened to hold.
    let (serving_ca, _, _) = learner.tls_material();
    assert_eq!(
        issued.ca_pem.as_bytes(),
        serving_ca.as_slice(),
        "the signed certificate's CA must be the cluster CA the survivor serves under"
    );

    learner.stop().await.expect("learner stops cleanly");
}
