//! `rotate-ca`'s durability redesign (ADR 0037 §4, `rotate.rs`'s module doc),
//! end to end against a real fleet: the invariant under test throughout is
//! "a root never becomes bundle position 0 until its private key is durably
//! held by every current voter", staged across every crash window the
//! redesign exists to make survivable.
//!
//! Five claims, each against its own formed 3-voter fleet:
//!
//! 1. `a_leader_lost_after_staging_leaves_a_cluster_that_still_signs`:
//!    blocker 1's window. A leader that dies right after the stage commit —
//!    before any distribution — leaves a cluster that still signs under the
//!    OUTGOING root (the pre-redesign code would have made the un-distributed
//!    incoming root active here, and nothing could sign at all), and a
//!    resumed `begin` on the new leader correctly replaces the pending root
//!    it never received.
//! 2. `replicated_staged_acks_survive_a_leader_change_mid_distribution`: a
//!    peer's staged-key confirmation is a *replicated* fact, readable from a
//!    survivor before anyone dies, and a resume after a leader change never
//!    double-mints (still exactly two roots) whichever of the two valid paths
//!    it takes (reuse or replace).
//! 3. `a_crash_between_activation_and_the_local_swap_self_heals`: the crash
//!    window that is only survivable *because* activation is gated on total
//!    coverage — every voter already holds the key that just became active,
//!    so the renewal task's self-heal promotes it on every replica with no
//!    operator action, and any surviving leader (including one elected after
//!    the original dies) can sign under the new root.
//! 4. `activation_is_refused_while_a_voter_is_missing_and_succeeds_after_replace_voter`:
//!    the coverage gate itself — a dead follower parks the rotation
//!    (old root still signs) until membership is repaired, and this is also
//!    the promotion-during-rotation wiring's proof: the replacement voter is
//!    keyed with the staged key by `ensure_key_transferred` before its own
//!    joint change commits, which is the only reason coverage can close once
//!    it joins.
//! 5. `a_voter_that_cannot_persist_the_staged_key_is_treated_as_no_ack`: a
//!    recipient that durably fails the staged write is indistinguishable, at
//!    the gate, from one that never answered — coverage stays incomplete, the
//!    old root keeps signing, and the fire-once failpoint's spent latch lets
//!    the very next `begin` complete normally.
//!
//! ## The failpoints are process-global — read this before touching any test
//!
//! Every test below arms one of `rotate.rs`'s three test-only crash windows
//! (`ROTATE_AFTER_STAGE_COMMIT`, `ROTATE_AFTER_FIRST_DISTRIBUTION`,
//! `ROTATE_AFTER_ACTIVATION_COMMIT`) or `admin.rs`'s `STAGED_KEY_WRITE_FAILS`,
//! via the `COPPICE_TEST_FAILPOINT` env var. As `key_custody.rs`'s header
//! explains at length: that var and every failpoint's fire-once latch are
//! genuinely process-global `AtomicBool`s, not per-daemon or per-test, and
//! `cargo test` runs every test function of one binary as concurrent threads
//! of one process by default (nextest gives per-test processes, but the
//! verification this file's task sheet prescribes runs plain `cargo test`
//! too — twice, specifically to prove order-independence). Two tests' armed
//! windows racing in the same process could trip the wrong daemon's abort
//! entirely, or find the latch already spent. So [`SERIAL`] is held for each
//! test's *entire* body — not just the window the failpoint is armed for —
//! because a still-running fleet from one test sharing CPU with another
//! test's fresh `begin` call is exactly the kind of interference that turns a
//! deterministic crash injection back into a race. [`FailpointGuard`] removes
//! the env var on drop — including on a panic mid-test — mirroring
//! `key_custody.rs`'s own guard.
//!
//! The three `rotate.rs` failpoint names, [`RotationPhase`] and
//! `COVERAGE_INCOMPLETE` are imported below from `coppice_coordinator::rotate`
//! (a `pub mod`, alongside `admin::STAGED_KEY_WRITE_FAILS`) rather than copied
//! as string literals, so a rename in `rotate.rs` breaks this file's build
//! instead of silently disarming a crash-window assertion.

mod common;

use std::time::{Duration, Instant};

use coppice_coordinator::admin::{self, STAGED_KEY_WRITE_FAILS};
use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_coordinator::rotate::{
    RotationPhase, COVERAGE_INCOMPLETE, ROTATE_AFTER_ACTIVATION_COMMIT,
    ROTATE_AFTER_FIRST_DISTRIBUTION, ROTATE_AFTER_STAGE_COMMIT,
};
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::pki;

use common::{poll, Ca, Daemon, Fleet};

type AdminClient = coppice_net::admin::Client<tonic::transport::Channel>;

// ---------------------------------------------------------------------------
// Serialization: see the module doc comment
// ---------------------------------------------------------------------------

/// See the module doc comment: every test in this file holds this for its
/// entire body, so no two ever run concurrently in this process.
///
/// `tokio::sync::Mutex` rather than `std::sync::Mutex`: every test's body is
/// riddled with `.await` points while the guard is held (fleet setup,
/// `Daemon::admin` calls, `poll`), and `std`'s guard is neither `Send`-safe to
/// hold across an await in general nor clippy-clean to do so
/// (`await_holding_lock`, denied here same as everywhere else in this crate).
/// Tokio's guard has neither problem, and its `Mutex` never poisons — a panic
/// mid-test simply drops the guard on unwind, same as any other value, so the
/// next test's `.lock().await` proceeds normally with no recovery dance
/// needed.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Removes `COPPICE_TEST_FAILPOINT` when dropped, including on a panic
/// mid-test, mirroring `key_custody.rs`'s guard of the same name.
struct FailpointGuard;
impl Drop for FailpointGuard {
    fn drop(&mut self) {
        std::env::remove_var("COPPICE_TEST_FAILPOINT");
    }
}

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

// ---------------------------------------------------------------------------
// Fixtures mirrored from `rotate_ca.rs` (that suite does not export them)
// ---------------------------------------------------------------------------

/// Form an `n`-voter fleet the only way one forms (ADR 0037 §1/§3): identical
/// certless configs, one `init`, converge to `n` voters.
///
/// Takes `ca` rather than minting its own (as `rotate_ca.rs`'s `form_fleet`
/// does): [`Fleet::add_member`] — needed by the coverage-gate test below —
/// needs the same `Ca` handle back afterwards, so it has to be created by the
/// caller and outlive this call.
async fn form_fleet(ca: &Ca, n: usize) -> (Fleet, OperatorPem) {
    let mut fleet = Fleet::new(n, ca);
    fleet.start_all();
    for member in &fleet.members {
        member.await_phase("waiting").await;
    }
    let operator = fleet.init().await;
    fleet.await_voters(n).await;
    (fleet, operator)
}

/// How many `-----BEGIN CERTIFICATE-----` blocks a PEM bundle carries.
fn cert_block_count(pem: &[u8]) -> usize {
    std::str::from_utf8(pem)
        .expect("a CA bundle is UTF-8")
        .matches("-----BEGIN CERTIFICATE-----")
        .count()
}

/// The first certificate block of a PEM bundle, standalone — position 0 is
/// always the active signing root.
fn first_cert_pem(pem: &[u8]) -> Vec<u8> {
    let text = std::str::from_utf8(pem).expect("a CA bundle is UTF-8");
    let start = text
        .find("-----BEGIN CERTIFICATE-----")
        .expect("a CA bundle has at least one certificate");
    let marker = "-----END CERTIFICATE-----";
    let end = text[start..]
        .find(marker)
        .map(|i| start + i + marker.len())
        .expect("an opened certificate block has a matching end marker");
    text.as_bytes()[start..end].to_vec()
}

/// The index of whichever RUNNING fleet member currently reports itself the
/// leader.
///
/// Filters on [`Daemon::is_running`] rather than polling every member
/// unconditionally (as `rotate_ca.rs`'s `fleet_leader_index` does): several
/// tests below kill a member mid-test, and `/readyz` on a dead daemon retries
/// for its own 30s deadline before panicking — fatal if this loop reached it
/// on its way to checking a live survivor.
async fn find_leader(fleet: &Fleet) -> usize {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for (i, member) in fleet.members.iter().enumerate() {
            if !member.is_running() {
                continue;
            }
            if member.readyz().await.1["is_leader"] == true {
                return i;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no running fleet member reported itself the leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// This (running) daemon's own raft node id, from its `/readyz` body.
async fn node_id_of(daemon: &Daemon) -> u64 {
    daemon.readyz().await.1["node_id"]
        .as_u64()
        .expect("a running daemon reports its node id")
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
    .expect("dial the admin surface as an operator")
}

/// This cluster's stamped history id, via a self-probe.
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

/// Drive a non-settled `ReplaceVoter` to success, re-resolving the current
/// leader on every attempt — as `replace_voter.rs`'s
/// `replace_voter_until_success` does, for the same reason: the first,
/// effecting call commits a joint change and so bounces off a follower with
/// `not the leader`, and this file's own CPU-heavy fleets can cost a resolved
/// leader its term before the call lands.
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
            let leader_idx = find_leader(fleet).await;
            let mut client = operator_client(&fleet.members[leader_idx], operator).await;
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
// 1. Blocker 1: a leader lost right after staging still signs
// ---------------------------------------------------------------------------

/// Blocker 1's exact window (`rotate.rs` module doc, "the obvious
/// implementation is unrecoverable"): the leader stages the incoming root and
/// commits the dual-root bundle, then dies before distributing anything at
/// all. The redesign's whole point is that this is survivable — the outgoing
/// root is still bundle position 0, so it is still the active signing root —
/// and this test proves it directly by having a *different* node sign after
/// the loss, then shows a resumed `begin` correctly replaces the pending root
/// the new leader never received.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_lost_after_staging_leaves_a_cluster_that_still_signs() {
    let _serial = SERIAL.lock().await;
    init_tracing();
    let ca = Ca::new();
    let (mut fleet, _operator) = form_fleet(&ca, 3).await;

    let leader_idx = find_leader(&fleet).await;
    let dead_id = node_id_of(&fleet.members[leader_idx]).await;

    std::env::set_var("COPPICE_TEST_FAILPOINT", ROTATE_AFTER_STAGE_COMMIT);
    let _fp = FailpointGuard;

    let reply = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::Error { message } = reply else {
        panic!("expected begin to abort at the stage-commit failpoint, got {reply:?}");
    };
    assert!(
        message.contains(ROTATE_AFTER_STAGE_COMMIT),
        "the abort should name the failpoint, got: {message}"
    );

    fleet.members[leader_idx].kill().await;

    let survivor_idx = find_leader(&fleet).await;
    let status_reply = fleet.members[survivor_idx]
        .admin(AdminCall::RotateCaStatus)
        .await;
    let AdminReply::RotationStatus { status } = status_reply else {
        panic!("expected rotate-ca status to answer on the survivor, got {status_reply:?}");
    };
    assert_eq!(status.roots.len(), 2, "{:?}", status.roots);
    assert!(
        status.roots[0].active && !status.roots[0].pending,
        "position 0 must still be the OUTGOING, active root: {:?}",
        status.roots
    );
    assert!(
        status.roots[1].pending,
        "position 1 must be the incoming, still-pending root: {:?}",
        status.roots
    );
    assert_eq!(
        status.phase,
        RotationPhase::Staged,
        "only this daemon's own self-confirmation is recorded so far"
    );
    assert!(status.staged_root_serial.is_some());

    // The direct proof blocker 1 is fixed: the cluster can still sign, under
    // the still-active OUTGOING root — the pre-redesign code would have made
    // the un-distributed incoming root active here, and nothing anywhere
    // could have signed at all.
    let issued = fleet.members[survivor_idx]
        .admin(AdminCall::IssueOperatorCert {
            operator_csr: None,
            operator_cn: Some("still-signs-after-leader-loss".to_string()),
        })
        .await;
    let AdminReply::Issued { operator: fresh } = issued else {
        panic!("expected the new leader to issue a certificate, got {issued:?}");
    };
    let (ca_bundle_pem, _, _) = fleet.members[survivor_idx].tls_material();
    assert_eq!(cert_block_count(&ca_bundle_pem), 2);
    let active_root_pem = first_cert_pem(&ca_bundle_pem);
    pki::verify_leaf(&active_root_pem, fresh.cert_pem.as_bytes()).expect(
        "the cluster must still be able to sign under the still-active outgoing root after the \
         leader that staged (and never distributed) the incoming one is lost",
    );

    // Re-running begin on the new leader recovers the staging: it never
    // received the staged key, so it must mint and record a replacement
    // pending root rather than reuse the recorded one.
    let resumed_reply = fleet.members[survivor_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::RotationBegun { report } = resumed_reply else {
        panic!("expected the new leader's begin to resume, got {resumed_reply:?}");
    };
    assert!(
        report.resumed,
        "a recorded staging must resume, not restart"
    );
    assert!(
        report.replaced_pending_root,
        "the new leader never received the staged key, so it must replace the pending root"
    );
    assert!(
        !report.activated,
        "the dead original leader is still a voter with no confirmation: {report:?}"
    );
    assert_eq!(report.missing_voters, vec![dead_id]);
    assert_eq!(report.refusal.as_deref(), Some(COVERAGE_INCOMPLETE));
    assert_eq!(
        report.status.roots.len(),
        2,
        "a replacement swaps the pending entry, it never appends: {:?}",
        report.status.roots
    );

    fleet.stop_all().await;
}

// ---------------------------------------------------------------------------
// 2. Replicated staged acks survive a leader change mid-distribution
// ---------------------------------------------------------------------------

/// A peer's staged-key confirmation is a *replicated* fact — readable from a
/// survivor before anyone dies — and a resumed `begin` after a leader change
/// never double-mints, whichever of the two valid resume paths (reuse the
/// recorded pending root, or replace it) the new leader happens to take.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_staged_acks_survive_a_leader_change_mid_distribution() {
    let _serial = SERIAL.lock().await;
    init_tracing();
    let ca = Ca::new();
    let (mut fleet, _operator) = form_fleet(&ca, 3).await;

    let leader_idx = find_leader(&fleet).await;
    let leader_id = node_id_of(&fleet.members[leader_idx]).await;
    let other_idx = (0..fleet.members.len())
        .find(|&i| i != leader_idx)
        .expect("a 3-voter fleet has a non-leader member");

    std::env::set_var("COPPICE_TEST_FAILPOINT", ROTATE_AFTER_FIRST_DISTRIBUTION);
    let _fp = FailpointGuard;

    let reply = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::Error { message } = reply else {
        panic!("expected begin to abort at the first-distribution failpoint, got {reply:?}");
    };
    assert!(
        message.contains(ROTATE_AFTER_FIRST_DISTRIBUTION),
        "the abort should name the failpoint, got: {message}"
    );

    // Before killing anything: the acks are REPLICATED, not leader-local —
    // read from a peer that never staged anything itself. Polled rather than
    // read once: the confirmations are visible on the LEADER's own view the
    // instant `begin` returns (`ctx.commit` awaits that), but this peer's own
    // view still has to catch up to them over the wire, which — under this
    // suite's own CPU contention (several fleets' worth of concurrent daemons
    // in one process) — is not instant.
    let deadline = Instant::now() + Duration::from_secs(15);
    let status = loop {
        let status_reply = fleet.members[other_idx]
            .admin(AdminCall::RotateCaStatus)
            .await;
        let AdminReply::RotationStatus { status } = status_reply else {
            panic!("expected rotate-ca status to answer on the peer, got {status_reply:?}");
        };
        if status.staged_key_holders.len() >= 2 {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the peer never observed both staged-key confirmations replicate: {:?}",
            status.staged_key_holders
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        status.staged_key_holders.len(),
        2,
        "the staging leader's self-confirmation plus the one peer that confirmed: {:?}",
        status.staged_key_holders
    );
    let staged_serial_before = status.staged_root_serial.clone();
    let confirmed_peer_id = status
        .staged_key_holders
        .iter()
        .copied()
        .find(|&id| id != leader_id)
        .expect("one of the two holders must be the peer that actually confirmed");

    fleet.members[leader_idx].kill().await;
    let survivor_idx = find_leader(&fleet).await;

    let resumed_reply = fleet.members[survivor_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::RotationBegun { report } = resumed_reply else {
        panic!("expected the new leader's begin to resume, got {resumed_reply:?}");
    };
    assert!(report.resumed);
    // No double-mint, whichever path this took: a resume never appends a
    // third root.
    assert_eq!(report.status.roots.len(), 2, "{:?}", report.status.roots);

    if !report.replaced_pending_root {
        // The new leader itself held the staged key (it was the peer that had
        // already confirmed): the SAME pending root is reused, serial
        // unchanged. Two legitimate outcomes from here — the dead leader's
        // own ack was replicated before it died, so once the resume pushes
        // the key to the remaining voter, coverage can become total and the
        // SAME call completes activation.
        if report.activated {
            // Reuse ran all the way to activation: the staged root is staged
            // no longer — it is the ACTIVE root, same serial, and the staged
            // bookkeeping is correctly cleared (holders merged into the live
            // custody maps).
            assert_eq!(
                report.status.roots[0].serial,
                staged_serial_before.clone().expect("a serial was staged"),
                "the reused pending root must become the active root, serial intact"
            );
            assert_eq!(
                report.status.staged_root_serial, None,
                "activation clears the staged bookkeeping"
            );
        } else {
            assert_eq!(
                report.status.staged_root_serial, staged_serial_before,
                "reusing the recorded staging must not change its serial"
            );
            assert!(
                report
                    .status
                    .staged_key_holders
                    .contains(&confirmed_peer_id),
                "the peer that already confirmed must still hold its confirmation on reuse: {:?}",
                report.status.staged_key_holders
            );
        }
    } else {
        // The new leader did not hold the staged key: it minted and recorded
        // a replacement pending root, which correctly re-scopes
        // staged_key_holders to that new root rather than carrying forward
        // confirmations against the discarded one.
        assert_ne!(
            report.status.staged_root_serial, staged_serial_before,
            "a replacement staging must record a fresh serial"
        );
    }

    fleet.stop_all().await;
}

// ---------------------------------------------------------------------------
// 3. A crash between activation and the local swap self-heals
// ---------------------------------------------------------------------------

/// The window that is survivable *because* activation is gated on total
/// coverage: by the time the activation commit lands, every current voter
/// already holds the key that just became active, so any daemon can promote
/// its own staged key to the live path independently. This test proves the
/// self-heal happens with no operator action and no restart (the renewal
/// task's own pass), and that any surviving leader — including a freshly
/// elected one — can sign under the newly active root afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_between_activation_and_the_local_swap_self_heals() {
    let _serial = SERIAL.lock().await;
    init_tracing();
    let ca = Ca::new();
    let (mut fleet, _operator) = form_fleet(&ca, 3).await;

    let leader_idx = find_leader(&fleet).await;

    std::env::set_var("COPPICE_TEST_FAILPOINT", ROTATE_AFTER_ACTIVATION_COMMIT);
    let _fp = FailpointGuard;

    let reply = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::Error { message } = reply else {
        panic!("expected begin to abort at the activation-commit failpoint, got {reply:?}");
    };
    assert!(
        message.contains(ROTATE_AFTER_ACTIVATION_COMMIT),
        "the abort should name the failpoint, got: {message}"
    );

    // The activation commit DID land, even though the local swap did not.
    let status_reply = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaStatus)
        .await;
    let AdminReply::RotationStatus { status } = status_reply else {
        panic!("expected rotate-ca status to answer, got {status_reply:?}");
    };
    assert!(
        status.staged_root_serial.is_none(),
        "activation must have cleared the pending marker: {status:?}"
    );
    assert_eq!(status.roots.len(), 2, "{:?}", status.roots);

    // Self-heal: every member's live key ends up signing the newly active
    // root with no operator action and no restart — the renewal task's own
    // pass, paced by the fixture's 200ms `renewal_reevaluate_interval`. Both
    // halves of `promote_staged_if_activated`'s work are awaited:
    // `local_key_signs_active_root` (the signing key is promoted) AND
    // `installed_matches_replicated` (this host trusts the anchor *set* the
    // cluster records) — two different facts that settle independently. Note
    // what the second one does NOT certify: it is a set comparison, so a file
    // written at stage time keeps stage-time block order through activation,
    // which is why the active-root check below reads the bundle from
    // replicated state (`fresh.ca_pem`), never from a file by position.
    poll(
        Duration::from_secs(15),
        "every member's live key signs the newly active root, and its own trust file agrees \
         (renewal task self-heal)",
        || async {
            for member in &fleet.members {
                let AdminReply::RotationStatus { status } =
                    member.admin(AdminCall::RotateCaStatus).await
                else {
                    return false;
                };
                if !status.local_key_signs_active_root || !status.installed_matches_replicated {
                    return false;
                }
            }
            true
        },
    )
    .await;

    // Kill the original leader and prove ANY leader can sign under the new
    // root — the coverage gate's whole point.
    fleet.members[leader_idx].kill().await;
    let survivor_idx = find_leader(&fleet).await;

    let issued = fleet.members[survivor_idx]
        .admin(AdminCall::IssueOperatorCert {
            operator_csr: None,
            operator_cn: Some("any-leader-signs-post-activation".to_string()),
        })
        .await;
    let AdminReply::Issued { operator: fresh } = issued else {
        panic!("expected the new leader to issue a certificate, got {issued:?}");
    };
    // `fresh.ca_pem` is the bundle `IssueOperatorCert` read straight out of
    // *replicated* state (`formation::load_cluster_ca`), not this host's
    // on-disk trust file — so, unlike `Daemon::tls_material()`, it carries no
    // race against `adopt_anchors`' own disk write. (`installed_matches_replicated`
    // is a set comparison, not an order one, so polling on it — as the
    // self-heal wait above does — does not certify that a *file's* block order
    // has caught up to the replicated one; only the value `IssueOperatorCert`
    // itself read does.) Still dual-rooted: activation reorders the bundle, it
    // does not drop the outgoing root — only `complete` (not exercised by this
    // suite) does.
    assert_eq!(
        cert_block_count(fresh.ca_pem.as_bytes()),
        2,
        "{:?}",
        fresh.ca_pem
    );
    let active_root_pem = first_cert_pem(fresh.ca_pem.as_bytes());
    pki::verify_leaf(&active_root_pem, fresh.cert_pem.as_bytes())
        .expect("the new leader must sign under the newly active root");

    // Re-running begin short-circuits to done: nothing about the cluster is
    // outstanding.
    let final_reply = fleet.members[survivor_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::RotationBegun { report } = final_reply else {
        panic!("expected begin to report the rotation already activated, got {final_reply:?}");
    };
    assert!(report.activated);
    assert!(report.resumed);
    assert!(report.distribution.is_empty(), "{:?}", report.distribution);

    fleet.stop_all().await;
}

// ---------------------------------------------------------------------------
// 4. The coverage gate itself, and the promotion-during-rotation wiring
// ---------------------------------------------------------------------------

/// Activation refuses while a current voter is missing its staged-key
/// confirmation — the old root keeps signing throughout — and succeeds the
/// instant membership is repaired the runbook's documented way: replace the
/// dead voter, then re-run `begin`.
///
/// This is also the promotion-during-rotation wiring's proof: `admin.rs`'s
/// `ensure_key_transferred` keys a promotion candidate with BOTH the live and
/// the staged key before its joint change commits, so the replacement voter
/// this test admits already holds the staged key the instant it becomes a
/// voter — which is the only reason coverage can close here at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activation_is_refused_while_a_voter_is_missing_and_succeeds_after_replace_voter() {
    let _serial = SERIAL.lock().await;
    init_tracing();
    let ca = Ca::new();
    let (mut fleet, operator) = form_fleet(&ca, 3).await;

    let leader_idx = find_leader(&fleet).await;
    let follower_idx = (0..fleet.members.len())
        .find(|&i| i != leader_idx)
        .expect("a 3-voter fleet has a follower");
    let dead_id = node_id_of(&fleet.members[follower_idx]).await;
    fleet.members[follower_idx].kill().await;

    // No failpoint here: this is the ordinary coverage refusal, staged by a
    // genuinely missing voter rather than an injected abort.
    let reply = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::RotationBegun { report } = reply else {
        panic!("expected a parked (coverage-incomplete) begin, got {reply:?}");
    };
    assert!(
        !report.activated,
        "activation must be refused while a voter is missing: {report:?}"
    );
    assert_eq!(report.missing_voters, vec![dead_id]);
    assert_eq!(report.refusal.as_deref(), Some(COVERAGE_INCOMPLETE));
    let dead_outcome = report
        .distribution
        .iter()
        .find(|o| o.node_id == dead_id)
        .expect("the dead follower must appear in the distribution outcomes");
    assert!(!dead_outcome.installed, "{dead_outcome:?}");
    assert!(dead_outcome.error.is_some(), "{dead_outcome:?}");

    // The old root still signs.
    let issued = fleet.members[leader_idx]
        .admin(AdminCall::IssueOperatorCert {
            operator_csr: None,
            operator_cn: Some("still-signs-while-parked".to_string()),
        })
        .await;
    let AdminReply::Issued { operator: fresh } = issued else {
        panic!("expected the leader to issue a certificate while parked, got {issued:?}");
    };
    let (ca_bundle_pem, _, _) = fleet.members[leader_idx].tls_material();
    let active_root_pem = first_cert_pem(&ca_bundle_pem);
    pki::verify_leaf(&active_root_pem, fresh.cert_pem.as_bytes()).expect(
        "a certificate issued while the rotation is parked must still chain to the still-active \
         outgoing root",
    );

    let status_reply = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaStatus)
        .await;
    let AdminReply::RotationStatus { status } = status_reply else {
        panic!("expected rotate-ca status to answer, got {status_reply:?}");
    };
    assert!(
        matches!(
            status.phase,
            RotationPhase::Staged | RotationPhase::Distributing
        ),
        "a parked rotation must be in the staged/distributing family, got {:?}",
        status.phase
    );
    assert_eq!(status.missing_voters, vec![dead_id]);

    // Fix membership the runbook's documented way: a fresh installation joins
    // and catches up, then `ReplaceVoter` swaps it in for the dead follower.
    let newcomer_idx = fleet.add_member(&ca);
    fleet.members[newcomer_idx].start();
    fleet.members[newcomer_idx]
        .await_phase_in(&["learner", "voter"])
        .await;
    poll(
        Duration::from_secs(30),
        "the replacement member's learner catches up",
        || async {
            let (_, body) = fleet.members[newcomer_idx].readyz().await;
            body["replication_lag"].as_u64() == Some(0) && body["leader_contact_stale"] == false
        },
    )
    .await;
    let new_id = node_id_of(&fleet.members[newcomer_idx]).await;

    let hid = history_id_of(&fleet.members[find_leader(&fleet).await]).await;
    replace_voter_until_success(&fleet, &operator, hid, dead_id, new_id).await;

    poll(
        Duration::from_secs(30),
        "the replacement becomes a voter",
        || async {
            let (_, body) = fleet.members[newcomer_idx].readyz().await;
            body["phase"] == "voter"
        },
    )
    .await;

    // Re-run begin: coverage now closes. The replacement was already keyed
    // with the staged key by `ensure_key_transferred` before its own joint
    // change committed (ADR 0037 §4's promotion-during-rotation wiring) —
    // the only reason this can succeed without a second distribution round.
    let final_leader_idx = find_leader(&fleet).await;
    let final_reply = fleet.members[final_leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::RotationBegun { report } = final_reply else {
        panic!("expected begin to activate after membership was repaired, got {final_reply:?}");
    };
    assert!(report.activated, "coverage should have closed: {report:?}");
    assert!(report.missing_voters.is_empty(), "{report:?}");

    fleet.stop_all().await;
}

// ---------------------------------------------------------------------------
// 5. A staged-write failure is treated as no ack, not a dead end
// ---------------------------------------------------------------------------

/// A recipient that cannot durably persist the pushed staged key must be
/// indistinguishable, at the gate, from one that never answered: coverage
/// stays incomplete, the old root keeps signing, and the failpoint's
/// fire-once latch means the very next `begin` — the ordinary operator retry
/// — completes normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_voter_that_cannot_persist_the_staged_key_is_treated_as_no_ack() {
    let _serial = SERIAL.lock().await;
    init_tracing();
    let ca = Ca::new();
    let (mut fleet, _operator) = form_fleet(&ca, 3).await;

    let leader_idx = find_leader(&fleet).await;
    let mut peer_ids = Vec::new();
    for (i, member) in fleet.members.iter().enumerate() {
        if i != leader_idx {
            peer_ids.push(node_id_of(member).await);
        }
    }

    std::env::set_var("COPPICE_TEST_FAILPOINT", STAGED_KEY_WRITE_FAILS);
    let _fp = FailpointGuard;

    let reply = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::RotationBegun { report } = reply else {
        panic!("expected a parked begin (one recipient's staged write refused), got {reply:?}");
    };
    assert!(!report.activated, "{report:?}");
    assert_eq!(report.missing_voters.len(), 1, "{report:?}");
    assert!(
        peer_ids.contains(&report.missing_voters[0]),
        "the missing voter must be one of the two peers: {report:?}"
    );
    assert_eq!(report.refusal.as_deref(), Some(COVERAGE_INCOMPLETE));

    // The old root still signs.
    let issued = fleet.members[leader_idx]
        .admin(AdminCall::IssueOperatorCert {
            operator_csr: None,
            operator_cn: Some("still-signs-after-a-write-refusal".to_string()),
        })
        .await;
    let AdminReply::Issued { operator: fresh } = issued else {
        panic!("expected the leader to issue a certificate, got {issued:?}");
    };
    let (ca_bundle_pem, _, _) = fleet.members[leader_idx].tls_material();
    let active_root_pem = first_cert_pem(&ca_bundle_pem);
    pki::verify_leaf(&active_root_pem, fresh.cert_pem.as_bytes()).expect(
        "a certificate issued after a recipient's write refusal must still chain to the \
         still-active outgoing root",
    );

    // The retry: the fire-once latch is spent, so this persists normally —
    // the refusal was a park, not a dead end.
    let final_leader_idx = find_leader(&fleet).await;
    let final_reply = fleet.members[final_leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::RotationBegun { report } = final_reply else {
        panic!("expected the retried begin to activate, got {final_reply:?}");
    };
    assert!(report.activated, "{report:?}");
    assert!(report.missing_voters.is_empty(), "{report:?}");

    fleet.stop_all().await;
}
