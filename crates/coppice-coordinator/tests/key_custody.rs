//! ADR 0037 §4's CA-key custody invariants, end to end.
//!
//! Four claims, each staged against a real formed fleet (formation is the
//! only path that makes a cluster own its CA, ADR 0037 §3/§4 — a legacy
//! externally-provisioned installation never records a replicated CA
//! certificate at all, and key custody is entirely a no-op for one):
//!
//! 1. The crash window between confirmed key receipt and the joint change
//!    (`a_crash_between_key_receipt_and_the_joint_change_converges_without_a_second_transfer`):
//!    a promotion that aborts right after the candidate durably confirms the
//!    key leaves it a *keyed learner*, and the loop's unmarked-error retry
//!    converges it to voter without transferring or confirming anything a
//!    second time.
//! 2. The §4 removal postcondition
//!    (`a_removal_that_would_leave_no_confirmed_key_holder_is_refused`): an
//!    operator may never remove the voter set's last confirmed key holder.
//! 3. The blast-radius claim, byte-level
//!    (`the_ca_key_never_reaches_snapshots_or_a_learner_disk`): the key never
//!    appears in a snapshot, a log segment, or any file on a learner's disk —
//!    only the CA *certificate* does.
//! 4. "Granted grudgingly"
//!    (`an_evidence_promotion_that_cannot_proceed_never_keys_the_candidate`):
//!    a learner arriving at an already-full voter set is never keyed at all,
//!    for as long as it keeps polling.
//! 5. The transfer protocol's other crash window
//!    (`an_abandoned_transfer_keeps_the_keyed_disk_visible_in_custody_accounting`):
//!    a leader lost between the candidate's durable ack and the replicated
//!    confirmation leaves a keyed disk — which the pre-committed transfer
//!    intent keeps visible in custody accounting until a retry resolves it.
//!
//! ## The failpoint is process-global — read this before touching test 1
//!
//! Test 1 arms `AdminService::promote_voter`'s one test-only failpoint
//! (`coppice_coordinator::admin::PROMOTE_AFTER_KEY_TRANSFER`) via the
//! `COPPICE_TEST_FAILPOINT` env var. That failpoint is production code —
//! `promote_voter` is reached only through the gRPC surface, so there is no
//! per-call parameter to thread a test enum through the way
//! `formation::Failpoint` does — gated by a fire-once `AtomicBool` that is
//! genuinely process-global, not per-daemon or per-test. Every daemon this
//! whole test *binary* boots (across all four tests below) shares both the
//! env var and the latch, and `cargo test` runs the test functions in one
//! binary as concurrent threads by default. A real fleet's own promotions
//! (tests 3 and 4 build one each) running concurrently with test 1's armed
//! window could trip the latch on the wrong promotion entirely. The fix here
//! is a plain in-process async mutex: every test in this file takes [`SERIAL`] for
//! its whole body, so at most one of the four ever runs at a time — cheap
//! insurance given there is exactly one test file that ever sets the var,
//! and it does so only for its own duration. (Test 5 arms the second
//! failpoint, `TRANSFER_BEFORE_CONFIRM`, under the same rules.)
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rcgen::KeyPair;
use tonic::Status;

use coppice_coordinator::admin::{
    self, has_marker, NO_KEY_HOLDER, NO_REMOVABLE_PEER, PROMOTE_AFTER_KEY_TRANSFER,
    TRANSFER_BEFORE_CONFIRM, VOTER_SET_FULL,
};
use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::id::ClusterId;
use coppice_proto::pb::core::v1 as pbcore;
use coppice_proto::pb::raft::v1 as pb;

mod common;
use common::{Ca, Daemon, Fleet, FLEET_TOKEN};

type AdminClient = coppice_net::admin::Client<tonic::transport::Channel>;

/// See the module doc comment: every test in this file holds this for its
/// entire body, so no two ever run concurrently in this process.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Per-binary tracing, mirroring the other §6/§7 suites.
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

/// Dial `daemon`'s admin surface presenting the operator credential `init`
/// (or [`Fleet::init`]) minted.
async fn dial_operator(daemon: &Daemon, operator: &OperatorPem) -> AdminClient {
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

/// This cluster's stamped raft history, via `ProbeCluster`.
async fn probe_history_id(client: &mut AdminClient) -> [u8; 16] {
    let resp = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe the cluster")
        .into_inner();
    resp.history_id
        .try_into()
        .unwrap_or_else(|v: Vec<u8>| panic!("history id must be 16 bytes, got {}", v.len()))
}

/// Mint the coordinator enrollment token a fleet's config artifact carries
/// (ADR 0037 §5), using an operator credential.
async fn coordinator_token(client: &mut AdminClient, hid: [u8; 16]) -> String {
    client
        .mint_enroll_token(pb::MintEnrollTokenRequest {
            history_id: hid.to_vec(),
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

/// The voter ids in a `ClusterStatus` answer's active config.
fn voter_ids(status: &pb::ClusterStatusResponse) -> BTreeSet<u64> {
    status
        .membership
        .as_ref()
        .and_then(|m| m.configs.first())
        .map(|c| c.voters.iter().copied().collect())
        .unwrap_or_default()
}

/// Find the fleet member currently reporting itself leader — required rather
/// than assumed, because `Fleet::init` forms on member 0 but leadership can
/// move (the same helper `replace_voter.rs` carries; test binaries share code
/// only through `common`, and this stays test-local by that suite's
/// precedent). Retried: the gap between a step-down and the next election
/// completing is normal, not a failure.
async fn find_leader(fleet: &Fleet) -> &Daemon {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for member in &fleet.members {
            if member.readyz().await.1["is_leader"] == true {
                return member;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no fleet member reported itself leader within the deadline"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The member ids in a `ClusterStatus` answer's membership, voter or learner.
fn member_ids(status: &pb::ClusterStatusResponse) -> BTreeSet<u64> {
    status
        .membership
        .as_ref()
        .map(|m| m.members.iter().map(|m| m.node_id).collect())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// 1. The crash window between confirmed key receipt and the joint change
// ---------------------------------------------------------------------------

/// Removes the `COPPICE_TEST_FAILPOINT` env var when dropped, including on a
/// panic mid-test, so a failed run does not leave test 1's mutation sitting
/// in the process environment for whichever test the harness runs next
/// (harmless in practice — the latch has already fired by then, so the var
/// alone can no longer abort anything — but there is no reason to leave it).
struct FailpointGuard;
impl Drop for FailpointGuard {
    fn drop(&mut self) {
        std::env::remove_var("COPPICE_TEST_FAILPOINT");
    }
}

/// ADR 0037 §4's documented crash window, staged deterministically: "A crash
/// between key receipt and the joint change leaves a caught-up learner
/// holding the key for the promotion it was already gated into — accepted,
/// and covered by the custody statement... `confirmed durable key receipt is
/// a precondition of the joint change`... ensuring no second transfer ever
/// fires once the fact is recorded."
///
/// Staging: a two-daemon cluster (`cluster_size = 2`), the second admitted
/// the self-converging way (ADR 0037 §1) with
/// `COPPICE_TEST_FAILPOINT=promote-after-key-transfer` armed from before it
/// even starts. Its own convergence loop drives every step — enroll, join,
/// catch up, and (racing the armed failpoint) promote — with no test-issued
/// RPC in the loop at all, which is what makes the crash genuinely
/// server-side rather than a test artifact: `promote_voter`'s failpoint sits
/// on the *leader*'s admin service, between `ensure_key_transferred` (which
/// completes fully — the transfer, the durable write, and the replicated
/// confirmation) and `commit_promotion`, so the abort always lands after the
/// fact of possession is already recorded. The loop treats the abort as an
/// unmarked (hence retryable) refusal and tries again on its next tick —
/// one `[pacing] probe_interval` (50ms under the fixture) later — and that
/// retry is the one this test lets
/// through, because the failpoint fires at most once per process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_between_key_receipt_and_the_joint_change_converges_without_a_second_transfer() {
    let _serial = SERIAL.lock().await;
    init_tracing();

    std::env::set_var("COPPICE_TEST_FAILPOINT", PROMOTE_AFTER_KEY_TRANSFER);
    let _failpoint_guard = FailpointGuard;

    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    let mut leader = Daemon::new_certless(cluster_id, &ca);
    leader.set_cluster_size(2);
    let operator = form(&mut leader).await;

    let mut op_client = dial_operator(&leader, &operator).await;
    let hid = probe_history_id(&mut op_client).await;
    let token = coordinator_token(&mut op_client, hid).await;

    let mut candidate = newcomer(cluster_id, &ca, &leader, &token, 2);
    candidate.start();

    // Tight poll for the transient window: the candidate is keyed (it
    // appears in `key_holders`) but not yet a voter. The confirmation commit
    // that `ensure_key_transferred` makes is fully visible before the
    // failpoint ever gets a chance to abort anything, so this state is not
    // racy to detect — it is racy only to *not miss*, and it lasts a whole
    // probe interval, comfortably longer than this 10ms poll.
    let deadline = Instant::now() + Duration::from_secs(60);
    let (candidate_id, confirmed_before) = loop {
        let status = admin::cluster_status(&mut op_client, hid)
            .await
            .expect("read cluster status while waiting for the keyed-but-not-voter window");
        let voters = voter_ids(&status);
        if let Some(holder) = status
            .key_holders
            .iter()
            .find(|h| !voters.contains(&h.node_id))
        {
            break (holder.node_id, holder.confirmed_at_us);
        }
        assert!(
            Instant::now() < deadline,
            "the candidate never reached a keyed-but-not-voter state within the deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    // Confirmed possession is a replicated fact; the disk artifact backing it
    // must already exist too (ADR 0037 §4: the confirmation is never
    // replicated ahead of the durable write it attests to).
    let ca_key_path = candidate.data_dir().join(coppice_tls::pki::CA_KEY_FILE);
    let meta_before = std::fs::metadata(&ca_key_path)
        .unwrap_or_else(|e| panic!("the keyed candidate's ca.key must exist on disk: {e}"));
    let len_before = meta_before.len();
    let mtime_before = meta_before.modified().expect("ca.key mtime");

    // Let the loop re-enter: the failpoint is fire-once, so the very next
    // retry completes the joint change with no test intervention at all.
    let final_body = candidate.await_phase("voter").await;
    assert_eq!(
        final_body["node_id"].as_u64(),
        Some(candidate_id),
        "the promoted seat must be the one that was keyed"
    );

    let status_after = admin::cluster_status(&mut op_client, hid)
        .await
        .expect("read cluster status after promotion completes");
    assert!(
        voter_ids(&status_after).contains(&candidate_id),
        "the candidate must have converged to voter: {status_after:?}"
    );

    let holder_after = status_after
        .key_holders
        .iter()
        .find(|h| h.node_id == candidate_id)
        .expect("the candidate's key confirmation must still be recorded");
    assert_eq!(
        holder_after.confirmed_at_us, confirmed_before,
        "ensure_key_transferred must have skipped on the already-confirmed fact — no second \
         transfer, no second confirmation (ADR 0037 §4/§6)"
    );

    let meta_after =
        std::fs::metadata(&ca_key_path).expect("ca.key must still be on disk after promotion");
    assert_eq!(
        meta_after.len(),
        len_before,
        "the ca.key file must not have been rewritten by a second transfer"
    );
    assert_eq!(
        meta_after.modified().expect("ca.key mtime"),
        mtime_before,
        "the ca.key file's mtime must not have moved — TransferCaKey never re-fired"
    );

    candidate.stop().await.expect("candidate stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}

// ---------------------------------------------------------------------------
// 2. The removal postcondition
// ---------------------------------------------------------------------------

/// ADR 0037 §4's removal postcondition: "no change may leave the continuing
/// voter set without a confirmed key holder... refused for operator repair."
///
/// The single-voter case is the one this test drives, and it is the *whole*
/// honest test: a two-voter set with exactly one confirmed holder is not
/// constructible through any real flow this codebase has. The only path that
/// keys a second voter is promotion, and promotion's key transfer is itself
/// gated on confirmed receipt before the joint change commits (§4/§6) — so
/// every voter beyond the founder is, by construction, always a confirmed
/// holder by the time it is a voter at all. Reaching "two voters, one
/// confirmation" would require writing state no admitted verb ever produces
/// (deleting a `ConfirmKeyPossession` fact by hand), which is exactly the
/// kind of artificial state surgery this suite avoids — a real removal never
/// finds that shape, so there is nothing honest to assert about it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removal_that_would_leave_no_confirmed_key_holder_is_refused() {
    let _serial = SERIAL.lock().await;
    init_tracing();

    let ca = Ca::new();
    let cluster_id = ClusterId::new();
    let mut daemon = Daemon::new_certless(cluster_id, &ca);
    let operator = form(&mut daemon).await;

    let mut client = dial_operator(&daemon, &operator).await;
    let hid = probe_history_id(&mut client).await;
    let status = admin::cluster_status(&mut client, hid)
        .await
        .expect("read the single-voter cluster's status");
    let founder = status.local_node_id;

    // A single-voter cluster's founder mints the key locally rather than
    // receiving it (formation, not transfer) — but the removal postcondition
    // does not care how the founder came to hold the key: removing the
    // cluster's only voter always leaves an EMPTY continuing set, and an
    // empty set can never contain a confirmed holder, so the refusal is
    // unconditional here.
    let refusal: Status = client
        .remove_node(pb::RemoveNodeRequest {
            history_id: hid.to_vec(),
            node_id: founder,
        })
        .await
        .expect_err("removing the cluster's only voter must be refused");
    assert!(
        has_marker(refusal.message(), NO_KEY_HOLDER),
        "expected the {NO_KEY_HOLDER:?} marker, got ({:?}) {:?}",
        refusal.code(),
        refusal.message()
    );

    // Refused means refused: the seat is still there.
    let after = admin::cluster_status(&mut client, hid)
        .await
        .expect("read status after the refusal");
    assert!(
        member_ids(&after).contains(&founder),
        "a refused removal must not have dropped the seat: {after:?}"
    );

    daemon.stop().await.expect("daemon stops cleanly");
}

// ---------------------------------------------------------------------------
// 3. The key never reaches a snapshot, a log segment, or a learner's disk
// ---------------------------------------------------------------------------

/// Whether `needle` appears as a contiguous subsequence of `haystack` (as
/// `crates/coppice-consensus/tests/snapshot_secrecy.rs`).
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|w| w == needle)
}

/// Recursively collect every regular file under `dir` — never a directory,
/// and never something else a data dir can legitimately hold that is not a
/// byte-scannable artifact at all, like the local admin socket
/// (`admin.sock`, a Unix domain socket bound for the process lifetime).
fn walk_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if file_type.is_dir() {
            walk_files(&path, out);
        } else if file_type.is_file() {
            out.push(path);
        }
    }
}

/// The CA private key's DER encoding and its base64 PEM body (headers and
/// line wrapping stripped) — the two forms whose absence proves the key
/// never leaked, exactly as in `snapshot_secrecy.rs`.
fn key_fingerprints(key_pem: &[u8]) -> (Vec<u8>, String) {
    let pem_str = std::str::from_utf8(key_pem).expect("ca.key is UTF-8 PEM");
    let key = KeyPair::from_pem(pem_str).expect("parse the CA private key");
    let der = key.serialize_der();
    let body: String = pem_str
        .lines()
        .filter(|l| !l.starts_with("-----"))
        .flat_map(|l| l.trim().chars())
        .collect();
    assert!(body.len() > 40, "key PEM must have a base64 body");
    (der, body)
}

/// Assert that no file under `dir` carries the CA private key's DER bytes or
/// PEM body. `skip` excludes one path outright — the legitimate custody file
/// itself, when scanning a voter's own data directory.
fn assert_dir_never_carries_key(dir: &Path, key_der: &[u8], key_body: &str, skip: Option<&Path>) {
    let mut files = Vec::new();
    walk_files(dir, &mut files);
    assert!(
        !files.is_empty(),
        "{}: expected at least one file to scan",
        dir.display()
    );
    for path in files {
        if skip.is_some_and(|s| s == path) {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert!(
            !contains(&bytes, key_der),
            "{}: CA private key DER leaked (ADR 0037 §4)",
            path.display()
        );
        assert!(
            !contains(&bytes, key_body.as_bytes()),
            "{}: CA private key PEM body leaked (ADR 0037 §4)",
            path.display()
        );
    }
}

/// The ADR 0037 §4 blast-radius claim, at the byte level: "the CA *private
/// key* never enters replicated state... learners receive snapshots and log
/// replay, so a key in replicated state would let anyone who reaches *any*
/// learner seat mint arbitrary certificates." Only the CA *certificate* is
/// replicated, so it must appear (the positive control); the key must never
/// appear anywhere a snapshot, a log segment, or a learner's disk could carry
/// it.
///
/// Staging: a 3-voter fleet (`cluster_size = 3`, all three keyed and
/// confirmed the normal self-converging way) plus a 4th daemon that enrolls,
/// joins, and catches up but never gets past the §7 voter-count ceiling —
/// `plan_promotion` refuses it before any key transfer is even attempted, so
/// its disk is exactly the "learner disk" half of the claim. A real snapshot
/// is forced (not merely asserted to exist by construction) by driving the
/// leader past its configured `snapshot_log_entries` threshold with cheap,
/// side-effect-only commands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_ca_key_never_reaches_snapshots_or_a_learner_disk() {
    let _serial = SERIAL.lock().await;
    init_tracing();

    let ca = Ca::new();
    let mut fleet = Fleet::new(3, &ca);
    fleet.start_all();
    let operator = fleet.init().await;
    fleet.await_voters(3).await;

    let founder = &fleet.members[0];
    let mut op_client = dial_operator(founder, &operator).await;
    let hid = probe_history_id(&mut op_client).await;

    // A 4th daemon: certless, same cluster, same cluster_size — it enrolls
    // and joins (the voter set has room for a learner, just not a fourth
    // voter) and then simply never clears the ceiling. `plan_promotion`
    // refuses before any key transfer, so this disk is a control: a learner
    // that has done everything short of being keyed.
    let learner = Daemon::new_certless(fleet.cluster_id, &ca);
    learner.set_cluster_size(3);
    learner.set_static_discovery(&[founder.raft_target()]);
    learner.set_enrollment(&founder.api(""), FLEET_TOKEN);
    let mut learner = learner;
    learner.start();
    learner.await_phase("learner").await;

    // Force a real snapshot: cheap, repeated commands past the fixture's
    // `snapshot_log_entries = 32` threshold (ADR 0018's `LogsSinceLast`
    // policy), rather than assuming one exists. Leadership can move mid-loop
    // on a slow machine (observed in CI), so a refused mint re-resolves the
    // current leader and retries instead of staying pinned to the founder —
    // an ambiguous failure may double-mint a filler label, which is harmless.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut minted = 0u32;
    while minted < 64 {
        match op_client
            .mint_enroll_token(pb::MintEnrollTokenRequest {
                history_id: hid.to_vec(),
                role: pbcore::EnrollRole::Coordinator as i32,
                label: format!("snapshot-filler-{minted}"),
                ttl_seconds: None,
            })
            .await
        {
            Ok(_) => minted += 1,
            Err(status) => {
                assert!(
                    Instant::now() < deadline,
                    "minting filler token {minted} kept failing: {status}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                op_client = dial_operator(find_leader(&fleet).await, &operator).await;
            }
        }
    }

    let snap_dir = founder.data_dir().join("snap");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let has_snapshot = std::fs::read_dir(&snap_dir)
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(false);
        if has_snapshot {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the founder never produced a snapshot after {} filler commands",
            64
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The key material to hunt for, and the certificate to confirm is
    // legitimately present.
    let ca_key_path = founder.data_dir().join(coppice_tls::pki::CA_KEY_FILE);
    let key_pem = std::fs::read(&ca_key_path).expect("read the founder's ca.key");
    let (key_der, key_body) = key_fingerprints(&key_pem);
    let (cert_pem, _, _) = founder.tls_material();

    // The learner: no exclusion needed at all — nothing under its data dir
    // should ever have carried the key, including a file named `ca.key`
    // itself, because it was never keyed in the first place.
    assert!(
        !learner
            .data_dir()
            .join(coppice_tls::pki::CA_KEY_FILE)
            .exists(),
        "an unkeyed learner must never have a ca.key file at all (ADR 0037 §4)"
    );
    assert_dir_never_carries_key(&learner.data_dir(), &key_der, &key_body, None);

    // Every voter's storage — snapshot(s) and log segments — carries no key
    // material, with the one legitimate exception (`ca.key` itself) excluded.
    let mut cert_seen = false;
    for member in &fleet.members {
        let dir = member.data_dir();
        let skip = dir.join(coppice_tls::pki::CA_KEY_FILE);
        assert_dir_never_carries_key(&dir, &key_der, &key_body, Some(&skip));

        let mut files = Vec::new();
        walk_files(&dir, &mut files);
        for path in files {
            if path == skip {
                continue;
            }
            let bytes = std::fs::read(&path).expect("read voter storage file");
            if contains(&bytes, &cert_pem) {
                cert_seen = true;
            }
        }
    }
    assert!(
        cert_seen,
        "positive control failed: the CA *certificate* should be present in replicated \
         storage somewhere, and was not found anywhere — the scan itself is broken"
    );

    learner.stop().await.expect("learner stops cleanly");
    fleet.stop_all().await;
}

// ---------------------------------------------------------------------------
// 4. Granted grudgingly: a promotion that cannot proceed never keys anyone
// ---------------------------------------------------------------------------

/// ADR 0037 §4: "the key reaches a disk only for a promotion that can
/// actually proceed (§4 root-equivalence is granted grudgingly)." A learner
/// arriving at an already-full voter set (ADR 0037 §7's ceiling) is refused
/// before `ensure_key_transferred` is ever reached — `plan_promotion` checks
/// the ceiling first — so it must never appear in `key_holders` and its data
/// directory must never grow a `ca.key`, for as long as it keeps polling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_evidence_promotion_that_cannot_proceed_never_keys_the_candidate() {
    let _serial = SERIAL.lock().await;
    init_tracing();

    let ca = Ca::new();
    let mut fleet = Fleet::new(3, &ca);
    fleet.start_all();
    let operator = fleet.init().await;
    fleet.await_voters(3).await;

    let founder = &fleet.members[0];
    let mut op_client = dial_operator(founder, &operator).await;
    let hid = probe_history_id(&mut op_client).await;

    let learner = Daemon::new_certless(fleet.cluster_id, &ca);
    learner.set_cluster_size(3);
    learner.set_static_discovery(&[founder.raft_target()]);
    learner.set_enrollment(&founder.api(""), FLEET_TOKEN);
    let mut learner = learner;
    learner.start();
    let body = learner.await_phase("learner").await;
    let learner_id = body["node_id"]
        .as_u64()
        .expect("a caught-up learner reports its node id");

    // Sustained, not momentary: poll across several probe intervals so a
    // one-tick fluke cannot pass for the invariant.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let body = learner.readyz().await.1;
        assert_eq!(
            body["phase"], "learner",
            "the ceiling must hold: this seat must never reach voter: {body}"
        );
        assert!(
            !learner
                .data_dir()
                .join(coppice_tls::pki::CA_KEY_FILE)
                .exists(),
            "an unpromotable learner's data dir must never grow a ca.key file"
        );

        let status = admin::cluster_status(&mut op_client, hid)
            .await
            .expect("read cluster status while the learner polls");
        assert!(
            status.key_holders.iter().all(|h| h.node_id != learner_id),
            "an unpromotable learner must never appear in key_holders: {status:?}"
        );
        // Not a refusal, either — VOTER_SET_FULL/NO_REMOVABLE_PEER are
        // "keep polling" cases (ADR 0037 §6), never surfaced as a failure.
        assert!(body["last_admission_refusal"].is_null(), "{body}");

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    learner.stop().await.expect("learner stops cleanly");
    fleet.stop_all().await;
}

// ---------------------------------------------------------------------------
// 5. The crash window between the durable transfer ack and the confirmation
// ---------------------------------------------------------------------------

/// The transfer protocol's OTHER crash window (ADR 0037 §4), staged
/// deterministically: the candidate durably persists the CA key and
/// acknowledges `TransferCaKey`; the leader "crashes" (the failpoint aborts
/// the verb) before `ConfirmKeyPossession` is proposed. The disk now holds
/// the key with no confirmation — and in the launch-before-terminate
/// abandonment this test stages, nothing ever retries: the live voter set is
/// full, so the learner's own promotion attempts are refused *before* the
/// transfer path, and the operator's failed `ReplaceVoter` is simply never
/// re-issued.
///
/// What makes the disk visible anyway is the transfer INTENT, committed
/// before the key ever left the leader: it stays in the custody accounting
/// (`pending_key_transfers`, conservatively "possibly keyed") until a
/// completed transfer's confirmation resolves it. The test closes by
/// retrying the replacement (the failpoint is fire-once) and proving the
/// intent resolves into an ordinary confirmed holder.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_transfer_keeps_the_keyed_disk_visible_in_custody_accounting() {
    let _serial = SERIAL.lock().await;
    init_tracing();
    let ca = Ca::new();

    let mut founder = Daemon::new_certless(ClusterId::new(), &ca);
    let operator = form(&mut founder).await;
    let mut op = dial_operator(&founder, &operator).await;
    let hid = probe_history_id(&mut op).await;
    let token = coordinator_token(&mut op, hid).await;
    let founder_id = founder.readyz().await.1["node_id"]
        .as_u64()
        .expect("the founder has a node id");

    // A caught-up learner at a full (1/1) live voter set: the §7 hands-off
    // path can never fire (the predecessor is alive), so only `ReplaceVoter`
    // can key it.
    let mut learner = newcomer(founder.cluster_id, &ca, &founder, &token, 1);
    learner.start();
    learner.await_phase("learner").await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let learner_id = loop {
        let (_, body) = learner.readyz().await;
        if body["replication_lag"].as_u64() == Some(0) && body["leader_contact_stale"] == false {
            break body["node_id"].as_u64().expect("the learner has a node id");
        }
        assert!(
            Instant::now() < deadline,
            "the learner never settled: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // Arm the crash window and drive the replacement into it. A transient
    // catch-up refusal retries (the gate can flicker right after the lag
    // settles); the failpoint abort is the outcome under test.
    std::env::set_var("COPPICE_TEST_FAILPOINT", TRANSFER_BEFORE_CONFIRM);
    let _guard = FailpointGuard;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = op
            .replace_voter(pb::ReplaceVoterRequest {
                history_id: hid.to_vec(),
                old_node_id: founder_id,
                new_node_id: learner_id,
            })
            .await
            .expect_err("the staged crash must abort the replacement, not complete it");
        if status.message().contains(TRANSFER_BEFORE_CONFIRM) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the replacement never reached the staged window: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The key reached the disk (the ack preceded the abort)...
    assert!(
        learner
            .data_dir()
            .join(coppice_tls::pki::CA_KEY_FILE)
            .exists(),
        "the candidate acknowledged a durable transfer, so its ca.key must exist"
    );
    // ...and the §4 accounting sees it: an unresolved intent, not a
    // confirmed holder.
    let status = admin::cluster_status(&mut op, hid)
        .await
        .expect("cluster status");
    assert!(
        status
            .pending_key_transfers
            .iter()
            .any(|p| p.node_id == learner_id),
        "the keyed-but-unconfirmed disk must be visible as an unresolved intent: {status:?}"
    );
    assert!(
        !status.key_holders.iter().any(|h| h.node_id == learner_id),
        "no confirmation landed, so the learner must not be a confirmed holder: {status:?}"
    );

    // Abandonment persists: the full live voter set keeps refusing the
    // learner's own promotion (visibly — the hold reaches `/readyz`), and
    // several settled-interval ticks later the unresolved intent is still
    // the only custody record. Nothing quietly resolves or drops it.
    tokio::time::sleep(Duration::from_secs(7)).await;
    let (_, body) = learner.readyz().await;
    assert_eq!(body["phase"], "learner", "{body}");
    let hold = body["promotion_hold"].as_str().unwrap_or_default();
    assert!(
        has_marker(hold, NO_REMOVABLE_PEER) || has_marker(hold, VOTER_SET_FULL),
        "the abandoned learner keeps polling against a full live set: {body}"
    );
    let status = admin::cluster_status(&mut op, hid)
        .await
        .expect("cluster status");
    assert!(
        status
            .pending_key_transfers
            .iter()
            .any(|p| p.node_id == learner_id),
        "the unresolved intent must survive abandonment indefinitely: {status:?}"
    );
    assert!(!status.key_holders.iter().any(|h| h.node_id == learner_id));

    // The ordinary retry resolves it (the failpoint latch is spent): the
    // transfer re-acks idempotently, the confirmation lands, the joint
    // change commits — and the intent collapses into a confirmed holder.
    admin::replace_voter(&mut op, hid, founder_id, learner_id)
        .await
        .expect("the retried replacement completes");
    learner.await_phase("voter").await;
    let mut op = dial_operator(&learner, &operator).await;
    let status = admin::cluster_status(&mut op, hid)
        .await
        .expect("cluster status from the new sole voter");
    assert!(
        status.key_holders.iter().any(|h| h.node_id == learner_id),
        "the resolved transfer must appear as a confirmed holder: {status:?}"
    );
    assert!(
        status.pending_key_transfers.is_empty(),
        "a confirmation must resolve the intent: {status:?}"
    );

    learner.stop().await.expect("learner stops cleanly");
    founder.stop().await.expect("founder stops cleanly");
}
