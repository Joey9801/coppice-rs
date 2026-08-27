//! Cluster CA re-rooting (ADR 0037 §4), end to end against a real fleet.
//!
//! `rotate.rs`'s doc comment lays out the shape this pins: `begin` opens a
//! **dual-trust window** — a two-root bundle, new root first (the active
//! signing root) and the outgoing root second, still a trust anchor — and
//! `complete` closes it by dropping the outgoing root. Four claims, each
//! staged against its own formed 3-voter fleet:
//!
//! 1. `begin_opens_a_dual_trust_window_that_serves_both_roots`: the leader's
//!    `begin` call reports a two-root bundle with the new root active, backs
//!    up its key, and gets every other key-holding voter installed — and the
//!    original (pre-rotation) operator certificate keeps authenticating on
//!    the network admin plane throughout, because the outgoing root is still
//!    a trust anchor.
//! 2. `every_coordinator_renews_onto_the_new_root`: every replica ends up
//!    both *trusting* the recorded bundle and *serving* a leaf signed under
//!    the new root, with no operator action beyond `begin`. Those are two
//!    mechanisms — `begin` puts the bundle on every voter before it switches
//!    signing, and the renewal fast path (`tasks/renewal.rs`, paced by
//!    `[pacing] renewal_reevaluate_interval`, which this fixture shrinks to
//!    200ms) re-signs the leaf — so the test asserts both.
//! 3. `complete_is_refused_inside_the_leaf_lifetime_and_retires_the_old_root_under_force`:
//!    `complete` without `--force` refuses for a full leaf lifetime (30 days)
//!    after `begin`, no matter how far renewal has actually gotten; `--force`
//!    completes anyway, and from that instant the outgoing root's leaves —
//!    including the original operator certificate — stop authenticating,
//!    while a certificate issued fresh under the surviving root does.
//! 4. `rotate_ca_is_refused_off_the_leader_and_before_formation`: `begin` is
//!    refused on a non-leader (the new key must land where the disk that
//!    signs is) and before the cluster has formed at all, while `status`
//!    — deliberately not leader-gated — answers in both of the latter two
//!    situations' formed/non-leader half.
//!
//! Every test builds its own [`Ca`] + [`Fleet`] and tears it down with
//! `fleet.stop_all()`; nothing here shares fixture state across tests.

mod common;

use std::time::{Duration, Instant};

use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_tls::pki;

use common::{poll, Ca, Fleet};

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

/// Form an `n`-voter fleet the only way one forms (ADR 0037 §1/§3): identical
/// certless configs, one `init`, convergence to `n` voters.
async fn form_fleet(n: usize) -> (Fleet, OperatorPem) {
    let ca = Ca::new();
    let mut fleet = Fleet::new(n, &ca);
    fleet.start_all();
    for member in &fleet.members {
        member.await_phase("waiting").await;
    }
    let operator = fleet.init().await;
    fleet.await_voters(n).await;
    (fleet, operator)
}

/// The index of whichever fleet member currently reports itself the leader.
async fn fleet_leader_index(fleet: &Fleet) -> usize {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for (i, member) in fleet.members.iter().enumerate() {
            if member.readyz().await.1["is_leader"] == true {
                return i;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no fleet member reported itself the leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// How many `-----BEGIN CERTIFICATE-----` blocks a PEM bundle carries — the
/// cheap "is this a dual-trust bundle" probe a test needs without reaching
/// into `rotate`'s private certificate machinery.
fn cert_block_count(pem: &[u8]) -> usize {
    std::str::from_utf8(pem)
        .expect("a CA bundle is UTF-8")
        .matches("-----BEGIN CERTIFICATE-----")
        .count()
}

/// Every `-----BEGIN CERTIFICATE-----`…`-----END CERTIFICATE-----` block of a
/// PEM bundle, each standalone.
///
/// Deliberately no positional interpretation: position 0 of the *replicated*
/// bundle is the active signing root, but an on-disk copy makes no ordering
/// promise — `adopt_anchors` treats a bundle with the same certificate *set*
/// as already adopted, so a file written at stage time keeps stage-time order
/// through activation. Anything reading these files must select certificates
/// by identity, never by index.
fn cert_blocks(pem: &[u8]) -> Vec<String> {
    let text = std::str::from_utf8(pem).expect("a CA bundle is UTF-8");
    let marker = "-----END CERTIFICATE-----";
    let mut blocks = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("-----BEGIN CERTIFICATE-----") {
        let end = rest[start..]
            .find(marker)
            .map(|i| start + i + marker.len())
            .expect("an opened certificate block has a matching end marker");
        blocks.push(rest[start..end].to_string());
        rest = &rest[end..];
    }
    blocks
}

/// The trust anchors an operator holds if they followed the runbook's step 2.
///
/// The dual-trust window is one-directional and the runbook says so
/// (`docs/operations/re-rooting.md` step 2, "refresh operator trust anchors"):
/// it keeps the cluster **accepting** an operator certificate issued under the
/// outgoing root, and promises nothing about a *client's* static `--ca` file,
/// which goes stale the moment the coordinators re-sign their serving leaves
/// under the incoming root. So a test that dials during the window with the
/// pre-rotation CA file is not testing the window — it is racing the leader's
/// own renewal, and losing that race on a slow host says nothing about the
/// claim under test.
///
/// This is the refreshed file: the union of what the operator started with and
/// the bundle the member now serves. The certificate presented stays the day-0
/// operator leaf, so the assertion — an outgoing-root leaf still authenticates
/// — is the same one, minus the race.
fn refreshed_operator_anchors(member: &common::Daemon, original_ca_pem: &str) -> Vec<u8> {
    let (installed_ca_pem, _, _) = member.tls_material();
    let mut union: Vec<String> = Vec::new();
    for block in cert_blocks(installed_ca_pem.as_slice())
        .into_iter()
        .chain(cert_blocks(original_ca_pem.as_bytes()))
    {
        if !union.contains(&block) {
            union.push(block);
        }
    }
    union.join("\n").into_bytes()
}

/// `begin` opens the dual-trust window: two roots recorded, the new one
/// active, the outgoing one still good enough to authenticate — proved by
/// dialing the network admin plane with the pre-rotation operator credential
/// *after* `begin` has run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn begin_opens_a_dual_trust_window_that_serves_both_roots() {
    init_tracing();
    let (mut fleet, operator) = form_fleet(3).await;
    let original_ca_pem = operator.ca_pem.clone();
    let operator_key_pem = operator
        .key_pem
        .clone()
        .expect("the cluster minted the day-0 operator keypair (no CSR was supplied)");

    let leader_idx = fleet_leader_index(&fleet).await;
    let reply = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::RotationBegun { report } = reply else {
        panic!("expected the leader's rotate-ca begin to open a dual-trust window, got {reply:?}");
    };

    assert!(!report.resumed, "a fresh begin is not a resume");
    assert!(
        report.activated,
        "begin must run through to activation on a healthy fleet; a coverage refusal here \
         means a push failed: {report:?}"
    );
    assert_eq!(
        report.status.roots.len(),
        2,
        "begin must record a two-root bundle: {:?}",
        report.status.roots
    );
    assert!(
        report.status.roots[0].active,
        "position 0 is the active (new) signing root: {:?}",
        report.status.roots
    );
    assert!(
        !report.status.roots[1].active,
        "position 1 is the outgoing root, no longer active: {:?}",
        report.status.roots
    );
    assert!(
        report.status.rotation_in_progress,
        "a two-root bundle is a rotation in progress"
    );
    assert!(
        report.key_backup.is_some(),
        "the outgoing key must be preserved on disk before being replaced"
    );
    assert_eq!(
        report.distribution.len(),
        2,
        "the other two voters are the whole distribution set: {:?}",
        report.distribution
    );
    for outcome in &report.distribution {
        assert!(
            outcome.installed,
            "peer {} did not take the incoming CA key: {:?}",
            outcome.node_id, outcome.error
        );
    }

    // The dual-trust window's whole point: a leaf issued under the outgoing
    // root — the day-0 operator certificate — still authenticates on the
    // network admin plane, because the outgoing root is still a recorded
    // trust anchor.
    //
    // The anchors are the runbook's refreshed ones rather than the day-0 file:
    // the claim under test is about the *client* certificate the cluster
    // accepts, and the day-0 `--ca` file stops verifying the *server* the
    // moment the leader re-signs its serving leaf (documented, one-directional,
    // and nothing to do with this assertion). The certificate presented is
    // still the day-0 operator leaf.
    let anchors = refreshed_operator_anchors(&fleet.members[leader_idx], &original_ca_pem);
    assert_eq!(
        cert_block_count(&anchors),
        2,
        "begin must leave the leader's own trust store holding both roots before it returns"
    );
    fleet.members[leader_idx]
        .probe(
            &anchors,
            operator.cert_pem.as_bytes(),
            operator_key_pem.as_bytes(),
        )
        .await
        .expect(
            "the original operator certificate must still authenticate during the dual-trust \
             window",
        );

    fleet.stop_all().await;
}

/// Every replica ends up on the new root with no operator action beyond
/// `begin` — both halves of that: its trust store holds the two-root bundle
/// (`begin` installs it on the voters, and the renewal loop adopts it from
/// replicated state anywhere else), and its serving leaf is signed by the new
/// root alone (the renewal fast path, `tasks/renewal.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_coordinator_renews_onto_the_new_root() {
    init_tracing();
    let (mut fleet, operator) = form_fleet(3).await;

    let leader_idx = fleet_leader_index(&fleet).await;
    let reply = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::RotationBegun { report } = reply else {
        panic!("expected rotate-ca begin to succeed, got {reply:?}");
    };
    // Activation is what the rest of this test is about. Without this gate a
    // coverage refusal (`activated: false`) leaves the bundle `[old, new]`
    // with the OLD root active, every leaf trivially "under the active root",
    // and the poll below passes instantly against a rotation that never
    // happened — and the file check then fails with a misleading message.
    assert!(
        report.activated,
        "begin must run through to activation on a healthy fleet; a coverage refusal here \
         means a push failed: {report:?}"
    );

    // The activated (incoming) root's identity, from the leader's own report.
    // The turnover poll below pins each member to this serial rather than
    // trusting `leaf_under_active_root` alone: every status field is computed
    // against the member's OWN applied state, and a follower that has not yet
    // applied the activation entry reports the STAGE-phase bundle — two roots,
    // the OUTGOING one still active — under which its outgoing-root leaf
    // satisfies `leaf_under_active_root`, `installed_matches_replicated`, and
    // `roots == 2` simultaneously. This suite failed exactly there under CI
    // load: the poll passed against a lagging member's stage-phase view, and
    // the on-disk leaf check then found the outgoing-root leaf. Requiring the
    // member to report the incoming serial at position 0 is requiring it to
    // have applied the activation locally, which is what makes its
    // `leaf_under_active_root` mean "renewed under the NEW root".
    let incoming_serial = report
        .status
        .roots
        .first()
        .expect("an activated rotation has an active root at position 0")
        .serial
        .clone();

    // Wait on the daemon's OWN convergence predicate, not on a file-shaped
    // proxy for it.
    //
    // `installed_matches_replicated` is computed from the live `TlsStore`,
    // whereas `tls_material()` reads the three `[tls]` files off disk — and
    // `pki::install_leaf_material` writes all three *before* calling
    // `force_reload()`. So there is a real window in which the files already
    // show the new bundle while the store still serves the old one. Polling
    // the files and then asserting on the store races that window (observed:
    // this suite failed exactly there under the load of a full-file run, and
    // passed when run alone). Polling the store instead is both correct and
    // stronger: store convergence implies the files were already written.
    //
    // Both halves of turnover are asserted, because they are now two
    // different facts moving at two different speeds:
    //
    // - `installed_matches_replicated` — this replica *trusts* the recorded
    //   bundle. `begin` puts the bundle on every voter before it switches
    //   signing, and any replica it missed adopts it from replicated state
    //   without dialing anyone, so this is fast and needs no leader.
    // - `leaf_under_active_root` — this replica has *renewed* onto the new
    //   root. That takes a signature, so a follower has to reach the leader
    //   for it, and it is the half `complete` can strand.
    //
    // The leader is included deliberately — it renews through the same fast
    // path as everyone else (the loop compares its material against
    // replicated state, never "am I the leader"), and `begin` swapped only
    // its *key*, never its leaf. So this is a real assertion for it too.
    //
    // The ceiling is measured, not guessed: whole-fleet turnover ran 0.21s to
    // 0.36s across three runs under 12 concurrent CPU hogs on an 8-core host
    // (the fixture's `[pacing]`: 200ms re-evaluate, 300ms first retry). 10s
    // leaves room for an election to churn mid-window — the one thing that can
    // legitimately add seconds here, since a follower renews by dialing the
    // leader — and nothing else. Before the "trust before signature" ordering
    // landed in `rotate.rs`, a follower that lost the race to the leader's own
    // re-sign could not renew *at all* (it could not verify the leader it had
    // to dial), and no ceiling was the right fix for that.
    poll(
        Duration::from_secs(10),
        "every coordinator trusts the recorded two-root bundle and has renewed onto the new root",
        || async {
            for member in &fleet.members {
                let AdminReply::RotationStatus { status } =
                    member.admin(AdminCall::RotateCaStatus).await
                else {
                    return false;
                };
                let incoming_active_here = status
                    .roots
                    .first()
                    .is_some_and(|root| root.serial == incoming_serial);
                if !incoming_active_here
                    || !status.installed_matches_replicated
                    || !status.leaf_under_active_root
                    || status.roots.len() != 2
                {
                    return false;
                }
            }
            true
        },
    )
    .await;

    // Now that every store has swapped, the files behind them are certainly
    // written: each member's bundle carries both roots, and its leaf verifies
    // under the NEW root alone — the rotation's actual claim. The new root is
    // selected by identity — the block that is NOT the formation root — never
    // by file position (see `cert_pems` on why the on-disk order proves
    // nothing).
    let original_root_pem = cert_blocks(operator.ca_pem.as_bytes())
        .into_iter()
        .next()
        .expect("the formation bundle has one root");
    let new_root_pem = {
        let (ca_pem, _, _) = fleet.members[leader_idx].tls_material();
        cert_blocks(&ca_pem)
            .into_iter()
            .find(|block| *block != original_root_pem)
            .expect("the leader's bundle must carry the incoming root beside the original")
            .into_bytes()
    };
    for member in &fleet.members {
        let (ca_pem, cert_pem, _) = member.tls_material();
        assert_eq!(
            cert_block_count(&ca_pem),
            2,
            "member {} should hold both roots during the window",
            member.raft_target()
        );
        if let Err(e) = pki::verify_leaf(&new_root_pem, &cert_pem) {
            // Forensics before the panic: this assertion has failed flakily
            // under CI load after the store-level poll above passed, and a bare
            // `InvalidSignatureForPublicKey` cannot distinguish the candidate
            // causes — a disk leaf lagging the store, a mis-selected
            // `new_root_pem`, or a torn multi-file read. Name which known root
            // (if any) actually signs the leaf we read, and what the member's
            // own status says right now.
            let signer = |leaf: &[u8]| -> String {
                if pki::verify_leaf(original_root_pem.as_bytes(), leaf).is_ok() {
                    return "the ORIGINAL (outgoing) root".to_string();
                }
                for (i, block) in cert_blocks(&ca_pem).into_iter().enumerate() {
                    if block != original_root_pem
                        && block.as_bytes() != new_root_pem.as_slice()
                        && pki::verify_leaf(block.as_bytes(), leaf).is_ok()
                    {
                        return format!(
                            "block {i} of the member's own bundle — a root that is neither \
                             the formation root nor the leader's incoming root"
                        );
                    }
                }
                "no root either bundle carries (torn or foreign leaf)".to_string()
            };
            let status_now = match member.admin(AdminCall::RotateCaStatus).await {
                AdminReply::RotationStatus { status } => format!(
                    "installed_matches_replicated={} leaf_under_active_root={} roots={}",
                    status.installed_matches_replicated,
                    status.leaf_under_active_root,
                    status.roots.len()
                ),
                other => format!("unexpected status reply: {other:?}"),
            };
            panic!(
                "member {}'s renewed leaf must chain to the new root alone: {e}\n\
                 the on-disk leaf is signed by: {}\n\
                 the member's live status now says: {status_now}",
                member.raft_target(),
                signer(&cert_pem),
            );
        }
    }

    fleet.stop_all().await;
}

/// `complete` is a clock-gated verb: it refuses inside the leaf lifetime no
/// matter how far renewal has gotten, `--force` overrides that judgement, and
/// only once it has run does the outgoing root actually stop authenticating —
/// proved on both ends, the old operator credential going dark and a freshly
/// issued one working.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn complete_is_refused_inside_the_leaf_lifetime_and_retires_the_old_root_under_force() {
    init_tracing();
    let (mut fleet, operator) = form_fleet(3).await;
    let original_ca_pem = operator.ca_pem.clone();
    let operator_key_pem = operator
        .key_pem
        .clone()
        .expect("the cluster minted the day-0 operator keypair (no CSR was supplied)");

    let leader_idx = fleet_leader_index(&fleet).await;
    let begin_reply = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::RotationBegun { report } = begin_reply else {
        panic!("expected rotate-ca begin to succeed, got {begin_reply:?}");
    };
    assert!(
        report.activated,
        "begin must run through to activation on a healthy fleet; a coverage refusal here \
         means a push failed: {report:?}"
    );

    // Wait for turnover, mirroring the previous test, so this test's refusal
    // assertion below is against a fleet that has actually renewed — the
    // point being that turnover alone is not enough, only the clock (or
    // `--force`) is.
    // The predicate is the daemon's own `leaf_under_active_root`, not a
    // two-blocks-on-disk file check: `begin` now installs the recorded bundle
    // on every voter before it switches signing, so the *anchors* are in place
    // almost immediately and a bundle-shaped check would wave through a fleet
    // that has not renewed a single leaf — exactly what this wait is here to
    // rule out. Ceiling as in the previous test: 10s over a measured 0.21-0.36s
    // turnover under CPU pressure.
    //
    // Pinned to the incoming root's serial for the same reason as the previous
    // test: a follower that has not yet applied the activation entry satisfies
    // `leaf_under_active_root` against its stage-phase view (outgoing root
    // still active) while serving an outgoing-root leaf.
    let incoming_serial = report
        .status
        .roots
        .first()
        .expect("an activated rotation has an active root at position 0")
        .serial
        .clone();
    poll(
        Duration::from_secs(10),
        "every coordinator renews onto the new root before complete is attempted",
        || async {
            for member in &fleet.members {
                let AdminReply::RotationStatus { status } =
                    member.admin(AdminCall::RotateCaStatus).await
                else {
                    return false;
                };
                let incoming_active_here = status
                    .roots
                    .first()
                    .is_some_and(|root| root.serial == incoming_serial);
                if !incoming_active_here
                    || !status.leaf_under_active_root
                    || status.roots.len() != 2
                {
                    return false;
                }
            }
            true
        },
    )
    .await;

    // Re-resolve the leader rather than trusting `leader_idx` from `begin`:
    // the dual-trust window's own churn (renewal swaps serving material out
    // from under in-flight raft dials, `BadSignature` and transport errors
    // observed live in this suite) can cost the original leader its term
    // before turnover finishes, and `complete` — like `begin` — must land on
    // whoever leads *now*.
    let leader_idx = fleet_leader_index(&fleet).await;

    let refused = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaComplete { force: false })
        .await;
    let AdminReply::Error { message } = refused else {
        panic!("expected complete without --force to be refused, got {refused:?}");
    };
    assert!(
        message.contains("dual-trust window") || message.contains("leaf lifetime"),
        "the refusal should explain the leaf-lifetime bound, got: {message}"
    );

    let leader_idx = fleet_leader_index(&fleet).await;
    let completed = fleet.members[leader_idx]
        .admin(AdminCall::RotateCaComplete { force: true })
        .await;
    let AdminReply::RotationCompleted { report } = completed else {
        panic!("expected --force to complete the rotation, got {completed:?}");
    };
    assert_eq!(
        report.status.roots.len(),
        1,
        "complete must drop back to a single-root bundle: {:?}",
        report.status.roots
    );
    assert_eq!(
        report.retired.len(),
        1,
        "exactly the outgoing root retires: {:?}",
        report.retired
    );

    // The outgoing root is no longer a trust anchor anywhere: the original
    // day-0 operator certificate stops authenticating. `complete` adopts the
    // trimmed bundle into the answering daemon's own trust store before it
    // replies, so on this member it is already true — the poll covers the
    // in-flight connection the probe may still be reusing, and both of these
    // polls resolve in well under a second. 30s is the ceiling, not the
    // expectation.
    poll(
        Duration::from_secs(30),
        "the original operator certificate stops authenticating once the outgoing root retires",
        || {
            // Refreshed anchors again, so what this proves is the *client*
            // certificate being refused — the day-0 `--ca` file would fail
            // against the server certificate too, which is a different (and
            // already documented) fact.
            let ca = refreshed_operator_anchors(&fleet.members[leader_idx], &original_ca_pem);
            let cert = operator.cert_pem.clone();
            let key = operator_key_pem.clone();
            let member = &fleet.members[leader_idx];
            async move {
                member
                    .probe(&ca, cert.as_bytes(), key.as_bytes())
                    .await
                    .is_err()
            }
        },
    )
    .await;

    // A certificate minted fresh, after the retirement, authenticates fine.
    let issued = fleet.members[leader_idx]
        .admin(AdminCall::IssueOperatorCert {
            operator_csr: None,
            operator_cn: Some("post-rotation".to_string()),
        })
        .await;
    let AdminReply::Issued { operator: fresh } = issued else {
        panic!("expected a fresh operator certificate post-rotation, got {issued:?}");
    };
    let fresh_key_pem = fresh
        .key_pem
        .clone()
        .expect("the cluster minted the fresh operator keypair (no CSR was supplied)");
    poll(
        Duration::from_secs(30),
        "a freshly issued operator certificate authenticates post-rotation",
        || {
            let ca = fresh.ca_pem.clone();
            let cert = fresh.cert_pem.clone();
            let key = fresh_key_pem.clone();
            let member = &fleet.members[leader_idx];
            async move {
                member
                    .probe(ca.as_bytes(), cert.as_bytes(), key.as_bytes())
                    .await
                    .is_ok()
            }
        },
    )
    .await;

    fleet.stop_all().await;
}

/// `begin` is gated two ways `status` deliberately is not: it must run on the
/// leader (the new key lands on the disk that runs it), and it must run
/// against a formed cluster (there is no root to rotate before one exists).
/// `status` answers through both of those — it is read-only and answerable
/// on any replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rotate_ca_is_refused_off_the_leader_and_before_formation() {
    init_tracing();

    // (a) A formed cluster, but the call lands on a non-leader.
    let (mut fleet, _operator) = form_fleet(3).await;
    let leader_idx = fleet_leader_index(&fleet).await;
    let non_leader_idx = (0..fleet.members.len())
        .find(|&i| i != leader_idx)
        .expect("a 3-voter fleet has a non-leader member");

    let refused = fleet.members[non_leader_idx]
        .admin(AdminCall::RotateCaBegin)
        .await;
    let AdminReply::Error { message } = refused else {
        panic!("expected rotate-ca begin on a non-leader to be refused, got {refused:?}");
    };
    assert!(
        message.contains("not the leader"),
        "the refusal should say this replica is not the leader, got: {message}"
    );

    // status is deliberately not leader-gated: it answers on the non-leader
    // too, describing the single-root steady state.
    let status_reply = fleet.members[non_leader_idx]
        .admin(AdminCall::RotateCaStatus)
        .await;
    let AdminReply::RotationStatus { status } = status_reply else {
        panic!("expected rotate-ca status to answer on a non-leader, got {status_reply:?}");
    };
    assert!(
        !status.rotation_in_progress,
        "no rotation has begun yet: {status:?}"
    );
    assert_eq!(status.roots.len(), 1, "steady state is single-rooted");

    fleet.stop_all().await;

    // (b) A parked daemon: no cluster has formed at all.
    let ca = Ca::new();
    let mut unformed = Fleet::new(1, &ca);
    unformed.start_all();
    unformed.members[0].await_phase("waiting").await;

    let refused = unformed.members[0].admin(AdminCall::RotateCaBegin).await;
    let AdminReply::Error { message } = refused else {
        panic!("expected rotate-ca begin before formation to be refused, got {refused:?}");
    };
    assert!(
        message.contains("has not formed a cluster"),
        "the refusal should say the cluster has not formed, got: {message}"
    );

    unformed.stop_all().await;
}
