//! The ADR 0037 §1/§3/§9 daemon lifecycle, end to end.
//!
//! Everything here drives whole daemons through `bootstrap::run_with` — the
//! same code the binary runs — because the properties under test are about
//! *what a daemon serves in which state*, and only the real boot path decides
//! that. Each test stands a daemon up on real loopback ports, talks to it the
//! way an operator or a peer would (the admin socket, `/readyz`, the mTLS
//! `ProbeCluster` verb), and stops it.
//!
//! The load-bearing claim being tested is the marker's: **until
//! `formation_complete` exists, the external surface stays closed**, so a
//! failed formation is confined to the node that attempted it. That is checked
//! from three sides — the probe answer, the membership verbs, and the client
//! API — in every pre-formation state.

mod common;

use std::time::Duration;

use coppice_consensus::fs::RealFs;
use coppice_consensus::storage::{self, StorageOptions};
use coppice_consensus::{NodeOptions, StartIntent};
use coppice_coordinator::config::CliOverrides;
use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::id::ClusterId;
use coppice_core::time::Timestamp;

use common::{Ca, Daemon};

const PARKED: CliOverrides = CliOverrides {
    bootstrap: false,
    join: false,
};

const BOOTSTRAPPED: CliOverrides = CliOverrides {
    bootstrap: true,
    join: false,
};

/// A bootstrap policy that seeds one quota entity — the cheapest thing whose
/// arrival in replicated state is observable through the client API, so
/// "formation applied the policy" is asserted end to end rather than inferred.
fn policy_seeding(entity: &str) -> String {
    format!(
        r#"
[[quota_entity]]
id = "{entity}"
name = "seeded-by-formation"
quota = 1000000000
"#
    )
}

/// The operator credential `init` printed, as a client identity.
fn operator_identity(operator: &OperatorPem) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    (
        operator.ca_pem.as_bytes().to_vec(),
        operator.cert_pem.as_bytes().to_vec(),
        operator
            .key_pem
            .as_ref()
            .expect("no CSR was supplied, so the cluster minted the keypair")
            .as_bytes()
            .to_vec(),
    )
}

fn marks(daemon: &Daemon) -> storage::FormationMarks {
    storage::read_formation_marks(&RealFs::new(daemon.data_dir()))
        .expect("read formation marks")
        .expect("the data directory has a manifest")
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_fresh_daemon_parks_and_serves_only_readyz_and_the_admin_socket() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);
    daemon.start(PARKED);

    // Phase `waiting`, and 503: parked is alive-but-not-ready (ADR 0037 §9).
    let (status, body) = daemon.readyz().await;
    assert_eq!(status, 503, "a parked daemon must not be ready: {body}");
    assert_eq!(body["phase"], "waiting");
    assert_eq!(body["cluster_id"], daemon.cluster_id.to_string());
    // Nothing has been minted, so nothing is claimed.
    assert!(body.get("node_id").is_none(), "{body}");
    assert!(body.get("history_id").is_none(), "{body}");
    assert!(body["reason"]
        .as_str()
        .expect("a reason")
        .contains("coordinator init"));

    // The admin socket answers the same document, without the client listener.
    let AdminReply::Status { status } = daemon.admin(AdminCall::Status).await else {
        panic!("expected a status reply");
    };
    assert_eq!(serde_json::to_value(status.phase).unwrap(), "waiting");

    // The client API is not served at all.
    let response = reqwest::get(daemon.api("/api/v1/overview"))
        .await
        .expect("client listener is up");
    assert_eq!(response.status().as_u16(), 404);

    // Discoverable, and explicitly not joinable.
    let leaf = ca.leaf();
    let probe = daemon
        .probe(&ca.pem, &leaf.cert_pem, &leaf.key_pem)
        .await
        .expect("a parked daemon answers ProbeCluster");
    assert!(!probe.initialized);
    assert!(probe.history_id.is_empty());
    assert_eq!(probe.cluster_id, daemon.cluster_id.to_string());

    daemon.stop().await.expect("parked daemon stops cleanly");
}

#[tokio::test]
async fn init_forms_a_single_voter_cluster_opens_the_api_and_stamps_the_marker() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;

    let entity = coppice_core::id::QuotaEntityId::new();
    let reply = daemon
        .admin(AdminCall::Init {
            policy: Some(policy_seeding(&entity.to_string())),
            operator_csr: None,
            operator_cn: Some("day0".to_string()),
        })
        .await;

    let AdminReply::Formed {
        node_id, operator, ..
    } = reply
    else {
        panic!("expected the cluster to form, got {reply:?}");
    };
    assert_ne!(node_id, 0);
    // No CSR was supplied, so the cluster minted the keypair and must return
    // both halves plus the bundle — this terminal is the only place they exist.
    assert!(operator.cert_pem.contains("BEGIN CERTIFICATE"));
    assert!(operator
        .key_pem
        .as_ref()
        .expect("minted key")
        .contains("BEGIN"));
    assert!(operator.ca_pem.contains("BEGIN CERTIFICATE"));

    // Step 7 ran: both markers are on disk.
    let marks = marks(&daemon);
    assert!(marks.intent_at_us.is_some());
    assert!(marks.complete_at_us.is_some());
    assert!(!marks.failed());

    // A formed single voter, ready.
    let body = daemon.await_phase("voter").await;
    let (status, _) = daemon.readyz().await;
    assert_eq!(status, 200, "a caught-up single voter is ready: {body}");
    assert_eq!(body["node_id"], node_id);
    assert_eq!(body["is_leader"], true);
    assert_eq!(body["voters"].as_array().expect("voters").len(), 1);
    assert!(body["history_id"].is_string());
    assert!(body["instance_uuid"].is_string());

    // Step 6 ran: the seeded quota entity is in replicated state, reachable
    // through the client API that formation opened.
    let response = reqwest::get(daemon.api(&format!("/api/v1/quota-entities/{entity}")))
        .await
        .expect("client API is served");
    assert_eq!(
        response.status().as_u16(),
        200,
        "the bootstrap policy should have seeded {entity}"
    );

    // The external surface is open: the probe now reports an initialized
    // cluster, to a client presenting the cluster's own credentials.
    let (ca_pem, cert_pem, key_pem) = operator_identity(&operator);
    let probe = daemon
        .probe(&ca_pem, &cert_pem, &key_pem)
        .await
        .expect("probe a formed cluster");
    assert!(probe.initialized);
    assert_eq!(probe.node_id, Some(node_id));
    assert_eq!(probe.voters.len(), 1);
    assert_eq!(probe.history_id.len(), 16);

    daemon.stop().await.expect("formed daemon stops cleanly");
}

#[tokio::test]
async fn re_running_init_reports_already_initialized() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;

    let first = daemon.admin(plain_init()).await;
    assert!(matches!(first, AdminReply::Formed { .. }), "{first:?}");
    daemon.await_phase("voter").await;

    // Automation retries; the answer is a distinct success, not an error.
    let second = daemon.admin(plain_init()).await;
    let AdminReply::AlreadyInitialized { status } = second else {
        panic!("expected already-initialized, got {second:?}");
    };
    assert_eq!(serde_json::to_value(status.phase).unwrap(), "voter");

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn issue_operator_cert_signs_a_new_credential_post_formation() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;
    let formed = daemon.admin(plain_init()).await;
    let AdminReply::Formed { operator, .. } = formed else {
        panic!("expected the cluster to form, got {formed:?}");
    };
    daemon.await_phase("voter").await;

    // The day-0 recovery for a lost `init` output: a second, independent
    // operator credential, signed by the same cluster CA.
    let reply = daemon
        .admin(AdminCall::IssueOperatorCert {
            operator_csr: None,
            operator_cn: Some("break-glass".to_string()),
        })
        .await;
    let AdminReply::Issued { operator: reissued } = reply else {
        panic!("expected an issued certificate, got {reply:?}");
    };
    assert_ne!(reissued.cert_pem, operator.cert_pem);
    assert_eq!(
        reissued.ca_pem, operator.ca_pem,
        "re-issuance must not re-root the cluster"
    );

    // It is a real operator leaf under the cluster CA, and it authenticates.
    let verified =
        coppice_tls::pki::verify_leaf(reissued.ca_pem.as_bytes(), reissued.cert_pem.as_bytes())
            .expect("the reissued leaf verifies against the cluster CA");
    assert_eq!(
        verified.profile,
        coppice_tls::pki::Profile::Operator {
            cn: "break-glass".to_string()
        }
    );
    let (ca_pem, cert_pem, key_pem) = operator_identity(&reissued);
    daemon
        .probe(&ca_pem, &cert_pem, &key_pem)
        .await
        .expect("the reissued credential is accepted on the mTLS plane");

    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn a_crash_before_raft_initialize_restarts_into_formation_failed() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);

    // Stage exactly what a crash after step 2 leaves behind: an identity and a
    // formation intent, and no raft history at all.
    std::fs::create_dir_all(daemon.data_dir()).expect("create data dir");
    storage::init_forming(
        &RealFs::new(daemon.data_dir()),
        &StorageOptions::new(*daemon.cluster_id.0.as_bytes()),
        Timestamp::now().as_micros(),
    )
    .expect("stamp a formation intent");

    assert_failed_and_closed(&mut daemon, &ca).await;
}

#[tokio::test]
async fn a_crash_after_raft_initialize_restarts_into_formation_failed() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);

    // The other side of the boundary: the raft history exists — a resumable
    // design would call this "nearly done" — but no marker was stamped, so it
    // is identified as failed just the same.
    std::fs::create_dir_all(daemon.data_dir()).expect("create data dir");
    storage::init_forming(
        &RealFs::new(daemon.data_dir()),
        &StorageOptions::new(*daemon.cluster_id.0.as_bytes()),
        Timestamp::now().as_micros(),
    )
    .expect("stamp a formation intent");
    initialize_raft_history(&daemon, &ca).await;

    assert_failed_and_closed(&mut daemon, &ca).await;
}

#[tokio::test]
async fn wiping_the_data_directory_and_re_running_init_recovers() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);
    std::fs::create_dir_all(daemon.data_dir()).expect("create data dir");
    storage::init_forming(
        &RealFs::new(daemon.data_dir()),
        &StorageOptions::new(*daemon.cluster_id.0.as_bytes()),
        Timestamp::now().as_micros(),
    )
    .expect("stamp a formation intent");

    daemon.start(PARKED);
    daemon.await_phase("formation-failed").await;
    let _ = daemon.stop().await;

    let failed_history = storage::read_manifest_stamp(&RealFs::new(daemon.data_dir()))
        .expect("read stamp")
        .expect("manifest")
        .history_id;

    // The documented recovery, in full: wipe one data directory, restart
    // (which parks), re-run `init`.
    daemon.wipe_data_dir();
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;
    let reply = daemon.admin(plain_init()).await;
    assert!(matches!(reply, AdminReply::Formed { .. }), "{reply:?}");
    daemon.await_phase("voter").await;
    assert!(marks(&daemon).complete_at_us.is_some());

    // A re-formed cluster keeps its cluster_id but carries a NEW history
    // (ADR 0037 §3) — that distinction is what makes stale volumes from the
    // failed attempt distinguishable from the new history.
    let new_history = storage::read_manifest_stamp(&RealFs::new(daemon.data_dir()))
        .expect("read stamp")
        .expect("manifest")
        .history_id;
    assert_ne!(new_history, failed_history);

    daemon.stop().await.expect("recovered daemon stops cleanly");
}

#[tokio::test]
async fn two_formations_of_the_same_cluster_id_mint_distinct_histories() {
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // Two independent formations under the SAME logical cluster_id — the
    // re-formation case, compressed. `cluster_id` is the operator-chosen name
    // and may repeat; the history id names one raft lifetime and must not
    // (ADR 0037 §3). Deriving it from config would make every wiped-and-
    // re-formed cluster indistinguishable from its own corpse.
    let mut first = Daemon::new(cluster_id, &ca);
    first.start(PARKED);
    first.await_phase("waiting").await;
    assert!(matches!(
        first.admin(plain_init()).await,
        AdminReply::Formed { .. }
    ));
    let first_body = first.await_phase("voter").await;
    first.stop().await.expect("first daemon stops");

    let mut second = Daemon::new(cluster_id, &ca);
    second.start(PARKED);
    second.await_phase("waiting").await;
    assert!(matches!(
        second.admin(plain_init()).await,
        AdminReply::Formed { .. }
    ));
    let second_body = second.await_phase("voter").await;
    second.stop().await.expect("second daemon stops");

    let first_history = first_body["history_id"].as_str().expect("history");
    let second_history = second_body["history_id"].as_str().expect("history");
    assert_ne!(first_history, second_history);

    // And neither is the config-derived value the legacy flags stamp.
    let config_bytes = hex_bytes(cluster_id.0.as_bytes());
    assert_ne!(first_history, config_bytes);
    assert_ne!(second_history, config_bytes);

    // The stamp on disk agrees with what /readyz reported.
    let stamped = storage::read_manifest_stamp(&RealFs::new(first.data_dir()))
        .expect("read stamp")
        .expect("manifest")
        .history_id;
    assert_eq!(hex_bytes(&stamped), first_history);
}

#[tokio::test]
async fn a_formed_daemon_resumes_under_the_history_it_minted() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;
    assert!(matches!(
        daemon.admin(plain_init()).await,
        AdminReply::Formed { .. }
    ));
    let before = daemon.await_phase("voter").await;
    daemon.stop().await.expect("daemon stops");

    // Restart with no flags: the manifest stamp, not config, names the
    // history the resumed replica serves (ADR 0037 §1/§3).
    daemon.start(PARKED);
    let after = daemon.await_phase("voter").await;
    assert_eq!(after["history_id"], before["history_id"]);
    assert_eq!(after["node_id"], before["node_id"]);

    daemon.stop().await.expect("resumed daemon stops");
}

#[tokio::test]
async fn a_certless_daemon_parks_forms_and_resumes() {
    let ca = Ca::new();
    let mut daemon = Daemon::new_certless(ClusterId::new(), &ca);
    daemon.start(PARKED);

    // The ADR 0037 §4 minimal deployment: nothing provisioned. The daemon
    // still parks and says so — `/readyz` and the admin socket need no TLS.
    let (status, body) = daemon.readyz().await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["phase"], "waiting");

    // The mTLS probe surface cannot be served without material; a peer that
    // dials this daemon fails and skips it, per the ADR's probe posture.
    let leaf = ca.leaf();
    assert!(
        daemon
            .probe(&ca.pem, &leaf.cert_pem, &leaf.key_pem)
            .await
            .is_err(),
        "a certless daemon has no mTLS surface to answer probes on"
    );

    // Formation mints the first material (step 3) and everything follows.
    let reply = daemon.admin(plain_init()).await;
    let AdminReply::Formed { operator, .. } = reply else {
        panic!("expected the certless daemon to form, got {reply:?}");
    };
    daemon.await_phase("voter").await;
    let response = reqwest::get(daemon.api("/api/v1/overview"))
        .await
        .expect("client API is served");
    assert_eq!(response.status().as_u16(), 200);

    // The minted material is real: the probe plane now answers, to a client
    // presenting the cluster's own credentials.
    let (ca_pem, cert_pem, key_pem) = operator_identity(&operator);
    let probe = daemon
        .probe(&ca_pem, &cert_pem, &key_pem)
        .await
        .expect("the minted material serves the probe plane");
    assert!(probe.initialized);

    // And it persists: a restart resumes from the material formation wrote.
    daemon.stop().await.expect("formed daemon stops");
    daemon.start(PARKED);
    daemon.await_phase("voter").await;
    daemon.stop().await.expect("resumed daemon stops");
}

#[tokio::test]
async fn a_certless_init_with_candidates_it_cannot_probe_is_refused() {
    let ca = Ca::new();
    let mut daemon = Daemon::new_certless(ClusterId::new(), &ca);
    // Discovery names a peer, and this daemon has no credentials to ask it
    // anything. "Cannot probe" must not be read as "no cluster exists" — the
    // §3 step-1 guard fails closed rather than being silently skipped in the
    // one deployment (minimal, certless) it most protects.
    daemon.set_static_discovery(&["localhost:1".to_string()]);
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;

    let reply = daemon.admin(plain_init()).await;
    let AdminReply::Error { message } = reply else {
        panic!("expected the guard to refuse, got {reply:?}");
    };
    assert!(message.contains("double-init guard cannot"), "{message}");
    assert!(message.contains("empty discovery seed set"), "{message}");

    // The refusal is total: nothing durable happened, the daemon is still
    // parked, and a corrected `init` (fixed discovery or provisioned certs)
    // remains possible.
    assert!(
        storage::read_formation_marks(&RealFs::new(daemon.data_dir()))
            .expect("read marks")
            .is_none(),
        "the guard must refuse before the first durable act"
    );
    assert_eq!(daemon.readyz().await.1["phase"], "waiting");

    daemon
        .stop()
        .await
        .expect("still-parked daemon stops cleanly");
}

#[tokio::test]
async fn a_certless_daemon_that_discovers_itself_still_forms() {
    let ca = Ca::new();
    let mut daemon = Daemon::new_certless(ClusterId::new(), &ca);
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;
    // Several backends (`file` foremost) list this very process among the
    // candidates. A daemon is never the existing cluster its own formation
    // must not duplicate, so its own advertised address is excluded before
    // the guard decides anything — including the certless fail-closed check.
    daemon.set_static_discovery(&[daemon.raft_target()]);
    daemon.stop().await.expect("parked daemon stops");
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;

    let reply = daemon.admin(plain_init()).await;
    assert!(matches!(reply, AdminReply::Formed { .. }), "{reply:?}");
    daemon.await_phase("voter").await;

    daemon.stop().await.expect("formed daemon stops cleanly");
}

/// Hex of raw bytes, mirroring the daemon's identity rendering.
fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[tokio::test]
async fn the_probe_guard_refuses_init_when_a_candidate_is_already_initialized() {
    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // An existing cluster. Formed through the legacy `--bootstrap` flag so it
    // keeps the shared test CA and the second daemon can still reach it —
    // enrolling a second coordinator against a cluster-minted CA is chunk 04.
    let mut existing = Daemon::new(cluster_id, &ca);
    existing.start(BOOTSTRAPPED);
    existing.await_phase("voter").await;

    // A second daemon that would form the same cluster a second time.
    let mut newcomer = Daemon::new(cluster_id, &ca);
    newcomer.set_static_discovery(&[existing.raft_target()]);
    newcomer.start(PARKED);
    newcomer.await_phase("waiting").await;

    let reply = newcomer.admin(plain_init()).await;
    let AdminReply::Error { message } = reply else {
        panic!("expected the probe guard to refuse, got {reply:?}");
    };
    assert!(
        message.contains("already reports an initialized cluster"),
        "{message}"
    );

    // The refusal is total: nothing was stamped, so the daemon is still parked
    // and a corrected `init` is still possible.
    assert!(
        storage::read_formation_marks(&RealFs::new(newcomer.data_dir()))
            .expect("read marks")
            .is_none(),
        "the guard must refuse before the first durable act"
    );
    assert_eq!(newcomer.readyz().await.1["phase"], "waiting");

    newcomer.stop().await.expect("newcomer stops cleanly");
    existing
        .stop()
        .await
        .expect("existing cluster stops cleanly");
}

/// A daemon whose directory records an incomplete formation must fail-stop
/// with its external surface closed on every side, and must say why.
async fn assert_failed_and_closed(daemon: &mut Daemon, ca: &Ca) {
    daemon.start(PARKED);

    let body = daemon.await_phase("formation-failed").await;
    let (status, _) = daemon.readyz().await;
    assert_eq!(status, 503);
    let reason = body["reason"].as_str().expect("a reason");
    assert!(reason.contains("wipe the data directory"), "{reason}");
    assert!(reason.contains("coppice coordinator init"), "{reason}");

    // The client API is not served.
    let response = reqwest::get(daemon.api("/api/v1/overview"))
        .await
        .expect("client listener is up");
    assert_eq!(response.status().as_u16(), 404);

    // `ProbeCluster` does not report `initialized`, so a parked peer that
    // discovers this node cannot mistake it for the cluster...
    let leaf = ca.leaf();
    let probe = daemon
        .probe(&ca.pem, &leaf.cert_pem, &leaf.key_pem)
        .await
        .expect("a fail-stopped daemon still answers ProbeCluster");
    assert!(!probe.initialized);

    // ...and the membership verbs refuse outright, so it cannot join even by
    // being told to.
    let err = daemon
        .try_add_learner(&ca.pem, &leaf.cert_pem, &leaf.key_pem)
        .await
        .expect_err("membership verbs must be refused before the marker exists");
    assert!(
        err.to_string().contains("has not formed a cluster"),
        "{err:#}"
    );

    // `init` against it reports the failure rather than resuming it.
    let reply = daemon.admin(plain_init()).await;
    let AdminReply::FormationFailed { reason, .. } = reply else {
        panic!("expected formation-failed, got {reply:?}");
    };
    assert!(reason.contains("never completed"), "{reason}");

    // The daemon exits with the diagnostic, not silently.
    let err = daemon
        .stop()
        .await
        .expect_err("a fail-stopped daemon exits with an error");
    assert!(err.to_string().contains("never completed"), "{err:#}");
}

/// Bring the raft history into existence on an already-stamped directory, then
/// drop it — the "crashed after `raft.initialize`" state.
async fn initialize_raft_history(daemon: &Daemon, _ca: &Ca) {
    let root = daemon.data_dir();
    let certs = root.parent().expect("tempdir root");
    let tls = coppice_tls::TlsStore::load(coppice_tls::TlsPaths {
        cert: certs.join("node.crt"),
        key: certs.join("node.key"),
        ca: certs.join("ca.crt"),
    })
    .expect("load tls store");

    let started = coppice_consensus::start(
        NodeOptions {
            history_id: *daemon.cluster_id.0.as_bytes(),
            data_dir: root,
            advertise_addr: daemon.raft_target(),
            election_timeout: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(100),
            rpc_timeout: Duration::from_secs(2),
            snapshot_log_entries: 32,
            snapshot_keep_log_entries: 0,
            event_tap_capacity: 64,
            tls,
        },
        StartIntent::Restart,
    )
    .await
    .expect("open the stamped directory");
    started
        .handle
        .initialize_single_voter(daemon.raft_target())
        .await
        .expect("create the single-voter cluster");
    // Stop without stamping the marker — the crash.
    started
        .handle
        .shutdown()
        .await
        .expect("shut the replica down");
}

#[tokio::test]
async fn a_stalled_client_cannot_wedge_the_parked_to_formed_handover() {
    use tokio::io::AsyncWriteExt;

    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;

    // A connection that begins a request and never finishes it — a stalled
    // health probe, a port scanner, a client that died mid-write. The
    // pre-formation server counts it as in flight, so an unbounded graceful
    // drain would block the handover forever, and with it the whole daemon.
    let mut stalled = tokio::net::TcpStream::connect(daemon.client_addr())
        .await
        .expect("connect to the pre-formation client listener");
    stalled
        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .expect("send a partial request");

    let reply = daemon.admin(plain_init()).await;
    assert!(matches!(reply, AdminReply::Formed { .. }), "{reply:?}");

    // The full runtime takes the listeners despite the hostage connection.
    daemon.await_phase("voter").await;
    let response = reqwest::get(daemon.api("/api/v1/overview"))
        .await
        .expect("the client API is served after the handover");
    assert_eq!(response.status().as_u16(), 200);

    drop(stalled);
    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn a_concurrent_init_is_always_answered() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;

    // Two callers race — automation that timed out and retried, say. Formation
    // runs once; both get an answer, and neither hangs.
    let socket = daemon.admin_socket();
    let (a, b) = tokio::join!(
        coppice_coordinator::localadmin::call(&socket, plain_init()),
        async {
            // Far enough behind to pass the phase check and queue on the
            // formation channel, not so far that formation is already done.
            tokio::time::sleep(Duration::from_millis(5)).await;
            coppice_coordinator::localadmin::call(&socket, plain_init()).await
        }
    );

    let outcomes = [
        a.expect("first call answered"),
        b.expect("second call answered"),
    ];
    assert!(
        outcomes
            .iter()
            .any(|r| matches!(r, AdminReply::Formed { .. })),
        "exactly one caller should have formed the cluster: {outcomes:?}"
    );
    for outcome in &outcomes {
        assert!(
            matches!(
                outcome,
                AdminReply::Formed { .. } | AdminReply::AlreadyInitialized { .. }
            ),
            "no caller may be left without a usable answer: {outcome:?}"
        );
    }

    daemon.await_phase("voter").await;
    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn a_policy_that_parses_but_cannot_be_ordered_costs_no_data_directory() {
    let ca = Ca::new();
    let mut daemon = Daemon::new(ClusterId::new(), &ca);
    daemon.start(PARKED);
    daemon.await_phase("waiting").await;

    // Valid TOML, valid ids, and an unsatisfiable quota hierarchy: two
    // entities that are each other's parent. `parse_toml` accepts it; only the
    // topological ordering rejects it. That rejection must land before the
    // formation intent is stamped, or an operator typo has destroyed a data
    // directory that only a wipe can recover.
    let (a, b) = (
        coppice_core::id::QuotaEntityId::new(),
        coppice_core::id::QuotaEntityId::new(),
    );
    let reply = daemon
        .admin(AdminCall::Init {
            policy: Some(format!(
                r#"
[[quota_entity]]
id = "{a}"
parent = "{b}"
name = "a"
quota = 1

[[quota_entity]]
id = "{b}"
parent = "{a}"
name = "b"
quota = 1
"#
            )),
            operator_csr: None,
            operator_cn: None,
        })
        .await;

    let AdminReply::Error { message } = reply else {
        panic!("expected the policy to be refused, got {reply:?}");
    };
    assert!(message.contains("bootstrap policy"), "{message}");
    assert!(
        storage::read_formation_marks(&RealFs::new(daemon.data_dir()))
            .expect("read marks")
            .is_none(),
        "nothing durable may have happened"
    );

    // Still parked, and a corrected `init` still works.
    assert_eq!(daemon.readyz().await.1["phase"], "waiting");
    let reply = daemon.admin(plain_init()).await;
    assert!(matches!(reply, AdminReply::Formed { .. }), "{reply:?}");

    daemon.await_phase("voter").await;
    daemon.stop().await.expect("daemon stops cleanly");
}

#[tokio::test]
async fn the_admin_socket_is_owner_only() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let ca = Ca::new();
        let mut daemon = Daemon::new(ClusterId::new(), &ca);
        daemon.start(PARKED);
        daemon.await_phase("waiting").await;

        // Local access IS the authority for formation, so neither the socket
        // nor the directory holding it may be reachable by anyone else.
        let socket_mode = std::fs::metadata(daemon.admin_socket())
            .expect("socket exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(socket_mode & 0o077, 0, "socket mode {socket_mode:04o}");
        let dir_mode = std::fs::metadata(daemon.data_dir())
            .expect("data dir exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode & 0o077, 0, "data dir mode {dir_mode:04o}");

        daemon.stop().await.expect("daemon stops cleanly");
    }
}

/// `init` with nothing extra: the shape most callers use.
fn plain_init() -> AdminCall {
    AdminCall::Init {
        policy: None,
        operator_csr: None,
        operator_cn: None,
    }
}

// ---------------------------------------------------------------------------
// Readiness under partition (ADR 0037 §9)
// ---------------------------------------------------------------------------

/// Build a two-voter cluster from `Node`s (the legacy-flag path — chunk 03
/// has no network join) and return it with the leader first.
async fn two_voter_cluster(ca: &common::Ca) -> (common::Node, common::Node) {
    use coppice_consensus::Consensus;

    let cluster_id = ClusterId::new();
    let mut leader = common::Node::new(1, cluster_id, ca);
    leader
        .boot(CliOverrides {
            bootstrap: true,
            join: false,
        })
        .await;
    common::poll(Duration::from_secs(10), "node 1 becomes leader", || async {
        leader.is_leader()
    })
    .await;

    let mut follower = common::Node::new(2, cluster_id, ca);
    follower
        .boot(CliOverrides {
            bootstrap: false,
            join: true,
        })
        .await;

    leader
        .consensus()
        .add_learner(follower.raft_id(), follower.advertise.clone())
        .await
        .expect("add learner");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        match leader
            .consensus()
            .promote_voter(follower.raft_id(), None)
            .await
        {
            Ok(()) => break,
            Err(e) if e.is_retryable() && std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("promote failed: {e:#}"),
        }
    }
    (leader, follower)
}

#[tokio::test]
async fn a_voter_that_loses_its_leader_stops_reporting_ready() {
    use coppice_consensus::PROMOTION_LAG_MAX;

    let ca = Ca::new();
    let (mut leader, follower) = two_voter_cluster(&ca).await;

    // In contact: the follower is a ready voter.
    common::poll(
        Duration::from_secs(10),
        "follower reports ready while in contact",
        || async {
            let report = follower.readyz();
            report.is_ready(PROMOTION_LAG_MAX)
        },
    )
    .await;

    // Kill the leader. The follower's applied and known-committed indexes
    // freeze TOGETHER, so its local lag stays zero — the naive gate would
    // report 200 forever. What actually happens: its election timeout fires,
    // it becomes a candidate in a term with no leader, and readiness must
    // follow (ADR 0037 §9: within the promotion threshold of the *leader*,
    // and there is none).
    leader.kill().await;
    common::poll(
        Duration::from_secs(10),
        "partitioned follower stops reporting ready",
        || async {
            let report = follower.readyz();
            !report.is_ready(PROMOTION_LAG_MAX)
        },
    )
    .await;
    let report = follower.readyz();
    assert_eq!(report.replication_lag, 0, "the lag alone cannot see this");
}

#[tokio::test]
async fn a_leader_that_loses_quorum_stops_reporting_ready() {
    use coppice_consensus::PROMOTION_LAG_MAX;

    let ca = Ca::new();
    let (leader, mut follower) = two_voter_cluster(&ca).await;

    common::poll(
        Duration::from_secs(10),
        "leader reports ready while quorum holds",
        || async {
            let report = leader.readyz();
            report.is_leader && report.is_ready(PROMOTION_LAG_MAX)
        },
    )
    .await;

    // Kill the follower: the leader keeps its role (openraft has no automatic
    // stepdown) but its quorum acknowledgment goes stale, and a leader whose
    // cluster cannot hear it must not gate an instance refresh as "ready".
    follower.kill().await;
    common::poll(
        Duration::from_secs(10),
        "quorumless leader stops reporting ready",
        || async {
            let report = leader.readyz();
            !report.is_ready(PROMOTION_LAG_MAX)
        },
    )
    .await;
    assert!(leader.readyz().leader_contact_stale);
}
