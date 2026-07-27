//! A genuine **host** move under `admin set-address` (ADR 0037 §4/§6).
//!
//! The port-only repoint in `admin_membership.rs` never leaves the SANs the
//! member's leaf already carries, so it cannot catch the wedge this test
//! exists for: a member whose advertised *host* changes serves a leaf that
//! does not cover the new name, the leader's dial-back verification fails on
//! the TLS handshake, and no repoint can ever commit. The supported way out —
//! deliberately not a new verb — is the renewal task's config-truth rule
//! (`coppice_coordinator::tasks::renewal` module doc):
//!
//! 1. the operator rewrites the daemon's config to the new advertise host and
//!    restarts it;
//! 2. the renewal task notices the installed leaf does not cover the
//!    configured serving names and re-issues **immediately**, declaring
//!    `formation::leaf_sans(config)` — which now includes the new host;
//! 3. `admin set-address` then dial-back-verifies at the new name and commits
//!    both replicated facts (membership address + machine binding) together;
//! 4. the member's own convergence AddLearner replay at the new address
//!    no-ops.
//!
//! # The new host
//!
//! `127.0.0.2` is not bindable on stock macOS (`lo0` only aliases
//! `127.0.0.1`), so the move target is a **name**: `coppice-move.localhost`,
//! which the system resolver maps to loopback on macOS (mDNSResponder
//! resolves `*.localhost`) and on any Linux running systemd-resolved or
//! nss-myhostname. The test asserts resolution up front so an environment
//! without it fails with a diagnosis instead of a timeout. The raft *bind*
//! address stays `127.0.0.1:{port}`; only the advertised (and therefore
//! dialed, and TLS-verified) host moves — which is exactly the part the
//! serving certificate must cover.

mod common;

use std::time::Duration;

use coppice_coordinator::admin;
use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::id::{ClusterId, MachineId};
use coppice_proto::pb::core::v1 as pbcore;
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::pki;

use common::{poll, Ca, Daemon};

/// The move target: a loopback-mapped name the member's original leaf (SANs
/// `advertise_host` + `localhost`/`127.0.0.1`/`::1`) does NOT cover.
const NEW_HOST: &str = "coppice-move.localhost";

type AdminClient = coppice_net::admin::Client<tonic::transport::Channel>;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Dial `daemon`'s admin surface as the operator credential `init` minted.
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
    .expect("dial the admin surface as the operator")
}

/// Form a cluster on `daemon` the only way one can be formed (ADR 0037 §3).
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
/// (ADR 0037 §5).
async fn coordinator_token(daemon: &Daemon, operator: &OperatorPem) -> String {
    let mut client = operator_client(daemon, operator).await;
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

/// A signer over the cluster CA, loaded from the forming voter's disk.
fn cluster_signer(daemon: &Daemon) -> pki::CaSigner {
    let (ca_pem, _, _) = daemon.tls_material();
    let key = pki::load_ca_key(&daemon.data_dir(), &ca_pem)
        .expect("the forming voter's data dir holds the CA key");
    pki::CaSigner::load(&ca_pem, &key).expect("load the cluster CA signer")
}

/// The SANs of the leaf currently installed at `daemon`'s `[tls]` cert path.
fn on_disk_sans(daemon: &Daemon) -> Vec<String> {
    let (_, cert_pem, _) = daemon.tls_material();
    pki::leaf_sans(&cert_pem).expect("the installed leaf parses")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_move_renews_the_leaf_then_set_address_verifies_and_commits() {
    init_tracing();

    // Fail early, with a diagnosis, in an environment whose resolver does not
    // map `*.localhost` to loopback (see the module doc).
    assert!(
        tokio::net::lookup_host((NEW_HOST, 1)).await.is_ok(),
        "{NEW_HOST} does not resolve here; this test needs a resolver that maps \
         *.localhost to loopback (macOS, or Linux with systemd-resolved)"
    );

    let ca = Ca::new();
    let cluster_id = ClusterId::new();

    // A two-voter cluster stood up the self-converging way (§1): the member
    // enrolls, joins, and promotes itself, so its machine binding and its
    // leaf were both minted by the real admission path.
    let mut leader = Daemon::new_certless(cluster_id, &ca);
    leader.set_cluster_size(2);
    let operator = form(&mut leader).await;
    let token = coordinator_token(&leader, &operator).await;

    let mut member = Daemon::new_certless(cluster_id, &ca);
    member.set_cluster_size(2);
    member.set_static_discovery(&[leader.raft_target()]);
    member.set_enrollment(&leader.api(""), &token);
    member.start();
    member.await_phase("voter").await;

    let mut client = operator_client(&leader, &operator).await;
    let hid = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: String::new(),
        })
        .await
        .expect("probe")
        .into_inner()
        .history_id;

    let status_resp = client
        .cluster_status(pb::ClusterStatusRequest {
            history_id: hid.clone(),
        })
        .await
        .expect("read status")
        .into_inner();
    let leader_seat = status_resp.local_node_id;
    let member_rec = status_resp
        .membership
        .as_ref()
        .expect("membership present")
        .members
        .iter()
        .find(|m| m.node_id != leader_seat)
        .expect("the joined member is in membership")
        .clone();
    let member_machine: MachineId = status_resp
        .bindings
        .iter()
        .find(|b| b.node_id == member_rec.node_id)
        .expect("admission bound the member's machine identity")
        .machine_id
        .parse()
        .expect("binding machine id parses");
    let old_addr = member_rec.address.clone();

    // The premise of the whole test: the member's enrolled leaf does NOT
    // cover the host it is about to move to. Without this, the "renewal
    // re-issues for the new name" phase below would be vacuous.
    assert!(
        !on_disk_sans(&member).contains(&NEW_HOST.to_string()),
        "the member's enrolled leaf must not already cover {NEW_HOST}"
    );

    // --- Choreography step 1: config move + restart -----------------------
    // The operator's edit: advertise the new host. The bind address is left
    // alone (the name maps to loopback), so the same listener now serves a
    // name its certificate does not cover — the wedge under test.
    member.stop().await.expect("member stops cleanly");
    let config_path = member.config_path();
    let toml = std::fs::read_to_string(&config_path).expect("read member config");
    let rewritten = toml.replace(
        "advertise_host = \"localhost\"",
        &format!("advertise_host = \"{NEW_HOST}\""),
    );
    assert_ne!(toml, rewritten, "advertise_host line not found to rewrite");
    std::fs::write(&config_path, rewritten).expect("rewrite member config");
    member.start();

    let port = member
        .raft_target()
        .rsplit_once(':')
        .expect("raft target has a port")
        .1
        .to_string();
    let new_addr = format!("{NEW_HOST}:{port}");

    // --- Choreography step 2: the renewal task re-issues for the config ---
    // The SAN-mismatch fast path must fire well before the ~2/3-lifetime
    // timer: poll the on-disk leaf until it covers the new host. Generous
    // deadline — the very first attempt can race the member learning who
    // leads and then sits out one 30s retry backoff.
    {
        let member_ref = &member;
        poll(
            Duration::from_secs(120),
            "renewal re-issues the member's leaf with the configured new host",
            move || async move { on_disk_sans(member_ref).contains(&NEW_HOST.to_string()) },
        )
        .await;
    }

    // The renewed leaf must have hot-reloaded into the serving side too:
    // probe the member AT the new name, over the very TLS handshake the
    // leader's dial-back verification will perform.
    {
        let operator_ref = &operator;
        let member_node_id = member_rec.node_id;
        let new_addr = new_addr.clone();
        poll(
            Duration::from_secs(30),
            "the moved member serves its renewed leaf at the new name",
            move || {
                let new_addr = new_addr.clone();
                async move {
                    let Ok(mut probe_client) = admin::admin_channel(
                        &new_addr,
                        operator_ref.ca_pem.as_bytes(),
                        operator_ref.cert_pem.as_bytes(),
                        operator_ref
                            .key_pem
                            .as_ref()
                            .expect("cluster-minted operator key")
                            .as_bytes(),
                    )
                    .await
                    else {
                        return false;
                    };
                    probe_client
                        .probe_cluster(pb::ProbeClusterRequest {
                            cluster_id: String::new(),
                        })
                        .await
                        .is_ok_and(|resp| resp.into_inner().node_id == Some(member_node_id))
                }
            },
        )
        .await;
    }

    // --- Choreography step 3: the operator repoints the seat --------------
    // Retried under poll() because the verification dial can transiently race
    // the member's listener; a terminal refusal keeps failing until the
    // deadline names it.
    {
        let hid = hid.clone();
        let new_addr = new_addr.clone();
        let operator_ref = &operator;
        let leader_ref = &leader;
        let member_node_id = member_rec.node_id;
        poll(
            Duration::from_secs(30),
            "set-address dial-back-verifies the new host and commits",
            move || {
                let hid = hid.clone();
                let new_addr = new_addr.clone();
                async move {
                    let Ok(mut c) = admin::admin_channel(
                        &leader_ref.raft_target(),
                        operator_ref.ca_pem.as_bytes(),
                        operator_ref.cert_pem.as_bytes(),
                        operator_ref
                            .key_pem
                            .as_ref()
                            .expect("cluster-minted operator key")
                            .as_bytes(),
                    )
                    .await
                    else {
                        return false;
                    };
                    c.set_node_address(pb::SetNodeAddressRequest {
                        history_id: hid,
                        node_id: member_node_id,
                        address: new_addr,
                    })
                    .await
                    .is_ok()
                }
            },
        )
        .await;
    }

    // Both replicated facts moved together (§6): raft membership dials the
    // new host, and the machine binding follows it.
    let status_resp = client
        .cluster_status(pb::ClusterStatusRequest {
            history_id: hid.clone(),
        })
        .await
        .expect("re-read status")
        .into_inner();
    let member_after = status_resp
        .membership
        .as_ref()
        .expect("membership present")
        .members
        .iter()
        .find(|m| m.node_id == member_rec.node_id)
        .expect("the member is still in membership");
    assert_eq!(
        member_after.address, new_addr,
        "membership follows the host move"
    );
    assert_ne!(member_after.address, old_addr, "the address really moved");
    let binding_after = status_resp
        .bindings
        .iter()
        .find(|b| b.node_id == member_rec.node_id)
        .expect("the binding survives the repoint");
    assert_eq!(
        binding_after.address, new_addr,
        "the machine binding follows the host move"
    );

    // --- Choreography step 4: the member's own replay no-ops --------------
    // Present the member's machine identity and re-offer the §6 step-3
    // AddLearner exactly as its convergence loop does on every restart.
    let signer = cluster_signer(&leader);
    let (member_cert, member_key) = pki::mint_coordinator_local(&signer, &member_machine, &[])
        .expect("mint the member's machine leaf");
    let (cluster_ca, _, _) = leader.tls_material();
    let mut member_as_machine = admin::admin_channel(
        &leader.raft_target(),
        &cluster_ca,
        &member_cert,
        &member_key,
    )
    .await
    .expect("dial the admin surface as the member's machine");
    member_as_machine
        .add_learner(pb::AddLearnerRequest {
            history_id: hid,
            node_id: member_rec.node_id,
            address: new_addr,
        })
        .await
        .expect(
            "after set-address the member's own AddLearner replay is a no-op success, \
             not machine-address-conflict",
        );

    member.stop().await.expect("member stops cleanly");
    leader.stop().await.expect("leader stops cleanly");
}
