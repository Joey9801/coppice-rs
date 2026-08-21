//! Re-forming a fleet that lost its cluster (ADR 0037 §1/§3/§6).
//!
//! Every other suite in this crate exercises a cluster that *exists*. This one
//! starts from the state an operator actually fears: the fleet is down, and
//! most of its data volumes are gone. The ADR's answer is deliberately
//! unheroic — parked daemons never self-bootstrap however bad things get, one
//! deliberate `init` re-forms, and everything else converges onto the new
//! history by the ordinary path — and the properties worth pinning are the
//! ones that stop it from turning into two clusters:
//!
//! 1. wiped nodes **park and alarm** rather than forming anything;
//! 2. exactly **one** operator act re-forms, and the survivors of the wipe join
//!    that one history rather than each starting their own;
//! 3. the new history is genuinely new while the operator-chosen `cluster_id`
//!    is unchanged — the two identifiers answer different questions (ADR 0016 /
//!    ADR 0020), and this is the scenario that separates them;
//! 4. a node that comes back holding the **old** volume **fail-stops** on the
//!    history id rather than serving state the fleet has moved past — and is
//!    neither absorbed into the new history nor able to fork it;
//! 5. and the negative of (4): a voter resuming into a cluster still running
//!    its own history never fail-stops, however little discovery can tell it.
//!
//! The whole thing is driven through [`Fleet`]: shape-identical configs, file
//! discovery, one `init`. If re-forming needed a config edit or a per-node
//! instruction, that would be the finding.

mod common;

use std::time::Duration;

use coppice_coordinator::localadmin::{AdminCall, AdminReply};

use common::{poll, Ca, Fleet};

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// The `init` an operator types on the one node they have chosen to re-form
/// from — the same call [`Fleet::init`] makes, re-seeding the launch
/// template's enrollment token so the surviving configs are live again.
fn deliberate_init() -> AdminCall {
    AdminCall::Init {
        policy: Some(Fleet::seeding_policy()),
        operator_csr: None,
        operator_cn: Some("re-init".to_string()),
    }
}

/// A three-voter fleet loses two data volumes, is re-formed by one deliberate
/// `init`, and refuses to absorb the survivor of the old history.
///
/// Staged as a single test because the interesting assertions are about the
/// *sequence*: that the wiped nodes park before the `init` is what makes the
/// `init` the only thing that could have re-formed them, and the old-volume
/// node's refusal only means something against a cluster that has provably
/// moved to a new history.
///
/// Runtime is kept test-sized entirely by the fixture's `[pacing]` and
/// `[token_kdf]` knobs — no step here waits on anything but evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fleet_that_lost_its_volumes_re_forms_once_and_refuses_the_old_history() {
    init_tracing();
    let ca = Ca::new();
    let mut fleet = Fleet::new(3, &ca);

    // ---- life one ----------------------------------------------------------
    fleet.start_all();
    let _operator = fleet.init().await;
    fleet.await_voters(3).await;

    let first_life = fleet.members[0].readyz().await.1;
    let old_history = first_life["history_id"]
        .as_str()
        .expect("a formed cluster reports its history")
        .to_string();
    let cluster_id = first_life["cluster_id"]
        .as_str()
        .expect("the logical cluster id")
        .to_string();
    // Every member belongs to that one history.
    for (i, member) in fleet.members.iter().enumerate() {
        assert_eq!(
            member.readyz().await.1["history_id"],
            old_history,
            "member {i} must share the fleet's one history"
        );
    }

    // The fleet goes down, and two of the three volumes do not come back.
    fleet.stop_all().await;
    // Members 0 and 1 lose everything; member 2 keeps its disk. Member 0 is
    // the one wiped node that must be re-inited, because [`Fleet`] baked its
    // address in as the fleet-wide enrollment endpoint (standing in for a
    // load-balanced name) — re-forming on a node the template cannot enroll
    // against would be a fixture artifact, not a deployment.
    fleet.members[0].wipe_installation();
    fleet.members[1].wipe_installation();

    // ---- the wiped nodes park, loudly --------------------------------------
    fleet.members[0].start();
    fleet.members[1].start();
    for i in [0, 1] {
        let body = fleet.members[i].await_phase("waiting").await;
        let (status, _) = fleet.members[i].readyz().await;
        assert_eq!(
            status, 503,
            "member {i} came back with no cluster and must not report ready: {body}"
        );
        // The alarm surface (ADR 0037 §9): what an operator or a probe sees.
        assert!(
            body.get("history_id").is_none(),
            "a wiped volume claims no history: {body}"
        );
        assert!(
            body.get("node_id").is_none(),
            "a wiped volume claims no identity: {body}"
        );
        assert_eq!(body["voters"].as_array().map(Vec::len), Some(0));
        assert!(
            body["reason"]
                .as_str()
                .expect("a parked daemon says why")
                .contains("coordinator init"),
            "the reason must name the one operator act that resolves it: {body}"
        );
        assert_eq!(body["cluster_id"], cluster_id);
    }
    // Neither of them formed anything while the other was there to find: two
    // parked daemons probing each other is precisely the shape a
    // self-bootstrapping daemon would turn into a split cluster. Several probe
    // rounds pass (the fixture's park backoff maxes at 250ms) and both are
    // still parked.
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in [0, 1] {
        assert_eq!(
            fleet.members[i].readyz().await.1["phase"],
            "waiting",
            "member {i} must not self-bootstrap, however long it waits"
        );
    }

    // ---- one deliberate re-init --------------------------------------------
    let reply = fleet.members[0].admin(deliberate_init()).await;
    let AdminReply::Formed { .. } = reply else {
        panic!("the re-init must form a fresh cluster, got {reply:?}");
    };
    fleet.members[0].await_phase("voter").await;

    // Member 1 was told nothing. It enrolls against the endpoint its unchanged
    // config already names, discovers the re-formed node through the shared
    // registration directory, and joins — the ordinary §6 path, on a cluster
    // that is minutes younger than its own config.
    let rejoined = fleet.members[1].await_phase("voter").await;

    let new_history = fleet.members[0].readyz().await.1["history_id"]
        .as_str()
        .expect("the re-formed cluster reports its history")
        .to_string();
    assert_ne!(
        new_history, old_history,
        "wiping and re-forming mints a NEW raft history: a re-init is not a resume (ADR 0016)"
    );
    assert_eq!(
        fleet.members[0].readyz().await.1["cluster_id"],
        cluster_id.as_str(),
        "the operator-chosen cluster_id is config, and survives the volumes (ADR 0020)"
    );
    // No second history: the node that was not re-inited joined the one that
    // was, rather than forming its own and leaving two clusters wearing the
    // same name.
    assert_eq!(
        rejoined["history_id"], new_history,
        "the surviving wiped node must join the re-formed history, not start another: {rejoined}"
    );
    poll(
        Duration::from_secs(60),
        "the re-formed cluster settles at two voters",
        || async {
            let a = fleet.members[0].readyz().await.1;
            let b = fleet.members[1].readyz().await.1;
            a["voters"].as_array().map(Vec::len) == Some(2)
                && b["voters"].as_array().map(Vec::len) == Some(2)
        },
    )
    .await;
    let new_voters: Vec<serde_json::Value> = fleet.members[0].readyz().await.1["voters"]
        .as_array()
        .expect("voters")
        .clone();

    // ---- the old volume comes back -----------------------------------------
    // Member 2 still holds its disk from life one: it resumes as a voter of a
    // three-voter membership whose other two seats are addresses that now
    // serve a *different* history. Resuming that far is expected — nothing on
    // its disk is inconsistent, which is exactly the trap — and what it must
    // do next is **fail-stop**, not sit there serving a history the fleet has
    // moved past (ADR 0037 §3 Consequences).
    //
    // Watch `/readyz` from the moment it starts: the fail-stop publishes the
    // terminal phase before it drains (§9), and the assertion below would
    // otherwise be racing the listener's shutdown.
    let readyz_url = fleet.members[2].api("/readyz");
    let terminal_readyz = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        while tokio::time::Instant::now() < deadline {
            if let Ok(response) = client.get(&readyz_url).send().await {
                let status = response.status().as_u16();
                if let Ok(body) = response.json::<serde_json::Value>().await {
                    if body["phase"] == "history-superseded" {
                        return Some((status, body));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    });

    fleet.members[2].start();
    let stale = fleet.members[2]
        .await_phase_in(&["voter", "learner", "joining"])
        .await;
    assert_eq!(
        stale["history_id"], old_history,
        "member 2 resumed the history stamped on its own disk: {stale}"
    );

    // The daemon stops itself. Nothing signals it — `await_exit` waits for
    // `run_with` to return on its own — and it must return the refusal, so the
    // process exits nonzero and a `Restart=always` unit loops on the same
    // alarm rather than quietly serving the old history forever.
    let exit = fleet.members[2]
        .await_exit(Duration::from_secs(120))
        .await
        .expect_err("the old-volume node must fail-stop, not keep serving");
    let refusal = format!("{exit:#}");
    assert!(
        refusal.contains("history-superseded"),
        "the exit must carry the machine-readable marker: {refusal}"
    );
    for history in [&old_history, &new_history] {
        assert!(
            refusal.contains(history.as_str()),
            "the refusal must name both histories so an operator can check the surviving one \
             against a backup ({history} missing): {refusal}"
        );
    }
    assert!(
        refusal.contains("wipe") && refusal.contains("restore"),
        "the refusal must name both remedies — restore the old history, or wipe into the new \
         one: {refusal}"
    );

    // And the terminal state was readable on `/readyz` on the way out (§9):
    // 503, with the phase automation matches on.
    let (status, body) = terminal_readyz
        .await
        .expect("the readyz watcher joined")
        .expect("the fail-stopping daemon must publish `history-superseded` on /readyz");
    assert_eq!(status, 503, "a superseded replica is never ready: {body}");
    assert_eq!(
        body["history_id"], old_history,
        "the terminal report still names the history it is stopping over: {body}"
    );

    // Several election timeouts (1s under the fixture) and many convergence
    // ticks: long enough for anything member 2 could have done to the new
    // cluster to have happened. The properties that mattered before the
    // fail-stop landed still matter: the new history was never forked and the
    // old-history node was never absorbed.
    tokio::time::sleep(Duration::from_secs(4)).await;
    for i in [0, 1] {
        let body = fleet.members[i].readyz().await.1;
        assert_eq!(
            body["history_id"], new_history,
            "member {i} must still be on the re-formed history: {body}"
        );
        assert_eq!(
            body["voters"].as_array().expect("voters"),
            &new_voters,
            "the old-history node must not have changed the new cluster's membership: {body}"
        );
    }
    assert!(
        !new_voters.iter().any(|v| v["node_id"] == stale["node_id"]),
        "the old-history node must not hold a seat in the new cluster: {stale}"
    );

    fleet.stop_all().await;
}

/// The negative half of the same rule: a voter that resumes into a cluster
/// **still running its own history** never fail-stops — not when discovery has
/// gone stale, and not when it has gone empty.
///
/// This is the case the supersession check is one bad predicate away from
/// killing, and it is by far the more common one: every ordinary restart, every
/// rolling reboot, every daemon that comes back while its peers are mid-
/// election passes through "a voter that cannot see its cluster yet". The
/// trigger is a *positive* observation of a formed same-`cluster_id`
/// different-`history_id` cluster and nothing else, so an absence of evidence
/// — no discovery entries, no answering peer — must read as no evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resuming_voter_of_a_live_cluster_never_fail_stops() {
    init_tracing();
    let ca = Ca::new();
    let mut fleet = Fleet::new(2, &ca);

    fleet.start_all();
    let _operator = fleet.init().await;
    fleet.await_voters(2).await;
    let history = fleet.members[0].readyz().await.1["history_id"]
        .as_str()
        .expect("a formed cluster reports its history")
        .to_string();

    // Down it goes, and its registration with it.
    fleet.members[1].stop().await.expect("a clean stop");
    // Empty the shared registration directory outright: on the way back up
    // this member's discovery answers *nothing*, which must be indistinguish-
    // able from an ordinary restart. Its own registration reappears when it
    // binds; member 0's does not, because member 0 registered once at startup.
    let registry = fleet.registry_dir();
    for entry in std::fs::read_dir(&registry).expect("read the registration directory") {
        let entry = entry.expect("registration entry");
        std::fs::remove_file(entry.path()).expect("remove registration");
    }

    fleet.members[1].start();
    let resumed = fleet.members[1].await_phase("voter").await;
    assert_eq!(
        resumed["history_id"], history,
        "a resume is not a re-init: the history is the one on this disk: {resumed}"
    );

    // Well past the corroboration window (three convergence rounds at the
    // fixture's 250ms settled interval) and past several election timeouts.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        fleet.members[1].is_running(),
        "a healthy resuming voter must never fail-stop over stale or empty discovery"
    );
    let (status, body) = fleet.members[1].readyz().await;
    assert_eq!(status, 200, "and it must be serving: {body}");
    assert_eq!(body["phase"], "voter", "{body}");
    assert_eq!(body["history_id"], history, "{body}");

    fleet.stop_all().await;
}
