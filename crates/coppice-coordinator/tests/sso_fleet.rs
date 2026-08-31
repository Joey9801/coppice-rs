//! The one issue #45 acceptance criterion that needs a whole fleet: a
//! revocation racing an in-flight command resolves **in log order**, and does
//! so identically on every replica.
//!
//! ADR 0023 decides authorization twice — once at the API layer, before
//! anything is proposed, and once at apply, against the bindings as of the
//! command's own log position. The first exists for the client's sake; the
//! second is the authority, and the property it buys is exactly this one: a
//! command whose pre-check passed against bindings that were then replaced is
//! refused when it applies, deterministically, on all three nodes, because
//! every replica computes the same function of the same state at the same log
//! position.
//!
//! Staging that interleaving is the hard part, and it is why this suite is
//! separate. See [`the gate`](coppice_coordinator::failpoints::API_SUBMIT_BEFORE_PROPOSE)
//! for the mechanism; the short version is that the daemon parks between the
//! pre-check and the proposal until this test has committed the revocation and
//! read back its log index, so "pre-check first, revocation second, proposal
//! third" is established rather than hoped for.
//!
//! Fleet-shaped (three in-process daemons), so it is carved out of the
//! ordinary CI job and assigned to a `ci-fleet-*` shard in
//! `.config/nextest.toml`.

mod common;

use std::time::Duration;

use coppice_coordinator::failpoints::API_SUBMIT_BEFORE_PROPOSE;
use coppice_core::id::{JobId, QuotaEntityId};
use coppice_testkit::oidc::{FakeIdp, TokenClaims};
use serde_json::{json, Value};

use common::{poll, Ca, Fleet};

const CLIENT_ID: &str = "coppice";
const SUBMITTER_GROUP: &str = "batch-users";
const ADMIN_GROUP: &str = "platform-admins";

/// Member 1 serves its client listener over TLS, which is what makes the day-0
/// operator certificate presentable — the only authority a freshly formed
/// cluster has, and therefore the only way to install the first bindings.
///
/// Not member 0: that one forms the cluster and its address is the enrollment
/// endpoint baked into every member's config, so it has to stay plain HTTP.
const OPERATOR_EDGE: usize = 1;
/// Member 2 takes the submission whose proposal this test holds back.
const GATED: usize = 2;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Send a request and split the response into (status, `coppice-applied-index`,
/// JSON body).
async fn send(request: reqwest::RequestBuilder) -> (u16, Option<u64>, Value) {
    let response = request.send().await.expect("the request reaches the API");
    let status = response.status().as_u16();
    let applied = response
        .headers()
        .get(coppice_api::http::COPPICE_APPLIED_INDEX)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    let bytes = response.bytes().await.expect("read the response body");
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, applied, body)
}

fn submit_body(job: JobId, entity: QuotaEntityId) -> Value {
    json!({
        "job": job.to_string(),
        "image": "busybox",
        "command": ["true"],
        "requests": { "cpu_millis": 100, "memory_bytes": 1_048_576u64, "disk_bytes": 0 },
        "priority": 0,
        "quota_entity": entity.to_string(),
    })
}

fn entity_body(entity: QuotaEntityId, name: &str) -> Value {
    json!({
        "entity": entity.to_string(),
        "parent": null,
        "name": name,
        "quota_ucu": 1_000_000_000_000u64,
    })
}

/// The bindings list before the revocation: an unscoped admin group (so the
/// list is not a lockout) and the scoped submitter binding the racing
/// submission depends on.
fn bindings_with_submitter(entity: QuotaEntityId) -> Value {
    json!([
        { "group": ADMIN_GROUP, "role": "admin" },
        { "group": SUBMITTER_GROUP, "role": "submitter", "scope": entity.to_string() },
    ])
}

/// The same list with the submitter binding removed — the revocation.
fn bindings_without_submitter() -> Value {
    json!([{ "group": ADMIN_GROUP, "role": "admin" }])
}

/// Issue #45 acceptance criterion 3.
///
/// A job submission is held between its authorization pre-check — which
/// passed, against a bindings list that still granted the submitter role — and
/// its proposal. While it is held, the binding it relied on is revoked by an
/// `UpdateAuthorization` that commits at a known log index. The submission is
/// then released, proposes, and lands *after* that index.
///
/// It must be refused. Not because the pre-check refused it — the pre-check
/// passed, and the gate's marker file is the evidence that it did — but
/// because apply re-evaluates the same decision against the bindings as of the
/// command's own log position, where the binding no longer exists. And it must
/// be refused the same way everywhere: the job is absent from all three
/// replicas' state and all three agree on the bindings, at a log position past
/// the whole exchange.
#[tokio::test]
async fn a_revocation_racing_an_in_flight_submission_resolves_in_log_order() {
    init_tracing();
    let ca = Ca::new();
    let idp = FakeIdp::start().await;

    let mut fleet = Fleet::new(3, &ca);

    // The operator edge serves HTTPS so a client certificate can be presented
    // to it at all; the other two stay plain HTTP (member 0 because its own
    // address is the enrollment endpoint baked into every member's config).
    let (server_root_pem, operator_base) = fleet.members[OPERATOR_EDGE].set_client_tls(&ca);
    for member in &fleet.members {
        member.set_sso(&idp.issuer(), CLIENT_ID);
    }
    // Only the member that will take the submission is armed. A gate is
    // per-daemon config precisely so a fleet sharing one test process can hold
    // one replica's write path without touching any other's.
    fleet.members[GATED].arm_gates(&[API_SUBMIT_BEFORE_PROPOSE]);

    fleet.start_all();
    let operator_pem = fleet
        .init_with_policy(format!(
            "{}\n[[priority_multiplier]]\nindex = 0\nmultiplier = 1.0\n",
            Fleet::seeding_policy()
        ))
        .await;
    await_voters(&fleet, &server_root_pem, &operator_base, 3).await;

    let plain = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build the plain-http client");
    let operator = {
        let key = operator_pem
            .key_pem
            .as_ref()
            .expect("no CSR was supplied, so the cluster minted the keypair");
        // reqwest's rustls identity takes one PEM blob: key then chain.
        let mut identity = key.clone().into_bytes();
        identity.extend_from_slice(operator_pem.cert_pem.as_bytes());
        reqwest::Client::builder()
            .add_root_certificate(
                reqwest::Certificate::from_pem(&server_root_pem).expect("serving root"),
            )
            .identity(reqwest::Identity::from_pem(&identity).expect("operator identity"))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("build the operator-certificate https client")
    };
    let op_url = |path: &str| format!("{operator_base}{path}");

    // ---- Setup: an entity, and the binding the submission will rely on ----
    let entity = QuotaEntityId::new();
    let (status, _, body) = send(
        operator
            .post(op_url("/api/v1/quota-entities"))
            .json(&entity_body(entity, "org")),
    )
    .await;
    assert_eq!(
        status, 200,
        "the day-0 operator certificate creates the quota entity: {body}"
    );

    let (status, _, body) = send(
        operator
            .put(op_url("/api/v1/authorization"))
            .json(&json!({ "bindings": bindings_with_submitter(entity) })),
    )
    .await;
    assert_eq!(
        status, 200,
        "installing the pre-revocation bindings: {body}"
    );
    let granted_at = body["log_index"]
        .as_u64()
        .unwrap_or_else(|| panic!("the grant carries a log index: {body}"));

    // Every replica must have applied the grant before the submission's
    // pre-check reads it — otherwise the pre-check could fail for staleness
    // and this test would prove nothing about revocation at all.
    let alice = idp.sign(
        TokenClaims::new("alice")
            .audience(CLIENT_ID)
            .claim("groups", json!([SUBMITTER_GROUP]))
            .expires_in(600),
    );
    let gated_api = fleet.members[GATED].api("");
    poll(
        Duration::from_secs(30),
        "the gated member has applied the grant and accepts alice's token",
        || async {
            let (status, _, body) = send(
                plain
                    .get(format!("{gated_api}/api/v1/session?min_index={granted_at}"))
                    .bearer_auth(&alice),
            )
            .await;
            status == 200 && body["bindings"].as_array().is_some_and(|b| b.len() == 1)
        },
    )
    .await;

    // ---- The race ---------------------------------------------------------

    // (1) The submission. It runs its pre-check against the bindings above —
    // which grant it — and then parks, having proposed nothing.
    let job = JobId::new();
    let submission = {
        let request = plain
            .post(format!("{gated_api}/api/v1/jobs"))
            .bearer_auth(&alice)
            .json(&submit_body(job, entity));
        tokio::spawn(async move { send(request).await })
    };

    // (2) Wait until it is demonstrably parked. This is the whole point of
    // the gate: past this line, the pre-check has run and passed, and nothing
    // has been proposed.
    fleet.members[GATED]
        .await_gate(API_SUBMIT_BEFORE_PROPOSE)
        .await;

    // (3) Revoke, through a different member, and learn where it landed.
    let (status, _, body) = send(
        operator
            .put(op_url("/api/v1/authorization"))
            .json(&json!({ "bindings": bindings_without_submitter() })),
    )
    .await;
    assert_eq!(status, 200, "the revocation commits: {body}");
    let revoked_at = body["log_index"]
        .as_u64()
        .unwrap_or_else(|| panic!("the revocation carries a log index: {body}"));
    assert!(
        revoked_at > granted_at,
        "the revocation must land after the grant it replaces ({revoked_at} vs {granted_at})"
    );

    // (4) Release. The submission proposes now, so its log position is
    // strictly after `revoked_at` — the ordering this test asserts is
    // established by construction, not observed after the fact.
    fleet.members[GATED].release_gate(API_SUBMIT_BEFORE_PROPOSE);

    let (status, _, body) = submission.await.expect("the submission task joins");
    assert_eq!(
        status, 403,
        "the submission's pre-check passed against bindings that no longer \
         exist at its log position, so apply refuses it — in log order, and \
         not by whichever check happened to run first: {body}"
    );
    assert_eq!(
        body["code"], "PERMISSION_DENIED",
        "an apply-time authorization refusal is a 403, the same answer the \
         pre-check would have given: {body}"
    );

    // ---- The outcome is the same on every replica -------------------------
    //
    // One more successful write, so there is a log index every replica can be
    // waited to: everything at or below it — the revocation, the refused
    // submission, and this — has applied wherever `min_index` is satisfied.
    let (status, _, body) = send(
        operator
            .post(op_url("/api/v1/quota-entities"))
            .json(&entity_body(QuotaEntityId::new(), "after-the-race")),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let settled_at = body["log_index"]
        .as_u64()
        .unwrap_or_else(|| panic!("the follow-up write carries a log index: {body}"));
    assert!(
        settled_at > revoked_at + 1,
        "at least one log entry landed between the revocation ({revoked_at}) and \
         this follow-up write ({settled_at}) — the refused submission's own \
         position. Nothing else in this test proposes in that window, and the \
         gate proves the submission had not proposed at {revoked_at}, so this \
         is where it went: refused *at apply*, having consumed a log position, \
         rather than turned away by the pre-check without one."
    );

    for i in 0..fleet.members.len() {
        let (client, base) = if i == OPERATOR_EDGE {
            (&operator, operator_base.clone())
        } else {
            (&plain, fleet.members[i].api(""))
        };

        // The job. `min_index` is a read-your-writes wait, not a consistency
        // upgrade: it holds the read until this replica's own applied state is
        // at least `settled_at`, which is past the refused submission.
        let (status, applied, body) = send(
            client
                .get(format!("{base}/api/v1/jobs/{job}?min_index={settled_at}"))
                .bearer_auth(&alice),
        )
        .await;
        assert_eq!(
            status, 404,
            "member {i} must not hold the job: a command refused at apply \
             changes nothing but the log position it occupies: {body}"
        );
        assert!(
            applied.is_none_or(|a| a >= settled_at),
            "member {i} served the read from a view at {applied:?}, behind \
             the {settled_at} the request asked for"
        );

        // And the bindings, which are what the refusal was decided against.
        let (status, applied, body) = send(
            client
                .get(format!(
                    "{base}/api/v1/authorization?consistency=bounded&min_index={settled_at}"
                ))
                .bearer_auth(&alice),
        )
        .await;
        assert_eq!(status, 200, "member {i}: {body}");
        assert_eq!(
            body["bindings"],
            bindings_without_submitter(),
            "member {i} must hold exactly the post-revocation list — the \
             three replicas agree on the state the decision was made \
             against, which is what makes the decision deterministic: {body}"
        );
        assert!(
            applied.is_none_or(|a| a >= settled_at),
            "member {i} answered from {applied:?}, behind {settled_at}"
        );
    }

    // The gate is a staging device and nothing more: with the binding gone,
    // the same submission is refused by the pre-check too, before anything is
    // proposed. Same code, same message — which is the ADR 0023 property that
    // the two checks are one decision evaluated at two moments.
    let (status, _, body) = send(
        plain
            .post(format!("{gated_api}/api/v1/jobs"))
            .bearer_auth(&alice)
            .json(&submit_body(JobId::new(), entity)),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["code"], "PERMISSION_DENIED", "{body}");

    fleet.stop_all().await;
    idp.shutdown().await;
}

/// [`Fleet::await_voters`], but tolerating one member whose client listener
/// serves HTTPS: that helper polls `/readyz` over plain HTTP on every member,
/// which the operator edge here does not answer.
async fn await_voters(fleet: &Fleet, server_root_pem: &[u8], operator_base: &str, expected: usize) {
    let plain = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build the plain-http client");
    let https = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(server_root_pem).expect("root"))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build the https client");

    for i in 0..fleet.members.len() {
        let (client, url) = if i == OPERATOR_EDGE {
            (&https, format!("{operator_base}/readyz"))
        } else {
            (&plain, fleet.members[i].api("/readyz"))
        };
        poll(
            Duration::from_secs(90),
            &format!("member {i} reaches a {expected}-voter set"),
            || async {
                let Ok(response) = client.get(&url).send().await else {
                    return false;
                };
                let Ok(body) = response.json::<Value>().await else {
                    return false;
                };
                body["phase"] == "voter"
                    && body["voters"].as_array().map(|v| v.len()) == Some(expected)
            },
        )
        .await;
    }
}
