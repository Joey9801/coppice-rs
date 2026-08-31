//! End-to-end OIDC authentication and authorization against a real
//! coordinator and a real (fake) identity provider — issue #45's acceptance
//! criteria, driven over the daemon's own client listener.
//!
//! Nothing here stubs anything the system under test would otherwise do. Each
//! test boots a whole `Daemon` through `bootstrap::run_with` with an `[sso]`
//! section in its config file, points it at a
//! [`FakeIdp`](coppice_testkit::oidc::FakeIdp) serving real discovery and JWKS
//! documents over loopback HTTP, and drives the resulting cluster with real
//! ES256 bearer tokens and real operator certificates over real sockets. The
//! authorization decisions asserted below are therefore the ones the
//! middleware, the pre-log check and apply actually made — not a router
//! exercised against a `StubPlane`.
//!
//! The client listener runs HTTPS throughout (`[client_tls]` with a serving
//! leaf), because operator-certificate break-glass (ADR 0022) is only
//! presentable on a TLS listener and it is the *only* credential a freshly
//! formed cluster has: the replicated bindings list starts empty, so the first
//! `PUT /api/v1/authorization` cannot be authorized by any bearer token that
//! could exist. Formation seeds no bindings on purpose (see
//! `policy::tests::seeding_commands_carry_no_actor`), which makes the operator
//! certificate the bootstrap path rather than a fallback.
//!
//! The one criterion this file does *not* cover is the revocation race, which
//! needs a fleet — see `sso_fleet.rs`.

mod common;

use std::time::Duration;

use coppice_coordinator::localadmin::{AdminCall, AdminReply, OperatorPem};
use coppice_core::id::{ClusterId, JobId, QuotaEntityId};
use coppice_testkit::oidc::{FakeIdp, SigningKey, TokenClaims};
use serde_json::{json, Value};

use common::{poll, Ca, Daemon};

/// The `client_id` every fixture configures, and therefore (via the
/// "audience defaults to client_id" rule) the `aud` every accepted token must
/// carry.
const CLIENT_ID: &str = "coppice";

/// Group names used by the bindings the matrix test installs.
const SUBMITTER_GROUP: &str = "batch-users";
const ADMIN_GROUP: &str = "platform-admins";

/// The full role x scope x subject matrix
/// [`role_and_verb_bindings_are_enforced_for_groups_principals_and_scopes`]
/// installs: `{submitter, operator, admin} x {scoped, unscoped} x {group,
/// principal}`, twelve bindings, each with a name distinct from every other
/// cell's so that a token selects exactly one of them.
mod matrix_cells {
    /// Group names, one per group-subject cell.
    pub const SUBMITTER_SCOPED_GROUP: &str = "grp-submitter-scoped";
    pub const SUBMITTER_UNSCOPED_GROUP: &str = "grp-submitter-unscoped";
    pub const OPERATOR_SCOPED_GROUP: &str = "grp-operator-scoped";
    pub const OPERATOR_UNSCOPED_GROUP: &str = "grp-operator-unscoped";
    pub const ADMIN_SCOPED_GROUP: &str = "grp-admin-scoped";
    pub const ADMIN_UNSCOPED_GROUP: &str = "grp-admin-unscoped";

    /// Principal names, one per principal-subject cell — also the `sub` the
    /// matrix test mints each one's token for.
    pub const SUBMITTER_SCOPED_PRINCIPAL: &str = "sub-scoped";
    pub const SUBMITTER_UNSCOPED_PRINCIPAL: &str = "sub-unscoped";
    pub const OPERATOR_SCOPED_PRINCIPAL: &str = "op-scoped";
    pub const OPERATOR_UNSCOPED_PRINCIPAL: &str = "op-unscoped";
    pub const ADMIN_SCOPED_PRINCIPAL: &str = "admin-scoped-principal";
    pub const ADMIN_UNSCOPED_PRINCIPAL: &str = "admin-unscoped-principal";
}

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
// Fixture
// ---------------------------------------------------------------------------

/// A formed single-coordinator cluster in the OIDC posture, its fake IdP, and
/// the credentials needed to drive it.
struct Cluster {
    idp: FakeIdp,
    daemon: Daemon,
    /// `https://localhost:PORT` — the client listener's base URL.
    base: String,
    /// The operator credential `init` printed: the cluster's only day-0
    /// authority (ADR 0022 break-glass).
    operator: OperatorPem,
    /// Trusts the listener's serving root; presents no client certificate.
    /// The client every bearer-token request below goes through.
    anon: reqwest::Client,
    /// PEM of the root that signed the listener's SERVING certificate — the
    /// anchor a client verifies the *server* against. Distinct from the
    /// cluster CA in `operator`, which is the anchor the listener verifies
    /// *client* certificates against; nothing chains to both.
    server_root_pem: Vec<u8>,
    /// The `aud` this cluster requires, read back off `GET /auth/config`
    /// rather than assumed — the coordinator resolves the "defaults to
    /// client_id" rule itself, and a test that re-applied it would be
    /// asserting its own copy of the rule.
    audience: String,
}

impl Cluster {
    /// Boot a certless daemon with `[sso]` pointed at a fresh fake IdP, form
    /// it, and wait until bearer authentication actually works (the JWKS cache
    /// starts empty and is filled by its refresh task, so "the daemon is a
    /// voter" is not yet "a token is accepted").
    ///
    /// The formation bootstrap document seeds the priority-multiplier table a
    /// submission needs and nothing else. It never seeds bindings, because it
    /// cannot: `UpdateAuthorization` is not a formation-policy verb, which is
    /// exactly why the operator certificate exists.
    async fn start() -> Cluster {
        Cluster::start_with_sso(None).await
    }

    /// [`Cluster::start`] with the `[sso]` table supplied verbatim instead of
    /// generated — the documented-example test's entry point. The issuer in
    /// the supplied block must already point at `idp`, which is why the fake
    /// IdP is handed back rather than taken.
    async fn start_with_sso(sso_toml: Option<&dyn Fn(&FakeIdp) -> String>) -> Cluster {
        init_tracing();
        let ca = Ca::new();
        let idp = FakeIdp::start().await;

        let mut daemon = Daemon::new_certless(ClusterId::new(), &ca);
        // `ca` signs the listener's own SERVING certificate here. It is not
        // the anchor client certificates are judged against — that is the
        // cluster's own CA, minted at formation below.
        let (server_root_pem, base) = daemon.set_client_tls(&ca);
        match sso_toml {
            Some(build) => daemon.set_sso_block(&build(&idp)),
            None => daemon.set_sso(&idp.issuer(), CLIENT_ID),
        }
        daemon.start();

        let anon = reqwest::Client::builder()
            .add_root_certificate(
                reqwest::Certificate::from_pem(&server_root_pem).expect("serving root"),
            )
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("build the certless https client");

        await_phase(&anon, &base, "waiting").await;
        let reply = daemon
            .admin(AdminCall::Init {
                // A job with no configured multiplier for its priority is
                // INVALID_ARGUMENT, so every fixture seeds 1.0x for priority 0
                // — otherwise a submission the matrix expects to be *allowed*
                // would fail for a reason that has nothing to do with authz.
                policy: Some("[[priority_multiplier]]\nindex = 0\nmultiplier = 1.0\n".to_string()),
                operator_csr: None,
                operator_cn: Some("day0".to_string()),
            })
            .await;
        let AdminReply::Formed { operator, .. } = reply else {
            panic!("expected the cluster to form, got {reply:?}");
        };
        await_phase(&anon, &base, "voter").await;

        let (status, config) = send(anon.get(format!("{base}/api/v1/auth/config"))).await;
        assert_eq!(status, 200, "the pre-auth posture document: {config}");
        assert_eq!(config["mode"], "oidc", "{config}");
        let audience = config["audience"]
            .as_str()
            .unwrap_or_else(|| panic!("the OIDC posture publishes an audience: {config}"))
            .to_string();

        let cluster = Cluster {
            idp,
            daemon,
            base,
            operator,
            anon,
            server_root_pem,
            audience,
        };
        cluster.await_bearer_ready().await;
        cluster
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// A token for `sub` with the fixture's audience and the given groups.
    fn token(&self, sub: &str, groups: &[&str]) -> String {
        self.idp.sign(
            TokenClaims::new(sub)
                .audience(&self.audience)
                .claim("groups", json!(groups)),
        )
    }

    /// A `reqwest::Client` presenting the day-0 operator certificate. Every
    /// request through it authenticates as `cert:day0` with implicit unscoped
    /// admin, and carries no bearer token at all.
    fn operator_client(&self) -> reqwest::Client {
        let key = self
            .operator
            .key_pem
            .as_ref()
            .expect("no CSR was supplied, so the cluster minted the keypair");
        // reqwest's rustls identity takes one PEM blob: key then chain.
        let mut identity = key.clone().into_bytes();
        identity.extend_from_slice(self.operator.cert_pem.as_bytes());
        reqwest::Client::builder()
            .add_root_certificate(
                reqwest::Certificate::from_pem(&self.server_root_pem).expect("serving root"),
            )
            .identity(reqwest::Identity::from_pem(&identity).expect("operator identity"))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("build the operator-certificate https client")
    }

    /// Poll `/api/v1/session` with a freshly minted token until it is
    /// accepted: the JWKS cache is filled by a background task, so a bearer
    /// request issued the instant the daemon becomes a voter can legitimately
    /// 401.
    async fn await_bearer_ready(&self) {
        poll(
            Duration::from_secs(30),
            "the coordinator's JWKS cache accepts a freshly minted token",
            || async {
                let token = self.token("readiness-probe", &[]);
                let response = self
                    .anon
                    .get(self.url("/api/v1/session"))
                    .bearer_auth(&token)
                    .send()
                    .await;
                matches!(response, Ok(r) if r.status() == 200)
            },
        )
        .await;
    }

    /// Install `bindings` as the whole replicated bindings list over the
    /// operator certificate, then wait until a read sees them — the pre-log
    /// authorization check reads an *eventual* view, so "the PUT returned" is
    /// not yet "the next request is judged against the new list".
    async fn put_bindings(&self, client: &reqwest::Client, bindings: Value) -> Value {
        let count = bindings.as_array().expect("bindings is an array").len();
        let (status, body) = send(
            client
                .put(self.url("/api/v1/authorization"))
                .json(&json!({ "bindings": bindings })),
        )
        .await;
        assert_eq!(status, 200, "installing the bindings list: {body}");
        poll(
            Duration::from_secs(10),
            "the new bindings list is visible to reads",
            || async {
                let (_, body) = send(
                    self.anon
                        .get(self.url("/api/v1/authorization"))
                        .bearer_auth(self.token("probe", &[])),
                )
                .await;
                body["bindings"].as_array().map(|b| b.len()) == Some(count)
            },
        )
        .await;
        body
    }

    async fn shutdown(mut self) {
        let _ = self.daemon.stop().await;
        self.idp.shutdown().await;
    }
}

/// Send a request and split the response into (status, JSON body). A body
/// that is not JSON becomes `{"raw": "..."}` so an assertion message still
/// shows what came back.
async fn send(request: reqwest::RequestBuilder) -> (u16, Value) {
    let response = request.send().await.expect("the request reaches the API");
    let status = response.status().as_u16();
    let bytes = response.bytes().await.expect("read the response body");
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, body)
}

/// [`Daemon::await_phase`] over HTTPS: that helper always dials plain HTTP,
/// which these daemons do not serve.
async fn await_phase(client: &reqwest::Client, base: &str, phase: &str) {
    poll(
        Duration::from_secs(60),
        &format!("the daemon reaches the {phase} phase"),
        || async {
            match client.get(format!("{base}/readyz")).send().await {
                Ok(response) => match response.json::<Value>().await {
                    Ok(body) => body["phase"] == phase,
                    Err(_) => false,
                },
                Err(_) => false,
            }
        },
    )
    .await;
}

/// A submission body charging `entity`, with a fresh client-minted id.
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

/// A quota-entity upsert body.
fn entity_body(entity: QuotaEntityId, parent: Option<QuotaEntityId>, name: &str) -> Value {
    json!({
        "entity": entity.to_string(),
        "parent": parent.map(|p| p.to_string()),
        "name": name,
        "quota_ucu": 1_000_000_000_000u64,
    })
}

// ---------------------------------------------------------------------------
// 1. Reject unauthenticated and invalid credentials
// ---------------------------------------------------------------------------

/// Issue #45 acceptance criterion 1: on an OIDC-mode coordinator every
/// missing or invalid credential is a 401 on a **read** and on a **write**
/// alike, and a valid one is not.
///
/// Reads and writes are asserted together on purpose. Authentication is a
/// property of the `/api/v1` namespace, not of individual handlers
/// (`api_v1_routes` wraps the whole table, its 404 fallback included), and the
/// regression worth pinning is a route that reaches its handler uncredentialed
/// because it was registered somewhere the layer does not cover.
///
/// The valid-token control at the end is what makes the rest evidence: it
/// proves the 401s above are about the credential and not about the request,
/// the route, or the cluster's state. It expects 403, not 200 — the token is
/// good, the actor simply holds no bindings.
#[tokio::test]
async fn missing_expired_and_forged_tokens_are_refused_on_reads_and_writes() {
    let cluster = Cluster::start().await;
    let idp = &cluster.idp;
    let entity = QuotaEntityId::new();

    // Each case: a label, and the `Authorization` header value (or `None`).
    let cases: Vec<(&str, Option<String>)> = vec![
        ("no credential at all", None),
        (
            "a bearer value that is not a JWT",
            Some("Bearer not-a-jwt-at-all".to_string()),
        ),
        (
            "a well-formed JWT expired beyond the clock-skew allowance",
            Some(format!(
                "Bearer {}",
                idp.sign(
                    TokenClaims::new("alice")
                        .audience(CLIENT_ID)
                        // Twice `CLOCK_SKEW_LEEWAY_SECS`, so this is expired
                        // by the validator's own definition rather than by a
                        // margin the leeway might absorb.
                        .expires_in(-2 * coppice_authn::CLOCK_SKEW_LEEWAY_SECS as i64)
                )
            )),
        ),
        (
            "a token not yet valid, beyond the clock-skew allowance",
            Some(format!(
                "Bearer {}",
                idp.sign(
                    TokenClaims::new("alice")
                        .audience(CLIENT_ID)
                        .not_before_in(2 * coppice_authn::CLOCK_SKEW_LEEWAY_SECS as i64)
                )
            )),
        ),
        (
            "a token minted for a different audience",
            Some(format!(
                "Bearer {}",
                idp.sign(TokenClaims::new("alice").audience("some-other-service"))
            )),
        ),
        (
            "a token carrying somebody else's issuer",
            Some(format!(
                "Bearer {}",
                idp.sign(
                    TokenClaims::new("alice")
                        .audience(CLIENT_ID)
                        .issuer("https://issuer.invalid/oidc")
                )
            )),
        ),
        (
            "a token signed by a key the JWKS never published",
            Some(format!(
                "Bearer {}",
                idp.sign_with_key(
                    TokenClaims::new("alice").audience(CLIENT_ID),
                    SigningKey::UnpublishedEs256 {
                        kid: "no-such-key".to_string(),
                    },
                )
            )),
        ),
        (
            "an HS256 alg-confusion attempt against a published kid",
            Some(format!(
                "Bearer {}",
                idp.sign_with_key(
                    TokenClaims::new("alice").audience(CLIENT_ID),
                    SigningKey::Hs256 {
                        kid: idp.current_kid(),
                        secret: b"the public key, offered as an hmac secret".to_vec(),
                    },
                )
            )),
        ),
        (
            "an alg=none token with an empty signature",
            Some(format!(
                "Bearer {}",
                idp.sign_with_key(
                    TokenClaims::new("alice").audience(CLIENT_ID),
                    SigningKey::NoneAlg {
                        kid: Some(idp.current_kid()),
                    },
                )
            )),
        ),
    ];

    for (label, header) in &cases {
        for (surface, request) in [
            (
                "a read (GET /api/v1/overview)",
                cluster.anon.get(cluster.url("/api/v1/overview")),
            ),
            (
                "a write (POST /api/v1/jobs)",
                cluster
                    .anon
                    .post(cluster.url("/api/v1/jobs"))
                    .json(&submit_body(JobId::new(), entity)),
            ),
        ] {
            let request = match header {
                Some(value) => request.header(reqwest::header::AUTHORIZATION, value),
                None => request,
            };
            let (status, body) = send(request).await;
            assert_eq!(
                status, 401,
                "{surface} with {label} must be refused as unauthenticated: {body}"
            );
            assert_eq!(
                body["code"], "UNAUTHENTICATED",
                "{surface} with {label} must carry the ADR 0031 authentication code: {body}"
            );
        }
    }

    // The control. A perfectly good token, from the same issuer, for the same
    // audience, signed by the current published key — and the actor behind it
    // holds no bindings at all.
    let good = cluster.token("alice", &["nobody-in-particular"]);
    let (status, body) = send(
        cluster
            .anon
            .get(cluster.url("/api/v1/overview"))
            .bearer_auth(&good),
    )
    .await;
    assert_eq!(
        status, 200,
        "a valid token reads the cluster: every read is open to any \
         authenticated principal in v1: {body}"
    );

    let (status, body) = send(
        cluster
            .anon
            .post(cluster.url("/api/v1/jobs"))
            .bearer_auth(&good)
            .json(&submit_body(JobId::new(), entity)),
    )
    .await;
    assert_eq!(
        status, 403,
        "a valid token with no binding is refused for *authorization*, not \
         authentication — which is what makes the 401s above evidence about \
         the credential: {body}"
    );
    assert_eq!(body["code"], "PERMISSION_DENIED", "{body}");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// 2. The role x verb matrix
// ---------------------------------------------------------------------------

/// Issue #45 acceptance criterion 2: every HTTP-reachable mutating verb, for
/// every role, positive and negative, scoped and unscoped, through **both**
/// group-claim and principal bindings — against one booted coordinator.
///
/// The full `{submitter, operator, admin} x {scoped, unscoped} x {group,
/// principal}` matrix: twelve bindings ([`matrix_cells`]), installed in one
/// `PUT /api/v1/authorization`, then driven table-driven (`CASES` below) over
/// the real HTTP surface. A four-binding sample missed cells like an unscoped
/// *operator* or a principal-bound unscoped *admin* silently; a binding this
/// test does not install cannot regress unnoticed here.
///
/// One coordinator and one test function because booting a daemon costs
/// seconds and asserting a matrix costs milliseconds; every case carries an
/// assertion message naming its cell and verb, so a failure pinpoints the
/// regressed combination rather than "the loop".
///
/// The bootstrap is the interesting part. A freshly formed cluster has an
/// empty bindings list and formation seeds none, so no bearer token in
/// existence can authorize the first `PUT /api/v1/authorization`. The
/// operator certificate `init` printed is the only authority there is — which
/// is what ADR 0022 means by break-glass, and it is exercised here as the
/// ordinary bootstrap path rather than as an emergency.
///
/// Not covered here because they are not on the HTTP surface at all:
/// `UpdatePolicy`, `SetNodeSchedulable` (node drain/undrain) and
/// `BumpClusterVersion` are actor-carrying commands with no route in
/// `routes.rs`; `Verb::Drain` and `Verb::UpdatePolicy` are reachable only from
/// the internal planes. The sibling
/// `the_http_surface_exposes_no_membership_or_enrollment_admin_verbs` pins the
/// related scope decision.
#[tokio::test]
async fn role_and_verb_bindings_are_enforced_for_groups_principals_and_scopes() {
    use matrix_cells::*;

    let cluster = Cluster::start().await;
    let operator = cluster.operator_client();

    // ---- The quota-entity tree the scopes are expressed over --------------
    //
    // Built over the operator certificate: creating an entity at the root
    // takes unscoped admin, which nothing else in this cluster has yet.
    let root = QuotaEntityId::new();
    let team_a = QuotaEntityId::new();
    let team_b = QuotaEntityId::new();
    for (entity, parent, name) in [
        (root, None, "org"),
        (team_a, Some(root), "team-a"),
        (team_b, Some(root), "team-b"),
    ] {
        let (status, body) = send(
            operator
                .post(cluster.url("/api/v1/quota-entities"))
                .json(&entity_body(entity, parent, name)),
        )
        .await;
        assert_eq!(
            status, 200,
            "the operator certificate creates the {name} entity: {body}"
        );
    }

    // ---- The twelve bindings, installed in one full replacement -----------

    fn group_binding(group: &str, role: &str, scope: Option<QuotaEntityId>) -> Value {
        match scope {
            Some(s) => json!({ "group": group, "role": role, "scope": s.to_string() }),
            None => json!({ "group": group, "role": role }),
        }
    }
    fn principal_binding(principal: &str, role: &str, scope: Option<QuotaEntityId>) -> Value {
        match scope {
            Some(s) => json!({ "principal": principal, "role": role, "scope": s.to_string() }),
            None => json!({ "principal": principal, "role": role }),
        }
    }

    let bindings = json!([
        group_binding(SUBMITTER_SCOPED_GROUP, "submitter", Some(team_a)),
        principal_binding(SUBMITTER_SCOPED_PRINCIPAL, "submitter", Some(team_a)),
        group_binding(SUBMITTER_UNSCOPED_GROUP, "submitter", None),
        principal_binding(SUBMITTER_UNSCOPED_PRINCIPAL, "submitter", None),
        group_binding(OPERATOR_SCOPED_GROUP, "operator", Some(team_a)),
        principal_binding(OPERATOR_SCOPED_PRINCIPAL, "operator", Some(team_a)),
        group_binding(OPERATOR_UNSCOPED_GROUP, "operator", None),
        principal_binding(OPERATOR_UNSCOPED_PRINCIPAL, "operator", None),
        group_binding(ADMIN_SCOPED_GROUP, "admin", Some(team_a)),
        principal_binding(ADMIN_SCOPED_PRINCIPAL, "admin", Some(team_a)),
        group_binding(ADMIN_UNSCOPED_GROUP, "admin", None),
        principal_binding(ADMIN_UNSCOPED_PRINCIPAL, "admin", None),
    ]);
    cluster.put_bindings(&operator, bindings.clone()).await;

    // Group cells authenticate as a distinct `sub` carrying the cell's group;
    // principal cells authenticate as the principal name itself. Distinct
    // subs everywhere, so no token accidentally also matches another cell's
    // binding or a job's `submitted_by` ownership grant.
    let submitter_scoped_group =
        cluster.token("submitter-scoped-group-user", &[SUBMITTER_SCOPED_GROUP]);
    let submitter_scoped_principal = cluster.token(SUBMITTER_SCOPED_PRINCIPAL, &[]);
    let submitter_unscoped_group =
        cluster.token("submitter-unscoped-group-user", &[SUBMITTER_UNSCOPED_GROUP]);
    let submitter_unscoped_principal = cluster.token(SUBMITTER_UNSCOPED_PRINCIPAL, &[]);
    let operator_scoped_group =
        cluster.token("operator-scoped-group-user", &[OPERATOR_SCOPED_GROUP]);
    let operator_scoped_principal = cluster.token(OPERATOR_SCOPED_PRINCIPAL, &[]);
    let operator_unscoped_group =
        cluster.token("operator-unscoped-group-user", &[OPERATOR_UNSCOPED_GROUP]);
    let operator_unscoped_principal = cluster.token(OPERATOR_UNSCOPED_PRINCIPAL, &[]);
    let admin_scoped_group = cluster.token("admin-scoped-group-user", &[ADMIN_SCOPED_GROUP]);
    let admin_scoped_principal = cluster.token(ADMIN_SCOPED_PRINCIPAL, &[]);
    let admin_unscoped_group = cluster.token("admin-unscoped-group-user", &[ADMIN_UNSCOPED_GROUP]);
    let admin_unscoped_principal = cluster.token(ADMIN_UNSCOPED_PRINCIPAL, &[]);
    let mallory = cluster.token("mallory", &["a-group-nobody-bound"]);

    // ---- Victim jobs for the operator-verb cases ---------------------------
    //
    // Submitted through the operator certificate (bypasses authz entirely),
    // so none of them are owned by any actor under test — an abort that
    // succeeds below succeeds on the *operator* grant, never the ownership
    // fast-path.
    let mint_job = |entity: QuotaEntityId, name: &'static str| {
        let job = JobId::new();
        let request = operator
            .post(cluster.url("/api/v1/jobs"))
            .json(&submit_body(job, entity));
        async move {
            let (status, body) = send(request).await;
            assert_eq!(status, 200, "seeding victim job {name}: {body}");
            job
        }
    };
    let victim_scoped_group_a = mint_job(team_a, "scoped-group/team-a").await;
    let victim_scoped_principal_a = mint_job(team_a, "scoped-principal/team-a").await;
    // Reused for both scoped-operator negative attempts below: a failed abort
    // never consumes the job.
    let victim_scope_negative_b = mint_job(team_b, "scope-negative/team-b").await;
    let victim_unscoped_group_a = mint_job(team_a, "unscoped-group/team-a").await;
    let victim_unscoped_group_b = mint_job(team_b, "unscoped-group/team-b").await;
    let victim_unscoped_principal_a = mint_job(team_a, "unscoped-principal/team-a").await;
    let victim_unscoped_principal_b = mint_job(team_b, "unscoped-principal/team-b").await;
    // For the admin upward-composition case: admin includes abort-others, not
    // just the operator role's abort. Distinct from every operator-cell
    // victim above so a regression that only exercised the operator path
    // cannot accidentally satisfy this one.
    let victim_admin_scoped_abort_a = mint_job(team_a, "admin-scoped-abort/team-a").await;

    // A fixed id for the submit-then-abort-own pairing below: the submitter
    // has no operator or admin role at all, so its abort of this job can only
    // succeed on the `Job.submitted_by` implicit grant, never on a bound
    // verb.
    let submitter_own_job = JobId::new();

    // ---- The table -----------------------------------------------------

    enum Call {
        /// Submit to the given entity using the given job id — a fixed id
        /// rather than a fresh one per case, so a case further down the table
        /// can name the exact job an earlier `Submit` created (the abort-own
        /// pairing below).
        Submit(JobId, QuotaEntityId),
        Abort(JobId),
        Configure(QuotaEntityId),
        PutAuthorization,
    }

    struct Case {
        cell: &'static str,
        verb: &'static str,
        token: String,
        call: Call,
        expect: u16,
        why: &'static str,
    }

    let cases = vec![
        // -- submitter / scoped --------------------------------------------
        Case {
            cell: "submitter/scoped/group",
            verb: "submit in scope",
            token: submitter_scoped_group.clone(),
            call: Call::Submit(JobId::new(), team_a),
            expect: 200,
            why: "a scoped group-bound submitter submits inside its own subtree",
        },
        Case {
            cell: "submitter/scoped/group",
            verb: "submit outside scope",
            token: submitter_scoped_group.clone(),
            call: Call::Submit(JobId::new(), team_b),
            expect: 403,
            why: "the scope is a subtree, not a hint",
        },
        Case {
            cell: "submitter/scoped/principal",
            verb: "submit in scope",
            token: submitter_scoped_principal.clone(),
            call: Call::Submit(JobId::new(), team_a),
            expect: 200,
            why: "a scoped principal-bound submitter submits inside its own subtree",
        },
        Case {
            cell: "submitter/scoped/principal",
            verb: "submit outside scope",
            token: submitter_scoped_principal.clone(),
            call: Call::Submit(JobId::new(), team_b),
            expect: 403,
            why: "the scope is a subtree, not a hint",
        },
        // -- submitter / scoped / abort-own vs abort-other -------------------
        //
        // The submitter role grants no abort at all — not even of its own
        // job, by role. What makes the own-job abort below succeed is a
        // different mechanism entirely: `Job.submitted_by`. Pairing the two
        // against the *same* actor is what makes that discrimination
        // explicit rather than merely implied by two unrelated cases.
        Case {
            cell: "submitter/scoped/group",
            verb: "submit its own job (seeds the abort-own case below)",
            token: submitter_scoped_group.clone(),
            call: Call::Submit(submitter_own_job, team_a),
            expect: 200,
            why: "the submitted_by grant needs a job this actor actually submitted",
        },
        Case {
            cell: "submitter/scoped/group",
            verb: "abort someone else's job in scope",
            token: submitter_scoped_group.clone(),
            call: Call::Abort(victim_scoped_group_a),
            expect: 403,
            why: "the submitter role does not grant abort, even of a job in its \
                  own scope and even in scope — a failed abort does not consume \
                  the job, so it is still available for the operator case below",
        },
        Case {
            cell: "submitter/scoped/group",
            verb: "abort the job it submitted itself",
            token: submitter_scoped_group.clone(),
            call: Call::Abort(submitter_own_job),
            expect: 200,
            why: "submitted_by implicit grant — a principal may always abort a \
                  job it submitted, even holding no operator role at all",
        },
        // -- submitter / unscoped -------------------------------------------
        Case {
            cell: "submitter/unscoped/group",
            verb: "submit anywhere (team-a)",
            token: submitter_unscoped_group.clone(),
            call: Call::Submit(JobId::new(), team_a),
            expect: 200,
            why: "an unscoped group-bound submitter reaches every subtree",
        },
        Case {
            cell: "submitter/unscoped/group",
            verb: "submit anywhere (team-b)",
            token: submitter_unscoped_group.clone(),
            call: Call::Submit(JobId::new(), team_b),
            expect: 200,
            why: "an unscoped group-bound submitter reaches every subtree",
        },
        Case {
            cell: "submitter/unscoped/group",
            verb: "abort someone else's job (role discrimination)",
            token: submitter_unscoped_group.clone(),
            call: Call::Abort(victim_unscoped_group_a),
            expect: 403,
            why: "unscoped reach is not a higher role — submit does not grant abort",
        },
        Case {
            cell: "submitter/unscoped/principal",
            verb: "submit anywhere (team-a)",
            token: submitter_unscoped_principal.clone(),
            call: Call::Submit(JobId::new(), team_a),
            expect: 200,
            why: "an unscoped principal-bound submitter reaches every subtree",
        },
        Case {
            cell: "submitter/unscoped/principal",
            verb: "submit anywhere (team-b)",
            token: submitter_unscoped_principal.clone(),
            call: Call::Submit(JobId::new(), team_b),
            expect: 200,
            why: "an unscoped principal-bound submitter reaches every subtree",
        },
        Case {
            cell: "submitter/unscoped/principal",
            verb: "configure a quota entity (role discrimination)",
            token: submitter_unscoped_principal.clone(),
            call: Call::Configure(team_a),
            expect: 403,
            why: "unscoped reach is not a higher role — submit does not grant admin",
        },
        // -- operator / scoped ------------------------------------------------
        Case {
            cell: "operator/scoped/group",
            verb: "abort someone else's job in scope",
            token: operator_scoped_group.clone(),
            call: Call::Abort(victim_scoped_group_a),
            expect: 200,
            why: "a scoped group-bound operator aborts anyone's job in its subtree",
        },
        Case {
            cell: "operator/scoped/group",
            verb: "abort a job outside scope",
            token: operator_scoped_group.clone(),
            call: Call::Abort(victim_scope_negative_b),
            expect: 403,
            why: "an operator scoped to team-a may not abort a job charging team-b",
        },
        Case {
            cell: "operator/scoped/principal",
            verb: "abort someone else's job in scope",
            token: operator_scoped_principal.clone(),
            call: Call::Abort(victim_scoped_principal_a),
            expect: 200,
            why: "a scoped principal-bound operator aborts anyone's job in its subtree",
        },
        Case {
            cell: "operator/scoped/principal",
            verb: "abort a job outside scope",
            token: operator_scoped_principal.clone(),
            call: Call::Abort(victim_scope_negative_b),
            expect: 403,
            why: "an operator scoped to team-a may not abort a job charging team-b",
        },
        // ADR 0023: verbs compose upward — operator includes submit. A
        // matrix that only ever exercised operator through abort could not
        // catch a regression that dropped that composition.
        Case {
            cell: "operator/scoped/group",
            verb: "submit in scope (upward composition)",
            token: operator_scoped_group.clone(),
            call: Call::Submit(JobId::new(), team_a),
            expect: 200,
            why: "operator includes submit by upward composition",
        },
        Case {
            cell: "operator/scoped/group",
            verb: "submit outside scope (upward composition)",
            token: operator_scoped_group.clone(),
            call: Call::Submit(JobId::new(), team_b),
            expect: 403,
            why: "operator includes submit by upward composition, but \
                  composition does not escape scope",
        },
        // -- operator / unscoped ----------------------------------------------
        Case {
            cell: "operator/unscoped/group",
            verb: "abort anywhere (team-a)",
            token: operator_unscoped_group.clone(),
            call: Call::Abort(victim_unscoped_group_a),
            expect: 200,
            why: "an unscoped group-bound operator reaches every subtree",
        },
        Case {
            cell: "operator/unscoped/group",
            verb: "abort anywhere (team-b)",
            token: operator_unscoped_group.clone(),
            call: Call::Abort(victim_unscoped_group_b),
            expect: 200,
            why: "an unscoped group-bound operator reaches every subtree",
        },
        Case {
            cell: "operator/unscoped/group",
            verb: "configure a quota entity (role discrimination)",
            token: operator_unscoped_group.clone(),
            call: Call::Configure(team_a),
            expect: 403,
            why: "unscoped reach is not a higher role — abort does not grant admin",
        },
        Case {
            cell: "operator/unscoped/principal",
            verb: "abort anywhere (team-a)",
            token: operator_unscoped_principal.clone(),
            call: Call::Abort(victim_unscoped_principal_a),
            expect: 200,
            why: "an unscoped principal-bound operator reaches every subtree",
        },
        Case {
            cell: "operator/unscoped/principal",
            verb: "abort anywhere (team-b)",
            token: operator_unscoped_principal.clone(),
            call: Call::Abort(victim_unscoped_principal_b),
            expect: 200,
            why: "an unscoped principal-bound operator reaches every subtree",
        },
        Case {
            cell: "operator/unscoped/principal",
            verb: "configure a quota entity (role discrimination)",
            token: operator_unscoped_principal.clone(),
            call: Call::Configure(team_a),
            expect: 403,
            why: "unscoped reach is not a higher role — abort does not grant admin",
        },
        Case {
            cell: "operator/unscoped/group",
            verb: "submit anywhere (upward composition)",
            token: operator_unscoped_group.clone(),
            call: Call::Submit(JobId::new(), team_b),
            expect: 200,
            why: "operator includes submit by upward composition, and that \
                  composition is not scope-dependent — an unscoped binding \
                  reaches every subtree exactly as it does for abort",
        },
        // -- admin / scoped -----------------------------------------------------
        Case {
            cell: "admin/scoped/group",
            verb: "configure a quota entity in scope",
            token: admin_scoped_group.clone(),
            call: Call::Configure(team_a),
            expect: 200,
            why: "a scoped group-bound admin configures entities inside its subtree",
        },
        Case {
            cell: "admin/scoped/group",
            verb: "configure a quota entity outside scope",
            token: admin_scoped_group.clone(),
            call: Call::Configure(team_b),
            expect: 403,
            why: "…and not outside it",
        },
        Case {
            cell: "admin/scoped/principal",
            verb: "configure a quota entity in scope",
            token: admin_scoped_principal.clone(),
            call: Call::Configure(team_a),
            expect: 200,
            why: "a scoped principal-bound admin configures entities inside its subtree",
        },
        Case {
            cell: "admin/scoped/principal",
            verb: "replace the bindings list (authorization-write)",
            token: admin_scoped_principal.clone(),
            call: Call::PutAuthorization,
            expect: 403,
            why: "authorization-write is a cluster verb: it takes an UNSCOPED \
                  admin binding specifically, and a scoped admin is not a \
                  smaller version of it",
        },
        // ADR 0023: verbs compose upward — admin includes both submit and
        // abort-others, not merely the entity-configuration verb the cells
        // above exercise.
        Case {
            cell: "admin/scoped/group",
            verb: "submit in scope (upward composition)",
            token: admin_scoped_group.clone(),
            call: Call::Submit(JobId::new(), team_a),
            expect: 200,
            why: "admin includes submit by upward composition",
        },
        Case {
            cell: "admin/scoped/group",
            verb: "abort someone else's job in scope (upward composition)",
            token: admin_scoped_group.clone(),
            call: Call::Abort(victim_admin_scoped_abort_a),
            expect: 200,
            why: "admin includes abort-others by upward composition, the same \
                  grant the operator role carries",
        },
        // -- admin / unscoped -----------------------------------------------------
        Case {
            cell: "admin/unscoped/group",
            verb: "configure a quota entity anywhere (team-a)",
            token: admin_unscoped_group.clone(),
            call: Call::Configure(team_a),
            expect: 200,
            why: "an unscoped group-bound admin reaches every subtree",
        },
        Case {
            cell: "admin/unscoped/group",
            verb: "configure a quota entity anywhere (team-b)",
            token: admin_unscoped_group.clone(),
            call: Call::Configure(team_b),
            expect: 200,
            why: "an unscoped group-bound admin reaches every subtree",
        },
        Case {
            cell: "admin/unscoped/group",
            verb: "replace the bindings list (authorization-write)",
            token: admin_unscoped_group.clone(),
            call: Call::PutAuthorization,
            expect: 200,
            why: "an unscoped GROUP-bound admin is exactly what authorization-write requires",
        },
        Case {
            cell: "admin/unscoped/principal",
            verb: "configure a quota entity anywhere (team-a)",
            token: admin_unscoped_principal.clone(),
            call: Call::Configure(team_a),
            expect: 200,
            why: "an unscoped principal-bound admin reaches every subtree",
        },
        Case {
            cell: "admin/unscoped/principal",
            verb: "configure a quota entity anywhere (team-b)",
            token: admin_unscoped_principal.clone(),
            call: Call::Configure(team_b),
            expect: 200,
            why: "an unscoped principal-bound admin reaches every subtree",
        },
        Case {
            cell: "admin/unscoped/principal",
            verb: "replace the bindings list (authorization-write)",
            token: admin_unscoped_principal.clone(),
            call: Call::PutAuthorization,
            expect: 200,
            why: "an unscoped PRINCIPAL-bound admin is exactly what authorization-write \
                  requires — the missing cell a four-binding sample could not catch",
        },
        Case {
            cell: "admin/unscoped/group",
            verb: "submit anywhere (upward composition)",
            token: admin_unscoped_group.clone(),
            call: Call::Submit(JobId::new(), team_a),
            expect: 200,
            why: "admin includes submit by upward composition, and that \
                  composition is not scope-dependent",
        },
        // -- the "no binding at all" control ---------------------------------
        Case {
            cell: "unbound",
            verb: "submit",
            token: mallory.clone(),
            call: Call::Submit(JobId::new(), team_a),
            expect: 403,
            why: "no binding, no submission",
        },
    ];

    for case in &cases {
        let (status, body) = match &case.call {
            Call::Submit(job, entity) => {
                send(
                    cluster
                        .anon
                        .post(cluster.url("/api/v1/jobs"))
                        .bearer_auth(&case.token)
                        .json(&submit_body(*job, *entity)),
                )
                .await
            }
            Call::Abort(job) => {
                send(
                    cluster
                        .anon
                        .post(cluster.url(&format!("/api/v1/jobs/{job}/abort")))
                        .bearer_auth(&case.token)
                        .json(&json!({})),
                )
                .await
            }
            Call::Configure(parent) => {
                send(
                    cluster
                        .anon
                        .post(cluster.url("/api/v1/quota-entities"))
                        .bearer_auth(&case.token)
                        .json(&entity_body(QuotaEntityId::new(), Some(*parent), case.cell)),
                )
                .await
            }
            Call::PutAuthorization => {
                send(
                    cluster
                        .anon
                        .put(cluster.url("/api/v1/authorization"))
                        .bearer_auth(&case.token)
                        .json(&json!({ "bindings": bindings })),
                )
                .await
            }
        };
        assert_eq!(
            status, case.expect,
            "[{}] {}: expected {}, got {status} — {}: {body}",
            case.cell, case.verb, case.expect, case.why
        );
    }

    cluster.shutdown().await;
}

/// Issue #45 acceptance criterion 2, the break-glass half: an operator
/// certificate exercises an admin verb end to end, over HTTPS, carrying no
/// bearer token at all.
///
/// Asserted on its own rather than only implied by the bootstrap in the
/// matrix test above, because the guarantee has three parts and only the
/// middle one is load-bearing there: the certificate authenticates as
/// `cert:<CN>`, it carries implicit unscoped admin that no bindings list
/// contains or can revoke, and the command it proposes is an ordinary
/// actor-carrying command that lands in the log like any other.
#[tokio::test]
async fn an_operator_certificate_is_break_glass_for_an_admin_verb() {
    let cluster = Cluster::start().await;
    let operator = cluster.operator_client();

    let (status, body) = send(operator.get(cluster.url("/api/v1/session"))).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["auth_method"], "operator_cert",
        "the chain authenticated the certificate, not a token: {body}"
    );
    assert_eq!(
        body["principal"], "cert:day0",
        "an operator certificate authenticates as cert:<CN>: {body}"
    );
    assert_eq!(
        body["implicit_admin"], true,
        "…with unscoped admin from outside the bindings list: {body}"
    );
    assert_eq!(
        body["bindings"],
        json!([]),
        "and that authority is NOT in the list, which is why it cannot be \
         revoked through it: {body}"
    );

    // The admin verb, end to end: a list that binds nobody but the day-0
    // certificate's own kind of authority — no principal in it is this
    // caller — still applies, because the caller's authority never came from
    // the list.
    let entity = QuotaEntityId::new();
    let (status, body) = send(
        operator
            .post(cluster.url("/api/v1/quota-entities"))
            .json(&entity_body(entity, None, "break-glass")),
    )
    .await;
    assert_eq!(
        status, 200,
        "creating a root quota entity takes unscoped admin: {body}"
    );

    let response = cluster
        .put_bindings(
            &operator,
            json!([{ "group": ADMIN_GROUP, "role": "admin" }]),
        )
        .await;
    assert!(
        response["log_index"].as_u64().is_some(),
        "the write is an ordinary command with an ordinary log position, not \
         a side channel: {response}"
    );

    // And it is still break-glass afterwards: the list it just installed
    // names a group this caller does not have and never will.
    let (status, body) = send(
        operator
            .post(cluster.url("/api/v1/quota-entities"))
            .json(&entity_body(QuotaEntityId::new(), Some(entity), "still-in")),
    )
    .await;
    assert_eq!(
        status, 200,
        "the operator certificate's authority survives a bindings list that \
         does not mention it: {body}"
    );

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// 4. IdP outage
// ---------------------------------------------------------------------------

/// Issue #45 acceptance criterion 4: when the IdP goes dark, tokens already
/// verifiable from the cached JWKS keep working, tokens that need something
/// the cache does not have do not, the operator certificate works throughout,
/// and recovery is prompt once the IdP is back.
///
/// This is ADR 0022's outage posture in full: offline validation means an
/// unreachable issuer costs availability only for the things that genuinely
/// need it — a key the cache never saw — and nothing else.
#[tokio::test]
async fn cached_jwks_serves_through_an_idp_outage_and_recovers_afterwards() {
    let cluster = Cluster::start().await;
    let operator = cluster.operator_client();

    // A long-lived token minted (and the key behind it cached) while the IdP
    // is up. Ten minutes so that nothing below is racing its expiry.
    let long_lived = cluster.idp.sign(
        TokenClaims::new("alice")
            .audience(CLIENT_ID)
            .expires_in(600),
    );
    let (status, body) = send(
        cluster
            .anon
            .get(cluster.url("/api/v1/session"))
            .bearer_auth(&long_lived),
    )
    .await;
    assert_eq!(status, 200, "the token works before the outage: {body}");

    // ---- Dark ------------------------------------------------------------
    cluster.idp.go_dark();

    let (status, body) = send(
        cluster
            .anon
            .get(cluster.url("/api/v1/overview"))
            .bearer_auth(&long_lived),
    )
    .await;
    assert_eq!(
        status, 200,
        "an already-issued token keeps working with the IdP unreachable: \
         validation is offline against the cached JWKS, and there is no call \
         on the request path to fail: {body}"
    );

    let expired = cluster.idp.sign(
        TokenClaims::new("alice")
            .audience(CLIENT_ID)
            .expires_in(-2 * coppice_authn::CLOCK_SKEW_LEEWAY_SECS as i64),
    );
    let (status, body) = send(
        cluster
            .anon
            .get(cluster.url("/api/v1/overview"))
            .bearer_auth(&expired),
    )
    .await;
    assert_eq!(
        status, 401,
        "serving stale keys is not serving stale *claims*: an expired token \
         is refused during the outage exactly as it would be outside one: {body}"
    );

    // A key minted while the IdP is dark: the cache cannot learn it, and the
    // fetch-on-unknown-kid path has nowhere to fetch from.
    let new_kid = cluster.idp.rotate_key();
    let rotated = cluster.idp.sign(
        TokenClaims::new("alice")
            .audience(CLIENT_ID)
            .expires_in(600),
    );
    let (status, body) = send(
        cluster
            .anon
            .get(cluster.url("/api/v1/overview"))
            .bearer_auth(&rotated),
    )
    .await;
    assert_eq!(
        status, 401,
        "a token signed by a key the cache never saw is refused while the \
         IdP is unreachable (kid {new_kid}): serving stale is not trusting \
         unknown: {body}"
    );

    // Break-glass, which is the entire point of having it.
    let (status, body) = send(operator.get(cluster.url("/api/v1/session"))).await;
    assert_eq!(
        status, 200,
        "the operator certificate authenticates against the cluster CA and \
         never touches the IdP, so an outage cannot lock an operator out: {body}"
    );
    assert_eq!(body["auth_method"], "operator_cert", "{body}");

    // ---- Recovery --------------------------------------------------------
    cluster.idp.resume();

    // Polled, not slept: the cache's scheduled refresh is ten minutes away,
    // so recovery here rides fetch-on-unknown-kid — which is rate limited
    // (`JwksTimings::on_demand_min_interval`, 10s in production) and whose
    // budget the failed attempt above has just spent. The poll waits out that
    // limiter and no longer; a fixed sleep would have to be the limiter's
    // worst case plus a margin, and would still be a guess.
    poll(
        Duration::from_secs(45),
        "the rotated key becomes verifiable once the IdP answers again",
        || async {
            let (status, _) = send(
                cluster
                    .anon
                    .get(cluster.url("/api/v1/overview"))
                    .bearer_auth(&rotated),
            )
            .await;
            status == 200
        },
    )
    .await;

    let (status, body) = send(
        cluster
            .anon
            .get(cluster.url("/api/v1/overview"))
            .bearer_auth(&long_lived),
    )
    .await;
    assert_eq!(
        status, 200,
        "and the pre-outage key is still published, so the token minted \
         before the outage did not stop working at recovery: {body}"
    );

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// 5. Lockout
// ---------------------------------------------------------------------------

/// Issue #45 acceptance criterion 5: a bindings list that would leave the
/// cluster with no unscoped admin is refused, with the error that says so.
///
/// The distinguishing text matters as much as the refusal. `UpdateAuthorization`
/// has three read-only apply checks that all arrive as the same 400, and an
/// operator who cannot tell "you scoped a role to an entity that does not
/// exist" from "this would lock you out" has to guess which of their edits was
/// the problem.
#[tokio::test]
async fn replacing_the_last_unscoped_admin_binding_is_refused_as_a_lockout() {
    let cluster = Cluster::start().await;
    let operator = cluster.operator_client();

    let entity = QuotaEntityId::new();
    let (status, body) = send(
        operator
            .post(cluster.url("/api/v1/quota-entities"))
            .json(&entity_body(entity, None, "org")),
    )
    .await;
    assert_eq!(status, 200, "{body}");

    // A list with authority in it, but none of it unscoped admin.
    let (status, body) = send(
        operator
            .put(cluster.url("/api/v1/authorization"))
            .json(&json!({
                "bindings": [
                    { "group": SUBMITTER_GROUP, "role": "submitter", "scope": entity.to_string() },
                    { "group": ADMIN_GROUP, "role": "admin", "scope": entity.to_string() },
                ]
            })),
    )
    .await;
    assert_eq!(
        status, 400,
        "a list retaining no unscoped admin is a document the operator got \
         wrong, not a race they lost: {body}"
    );
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("lock the cluster out of its own authorization"),
        "the refusal must be distinguishable from the other two \
         UpdateAuthorization refusals: {body}"
    );

    // The empty list is the same refusal, reached the obvious way.
    let (status, body) = send(
        operator
            .put(cluster.url("/api/v1/authorization"))
            .json(&json!({ "bindings": [] })),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("lock the cluster out of its own authorization"),
        "{body}"
    );

    // The neighbouring refusal, for contrast: a scope naming an entity that
    // does not exist is a *different* 400 with different text, so the two
    // cannot be confused for one another.
    let (status, body) = send(
        operator
            .put(cluster.url("/api/v1/authorization"))
            .json(&json!({
                "bindings": [
                    { "group": ADMIN_GROUP, "role": "admin" },
                    {
                        "group": SUBMITTER_GROUP,
                        "role": "submitter",
                        "scope": QuotaEntityId::new().to_string(),
                    },
                ]
            })),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("quota entity that does not exist"),
        "{body}"
    );

    // And the list that keeps an unscoped admin applies, so the refusals
    // above are about what they claim to be about.
    let response = cluster
        .put_bindings(
            &operator,
            json!([
                { "group": ADMIN_GROUP, "role": "admin" },
                { "group": SUBMITTER_GROUP, "role": "submitter", "scope": entity.to_string() },
            ]),
        )
        .await;
    assert!(response["log_index"].as_u64().is_some(), "{response}");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// 6. Open mode
// ---------------------------------------------------------------------------

/// Issue #45 acceptance criterion 6: an `[auth] insecure_open = true`
/// deployment serves every request as an anonymous actor with full admin
/// authority — reads, writes and all. This is `coppice dev`'s posture, and
/// the smoke test that keeps it working.
///
/// Plain HTTP and no credentials anywhere, because that is the entire point:
/// a caller that can reach the listener can do anything.
#[tokio::test]
async fn open_mode_serves_every_request_as_an_anonymous_admin() {
    init_tracing();
    let ca = Ca::new();
    // The fixture template's own `[auth] insecure_open = true`, left alone.
    let mut daemon = Daemon::new_certless(ClusterId::new(), &ca);
    daemon.start();
    daemon.await_phase("waiting").await;
    let reply = daemon
        .admin(AdminCall::Init {
            policy: Some("[[priority_multiplier]]\nindex = 0\nmultiplier = 1.0\n".to_string()),
            operator_csr: None,
            operator_cn: None,
        })
        .await;
    assert!(
        matches!(reply, AdminReply::Formed { .. }),
        "expected the cluster to form, got {reply:?}"
    );
    daemon.await_phase("voter").await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build the plain-http client");

    // The pre-auth posture document tells a UI there is nothing to log in to.
    let (status, body) = send(client.get(daemon.api("/api/v1/auth/config"))).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["mode"], "open", "{body}");
    assert_eq!(
        body.get("issuer").and_then(Value::as_str),
        None,
        "there is no issuer to advertise in open mode: {body}"
    );

    // The identity every request resolves to.
    let (status, body) = send(client.get(daemon.api("/api/v1/session"))).await;
    assert_eq!(status, 200, "an uncredentialed read is served: {body}");
    assert_eq!(body["principal"], "anonymous", "{body}");
    assert_eq!(body["auth_method"], "open", "{body}");
    assert_eq!(
        body["implicit_admin"], true,
        "open mode's authority is the actor's own flag, never a binding — \
         which is what keeps apply a pure function of state and command: {body}"
    );

    // A write chain no bindings list authorizes, because there is no bindings
    // list: create a root quota entity (unscoped admin), submit against it
    // (submitter), abort the result (operator), and replace the bindings list
    // (unscoped admin) — one verb from each rung of the ladder.
    let entity = QuotaEntityId::new();
    let (status, body) = send(
        client
            .post(daemon.api("/api/v1/quota-entities"))
            .json(&entity_body(entity, None, "dev")),
    )
    .await;
    assert_eq!(status, 200, "anonymous creates a root quota entity: {body}");

    let job = JobId::new();
    let (status, body) = send(
        client
            .post(daemon.api("/api/v1/jobs"))
            .json(&submit_body(job, entity)),
    )
    .await;
    assert_eq!(status, 200, "anonymous submits a job: {body}");

    let (status, body) = send(
        client
            .post(daemon.api(&format!("/api/v1/jobs/{job}/abort")))
            .json(&json!({})),
    )
    .await;
    assert_eq!(
        status, 200,
        "anonymous aborts a job it did not (identifiably) submit: {body}"
    );

    let (status, body) = send(
        client
            .put(daemon.api("/api/v1/authorization"))
            .json(&json!({ "bindings": [{ "group": ADMIN_GROUP, "role": "admin" }] })),
    )
    .await;
    assert_eq!(
        status, 200,
        "anonymous replaces the bindings list, the cluster verb: {body}"
    );

    // Installing a bindings list changes nothing: the posture is node config,
    // and the actor carries it.
    let (status, body) = send(client.post(daemon.api("/api/v1/quota-entities")).json(
        &entity_body(QuotaEntityId::new(), Some(entity), "still-open"),
    ))
    .await;
    assert_eq!(
        status, 200,
        "a bindings list that names nobody does not close an open \
         deployment: {body}"
    );

    daemon.stop().await.expect("daemon stops cleanly");
}

// ---------------------------------------------------------------------------
// 7. The documented example config
// ---------------------------------------------------------------------------

/// Issue #45 acceptance criterion 7: the `[sso]` block published in
/// `docs/operations/configuration.md` is extracted **at test runtime**,
/// booted, and used to serve an authenticated submission end to end.
///
/// Extracted rather than copied, so the test cannot pass against a
/// documentation example that no longer parses. Only the issuer is
/// substituted — a documented `https://sso.example.com/oidc` cannot be reached
/// from a test — and the substitution is mechanical, so a renamed key, a
/// dropped required field or a stray `deny_unknown_fields` violation breaks
/// this test rather than sitting in the docs waiting for an operator to find
/// it.
///
/// The audience is read back off `GET /api/v1/auth/config` rather than copied
/// out of the block, which makes this a check on the documented `audience`
/// line too: whatever the docs say, the token minted below has to satisfy the
/// running coordinator.
#[tokio::test]
async fn the_documented_sso_example_boots_and_serves_an_authenticated_submit() {
    let documented = documented_sso_block();
    assert!(
        documented.contains("client_id"),
        "the documented [sso] block must still declare a client_id: {documented}"
    );
    assert!(
        documented.contains(DOCUMENTED_ISSUER),
        "the documented issuer must still be the placeholder this test \
         rewrites: {documented}"
    );

    let cluster = Cluster::start_with_sso(Some(&|idp: &FakeIdp| {
        documented_sso_block().replace(DOCUMENTED_ISSUER, &idp.issuer())
    }))
    .await;
    let operator = cluster.operator_client();

    // The documented `audience` line, as the running coordinator resolved it.
    assert_eq!(
        cluster.audience, "coppice",
        "the documented audience is what the coordinator enforces"
    );

    let entity = QuotaEntityId::new();
    let (status, body) = send(
        operator
            .post(cluster.url("/api/v1/quota-entities"))
            .json(&entity_body(entity, None, "org")),
    )
    .await;
    assert_eq!(status, 200, "{body}");

    cluster
        .put_bindings(
            &operator,
            json!([
                { "group": ADMIN_GROUP, "role": "admin" },
                { "group": SUBMITTER_GROUP, "role": "submitter", "scope": entity.to_string() },
            ]),
        )
        .await;

    // The authenticated submit: a real ES256 token, minted for the audience
    // the documented config asks for, carrying the group the bindings list
    // names, through a coordinator whose entire auth posture came out of the
    // documentation.
    let alice = cluster.token("alice", &[SUBMITTER_GROUP]);
    let job = JobId::new();
    let (status, body) = send(
        cluster
            .anon
            .post(cluster.url("/api/v1/jobs"))
            .bearer_auth(&alice)
            .json(&submit_body(job, entity)),
    )
    .await;
    assert_eq!(
        status, 200,
        "the documented example config validates a real token and authorizes \
         a real submission: {body}"
    );
    assert_eq!(body["job"], job.to_string(), "{body}");

    // And it is really enforcing, not merely accepting: the same token is
    // refused the admin verb.
    let (status, body) = send(
        cluster
            .anon
            .post(cluster.url("/api/v1/quota-entities"))
            .bearer_auth(&alice)
            .json(&entity_body(QuotaEntityId::new(), Some(entity), "nope")),
    )
    .await;
    assert_eq!(
        status, 403,
        "a submitter token cannot configure quota entities: {body}"
    );

    cluster.shutdown().await;
}

/// The issuer URL the documented example carries — the one placeholder
/// [`the_documented_sso_example_boots_and_serves_an_authenticated_submit`]
/// rewrites, and the only thing about that block this test knows.
const DOCUMENTED_ISSUER: &str = "https://sso.example.com/oidc";

/// The `[sso]` table exactly as `docs/operations/configuration.md` publishes
/// it: from the `[sso]` header to the start of whatever table follows it —
/// which today is the commented-out `[auth]` alternative.
fn documented_sso_block() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/operations/configuration.md");
    let doc =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

    let header = "\n[sso]\n";
    let at = doc
        .find(header)
        .unwrap_or_else(|| panic!("no [sso] section in {}", path.display()));
    let rest = &doc[at + header.len()..];
    // The section runs until the next line that begins a table — a real
    // header, or the commented-out alternative that stands in for one.
    let end = [rest.find("\n# ["), rest.find("\n["), rest.find("\n```")]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or_else(|| panic!("the [sso] section in {} never ends", path.display()));
    format!("[sso]\n{}\n", rest[..end].trim_end())
}

// ---------------------------------------------------------------------------
// 8. Scope guard
// ---------------------------------------------------------------------------

/// Issue #45 acceptance criterion 8, and the 2026-08-29 scope decision it
/// records: membership and coordinator-enrollment administration stay on the
/// mTLS-authenticated internal planes, so the public HTTP surface must expose
/// none of those verbs — and `POST /api/v1/enroll` must keep working with an
/// enrollment token and no OIDC credential, on an OIDC-mode cluster.
///
/// The 404s are asserted **with a valid admin credential** on purpose. An
/// unauthenticated 404 would prove nothing: the authentication layer wraps the
/// whole `/api/v1` namespace including its own fallback, so every path under
/// it answers 401 to an anonymous caller whether it is routed or not. Asking
/// as an operator certificate — the strongest credential this system has —
/// is the only way to distinguish "not authorized" from "not there".
#[tokio::test]
async fn the_http_surface_exposes_no_membership_or_enrollment_admin_verbs() {
    let cluster = Cluster::start().await;
    let operator = cluster.operator_client();

    // Every membership/PKI verb the internal admin plane carries (ADR 0037
    // §7), written as the route a public API would have given it.
    let absent: &[(&str, &str)] = &[
        ("GET", "/api/v1/members"),
        ("POST", "/api/v1/members"),
        ("GET", "/api/v1/voters"),
        ("POST", "/api/v1/voters"),
        ("POST", "/api/v1/coordinators"),
        ("DELETE", "/api/v1/coordinators/1"),
        ("POST", "/api/v1/coordinators/1/promote"),
        ("POST", "/api/v1/coordinators/1/remove"),
        ("POST", "/api/v1/replace-voter"),
        ("POST", "/api/v1/rotate-ca"),
        ("POST", "/api/v1/enroll-tokens"),
        ("GET", "/api/v1/enroll-tokens"),
        ("POST", "/api/v1/cluster/init"),
    ];

    for (method, path) in absent {
        let url = cluster.url(path);
        let request = match *method {
            "GET" => operator.get(url),
            "POST" => operator.post(url).json(&json!({})),
            "DELETE" => operator.delete(url),
            other => panic!("unhandled method {other}"),
        };
        let (status, body) = send(request).await;
        assert!(
            status == 404 || status == 405,
            "{method} {path} must not exist on the public surface (got {status}) \
             — membership and enrollment administration are mTLS-plane verbs by \
             the 2026-08-29 scope decision: {body}"
        );
    }

    // The route that IS there, and must stay credential-free: a machine with
    // nothing but an enrollment token and an address enrolls against an
    // OIDC-mode cluster without holding an OIDC credential at all.
    let (mut admin, history_id) = {
        let key = cluster
            .operator
            .key_pem
            .as_ref()
            .expect("the cluster minted the operator keypair");
        let mut client = coppice_coordinator::admin::admin_channel(
            &cluster.daemon.raft_target(),
            cluster.operator.ca_pem.as_bytes(),
            cluster.operator.cert_pem.as_bytes(),
            key.as_bytes(),
        )
        .await
        .expect("dial the admin surface");
        let probe = client
            .probe_cluster(coppice_proto::pb::raft::v1::ProbeClusterRequest {
                cluster_id: String::new(),
            })
            .await
            .expect("probe")
            .into_inner();
        (client, probe.history_id)
    };
    let minted = admin
        .mint_enroll_token(coppice_proto::pb::raft::v1::MintEnrollTokenRequest {
            history_id,
            role: coppice_proto::pb::core::v1::EnrollRole::Agent as i32,
            label: "scope-guard".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("mint an enrollment token")
        .into_inner();

    let (_key, csr) = coppice_tls::pki::generate_key_and_csr().expect("generate a CSR");
    let request = coppice_enroll::EnrollRequest {
        csr_pem: String::from_utf8(csr).expect("the CSR is utf-8"),
        token: None,
        node_id: Some(coppice_core::id::NodeId::new()),
        machine_id: None,
        sans: Vec::new(),
    };
    let (status, body) = send(
        cluster
            .anon
            .post(cluster.url(coppice_enroll::ENROLL_PATH))
            .bearer_auth(&minted.secret)
            .json(&request),
    )
    .await;
    assert_eq!(
        status, 200,
        "/enroll authenticates on its enrollment token alone, on an OIDC \
         cluster, with no bearer identity and no client certificate — it is \
         one of exactly two credential-free routes: {body}"
    );
    assert!(
        body["cert_pem"].as_str().is_some_and(|p| !p.is_empty()),
        "the enrollment issued a leaf: {body}"
    );

    // And the other one, for completeness: the pre-auth posture document a
    // UI bootstraps its login from.
    let (status, body) = send(cluster.anon.get(cluster.url("/api/v1/auth/config"))).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["mode"], "oidc", "{body}");
    assert_eq!(body["issuer"], cluster.idp.issuer(), "{body}");

    cluster.shutdown().await;
}
