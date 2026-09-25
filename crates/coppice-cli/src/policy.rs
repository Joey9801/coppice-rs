//! `coppice policy`: the replicated policy surface. Today that is one group,
//! `authz`, over `GET`/`PUT /api/v1/authorization` (ADR 0023): the full
//! bindings list plus the `groups_claim` token-claim name.
//!
//! The wire shapes are the published [`coppice_client`] crate's own types
//! (`Binding`, `BindingView`, `GetAuthorizationResponse`,
//! `UpdateAuthorizationRequest`, `UpdateAuthorizationResponse`), as in every
//! other verb. What this module
//! owns outright is the *bindings file* — [`AuthzFile`] and its
//! [`FileBinding`] tables, the TOML document from
//! `notes/oidc_impl/SHARED.md` §6 that `policy authz get` prints and `policy
//! authz set --file` reads — plus the exactly-one-subject validation serde
//! cannot express, applied where a file binding becomes a wire one.
//!
//! ```toml
//! groups_claim = "groups"
//!
//! [[bindings]]
//! group = "batch-users"      # exactly one of group / principal
//! role  = "submitter"
//! scope = "acme/team-a"      # optional id or path (ADR 0045); absent = unscoped
//!
//! [[bindings]]
//! principal = "svc-ci"
//! role = "admin"
//! ```
//!
//! `scope` takes an id or a path (ADR 0045) — a path is resolved against the
//! server's read view before the write proposes. `policy authz get` prints
//! the entity's **path** when the server could resolve one (`scope_path`),
//! falling back to the bare id only when it could not (the scope names an
//! entity absent from the tree). `policy authz set --file` round-trips
//! either spelling back into a request.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use coppice_client::{
    paths, Binding, BindingRole, BindingView, Client, GetAuthorizationResponse, QuotaEntityRef,
    UpdateAuthorizationRequest, UpdateAuthorizationResponse,
};

use crate::client::{ctx, print_json, ApiConnection, ApiResultExt};

// ---------------------------------------------------------------------------
// CLI surface
// ---------------------------------------------------------------------------

/// `coppice policy` argument group. `--api`/`--token` are global, matching
/// every other client verb.
#[derive(Debug, clap::Args)]
pub struct PolicyArgs {
    #[command(flatten)]
    connection: ApiConnection,

    #[command(subcommand)]
    pub command: PolicyCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum PolicyCommand {
    /// The scoped role-bindings policy (`groups_claim` + `bindings`).
    Authz {
        #[command(subcommand)]
        command: AuthzCommand,
    },
}

#[derive(Debug, clap::Subcommand)]
pub enum AuthzCommand {
    /// Print the current authorization policy as the bindings TOML (see the
    /// module docs), or the raw server JSON with `--json`.
    Get {
        /// Print the server's JSON response instead of the TOML.
        #[arg(long)]
        json: bool,
    },
    /// Full-replacement update: read the bindings TOML from `--file`,
    /// convert it to the wire request, and `PUT` it.
    Set {
        /// Path to a bindings TOML file (see the module docs for the
        /// schema).
        #[arg(long)]
        file: PathBuf,
        /// Print the server's JSON response instead of the summary line.
        #[arg(long)]
        json: bool,
    },
}

/// Run the selected `coppice policy` verb.
pub async fn run(args: PolicyArgs) -> Result<()> {
    let client = args.connection.client()?;
    match args.command {
        PolicyCommand::Authz { command } => match command {
            AuthzCommand::Get { json } => get(&client, json).await,
            AuthzCommand::Set { file, json } => set(&client, &file, json).await,
        },
    }
}

// ---------------------------------------------------------------------------
// Bindings TOML
// ---------------------------------------------------------------------------

/// The bindings TOML document (SHARED.md §6): `policy authz get` writes it,
/// `policy authz set --file` reads it.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthzFile {
    /// The token claim group names are read from. Absent on `set` means
    /// "leave the current policy value unchanged" — it is never defaulted
    /// here, since a silent default could clobber a value the caller did not
    /// mean to touch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups_claim: Option<String>,
    #[serde(default)]
    pub bindings: Vec<FileBinding>,
}

/// One `[[bindings]]` table: the file's own strict shape, converted to the
/// wire [`Binding`] by [`AuthzFile::to_request`].
///
/// Not [`Binding`] itself, for two reasons. [`Binding`] carries no
/// `#[serde(deny_unknown_fields)]` — it also shapes `GET /authorization`'s
/// response, and the client library's responses tolerate a future server
/// field — whereas this file is a write path that has always rejected a
/// typo'd key (`rejects_unknown_keys`). And a file can spell the states the
/// wire type's constructors rule out (both subjects, or neither), which is
/// exactly what [`check_exactly_one_subject`] exists to name for the operator.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileBinding {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    pub role: BindingRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<QuotaEntityRef>,
}

/// The read-modify-write conversion `policy authz get` uses: a
/// [`BindingView`]'s scope, preferring the resolved path (`scope_path`) —
/// the whole point of printing a path rather than an id — and falling back
/// to the bare id only when the server could not resolve one (the scope
/// names an entity absent from the tree, which cannot happen for a live
/// binding but is not ruled out on the wire).
impl From<&BindingView> for FileBinding {
    fn from(view: &BindingView) -> FileBinding {
        let scope = match (&view.scope_path, view.scope) {
            (Some(path), _) => path.parse::<QuotaEntityRef>().ok(),
            (None, Some(id)) => Some(QuotaEntityRef::Id(id)),
            (None, None) => None,
        };
        FileBinding {
            group: view.group.clone(),
            principal: view.principal.clone(),
            role: view.role.clone(),
            scope,
        }
    }
}

/// Convert one file binding to the wire [`Binding`], enforcing the
/// exactly-one-subject rule serde cannot express and naming the binding's
/// index in any failure — 1-based, matching how an operator counts
/// `[[bindings]]` tables in the file. (The library's own
/// [`UpdateAuthorizationRequest::validate`] words the same rule 0-based, for
/// a caller indexing a `Vec`; a bad file never reaches it.)
fn check_exactly_one_subject(index: usize, binding: &FileBinding) -> Result<Binding> {
    let wire = match (&binding.group, &binding.principal) {
        (Some(_), Some(_)) => anyhow::bail!(
            "binding {} must give exactly one of group/principal, not both",
            index + 1
        ),
        (None, None) => anyhow::bail!(
            "binding {} must give exactly one of group/principal",
            index + 1
        ),
        (Some(group), None) => Binding::for_group(group, binding.role.clone()),
        (None, Some(principal)) => Binding::for_principal(principal, binding.role.clone()),
    };
    Ok(match &binding.scope {
        Some(scope) => wire.with_scope(scope.clone()),
        None => wire,
    })
}

impl AuthzFile {
    /// Read and parse a bindings TOML file, naming the file in every error.
    fn load(path: &Path) -> Result<AuthzFile> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading authorization policy file {}", path.display()))?;
        toml::from_str(&raw)
            .with_context(|| format!("reading authorization policy file {}", path.display()))
    }

    /// Convert to the wire [`UpdateAuthorizationRequest`], validating every
    /// binding's exactly-one-subject rule up front so a bad file fails before
    /// any request is sent.
    fn to_request(&self) -> Result<UpdateAuthorizationRequest> {
        let bindings = self
            .bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| check_exactly_one_subject(index, binding))
            .collect::<Result<Vec<Binding>>>()?;
        let mut request = UpdateAuthorizationRequest::new(bindings);
        if let Some(groups_claim) = self.groups_claim.clone() {
            request = request.with_groups_claim(groups_claim);
        }
        Ok(request)
    }

    /// Wire → file, the inverse `to_request` takes on `groups_claim` and
    /// `bindings` together, for rendering `get`. `get` always reports the
    /// live `groups_claim`, so it is never `None` here.
    fn from_response(response: &GetAuthorizationResponse) -> AuthzFile {
        AuthzFile {
            groups_claim: Some(response.groups_claim.clone()),
            bindings: response.bindings.iter().map(FileBinding::from).collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// get / set
// ---------------------------------------------------------------------------

/// `coppice policy authz get`: fetch the current policy and print it as the
/// bindings TOML (or the raw JSON with `--json`).
async fn get(client: &Client, json: bool) -> Result<()> {
    let read_ctx = ctx(
        "fetching authorization policy",
        "reading authorization policy",
    );
    if json {
        let body = client
            .get_value(paths::AUTHORIZATION, &[])
            .await
            .api_ctx(read_ctx)?;
        print_json(&body.value);
        return Ok(());
    }
    let response = client.authorization().await.api_ctx(read_ctx)?;
    let file = AuthzFile::from_response(&response);
    print!(
        "{}",
        toml::to_string_pretty(&file).context("rendering authorization policy as TOML")?
    );
    Ok(())
}

/// `coppice policy authz set`: parse the bindings TOML at `--file`, convert
/// to the wire request, `PUT` it, and print a one-line success.
async fn set(client: &Client, file: &Path, json: bool) -> Result<()> {
    let parsed = AuthzFile::load(file)?;
    let request = parsed.to_request()?;
    let write_ctx = ctx("updating authorization policy", "reading update response");
    if json {
        let body = client
            .put_value(paths::AUTHORIZATION, &request)
            .await
            .api_ctx(write_ctx)?;
        print_json(&body);
        return Ok(());
    }
    let response: UpdateAuthorizationResponse = client
        .update_authorization(&request)
        .await
        .api_ctx(write_ctx)?;
    println!(
        "updated authorization policy (log index {})",
        response.log_index
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write as _;
    use std::sync::{Arc, Mutex};

    use axum::extract::State;
    use axum::routing::{get as axum_get, put};
    use axum::{Json, Router};
    use tempfile::NamedTempFile;

    use coppice_api::http::dto;
    use coppice_core::id::QuotaEntityId;

    use crate::testsupport::{error_body, spawn};

    /// Round-trip a server-typed (`coppice_api::http::dto`) fixture through
    /// JSON into the `coppice_client` type the CLI actually reads/writes. The
    /// server type is the fixture that pins the wire contract; the client
    /// type is what `get`/`set` take — so this conversion, not a hand-written
    /// assertion, *is* the cross-check that the two crates still agree on the
    /// shape.
    fn to_client<S: serde::Serialize, C: serde::de::DeserializeOwned>(value: S) -> C {
        serde_json::from_value(serde_json::to_value(value).unwrap())
            .expect("the client type decodes the server type's own output")
    }

    fn client(base: &str) -> Client {
        Client::new(base).unwrap()
    }

    /// The same id, spelled once, parsed into whichever crate's typed id a
    /// given fixture needs: `coppice_client::QuotaEntityId` for a `Binding`
    /// this module builds directly, `coppice_core::id::QuotaEntityId` for a
    /// `dto::BindingDto` fixture standing in for the server.
    fn quota_id_str(n: u8) -> String {
        format!("quota-00000000-0000-0000-0000-{n:012}")
    }

    fn quota_id(n: u8) -> coppice_client::QuotaEntityId {
        quota_id_str(n).parse().unwrap()
    }

    fn dto_quota_id(n: u8) -> QuotaEntityId {
        quota_id_str(n).parse().unwrap()
    }

    fn write_toml(body: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(body.as_bytes()).unwrap();
        file
    }

    // -----------------------------------------------------------------
    // AuthzFile parsing / validation
    // -----------------------------------------------------------------

    #[test]
    fn parses_a_group_binding_with_scope() {
        let scope = quota_id(1);
        let toml_body = format!(
            "groups_claim = \"groups\"\n\n[[bindings]]\ngroup = \"batch-users\"\nrole = \"submitter\"\nscope = \"{scope}\"\n"
        );
        let file: AuthzFile = toml::from_str(&toml_body).expect("parses");
        assert_eq!(file.groups_claim.as_deref(), Some("groups"));
        assert_eq!(file.bindings.len(), 1);
        let binding = &file.bindings[0];
        assert_eq!(binding.group.as_deref(), Some("batch-users"));
        assert!(binding.principal.is_none());
        assert_eq!(binding.role, coppice_client::BindingRole::Submitter);
        assert_eq!(binding.scope, Some(QuotaEntityRef::Id(scope)));

        let request = file.to_request().expect("converts to a request");
        assert_eq!(request.groups_claim.as_deref(), Some("groups"));
        assert_eq!(
            request.bindings,
            vec![
                Binding::for_group("batch-users", coppice_client::BindingRole::Submitter)
                    .with_scope(scope)
            ]
        );
    }

    /// A path scope is as valid as an id (ADR 0045).
    #[test]
    fn parses_a_group_binding_with_a_path_scope() {
        let toml_body = "[[bindings]]\ngroup = \"batch-users\"\nrole = \"submitter\"\n\
                          scope = \"acme/team-a\"\n";
        let file: AuthzFile = toml::from_str(toml_body).expect("parses");
        let binding = &file.bindings[0];
        assert_eq!(
            binding.scope,
            Some(QuotaEntityRef::Path("acme/team-a".parse().unwrap()))
        );
        let request = file.to_request().expect("converts to a request");
        assert_eq!(
            request.bindings[0].scope,
            Some(QuotaEntityRef::Path("acme/team-a".parse().unwrap()))
        );
    }

    #[test]
    fn parses_a_principal_binding_without_scope() {
        let toml_body = "[[bindings]]\nprincipal = \"svc-ci\"\nrole = \"admin\"\n";
        let file: AuthzFile = toml::from_str(toml_body).expect("parses");
        assert!(file.groups_claim.is_none());
        let request = file.to_request().expect("converts to a request");
        assert!(request.groups_claim.is_none());
        assert_eq!(
            request.bindings,
            vec![Binding::for_principal(
                "svc-ci",
                coppice_client::BindingRole::Admin
            )]
        );
    }

    #[test]
    fn rejects_both_subjects() {
        let toml_body = "[[bindings]]\ngroup = \"g\"\nprincipal = \"p\"\nrole = \"admin\"\n";
        let file: AuthzFile = toml::from_str(toml_body).expect("parses");
        let err = file.to_request().expect_err("both subjects rejected");
        assert!(format!("{err:#}").contains("exactly one"));
    }

    #[test]
    fn rejects_neither_subject() {
        let toml_body = "[[bindings]]\nrole = \"admin\"\n";
        let file: AuthzFile = toml::from_str(toml_body).expect("parses");
        let err = file.to_request().expect_err("neither subject rejected");
        assert!(format!("{err:#}").contains("exactly one"));
    }

    #[test]
    fn rejects_unknown_keys() {
        let toml_body = "[[bindings]]\ngroup = \"g\"\nrole = \"admin\"\nbogus = 1\n";
        let result: Result<AuthzFile, _> = toml::from_str(toml_body);
        assert!(result.is_err());
    }

    #[test]
    fn get_rendering_round_trips_through_set_parsing() {
        let scope = dto_quota_id(2);
        // `GetAuthorizationResponse` is `#[non_exhaustive]`, so — as with
        // every other cross-check test here — the fixture is a server dto,
        // converted into the client type via the same JSON path a real
        // response takes.
        let response: GetAuthorizationResponse = to_client(dto::GetAuthorizationResponse {
            groups_claim: "groups".to_string(),
            bindings: vec![
                dto::BindingView {
                    group: Some("batch-users".to_string()),
                    principal: None,
                    role: dto::BindingRole::Submitter,
                    scope: Some(scope),
                    scope_path: Some("acme/team-a".to_string()),
                },
                dto::BindingView {
                    group: None,
                    principal: Some("svc-ci".to_string()),
                    role: dto::BindingRole::Admin,
                    scope: None,
                    scope_path: None,
                },
            ],
        });
        let file = AuthzFile::from_response(&response);
        // The path is what gets rendered, not the id — the whole point of
        // preferring `scope_path` (ADR 0045).
        let rendered = toml::to_string_pretty(&file).expect("renders");
        assert!(rendered.contains("acme/team-a"), "{rendered}");
        assert!(!rendered.contains(&scope.to_string()), "{rendered}");

        // Re-parse the rendered TOML exactly as `set --file` would, and
        // confirm the request it builds carries the same subjects/roles —
        // the scope legitimately differs in spelling (a path in, an id out
        // of `Binding::with_scope`'s echo), so compare that separately.
        let reparsed: AuthzFile = toml::from_str(&rendered).expect("re-parses");
        let request = reparsed.to_request().expect("converts");
        assert_eq!(request.groups_claim.as_deref(), Some("groups"));
        assert_eq!(request.bindings.len(), response.bindings.len());
        assert_eq!(
            request.bindings[0].scope,
            Some(coppice_client::QuotaEntityRef::Path(
                "acme/team-a".parse().unwrap()
            ))
        );
    }

    // -----------------------------------------------------------------
    // get / set over the wire
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn get_prints_the_bindings_toml() {
        let scope = dto_quota_id(3);
        let response = dto::GetAuthorizationResponse {
            groups_claim: "groups".to_string(),
            bindings: vec![dto::BindingView {
                group: Some("batch-users".to_string()),
                principal: None,
                role: dto::BindingRole::Operator,
                scope: Some(scope),
                scope_path: Some("acme/ops".to_string()),
            }],
        };
        let router = Router::new().route(
            "/api/v1/authorization",
            axum_get({
                let response = response.clone();
                move || {
                    let response = response.clone();
                    async move { Json(serde_json::to_value(response).unwrap()) }
                }
            }),
        );
        let base = spawn(router).await;
        get(&client(&base), false).await.expect("get succeeds");

        // The client decodes exactly what the server sent.
        let decoded: GetAuthorizationResponse = to_client(response);
        assert_eq!(decoded.bindings.len(), 1);
    }

    #[tokio::test]
    async fn set_puts_the_converted_request() {
        let captured: Arc<Mutex<Vec<dto::UpdateAuthorizationRequest>>> =
            Arc::new(Mutex::new(Vec::new()));
        let router = Router::new()
            .route(
                "/api/v1/authorization",
                put(
                    |State(captured): State<Arc<Mutex<Vec<dto::UpdateAuthorizationRequest>>>>,
                     Json(req): Json<dto::UpdateAuthorizationRequest>| async move {
                        captured.lock().unwrap().push(req);
                        Json(
                            serde_json::to_value(dto::UpdateAuthorizationResponse {
                                log_index: 11,
                            })
                            .unwrap(),
                        )
                    },
                ),
            )
            .with_state(captured.clone());
        let base = spawn(router).await;

        let scope = dto_quota_id(4);
        let toml_body = format!(
            "groups_claim = \"groups\"\n\n[[bindings]]\nprincipal = \"svc-ci\"\nrole = \"admin\"\n\n[[bindings]]\ngroup = \"batch-users\"\nrole = \"submitter\"\nscope = \"{scope}\"\n"
        );
        let file = write_toml(&toml_body);

        set(&client(&base), file.path(), false)
            .await
            .expect("set succeeds");

        let received = captured.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].groups_claim.as_deref(), Some("groups"));
        assert_eq!(received[0].bindings.len(), 2);
        assert_eq!(
            received[0].bindings[1].scope,
            Some(coppice_core::entity_ref::QuotaEntityRef::Id(scope))
        );
    }

    #[tokio::test]
    async fn set_surfaces_an_error_body() {
        let router = Router::new().route(
            "/api/v1/authorization",
            put(|| async {
                (
                    axum::http::StatusCode::FORBIDDEN,
                    Json(error_body("PERMISSION_DENIED", "not an admin")),
                )
            }),
        );
        let base = spawn(router).await;
        let toml_body = "[[bindings]]\nprincipal = \"svc-ci\"\nrole = \"admin\"\n";
        let file = write_toml(toml_body);

        let err = set(&client(&base), file.path(), false)
            .await
            .expect_err("set fails");
        let message = format!("{err:#}");
        assert!(message.contains("PERMISSION_DENIED"), "{message}");
    }
}
