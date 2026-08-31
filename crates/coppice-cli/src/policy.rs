//! `coppice policy`: the replicated policy surface. Today that is one group,
//! `authz`, over `GET`/`PUT /api/v1/authorization` (ADR 0023): the full
//! bindings list plus the `groups_claim` token-claim name.
//!
//! The wire shapes are [`coppice_api::http::dto`] types, unchanged from every
//! other verb's convention, and this module reuses them directly as the
//! bindings TOML's file shape (`dto::BindingDto` already round-trips through
//! TOML with the right keys). The one thing this module owns is the
//! *bindings file* wrapper — [`AuthzFile`], the TOML document from
//! `notes/oidc_impl/SHARED.md` §6 that `policy authz get` prints and
//! `policy authz set --file` reads — plus the exactly-one-subject validation
//! serde cannot express.
//!
//! ```toml
//! groups_claim = "groups"
//!
//! [[bindings]]
//! group = "batch-users"      # exactly one of group / principal
//! role  = "submitter"
//! scope = "quota-00000000-0000-0000-0000-000000000001"  # optional; absent = unscoped
//!
//! [[bindings]]
//! principal = "svc-ci"
//! role = "admin"
//! ```
//!
//! One deliberate divergence from the SHARED.md example: `scope` accepts a
//! `quota-<uuid>` entity id, not a display path (`"org/team-a"`). The server
//! stores no path for a quota entity — only the id and its parent pointer —
//! so a path is not a thing this CLI could resolve or round-trip; an
//! operator names the entity id directly, the same identity `quota show`
//! already prints.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use coppice_api::http::dto;

use crate::client::{ctx, print_json, ApiClient, ApiConnection};

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
    pub bindings: Vec<dto::BindingDto>,
}

/// Validate the exactly-one-subject rule serde cannot express, naming the
/// binding's index (1-based, matching how an operator counts `[[bindings]]`
/// tables in the file) in any failure.
fn check_exactly_one_subject(index: usize, binding: &dto::BindingDto) -> Result<()> {
    match (&binding.group, &binding.principal) {
        (Some(_), Some(_)) => anyhow::bail!(
            "binding {} must give exactly one of group/principal, not both",
            index + 1
        ),
        (None, None) => anyhow::bail!(
            "binding {} must give exactly one of group/principal",
            index + 1
        ),
        _ => Ok(()),
    }
}

impl AuthzFile {
    /// Read and parse a bindings TOML file, naming the file in every error.
    fn load(path: &Path) -> Result<AuthzFile> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading authorization policy file {}", path.display()))?;
        toml::from_str(&raw)
            .with_context(|| format!("reading authorization policy file {}", path.display()))
    }

    /// Convert to the wire [`dto::UpdateAuthorizationRequest`], validating
    /// every binding's exactly-one-subject rule up front so a bad file fails
    /// before any request is sent.
    fn to_request(&self) -> Result<dto::UpdateAuthorizationRequest> {
        for (index, binding) in self.bindings.iter().enumerate() {
            check_exactly_one_subject(index, binding)?;
        }
        Ok(dto::UpdateAuthorizationRequest {
            groups_claim: self.groups_claim.clone(),
            bindings: self.bindings.clone(),
        })
    }

    /// Wire → file, the inverse `to_request` takes on `groups_claim` and
    /// `bindings` together, for rendering `get`. `get` always reports the
    /// live `groups_claim`, so it is never `None` here.
    fn from_response(response: &dto::GetAuthorizationResponse) -> AuthzFile {
        AuthzFile {
            groups_claim: Some(response.groups_claim.clone()),
            bindings: response.bindings.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// get / set
// ---------------------------------------------------------------------------

/// `coppice policy authz get`: fetch the current policy and print it as the
/// bindings TOML (or the raw JSON with `--json`).
async fn get(client: &ApiClient, json: bool) -> Result<()> {
    let body: serde_json::Value = client
        .get_json(
            "/authorization",
            &Vec::new(),
            ctx(
                "fetching authorization policy",
                "reading authorization policy",
            ),
        )
        .await?;
    if json {
        print_json(&body);
        return Ok(());
    }
    let response: dto::GetAuthorizationResponse =
        serde_json::from_value(body).context("reading authorization policy")?;
    let file = AuthzFile::from_response(&response);
    print!(
        "{}",
        toml::to_string_pretty(&file).context("rendering authorization policy as TOML")?
    );
    Ok(())
}

/// `coppice policy authz set`: parse the bindings TOML at `--file`, convert
/// to the wire request, `PUT` it, and print a one-line success.
async fn set(client: &ApiClient, file: &Path, json: bool) -> Result<()> {
    let parsed = AuthzFile::load(file)?;
    let request = parsed.to_request()?;
    let body: serde_json::Value = client
        .put_json(
            "/authorization",
            &request,
            ctx("updating authorization policy", "reading update response"),
        )
        .await?;
    if json {
        print_json(&body);
        return Ok(());
    }
    let response: dto::UpdateAuthorizationResponse =
        serde_json::from_value(body).context("reading update response")?;
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

    use coppice_core::id::QuotaEntityId;

    use crate::testsupport::{error_body, spawn};

    fn client(base: &str) -> ApiClient {
        ApiClient::new(base).unwrap()
    }

    fn quota_id(n: u8) -> QuotaEntityId {
        format!("quota-00000000-0000-0000-0000-{n:012}")
            .parse()
            .unwrap()
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
        assert_eq!(binding.role, dto::BindingRole::Submitter);
        assert_eq!(binding.scope, Some(scope));

        let request = file.to_request().expect("converts to a request");
        assert_eq!(request.groups_claim.as_deref(), Some("groups"));
        assert_eq!(
            request.bindings,
            vec![dto::BindingDto {
                group: Some("batch-users".to_string()),
                principal: None,
                role: dto::BindingRole::Submitter,
                scope: Some(scope),
            }]
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
            vec![dto::BindingDto {
                group: None,
                principal: Some("svc-ci".to_string()),
                role: dto::BindingRole::Admin,
                scope: None,
            }]
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
        let scope = quota_id(2);
        let response = dto::GetAuthorizationResponse {
            groups_claim: "groups".to_string(),
            bindings: vec![
                dto::BindingDto {
                    group: Some("batch-users".to_string()),
                    principal: None,
                    role: dto::BindingRole::Submitter,
                    scope: Some(scope),
                },
                dto::BindingDto {
                    group: None,
                    principal: Some("svc-ci".to_string()),
                    role: dto::BindingRole::Admin,
                    scope: None,
                },
            ],
        };
        let file = AuthzFile::from_response(&response);
        let rendered = toml::to_string_pretty(&file).expect("renders");

        // Re-parse the rendered TOML exactly as `set --file` would, and
        // confirm the request it builds matches the response it came from.
        let reparsed: AuthzFile = toml::from_str(&rendered).expect("re-parses");
        let request = reparsed.to_request().expect("converts");
        assert_eq!(request.groups_claim.as_deref(), Some("groups"));
        assert_eq!(request.bindings, response.bindings);
    }

    // -----------------------------------------------------------------
    // get / set over the wire
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn get_prints_the_bindings_toml() {
        let scope = quota_id(3);
        let response = dto::GetAuthorizationResponse {
            groups_claim: "groups".to_string(),
            bindings: vec![dto::BindingDto {
                group: Some("batch-users".to_string()),
                principal: None,
                role: dto::BindingRole::Operator,
                scope: Some(scope),
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

        let scope = quota_id(4);
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
        assert_eq!(received[0].bindings[1].scope, Some(scope));
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
