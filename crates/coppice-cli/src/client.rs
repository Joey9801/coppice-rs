//! The shared HTTP plumbing every `coppice` client verb sits on.
//!
//! One place owns the things that must not drift between `job`, `cluster`,
//! `node`, and `quota`: how an `--api` value becomes a base URL, how a request
//! is built (including the request timeout `reqwest` does not set for us), and
//! how a non-2xx response becomes an `anyhow` error carrying the ADR 0031
//! `{code, message}` body and the `Coppice-Leader` retry hint.
//!
//! Nothing here knows about any endpoint: paths, query parameters, and the
//! response DTOs stay in the verb modules, which reuse
//! [`coppice_api::http::dto`] rather than redefining the contract.

use anyhow::{Context as _, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use coppice_api::http::COPPICE_LEADER;

/// How long a single client request may take before it is abandoned.
///
/// `reqwest` imposes no timeout of its own, which turns an unreachable or
/// wedged coordinator into a CLI that hangs forever with no output. Thirty
/// seconds is comfortably above any bounded read or a write's consensus
/// round-trip, and well below a human's patience for a dead endpoint.
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The port a coordinator's client API listens on unless configured otherwise.
///
/// This is one number with three users that must agree, or a first run does
/// not work without flags: the coordinator's own `[listen] client_addr`
/// default (`0.0.0.0:7070`, `coppice_coordinator::config`), the port
/// `coppice dev` asks for, and the base every client verb dials when neither
/// `--api` nor `COPPICE_API` says otherwise. The production ports table in
/// `docs/operations/configuration.md` documents the same convention
/// (client 7070, raft 7071, agent gateway 7072).
pub const DEFAULT_API_PORT: u16 = 7070;

/// The base URL every verb's `--api` falls back to: [`DEFAULT_API_PORT`] on
/// loopback.
///
/// Loopback rather than `0.0.0.0`: this is the address a client *dials*, and
/// the only coordinator a bare `coppice job …` can reasonably mean is one on
/// this machine — a local `coppice dev`. Reaching any other cluster is an
/// explicit act (`--api`, or `COPPICE_API` in the environment).
pub const DEFAULT_API_BASE: &str = "http://127.0.0.1:7070";

/// The two human contexts one request attaches to its failures: the send and
/// the body decode. They are separate because they fail for different reasons
/// — "fetching job status" is a transport problem, "reading job detail" is a
/// contract problem — and the distinction is what the operator reads first.
#[derive(Debug, Clone, Copy)]
pub struct Ctx {
    /// Wraps the transport failure (`"fetching job status"`).
    pub sending: &'static str,
    /// Wraps the response-body decode failure (`"reading job detail"`).
    pub reading: &'static str,
}

/// Shorthand for a [`Ctx`] literal at a call site.
pub const fn ctx(sending: &'static str, reading: &'static str) -> Ctx {
    Ctx { sending, reading }
}

/// A query string as the verbs build it: borrowed keys, owned values.
pub type Query = Vec<(&'static str, String)>;

/// The shared `--api`/`--token` connection flags, flattened
/// (`#[command(flatten)]`) into every HTTP verb group's argument struct so
/// the pair is declared exactly once — the same reuse move `node admin`
/// makes for the coordinator's own verb enum. One nuance lives at a verb,
/// not here: `coppice node`'s admin verbs speak the mTLS channel and ignore
/// both flags (said on its `NodeArgs`).
#[derive(Debug, clap::Args)]
pub struct ApiConnection {
    /// Base URL of the coordinator's client API. Accepts either a bare base
    /// (`http://host:7070`) or one already ending in `/api/v1`.
    #[arg(
        long,
        global = true,
        env = "COPPICE_API",
        default_value = DEFAULT_API_BASE
    )]
    pub api: String,

    /// Bearer token attached as `Authorization: Bearer <token>` on every
    /// request. No login flow, no cache, no refresh — a cluster requiring
    /// authentication expects this from an out-of-band credential.
    #[arg(long, global = true, env = "COPPICE_TOKEN", hide_env_values = true)]
    pub token: Option<String>,
}

impl ApiConnection {
    /// The [`ApiClient`] these flags describe.
    pub fn client(&self) -> Result<ApiClient> {
        ApiClient::with_token(&self.api, self.token.as_deref())
    }
}

/// A client bound to one coordinator's `/api/v1` surface.
///
/// Holds the normalized base URL and a single `reqwest::Client`, so a verb
/// that makes several requests (a paging walk, `job logs --follow`) reuses one
/// connection pool instead of dialing afresh each time.
#[derive(Debug, Clone)]
pub struct ApiClient {
    base: String,
    http: reqwest::Client,
    /// The bearer token from `--token`/`COPPICE_TOKEN`, when set and
    /// non-empty. Attached as `Authorization: Bearer <token>` on every
    /// request; `None` sends no `Authorization` header at all (the open-mode
    /// posture `coppice dev` runs in needs none).
    token: Option<String>,
}

impl ApiClient {
    /// Build a client for an `--api` value, normalizing the base URL, with no
    /// bearer token attached. Every verb's `run()` goes through
    /// [`Self::with_token`] instead (there is always a `--token` flag to
    /// thread through, even when unset); this convenience constructor is for
    /// tests that do not exercise the token path.
    #[cfg(test)]
    pub fn new(api: &str) -> Result<ApiClient> {
        ApiClient::with_token(api, None)
    }

    /// Build a client for an `--api` value and an optional bearer token
    /// (`--token`/`COPPICE_TOKEN`). An empty token string is treated the same
    /// as `None` — no `Authorization` header — since clap surfaces `env =
    /// "COPPICE_TOKEN"` as `Some("")` when the variable is set but empty.
    pub fn with_token(api: &str, token: Option<&str>) -> Result<ApiClient> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("building the HTTP client")?;
        Ok(ApiClient {
            base: normalize_base(api),
            http,
            token: token
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_string),
        })
    }

    /// The absolute URL for an `/api/v1`-relative path (`"/jobs"`).
    pub fn url(&self, path: &str) -> String {
        format!("{}/api/v1{path}", self.base)
    }

    /// Attach `Authorization: Bearer <token>` to a request builder when a
    /// token is configured, otherwise pass it through unchanged.
    fn authed(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    /// GET a path and decode its JSON body.
    ///
    /// `T` may be a DTO or `serde_json::Value` — the `--json` verbs decode to a
    /// `Value` and print the server's own bytes back, so a pass-through render
    /// can never disagree with the wire.
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &Query,
        ctx: Ctx,
    ) -> Result<T> {
        let request = self.authed(self.http.get(self.url(path)).query(query));
        let response = request.send().await.context(ctx.sending)?;
        if !response.status().is_success() {
            return Err(api_error(response).await);
        }
        response.json().await.context(ctx.reading)
    }

    /// POST a JSON body and decode the JSON response.
    pub async fn post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        ctx: Ctx,
    ) -> Result<T> {
        let response = self.send_post(path, body, ctx.sending).await?;
        response.json().await.context(ctx.reading)
    }

    /// POST a JSON body for its status alone, discarding the response body —
    /// for a write whose success carries no information the caller needs.
    pub async fn post_ignoring_body<B: Serialize>(
        &self,
        path: &str,
        body: &B,
        sending: &'static str,
    ) -> Result<()> {
        self.send_post(path, body, sending).await?;
        Ok(())
    }

    /// PUT a JSON body and decode the JSON response — the full-replacement
    /// counterpart to [`Self::post_json`], used by `policy authz set`.
    pub async fn put_json<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        ctx: Ctx,
    ) -> Result<T> {
        let request = self.authed(self.http.put(self.url(path)).json(body));
        let response = request.send().await.context(ctx.sending)?;
        if !response.status().is_success() {
            return Err(api_error(response).await);
        }
        response.json().await.context(ctx.reading)
    }

    /// The shared POST half: send, and map a non-2xx to a rich error.
    async fn send_post<B: Serialize>(
        &self,
        path: &str,
        body: &B,
        sending: &'static str,
    ) -> Result<reqwest::Response> {
        let request = self.authed(self.http.post(self.url(path)).json(body));
        let response = request.send().await.context(sending)?;
        if !response.status().is_success() {
            return Err(api_error(response).await);
        }
        Ok(response)
    }
}

/// Reduce an `--api` value to a bare base URL. Trims a trailing slash, then a
/// trailing `/api/v1` (the form the dev banner prints and users paste), then
/// any slash that exposes — so every accepted form maps to the same base.
pub fn normalize_base(raw: &str) -> String {
    let trimmed = raw.trim_end_matches('/');
    trimmed
        .strip_suffix("/api/v1")
        .unwrap_or(trimmed)
        .trim_end_matches('/')
        .to_string()
}

/// The wire error body (ADR 0031). The API's own `ErrorBody` is private and
/// serialize-only, so the client mirrors just the two fields it reads.
#[derive(Debug, Deserialize)]
struct ApiErrorBody {
    code: String,
    message: String,
}

/// Turn a non-2xx response into an `anyhow` error, reading the `{code,
/// message}` body (falling back to raw text) and, on a 421/NOT_LEADER with a
/// `Coppice-Leader` hint, appending where to retry.
pub async fn api_error(response: reqwest::Response) -> anyhow::Error {
    let status = response.status();
    let leader = response
        .headers()
        .get(COPPICE_LEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response.text().await.unwrap_or_default();

    let mut message = match serde_json::from_str::<ApiErrorBody>(&body) {
        Ok(parsed) => format!("api error ({}): {}", parsed.code, parsed.message),
        Err(_) if !body.trim().is_empty() => {
            format!("api error (HTTP {}): {}", status.as_u16(), body.trim())
        }
        Err(_) => format!("api error (HTTP {})", status.as_u16()),
    };
    if status == reqwest::StatusCode::MISDIRECTED_REQUEST {
        if let Some(leader) = leader {
            message.push_str(&format!("; retry against the leader at {leader}"));
        }
    }
    if status == reqwest::StatusCode::UNAUTHORIZED {
        message.push_str(
            "; this cluster requires authentication — set COPPICE_TOKEN to a bearer token, \
             or use a dev cluster (`coppice dev`), which runs in open mode and needs none",
        );
    }
    anyhow::anyhow!(message)
}

/// Print a JSON value as the `--json` rendering: pretty, one trailing newline.
///
/// Every `--json` verb prints the body the server sent (decoded to a
/// [`serde_json::Value`] and re-emitted), never a re-serialization of a parsed
/// DTO — so the machine-readable output is the contract itself, including any
/// field this CLI is too old to know about.
pub fn print_json(value: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

/// Render an aligned plain-text table: a header row plus body rows, columns
/// padded to the widest cell and left-aligned, two spaces between columns.
///
/// Shared by every list verb so `job list`, `node list`, and `quota list` all
/// look like one program. Rows are ragged-tolerant: a short row simply ends
/// early rather than panicking.
pub fn render_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    use std::fmt::Write;

    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let mut out = String::new();
    let line = |out: &mut String, cells: &[&str]| {
        let mut line = String::new();
        for (i, cell) in cells.iter().enumerate() {
            if i > 0 {
                line.push_str("  ");
            }
            let _ = write!(line, "{cell:<width$}", width = widths[i]);
        }
        let _ = writeln!(out, "{}", line.trim_end());
    };

    line(&mut out, headers);
    for row in rows {
        let cells: Vec<&str> = row.iter().map(String::as_str).collect();
        line(&mut out, &cells);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::routing::get;
    use axum::Router;

    use crate::testsupport::{error_body, spawn};

    /// `--token`/`COPPICE_TOKEN`, when set and non-empty, attaches
    /// `Authorization: Bearer <token>` to every request.
    #[tokio::test]
    async fn token_attaches_the_authorization_header() {
        let captured: std::sync::Arc<std::sync::Mutex<Option<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let router = {
            let captured = captured.clone();
            Router::new().route(
                "/api/v1/session",
                get(move |headers: axum::http::HeaderMap| {
                    let captured = captured.clone();
                    async move {
                        let auth = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        captured.lock().unwrap().replace(auth.unwrap_or_default());
                        axum::Json(serde_json::json!({}))
                    }
                }),
            )
        };
        let base = spawn(router).await;
        let client = ApiClient::with_token(&base, Some("secret-token")).unwrap();
        let _: serde_json::Value = client
            .get_json("/session", &Vec::new(), ctx("fetching", "reading"))
            .await
            .unwrap();
        assert_eq!(
            captured.lock().unwrap().as_deref(),
            Some("Bearer secret-token")
        );
    }

    /// No `--token` means no `Authorization` header at all — the header's
    /// mere presence is what the open-mode posture must never see.
    #[tokio::test]
    async fn no_token_sends_no_authorization_header() {
        let captured: std::sync::Arc<std::sync::Mutex<Option<bool>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let router = {
            let captured = captured.clone();
            Router::new().route(
                "/api/v1/session",
                get(move |headers: axum::http::HeaderMap| {
                    let captured = captured.clone();
                    async move {
                        let present = headers.contains_key(axum::http::header::AUTHORIZATION);
                        captured.lock().unwrap().replace(present);
                        axum::Json(serde_json::json!({}))
                    }
                }),
            )
        };
        let base = spawn(router).await;
        let client = ApiClient::new(&base).unwrap();
        let _: serde_json::Value = client
            .get_json("/session", &Vec::new(), ctx("fetching", "reading"))
            .await
            .unwrap();
        assert_eq!(*captured.lock().unwrap(), Some(false));
    }

    /// An empty `COPPICE_TOKEN` (clap surfaces the env var set-but-empty as
    /// `Some("")`) must behave exactly like no token at all.
    #[tokio::test]
    async fn empty_token_sends_no_authorization_header() {
        let router = Router::new().route(
            "/api/v1/session",
            get(|headers: axum::http::HeaderMap| async move {
                assert!(!headers.contains_key(axum::http::header::AUTHORIZATION));
                axum::Json(serde_json::json!({}))
            }),
        );
        let base = spawn(router).await;
        let client = ApiClient::with_token(&base, Some("")).unwrap();
        let _: serde_json::Value = client
            .get_json("/session", &Vec::new(), ctx("fetching", "reading"))
            .await
            .unwrap();
    }

    /// A 401 error message mentions `COPPICE_TOKEN` and that dev clusters run
    /// in open mode — the two facts an operator needs to unblock themselves.
    #[tokio::test]
    async fn unauthorized_error_mentions_coppice_token_and_open_mode() {
        let router = Router::new().route(
            "/api/v1/session",
            get(|| async {
                (
                    axum::http::StatusCode::UNAUTHORIZED,
                    axum::Json(error_body("UNAUTHENTICATED", "no credentials")),
                )
            }),
        );
        let base = spawn(router).await;
        let client = ApiClient::new(&base).unwrap();
        let err = client
            .get_json::<serde_json::Value>("/session", &Vec::new(), ctx("fetching", "reading"))
            .await
            .expect_err("401 fails");
        let message = format!("{err:#}");
        assert!(message.contains("COPPICE_TOKEN"), "{message}");
        assert!(message.contains("open mode"), "{message}");
    }

    #[test]
    fn api_base_normalizes_to_one_form() {
        let want = "http://h:7070";
        for raw in [
            "http://h:7070",
            "http://h:7070/",
            "http://h:7070/api/v1",
            "http://h:7070/api/v1/",
        ] {
            assert_eq!(normalize_base(raw), want, "{raw}");
        }
    }

    #[test]
    fn url_joins_the_api_prefix() {
        let client = ApiClient::new("http://h:7070/api/v1/").unwrap();
        assert_eq!(client.url("/jobs"), "http://h:7070/api/v1/jobs");
    }

    /// The default base and the default port are two literals that must name
    /// the same endpoint — `clap`'s `default_value` needs a `&'static str`, so
    /// the base cannot be built from the port at compile time. This closes the
    /// gap the other way: if either moves without the other, the CLI's default
    /// stops pointing at the port `coppice dev` binds.
    #[test]
    fn the_default_api_base_names_the_default_api_port() {
        assert_eq!(
            DEFAULT_API_BASE,
            format!("http://127.0.0.1:{DEFAULT_API_PORT}")
        );
        // And it must survive normalization unchanged, since every verb feeds
        // it straight into `ApiClient::new`.
        assert_eq!(normalize_base(DEFAULT_API_BASE), DEFAULT_API_BASE);
    }

    #[test]
    fn table_pads_to_the_widest_cell() {
        let rendered = render_table(
            &["id", "state"],
            &[
                vec!["a".to_string(), "queued".to_string()],
                vec!["longer-id".to_string(), "running".to_string()],
            ],
        );
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines[0], "id         state");
        assert_eq!(lines[1], "a          queued");
        assert_eq!(lines[2], "longer-id  running");
    }
}
