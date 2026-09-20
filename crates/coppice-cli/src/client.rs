//! What the `coppice` client verbs need on top of [`coppice_client`].
//!
//! The transport itself — base-URL normalization, the bearer token, the
//! request timeout, paths, query parameters, wire types, pagination, the log
//! follower, and turning a non-2xx response into the ADR 0031 `{code,
//! message}` error with its `Coppice-Leader` hint — belongs to the published
//! [`coppice_client`] crate, which is also where it is tested. Every verb
//! module builds a [`coppice_client::Client`] from the flags below and calls
//! it directly.
//!
//! Four things are left here, because they are about *this CLI* rather than
//! about the API:
//!
//! - [`ApiConnection`], the `--api`/`--token` flag pair every HTTP verb group
//!   flattens in;
//! - [`Ctx`] and [`ApiResultExt`], which attach the human "what was I doing"
//!   context to a failure and append the 401 hint that tells an operator to
//!   set `COPPICE_TOKEN`;
//! - [`print_json`], the shared `--json` rendering;
//! - [`render_table`], the shared list-verb table.

use anyhow::Result;

use coppice_client::{Client, Error, DEFAULT_BASE_URL, DEFAULT_PORT};

/// The port a coordinator's client API listens on unless configured otherwise.
///
/// This is one number with three users that must agree, or a first run does
/// not work without flags: the coordinator's own `[listen] client_addr`
/// default (`0.0.0.0:7070`, `coppice_coordinator::config`), the port
/// `coppice dev` asks for, and the base every client verb dials when neither
/// `--api` nor `COPPICE_API` says otherwise. The production ports table in
/// `docs/operations/configuration.md` documents the same convention
/// (client 7070, raft 7071, agent gateway 7072). The number itself is the
/// client library's [`DEFAULT_PORT`], so the CLI and anything else built on
/// that crate cannot disagree about it.
pub const DEFAULT_API_PORT: u16 = DEFAULT_PORT;

/// The base URL every verb's `--api` falls back to: [`DEFAULT_API_PORT`] on
/// loopback, as [`coppice_client::DEFAULT_BASE_URL`] spells it.
///
/// Loopback rather than `0.0.0.0`: this is the address a client *dials*, and
/// the only coordinator a bare `coppice job …` can reasonably mean is one on
/// this machine — a local `coppice dev`. Reaching any other cluster is an
/// explicit act (`--api`, or `COPPICE_API` in the environment).
pub const DEFAULT_API_BASE: &str = DEFAULT_BASE_URL;

/// The hint a 401 carries, on top of whatever the server said: the two facts
/// an operator needs to unblock themselves. It is a CLI concern — it names an
/// environment variable and a subcommand — so it lives here rather than in the
/// library's error text.
const UNAUTHORIZED_HINT: &str =
    "; this cluster requires authentication — set COPPICE_TOKEN to a bearer token, \
     or use a dev cluster (`coppice dev`), which runs in open mode and needs none";

/// The two human contexts one request attaches to its failures: the send and
/// the body decode. They are separate because they fail for different reasons
/// — "fetching job status" is a transport problem, "reading job detail" is a
/// contract problem — and the distinction is what the operator reads first.
/// They map onto [`Error::Transport`] and [`Error::Decode`] respectively.
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
    /// The [`Client`] these flags describe.
    ///
    /// An empty `--token` is treated as no token at all — clap surfaces `env =
    /// "COPPICE_TOKEN"` as `Some("")` when the variable is set but empty, and
    /// the library's builder applies that rule.
    pub fn client(&self) -> Result<Client> {
        Ok(Client::builder(&self.api)
            .token_opt(self.token.as_deref())
            .build()?)
    }
}

/// Turn a [`coppice_client::Error`] into the `anyhow` error this CLI prints.
///
/// The three cases that reach an operator differently:
///
/// - a transport failure carries `ctx.sending` ("listing jobs"), with the
///   underlying `reqwest` error as its source;
/// - a body that did not decode carries `ctx.reading` ("reading the job
///   list"), with the `serde_json` error as its source;
/// - everything the server answered keeps the library's own `Display`, which
///   is the `api error (CODE): message` wording the CLI has always printed —
///   plus, on a 401, [`UNAUTHORIZED_HINT`].
pub fn api_error(err: Error, ctx: Ctx) -> anyhow::Error {
    match err {
        Error::Transport(e) => anyhow::Error::new(e).context(ctx.sending),
        Error::Decode(e) => anyhow::Error::new(e).context(ctx.reading),
        Error::Build(e) => anyhow::Error::new(e).context("building the HTTP client"),
        other => {
            let mut message = other.to_string();
            if other.status() == Some(401) {
                message.push_str(UNAUTHORIZED_HINT);
            }
            anyhow::anyhow!(message)
        }
    }
}

/// Attach a [`Ctx`] to a [`coppice_client`] call, the way a verb wants it.
///
/// Spelled `api_ctx` rather than `context`: `anyhow::Context` is already
/// implemented for this very `Result`, and a same-named method would be
/// ambiguous at every call site.
pub trait ApiResultExt<T> {
    /// Map the library error into an `anyhow` one carrying `ctx`.
    fn api_ctx(self, ctx: Ctx) -> Result<T>;
}

impl<T> ApiResultExt<T> for coppice_client::Result<T> {
    fn api_ctx(self, ctx: Ctx) -> Result<T> {
        self.map_err(|e| api_error(e, ctx))
    }
}

/// Print a JSON value as the `--json` rendering: pretty, one trailing newline.
///
/// Every `--json` verb prints the body the server sent — fetched through
/// [`Client::get_value`] and friends, never re-serialized from a parsed type —
/// so the machine-readable output is the contract itself, including any field
/// this CLI is too old to know about.
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

    /// A 401 error message mentions `COPPICE_TOKEN` and that dev clusters run
    /// in open mode — the two facts an operator needs to unblock themselves.
    /// The library answers the 401; the hint is this CLI's addition.
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
        let client = Client::new(&base).unwrap();
        let err = client
            .session()
            .await
            .api_ctx(ctx("fetching", "reading"))
            .expect_err("401 fails");
        let message = format!("{err:#}");
        assert!(message.contains("UNAUTHENTICATED"), "{message}");
        assert!(message.contains("no credentials"), "{message}");
        assert!(message.contains("COPPICE_TOKEN"), "{message}");
        assert!(message.contains("open mode"), "{message}");
    }

    /// A server error that is not a 401 keeps the library's wording and gains
    /// nothing — the hint is specific to a missing credential.
    #[tokio::test]
    async fn a_non_401_error_carries_no_token_hint() {
        let router = Router::new().route(
            "/api/v1/session",
            get(|| async {
                (
                    axum::http::StatusCode::NOT_FOUND,
                    axum::Json(error_body("NOT_FOUND", "no such thing")),
                )
            }),
        );
        let base = spawn(router).await;
        let err = Client::new(&base)
            .unwrap()
            .session()
            .await
            .api_ctx(ctx("fetching", "reading"))
            .expect_err("404 fails");
        let message = format!("{err:#}");
        assert_eq!(message, "api error (NOT_FOUND): no such thing");
    }

    /// A transport failure reads as the `sending` half of its [`Ctx`]; a body
    /// that is not the promised shape reads as the `reading` half.
    #[tokio::test]
    async fn the_two_context_halves_name_the_two_failures() {
        // Nothing listens on port 1, so the send itself fails.
        let err = Client::new("http://127.0.0.1:1")
            .unwrap()
            .session()
            .await
            .api_ctx(ctx("fetching the session", "reading the session"))
            .expect_err("an unreachable coordinator fails");
        assert!(
            format!("{err:#}").starts_with("fetching the session"),
            "{err:#}"
        );

        let router = Router::new().route("/api/v1/session", get(|| async { "not json at all" }));
        let base = spawn(router).await;
        let err = Client::new(&base)
            .unwrap()
            .session()
            .await
            .api_ctx(ctx("fetching the session", "reading the session"))
            .expect_err("a non-JSON 200 fails");
        assert!(
            format!("{err:#}").starts_with("reading the session"),
            "{err:#}"
        );
    }

    /// `--token`/`COPPICE_TOKEN`, when set and non-empty, reaches the client;
    /// an empty one does not. (The header itself is the library's contract and
    /// is tested there; this pins the flag-to-client wiring.)
    #[test]
    fn the_connection_flags_build_a_client() {
        let connection = ApiConnection {
            api: "http://h:7070/api/v1".to_string(),
            token: Some("secret-token".to_string()),
        };
        let client = connection.client().unwrap();
        assert_eq!(client.base_url(), "http://h:7070");
        assert!(client.has_token());

        let empty = ApiConnection {
            api: DEFAULT_API_BASE.to_string(),
            token: Some(String::new()),
        };
        assert!(!empty.client().unwrap().has_token());
    }

    /// The default base and the default port are two literals that must name
    /// the same endpoint — `clap`'s `default_value` needs a `&'static str`, so
    /// the base cannot be built from the port at compile time. Both now come
    /// from the client library; this pins that they still agree.
    #[test]
    fn the_default_api_base_names_the_default_api_port() {
        assert_eq!(
            DEFAULT_API_BASE,
            format!("http://127.0.0.1:{DEFAULT_API_PORT}")
        );
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
