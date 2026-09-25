//! `coppice quota`: the quota-entity read verbs plus the `configure` upsert.
//!
//! The three verbs answer the operator's usual questions in ascending order
//! of depth: `list` is "what entities exist and how are they doing", `show`
//! is "tell me everything about one entity — its ancestry, its children, its
//! subtree stats", and `configure` is the one write, the `ConfigureQuotaEntity`
//! create-or-update upsert (no delete in v1, matching the wire contract).
//!
//! As with `coppice job` and `coppice cluster`, every wire shape is a
//! [`coppice_client`] type — nothing here redefines the `/api/v1`
//! contract the web UI is built on. The one thing this module owns is the
//! *entity file*: a single-entity TOML description accepted by
//! `quota configure --file` (`path`, `quota`, optional `id`).
//!
//! Since ADR 0045, `configure` takes a **path**, not an id: `quota configure
//! <PATH> --quota-ucu N [--entity <id>]`. The path is resolved against the
//! server's read view first (`GET /quota-entities/{path}`), and the
//! resolution decides the upsert (see [`build_configure_request`]):
//!
//! - the path already names an entity: that entity's quota is updated, its
//!   name and parent restated unchanged. An `--entity <id>` naming a
//!   *different* entity is refused here, before anything is sent: an
//!   entity's name and parent are fixed at creation, so there is no rename
//!   or move that could put `id` at a path someone else holds;
//! - the path names nothing: an entity is created under the path's parent
//!   (which must already exist) with `--entity <id>` if given, else a freshly
//!   minted id.
//!
//! Re-running the command is always safe: once a path names an entity it
//! names it for good, so a retry after an unknown outcome re-resolves to the
//! entity the first attempt created and updates it instead of minting a
//! second. If another caller creates the same path between this command's
//! lookup and its write, the server refuses the create
//! (`QuotaEntityNameTaken`, a 409); the command then re-resolves once and
//! updates the entity that won — by design, since the path is what both
//! callers meant.
//!
//! The `--file` document is deliberately spelled the same way as one
//! `[[quota_entity]]` entry in the coordinator's formation-policy TOML
//! (`coppice_coordinator::policy::QuotaEntitySpec`: `path`, `quota`, optional
//! `id`), so an operator only ever learns one quota-entity key vocabulary,
//! not two — even though this command's own flags stay `--quota-ucu` and
//! `--entity` for backward continuity with the direct-flag form, and this
//! file is still a path-first single-entity upsert, retried by re-resolving
//! its path as above, unlike the formation policy's whole-tree document.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

use coppice_client::{
    paths, Client, ConfigureQuotaEntityRequest, ErrorCode, GetQuotaEntityResponse, QuotaEntityId,
    QuotaEntityNode, QuotaEntityPath,
};

use crate::client::{ctx, print_json, render_table, ApiConnection, ApiResultExt};
use crate::cluster::{indent, phase_label};

// ---------------------------------------------------------------------------
// CLI surface
// ---------------------------------------------------------------------------

/// `coppice quota` argument group. `--api` is global, matching `coppice job`
/// and `coppice cluster`.
#[derive(Debug, clap::Args)]
pub struct QuotaArgs {
    #[command(flatten)]
    connection: ApiConnection,

    #[command(subcommand)]
    pub command: QuotaCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum QuotaCommand {
    /// List every quota entity as one flat table.
    List {
        /// Print the server's JSON response instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Show one quota entity: its own figures, ancestry chain, direct
    /// children, and subtree stats.
    Show {
        /// Entity id or path (ADR 0045).
        entity: coppice_client::QuotaEntityRef,
        /// Print the server's JSON response instead of the summary.
        #[arg(long)]
        json: bool,
    },
    /// Create or update a quota entity at a path (the `ConfigureQuotaEntity`
    /// upsert; there is no delete in v1). Either give `PATH` and the direct
    /// flags, or point `--file` at a single-entity TOML document — the two
    /// are mutually exclusive. See the module docs for the resolution rules.
    Configure {
        /// The entity's path (ADR 0045), e.g. `acme/eng/platform`. Resolved
        /// against the server's current tree to decide the upsert. Conflicts
        /// with `--file`, which carries its own `path` key instead.
        #[arg(conflicts_with = "file")]
        path: Option<QuotaEntityPath>,
        /// A single-entity TOML file (see the module docs for the schema).
        /// Conflicts with `PATH` and every direct flag below.
        #[arg(long, conflicts_with_all = ["entity", "quota_ucu"])]
        file: Option<PathBuf>,
        /// The id to create the entity with when nothing lives at `PATH` yet
        /// (omit to mint one). When something does, it must be this id —
        /// entities cannot be renamed or moved onto a path another holds.
        #[arg(long, conflicts_with = "file")]
        entity: Option<QuotaEntityId>,
        /// Soft quota, as a stock in µCU (ADR 0019). Required unless
        /// `--file` is given.
        #[arg(long, conflicts_with = "file")]
        quota_ucu: Option<u64>,
        /// Print the server's JSON response instead of the summary line.
        #[arg(long)]
        json: bool,
    },
}

/// Run the selected `coppice quota` verb.
pub async fn run(args: QuotaArgs) -> Result<()> {
    let client = args.connection.client()?;
    match args.command {
        QuotaCommand::List { json } => list(&client, json).await,
        QuotaCommand::Show { entity, json } => show(&client, entity, json).await,
        QuotaCommand::Configure {
            path,
            file,
            entity,
            quota_ucu,
            json,
        } => configure(&client, file.as_deref(), path, entity, quota_ucu, json).await,
    }
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

/// `coppice quota list`: every quota entity, root and descendants together,
/// as one flat table (the tree shape is `show`'s job, not `list`'s).
async fn list(client: &Client, json: bool) -> Result<()> {
    if json {
        let body = client
            .get_value(paths::QUOTA_ENTITIES, &[])
            .await
            .api_ctx(ctx(
                "listing quota entities",
                "reading the quota entity list",
            ))?;
        print_json(&body.value);
        return Ok(());
    }
    let page = client.list_quota_entities().await.api_ctx(ctx(
        "listing quota entities",
        "reading the quota entity list",
    ))?;
    print!("{}", render_quota_list(&page.entities));
    Ok(())
}

/// The column headers shared by `quota list` and the `children:` table nested
/// under `quota show`, so both renders look like one program. The path is
/// the primary column (ADR 0045); the id rides alongside rather than `name`
/// and `parent`, which the path already carries.
const QUOTA_LIST_HEADERS: [&str; 8] = [
    "path",
    "id",
    "quota (uCU)",
    "usage (uCU)",
    "over quota",
    "penalty",
    "queued",
    "running",
];

/// Render a flat list of quota-entity nodes as an aligned table, or the
/// "empty" sentinel when there are none.
fn render_quota_list(entities: &[QuotaEntityNode]) -> String {
    if entities.is_empty() {
        return "(no quota entities)\n".to_string();
    }
    let rows: Vec<Vec<String>> = entities.iter().map(quota_node_row).collect();
    render_table(&QUOTA_LIST_HEADERS, &rows)
}

/// The row cells for one [`QuotaEntityNode`], in [`QUOTA_LIST_HEADERS`]
/// order.
fn quota_node_row(node: &QuotaEntityNode) -> Vec<String> {
    vec![
        node.path.clone(),
        node.id.to_string(),
        node.quota_ucu.to_string(),
        node.usage_ucu.to_string(),
        format_ratio(node.over_quota_ratio),
        format_ratio(node.penalty),
        node.queued_count.to_string(),
        node.running_count.to_string(),
    ]
}

/// Format a ratio (`over_quota_ratio` or `penalty`) to two decimals, or as
/// `unbounded` when it is not finite. The infinite case is real and reachable
/// — an entity with zero quota and nonzero usage is infinitely over — and it
/// arrives here as a wire `null` that the DTO reads back as `f64::INFINITY`;
/// this branch gives it a readable word instead of Rust's raw `inf`/`NaN`.
fn format_ratio(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.2}")
    } else {
        "unbounded".to_string()
    }
}

// ---------------------------------------------------------------------------
// show
// ---------------------------------------------------------------------------

/// `coppice quota show`: one entity's own figures, its ancestry, its direct
/// children, and its subtree stats.
async fn show(client: &Client, entity: coppice_client::QuotaEntityRef, json: bool) -> Result<()> {
    if json {
        let route = paths::quota_entity(entity);
        let body = client
            .get_value(&route, &[])
            .await
            .api_ctx(ctx("fetching quota entity", "reading quota entity detail"))?;
        print_json(&body.value);
        return Ok(());
    }
    let detail = client
        .quota_entity(entity)
        .await
        .api_ctx(ctx("fetching quota entity", "reading quota entity detail"))?;
    print!("{}", render_quota_detail(&detail));
    Ok(())
}

/// Render a `GetQuotaEntityResponse` as: a key/value block for the entity
/// itself, then a `chain:` section (ancestry, root first), then a
/// `children:` section (the same table `quota list` uses, indented), then a
/// `stats:` block. `usage_history` is never rendered — the field is always
/// empty (no usage-series sampler exists yet, see [`GetQuotaEntityResponse`])
/// — so a section for it would only ever show as absent noise.
fn render_quota_detail(detail: &GetQuotaEntityResponse) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let kv = |out: &mut String, key: &str, value: &str| {
        let _ = writeln!(out, "{key:<16}{value}");
    };

    let node = &detail.entity;
    kv(&mut out, "path", &node.path);
    kv(&mut out, "id", &node.id.to_string());
    kv(&mut out, "name", &node.name);
    kv(
        &mut out,
        "parent",
        &node
            .parent
            .map(|p| p.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );
    kv(&mut out, "quota", &format!("{} uCU", node.quota_ucu));
    kv(&mut out, "usage", &format!("{} uCU", node.usage_ucu));
    kv(&mut out, "over quota", &format_ratio(node.over_quota_ratio));
    kv(&mut out, "penalty", &format_ratio(node.penalty));
    kv(&mut out, "created", &node.created_at.to_string());
    kv(&mut out, "updated", &node.updated_at.to_string());
    kv(&mut out, "queued", &node.queued_count.to_string());
    kv(&mut out, "running", &node.running_count.to_string());

    let _ = writeln!(out);
    if detail.chain.is_empty() {
        out.push_str("chain: (none)\n");
    } else {
        out.push_str("chain:\n");
        for view in &detail.chain {
            let _ = writeln!(
                out,
                "  {} ({}) quota {} uCU, usage {} uCU",
                view.path, view.id, view.quota_ucu, view.usage_ucu
            );
        }
    }

    let _ = writeln!(out);
    if detail.children.is_empty() {
        out.push_str("children: (none)\n");
    } else {
        out.push_str("children:\n");
        let rows: Vec<Vec<String>> = detail.children.iter().map(quota_node_row).collect();
        out.push_str(&indent(&render_table(&QUOTA_LIST_HEADERS, &rows)));
    }

    let _ = writeln!(out);
    out.push_str("stats:\n");
    let stats = &detail.stats;
    let by_state: Vec<String> = stats
        .by_state
        .iter()
        .map(|(phase, count)| format!("{} {count}", phase_label(phase.clone())))
        .collect();
    let _ = writeln!(out, "  by phase       {}", by_state.join(", "));
    let _ = writeln!(
        out,
        "  oldest queued  {}",
        stats
            .oldest_queued_age
            .map(|d| format!("{}s", d.as_secs()))
            .unwrap_or_else(|| "(nothing queued)".to_string())
    );
    let _ = writeln!(
        out,
        "  burn rate      {} uCU/s",
        stats.burn_rate_ucu_per_second
    );
    // `charged_ucu_24h` is always null today: no charge ledger exists to
    // measure a trailing-24h total (a true-up settles against entity usage
    // and retains no per-window sum). Rendering it as "(not measured)" rather
    // than `0` matters — a real zero and an unmeasured figure are different
    // facts, and this field can never today distinguish them on its own.
    let _ = writeln!(
        out,
        "  charged (24h)  {}",
        stats
            .charged_ucu_24h
            .map(|v| format!("{v} uCU"))
            .unwrap_or_else(|| "(not measured)".to_string())
    );
    out
}

// ---------------------------------------------------------------------------
// configure
// ---------------------------------------------------------------------------

/// A single-entity TOML file for `quota configure --file` (ADR 0045).
///
/// Spelled deliberately the same way as one `[[quota_entity]]` entry in the
/// coordinator's formation-policy TOML
/// (`coppice_coordinator::policy::QuotaEntitySpec`: `path`, `quota`, optional
/// `id`), so an operator learns one quota-entity key vocabulary, not two —
/// even though this file is still a path-first single-entity upsert with its
/// own idempotency story, not the formation policy's whole-tree-by-id
/// document.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaEntityFile {
    /// The entity's path (ADR 0045), e.g. `acme/eng/platform`.
    pub path: QuotaEntityPath,
    /// The quota stock in µCU (ADR 0019).
    pub quota: u64,
    /// The id to create the entity with when nothing lives at `path` yet;
    /// when something does, it must be this id. Absent: update whatever
    /// already lives at `path`, or mint a fresh id there.
    #[serde(default)]
    pub id: Option<QuotaEntityId>,
}

impl QuotaEntityFile {
    /// Read and parse a single-entity TOML file, naming the file in every
    /// error so a typo'd key or a missing file both fail with the file path
    /// attached.
    fn load(path: &Path) -> Result<QuotaEntityFile> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading quota entity file {}", path.display()))?;
        toml::from_str(&raw)
            .with_context(|| format!("reading quota entity file {}", path.display()))
    }
}

/// What the server's read view said about the path being configured, ahead
/// of building the upsert request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathLookup {
    /// An entity already lives at the path, with this id.
    Found(QuotaEntityId),
    /// Nothing lives at the path (a 404 on `GET /quota-entities/{path}`).
    NotFound,
}

/// `coppice quota configure`: resolve `PATH` (or the file's `path`) against
/// the server, build the upsert request from the resolution (see
/// [`build_configure_request`]), POST it, and render the result.
async fn configure(
    client: &Client,
    file: Option<&Path>,
    path: Option<QuotaEntityPath>,
    entity: Option<QuotaEntityId>,
    quota_ucu: Option<u64>,
    json: bool,
) -> Result<()> {
    let (path, entity, quota_ucu) = match file {
        Some(file) => {
            let spec = QuotaEntityFile::load(file)?;
            (spec.path, spec.id, spec.quota)
        }
        None => {
            let path = path.context("PATH is required unless --file is given")?;
            let quota_ucu = quota_ucu.context("--quota-ucu is required unless --file is given")?;
            (path, entity, quota_ucu)
        }
    };

    let lookup = resolve_path(client, &path).await?;
    let (request, minted) = build_configure_request(&path, entity, lookup, quota_ucu)?;
    let outcome = match post_configure(client, &request, json).await {
        // A create refused as a conflict may have lost a race: another caller
        // created this path between the lookup and the write
        // (`QuotaEntityNameTaken`). The path names that entity for good, so
        // re-resolve once and, if it now exists, update it. Any other 409
        // (an unknown parent, a too-deep path) leaves the path unoccupied and
        // surfaces as it was.
        Err(e) if lookup == PathLookup::NotFound && e.code() == Some(&ErrorCode::Rejected) => {
            match resolve_path(client, &path).await? {
                PathLookup::NotFound => Err(e),
                found => {
                    let (request, _) = build_configure_request(&path, entity, found, quota_ucu)?;
                    eprintln!(
                        "note: {path} was created concurrently by another caller; updating that \
                         entity ({})",
                        request.entity
                    );
                    post_configure(client, &request, json).await
                }
            }
        }
        Ok(outcome) => {
            if minted {
                // The id need not be captured for a retry: re-running the
                // command re-resolves the path, which now names this entity
                // for good, and updates it.
                eprintln!(
                    "note: created {path} with the newly minted id {}; re-running this command \
                     is safe — the path now permanently names this entity, so a retry updates \
                     it rather than creating another",
                    request.entity
                );
            }
            Ok(outcome)
        }
        Err(e) => Err(e),
    };
    match outcome.api_ctx(ctx(
        "configuring quota entity",
        "reading configure response",
    ))? {
        ConfigureOutcome::Json(body) => print_json(&body),
        ConfigureOutcome::Typed(response) => println!(
            "configured {} ({}) (log index {})",
            response.path, response.entity, response.log_index
        ),
    }
    Ok(())
}

/// What one POST of the upsert answered: the raw JSON for `--json`, the typed
/// response otherwise.
enum ConfigureOutcome {
    Json(serde_json::Value),
    Typed(coppice_client::ConfigureQuotaEntityResponse),
}

/// POST the upsert, keeping the client error unwrapped so the caller can
/// inspect its code.
async fn post_configure(
    client: &Client,
    request: &ConfigureQuotaEntityRequest,
    json: bool,
) -> Result<ConfigureOutcome, coppice_client::Error> {
    if json {
        client
            .post_value(paths::QUOTA_ENTITIES, request)
            .await
            .map(ConfigureOutcome::Json)
    } else {
        client
            .configure_quota_entity(request)
            .await
            .map(ConfigureOutcome::Typed)
    }
}

/// `GET /quota-entities/{path}` and turn its outcome into a [`PathLookup`], so
/// the resolution logic in [`build_configure_request`] never has to see an
/// HTTP status. Any error other than a clean 404 (unreachable server, a
/// permission failure, …) still propagates as a real error — only "nothing
/// lives there" collapses to [`PathLookup::NotFound`].
async fn resolve_path(client: &Client, path: &QuotaEntityPath) -> Result<PathLookup> {
    match client.quota_entity(path.clone()).await {
        Ok(detail) => Ok(PathLookup::Found(detail.entity.id)),
        Err(coppice_client::Error::Api {
            code: ErrorCode::NotFound,
            ..
        }) => Ok(PathLookup::NotFound),
        Err(e) => Err(e).api_ctx(ctx(
            "resolving the quota entity path",
            "reading quota entity detail",
        )),
    }
}

/// Build the wire request from `PATH`'s resolution: update whatever already
/// lives at `PATH`, or create there with `--entity` if given, else a freshly
/// minted id. The request always states `PATH`'s own leaf and parent — for an
/// update those are the entity's stored name and parent, which never change
/// (ADR 0045). Returns whether the id was freshly minted here, so the caller
/// can say so.
///
/// An `--entity` naming a different id than the one already at `PATH` is an
/// error: entities cannot be renamed or moved, so the request could only be
/// refused by the server.
///
/// A pure function (no I/O), so every case — update in place, a conflicting
/// `--entity`, create under an existing parent, create at the root — is a
/// plain unit test over [`PathLookup`] rather than a fixture server.
fn build_configure_request(
    path: &QuotaEntityPath,
    entity: Option<QuotaEntityId>,
    lookup: PathLookup,
    quota_ucu: u64,
) -> Result<(ConfigureQuotaEntityRequest, bool)> {
    let (id, minted) = match (entity, lookup) {
        (Some(given), PathLookup::Found(holder)) if given != holder => anyhow::bail!(
            "{path} is already held by {holder}, not {given}; an entity's name and parent are \
             fixed at creation, so {given} cannot be renamed or moved there — drop --entity to \
             update {holder}, or pick a path nothing holds yet to create {given}"
        ),
        (_, PathLookup::Found(holder)) => (holder, false),
        (Some(given), PathLookup::NotFound) => (given, false),
        (None, PathLookup::NotFound) => (QuotaEntityId::new(), true),
    };
    let mut request = ConfigureQuotaEntityRequest::new(id, path.leaf(), quota_ucu);
    if let Some(parent) = path.parent() {
        request = request.with_parent(parent);
    }
    Ok((request, minted))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::io::Write as _;
    use std::sync::{Arc, Mutex};

    use axum::extract::{Path as AxumPath, State};
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use tempfile::NamedTempFile;

    use coppice_api::http::dto;
    use coppice_client::ListQuotaEntitiesResponse;
    use coppice_core::id::QuotaEntityId as CoreQuotaEntityId;
    use coppice_core::time::Timestamp;

    use crate::testsupport::{error_body, leader_hint, spawn};

    /// Round-trip a server-typed (`coppice_api::http::dto`) fixture through
    /// JSON into the `coppice_client` type the CLI actually renders. The
    /// server type is the fixture that pins the wire contract; the client
    /// type is what `render_quota_list`/`render_quota_detail` take — so this
    /// conversion, not a hand-written assertion, *is* the cross-check that
    /// the two crates still agree on the shape.
    fn to_client<S: serde::Serialize, C: serde::de::DeserializeOwned>(value: S) -> C {
        serde_json::from_value(serde_json::to_value(value).unwrap())
            .expect("the client type decodes the server type's own output")
    }

    /// The client-typed entity id, from a small integer, for calling the verb
    /// functions and for assertions.
    fn quota_id(n: u8) -> QuotaEntityId {
        format!("quota-00000000-0000-0000-0000-{n:012}")
            .parse()
            .unwrap()
    }

    /// The same id, server-typed, for `dto::` fixtures.
    fn dto_quota_id(n: u8) -> CoreQuotaEntityId {
        format!("quota-00000000-0000-0000-0000-{n:012}")
            .parse()
            .unwrap()
    }

    fn ts(micros: i64) -> Timestamp {
        Timestamp::from_micros(micros).unwrap()
    }

    fn sample_node(
        id: CoreQuotaEntityId,
        parent: Option<CoreQuotaEntityId>,
    ) -> dto::QuotaEntityNode {
        dto::QuotaEntityNode {
            id,
            name: "team-a".to_string(),
            path: "acme/team-a".to_string(),
            parent,
            origin: dto::QuotaEntityOrigin::Configured,
            principal: None,
            quota_ucu: 1000,
            usage_ucu: 250,
            over_quota_ratio: 0.25,
            penalty: 1.0,
            created_at: ts(1_000_000),
            updated_at: ts(2_000_000),
            queued_count: 2,
            running_count: 1,
        }
    }

    fn sample_view(
        id: CoreQuotaEntityId,
        parent: Option<CoreQuotaEntityId>,
    ) -> dto::QuotaEntityView {
        dto::QuotaEntityView {
            id,
            name: "root".to_string(),
            path: "acme".to_string(),
            parent,
            quota_ucu: 5000,
            usage_ucu: 100,
            over_quota_ratio: 0.02,
            penalty: 1.0,
        }
    }

    fn sample_stats() -> dto::QuotaEntityStats {
        let mut by_state = BTreeMap::new();
        for phase in dto::JobPhase::ALL {
            by_state.insert(phase, 0);
        }
        by_state.insert(dto::JobPhase::Queued, 2);
        dto::QuotaEntityStats {
            by_state,
            oldest_queued_age_seconds: Some(30),
            burn_rate_ucu_per_second: 5,
            charged_ucu_24h: None,
            usage_history: Vec::new(),
        }
    }

    /// The wire body for an entity that is infinitely over quota (zero quota,
    /// nonzero usage): the API renders those non-finite floats as JSON
    /// `null`, so it is written literally here rather than via a DTO, to pin
    /// the shape the CLI actually has to decode.
    fn unbounded_node_json(id: QuotaEntityId) -> serde_json::Value {
        serde_json::json!({
            "id": id.to_string(),
            "name": "starved",
            "path": "starved",
            "parent": null,
            "origin": "configured",
            "principal": null,
            "quota_ucu": 0,
            "usage_ucu": 42,
            "over_quota_ratio": null,
            "penalty": null,
            "created_at": "1970-01-01T00:00:01.000000Z",
            "updated_at": "1970-01-01T00:00:02.000000Z",
            "queued_count": 1,
            "running_count": 0,
        })
    }

    fn unbounded_view_json(id: QuotaEntityId) -> serde_json::Value {
        serde_json::json!({
            "id": id.to_string(),
            "name": "starved",
            "path": "starved",
            "parent": null,
            "quota_ucu": 0,
            "usage_ucu": 42,
            "over_quota_ratio": null,
            "penalty": null,
        })
    }

    fn client(base: &str) -> Client {
        Client::new(base).unwrap()
    }

    // -----------------------------------------------------------------
    // list
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn list_decodes_the_real_response() {
        let entities = vec![sample_node(dto_quota_id(1), None)];
        let body = serde_json::to_value(dto::ListQuotaEntitiesResponse { entities }).unwrap();
        let router = Router::new().route(
            "/api/v1/quota-entities",
            get(move || {
                let body = body.clone();
                async move { Json(body) }
            }),
        );
        let base = spawn(router).await;
        list(&client(&base), false).await.expect("list succeeds");
    }

    /// A zero-quota entity with nonzero usage is infinitely over quota, and
    /// the API serves that as JSON `null`. `quota list` must decode it (not
    /// fail on the null) and render it as `unbounded`.
    #[tokio::test]
    async fn list_accepts_null_over_quota_ratio_and_renders_unbounded() {
        let id = quota_id(12);
        let body = serde_json::json!({ "entities": [unbounded_node_json(id)] });
        let router = Router::new().route(
            "/api/v1/quota-entities",
            get({
                let body = body.clone();
                move || {
                    let body = body.clone();
                    async move { Json(body) }
                }
            }),
        );
        let base = spawn(router).await;
        list(&client(&base), false)
            .await
            .expect("list decodes null over_quota_ratio");

        // The same body, through the same decode the renderer sees.
        let page: ListQuotaEntitiesResponse = serde_json::from_value(body).unwrap();
        assert_eq!(page.entities[0].over_quota_ratio, f64::INFINITY);
        assert_eq!(page.entities[0].penalty, f64::INFINITY);
        let rendered = render_quota_list(&page.entities);
        assert!(rendered.contains("unbounded"), "{rendered}");
        assert!(!rendered.contains("inf"), "{rendered}");
    }

    #[tokio::test]
    async fn show_accepts_null_over_quota_ratio_and_renders_unbounded() {
        let id = quota_id(13);
        let body = serde_json::json!({
            "entity": unbounded_node_json(id),
            "chain": [unbounded_view_json(id)],
            "children": [unbounded_node_json(quota_id(14))],
            "stats": serde_json::to_value(sample_stats()).unwrap(),
        });
        let router = Router::new().route(
            "/api/v1/quota-entities/:entity",
            get({
                let body = body.clone();
                move |AxumPath(_entity): AxumPath<String>| {
                    let body = body.clone();
                    async move { Json(body) }
                }
            }),
        );
        let base = spawn(router).await;
        show(&client(&base), id.into(), false)
            .await
            .expect("show decodes null over_quota_ratio");

        let detail: GetQuotaEntityResponse = serde_json::from_value(body).unwrap();
        assert_eq!(detail.entity.over_quota_ratio, f64::INFINITY);
        assert_eq!(detail.chain[0].penalty, f64::INFINITY);
        let rendered = render_quota_detail(&detail);
        assert!(rendered.contains("over quota      unbounded"), "{rendered}");
        assert!(rendered.contains("penalty         unbounded"), "{rendered}");
        assert!(!rendered.contains("inf"), "{rendered}");
    }

    #[test]
    fn render_quota_list_shows_path_id_and_quota() {
        let node = sample_node(dto_quota_id(1), None);
        let client_node: QuotaEntityNode = to_client(node.clone());
        let rendered = render_quota_list(std::slice::from_ref(&client_node));
        assert!(rendered.contains(&node.id.to_string()), "{rendered}");
        assert!(rendered.contains("acme/team-a"), "{rendered}");
        assert!(rendered.contains("1000"), "{rendered}");
        // NAME and PARENT are dropped as columns: the path already carries
        // that information (ADR 0045).
        let header = rendered.lines().next().unwrap_or_default();
        assert!(!header.contains("name"), "{header}");
        assert!(!header.contains("parent"), "{header}");
    }

    #[test]
    fn render_quota_list_reports_empty() {
        assert_eq!(render_quota_list(&[]), "(no quota entities)\n");
    }

    #[tokio::test]
    async fn list_surfaces_an_error_body() {
        let router = Router::new().route(
            "/api/v1/quota-entities",
            get(|| async {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(error_body("INTERNAL", "boom")),
                )
            }),
        );
        let base = spawn(router).await;
        let err = list(&client(&base), false).await.expect_err("list fails");
        let message = format!("{err:#}");
        assert!(message.contains("INTERNAL"), "{message}");
        assert!(message.contains("boom"), "{message}");
    }

    // -----------------------------------------------------------------
    // show
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn show_fetches_the_requested_entity() {
        let id = quota_id(2);
        let response = dto::GetQuotaEntityResponse {
            entity: sample_node(dto_quota_id(2), None),
            chain: vec![sample_view(dto_quota_id(9), None)],
            children: vec![sample_node(dto_quota_id(3), Some(dto_quota_id(2)))],
            stats: sample_stats(),
        };
        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let router = {
            let captured = captured.clone();
            Router::new().route(
                "/api/v1/quota-entities/:entity",
                get(move |AxumPath(entity): AxumPath<String>| {
                    let captured = captured.clone();
                    let response = response.clone();
                    async move {
                        captured.lock().unwrap().replace(entity);
                        Json(serde_json::to_value(response).unwrap())
                    }
                }),
            )
        };
        let base = spawn(router).await;

        show(&client(&base), id.into(), false)
            .await
            .expect("show succeeds");

        assert_eq!(
            captured.lock().unwrap().as_deref(),
            Some(id.to_string().as_str())
        );
    }

    #[test]
    fn render_quota_detail_reports_charged_ucu_as_not_measured() {
        let detail = dto::GetQuotaEntityResponse {
            entity: sample_node(dto_quota_id(1), None),
            chain: Vec::new(),
            children: Vec::new(),
            stats: sample_stats(),
        };
        let rendered = render_quota_detail(&to_client(detail));
        assert!(rendered.contains("(not measured)"), "{rendered}");
        assert!(!rendered.contains("charged (24h)  0"), "{rendered}");
    }

    #[tokio::test]
    async fn show_surfaces_not_found() {
        let router = Router::new().route(
            "/api/v1/quota-entities/:entity",
            get(|| async {
                (
                    StatusCode::NOT_FOUND,
                    Json(error_body("NOT_FOUND", "quota entity quota-x not found")),
                )
            }),
        );
        let base = spawn(router).await;
        let err = show(&client(&base), quota_id(4).into(), false)
            .await
            .expect_err("show fails");
        let message = format!("{err:#}");
        assert!(message.contains("NOT_FOUND"), "{message}");
    }

    // -----------------------------------------------------------------
    // build_configure_request (pure resolution logic)
    // -----------------------------------------------------------------

    fn quota_path(s: &str) -> QuotaEntityPath {
        s.parse().unwrap()
    }

    /// Path already exists, no `--entity`: update it in place.
    #[test]
    fn build_request_updates_the_existing_entity_in_place() {
        let existing = quota_id(1);
        let path = quota_path("acme/eng/platform");
        let (request, minted) =
            build_configure_request(&path, None, PathLookup::Found(existing), 500).unwrap();
        assert!(!minted);
        assert_eq!(request.entity, existing);
        assert_eq!(request.name, "platform");
        assert_eq!(request.quota_ucu, 500);
        assert_eq!(
            request.parent,
            Some(coppice_client::QuotaEntityRef::Path(quota_path("acme/eng")))
        );
    }

    /// `--entity <id>` naming the entity already at `PATH` is a plain update.
    #[test]
    fn build_request_with_the_holders_own_id_updates_it() {
        let holder = quota_id(2);
        let path = quota_path("acme/eng/platform");
        let (request, minted) =
            build_configure_request(&path, Some(holder), PathLookup::Found(holder), 500).unwrap();
        assert!(!minted);
        assert_eq!(request.entity, holder);
        assert_eq!(request.name, "platform");
    }

    /// `--entity <id>` naming a *different* entity than the one at `PATH` is
    /// refused client-side: there is no rename or move (ADR 0045), so nothing
    /// is sent.
    #[test]
    fn build_request_refuses_an_entity_other_than_the_paths_holder() {
        let given = quota_id(2);
        let holder = quota_id(3);
        let path = quota_path("acme/eng/platform");
        let err = build_configure_request(&path, Some(given), PathLookup::Found(holder), 500)
            .expect_err("a different id cannot take a held path");
        let message = format!("{err:#}");
        assert!(message.contains("acme/eng/platform"), "{message}");
        assert!(message.contains(&holder.to_string()), "{message}");
        assert!(message.contains(&given.to_string()), "{message}");
    }

    /// `--entity <id>` on an unoccupied path creates with that id.
    #[test]
    fn build_request_creates_with_the_given_id_on_an_unoccupied_path() {
        let given = quota_id(4);
        let path = quota_path("acme/eng/platform");
        let (request, minted) =
            build_configure_request(&path, Some(given), PathLookup::NotFound, 500).unwrap();
        assert!(!minted);
        assert_eq!(request.entity, given);
    }

    /// Path does not exist, no `--entity`: mint a fresh id under the path's
    /// parent.
    #[test]
    fn build_request_mints_a_fresh_id_when_the_path_is_unoccupied() {
        let path = quota_path("acme/eng/platform");
        let (request, minted) =
            build_configure_request(&path, None, PathLookup::NotFound, 500).unwrap();
        assert!(minted);
        assert_eq!(request.name, "platform");
        assert_eq!(
            request.parent,
            Some(coppice_client::QuotaEntityRef::Path(quota_path("acme/eng")))
        );
    }

    /// A single-segment path is a root: no parent at all, existing or fresh.
    #[test]
    fn build_request_at_the_root_has_no_parent() {
        let path = quota_path("acme");
        let (request, minted) =
            build_configure_request(&path, None, PathLookup::NotFound, 500).unwrap();
        assert!(minted);
        assert_eq!(request.name, "acme");
        assert!(request.parent.is_none());
    }

    // -----------------------------------------------------------------
    // configure (end to end: resolution + POST)
    // -----------------------------------------------------------------

    /// A router serving both `GET /quota-entities/{path}` (the resolution)
    /// and `POST /quota-entities` (the upsert), so `configure` can be driven
    /// end to end. `lookup` is the resolution response: `Some` for a 200
    /// body, `None` for a 404.
    fn configure_router(
        lookup: Option<dto::GetQuotaEntityResponse>,
        captured: Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>>,
        response_log_index: u64,
    ) -> Router {
        Router::new()
            .route(
                "/api/v1/quota-entities/:entity",
                get(move |AxumPath(_entity): AxumPath<String>| {
                    let lookup = lookup.clone();
                    async move {
                        use axum::response::IntoResponse;
                        match lookup {
                            Some(body) => Json(serde_json::to_value(body).unwrap()).into_response(),
                            None => (
                                StatusCode::NOT_FOUND,
                                Json(error_body("NOT_FOUND", "no entity at that path")),
                            )
                                .into_response(),
                        }
                    }
                }),
            )
            .route(
                "/api/v1/quota-entities",
                post(
                    move |State(captured): State<
                        Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>>,
                    >,
                          Json(req): Json<dto::ConfigureQuotaEntityRequest>| async move {
                        let response = dto::ConfigureQuotaEntityResponse {
                            entity: req.entity,
                            path: format!("resolved/{}", req.name),
                            log_index: response_log_index,
                        };
                        captured.lock().unwrap().push(req);
                        Json(serde_json::to_value(response).unwrap())
                    },
                ),
            )
            .with_state(captured)
    }

    fn found_response(
        id: CoreQuotaEntityId,
        parent: Option<CoreQuotaEntityId>,
    ) -> Option<dto::GetQuotaEntityResponse> {
        Some(dto::GetQuotaEntityResponse {
            entity: sample_node(id, parent),
            chain: Vec::new(),
            children: Vec::new(),
            stats: sample_stats(),
        })
    }

    #[tokio::test]
    async fn configure_from_flags_updates_the_resolved_entity() {
        let existing = dto_quota_id(5);
        let captured: Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>> =
            Arc::new(Mutex::new(Vec::new()));
        let router = configure_router(found_response(existing, None), captured.clone(), 7);
        let base = spawn(router).await;

        configure(
            &client(&base),
            None,
            Some(quota_path("acme/team-b")),
            None,
            Some(500),
            false,
        )
        .await
        .expect("configure succeeds");

        let received = captured.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].entity, existing);
        assert_eq!(received[0].name, "team-b");
        assert_eq!(received[0].quota_ucu, 500);
    }

    #[tokio::test]
    async fn configure_from_flags_mints_on_an_unoccupied_path() {
        let captured: Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>> =
            Arc::new(Mutex::new(Vec::new()));
        let router = configure_router(None, captured.clone(), 8);
        let base = spawn(router).await;

        configure(
            &client(&base),
            None,
            Some(quota_path("acme/team-new")),
            None,
            Some(500),
            false,
        )
        .await
        .expect("configure succeeds");

        let received = captured.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].name, "team-new");
    }

    #[tokio::test]
    async fn configure_from_file_posts_the_dto() {
        let captured: Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>> =
            Arc::new(Mutex::new(Vec::new()));
        let router = configure_router(None, captured.clone(), 9);
        let base = spawn(router).await;

        let entity = quota_id(20);
        let toml_body = format!("path = \"acme/team-c\"\nquota = 750\nid = \"{entity}\"\n");
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml_body.as_bytes()).unwrap();

        configure(&client(&base), Some(file.path()), None, None, None, false)
            .await
            .expect("configure --file succeeds");

        let received = captured.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].entity.to_string(), entity.to_string());
        assert_eq!(received[0].name, "team-c");
        assert_eq!(received[0].quota_ucu, 750);
    }

    /// `--entity` naming a different id than the path's holder fails before
    /// anything is POSTed.
    #[tokio::test]
    async fn configure_refuses_an_entity_other_than_the_paths_holder() {
        let holder = dto_quota_id(5);
        let captured: Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>> =
            Arc::new(Mutex::new(Vec::new()));
        let router = configure_router(found_response(holder, None), captured.clone(), 7);
        let base = spawn(router).await;

        let err = configure(
            &client(&base),
            None,
            Some(quota_path("acme/team-b")),
            Some(quota_id(6)),
            Some(500),
            false,
        )
        .await
        .expect_err("no rename onto a held path");
        assert!(format!("{err:#}").contains("cannot be renamed or moved"));
        assert!(captured.lock().unwrap().is_empty(), "nothing was sent");
    }

    /// Losing a create race to another caller (`QuotaEntityNameTaken`, a
    /// 409) re-resolves the path once and updates the winner's entity.
    #[tokio::test]
    async fn configure_falls_through_to_an_update_after_losing_a_create_race() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let winner = dto_quota_id(9);
        let created = Arc::new(AtomicBool::new(false));
        let captured: Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>> =
            Arc::new(Mutex::new(Vec::new()));
        let lookup = {
            let created = Arc::clone(&created);
            move |AxumPath(_entity): AxumPath<String>| {
                let created = Arc::clone(&created);
                async move {
                    use axum::response::IntoResponse;
                    if created.load(Ordering::SeqCst) {
                        Json(serde_json::to_value(found_response(winner, None).unwrap()).unwrap())
                            .into_response()
                    } else {
                        (
                            StatusCode::NOT_FOUND,
                            Json(error_body("NOT_FOUND", "no entity at that path")),
                        )
                            .into_response()
                    }
                }
            }
        };
        let upsert = {
            let created = Arc::clone(&created);
            let captured = Arc::clone(&captured);
            move |Json(req): Json<dto::ConfigureQuotaEntityRequest>| {
                let created = Arc::clone(&created);
                let captured = Arc::clone(&captured);
                async move {
                    use axum::response::IntoResponse;
                    captured.lock().unwrap().push(req.clone());
                    if req.entity != winner {
                        // The other caller's create lands first.
                        created.store(true, Ordering::SeqCst);
                        return (
                            StatusCode::CONFLICT,
                            Json(error_body(
                                "REJECTED",
                                "quota entity name \"team-new\" is already taken",
                            )),
                        )
                            .into_response();
                    }
                    Json(
                        serde_json::to_value(dto::ConfigureQuotaEntityResponse {
                            entity: req.entity,
                            path: "acme/team-new".into(),
                            log_index: 12,
                        })
                        .unwrap(),
                    )
                    .into_response()
                }
            }
        };
        let router = Router::new()
            .route("/api/v1/quota-entities/:entity", get(lookup))
            .route("/api/v1/quota-entities", post(upsert));
        let base = spawn(router).await;

        configure(
            &client(&base),
            None,
            Some(quota_path("acme/team-new")),
            None,
            Some(500),
            false,
        )
        .await
        .expect("the retry updates the winner");

        let received = captured.lock().unwrap();
        assert_eq!(received.len(), 2, "one refused create, one update");
        assert_ne!(received[0].entity, winner);
        assert_eq!(received[1].entity, winner);
        assert_eq!(received[1].name, "team-new");
        assert_eq!(received[1].quota_ucu, 500);
    }

    #[tokio::test]
    async fn configure_surfaces_the_leader_hint_on_421() {
        let router = Router::new()
            .route(
                "/api/v1/quota-entities/:entity",
                get(|AxumPath(_entity): AxumPath<String>| async {
                    (
                        StatusCode::NOT_FOUND,
                        Json(error_body("NOT_FOUND", "no entity at that path")),
                    )
                }),
            )
            .route(
                "/api/v1/quota-entities",
                post(|| async {
                    (
                        StatusCode::MISDIRECTED_REQUEST,
                        leader_hint("10.0.0.3:7070"),
                        Json(error_body("NOT_LEADER", "not the leader")),
                    )
                }),
            );
        let base = spawn(router).await;

        let err = configure(
            &client(&base),
            None,
            Some(quota_path("acme/x")),
            None,
            Some(1),
            false,
        )
        .await
        .expect_err("configure fails");
        let message = format!("{err:#}");
        assert!(message.contains("NOT_LEADER"), "{message}");
        assert!(message.contains("10.0.0.3:7070"), "{message}");
    }

    #[test]
    fn quota_entity_file_rejects_unknown_keys() {
        let toml = "path = \"acme/x\"\nquota = 1\nbogus = 2\n";
        let result: Result<QuotaEntityFile, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    #[test]
    fn quota_entity_file_parses_the_minimal_form() {
        let toml = "path = \"acme/x\"\nquota = 1\n";
        let file: QuotaEntityFile = toml::from_str(toml).expect("parses");
        assert_eq!(file.path.as_str(), "acme/x");
        assert_eq!(file.quota, 1);
        assert!(file.id.is_none());
    }
}
