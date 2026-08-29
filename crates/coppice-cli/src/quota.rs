//! `coppice quota`: the quota-entity read verbs plus the `configure` upsert.
//!
//! The three verbs answer the operator's usual questions in ascending order
//! of depth: `list` is "what entities exist and how are they doing", `show`
//! is "tell me everything about one entity — its ancestry, its children, its
//! subtree stats", and `configure` is the one write, the `ConfigureQuotaEntity`
//! create-or-update upsert (no delete in v1, matching the wire contract).
//!
//! As with `coppice job` and `coppice cluster`, every wire shape is a
//! [`coppice_api::http::dto`] type — nothing here redefines the `/api/v1`
//! contract the web UI is built on. The one thing this module owns is the
//! *entity file*: a single-entity TOML description accepted by
//! `quota configure --file`, deliberately spelled the same as one
//! `[[quota_entity]]` entry in the coordinator's formation-policy TOML
//! (`coppice_coordinator::policy::QuotaEntitySpec`), so an operator who has
//! already written a formation policy needs to learn no second vocabulary.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

use coppice_api::http::dto;
use coppice_core::id::QuotaEntityId;

use crate::client::{ctx, print_json, render_table, ApiClient, DEFAULT_API_BASE};
use crate::cluster::{indent, phase_label};

// ---------------------------------------------------------------------------
// CLI surface
// ---------------------------------------------------------------------------

/// `coppice quota` argument group. `--api` is global, matching `coppice job`
/// and `coppice cluster`.
#[derive(Debug, clap::Args)]
pub struct QuotaArgs {
    /// Base URL of the coordinator's client API. Accepts either a bare base
    /// (`http://host:7070`) or one already ending in `/api/v1`.
    #[arg(
        long,
        global = true,
        env = "COPPICE_API",
        default_value = DEFAULT_API_BASE
    )]
    api: String,

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
        /// Entity id (`quota-<uuid>`).
        entity: QuotaEntityId,
        /// Print the server's JSON response instead of the summary.
        #[arg(long)]
        json: bool,
    },
    /// Create or update a quota entity (the `ConfigureQuotaEntity` upsert;
    /// there is no delete in v1). Either give the direct flags, or point
    /// `--file` at a single-entity TOML document — the two are mutually
    /// exclusive.
    Configure {
        /// A single-entity TOML file (see the module docs for the schema).
        /// Conflicts with every direct flag below.
        #[arg(long, conflicts_with_all = ["entity", "name", "quota_ucu", "parent"])]
        file: Option<PathBuf>,
        /// Entity id to upsert (`quota-<uuid>`). This id is the upsert's
        /// idempotency identity (ADR 0026): a caller retrying after an
        /// unknown outcome must pass the *same* id back explicitly, or the
        /// retry mints a second entity instead of landing on the first. When
        /// omitted, a fresh id is minted here and printed, precisely so it
        /// can be captured and reused on a retry.
        #[arg(long, conflicts_with = "file")]
        entity: Option<QuotaEntityId>,
        /// Human name for the entity. Required unless `--file` is given.
        #[arg(long, conflicts_with = "file")]
        name: Option<String>,
        /// Soft quota, as a stock in µCU (ADR 0019). Required unless
        /// `--file` is given.
        #[arg(long, conflicts_with = "file")]
        quota_ucu: Option<u64>,
        /// Parent entity in the quota tree; absent roots the entity.
        #[arg(long, conflicts_with = "file")]
        parent: Option<QuotaEntityId>,
        /// Print the server's JSON response instead of the summary line.
        #[arg(long)]
        json: bool,
    },
}

/// Run the selected `coppice quota` verb.
pub async fn run(args: QuotaArgs) -> Result<()> {
    let client = ApiClient::new(&args.api)?;
    match args.command {
        QuotaCommand::List { json } => list(&client, json).await,
        QuotaCommand::Show { entity, json } => show(&client, entity, json).await,
        QuotaCommand::Configure {
            file,
            entity,
            name,
            quota_ucu,
            parent,
            json,
        } => {
            configure(
                &client,
                file.as_deref(),
                entity,
                name,
                quota_ucu,
                parent,
                json,
            )
            .await
        }
    }
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

/// `coppice quota list`: every quota entity, root and descendants together,
/// as one flat table (the tree shape is `show`'s job, not `list`'s).
async fn list(client: &ApiClient, json: bool) -> Result<()> {
    let body: serde_json::Value = client
        .get_json(
            "/quota-entities",
            &Vec::new(),
            ctx("listing quota entities", "reading the quota entity list"),
        )
        .await?;
    if json {
        print_json(&body);
        return Ok(());
    }
    let page: dto::ListQuotaEntitiesResponse =
        serde_json::from_value(body).context("reading the quota entity list")?;
    print!("{}", render_quota_list(&page.entities));
    Ok(())
}

/// The column headers shared by `quota list` and the `children:` table nested
/// under `quota show`, so both renders look like one program.
const QUOTA_LIST_HEADERS: [&str; 9] = [
    "id",
    "name",
    "parent",
    "quota (uCU)",
    "usage (uCU)",
    "over quota",
    "penalty",
    "queued",
    "running",
];

/// Render a flat list of quota-entity nodes as an aligned table, or the
/// "empty" sentinel when there are none.
fn render_quota_list(entities: &[dto::QuotaEntityNode]) -> String {
    if entities.is_empty() {
        return "(no quota entities)\n".to_string();
    }
    let rows: Vec<Vec<String>> = entities.iter().map(quota_node_row).collect();
    render_table(&QUOTA_LIST_HEADERS, &rows)
}

/// The row cells for one [`dto::QuotaEntityNode`], in [`QUOTA_LIST_HEADERS`]
/// order.
fn quota_node_row(node: &dto::QuotaEntityNode) -> Vec<String> {
    vec![
        node.id.to_string(),
        node.name.clone(),
        node.parent
            .map(|p| p.to_string())
            .unwrap_or_else(|| "-".to_string()),
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
async fn show(client: &ApiClient, entity: QuotaEntityId, json: bool) -> Result<()> {
    let body: serde_json::Value = client
        .get_json(
            &format!("/quota-entities/{entity}"),
            &Vec::new(),
            ctx("fetching quota entity", "reading quota entity detail"),
        )
        .await?;
    if json {
        print_json(&body);
        return Ok(());
    }
    let detail: dto::GetQuotaEntityResponse =
        serde_json::from_value(body).context("reading quota entity detail")?;
    print!("{}", render_quota_detail(&detail));
    Ok(())
}

/// Render a `GetQuotaEntityResponse` as: a key/value block for the entity
/// itself, then a `chain:` section (ancestry, root first), then a
/// `children:` section (the same table `quota list` uses, indented), then a
/// `stats:` block. `usage_history` is never rendered — the field is always
/// empty (no usage-series sampler exists yet, see [`dto::QuotaEntityStats`])
/// — so a section for it would only ever show as absent noise.
fn render_quota_detail(detail: &dto::GetQuotaEntityResponse) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let kv = |out: &mut String, key: &str, value: &str| {
        let _ = writeln!(out, "{key:<16}{value}");
    };

    let node = &detail.entity;
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
                "  {} {} quota {} uCU, usage {} uCU",
                view.id, view.name, view.quota_ucu, view.usage_ucu
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
        .map(|(phase, count)| format!("{} {count}", phase_label(*phase)))
        .collect();
    let _ = writeln!(out, "  by phase       {}", by_state.join(", "));
    let _ = writeln!(
        out,
        "  oldest queued  {}",
        stats
            .oldest_queued_age_seconds
            .map(|s| format!("{s}s"))
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

/// A single-entity TOML file for `quota configure --file`.
///
/// Deliberately the same key vocabulary as one `[[quota_entity]]` entry in
/// the coordinator's formation-policy TOML
/// (`coppice_coordinator::policy::QuotaEntitySpec`), just flattened to a
/// single entity rather than an array of them: an operator who has already
/// written a formation-policy document can lift one `[[quota_entity]]` block
/// straight into this file (dropping the array-table brackets) and it reads
/// as-is. Note the TOML key is `quota`, matching the policy file, even though
/// the wire field it becomes is `quota_ucu` — the same rename the policy
/// parser performs.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaEntityFile {
    /// The entity id (`quota-<uuid>`).
    pub id: QuotaEntityId,
    /// A human label recorded on the entity.
    pub name: String,
    /// The quota stock in µCU (ADR 0019). Wire field: `quota_ucu`.
    pub quota: u64,
    /// Optional parent entity for hierarchical accounting.
    #[serde(default)]
    pub parent: Option<QuotaEntityId>,
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

/// `coppice quota configure`: build the upsert request from either input
/// mode, POST it, and render the result.
async fn configure(
    client: &ApiClient,
    file: Option<&Path>,
    entity: Option<QuotaEntityId>,
    name: Option<String>,
    quota_ucu: Option<u64>,
    parent: Option<QuotaEntityId>,
    json: bool,
) -> Result<()> {
    let (request, minted) = build_configure_request(file, entity, name, quota_ucu, parent)?;
    if minted {
        // The id is the upsert's idempotency identity (ADR 0026): a caller
        // who does not capture and reuse it on a retry after a dropped
        // response will create a second entity instead of landing on the
        // first. Say so loudly, on stderr, ahead of the write.
        eprintln!(
            "note: no --entity given; minted {} — pass --entity {} explicitly on any \
             retry so it lands on the same entity (ADR 0026)",
            request.entity, request.entity
        );
    }
    let body: serde_json::Value = client
        .post_json(
            "/quota-entities",
            &request,
            ctx("configuring quota entity", "reading configure response"),
        )
        .await?;
    if json {
        print_json(&body);
        return Ok(());
    }
    let response: dto::ConfigureQuotaEntityResponse =
        serde_json::from_value(body).context("reading configure response")?;
    println!(
        "configured {} (log index {})",
        response.entity, response.log_index
    );
    Ok(())
}

/// Build the wire request from either input mode, and report whether the
/// entity id was freshly minted here (as opposed to given via `--entity` or
/// read from `--file`) so the caller can warn about ADR 0026 idempotency.
///
/// `--file` and the direct flags are already `conflicts_with` at the clap
/// level, so this function only ever sees one populated side when reached
/// through the CLI; the flag-mode branch still validates `name`/`quota_ucu`
/// itself, since clap's `Option<T>` fields cannot express "required unless
/// `--file` is given" on their own.
fn build_configure_request(
    file: Option<&Path>,
    entity: Option<QuotaEntityId>,
    name: Option<String>,
    quota_ucu: Option<u64>,
    parent: Option<QuotaEntityId>,
) -> Result<(dto::ConfigureQuotaEntityRequest, bool)> {
    if let Some(path) = file {
        let spec = QuotaEntityFile::load(path)?;
        return Ok((
            dto::ConfigureQuotaEntityRequest {
                entity: spec.id,
                parent: spec.parent,
                name: spec.name,
                quota_ucu: spec.quota,
            },
            false,
        ));
    }
    let name = name.context("--name is required unless --file is given")?;
    let quota_ucu = quota_ucu.context("--quota-ucu is required unless --file is given")?;
    let (entity, minted) = match entity {
        Some(entity) => (entity, false),
        None => (QuotaEntityId::new(), true),
    };
    Ok((
        dto::ConfigureQuotaEntityRequest {
            entity,
            parent,
            name,
            quota_ucu,
        },
        minted,
    ))
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

    use coppice_core::time::Timestamp;

    use crate::testsupport::{error_body, leader_hint, spawn};

    /// A fixed quota-entity id from a small integer, so test bodies can name
    /// distinct entities tersely.
    fn quota_id(n: u8) -> QuotaEntityId {
        format!("quota-00000000-0000-0000-0000-{n:012}")
            .parse()
            .unwrap()
    }

    fn ts(micros: i64) -> Timestamp {
        Timestamp::from_micros(micros).unwrap()
    }

    fn sample_node(id: QuotaEntityId, parent: Option<QuotaEntityId>) -> dto::QuotaEntityNode {
        dto::QuotaEntityNode {
            id,
            name: "team-a".to_string(),
            parent,
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

    fn sample_view(id: QuotaEntityId, parent: Option<QuotaEntityId>) -> dto::QuotaEntityView {
        dto::QuotaEntityView {
            id,
            name: "root".to_string(),
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
            "parent": null,
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
            "parent": null,
            "quota_ucu": 0,
            "usage_ucu": 42,
            "over_quota_ratio": null,
            "penalty": null,
        })
    }

    fn client(base: &str) -> ApiClient {
        ApiClient::new(base).unwrap()
    }

    // -----------------------------------------------------------------
    // list
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn list_decodes_the_real_response() {
        let entities = vec![sample_node(quota_id(1), None)];
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
        let page: dto::ListQuotaEntitiesResponse = serde_json::from_value(body).unwrap();
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
        show(&client(&base), id, false)
            .await
            .expect("show decodes null over_quota_ratio");

        let detail: dto::GetQuotaEntityResponse = serde_json::from_value(body).unwrap();
        assert_eq!(detail.entity.over_quota_ratio, f64::INFINITY);
        assert_eq!(detail.chain[0].penalty, f64::INFINITY);
        let rendered = render_quota_detail(&detail);
        assert!(rendered.contains("over quota      unbounded"), "{rendered}");
        assert!(rendered.contains("penalty         unbounded"), "{rendered}");
        assert!(!rendered.contains("inf"), "{rendered}");
    }

    #[test]
    fn render_quota_list_shows_id_name_and_quota() {
        let node = sample_node(quota_id(1), None);
        let rendered = render_quota_list(std::slice::from_ref(&node));
        assert!(rendered.contains(&node.id.to_string()), "{rendered}");
        assert!(rendered.contains("team-a"), "{rendered}");
        assert!(rendered.contains("1000"), "{rendered}");
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
            entity: sample_node(id, None),
            chain: vec![sample_view(quota_id(9), None)],
            children: vec![sample_node(quota_id(3), Some(id))],
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

        show(&client(&base), id, false)
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
            entity: sample_node(quota_id(1), None),
            chain: Vec::new(),
            children: Vec::new(),
            stats: sample_stats(),
        };
        let rendered = render_quota_detail(&detail);
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
        let err = show(&client(&base), quota_id(4), false)
            .await
            .expect_err("show fails");
        let message = format!("{err:#}");
        assert!(message.contains("NOT_FOUND"), "{message}");
    }

    // -----------------------------------------------------------------
    // configure
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn configure_from_flags_posts_the_dto() {
        let captured: Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>> =
            Arc::new(Mutex::new(Vec::new()));
        let router = Router::new()
            .route(
                "/api/v1/quota-entities",
                post(
                    |State(captured): State<Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>>>,
                     Json(req): Json<dto::ConfigureQuotaEntityRequest>| async move {
                        let response = dto::ConfigureQuotaEntityResponse {
                            entity: req.entity,
                            log_index: 7,
                        };
                        captured.lock().unwrap().push(req);
                        Json(serde_json::to_value(response).unwrap())
                    },
                ),
            )
            .with_state(captured.clone());
        let base = spawn(router).await;

        let entity = quota_id(5);
        let parent = quota_id(6);
        configure(
            &client(&base),
            None,
            Some(entity),
            Some("team-b".to_string()),
            Some(500),
            Some(parent),
            false,
        )
        .await
        .expect("configure succeeds");

        let received = captured.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].entity, entity);
        assert_eq!(received[0].name, "team-b");
        assert_eq!(received[0].quota_ucu, 500);
        assert_eq!(received[0].parent, Some(parent));
    }

    #[tokio::test]
    async fn configure_from_file_posts_the_dto() {
        let captured: Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>> =
            Arc::new(Mutex::new(Vec::new()));
        let router = Router::new()
            .route(
                "/api/v1/quota-entities",
                post(
                    |State(captured): State<Arc<Mutex<Vec<dto::ConfigureQuotaEntityRequest>>>>,
                     Json(req): Json<dto::ConfigureQuotaEntityRequest>| async move {
                        let response = dto::ConfigureQuotaEntityResponse {
                            entity: req.entity,
                            log_index: 9,
                        };
                        captured.lock().unwrap().push(req);
                        Json(serde_json::to_value(response).unwrap())
                    },
                ),
            )
            .with_state(captured.clone());
        let base = spawn(router).await;

        let id = quota_id(7);
        let parent = quota_id(8);
        let toml_body =
            format!("id = \"{id}\"\nname = \"team-c\"\nquota = 750\nparent = \"{parent}\"\n");
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml_body.as_bytes()).unwrap();

        configure(
            &client(&base),
            Some(file.path()),
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .expect("configure --file succeeds");

        let received = captured.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].entity, id);
        assert_eq!(received[0].name, "team-c");
        assert_eq!(received[0].quota_ucu, 750);
        assert_eq!(received[0].parent, Some(parent));
    }

    #[tokio::test]
    async fn configure_surfaces_the_leader_hint_on_421() {
        let router = Router::new().route(
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
            Some(quota_id(9)),
            Some("x".to_string()),
            Some(1),
            None,
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
        let toml = "id = \"quota-00000000-0000-0000-0000-000000000001\"\n\
                     name = \"x\"\nquota = 1\nbogus = 2\n";
        let result: Result<QuotaEntityFile, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    /// The `--file` vocabulary must match one `[[quota_entity]]` entry of the
    /// coordinator's formation-policy TOML exactly: an operator who already
    /// wrote a formation policy should be able to lift a block straight into
    /// a `quota configure --file` document (dropping the array-table
    /// brackets) with no re-spelling.
    #[test]
    fn file_vocabulary_matches_the_formation_policy_quota_entity_entry() {
        let id = quota_id(10);
        let parent = quota_id(11);
        let body =
            format!("id = \"{id}\"\nname = \"team-d\"\nquota = 321\nparent = \"{parent}\"\n");
        let file: QuotaEntityFile = toml::from_str(&body).expect("file parses");

        let policy_body = format!("[[quota_entity]]\n{body}");
        let policy =
            coppice_coordinator::policy::FormationPolicy::parse_toml(policy_body.as_bytes())
                .expect("parses as a formation-policy quota_entity entry");
        let spec = &policy.quota_entities[0];
        assert_eq!(spec.id, file.id);
        assert_eq!(spec.name, file.name);
        assert_eq!(spec.quota, file.quota);
        assert_eq!(spec.parent, file.parent);
    }
}
