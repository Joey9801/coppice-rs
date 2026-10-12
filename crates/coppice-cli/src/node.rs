//! `coppice node`: one command group over two very different transports.
//!
//! Compute nodes are read through the coordinator's JSON HTTP API (`/nodes`,
//! `/nodes/{node}`) like every other client verb, while the enrollment-token
//! and identity verbs (ADR 0037 §5) go over the coordinator's **admin**
//! channel: mutual TLS with an operator certificate, a different port, a
//! different authorization story. They still belong in one `coppice node`
//! group — an operator asking "what is this node doing?" and "revoke this
//! node's identity" is asking about the same thing — so this module owns the
//! group and routes each verb to the transport it needs.
//!
//! The split is visible in the flags, deliberately: `list`/`show` take `--api`,
//! the admin verbs take `--target`/`--ca`/`--cert`/`--key`. Because the two
//! sets cannot both be required, the admin connection flags are optional at
//! parse time and checked here, naming every flag that is missing at once
//! rather than one per run. The admin verbs themselves are **not** redeclared:
//! [`coppice_coordinator::cli::NodeVerb`] is flattened in whole, so their
//! spelling, help text, and argument groups stay owned by the crate that
//! implements them.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};

use coppice_client::{
    paths, Client, GetNodeResponse, ListNodesResponse, NodeDrain, NodeHealth, NodeId, NodeSummary,
    Resources, Timestamp,
};
use coppice_core::bytes::ByteSize;

use crate::client::{ctx, print_json, render_table, ApiConnection, ApiResultExt};

/// `coppice node` argument group.
///
/// `--api`/`--token` serve the HTTP reads (`list`, `show`); the admin verbs
/// speak the mTLS channel (`--target`/`--ca`/`--cert`/`--key`) and ignore
/// both.
#[derive(Debug, clap::Args)]
pub struct NodeArgs {
    #[command(flatten)]
    pub connection: ApiConnection,

    /// The `host:port` of a coordinator's admin surface. Required by the
    /// enrollment-token and identity verbs; unused by `list` and `show`.
    #[arg(long, global = true)]
    pub target: Option<String>,

    /// The cluster CA bundle (PEM) that verifies the admin target.
    #[arg(long, global = true)]
    pub ca: Option<PathBuf>,

    /// The operator certificate (PEM) to present to the admin target.
    #[arg(long, global = true)]
    pub cert: Option<PathBuf>,

    /// The operator private key (PEM).
    #[arg(long, global = true)]
    pub key: Option<PathBuf>,

    /// The logical cluster id the admin target must serve. Optional: the
    /// target was named explicitly, so this is a guard, not a lookup.
    #[arg(long, global = true)]
    pub cluster_id: Option<String>,

    #[command(subcommand)]
    pub verb: NodeVerb,
}

#[derive(Debug, clap::Subcommand)]
pub enum NodeVerb {
    /// List the cluster's compute nodes.
    List {
        /// Print the server's JSON response instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Show one node's capacity, health, and current work.
    Show {
        /// Node id (`node-<uuid>`).
        node: NodeId,
        /// Print the server's JSON response instead of the summary.
        #[arg(long)]
        json: bool,
    },
    /// Drain a node: running work continues, and only bounded backfill
    /// guaranteed gone by the node's horizon is admitted.
    Drain {
        /// Node id (`node-<uuid>`).
        node: NodeId,
        /// Poll the node until the drain is complete (no live work left on
        /// it), printing its horizon — the time by which it will be empty, or
        /// "unbounded" — as it changes, or until the duration elapses. Bare
        /// `--wait` means 10 minutes; `--wait 30s`
        /// names a duration explicitly; omitting the flag entirely means
        /// return as soon as the drain is requested, without waiting.
        #[arg(
            long,
            num_args = 0..=1,
            default_missing_value = "10m",
            value_parser = parse_wait
        )]
        wait: Option<Duration>,
        /// Print the server's JSON response instead of a confirmation line.
        #[arg(long)]
        json: bool,
    },
    /// Clear a node's drain, reopening it to any work.
    Undrain {
        /// Node id (`node-<uuid>`).
        node: NodeId,
        /// Print the server's JSON response instead of a confirmation line.
        #[arg(long)]
        json: bool,
    },
    /// Remove a node from the cluster (ADR 0041).
    Remove {
        /// Node id (`node-<uuid>`).
        node: NodeId,
        /// Print the server's JSON response instead of a confirmation line.
        #[arg(long)]
        json: bool,
    },
    /// The mTLS admin verbs (`enroll-token`, `revoke-identity`), owned by the
    /// coordinator crate and flattened in whole so this CLI never restates
    /// their contract.
    #[command(flatten)]
    Admin(coppice_coordinator::cli::NodeVerb),
}

/// Parse a humane duration string (`"30s"`, `"10m"`) for `--wait`.
///
/// Reuses `humantime`'s parser (the same grammar the config file's durations
/// use, and the one `coppice-coordinator`'s own `--wait` flags use), so an
/// unlabelled bare integer is rejected rather than silently meaning some
/// unit. Kept private to this module — `coppice-coordinator::cli` has its own
/// copy for its own flags, and neither needs to be a shared dependency of the
/// other.
fn parse_wait(raw: &str) -> Result<Duration, String> {
    humantime_serde::re::humantime::parse_duration(raw).map_err(|e| e.to_string())
}

/// Run the selected `coppice node` verb, routing to the transport it needs.
pub async fn run(args: NodeArgs) -> Result<()> {
    match args.verb {
        NodeVerb::List { json } => {
            let client = args.connection.client()?;
            list(&client, json).await
        }
        NodeVerb::Show { node, json } => {
            let client = args.connection.client()?;
            show(&client, node, json).await
        }
        NodeVerb::Drain { node, wait, json } => {
            let client = args.connection.client()?;
            drain(&client, node, wait, json).await
        }
        NodeVerb::Undrain { node, json } => {
            let client = args.connection.client()?;
            undrain(&client, node, json).await
        }
        NodeVerb::Remove { node, json } => {
            let client = args.connection.client()?;
            remove(&client, node, json).await
        }
        NodeVerb::Admin(verb) => {
            coppice_coordinator::node::run_cli(admin_args(
                args.target,
                args.ca,
                args.cert,
                args.key,
                args.cluster_id,
                verb,
            )?)
            .await
        }
    }
}

/// Assemble the coordinator crate's `NodeArgs` from the optional connection
/// flags, refusing with every missing flag named at once.
///
/// These four are required by the admin channel and meaningless to `list` and
/// `show`, so clap cannot enforce them for us: one command group cannot make an
/// argument required for some of its subcommands and forbidden for others.
/// Collecting the misses into a single message keeps that from becoming four
/// consecutive failed runs.
fn admin_args(
    target: Option<String>,
    ca: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    cluster_id: Option<String>,
    verb: coppice_coordinator::cli::NodeVerb,
) -> Result<coppice_coordinator::cli::NodeArgs> {
    let mut missing: Vec<&str> = Vec::new();
    if target.is_none() {
        missing.push("--target");
    }
    if ca.is_none() {
        missing.push("--ca");
    }
    if cert.is_none() {
        missing.push("--cert");
    }
    if key.is_none() {
        missing.push("--key");
    }
    if !missing.is_empty() {
        bail!(
            "this verb talks to a coordinator's admin channel over mutual TLS and needs {} \
             (the `list` and `show` verbs use --api instead)",
            missing.join(", ")
        );
    }
    Ok(coppice_coordinator::cli::NodeArgs {
        // Each is `Some` — the loop above refused otherwise.
        target: target.expect("target checked above"),
        ca: ca.expect("ca checked above"),
        cert: cert.expect("cert checked above"),
        key: key.expect("key checked above"),
        cluster_id,
        verb,
    })
}

// ---------------------------------------------------------------------------
// HTTP verbs
// ---------------------------------------------------------------------------

/// `coppice node list`: every registered node, one row each.
async fn list(client: &Client, json: bool) -> Result<()> {
    if json {
        let body = client
            .get_value(paths::NODES, &[])
            .await
            .api_ctx(ctx("listing nodes", "reading the node list"))?;
        print_json(&body.value);
        return Ok(());
    }
    let response = client
        .list_nodes()
        .await
        .api_ctx(ctx("listing nodes", "reading the node list"))?;
    print!("{}", render_list(&response));
    Ok(())
}

/// `coppice node show`: one node's summary, its live attempts, and the
/// allocations still accruing against it.
async fn show(client: &Client, node: NodeId, json: bool) -> Result<()> {
    let path = paths::node(node);
    if json {
        let body = client
            .get_value(&path, &[])
            .await
            .api_ctx(ctx("fetching node status", "reading node detail"))?;
        print_json(&body.value);
        return Ok(());
    }
    let response = client
        .node(node)
        .await
        .api_ctx(ctx("fetching node status", "reading node detail"))?;
    print!("{}", render_detail(&response));
    Ok(())
}

/// `coppice node drain`: drain a node (ADR 0044 §5).
///
/// With `--wait`, polls `GET /nodes/{node}` once a second until the drain's
/// derived status reads complete, or the wait duration elapses.
async fn drain(client: &Client, node: NodeId, wait: Option<Duration>, json: bool) -> Result<()> {
    let path = paths::node_drain(node);
    post_node_action(
        client,
        &path,
        "requesting drain",
        json,
        || client.drain_node(node),
        || format!("node {node} draining"),
    )
    .await?;
    if let Some(deadline) = wait {
        wait_for_drain(client, node, deadline, Duration::from_secs(1)).await?;
    }
    Ok(())
}

/// `coppice node undrain`: clear a node's drain.
async fn undrain(client: &Client, node: NodeId, json: bool) -> Result<()> {
    let path = paths::node_undrain(node);
    post_node_action(
        client,
        &path,
        "requesting undrain",
        json,
        || client.undrain_node(node),
        || format!("node {node} undrained"),
    )
    .await
}

/// `coppice node remove`: remove a node from the cluster (ADR 0041).
async fn remove(client: &Client, node: NodeId, json: bool) -> Result<()> {
    let path = paths::node_remove(node);
    post_node_action(
        client,
        &path,
        "requesting removal",
        json,
        || client.remove_node(node),
        || format!("node {node} removed"),
    )
    .await
}

/// The shared POST half of `drain`/`undrain`/`remove`: all three take no
/// request body and return an empty `{}` on success; only the path, the typed
/// call, and the confirmation words differ.
///
/// `--json` prints the server's own response body verbatim, unparsed — the
/// same convention `list`/`show` use, via [`Client::post_value`] against the
/// same `path` the typed call resolves to. Without `--json`, the typed call
/// (`drain_node`/`undrain_node`/`remove_node`) runs instead and a one-line
/// confirmation is printed.
async fn post_node_action<T, F>(
    client: &Client,
    path: &str,
    sending: &'static str,
    json: bool,
    typed_call: impl FnOnce() -> F,
    confirmation: impl FnOnce() -> String,
) -> Result<()>
where
    F: std::future::Future<Output = coppice_client::Result<T>>,
{
    if json {
        let body = client
            .post_value(path, &serde_json::json!({}))
            .await
            .api_ctx(ctx(sending, "reading response"))?;
        print_json(&body);
    } else {
        typed_call()
            .await
            .api_ctx(ctx(sending, "reading response"))?;
        println!("{}", confirmation());
    }
    Ok(())
}

/// Poll a node's summary once every `interval` until its drain is complete
/// or `deadline` elapses, printing a stderr progress line whenever the
/// [`DrainProgress`] changes — the horizon (or "unbounded") and the running
/// and accruing counts — and a final line when it is complete.
///
/// Progress goes to stderr, not stdout: `--json` writes the server's own
/// response body to stdout, and a script that pipes that into `jq` must not
/// have narration interleaved with it.
async fn wait_for_drain(
    client: &Client,
    node: NodeId,
    deadline: Duration,
    interval: Duration,
) -> Result<()> {
    let start = std::time::Instant::now();
    let mut last: Option<DrainProgress> = None;
    loop {
        let response = client
            .node(node)
            .await
            .api_ctx(ctx("checking drain progress", "reading node detail"))?;
        let current = drain_progress(&response.summary);
        // The verdict before the progress line, so a node that is already
        // empty is answered rather than narrated.
        if current.complete {
            eprintln!("node {node} drained: drain complete");
            return Ok(());
        }
        if last.as_ref() != Some(&current) {
            eprintln!("waiting for node {node} to drain: {}", current.describe());
            last = Some(current.clone());
        }
        if start.elapsed() >= deadline {
            return Err(anyhow!(
                "node {node} did not drain within {deadline:?}: {} outstanding",
                current.describe()
            ));
        }
        tokio::time::sleep(interval).await;
    }
}

/// What a drain wait watches, pulled out so the "did the progress line need
/// reprinting" decision in [`wait_for_drain`] is testable without a live
/// poll loop.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DrainProgress {
    /// The drain record is visible on the read. `false` right after the
    /// request on a replica that has not applied it yet, or after an
    /// undrain.
    recorded: bool,
    /// The node's horizon; `None` while unbounded (or not recorded).
    horizon: Option<Timestamp>,
    complete: bool,
    running: u32,
    accruing: u32,
}

impl DrainProgress {
    fn describe(&self) -> String {
        let horizon = match (self.recorded, self.horizon) {
            (false, _) => "drain not recorded".to_string(),
            (true, Some(at)) => format!("empty by {at}"),
            (true, None) => "horizon unbounded".to_string(),
        };
        format!(
            "{horizon}, running {}, accruing {}",
            self.running, self.accruing
        )
    }
}

/// The drain's derived status off a node summary (ADR 0044 §5).
fn drain_progress(summary: &NodeSummary) -> DrainProgress {
    let drain = summary.drain.as_ref();
    DrainProgress {
        recorded: drain.is_some(),
        horizon: drain.and_then(|d| d.horizon),
        complete: drain.is_some_and(|d| d.complete),
        running: summary.running_count,
        accruing: summary.accruing_count,
    }
}

/// The drain cell of the detail view: when it was requested and its derived
/// status — the horizon, "unbounded", or "complete".
fn drain_detail(drain: &NodeDrain) -> String {
    let status = if drain.complete {
        "complete".to_string()
    } else {
        match drain.horizon {
            Some(at) => format!("empty by {at}"),
            None => "unbounded (live work with no runtime bound)".to_string(),
        }
    };
    format!("requested {}, {status}", drain.requested_at)
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Node liveness, in the wire's own vocabulary, printed as the coordinator
/// spelled it and never softened.
///
/// All three values are reachable from any replica now (ADR 0040 fetches the
/// leader's liveness marks when a follower serves the read). `unknown` is a
/// third answer and not a synonym for either of the others: it means the
/// coordinator has nothing to judge the node by — the marks could not be
/// fetched, or the node is inside the grace window a new leader granted it —
/// so rendering it as "healthy" would invent a report that never arrived.
fn health_label(health: &NodeHealth) -> String {
    health.to_string()
}

/// The list table's scheduling cell: [`drain_label`], marked when the node
/// takes only effectively preemptible work (ADR 0044 §4) and when its host
/// has reported an interruption (ADR 0044 §5).
fn scheduling_cell(
    drained: bool,
    agent_draining: bool,
    preemptible_only: bool,
    interrupted: bool,
) -> String {
    let mut cell = drain_label(drained, agent_draining).to_string();
    if preemptible_only {
        cell.push_str(", preemptible only");
    }
    if interrupted {
        cell.push_str(", interruption pending");
    }
    cell
}

/// The scheduling posture, naming both independent flags on
/// [`NodeSummary`]: the operator's drain (`drain`) and the agent's own
/// shutdown announcement (`agent_draining`). Neither hides the other — an
/// operator reading the list must be able to tell who is emptying the node.
fn drain_label(drained: bool, agent_draining: bool) -> &'static str {
    match (drained, agent_draining) {
        (false, false) => "open",
        (true, false) => "drained",
        (false, true) => "agent draining",
        (true, true) => "drained, agent draining",
    }
}

/// One resource triple on a single line, byte dimensions humanized.
fn resources(r: &Resources) -> String {
    format!(
        "cpu {} mCPU, memory {}, disk {}",
        r.cpu_millis,
        ByteSize::from_bytes(r.memory_bytes),
        ByteSize::from_bytes(r.disk_bytes),
    )
}

/// A compact `cpu/mem/disk` cell for the list table, where a full sentence per
/// dimension would not fit.
fn resources_cell(r: &Resources) -> String {
    format!(
        "{}m/{}/{}",
        r.cpu_millis,
        ByteSize::from_bytes(r.memory_bytes),
        ByteSize::from_bytes(r.disk_bytes),
    )
}

/// Render the node list as a table. `allocated` is funded resources across
/// non-Released allocations; `used` is measured consumption, which is zero
/// everywhere until agent telemetry lands — the header says `used` rather than
/// implying it is a live figure.
fn render_list(response: &ListNodesResponse) -> String {
    if response.nodes.is_empty() {
        return "(no nodes registered)\n".to_string();
    }
    let rows: Vec<Vec<String>> = response
        .nodes
        .iter()
        .map(|node| {
            vec![
                node.id.to_string(),
                health_label(&node.health),
                scheduling_cell(
                    node.drain.is_some(),
                    node.agent_draining,
                    node.preemptible_only,
                    node.interruption_deadline.is_some(),
                ),
                resources_cell(&node.capacity),
                resources_cell(&node.allocated),
                node.running_count.to_string(),
                node.accruing_count.to_string(),
                node.last_heartbeat
                    .map(|at| at.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ]
        })
        .collect();
    render_table(
        &[
            "id",
            "health",
            "scheduling",
            "capacity",
            "allocated",
            "running",
            "accruing",
            "last heartbeat",
        ],
        &rows,
    )
}

/// Render one node's detail: a key/value block for the summary, then the
/// attempts currently on the node and the allocations still accruing against
/// it, each as its own section.
fn render_detail(response: &GetNodeResponse) -> String {
    use std::fmt::Write;

    let summary = &response.summary;
    let mut out = String::new();
    let kv = |out: &mut String, key: &str, value: &str| {
        let _ = writeln!(out, "{key:<16}{value}");
    };

    kv(&mut out, "id", &summary.id.to_string());
    kv(&mut out, "health", &health_label(&summary.health));
    kv(
        &mut out,
        "scheduling",
        drain_label(summary.drain.is_some(), summary.agent_draining),
    );
    if let Some(drain) = summary.drain.as_ref() {
        kv(&mut out, "drain", &drain_detail(drain));
    }
    kv(
        &mut out,
        "agent",
        if summary.agent_draining {
            "draining (shutting down)"
        } else {
            "not draining"
        },
    );
    kv(
        &mut out,
        "accepts",
        // ADR 0044 §4: a spot-only node takes effectively preemptible work
        // only.
        if summary.preemptible_only {
            "preemptible work only"
        } else {
            "any work"
        },
    );
    if let Some(deadline) = summary.interruption_deadline {
        // ADR 0044 §5: the provider is reclaiming this node's host.
        kv(&mut out, "interruption", &format!("deadline {deadline}"));
    }
    kv(&mut out, "epoch", &summary.epoch.to_string());
    kv(&mut out, "capacity", &resources(&summary.capacity));
    kv(&mut out, "allocated", &resources(&summary.allocated));
    kv(
        &mut out,
        "used",
        // Absence, never a zero triple: the node is not reporting usage
        // (ADR 0039).
        &match summary.used.as_ref() {
            Some(used) => resources(used),
            None => "(not reported)".to_string(),
        },
    );
    kv(
        &mut out,
        "last heartbeat",
        &summary
            .last_heartbeat
            .map(|at| at.to_string())
            // No heartbeat is not "never ran": agents do not report liveness
            // yet, so absence here says nothing about the node.
            .unwrap_or_else(|| "(agents do not report yet)".to_string()),
    );
    if summary.labels.is_empty() {
        kv(&mut out, "labels", "(none)");
    } else {
        let labels: Vec<String> = summary
            .labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        kv(&mut out, "labels", &labels.join(", "));
    }

    let _ = writeln!(out);
    if response.active_attempts.is_empty() {
        out.push_str("attempts:       (none)\n");
    } else {
        out.push_str("attempts:\n");
        for attempt in &response.active_attempts {
            let mut line = format!("  {} job {}", attempt.id, attempt.job);
            if let Some(started) = attempt.started_at {
                let _ = write!(line, " started {started}");
            }
            let _ = writeln!(out, "{line}");
        }
    }

    let _ = writeln!(out);
    if response.accrual_queue.is_empty() {
        out.push_str("accruing:       (none)\n");
    } else {
        out.push_str("accruing:\n");
        for accrual in &response.accrual_queue {
            // `projected_start` is null when full funding is unbounded — the
            // allocation may never start — which is a different claim from
            // "starts now", so it is spelled out rather than left blank.
            let projected = accrual
                .projected_start
                .map(|at| at.to_string())
                .unwrap_or_else(|| "unbounded".to_string());
            let _ = writeln!(
                out,
                "  {} job {} funded cpu {:.0}% mem {:.0}% disk {:.0}%, projected start {}",
                accrual.allocation.id,
                accrual.allocation.job,
                accrual.funded_fraction.cpu * 100.0,
                accrual.funded_fraction.memory * 100.0,
                accrual.funded_fraction.disk * 100.0,
                projected,
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use axum::extract::{Path as AxumPath, State};
    use axum::routing::{get, post};
    use axum::{Json, Router};

    use coppice_api::http::dto;
    use coppice_core::id::NodeId as CoreNodeId;

    use crate::testsupport::{error_body, spawn};

    /// Round-trip a server-typed (`coppice_api::http::dto`) fixture through
    /// JSON into the `coppice_client` type the CLI actually renders. The
    /// server type is the fixture that pins the wire contract; the client
    /// type is what `render_list`/`render_detail`/`drained`/`counts` take —
    /// so this conversion, not a hand-written assertion, *is* the
    /// cross-check that the two crates still agree on the shape.
    fn to_client<S: serde::Serialize, C: serde::de::DeserializeOwned>(value: S) -> C {
        serde_json::from_value(serde_json::to_value(value).unwrap())
            .expect("the client type decodes the server type's own output")
    }

    /// The client-typed node id, for calling the verb functions and for
    /// assertions.
    fn node_id() -> NodeId {
        "node-00000000-0000-0000-0000-000000000001".parse().unwrap()
    }

    /// The same id, server-typed, for `dto::` fixtures.
    fn dto_node_id() -> CoreNodeId {
        "node-00000000-0000-0000-0000-000000000001".parse().unwrap()
    }

    fn sample_summary() -> dto::NodeSummary {
        let mut labels = BTreeMap::new();
        labels.insert("zone".to_string(), "a".to_string());
        dto::NodeSummary {
            id: dto_node_id(),
            capacity: dto::Resources {
                cpu_millis: 4000,
                memory_bytes: 8 * 1024 * 1024 * 1024,
                disk_bytes: 100 * 1024 * 1024 * 1024,
            },
            allocated: dto::Resources {
                cpu_millis: 500,
                memory_bytes: 1024 * 1024 * 1024,
                disk_bytes: 1024 * 1024 * 1024,
            },
            used: Some(dto::Resources {
                cpu_millis: 250,
                memory_bytes: 512 * 1024 * 1024,
                disk_bytes: 0,
            }),
            labels,
            drain: None,
            agent_draining: false,
            health: dto::NodeHealth::Unknown,
            epoch: 3,
            last_heartbeat: None,
            running_count: 1,
            accruing_count: 2,
            preemptible_only: false,
            interruption_deadline: None,
        }
    }

    #[tokio::test]
    async fn list_gets_the_nodes_route() {
        let seen: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
        let body = serde_json::to_value(dto::ListNodesResponse {
            nodes: vec![sample_summary()],
        })
        .unwrap();
        let router = Router::new()
            .route(
                "/api/v1/nodes",
                get(
                    |State((seen, body)): State<(Arc<Mutex<u32>>, serde_json::Value)>| async move {
                        *seen.lock().unwrap() += 1;
                        Json(body)
                    },
                ),
            )
            .with_state((seen.clone(), body));
        let base = spawn(router).await;

        list(&Client::new(&base).unwrap(), false)
            .await
            .expect("list succeeds");
        assert_eq!(*seen.lock().unwrap(), 1);
    }

    #[test]
    fn list_renders_the_node_row() {
        let rendered = render_list(&to_client(dto::ListNodesResponse {
            nodes: vec![sample_summary()],
        }));
        assert!(rendered.contains(&node_id().to_string()), "{rendered}");
        // Health is `unknown` today and must not be dressed up as healthy.
        assert!(rendered.contains("unknown"), "{rendered}");
        assert!(rendered.contains("open"), "{rendered}");
        assert!(rendered.contains("4000m"), "{rendered}");
        assert!(!rendered.contains("preemptible only"), "{rendered}");
        assert!(!rendered.contains("interruption"), "{rendered}");

        // A spot-only node says so in its scheduling cell (ADR 0044 §4).
        let mut spot = sample_summary();
        spot.preemptible_only = true;
        let rendered = render_list(&to_client(dto::ListNodesResponse { nodes: vec![spot] }));
        assert!(rendered.contains("open, preemptible only"), "{rendered}");
    }

    #[test]
    fn drain_label_names_both_flags() {
        assert_eq!(drain_label(false, false), "open");
        assert_eq!(drain_label(true, false), "drained");
        assert_eq!(drain_label(false, true), "agent draining");
        // Neither flag hides the other.
        assert_eq!(drain_label(true, true), "drained, agent draining");
    }

    #[test]
    fn list_says_so_when_no_nodes_are_registered() {
        let rendered = render_list(&to_client(dto::ListNodesResponse { nodes: Vec::new() }));
        assert_eq!(rendered, "(no nodes registered)\n");
    }

    #[tokio::test]
    async fn show_puts_the_node_id_in_the_path() {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let body = serde_json::to_value(dto::GetNodeResponse {
            summary: sample_summary(),
            host: None,
            detected_capacity: None,
            active_attempts: Vec::new(),
            accrual_queue: Vec::new(),
        })
        .unwrap();
        let router =
            Router::new()
                .route(
                    "/api/v1/nodes/:node",
                    get(
                        |AxumPath(node): AxumPath<String>,
                         State((seen, body)): State<(
                            Arc<Mutex<Vec<String>>>,
                            serde_json::Value,
                        )>| async move {
                            seen.lock().unwrap().push(node);
                            Json(body)
                        },
                    ),
                )
                .with_state((seen.clone(), body));
        let base = spawn(router).await;

        show(&Client::new(&base).unwrap(), node_id(), false)
            .await
            .expect("show succeeds");
        assert_eq!(*seen.lock().unwrap(), [node_id().to_string()]);
    }

    #[tokio::test]
    async fn show_surfaces_a_not_found_body() {
        let router = Router::new().route(
            "/api/v1/nodes/:node",
            get(|| async {
                (
                    axum::http::StatusCode::NOT_FOUND,
                    Json(error_body("NOT_FOUND", "node node-… not found")),
                )
            }),
        );
        let base = spawn(router).await;
        let err = show(&Client::new(&base).unwrap(), node_id(), false)
            .await
            .expect_err("show fails");
        let message = format!("{err:#}");
        assert!(message.contains("NOT_FOUND"), "{message}");
        assert!(message.contains("not found"), "{message}");
    }

    #[test]
    fn detail_reports_empty_sections_and_the_heartbeat_gap() {
        let rendered = render_detail(&to_client(dto::GetNodeResponse {
            summary: sample_summary(),
            host: None,
            detected_capacity: None,
            active_attempts: Vec::new(),
            accrual_queue: Vec::new(),
        }));
        assert!(rendered.contains("epoch           3"), "{rendered}");
        // A missing heartbeat is a missing input, not a dead node.
        assert!(
            rendered.contains("last heartbeat  (agents do not report yet)"),
            "{rendered}"
        );
        assert!(rendered.contains("labels          zone=a"), "{rendered}");
        assert!(rendered.contains("attempts:       (none)"), "{rendered}");
        assert!(rendered.contains("accruing:       (none)"), "{rendered}");
    }

    #[test]
    fn detail_shows_the_interruption_deadline_only_when_present() {
        let none = render_detail(&to_client(dto::GetNodeResponse {
            summary: sample_summary(),
            host: None,
            detected_capacity: None,
            active_attempts: Vec::new(),
            accrual_queue: Vec::new(),
        }));
        assert!(!none.contains("interruption"), "{none}");

        let mut summary = sample_summary();
        summary.interruption_deadline = Some(
            coppice_core::time::Timestamp::from_micros(1_700_000_000_000_000).expect("in range"),
        );
        let listed = render_list(&to_client(dto::ListNodesResponse {
            nodes: vec![summary.clone()],
        }));
        assert!(listed.contains("interruption pending"), "{listed}");
        let shown = render_detail(&to_client(dto::GetNodeResponse {
            summary,
            host: None,
            detected_capacity: None,
            active_attempts: Vec::new(),
            accrual_queue: Vec::new(),
        }));
        assert!(shown.contains("interruption    deadline 2023-"), "{shown}");
    }

    #[test]
    fn admin_args_name_every_missing_flag_at_once() {
        let err = admin_args(
            None,
            None,
            None,
            None,
            None,
            coppice_coordinator::cli::NodeVerb::EnrollToken {
                verb: coppice_coordinator::cli::EnrollTokenVerb::List,
            },
        )
        .expect_err("missing connection flags are refused");
        let message = format!("{err:#}");
        for flag in ["--target", "--ca", "--cert", "--key"] {
            assert!(message.contains(flag), "{message} is missing {flag}");
        }
    }

    #[test]
    fn admin_args_pass_the_connection_flags_through() {
        let args = admin_args(
            Some("coord-1:7071".to_string()),
            Some(PathBuf::from("ca.crt")),
            Some(PathBuf::from("op.crt")),
            Some(PathBuf::from("op.key")),
            Some("cluster-x".to_string()),
            coppice_coordinator::cli::NodeVerb::EnrollToken {
                verb: coppice_coordinator::cli::EnrollTokenVerb::List,
            },
        )
        .expect("complete connection flags are accepted");
        assert_eq!(args.target, "coord-1:7071");
        assert_eq!(args.ca, PathBuf::from("ca.crt"));
        assert_eq!(args.cert, PathBuf::from("op.crt"));
        assert_eq!(args.key, PathBuf::from("op.key"));
        assert_eq!(args.cluster_id.as_deref(), Some("cluster-x"));
    }

    /// The detail render must not silently drop an accrual whose full funding
    /// is unbounded; `projected_start: None` is a real, different claim.
    #[test]
    fn unbounded_projected_start_is_spelled_out() {
        let rendered = render_detail(&to_client(dto::GetNodeResponse {
            summary: sample_summary(),
            host: None,
            detected_capacity: None,
            active_attempts: Vec::new(),
            accrual_queue: vec![dto::AccrualView {
                allocation: dto::AllocationView {
                    id: "alloc-00000000-0000-0000-0000-000000000001"
                        .parse()
                        .unwrap(),
                    job: "job-00000000-0000-0000-0000-000000000001".parse().unwrap(),
                    attempt: "attempt-00000000-0000-0000-0000-000000000001"
                        .parse()
                        .unwrap(),
                    node: dto_node_id(),
                    state: dto::AllocationState::Accruing,
                    requested: dto::Resources {
                        cpu_millis: 500,
                        memory_bytes: 0,
                        disk_bytes: 0,
                    },
                    funded: dto::Resources {
                        cpu_millis: 250,
                        memory_bytes: 0,
                        disk_bytes: 0,
                    },
                    seq: 1,
                },
                funded_fraction: dto::FundedFraction {
                    cpu: 0.5,
                    memory: 1.0,
                    disk: 1.0,
                },
                projected_start: None,
            }],
        }));
        assert!(rendered.contains("projected start unbounded"), "{rendered}");
    }

    /// A `GetNodeResponse` with the given running/accruing counts, otherwise
    /// identical to [`sample_summary`] — what the `--wait` poll decodes.
    fn get_node_response(running: u32, accruing: u32) -> dto::GetNodeResponse {
        drained_node_response(running, accruing, None, running == 0 && accruing == 0)
    }

    /// A drained node's detail, with the drain's derived status.
    fn drained_node_response(
        running: u32,
        accruing: u32,
        horizon: Option<coppice_core::time::Timestamp>,
        complete: bool,
    ) -> dto::GetNodeResponse {
        dto::GetNodeResponse {
            summary: dto::NodeSummary {
                running_count: running,
                accruing_count: accruing,
                drain: Some(dto::NodeDrain {
                    requested_at: coppice_core::time::Timestamp::UNIX_EPOCH,
                    horizon,
                    complete,
                }),
                ..sample_summary()
            },
            host: None,
            detected_capacity: None,
            active_attempts: Vec::new(),
            accrual_queue: Vec::new(),
        }
    }

    #[tokio::test]
    async fn drain_posts_to_the_drain_path() {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let router = Router::new()
            .route(
                "/api/v1/nodes/:node/drain",
                post(
                    |AxumPath(node): AxumPath<String>,
                     State(seen): State<Arc<Mutex<Vec<String>>>>| async move {
                        seen.lock().unwrap().push(node);
                        Json(serde_json::json!({}))
                    },
                ),
            )
            .with_state(seen.clone());
        let base = spawn(router).await;

        drain(&Client::new(&base).unwrap(), node_id(), None, false)
            .await
            .expect("drain succeeds");
        assert_eq!(*seen.lock().unwrap(), [node_id().to_string()]);
    }

    #[tokio::test]
    async fn undrain_posts_to_the_undrain_path() {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let router = Router::new()
            .route(
                "/api/v1/nodes/:node/undrain",
                post(
                    |AxumPath(node): AxumPath<String>,
                     State(seen): State<Arc<Mutex<Vec<String>>>>| async move {
                        seen.lock().unwrap().push(node);
                        Json(serde_json::json!({}))
                    },
                ),
            )
            .with_state(seen.clone());
        let base = spawn(router).await;

        undrain(&Client::new(&base).unwrap(), node_id(), false)
            .await
            .expect("undrain succeeds");
        assert_eq!(*seen.lock().unwrap(), [node_id().to_string()]);
    }

    #[tokio::test]
    async fn remove_posts_to_the_remove_path() {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let router = Router::new()
            .route(
                "/api/v1/nodes/:node/remove",
                post(
                    |AxumPath(node): AxumPath<String>,
                     State(seen): State<Arc<Mutex<Vec<String>>>>| async move {
                        seen.lock().unwrap().push(node);
                        Json(serde_json::json!({}))
                    },
                ),
            )
            .with_state(seen.clone());
        let base = spawn(router).await;

        remove(&Client::new(&base).unwrap(), node_id(), false)
            .await
            .expect("remove succeeds");
        assert_eq!(*seen.lock().unwrap(), [node_id().to_string()]);
    }

    #[tokio::test]
    async fn drain_json_prints_the_servers_body_and_still_posts() {
        let router = Router::new().route(
            "/api/v1/nodes/:node/drain",
            post(|| async { Json(serde_json::json!({})) }),
        );
        let base = spawn(router).await;
        drain(&Client::new(&base).unwrap(), node_id(), None, true)
            .await
            .expect("drain --json succeeds");
    }

    #[tokio::test]
    async fn an_error_body_surfaces_its_code_and_message() {
        let router = Router::new().route(
            "/api/v1/nodes/:node/remove",
            post(|| async {
                (
                    axum::http::StatusCode::CONFLICT,
                    Json(error_body("NODE_BUSY", "node still has running attempts")),
                )
            }),
        );
        let base = spawn(router).await;
        let err = remove(&Client::new(&base).unwrap(), node_id(), false)
            .await
            .expect_err("remove fails");
        let message = format!("{err:#}");
        assert!(message.contains("NODE_BUSY"), "{message}");
        assert!(message.contains("running attempts"), "{message}");
    }

    #[tokio::test]
    async fn wait_for_drain_returns_once_the_drain_is_complete() {
        let calls: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
        let router = Router::new()
            .route(
                "/api/v1/nodes/:node",
                get(|State(calls): State<Arc<Mutex<u32>>>| async move {
                    let mut calls = calls.lock().unwrap();
                    *calls += 1;
                    let body = if *calls == 1 {
                        get_node_response(1, 2)
                    } else {
                        get_node_response(0, 0)
                    };
                    Json(serde_json::to_value(body).unwrap())
                }),
            )
            .with_state(calls.clone());
        let base = spawn(router).await;

        // A short interval keeps this fast: at most one sleep of a few
        // milliseconds between the two polls.
        wait_for_drain(
            &Client::new(&base).unwrap(),
            node_id(),
            Duration::from_secs(60),
            Duration::from_millis(1),
        )
        .await
        .expect("wait succeeds once the drain is complete");
        assert_eq!(*calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn wait_for_drain_errors_naming_what_is_outstanding_when_the_deadline_elapses() {
        let router = Router::new().route(
            "/api/v1/nodes/:node",
            get(|| async { Json(serde_json::to_value(get_node_response(1, 2)).unwrap()) }),
        );
        let base = spawn(router).await;

        let err = wait_for_drain(
            &Client::new(&base).unwrap(),
            node_id(),
            Duration::ZERO,
            Duration::from_millis(1),
        )
        .await
        .expect_err("wait fails when the node never drains");
        let message = format!("{err:#}");
        assert!(message.contains(&node_id().to_string()), "{message}");
        assert!(message.contains("running 1"), "{message}");
        assert!(message.contains("accruing 2"), "{message}");
    }

    /// The `--wait` poll's `GetNodeResponse`, client-typed — what
    /// `drain_progress` actually takes.
    fn client(response: dto::GetNodeResponse) -> GetNodeResponse {
        to_client(response)
    }

    #[test]
    fn drain_progress_reads_the_derived_status() {
        let horizon = coppice_core::time::Timestamp::UNIX_EPOCH
            + coppice_core::time::Duration::from_secs(3_600);
        let bounded =
            drain_progress(&client(drained_node_response(1, 0, Some(horizon), false)).summary);
        assert!(!bounded.complete);
        assert!(
            bounded.describe().starts_with("empty by "),
            "{}",
            bounded.describe()
        );
        let unbounded = drain_progress(&client(drained_node_response(1, 2, None, false)).summary);
        assert_eq!(
            unbounded.describe(),
            "horizon unbounded, running 1, accruing 2"
        );
        assert!(drain_progress(&client(drained_node_response(0, 0, None, true)).summary).complete);
        // Not yet visible on this replica: pending, never complete.
        let unseen = drain_progress(&to_client::<_, NodeSummary>(sample_summary()));
        assert!(!unseen.complete);
        assert!(unseen.describe().starts_with("drain not recorded"));
    }

    #[test]
    fn detail_shows_the_drain_and_the_agent_separately() {
        let mut response = drained_node_response(1, 0, None, false);
        response.summary.agent_draining = true;
        let rendered = render_detail(&client(response));
        assert!(rendered.contains("drained, agent draining"), "{rendered}");
        assert!(rendered.contains("unbounded"), "{rendered}");
        assert!(rendered.contains("draining (shutting down)"), "{rendered}");
    }
}
