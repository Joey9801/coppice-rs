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

use anyhow::{bail, Context as _, Result};

use coppice_api::http::dto;
use coppice_core::bytes::ByteSize;
use coppice_core::id::NodeId;

use crate::client::{ctx, print_json, render_table, ApiClient, ApiConnection, Query};

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
    /// The mTLS admin verbs (`enroll-token`, `revoke-identity`), owned by the
    /// coordinator crate and flattened in whole so this CLI never restates
    /// their contract.
    #[command(flatten)]
    Admin(coppice_coordinator::cli::NodeVerb),
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
async fn list(client: &ApiClient, json: bool) -> Result<()> {
    let body: serde_json::Value = client
        .get_json(
            "/nodes",
            &Vec::new(),
            ctx("listing nodes", "reading the node list"),
        )
        .await?;
    if json {
        print_json(&body);
        return Ok(());
    }
    let response: dto::ListNodesResponse =
        serde_json::from_value(body).context("reading the node list")?;
    print!("{}", render_list(&response));
    Ok(())
}

/// `coppice node show`: one node's summary, its live attempts, and the
/// allocations still accruing against it.
async fn show(client: &ApiClient, node: NodeId, json: bool) -> Result<()> {
    let query: Query = Vec::new();
    let body: serde_json::Value = client
        .get_json(
            &format!("/nodes/{node}"),
            &query,
            ctx("fetching node status", "reading node detail"),
        )
        .await?;
    if json {
        print_json(&body);
        return Ok(());
    }
    let response: dto::GetNodeResponse =
        serde_json::from_value(body).context("reading node detail")?;
    print!("{}", render_detail(&response));
    Ok(())
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Node liveness, in the wire's own vocabulary. `unknown` is the only value a
/// coordinator produces today (the replicated state records no health input),
/// so the label is deliberately not softened into "healthy".
fn health_label(health: dto::NodeHealth) -> &'static str {
    match health {
        dto::NodeHealth::Unknown => "unknown",
        dto::NodeHealth::Healthy => "healthy",
        dto::NodeHealth::Lost => "lost",
    }
}

/// The scheduling posture: `schedulable: false` is a drain, not a fault —
/// running work continues, only new placements stop — so it gets its own word.
fn schedulable_label(schedulable: bool) -> &'static str {
    if schedulable {
        "schedulable"
    } else {
        "draining"
    }
}

/// One resource triple on a single line, byte dimensions humanized.
fn resources(r: &dto::Resources) -> String {
    format!(
        "cpu {} mCPU, memory {}, disk {}",
        r.cpu_millis,
        ByteSize::from_bytes(r.memory_bytes),
        ByteSize::from_bytes(r.disk_bytes),
    )
}

/// A compact `cpu/mem/disk` cell for the list table, where a full sentence per
/// dimension would not fit.
fn resources_cell(r: &dto::Resources) -> String {
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
fn render_list(response: &dto::ListNodesResponse) -> String {
    if response.nodes.is_empty() {
        return "(no nodes registered)\n".to_string();
    }
    let rows: Vec<Vec<String>> = response
        .nodes
        .iter()
        .map(|node| {
            vec![
                node.id.to_string(),
                health_label(node.health).to_string(),
                schedulable_label(node.schedulable).to_string(),
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
fn render_detail(response: &dto::GetNodeResponse) -> String {
    use std::fmt::Write;

    let summary = &response.summary;
    let mut out = String::new();
    let kv = |out: &mut String, key: &str, value: &str| {
        let _ = writeln!(out, "{key:<16}{value}");
    };

    kv(&mut out, "id", &summary.id.to_string());
    kv(&mut out, "health", health_label(summary.health));
    kv(
        &mut out,
        "scheduling",
        schedulable_label(summary.schedulable),
    );
    kv(&mut out, "epoch", &summary.epoch.to_string());
    kv(&mut out, "capacity", &resources(&summary.capacity));
    kv(&mut out, "allocated", &resources(&summary.allocated));
    kv(&mut out, "used", &resources(&summary.used));
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
    use axum::routing::get;
    use axum::{Json, Router};

    use crate::testsupport::{error_body, spawn};

    fn node_id() -> NodeId {
        "node-00000000-0000-0000-0000-000000000001".parse().unwrap()
    }

    fn sample_summary() -> dto::NodeSummary {
        let mut labels = BTreeMap::new();
        labels.insert("zone".to_string(), "a".to_string());
        dto::NodeSummary {
            id: node_id(),
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
            used: dto::Resources {
                cpu_millis: 0,
                memory_bytes: 0,
                disk_bytes: 0,
            },
            labels,
            schedulable: true,
            health: dto::NodeHealth::Unknown,
            epoch: 3,
            last_heartbeat: None,
            running_count: 1,
            accruing_count: 2,
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

        list(&ApiClient::new(&base).unwrap(), false)
            .await
            .expect("list succeeds");
        assert_eq!(*seen.lock().unwrap(), 1);
    }

    #[test]
    fn list_renders_the_node_row() {
        let rendered = render_list(&dto::ListNodesResponse {
            nodes: vec![sample_summary()],
        });
        assert!(rendered.contains(&node_id().to_string()), "{rendered}");
        // Health is `unknown` today and must not be dressed up as healthy.
        assert!(rendered.contains("unknown"), "{rendered}");
        assert!(rendered.contains("schedulable"), "{rendered}");
        assert!(rendered.contains("4000m"), "{rendered}");
    }

    #[test]
    fn list_says_so_when_no_nodes_are_registered() {
        let rendered = render_list(&dto::ListNodesResponse { nodes: Vec::new() });
        assert_eq!(rendered, "(no nodes registered)\n");
    }

    #[tokio::test]
    async fn show_puts_the_node_id_in_the_path() {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let body = serde_json::to_value(dto::GetNodeResponse {
            summary: sample_summary(),
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

        show(&ApiClient::new(&base).unwrap(), node_id(), false)
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
        let err = show(&ApiClient::new(&base).unwrap(), node_id(), false)
            .await
            .expect_err("show fails");
        let message = format!("{err:#}");
        assert!(message.contains("NOT_FOUND"), "{message}");
        assert!(message.contains("not found"), "{message}");
    }

    #[test]
    fn detail_reports_empty_sections_and_the_heartbeat_gap() {
        let rendered = render_detail(&dto::GetNodeResponse {
            summary: sample_summary(),
            active_attempts: Vec::new(),
            accrual_queue: Vec::new(),
        });
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
        let rendered = render_detail(&dto::GetNodeResponse {
            summary: sample_summary(),
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
                    node: node_id(),
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
        });
        assert!(rendered.contains("projected start unbounded"), "{rendered}");
    }
}
