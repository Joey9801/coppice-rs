//! `coppice cluster`: the whole-cluster read verbs.
//!
//! Today that is one verb, `status`, which is the operator's first question —
//! "is this cluster healthy, and what is it doing?" — answered from the three
//! endpoints that between them hold the answer: `/overview` (identity and
//! capacity), `/queue/stats` (backlog and flow), and `/coordinators` (the raft
//! view from the replica that served the request).
//!
//! Every figure is "as this replica sees it". The reads are separate requests
//! against separate consistency classes (bounded for the replicated state,
//! eventual for the derived queue window and the local raft metrics), so the
//! three sections are not a single atomic snapshot and the render never claims
//! they are.

use anyhow::{Context as _, Result};

use coppice_api::http::dto;
use coppice_core::bytes::ByteSize;

use crate::client::{ctx, print_json, render_table, ApiClient, ApiConnection, Query};

/// `coppice cluster` argument group. `--api` is global, matching `coppice job`.
#[derive(Debug, clap::Args)]
pub struct ClusterArgs {
    #[command(flatten)]
    connection: ApiConnection,

    #[command(subcommand)]
    pub command: ClusterCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum ClusterCommand {
    /// Summarize the cluster: identity, capacity, queue, and coordinators.
    Status {
        /// Print the combined JSON (`{overview, queue, coordinators}`) instead
        /// of the summary.
        #[arg(long)]
        json: bool,
    },
}

/// Run the selected `coppice cluster` verb.
pub async fn run(args: ClusterArgs) -> Result<()> {
    let client = args.connection.client()?;
    match args.command {
        ClusterCommand::Status { json } => status(&client, json).await,
    }
}

/// `coppice cluster status`: fetch the three reads and render them as one
/// summary (or, with `--json`, as one combined object).
async fn status(client: &ApiClient, json: bool) -> Result<()> {
    let empty: Query = Vec::new();
    let overview: serde_json::Value = client
        .get_json(
            "/overview",
            &empty,
            ctx(
                "fetching the cluster overview",
                "reading the cluster overview",
            ),
        )
        .await?;
    let queue: serde_json::Value = client
        .get_json(
            "/queue/stats",
            &empty,
            ctx("fetching queue stats", "reading queue stats"),
        )
        .await?;
    let coordinators: serde_json::Value = client
        .get_json(
            "/coordinators",
            &empty,
            ctx("fetching coordinator status", "reading coordinator status"),
        )
        .await?;

    if json {
        print_json(&serde_json::json!({
            "overview": overview,
            "queue": queue,
            "coordinators": coordinators,
        }));
        return Ok(());
    }

    let overview: dto::GetClusterOverviewResponse =
        serde_json::from_value(overview).context("reading the cluster overview")?;
    let queue: dto::QueueStats = serde_json::from_value(queue).context("reading queue stats")?;
    let coordinators: dto::GetCoordinatorStatusResponse =
        serde_json::from_value(coordinators).context("reading coordinator status")?;
    print!("{}", render_status(&overview, &queue, &coordinators));
    Ok(())
}

/// Render the three reads as one plain-text summary: a key/value block for the
/// cluster and its raft position, one for capacity, one for the queue, then the
/// member roster as a table.
fn render_status(
    overview: &dto::GetClusterOverviewResponse,
    queue: &dto::QueueStats,
    coordinators: &dto::GetCoordinatorStatusResponse,
) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let kv = |out: &mut String, key: &str, value: &str| {
        let _ = writeln!(out, "{key:<16}{value}");
    };

    // Both reads report the cluster this replica belongs to, so they agree in
    // every sane world. Showing the disagreement rather than silently
    // preferring one is the only honest render if they ever do not.
    let cluster = if overview.cluster_id == coordinators.cluster_id {
        overview.cluster_id.to_string()
    } else {
        format!(
            "{} (the coordinator read reports {})",
            overview.cluster_id, coordinators.cluster_id
        )
    };
    kv(&mut out, "cluster", &cluster);
    kv(
        &mut out,
        "leader",
        &coordinators
            .leader
            .map(|id| id.to_string())
            .unwrap_or_else(|| "(none known)".to_string()),
    );
    kv(&mut out, "term", &coordinators.term.to_string());
    kv(
        &mut out,
        "log",
        &format!(
            "committed {}, applied {} (state version {})",
            coordinators.known_committed, coordinators.last_applied, coordinators.state_version
        ),
    );
    match &coordinators.snapshot {
        Some(snapshot) => kv(
            &mut out,
            "snapshot",
            &format!(
                "covers index {}, {} entries since{}",
                snapshot.last_included_index,
                snapshot.entries_since_snapshot,
                // Both are always null today (SnapshotMeta records neither);
                // print them only if a future coordinator starts filling them.
                {
                    let mut extra = String::new();
                    if let Some(size) = snapshot.size_bytes {
                        let _ = write!(extra, ", {}", ByteSize::from_bytes(size));
                    }
                    if let Some(taken) = snapshot.taken_at {
                        let _ = write!(extra, ", taken {taken}");
                    }
                    extra
                }
            ),
        ),
        None => kv(&mut out, "snapshot", "(none taken on this replica)"),
    }
    kv(
        &mut out,
        "state",
        &format!(
            "{} jobs, {} attempts, {} allocations, {} nodes, {} quota entities",
            coordinators.state_counts.jobs,
            coordinators.state_counts.attempts,
            coordinators.state_counts.allocations,
            coordinators.state_counts.nodes,
            coordinators.state_counts.quota_entities,
        ),
    );

    let capacity = &overview.capacity;
    let _ = writeln!(out);
    kv(
        &mut out,
        "nodes",
        &format!(
            "{} total, {} schedulable, {} lost",
            capacity.nodes.total, capacity.nodes.schedulable, capacity.nodes.lost
        ),
    );
    kv(&mut out, "capacity", &resources(&capacity.capacity));
    kv(&mut out, "allocated", &resources(&capacity.allocated));
    kv(&mut out, "used", &resources(&capacity.used));

    let _ = writeln!(out);
    kv(&mut out, "queue depth", &queue.depth.to_string());
    // A null rate is a coverage gap in the derived window, not zero flow — say
    // so rather than printing a `0.0` that reads as "nothing is moving".
    kv(&mut out, "drain rate", &rate(queue.drain_rate_per_minute));
    kv(
        &mut out,
        "arrival rate",
        &rate(queue.arrival_rate_per_minute),
    );
    kv(
        &mut out,
        "oldest queued",
        &queue
            .oldest_queued_age_seconds
            .map(|s| format!("{s}s"))
            .unwrap_or_else(|| "(nothing queued)".to_string()),
    );
    let by_state: Vec<String> = queue
        .by_state
        .iter()
        .map(|(phase, count)| format!("{} {count}", phase_label(*phase)))
        .collect();
    kv(&mut out, "jobs by phase", &by_state.join(", "));

    let _ = writeln!(out);
    if coordinators.members.is_empty() {
        out.push_str("coordinators: (none reported)\n");
        return out;
    }
    out.push_str("coordinators:\n");
    let rows: Vec<Vec<String>> = coordinators
        .members
        .iter()
        .map(|m| {
            vec![
                m.id.to_string(),
                role_label(m.role).to_string(),
                m.addr.clone(),
                if m.voter { "voter" } else { "learner" }.to_string(),
                m.last_applied
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                m.replication_lag_entries
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ]
        })
        .collect();
    out.push_str(&indent(&render_table(
        &["id", "role", "addr", "membership", "applied", "lag"],
        &rows,
    )));
    out
}

/// The wire spelling of a coordinator role, so the CLI and the JSON agree.
fn role_label(role: dto::CoordinatorRole) -> &'static str {
    match role {
        dto::CoordinatorRole::Leader => "leader",
        dto::CoordinatorRole::Follower => "follower",
        dto::CoordinatorRole::Learner => "learner",
    }
}

/// A rate per minute, or the honest "unknown" a null carries.
fn rate(per_minute: Option<f64>) -> String {
    match per_minute {
        Some(rate) => format!("{rate:.2}/min"),
        None => "(no window coverage)".to_string(),
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

/// The wire spelling of a display phase, so the CLI and the JSON agree.
pub fn phase_label(phase: dto::JobPhase) -> &'static str {
    use dto::JobPhase as P;
    match phase {
        P::Submitted => "submitted",
        P::Accepted => "accepted",
        P::Queued => "queued",
        P::Preparing => "preparing",
        P::Running => "running",
        P::Finalizing => "finalizing",
        P::Succeeded => "succeeded",
        P::Failed => "failed",
        P::Aborted => "aborted",
    }
}

/// Indent every line of a block by two spaces (for a table nested under a
/// section header).
pub fn indent(block: &str) -> String {
    block
        .lines()
        .map(|line| format!("  {line}\n"))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use axum::routing::get;
    use axum::{Json, Router};

    use coppice_core::id::ClusterId;

    use crate::testsupport::{error_body, spawn};

    /// One fixed cluster id, so the render assertions can name it.
    fn cluster_id() -> ClusterId {
        "cluster-00000000-0000-0000-0000-0000000000aa"
            .parse()
            .unwrap()
    }

    fn sample_queue() -> dto::QueueStats {
        let mut by_state = BTreeMap::new();
        for phase in dto::JobPhase::ALL {
            by_state.insert(phase, 0);
        }
        by_state.insert(dto::JobPhase::Queued, 3);
        dto::QueueStats {
            depth: 3,
            drain_rate_per_minute: Some(1.5),
            arrival_rate_per_minute: None,
            oldest_queued_age_seconds: Some(42),
            by_state,
            history: Vec::new(),
        }
    }

    fn sample_overview() -> dto::GetClusterOverviewResponse {
        dto::GetClusterOverviewResponse {
            cluster_id: cluster_id(),
            queue: sample_queue(),
            capacity: dto::ClusterCapacity {
                nodes: dto::NodeCounts {
                    total: 2,
                    schedulable: 1,
                    lost: 0,
                },
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
            },
            recent_events: dto::RecentEventsWindow {
                floor_index: 7,
                events: Vec::new(),
            },
        }
    }

    fn sample_coordinators() -> dto::GetCoordinatorStatusResponse {
        dto::GetCoordinatorStatusResponse {
            cluster_id: cluster_id(),
            leader: Some(1),
            term: 9,
            known_committed: 120,
            last_applied: 118,
            state_version: 118,
            snapshot: Some(dto::CoordinatorSnapshot {
                size_bytes: None,
                last_included_index: 100,
                taken_at: None,
                entries_since_snapshot: 18,
            }),
            state_counts: dto::CoordinatorStateCounts {
                jobs: 4,
                attempts: 5,
                allocations: 6,
                nodes: 2,
                quota_entities: 1,
            },
            members: vec![dto::CoordinatorMember {
                id: 1,
                addr: "10.0.0.1:7071".to_string(),
                role: dto::CoordinatorRole::Leader,
                voter: true,
                last_applied: Some(118),
                replication_lag_entries: None,
            }],
        }
    }

    /// The DTO must survive its own serialize→deserialize round trip — this is
    /// what lets the CLI decode the contract type directly instead of keeping
    /// a local mirror.
    #[test]
    fn coordinator_dto_round_trips() {
        let value = serde_json::to_value(sample_coordinators()).unwrap();
        let decoded: dto::GetCoordinatorStatusResponse =
            serde_json::from_value(value).expect("the DTO decodes its own output");
        assert_eq!(decoded.cluster_id, cluster_id());
        assert_eq!(decoded.leader, Some(1));
        assert_eq!(decoded.members.len(), 1);
        assert_eq!(decoded.members[0].role, dto::CoordinatorRole::Leader);
    }

    /// The paths a spawned fake server was asked for, in order.
    type Seen = Arc<Mutex<Vec<String>>>;

    /// One route serving a canned body and recording that it was hit.
    fn recording_route(seen: &Seen, path: &'static str, body: serde_json::Value) -> Router {
        let seen = seen.clone();
        Router::new().route(
            &format!("/api/v1{path}"),
            get(move || {
                let (seen, body) = (seen.clone(), body.clone());
                async move {
                    seen.lock().unwrap().push(path.to_string());
                    Json(body)
                }
            }),
        )
    }

    /// A router serving all three status reads, recording the paths it saw.
    fn status_router(seen: &Seen) -> Router {
        recording_route(
            seen,
            "/overview",
            serde_json::to_value(sample_overview()).unwrap(),
        )
        .merge(recording_route(
            seen,
            "/queue/stats",
            serde_json::to_value(sample_queue()).unwrap(),
        ))
        .merge(recording_route(
            seen,
            "/coordinators",
            serde_json::to_value(sample_coordinators()).unwrap(),
        ))
    }

    #[tokio::test]
    async fn status_reads_all_three_endpoints() {
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let base = spawn(status_router(&seen)).await;
        status(&ApiClient::new(&base).unwrap(), false)
            .await
            .expect("status succeeds");
        let mut seen = seen.lock().unwrap().clone();
        seen.sort();
        assert_eq!(seen, ["/coordinators", "/overview", "/queue/stats"]);
    }

    /// `--json` takes the same three reads and must not fail on a body the
    /// human render would parse — it is a pass-through, not a re-render.
    #[tokio::test]
    async fn status_json_reads_the_same_three_endpoints() {
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let base = spawn(status_router(&seen)).await;
        status(&ApiClient::new(&base).unwrap(), true)
            .await
            .expect("status --json succeeds");
        assert_eq!(seen.lock().unwrap().len(), 3);
    }

    #[test]
    fn render_names_the_cluster_queue_and_members() {
        let rendered = render_status(&sample_overview(), &sample_queue(), &sample_coordinators());
        assert!(
            rendered.contains(&format!("cluster         {}", cluster_id())),
            "{rendered}"
        );
        assert!(rendered.contains("leader          1"), "{rendered}");
        assert!(
            rendered.contains("log             committed 120, applied 118"),
            "{rendered}"
        );
        assert!(rendered.contains("queue depth     3"), "{rendered}");
        assert!(rendered.contains("drain rate      1.50/min"), "{rendered}");
        // A null rate must read as a gap, never as zero flow.
        assert!(
            rendered.contains("arrival rate    (no window coverage)"),
            "{rendered}"
        );
        assert!(rendered.contains("queued 3"), "{rendered}");
        assert!(rendered.contains("10.0.0.1:7071"), "{rendered}");
    }

    #[tokio::test]
    async fn status_surfaces_an_error_body() {
        let base = spawn(Router::new().route(
            "/api/v1/overview",
            get(|| async {
                (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    Json(error_body("UNAVAILABLE", "no consensus handle attached")),
                )
            }),
        ))
        .await;
        let err = status(&ApiClient::new(&base).unwrap(), false)
            .await
            .expect_err("status fails");
        let message = format!("{err:#}");
        assert!(message.contains("UNAVAILABLE"), "{message}");
        assert!(message.contains("no consensus handle"), "{message}");
    }
}
