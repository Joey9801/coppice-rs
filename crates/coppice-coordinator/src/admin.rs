//! The membership admin surface (ADR 0016).
//!
//! Two halves share this module. The **server** ([`AdminService`]) implements
//! the generated `RaftAdminService` over the local [`Consensus`] seam and
//! [`NodeHandle`]; `bootstrap` mounts it on the coordinator's mTLS server next
//! to the Raft transport. The **client** helpers ([`admin_channel`] and the
//! per-verb wrappers) dial that surface over mTLS; the CLI ([`run_cli`]) and
//! the multi-node integration test share them, so the poll-until-caught-up
//! promotion loop lives in exactly one place.
//!
//! Every RPC first checks the request's stamped cluster identity (ADR 0016)
//! before touching Raft, mirroring the transport handler in `coppice-consensus`.

// tonic's generated service trait returns `Result<_, Status>`; `Status` is a
// large error type, and the signatures here are dictated by that trait.
#![allow(clippy::result_large_err)]

use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::{Code, Request, Response, Status};

use coppice_consensus::{ClusterSummary, Consensus, ConsensusError, CoordinatorId, NodeHandle};
use coppice_net::admin::{Client, RaftAdminService};
use coppice_proto::pb::raft::v1 as pb;

use crate::cli::{AdminArgs, AdminVerb};
use crate::config;

/// How often the promotion wrapper retries while a learner is still catching up.
const PROMOTE_POLL_INTERVAL: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

/// Serves the membership admin RPCs over the local consensus seam (ADR 0016).
pub struct AdminService<C: Consensus> {
    consensus: Arc<C>,
    handle: NodeHandle,
    cluster_uuid: [u8; 16],
}

impl<C: Consensus> AdminService<C> {
    /// Bind the service to the local consensus seam, admin handle, and this
    /// node's stamped cluster identity.
    pub fn new(consensus: Arc<C>, handle: NodeHandle, cluster_uuid: [u8; 16]) -> Self {
        AdminService {
            consensus,
            handle,
            cluster_uuid,
        }
    }

    /// Refuse a request that is malformed or stamped for a different cluster
    /// (ADR 0016), before any Raft state is touched.
    fn check_cluster(&self, incoming: &[u8]) -> Result<(), Status> {
        if incoming.len() != 16 {
            return Err(Status::invalid_argument(format!(
                "cluster_uuid must be 16 bytes, got {} (ADR 0016)",
                incoming.len()
            )));
        }
        if incoming != self.cluster_uuid {
            return Err(Status::failed_precondition(format!(
                "request is from cluster {}, this node is stamped for cluster {} — \
                 cross-cluster admin contact refused (ADR 0016)",
                hex(incoming),
                hex(&self.cluster_uuid),
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl<C: Consensus> RaftAdminService for AdminService<C> {
    async fn add_learner(
        &self,
        request: Request<pb::AddLearnerRequest>,
    ) -> Result<Response<pb::AddLearnerResponse>, Status> {
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;
        self.consensus
            .add_learner(req.node_id, req.address)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::AddLearnerResponse {}))
    }

    async fn promote_voter(
        &self,
        request: Request<pb::PromoteVoterRequest>,
    ) -> Result<Response<pb::PromoteVoterResponse>, Status> {
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;
        self.consensus
            .promote_voter(req.promote_node_id, req.remove_node_id)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::PromoteVoterResponse {}))
    }

    async fn remove_node(
        &self,
        request: Request<pb::RemoveNodeRequest>,
    ) -> Result<Response<pb::RemoveNodeResponse>, Status> {
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;
        self.consensus
            .remove_node(req.node_id)
            .await
            .map_err(consensus_error_to_status)?;
        Ok(Response::new(pb::RemoveNodeResponse {}))
    }

    async fn cluster_status(
        &self,
        request: Request<pb::ClusterStatusRequest>,
    ) -> Result<Response<pb::ClusterStatusResponse>, Status> {
        let req = request.into_inner();
        self.check_cluster(&req.cluster_uuid)?;
        Ok(Response::new(cluster_summary_to_pb(
            self.handle.cluster_summary(),
        )))
    }
}

/// Map a consensus-seam failure onto the gRPC status the admin RPC returns.
///
/// The retryable variants (ADR 0016) become `FAILED_PRECONDITION` /`ABORTED` /
/// `DEADLINE_EXCEEDED` so a caller can branch and retry; terminal ones become
/// `UNAVAILABLE` / `INTERNAL`. The `LearnerNotCaughtUp` message deliberately
/// contains "behind" — the promotion client keys its poll loop on it.
fn consensus_error_to_status(err: ConsensusError) -> Status {
    match err {
        ConsensusError::NotLeader { leader: Some(id) } => Status::failed_precondition(format!(
            "not the leader; current leader is node {id} — retarget the request"
        )),
        ConsensusError::NotLeader { leader: None } => Status::failed_precondition(
            "not the leader; no leader currently known (election in progress) — retry",
        ),
        ConsensusError::LearnerNotCaughtUp { lag } => Status::failed_precondition(format!(
            "learner is {lag} entries behind; retry after catch-up (ADR 0016)"
        )),
        ConsensusError::MembershipInProgress => Status::aborted(
            "a membership change is already in progress; only one may be outstanding (ADR 0016)",
        ),
        ConsensusError::Timeout => {
            Status::deadline_exceeded("consensus operation timed out; outcome unknown")
        }
        ConsensusError::Shutdown => Status::unavailable("consensus is shutting down"),
        ConsensusError::Fatal(msg) => Status::internal(format!("consensus fault: {msg}")),
    }
}

/// Convert a [`ClusterSummary`] into the `ClusterStatus` response wire form.
///
/// Canonicalizes per `raft.proto`: voters into a single ascending
/// [`VoterConfig`](pb::VoterConfig), members ascending by `node_id`.
pub fn cluster_summary_to_pb(summary: ClusterSummary) -> pb::ClusterStatusResponse {
    let mut voters: Vec<u64> = summary
        .members
        .iter()
        .filter(|m| m.voter)
        .map(|m| m.id)
        .collect();
    voters.sort_unstable();

    let mut members: Vec<pb::RaftMember> = summary
        .members
        .iter()
        .map(|m| pb::RaftMember {
            node_id: m.id,
            address: m.addr.clone(),
        })
        .collect();
    members.sort_by_key(|m| m.node_id);

    let replication = summary
        .replication
        .iter()
        .map(|(id, matched)| pb::ReplicationProgress {
            node_id: *id,
            matched_index: *matched,
        })
        .collect();

    pb::ClusterStatusResponse {
        local_node_id: summary.local_id,
        leader_node_id: summary.leader,
        term: summary.term,
        last_applied_index: summary.last_applied,
        known_committed_index: summary.known_committed,
        membership: Some(pb::Membership {
            configs: vec![pb::VoterConfig { voters }],
            members,
        }),
        replication,
    }
}

/// Lowercase hex of raw identity bytes, for operator-facing messages.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String is infallible.
        let _ = write!(out, "{b:02x}");
    }
    out
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

/// Dial the admin surface of `target` (`host:port`) over mTLS (ADR 0011).
///
/// The client presents this node's certificate and trusts the cluster CA; the
/// TLS domain is the host half of `target`, which must match the peer
/// certificate's SAN.
pub async fn admin_channel(
    target: &str,
    ca_pem: &[u8],
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<Client<Channel>> {
    let host = target
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(target)
        .to_string();

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
        .domain_name(host);

    let channel = Channel::from_shared(format!("https://{target}"))
        .with_context(|| format!("invalid admin target {target}"))?
        .tls_config(tls)
        .context("configuring admin client TLS")?
        .connect()
        .await
        .with_context(|| format!("connecting to admin target {target}"))?;

    Ok(Client::new(channel))
}

/// Add a learner (ADR 0016 step 2).
pub async fn add_learner(
    client: &mut Client<Channel>,
    cluster_uuid: [u8; 16],
    node_id: CoordinatorId,
    addr: String,
) -> Result<()> {
    client
        .add_learner(pb::AddLearnerRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            node_id,
            address: addr,
        })
        .await
        .map_err(status_to_anyhow)?;
    Ok(())
}

/// Remove a node from membership (ADR 0016).
pub async fn remove_node(
    client: &mut Client<Channel>,
    cluster_uuid: [u8; 16],
    node_id: CoordinatorId,
) -> Result<()> {
    client
        .remove_node(pb::RemoveNodeRequest {
            cluster_uuid: cluster_uuid.to_vec(),
            node_id,
        })
        .await
        .map_err(status_to_anyhow)?;
    Ok(())
}

/// Fetch a coordinator's cluster-status view (ADR 0016).
pub async fn cluster_status(
    client: &mut Client<Channel>,
    cluster_uuid: [u8; 16],
) -> Result<pb::ClusterStatusResponse> {
    let resp = client
        .cluster_status(pb::ClusterStatusRequest {
            cluster_uuid: cluster_uuid.to_vec(),
        })
        .await
        .map_err(status_to_anyhow)?;
    Ok(resp.into_inner())
}

/// Promote a learner to voter, polling until it catches up or `wait` elapses
/// (ADR 0016 step 3).
///
/// A learner still behind the promotion threshold yields a retryable
/// `FAILED_PRECONDITION`/"behind" response; this retries every 500ms up to the
/// `wait` deadline before giving up, which is what makes `coordinator replace`
/// operable end to end. Any other failure returns immediately.
pub async fn promote_voter(
    client: &mut Client<Channel>,
    cluster_uuid: [u8; 16],
    promote: CoordinatorId,
    remove: Option<CoordinatorId>,
    wait: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let result = client
            .promote_voter(pb::PromoteVoterRequest {
                cluster_uuid: cluster_uuid.to_vec(),
                promote_node_id: promote,
                remove_node_id: remove,
            })
            .await;

        match result {
            Ok(_) => return Ok(()),
            Err(status) if is_learner_behind(&status) => {
                if tokio::time::Instant::now() + PROMOTE_POLL_INTERVAL >= deadline {
                    bail!(
                        "learner {promote} did not catch up within {}: {}",
                        humantime_serde::re::humantime::format_duration(wait),
                        status.message()
                    );
                }
                tokio::time::sleep(PROMOTE_POLL_INTERVAL).await;
            }
            Err(status) => return Err(status_to_anyhow(status)),
        }
    }
}

/// Whether a promotion failure is the retryable "learner still catching up"
/// case (ADR 0016) — the poll loop's continue condition.
fn is_learner_behind(status: &Status) -> bool {
    status.code() == Code::FailedPrecondition && status.message().contains("behind")
}

/// Flatten a gRPC [`Status`] into an `anyhow` error naming the code and message.
fn status_to_anyhow(status: Status) -> anyhow::Error {
    anyhow!(
        "admin RPC failed ({:?}): {}",
        status.code(),
        status.message()
    )
}

// ---------------------------------------------------------------------------
// CLI dispatch
// ---------------------------------------------------------------------------

/// Run one `admin` invocation: load config for TLS material and the default
/// target, dial the admin surface, and execute the verb.
pub async fn run_cli(args: AdminArgs) -> Result<()> {
    let resolved = config::load(&args.config, config::CliOverrides::default())
        .with_context(|| format!("reading config {}", args.config.display()))?;
    let cfg = &resolved.config;

    let target = match &args.target {
        Some(t) => t.clone(),
        None => cfg.peers.first().cloned().ok_or_else(|| {
            anyhow!(
                "no --target given and the config's `peers` list is empty; \
                 pass --target <host:port>"
            )
        })?,
    };

    let cluster_uuid = *cfg.cluster_id.0.as_bytes();
    let ca = read_pem(&cfg.tls.ca_path)?;
    let cert = read_pem(&cfg.tls.cert_path)?;
    let key = read_pem(&cfg.tls.key_path)?;

    let mut client = admin_channel(&target, &ca, &cert, &key).await?;

    match args.verb {
        AdminVerb::AddLearner { node_id, addr } => {
            add_learner(&mut client, cluster_uuid, node_id, addr.clone()).await?;
            println!("added node {node_id} as a learner ({addr})");
        }
        AdminVerb::Promote {
            node_id,
            remove,
            wait,
        } => {
            promote_voter(&mut client, cluster_uuid, node_id, remove, wait).await?;
            match remove {
                Some(r) => println!("promoted node {node_id} to voter, removed node {r}"),
                None => println!("promoted node {node_id} to voter"),
            }
        }
        AdminVerb::Remove { node_id } => {
            remove_node(&mut client, cluster_uuid, node_id).await?;
            println!("removed node {node_id} from membership");
        }
        AdminVerb::Status => {
            let status = cluster_status(&mut client, cluster_uuid).await?;
            print!("{}", render_status(&status));
        }
    }
    Ok(())
}

/// Read a PEM file, naming the path on failure (ADR 0011).
fn read_pem(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("reading TLS material {}", path.display()))
}

/// Pretty-print a `ClusterStatus` response for the `admin status` verb.
fn render_status(s: &pb::ClusterStatusResponse) -> String {
    let mut out = String::new();
    let leader = match s.leader_node_id {
        Some(id) if id == s.local_node_id => format!("{id} (this node)"),
        Some(id) => id.to_string(),
        None => "unknown".to_string(),
    };
    let _ = writeln!(out, "node          {}", s.local_node_id);
    let _ = writeln!(out, "leader        {leader}");
    let _ = writeln!(out, "term          {}", s.term);
    let _ = writeln!(out, "applied       {}", s.last_applied_index);
    let _ = writeln!(out, "committed     {}", s.known_committed_index);

    let voters: std::collections::BTreeSet<u64> = s
        .membership
        .as_ref()
        .and_then(|m| m.configs.first())
        .map(|c| c.voters.iter().copied().collect())
        .unwrap_or_default();

    let _ = writeln!(out, "members:");
    if let Some(membership) = &s.membership {
        for member in &membership.members {
            let role = if voters.contains(&member.node_id) {
                "voter"
            } else {
                "learner"
            };
            let _ = writeln!(
                out,
                "  node {:<6} {:<8} {}",
                member.node_id, role, member.address
            );
        }
    }

    if !s.replication.is_empty() {
        let _ = writeln!(out, "replication (leader view):");
        for r in &s.replication {
            let lag = s.last_applied_index.saturating_sub(r.matched_index);
            let _ = writeln!(
                out,
                "  node {:<6} matched {:<12} lag {}",
                r.node_id, r.matched_index, lag
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_consensus::MemberSummary;

    #[test]
    fn not_leader_maps_to_failed_precondition_naming_leader() {
        let status = consensus_error_to_status(ConsensusError::NotLeader { leader: Some(7) });
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(status.message().contains("node 7"), "{}", status.message());
    }

    #[test]
    fn not_leader_unknown_maps_to_failed_precondition() {
        let status = consensus_error_to_status(ConsensusError::NotLeader { leader: None });
        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    #[test]
    fn learner_behind_maps_and_is_detected_by_the_poll_predicate() {
        let status = consensus_error_to_status(ConsensusError::LearnerNotCaughtUp { lag: 42 });
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(status.message().contains("42"));
        // The client poll loop must recognize exactly this status.
        assert!(is_learner_behind(&status));
    }

    #[test]
    fn membership_in_progress_maps_to_aborted() {
        let status = consensus_error_to_status(ConsensusError::MembershipInProgress);
        assert_eq!(status.code(), Code::Aborted);
        assert!(!is_learner_behind(&status));
    }

    #[test]
    fn timeout_maps_to_deadline_exceeded() {
        let status = consensus_error_to_status(ConsensusError::Timeout);
        assert_eq!(status.code(), Code::DeadlineExceeded);
    }

    #[test]
    fn shutdown_and_fatal_map_to_unavailable_and_internal() {
        assert_eq!(
            consensus_error_to_status(ConsensusError::Shutdown).code(),
            Code::Unavailable
        );
        assert_eq!(
            consensus_error_to_status(ConsensusError::Fatal("disk gone".into())).code(),
            Code::Internal
        );
    }

    #[test]
    fn summary_to_pb_canonicalizes_voters_and_members() {
        let summary = ClusterSummary {
            local_id: 2,
            leader: Some(1),
            term: 5,
            last_applied: 100,
            known_committed: 100,
            members: vec![
                MemberSummary {
                    id: 3,
                    addr: "c3:7071".into(),
                    voter: false,
                },
                MemberSummary {
                    id: 1,
                    addr: "c1:7071".into(),
                    voter: true,
                },
                MemberSummary {
                    id: 2,
                    addr: "c2:7071".into(),
                    voter: true,
                },
            ],
            replication: vec![(1, 100), (3, 40)],
        };

        let pbm = cluster_summary_to_pb(summary);
        assert_eq!(pbm.local_node_id, 2);
        assert_eq!(pbm.leader_node_id, Some(1));
        assert_eq!(pbm.term, 5);

        let membership = pbm.membership.expect("membership present");
        // Voters ascending, learners excluded.
        assert_eq!(membership.configs.len(), 1);
        assert_eq!(membership.configs[0].voters, vec![1, 2]);
        // Members ascending by node_id, learners included.
        let ids: Vec<u64> = membership.members.iter().map(|m| m.node_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);

        assert_eq!(pbm.replication.len(), 2);
    }

    #[test]
    fn a_generic_failed_precondition_is_not_a_behind_signal() {
        let status = Status::failed_precondition("not the leader; current leader is node 3");
        assert!(!is_learner_behind(&status));
    }
}
