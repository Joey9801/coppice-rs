//! The client half of `ProbeCluster` (ADR 0037 §3).
//!
//! A converging process asks discovered candidates one question — *are you an
//! initialized cluster, and which one?* — and the answers decide everything
//! that follows. In this chunk there is exactly one caller: formation's
//! double-init guard. The self-join loop that consumes the same answers
//! arrives with the convergence work.
//!
//! Two properties are deliberate. **Unreachable candidates are skipped**:
//! probing is a search for the cluster, not a census, so a stale discovery
//! entry can slow the search but never wedge it. And every probe is
//! **bounded** by [`PROBE_TIMEOUT`] and runs concurrently with the others, so
//! a handful of black-holed addresses cannot hang a formation attempt an
//! operator is waiting on.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use coppice_proto::pb::raft::v1 as pb;

use crate::config::Config;

/// How long one candidate gets to answer, dial included. Short: the caller is
/// an interactive command, and the answer's absence is itself informative.
pub(crate) const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// One candidate's answer.
#[derive(Debug, Clone)]
pub(crate) struct ProbeResult {
    pub(crate) cluster_id: String,
    pub(crate) history_id: Vec<u8>,
    pub(crate) initialized: bool,
    #[allow(dead_code)] // consumed by the convergence loop (chunk 05)
    pub(crate) node_id: Option<u64>,
    #[allow(dead_code)]
    pub(crate) leader_hint: Option<u64>,
    #[allow(dead_code)]
    pub(crate) voters: Vec<(u64, String)>,
}

/// The mTLS material and cluster name every probe in a round shares. Read
/// once per round rather than per candidate — and read at probe time rather
/// than from the running [`TlsStore`](coppice_tls::TlsStore), because the one
/// caller in this chunk probes *before* the cluster has minted anything.
struct ProbeCreds {
    ca: Vec<u8>,
    cert: Vec<u8>,
    key: Vec<u8>,
    cluster_id: String,
}

fn creds(cfg: &Config) -> Result<Arc<ProbeCreds>> {
    let read = |path: &std::path::Path| {
        std::fs::read(path).with_context(|| format!("reading TLS material {}", path.display()))
    };
    Ok(Arc::new(ProbeCreds {
        ca: read(&cfg.tls.ca_path)?,
        cert: read(&cfg.tls.cert_path)?,
        key: read(&cfg.tls.key_path)?,
        cluster_id: cfg.cluster_id.to_string(),
    }))
}

/// Probe every candidate concurrently, returning only those that answered.
///
/// The returned pairs carry the target that answered, because the caller's
/// error messages name it ("`coord-2:7071` already reports an initialized
/// cluster").
pub(crate) async fn probe_all(
    cfg: &Config,
    candidates: &[String],
) -> Result<Vec<(String, ProbeResult)>> {
    let creds = creds(cfg)?;
    let mut round = tokio::task::JoinSet::new();
    for target in candidates {
        let creds = Arc::clone(&creds);
        let target = target.clone();
        round.spawn(async move {
            let outcome = probe_with(&creds, &target).await;
            (target, outcome)
        });
    }

    let mut answers = Vec::new();
    while let Some(joined) = round.join_next().await {
        let (target, outcome) = match joined {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(error = %e, "probe: task did not join");
                continue;
            }
        };
        match outcome {
            Ok(answer) => answers.push((target, answer)),
            // Not a failure of the round: a candidate that does not answer is
            // simply not the cluster we are looking for.
            Err(e) => {
                tracing::debug!(%target, error = %e, "probe: candidate did not answer, skipping")
            }
        }
    }
    Ok(answers)
}

async fn probe_with(creds: &ProbeCreds, target: &str) -> Result<ProbeResult> {
    let probe = async {
        let mut client =
            crate::admin::admin_channel(target, &creds.ca, &creds.cert, &creds.key).await?;
        let resp = client
            .probe_cluster(pb::ProbeClusterRequest {
                cluster_id: creds.cluster_id.clone(),
            })
            .await
            .map_err(|s| anyhow::anyhow!("ProbeCluster failed ({:?}): {}", s.code(), s.message()))?
            .into_inner();
        anyhow::Ok(resp)
    };

    let resp = tokio::time::timeout(PROBE_TIMEOUT, probe)
        .await
        .map_err(|_| anyhow::anyhow!("probe timed out after {PROBE_TIMEOUT:?}"))??;

    Ok(ProbeResult {
        cluster_id: resp.cluster_id,
        history_id: resp.history_id,
        initialized: resp.initialized,
        node_id: resp.node_id,
        leader_hint: resp.leader_hint,
        voters: resp
            .voters
            .into_iter()
            .map(|v| (v.node_id, v.address))
            .collect(),
    })
}
