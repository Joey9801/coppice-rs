//! API server (every replica).
//!
//! Implements `coppice_api::ControlPlane` by proposing through consensus —
//! the write path is `Consensus::propose`, mapping an `Applied.outcome`
//! rejection to a user-facing error
//! (`docs/architecture/coordinator-runtime.md`, "API server"). This runs on
//! every replica, including followers: a follower still accepts requests and
//! maps `ConsensusError::NotLeader` to a redirect, per the trait's contract.
//!
//! The HTTP/gRPC listener itself is not built (no axum/hyper dependency
//! added here); [`run_placeholder`] just holds the real `ControlPlane` impl
//! and parks on the shutdown watch so it is constructed by production code
//! and its trait methods stay exercised by the tests below rather than dead.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use coppice_api::{ApiError, ControlPlane};
use coppice_consensus::{Applied, Consensus, ConsensusError, StateViews};
use coppice_core::id::JobId;
use coppice_core::job::Job;
use coppice_proto::convert::ConvertError;
use coppice_proto::pb::api::v1::{AbortJobRequest, SubmitJobRequest, SubmitJobResponse};
use coppice_state::command::{AbortJob, SubmitJob};
use coppice_state::Command;

/// Implements [`ControlPlane`] by proposing through the consensus seam.
#[allow(dead_code)] // fields are read by submit_job/abort_job, exercised in tests below.
pub struct CoordinatorControlPlane<C> {
    consensus: Arc<C>,
    views: StateViews,
}

impl<C> CoordinatorControlPlane<C> {
    pub fn new(consensus: Arc<C>, views: StateViews) -> Self {
        CoordinatorControlPlane { consensus, views }
    }
}

impl<C: Consensus> ControlPlane for CoordinatorControlPlane<C> {
    async fn submit_job(&self, req: SubmitJobRequest) -> Result<SubmitJobResponse, ApiError> {
        // The client-minted job id is the submission's idempotency identity
        // (ADR 0026): a retry re-sends the same id, and apply resolves a
        // repeat of an already-committed submission as an accepted no-op, so
        // the retrying caller still lands in the `Ok` arm below with the
        // original id.
        let job: JobId = req
            .job
            .ok_or_else(|| ApiError::Invalid("missing job id".into()))?
            .try_into()
            .map_err(invalid)?;
        let requests = req
            .requests
            .ok_or_else(|| ApiError::Invalid("missing requests".into()))?
            .try_into()
            .map_err(invalid)?;
        let quota_entity = req
            .quota_entity
            .ok_or_else(|| ApiError::Invalid("missing quota_entity".into()))?
            .try_into()
            .map_err(invalid)?;
        let retry = req.retry.map(Into::into).unwrap_or_default();
        // Same rules the conversion boundary enforces on core.v1.Job: a
        // command is required (empty repeated = absent on the wire), and an
        // entrypoint override, when present, is non-empty.
        if req.command.is_empty() {
            return Err(ApiError::Invalid("missing command".into()));
        }
        let entrypoint = match req.entrypoint {
            None => None,
            Some(e) if e.argv.is_empty() => {
                return Err(ApiError::Invalid(
                    "entrypoint override must have at least one token".into(),
                ));
            }
            Some(e) => Some(e.argv),
        };

        // Multiplier resolution reads the replicated table off the latest
        // view (ADR 0019: apply never sees the raw `priority: i32` in
        // arithmetic) — this is the "synchronous validation" that needs
        // `self.views` rather than being purely shape-level.
        let view = self.views.latest();
        let multiplier = *view
            .state()
            .policy
            .priority_multipliers
            .get(&req.priority)
            .ok_or_else(|| {
                ApiError::Invalid(format!(
                    "no multiplier configured for priority {}",
                    req.priority
                ))
            })?;

        let command = Command::SubmitJob(SubmitJob {
            job: Job {
                id: job,
                image: req.image,
                command: req.command,
                entrypoint,
                requests,
                priority: req.priority,
                max_runtime_us: req.max_runtime_us,
                quota_entity,
                retry,
                abort_requested: None,
            },
            multiplier,
            submitted_at_us: now_us(),
        });

        match self.consensus.propose(command).await {
            // `log_index` lets the caller pair this write with a strong read
            // (ADR 0007 read-your-writes). On an idempotent repeat it is the
            // repeat's own apply index — ≥ the original commit, so still a
            // valid cursor for the job.
            Ok(Applied {
                outcome: Ok(_),
                log_index,
            }) => Ok(SubmitJobResponse {
                job: Some(job.into()),
                log_index,
            }),
            Ok(Applied {
                outcome: Err(rejection),
                ..
            }) => Err(ApiError::Rejected(rejection)),
            Err(e) => Err(map_consensus_error(e)),
        }
    }

    async fn abort_job(&self, req: AbortJobRequest) -> Result<(), ApiError> {
        let job = req
            .job
            .ok_or_else(|| ApiError::Invalid("missing job".into()))?
            .try_into()
            .map_err(invalid)?;

        let command = Command::AbortJob(AbortJob {
            job,
            reason: req.reason,
            requested_at_us: now_us(),
        });

        match self.consensus.propose(command).await {
            Ok(Applied { outcome: Ok(_), .. }) => Ok(()),
            Ok(Applied {
                outcome: Err(rejection),
                ..
            }) => Err(ApiError::Rejected(rejection)),
            Err(e) => Err(map_consensus_error(e)),
        }
    }
}

fn invalid(e: ConvertError) -> ApiError {
    ApiError::Invalid(e.to_string())
}

/// Map every non-`NotLeader` consensus failure to an API error.
///
/// Retryable failures (`Timeout`, `MembershipInProgress`, `LearnerNotCaughtUp`)
/// or fatal ones (`Shutdown`, `Fatal`) both surface as `Unavailable`: retryable
/// ones are literally "try again"; fatal ones still mean "this replica cannot
/// serve the write right now," which is the same actionable advice from the
/// caller's side.
fn map_consensus_error(e: ConsensusError) -> ApiError {
    match e {
        ConsensusError::NotLeader { leader } => ApiError::NotLeader {
            leader_hint: leader.map(|l| l.to_string()),
        },
        other => ApiError::Unavailable(other.to_string()),
    }
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Placeholder for the HTTP/gRPC listener that will host [`ControlPlane`].
///
/// No transport dependency added yet — see the module doc. Holds the real
/// `CoordinatorControlPlane` so it is constructed by production code, and
/// simply parks until shutdown.
pub async fn run_placeholder<C: Consensus>(
    control_plane: Arc<CoordinatorControlPlane<C>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let _ = control_plane;
    tracing::info!("api server: placeholder listening (no HTTP transport wired yet)");
    let _ = shutdown.wait_for(|s| *s).await;
    tracing::info!("api server: shutting down");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeConsensus, ProposeOutcome};
    use coppice_proto::pb::core::v1 as pbcore;

    fn control_plane(outcome: ProposeOutcome) -> CoordinatorControlPlane<FakeConsensus> {
        let (consensus, mut publisher) = FakeConsensus::new(outcome);

        // Seed a multiplier for priority 0 so submit_job's synchronous
        // validation passes and the test actually reaches `propose`.
        let mut policy = coppice_state::PolicyConfig::default();
        policy
            .priority_multipliers
            .insert(0, coppice_core::quota::PriorityMultiplier::ONE);
        let state = coppice_state::StateMachine {
            policy,
            ..coppice_state::StateMachine::default()
        };
        publisher.publish_now(&state, 1);

        let views = consensus.views();
        CoordinatorControlPlane::new(Arc::new(consensus), views)
    }

    fn submit_request(job: JobId) -> SubmitJobRequest {
        SubmitJobRequest {
            image: "busybox".to_string(),
            requests: Some(pbcore::Resources { quantities: vec![] }),
            priority: 0,
            max_runtime_us: None,
            quota_entity: Some(coppice_core::id::QuotaEntityId::new().into()),
            retry: None,
            job: Some(job.into()),
            command: vec!["run".to_string()],
            entrypoint: None,
        }
    }

    #[tokio::test]
    async fn accepted_submit_echoes_the_client_minted_job() {
        let cp = control_plane(ProposeOutcome::Accepted);
        let job = JobId::new();
        let response = cp.submit_job(submit_request(job)).await.expect("accepted");
        assert_eq!(response.job, Some(job.into()));
        assert!(response.log_index > 0);
    }

    #[tokio::test]
    async fn submit_without_a_job_id_is_invalid() {
        let cp = control_plane(ProposeOutcome::Accepted);
        let mut req = submit_request(JobId::new());
        req.job = None;
        let result = cp.submit_job(req).await;
        assert!(matches!(result, Err(ApiError::Invalid(_))));
    }

    #[tokio::test]
    async fn rejected_submit_maps_to_rejected() {
        let reason = coppice_state::RejectionReason::SubmitSpecMismatch(JobId::new());
        let cp = control_plane(ProposeOutcome::Rejected(reason));
        let result = cp.submit_job(submit_request(JobId::new())).await;
        assert!(matches!(result, Err(ApiError::Rejected(_))));
    }

    #[tokio::test]
    async fn not_leader_submit_maps_to_not_leader() {
        let cp = control_plane(ProposeOutcome::NotLeader(Some(7)));
        let result = cp.submit_job(submit_request(JobId::new())).await;
        assert!(
            matches!(result, Err(ApiError::NotLeader { leader_hint: Some(hint) }) if hint == "7")
        );
    }

    #[tokio::test]
    async fn accepted_abort_returns_ok() {
        let cp = control_plane(ProposeOutcome::Accepted);
        let req = AbortJobRequest {
            job: Some(JobId::new().into()),
            reason: None,
        };
        assert!(cp.abort_job(req).await.is_ok());
    }
}
