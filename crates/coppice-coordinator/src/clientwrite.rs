//! The transport half of follower write forwarding (ADR 0038).
//!
//! A follower that cannot propose a client write sends it to the leader over
//! the coordinator-to-coordinator mTLS admin channel — the same channel, the
//! same dial helper, and the same authorization posture the follower
//! `/enroll` proxy has used since ADR 0037 §4. This module holds only that
//! hop: resolving the leader's raft address out of membership, dialing,
//! calling one of the `Forward*` RPCs under a bounded timeout, and mapping
//! what comes back onto the ordinary [`ApiError`] vocabulary.
//!
//! The *decision* to forward, and every rule about what a forwarded write
//! means, lives with the write path in
//! [`crate::tasks::api_server`]; the leader-side handlers live in
//! [`crate::admin`]. What crosses this hop is the client's request, never a
//! pre-built log command: the leader re-runs the whole write path on it.
//!
//! One hop, always. If the request reaches a coordinator that is not (or is
//! no longer) the leader, that coordinator answers `NotLeader` and this side
//! surfaces the ordinary redirect — it never chases a second hop.

use std::sync::Arc;

use coppice_api::http::dto::{
    ConfigureQuotaEntityRequest, ConfigureQuotaEntityResponse, SubmitJobRequest, SubmitJobResponse,
};
use coppice_api::ApiError;
use coppice_consensus::{CoordinatorId, NodeHandle};
use coppice_core::id::JobId;
use coppice_net::admin::Client;
use coppice_proto::convert::ConvertError;
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::TlsStore;
use tonic::transport::Channel;

use crate::tasks::api_server::{BoxFuture, LeaderWrites};

/// How long a follower waits for the leader to answer a forwarded write.
///
/// The same 10s the `/enroll` proxy allows (`crate::enroll::PROXY_TIMEOUT`),
/// and for the same reason: generous next to a commit-and-apply round trip,
/// short enough that a wedged leader does not hold a client's connection —
/// or this replica's ingress slot — indefinitely.
///
/// It bounds the *dial* as well as the call. A leader address that blackholes
/// packets — the interesting failure, since a dead process refuses in
/// microseconds — leaves a TCP connect hanging for the OS's own retry budget,
/// which is minutes on every platform this runs on. Budgeting only the RPC
/// would have made the 10s a promise this path could not keep.
const FORWARD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Forwards client writes to the leader over the mTLS admin channel.
pub(crate) struct AdminForwarder {
    /// Resolves the leader's raft address; read per call, never cached, so a
    /// membership change between two writes is picked up immediately.
    node: NodeHandle,
    /// This replica's machine-plane identity for the dial. Read at dial time
    /// (not frozen) so a rotated leaf is presented on the next hop, exactly
    /// as [`crate::enroll::EnrollService`] does it.
    tls: Arc<TlsStore>,
}

impl AdminForwarder {
    pub(crate) fn new(node: NodeHandle, tls: Arc<TlsStore>) -> Arc<AdminForwarder> {
        Arc::new(AdminForwarder { node, tls })
    }

    /// Dial the leader's admin surface, or say why not.
    ///
    /// A leader with no address in this replica's membership view is the
    /// fallback case, not a failure: nothing can be forwarded, so the client
    /// gets the hintless redirect and retries. A leader that *has* an address
    /// but will not answer is a genuine unavailability.
    async fn dial(&self, leader: CoordinatorId) -> Result<(Client<Channel>, [u8; 16]), ApiError> {
        let summary = self.node.cluster_summary();
        let Some(addr) = summary
            .members
            .iter()
            .find(|m| m.id == leader)
            .map(|m| m.addr.clone())
        else {
            tracing::debug!(
                leader,
                "cannot forward the write: the leader has no address in this replica's \
                 membership view"
            );
            return Err(ApiError::NotLeader { leader_hint: None });
        };

        let client =
            under_dial_timeout(crate::admin::admin_channel_from_store(&addr, &self.tls)).await?;
        Ok((client, self.node.history_id()))
    }
}

/// The answer for every way the dial can end badly.
///
/// A dial that failed and a dial that never finished are the same fact:
/// nothing left this replica, so the outcome is *known* — the write did not
/// commit, and the client may retry it anywhere, including against a replica
/// that turns out to be the leader. That is the distinction this message
/// carries and [`under_timeout`]'s does not: once the request is on the wire,
/// silence means the leader may have committed it.
fn not_sent(detail: String) -> ApiError {
    ApiError::Unavailable(format!(
        "could not reach the leader to forward the write: {detail}; nothing was sent, so the \
         write did not commit — it is safe to retry against any replica"
    ))
}

/// Dial under the same budget the call gets.
///
/// Split out from [`AdminForwarder::dial`] so the budget is testable against a
/// future that never resolves. The failure it exists for — an address that
/// neither answers nor refuses — is exactly the one a unit test cannot
/// manufacture from a real socket: a listener it binds accepts, and one it
/// does not bind refuses.
async fn under_dial_timeout<T>(
    dial: impl std::future::Future<Output = anyhow::Result<T>>,
) -> Result<T, ApiError> {
    match tokio::time::timeout(FORWARD_TIMEOUT, dial).await {
        Ok(Ok(client)) => Ok(client),
        Ok(Err(e)) => Err(not_sent(format!("{e:#}"))),
        Err(_elapsed) => Err(not_sent(format!(
            "the connection did not establish within {}s",
            FORWARD_TIMEOUT.as_secs()
        ))),
    }
}

/// Run one forwarding call under the timeout, collapsing both failure shapes
/// onto the retriable answer.
///
/// A timeout here means the outcome is genuinely **unknown**: the leader may
/// have committed the write and lost the answer on the way back. That is
/// exactly what `UNAVAILABLE` promises — "did not resolve to a replicated
/// decision" — and it is safe to retry because every write on this path
/// carries a client-minted identity (ADR 0026), so a repeat resolves as an
/// accepted no-op rather than a second job or a second entity. What must
/// never happen is reporting success for a write this replica cannot vouch
/// for.
async fn under_timeout<F, T>(call: F) -> Result<T, ApiError>
where
    F: std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    match tokio::time::timeout(FORWARD_TIMEOUT, call).await {
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(status)) => Err(ApiError::Unavailable(format!(
            "the leader did not complete the forwarded write: {}",
            status.message()
        ))),
        Err(_elapsed) => Err(ApiError::Unavailable(
            "the leader did not answer the forwarded write within 10s; the outcome is unknown \
             and the request may still commit — retry it unchanged (its id makes the retry a \
             no-op if it did)"
                .to_string(),
        )),
    }
}

/// Map the leader's outcome onto this replica's answer to the client.
///
/// Returns the apply index on success; every refusal becomes the error the
/// client would have seen from the leader directly, which is the whole point
/// of carrying the outcome in the body rather than in a status code.
fn applied_index(outcome: Option<pb::ForwardWriteOutcome>) -> Result<u64, ApiError> {
    use pb::forward_write_outcome::Outcome;

    match outcome.and_then(|o| o.outcome) {
        Some(Outcome::Applied(applied)) => Ok(applied.log_index),
        Some(Outcome::Rejected(rejected)) => Err(ApiError::ForwardedRejection(rejected.reason)),
        Some(Outcome::Invalid(invalid)) => Err(ApiError::Invalid(invalid.message)),
        // The hop landed on a coordinator that is not the leader. One hop is
        // the rule, so this is where forwarding stops and the client gets the
        // redirect it would have got before ADR 0038.
        Some(Outcome::NotLeader(_)) => Err(ApiError::NotLeader { leader_hint: None }),
        // A response with no outcome at all is a peer speaking a schema this
        // one does not understand; it is not a decision, so it is not
        // reported as one.
        None => Err(ApiError::Unavailable(
            "the leader answered the forwarded write with no outcome".to_string(),
        )),
    }
}

impl LeaderWrites for AdminForwarder {
    fn submit_job<'a>(
        &'a self,
        leader: CoordinatorId,
        req: &'a SubmitJobRequest,
    ) -> BoxFuture<'a, Result<SubmitJobResponse, ApiError>> {
        Box::pin(async move {
            let (mut client, history_id) = self.dial(leader).await?;
            let wire = submit_to_pb(history_id, req);
            let response = under_timeout(client.forward_submit_job(wire)).await?;
            let log_index = applied_index(response.outcome)?;
            Ok(SubmitJobResponse {
                job: req.job,
                log_index,
            })
        })
    }

    fn abort_job<'a>(
        &'a self,
        leader: CoordinatorId,
        job: JobId,
        reason: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), ApiError>> {
        Box::pin(async move {
            let (mut client, history_id) = self.dial(leader).await?;
            let wire = pb::ForwardAbortJobRequest {
                history_id: history_id.to_vec(),
                job: Some(job.into()),
                reason: reason.map(str::to_string),
            };
            let response = under_timeout(client.forward_abort_job(wire)).await?;
            applied_index(response.outcome)?;
            Ok(())
        })
    }

    fn configure_quota_entity<'a>(
        &'a self,
        leader: CoordinatorId,
        req: &'a ConfigureQuotaEntityRequest,
    ) -> BoxFuture<'a, Result<ConfigureQuotaEntityResponse, ApiError>> {
        Box::pin(async move {
            let (mut client, history_id) = self.dial(leader).await?;
            let wire = pb::ForwardConfigureQuotaEntityRequest {
                history_id: history_id.to_vec(),
                entity: Some(req.entity.into()),
                parent: req.parent.map(Into::into),
                name: req.name.clone(),
                quota_ucu: req.quota_ucu,
            };
            let response = under_timeout(client.forward_configure_quota_entity(wire)).await?;
            let log_index = applied_index(response.outcome)?;
            Ok(ConfigureQuotaEntityResponse {
                entity: req.entity,
                log_index,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// DTO ⇄ pb at the forwarding boundary
// ---------------------------------------------------------------------------
//
// `coppice-api` speaks DTOs and deliberately does not depend on
// `coppice-proto` (ADR 0031), so the conversion lives here — the same
// arrangement the log-fetch seam uses. Resources go through
// `coppice_proto::convert`'s canonicalizing boundary rather than being
// re-encoded by hand: three scalars are easy to transpose, and the boundary
// already rejects duplicate and unknown kinds.

/// Unwrap a required message field (prost decodes them as `Option`), the
/// local twin of `coppice_proto::convert`'s own — which is private to that
/// crate's boundary and has no business being public for one caller.
fn required<T>(field: Option<T>, name: &'static str) -> Result<T, ConvertError> {
    field.ok_or(ConvertError::MissingField(name))
}

/// The client's submission, on the wire.
pub(crate) fn submit_to_pb(
    history_id: [u8; 16],
    req: &SubmitJobRequest,
) -> pb::ForwardSubmitJobRequest {
    let requests: coppice_core::resource::Resources = req.requests.into();
    pb::ForwardSubmitJobRequest {
        history_id: history_id.to_vec(),
        job: Some(req.job.into()),
        image: req.image.clone(),
        command: req.command.clone(),
        entrypoint: req
            .entrypoint
            .as_ref()
            .map(|argv| coppice_proto::pb::core::v1::Entrypoint { argv: argv.clone() }),
        requests: Some((&requests).into()),
        priority: req.priority,
        max_runtime_seconds: req.max_runtime_seconds,
        quota_entity: Some(req.quota_entity.into()),
        retry: req
            .retry
            .map(|r| coppice_core::job::RetryPolicy::from(r).into()),
    }
}

/// The submission again, back as the DTO the leader's write path takes.
///
/// Fallible where the DTO's own deserializer is: an id that does not parse or
/// a resource vector with a duplicate kind is a malformed request, and the
/// leader refuses it as one rather than defaulting it into something
/// plausible. Everything the DTO calls *optional* stays optional here — the
/// leader's validation, not this conversion, decides what is required.
pub(crate) fn submit_from_pb(
    req: pb::ForwardSubmitJobRequest,
) -> Result<SubmitJobRequest, ConvertError> {
    let requests: coppice_core::resource::Resources =
        required(req.requests, "ForwardSubmitJobRequest.requests")?.try_into()?;
    let entrypoint = match req.entrypoint {
        // Present-but-empty is not a second spelling of absent: the DTO
        // rejects it and so does `core.v1.Job`'s conversion, so it must
        // survive the hop as itself and be refused by the leader's
        // validation, not silently normalized here.
        Some(e) => Some(e.argv),
        None => None,
    };
    Ok(SubmitJobRequest {
        job: required(req.job, "ForwardSubmitJobRequest.job")?.try_into()?,
        image: req.image,
        command: req.command,
        entrypoint,
        requests: (&requests).into(),
        priority: req.priority,
        max_runtime_seconds: req.max_runtime_seconds,
        quota_entity: required(req.quota_entity, "ForwardSubmitJobRequest.quota_entity")?
            .try_into()?,
        retry: req.retry.map(|r| {
            let core: coppice_core::job::RetryPolicy = r.into();
            coppice_api::http::dto::RetryPolicy {
                max_retries: core.max_retries,
                retry_user_errors: core.retry_user_errors,
            }
        }),
    })
}

/// The quota upsert, back as the DTO the leader's write path takes.
pub(crate) fn configure_from_pb(
    req: pb::ForwardConfigureQuotaEntityRequest,
) -> Result<ConfigureQuotaEntityRequest, ConvertError> {
    Ok(ConfigureQuotaEntityRequest {
        entity: required(req.entity, "ForwardConfigureQuotaEntityRequest.entity")?.try_into()?,
        parent: req.parent.map(TryInto::try_into).transpose()?,
        name: req.name,
        quota_ucu: req.quota_ucu,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_api::http::dto;
    use coppice_core::id::QuotaEntityId;

    const HISTORY: [u8; 16] = [7; 16];

    fn submit_request() -> SubmitJobRequest {
        SubmitJobRequest {
            job: JobId::new(),
            image: "busybox:latest".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "true".to_string()],
            entrypoint: Some(vec!["/bin/sh".to_string()]),
            requests: dto::Resources {
                cpu_millis: 1500,
                memory_bytes: 1 << 30,
                disk_bytes: 0,
            },
            priority: -3,
            max_runtime_seconds: Some(3600),
            quota_entity: QuotaEntityId::new(),
            retry: Some(dto::RetryPolicy {
                max_retries: 2,
                retry_user_errors: true,
            }),
        }
    }

    #[test]
    fn a_submission_survives_the_hop_field_for_field() {
        // The leader validates what the *client* sent, so every field has to
        // arrive as it left — including the ones this replica already checked.
        // A dropped `max_runtime_seconds` or a transposed resource dimension
        // would run a different job than the one asked for.
        let original = submit_request();
        let round_tripped =
            submit_from_pb(submit_to_pb(HISTORY, &original)).expect("a well-formed request");

        assert_eq!(round_tripped.job, original.job);
        assert_eq!(round_tripped.image, original.image);
        assert_eq!(round_tripped.command, original.command);
        assert_eq!(round_tripped.entrypoint, original.entrypoint);
        assert_eq!(round_tripped.requests, original.requests);
        assert_eq!(round_tripped.priority, original.priority);
        assert_eq!(
            round_tripped.max_runtime_seconds,
            original.max_runtime_seconds
        );
        assert_eq!(round_tripped.quota_entity, original.quota_entity);
        let retry = round_tripped.retry.expect("retry policy survives");
        assert_eq!(retry.max_retries, 2);
        assert!(retry.retry_user_errors);
    }

    #[test]
    fn an_absent_entrypoint_stays_absent_and_an_empty_one_stays_empty() {
        // Two distinct meanings — "use the image's entrypoint" and "a
        // malformed override" — and the hop must not collapse them: the
        // second is refused by the *leader's* validation, so it has to get
        // there recognizable.
        let mut req = submit_request();
        req.entrypoint = None;
        let absent = submit_from_pb(submit_to_pb(HISTORY, &req)).expect("absent entrypoint");
        assert_eq!(absent.entrypoint, None);

        req.entrypoint = Some(Vec::new());
        let empty = submit_from_pb(submit_to_pb(HISTORY, &req)).expect("empty entrypoint");
        assert_eq!(empty.entrypoint, Some(Vec::new()));
    }

    #[test]
    fn an_upsert_survives_the_hop_including_a_null_parent() {
        let entity = QuotaEntityId::new();
        let parent = QuotaEntityId::new();
        for expected_parent in [None, Some(parent)] {
            let req = ConfigureQuotaEntityRequest {
                entity,
                parent: expected_parent,
                name: "platform".to_string(),
                quota_ucu: 12_345,
            };
            let wire = pb::ForwardConfigureQuotaEntityRequest {
                history_id: HISTORY.to_vec(),
                entity: Some(req.entity.into()),
                parent: req.parent.map(Into::into),
                name: req.name.clone(),
                quota_ucu: req.quota_ucu,
            };
            let back = configure_from_pb(wire).expect("a well-formed upsert");
            assert_eq!(back.entity, entity);
            assert_eq!(back.parent, expected_parent);
            assert_eq!(back.name, "platform");
            assert_eq!(back.quota_ucu, 12_345);
        }
    }

    #[test]
    fn a_missing_required_field_is_a_malformed_request_not_a_default() {
        let mut wire = submit_to_pb(HISTORY, &submit_request());
        wire.quota_entity = None;
        assert!(submit_from_pb(wire).is_err());
    }

    #[test]
    fn each_outcome_becomes_the_answer_the_client_would_have_got_from_the_leader() {
        use pb::forward_write_outcome::Outcome;

        let wrap = |outcome: Outcome| {
            Some(pb::ForwardWriteOutcome {
                outcome: Some(outcome),
            })
        };

        assert_eq!(
            applied_index(wrap(Outcome::Applied(pb::forward_write_outcome::Applied {
                log_index: 91
            })))
            .expect("applied"),
            91
        );
        assert!(matches!(
            applied_index(wrap(Outcome::Rejected(
                pb::forward_write_outcome::Rejected {
                    reason: "job 1 is terminal".to_string()
                }
            ))),
            Err(ApiError::ForwardedRejection(reason)) if reason == "job 1 is terminal"
        ));
        assert!(matches!(
            applied_index(wrap(Outcome::Invalid(pb::forward_write_outcome::Invalid {
                message: "bad priority".to_string()
            }))),
            Err(ApiError::Invalid(_))
        ));
        // One hop: the far side is not the leader either, and the client gets
        // the redirect rather than another forward.
        assert!(matches!(
            applied_index(wrap(Outcome::NotLeader(
                pb::forward_write_outcome::NotLeader {}
            ))),
            Err(ApiError::NotLeader { leader_hint: None })
        ));
        // No outcome at all is not a decision, and must not be reported as
        // one.
        assert!(matches!(applied_index(None), Err(ApiError::Unavailable(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn a_dial_that_never_completes_is_bounded_by_the_forward_budget() {
        // The blackholed leader address: connect neither succeeds nor fails.
        // Without a budget here the client's request outlives the 10s the
        // forwarding path advertises, by however long the kernel keeps
        // retransmitting SYNs. Virtual time, so nothing waits and nothing
        // flakes.
        let started = tokio::time::Instant::now();
        let never = std::future::pending::<anyhow::Result<()>>();
        let error = under_dial_timeout(never).await.expect_err("bounded");
        assert!(started.elapsed() >= FORWARD_TIMEOUT);
        match error {
            ApiError::Unavailable(message) => {
                // Known-not-sent, not outcome-unknown: nothing reached the
                // leader, so the retry advice is unconditional.
                assert!(message.contains("nothing was sent"), "{message}");
                assert!(!message.contains("unknown"), "{message}");
            }
            other => panic!("a bounded dial must be retriable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_dial_that_fails_says_the_write_was_not_sent() {
        // The other half of the same fact, and the reason both arms share one
        // message: a refused connection and an abandoned one are equally
        // "never left this replica".
        let refused =
            std::future::ready::<anyhow::Result<()>>(Err(anyhow::anyhow!("connection refused")));
        match under_dial_timeout(refused).await.expect_err("dial failed") {
            ApiError::Unavailable(message) => {
                assert!(message.contains("connection refused"), "{message}");
                assert!(message.contains("nothing was sent"), "{message}");
            }
            other => panic!("a failed dial must be retriable, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_call_says_the_outcome_is_unknown_instead() {
        // The distinction the two budgets exist to keep apart: once the
        // request is on the wire, silence is not evidence that it did not
        // commit, so this message must never claim it was not sent.
        let never = std::future::pending::<Result<tonic::Response<()>, tonic::Status>>();
        match under_timeout(never).await.expect_err("bounded") {
            ApiError::Unavailable(message) => {
                assert!(message.contains("unknown"), "{message}");
                assert!(!message.contains("nothing was sent"), "{message}");
            }
            other => panic!("a lost call must be retriable, got {other:?}"),
        }
    }
}
