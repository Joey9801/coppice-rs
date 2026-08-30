//! The pre-log authorization check (ADR 0023): one read view, one call to
//! [`coppice_state::authz::evaluate`], one 403.
//!
//! ## Why there is a module for this
//!
//! Authorization is decided **twice** by design — here, synchronously, before
//! a command is proposed; and again in apply, against the bindings as of the
//! command's own log position. The second is the authority: it is
//! deterministic, every replica computes it identically, and it is what makes
//! a revocation racing an in-flight write resolve in log order rather than by
//! whichever check happened to run first. This one exists for the client's
//! sake: a refusal that costs no consensus round trip, arrives as a real 403
//! rather than a 409-shaped rejection, and does not put an entry in the log
//! for a request that was never going to take effect.
//!
//! Both run the *same function* over the same inputs, which is the only thing
//! that keeps them from drifting into two different policies wearing one
//! name. What differs is the state each reads: this one an eventual view,
//! apply the state at its log position. A disagreement between them is
//! therefore always a staleness window and never a rule — and apply wins.
//!
//! Every mutating handler routes through [`precheck`] and no handler
//! evaluates anything itself: one place, so a verb cannot be added with its
//! check quietly omitted.

use coppice_core::id::{JobId, QuotaEntityId};
use coppice_state::authz::{self, Verb};
use coppice_state::Actor;

use super::error::HttpError;
use crate::{Consistency, ControlPlane, ReadOptions};

/// What a handler is about to propose, in the terms the handler has.
///
/// Not [`Verb`] directly, for one reason: [`Verb::Abort`] needs the target
/// job's quota entity and submitting principal, which live in replicated
/// state — and the view they must be read from is the same one the decision
/// is evaluated against. A handler holding only a path parameter cannot build
/// that arm, so it names the job and [`precheck`] resolves it.
#[derive(Debug, Clone, Copy)]
pub(super) enum Intent<'a> {
    /// `POST /api/v1/jobs`.
    Submit { entity: &'a QuotaEntityId },
    /// `POST /api/v1/jobs/{job}/abort`.
    Abort { job: JobId },
    /// `POST /api/v1/quota-entities`.
    ConfigureQuotaEntity {
        entity: &'a QuotaEntityId,
        new_parent: Option<&'a QuotaEntityId>,
    },
    /// `PUT /api/v1/authorization`.
    UpdateAuthorization,
}

/// Refuse `intent` with a 403 if the actor's bindings do not cover it.
///
/// `Ok(())` means "not refused *here*", which is weaker than "authorized" and
/// is meant to be: the proposal that follows carries the actor, and apply
/// decides for real.
///
/// The view is read **eventual** on purpose. A strong read would put a
/// consensus round trip in front of every write to sharpen a check whose
/// answer apply is about to recompute authoritatively anyway — paying for
/// linearizability to reach a verdict that is not the one that counts. A
/// stale view can only ever cost a moment's extra permissiveness, which the
/// re-check closes at the log position.
pub(super) async fn precheck<P: ControlPlane>(
    plane: &P,
    actor: &Actor,
    intent: Intent<'_>,
) -> Result<(), HttpError> {
    let view = plane
        .read_state(ReadOptions {
            consistency: Consistency::Eventual,
            min_index: None,
        })
        .await?;
    let state = view.state();

    let verb = match intent {
        Intent::Submit { entity } => Verb::Submit { entity },
        Intent::Abort { job } => match state.jobs.get(&job) {
            Some(record) => Verb::Abort {
                entity: &record.spec.quota_entity,
                submitted_by: record.spec.submitted_by.as_deref(),
            },
            // A job this view has never heard of. It may not exist, or it may
            // simply not have applied here yet — and this check cannot tell
            // the two apart, so it declines to guess.
            //
            // Guessing either way would be wrong in a visible way. A 403
            // would refuse a caller who may well own the job, on the evidence
            // of a lagging view. A 404 would answer a *different* question
            // than the one asked and, worse, would be an existence oracle for
            // anyone probing job ids. Skipping leaves the decision to the
            // proposal: apply looks the job up in the state at the command's
            // log position, where the answer is real — an unknown job is
            // `UnknownJob`, and a known one is authorized against its actual
            // owner.
            None => return Ok(()),
        },
        Intent::ConfigureQuotaEntity { entity, new_parent } => {
            Verb::ConfigureQuotaEntity { entity, new_parent }
        }
        Intent::UpdateAuthorization => Verb::UpdateAuthorization,
    };

    // The refusal text is the `Denial`'s own — byte for byte what apply would
    // have rendered into `RejectionReason::PermissionDenied` — so which of the
    // two checks refused a request is invisible to the client, which is the
    // point: they are one decision evaluated at two moments.
    authz::evaluate(&state.bindings, &state.quota_entities, actor, verb)
        .map_err(|denial| HttpError::permission_denied(denial.to_string()))
}
