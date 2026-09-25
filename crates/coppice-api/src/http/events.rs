//! `GET /api/v1/events` — the filtered SSE job-event stream (ADR 0043),
//! serving the route ADR 0008 reserved.
//!
//! Everything endpoint-specific lives here: parsing and restricting the
//! `jobs` selector, resolving the resume cursor, and rendering
//! [`EventStreamItem`]s as the three SSE frames. The stream itself — the
//! catch-up, the live handoff, the drain — is the control plane's
//! ([`ControlPlane::subscribe_events`]), because it needs the replica's
//! fanout and this crate must not know what one is.

use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, Sse};

use serde::Deserialize;

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;

use coppice_core::time::Timestamp;

use crate::events::{EventStreamItem, JobSelector};
use crate::{Consistency, ControlPlane, ReadOptions};

use super::authn::RequestDeadline;
use super::dto;
use super::error::HttpError;

/// The header an `EventSource` (and any well-behaved SSE client) sends on
/// reconnect, carrying the id of the last frame it processed.
const LAST_EVENT_ID: &str = "last-event-id";

/// `GET /api/v1/events` query parameters.
///
/// `jobs` is named rather than `filter` so the selector vocabulary can grow
/// siblings — `nodes=`, `entities=` — without either renaming this one or
/// overloading it into a union that means different things by shape.
#[derive(Debug, Default, Deserialize)]
pub(super) struct EventsParams {
    /// URL-encoded JSON [`dto::JobFilter`], restricted to the leaves a
    /// subscription can answer. **Required** in v1.
    #[serde(default)]
    jobs: Option<String>,
    /// Resume from just after this applied index. Overridden by
    /// `Last-Event-ID` when the client sends one.
    #[serde(default)]
    cursor: Option<String>,
}

/// `GET /api/v1/events` — subscribe to the derived event stream (ADR 0043).
///
/// Any authenticated principal may subscribe and there is no per-event
/// authorization: reads are unscoped in ADR 0023, and an event payload is
/// thinner than the `ListJobs` row the same caller can already fetch.
///
/// Replica-local, like every other derived read — no leader involvement, and
/// a client that reconnects to a different replica resumes from the same
/// cursor, because the cursor is the Raft applied index (ADR 0008).
///
/// `INVALID_ARGUMENT` (400) for a missing or unparseable `jobs`, a filter
/// naming a leaf a subscription cannot answer (the message names it), or a
/// non-numeric cursor. `UNAVAILABLE` (503) when this replica will not take
/// the subscription — its cap is reached, or it has no fanout.
pub(super) async fn subscribe_events<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    RequestDeadline(deadline): RequestDeadline,
    headers: HeaderMap,
    params: Result<Query<EventsParams>, QueryRejection>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, HttpError> {
    let Query(params) = params.map_err(|e: QueryRejection| HttpError::invalid(e.body_text()))?;

    // Required rather than defaulting to "everything": an unfiltered
    // subscription is a firehose of every job in the cluster, and a client
    // that wanted one would have to ask for it explicitly — which, today, it
    // cannot. That is deliberate.
    let raw = params.jobs.as_deref().ok_or_else(|| {
        HttpError::invalid(
            "a `jobs` filter is required: an event subscription must say which jobs it wants",
        )
    })?;
    let mut filter: dto::JobFilter = serde_json::from_str(raw)
        .map_err(|e| HttpError::invalid(format!("invalid jobs filter: {e}")))?;
    // The shape rules shared with ListJobs (depth, node and non-empty-list
    // caps) first, then the leaf restriction that is this endpoint's own.
    filter.validate().map_err(HttpError::invalid)?;
    // Entity paths resolve once, here, against the latest view (ADR 0045):
    // the selector matches the ids apply stamped, so a subscription binds to
    // the entities its paths named when it opened, and a later rename does
    // not move it. An unresolvable path is a 400, exactly as on `ListJobs`.
    let view = plane
        .read_state(ReadOptions {
            consistency: Consistency::Eventual,
            min_index: None,
        })
        .await?;
    filter
        .resolve_entity_refs(view.state())
        .map_err(|e| HttpError::invalid(format!("invalid jobs filter: {e}")))?;
    drop(view);
    let selector = JobSelector::compile(&filter).map_err(|e| HttpError::invalid(e.to_string()))?;

    let cursor = resume_cursor(&headers, params.cursor.as_deref())?;

    let subscription = plane.subscribe_events(Arc::new(selector), cursor).await?;

    // No axum keep-alive: the ADR 0043 progress bookmark is this stream's
    // keepalive, and it says something true about coverage rather than being
    // a comment the client must discard.
    Ok(Sse::new(UntilDeadline {
        items: ReceiverStream::new(subscription.items),
        // Open mode and operator certificates offer no deadline, so those
        // streams are ended only by the drain or by the client.
        deadline: deadline.map(|at| Box::pin(tokio::time::sleep(remaining_until(at)))),
    }))
}

/// How long until `at`, saturating at zero for an instant already past.
fn remaining_until(at: Timestamp) -> std::time::Duration {
    (at - Timestamp::now()).to_std().unwrap_or_default()
}

/// The subscription's items, rendered as SSE frames and cut off at the
/// credential's expiry (ADR 0043).
///
/// Hand-written rather than a combinator chain for one reason worth being
/// explicit about: the deadline is polled **first**, so a stream with a
/// backlog still ends at the token's expiry instead of draining the backlog
/// past it. A subscription is the one API surface that outlives the
/// per-request credential check, and the check that replaces it has to be the
/// one that wins.
struct UntilDeadline {
    items: ReceiverStream<EventStreamItem>,
    /// `None` for a credential with no deadline. Boxed so this type stays
    /// `Unpin` and needs no projection.
    deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl Stream for UntilDeadline {
    type Item = Result<Event, std::convert::Infallible>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::future::Future;
        use std::task::Poll;

        let this = self.get_mut();
        if let Some(deadline) = &mut this.deadline {
            if deadline.as_mut().poll(cx).is_ready() {
                // Clean EOF, not an error: the client reconnects with a fresh
                // token and its last event id, losing nothing.
                return Poll::Ready(None);
            }
        }
        std::pin::Pin::new(&mut this.items)
            .poll_next(cx)
            .map(|item| item.map(|item| Ok(frame(item))))
    }
}

/// The resume cursor: `Last-Event-ID` if the client sent one, else
/// `?cursor=`.
///
/// The header wins because it is what the client *actually last processed* —
/// an `EventSource` sets it automatically on every reconnect, while the query
/// string is whatever was baked into the URL when the connection was first
/// opened, and after one reconnect that is stale by definition. A caller that
/// wants the query parameter honoured simply does not send the header.
fn resume_cursor(headers: &HeaderMap, query: Option<&str>) -> Result<Option<u64>, HttpError> {
    let header = headers.get(LAST_EVENT_ID).map(|value| {
        value
            .to_str()
            .map_err(|_| HttpError::invalid("Last-Event-ID must be an applied index"))
    });
    let raw = match header {
        Some(value) => Some(value?),
        None => query,
    };
    match raw {
        None => Ok(None),
        Some(raw) => raw
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|e| HttpError::invalid(format!("invalid cursor `{raw}`: {e}"))),
    }
}

/// Render one stream item as its SSE frame.
fn frame(item: EventStreamItem) -> Event {
    match item {
        EventStreamItem::Batch(batch) => {
            let index = batch.index;
            let body = dto::EventBatchFrame {
                index,
                at: batch.at,
                events: batch
                    .events
                    .into_iter()
                    .map(|e| dto::TimelineEvent {
                        index,
                        ordinal: e.ordinal,
                        at: batch.at,
                        body: (&e.event).into(),
                    })
                    .collect(),
            };
            // `id` is the resume cursor, which is why it is the index and not
            // some frame counter: a client reconnects with it verbatim.
            json_event("batch", &body).id(index.to_string())
        }
        EventStreamItem::Progress { index } => {
            json_event("progress", &dto::EventProgressFrame { index }).id(index.to_string())
        }
        // No id: see `dto::EventGapFrame`. A gap is not a resumable position.
        EventStreamItem::Gap { earliest_available } => {
            json_event("gap", &dto::EventGapFrame { earliest_available })
        }
    }
}

/// One named SSE frame carrying `body` as JSON.
///
/// Serialization cannot fail for these three types (plain owned data, no map
/// keys but strings), but `Event::json_data` is fallible; an impossible
/// failure becomes an empty object rather than a dropped frame, so a client
/// still sees the event name and the stream's framing stays intact.
fn json_event<T: serde::Serialize>(name: &'static str, body: &T) -> Event {
    match Event::default().event(name).json_data(body) {
        Ok(event) => event,
        Err(e) => {
            tracing::error!(error = %e, frame = name, "event frame failed to serialize");
            Event::default().event(name).data("{}")
        }
    }
}
