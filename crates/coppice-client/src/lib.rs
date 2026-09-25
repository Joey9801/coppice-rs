#![warn(missing_docs)]
#![doc = include_str!("../README.md")]
//!
//! # Guide
//!
//! The README above is the tour; what follows is the detail behind it.
//!
//! ## Building a client
//!
//! A [`Client`] is bound to one coordinator's base URL. It is cheap to clone —
//! the connection pool and the credential are shared — so build one and pass
//! it around.
//!
//! ```no_run
//! use coppice_client::Client;
//!
//! # fn main() -> coppice_client::Result<()> {
//! // A bare base, a trailing slash, or a pasted `…/api/v1` all work.
//! let client = Client::new("http://127.0.0.1:7070")?;
//! # Ok(()) }
//! ```
//!
//! [`Client::local`] is shorthand for the default loopback base, and
//! [`Client::builder`] is where the token, the timeout and a caller-supplied
//! `reqwest::Client` go.
//!
//! Requests are rate-limited by default — [`DEFAULT_RATE_LIMIT_RPS`] a
//! second, shared across every clone of a client so concurrent pagers and
//! followers draw on one allowance — because a client that polls as fast as
//! its pagers resume is the one caller a coordinator cannot argue with.
//! [`ClientBuilder::rate_limit`] replaces the quota, and
//! [`ClientBuilder::no_rate_limit`] turns the limiter off for a caller that
//! already paces itself.
//!
//! Two details of the bearer token the README's sketch leaves out. An empty or
//! whitespace-only token — an environment variable that is set but empty —
//! counts as no token at all, and with no token **no `Authorization` header is
//! sent**, which is the one thing a cluster in open mode must not see.
//! [`Client::auth_config`] reports which posture a deployment is in without
//! needing a credential; [`Client::session`] reports who a credential proved
//! you are and what that identity may do.
//!
//! The token is held as a [`BearerToken`], whose `Debug` prints
//! `BearerToken(<redacted>)` and which has neither `Display` nor `Serialize` —
//! so a `{:?}` of a `Client` or a `ClientBuilder` cannot leak a credential into
//! a log line. The `Authorization` header is marked sensitive as well, which is
//! what keeps it out of `reqwest`'s own renderings of a request.
//!
//! A fixed token is the convenience for a CLI or any other short-lived tool,
//! whose process does not outlive its credential. A long-running one does, so
//! it hands [`ClientBuilder::token_provider`] a [`TokenProvider`] instead: the
//! client asks it for a token once per request, immediately before sending,
//! and caches nothing — refresh, caching and single-flight locking are the
//! provider's business, because only it knows what a token costs and how long
//! it lasts. A provider answering `Ok(None)` sends no header at all; one
//! answering `Err` fails the call with [`Error::Credential`] before anything
//! reaches the wire.
//!
//! The base's scheme also decides how the default `reqwest::Client` is built:
//! a base that is not `https://` skips loading the platform's native root
//! store even with the `rustls-tls-native-roots` feature on, because
//! enumerating a keychain costs seconds and buys nothing for
//! `http://127.0.0.1`. A program that builds its own HTTP client and wants the
//! same treatment can call [`plain_http_builder`].
//!
//! ## Reads, writes and read-your-writes
//!
//! Every `/api/v1` read returns a [`Versioned<T>`](Versioned): the body, plus
//! how far the replica that served it had applied and how far the cluster had
//! committed. That includes every page a pager or the log follower hands back.
//! `Versioned` derefs to the body, so most code ignores it. [`Client::healthz`]
//! is the one read that does not: it is outside `/api/v1` and outside
//! consensus, so there is no index for it to report.
//!
//! Reads have per-endpoint consistency defaults — a list is bounded, a
//! configuration read is strong, a derived series is eventual. Override them
//! with [`ReadOptions`] through [`Client::with_read_options`], which returns a
//! scoped clone rather than mutating the client.
//!
//! Every write answers with a `log_index`: the position its command applied
//! at. Pairing that with [`ReadOptions::at_least`] is the read-your-writes
//! idiom, and it is cheaper than asking for a strong read:
//!
//! ```no_run
//! # use coppice_client::{Client, JobId, ReadOptions, Resources, SubmitJobRequest};
//! # async fn go(client: &Client, entity: coppice_client::QuotaEntityId)
//! #     -> coppice_client::Result<()> {
//! let request = SubmitJobRequest::new(
//!     JobId::new(),
//!     "alpine:3",
//!     ["sleep", "60"],
//!     Resources::new(1_000, 512 * 1024 * 1024, 0),
//!     entity,
//! );
//! let submitted = client.submit_job(&request).await?;
//!
//! let job = client
//!     .with_read_options(ReadOptions::at_least(submitted.log_index))
//!     .job(submitted.job)
//!     .await?;
//! println!("{:?}", job.state);
//! # Ok(()) }
//! ```
//!
//! ## Pagination
//!
//! Four endpoints paginate: the job list, a job's timeline, its logs and its
//! usage samples. Each answers with a `next_cursor`, and **a short page with a
//! non-null cursor means continue, never done** — the server ends a page early
//! for reasons that have nothing to do with the `limit` you asked for.
//!
//! The pagers in [`pagination`] do the walking:
//!
//! ```no_run
//! # use coppice_client::{Client, JobFilter, JobPhase, ListJobsParams};
//! # async fn go(client: &Client) -> coppice_client::Result<()> {
//! let params = ListJobsParams::new()
//!     .with_filter(JobFilter::phase_in([JobPhase::Running]))
//!     .with_limit(50);
//!
//! let mut pages = client.list_jobs_paged(params);
//! while let Some(page) = pages.next_page().await? {
//!     for job in &page.jobs {
//!         println!("{} {}", job.id, job.image);
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! ## Following logs
//!
//! [`LogFollower`] is the polling loop a `logs --follow` wants: it walks to
//! the live head, then polls from the page's `resume_cursor`, and stops once
//! the job's own state says it is finished — after one last drain, which is
//! what catches the final lines. See the [`follow`] module.
//!
//! ## Watching a set of jobs
//!
//! Polling every job a service owns is almost entirely redundant requests. The
//! alternative is one connection: `GET /api/v1/events` (ADR 0043) subscribes
//! to an **open set** — every job matching a filter, including jobs submitted
//! after the subscription opened — and this crate provides the loop ADR 0008
//! says a client library should.
//!
//! [`Client::watch_jobs`] is that loop, and the one to reach for first. It
//! lists with the filter — one [`JobWatchItem::SnapshotPage`] per page —
//! subscribes from the first page's applied index, and answers a `gap` —
//! delivery having been discontinuous — with a fresh snapshot rather than a
//! silence:
//!
//! ```no_run
//! # async fn go(client: &coppice_client::Client) -> coppice_client::Result<()> {
//! use coppice_client::{JobFilter, JobWatchItem, WatchOptions};
//!
//! let filter = JobFilter::metadata_equals("owner", "batch-service");
//! let mut watch = client.watch_jobs(filter, WatchOptions::new());
//!
//! while let Some(item) = watch.next_item().await? {
//!     match item {
//!         JobWatchItem::SnapshotPage { jobs, index, first, last } => {
//!             println!("{} jobs at {index:?} (first={first} last={last})", jobs.len());
//!         }
//!         JobWatchItem::Batch(batch) => println!("{} events", batch.events.len()),
//!         // The enum is `#[non_exhaustive]`: a later release may add an item.
//!         _ => {}
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! Four things are worth knowing before you build on it. **The snapshot is the
//! live set**: the list is the caller's filter AND a non-terminal phase, so
//! its size follows the cluster's working set rather than its retention —
//! [`SnapshotScope::All`] opts out. A job you track that is *absent* from a
//! snapshot has left the live set; read it individually if you need its
//! outcome. **The filter is restricted**: a subscription matches on the keys
//! that say *which job this is* — `metadata`, `entity`, `id`, `submitted_by`,
//! under `all`/`any`/`not` — and [`JobFilter::validate_subscribable`] refuses
//! the rest by name before a request is sent, because the others read state
//! that changes underneath a live stream. **Payloads are thin**: an event
//! carries identity, stamp, kind and scope ids, so a consumer that wants the
//! job reads it with [`ReadOptions::at_least`] set to the event's index. And
//! **delivery is per-job clean but not a consistent cut**: each snapshot page
//! reflects its own index, events a page already reflected are dropped from
//! the stream, and everything after the first page's index arrives as an
//! event. [`JobWatcher`] spells the guarantee out.
//!
//! [`Client::watch_job_events`] is the same subscription without the
//! list — a [`JobEventWatcher`] that reconnects and hands gaps to you — and
//! [`Client::subscribe_job_events`] is one bare connection. The [`events`]
//! module has the detail.
//!
//! ## Error handling
//!
//! Everything fails with [`Error`]. The three cases worth distinguishing are a
//! transport failure, a server error carrying the `{code, message}` body
//! ([`ErrorCode`]), and a non-2xx that was not one of those. The predicates do
//! the usual triage:
//!
//! ```no_run
//! # use coppice_client::{Client, NodeId};
//! # async fn go(client: &Client, node: NodeId) -> coppice_client::Result<()> {
//! match client.node(node).await {
//!     Ok(node) => println!("{:?}", node.summary.health),
//!     Err(e) if e.is_not_found() => println!("no such node"),
//!     Err(e) if e.is_retryable() => {
//!         // A follower refusing a write says where the leader is.
//!         if let Some(leader) = e.leader_hint() {
//!             eprintln!("retry against {leader}");
//!         }
//!     }
//!     Err(e) => return Err(e),
//! }
//! # Ok(()) }
//! ```
//!
//! ## The escape hatch
//!
//! Typed methods cannot cover a field or a parameter a newer server grew, and
//! a `--json` mode should print the server's own body rather than a
//! re-serialization of a parsed struct. So every endpoint is also reachable
//! untyped: [`Client::get_value`], [`Client::post_value`] and
//! [`Client::put_value`] take an `/api/v1`-relative path and return
//! `serde_json::Value`.
//!
//! They are not a parallel implementation. [`paths`] builds the same paths the
//! typed methods use, and every params struct exposes the same query pairs, so
//! the two cannot drift:
//!
//! ```no_run
//! # use coppice_client::{paths, Client, ListJobsParams};
//! # async fn go(client: &Client) -> coppice_client::Result<()> {
//! let params = ListJobsParams::new().with_limit(10);
//! let body = client.get_value(paths::JOBS, &params.query_pairs()).await?;
//! println!("{}", serde_json::to_string_pretty(&body.value).unwrap());
//! # Ok(()) }
//! ```
//!
//! ## How this crate tracks the server
//!
//! The wire types here are hand-written copies of the coordinator's own DTOs,
//! which is what lets this crate publish with no `coppice-*` dependency. A
//! contract test that lives inside the server — the one place that can see
//! both crates at once — round-trips every server value through the copy here
//! and compares the JSON on both sides as `serde_json::Value`s: every key,
//! every value, every enum spelling, and every shared limit must match, while
//! key order and whitespace are not compared.
//!
//! That copy is why tolerance runs one way: responses ignore fields this
//! client is too old to know, and a string-valued enum keeps an unrecognized
//! value verbatim ([`types`] explains the mechanism, and names the one
//! tagged-union enum whose unknown payload is discarded instead), so an older
//! client degrades against a newer coordinator rather than failing outright.
//! Requests are the opposite — the server rejects an unknown field in a write
//! body, so a typo is an error and not a silent default, and this crate
//! refuses an enum's `Unknown` value in a request before sending it. When a
//! typed method cannot express something a newer server grew, the escape
//! hatch above reaches it anyway.

mod client;
mod credential;
mod entity_ref;
mod env;
mod error;
pub mod events;
pub mod follow;
mod id;
mod metadata;
pub mod pagination;
pub mod paths;
mod sse;
mod time;
pub mod types;

pub use client::{
    plain_http_builder, Client, ClientBuilder, Consistency, ReadOptions, Versioned,
    APPLIED_INDEX_HEADER, COMMITTED_INDEX_HEADER, DEFAULT_BASE_URL, DEFAULT_PORT,
    DEFAULT_RATE_LIMIT_RPS, DEFAULT_TIMEOUT, LAST_EVENT_ID_HEADER, LEADER_HEADER, STREAM_TIMEOUT,
};
pub use credential::{BearerToken, BoxError, TokenProvider};
pub use entity_ref::{
    validate_segment, InvalidSegment, ParsePathError, QuotaEntityPath, QuotaEntityRef,
    MAX_SEGMENT_LEN, PATH_SEPARATOR,
};
pub use env::{
    EnvError, JobEnv, MAX_ENV_NAME_BYTES, MAX_ENV_TOTAL_BYTES, MAX_ENV_VALUE_BYTES, MAX_ENV_VARS,
};
pub use error::{Error, ErrorCode, Result};
pub use events::{
    JobEventItem, JobEventStream, JobEventWatcher, JobWatchItem, JobWatcher, SnapshotScope,
    WatchOptions, DEFAULT_MAX_RECONNECT_BACKOFF, DEFAULT_MIN_RECONNECT_BACKOFF,
    DEFAULT_STREAM_IDLE_TIMEOUT,
};
pub use follow::{FollowOptions, LogFollower, DEFAULT_POLL_INTERVAL};
pub use governor::Quota;
pub use id::{AllocationId, AttemptId, ClusterId, JobId, NodeId, ParseIdError, QuotaEntityId};
pub use metadata::{
    validate_key, JobMetadata, MetadataError, MAX_KEYS, MAX_KEY_BYTES, MAX_VALUE_BYTES,
};
pub use pagination::{
    JobCursor, JobPager, LogCursor, LogPager, TimelineCursor, TimelinePager, UsageCursor,
    UsagePager,
};
pub use time::{ParseTimestampError, Timestamp};
pub use types::*;
