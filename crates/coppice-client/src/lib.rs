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
//! Two details of the bearer token the README's sketch leaves out. An empty or
//! whitespace-only token — an environment variable that is set but empty —
//! counts as no token at all, and with no token **no `Authorization` header is
//! sent**, which is the one thing a cluster in open mode must not see.
//! [`Client::auth_config`] reports which posture a deployment is in without
//! needing a credential; [`Client::session`] reports who a credential proved
//! you are and what that identity may do.
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
//! Every read returns a [`Versioned<T>`](Versioned): the body, plus how far
//! the replica that served it had applied and how far the cluster had
//! committed. `Versioned` derefs to the body, so most code ignores it.
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
//! both crates at once — holds the two to byte-identical JSON, variant for
//! variant and limit for limit.
//!
//! That copy is why tolerance runs one way: responses ignore fields this
//! client is too old to know and keep unknown enum values verbatim ([`types`]
//! explains the mechanism), so an older client degrades against a newer
//! coordinator rather than failing outright. Requests are the opposite — the
//! server rejects an unknown field in a write body, so a typo is an error and
//! not a silent default. When a typed method cannot express something a newer
//! server grew, the escape hatch above reaches it anyway.

mod client;
mod env;
mod error;
pub mod follow;
mod id;
mod metadata;
pub mod pagination;
pub mod paths;
mod time;
pub mod types;

pub use client::{
    plain_http_builder, Client, ClientBuilder, Consistency, ReadOptions, Versioned,
    APPLIED_INDEX_HEADER, COMMITTED_INDEX_HEADER, DEFAULT_BASE_URL, DEFAULT_PORT, DEFAULT_TIMEOUT,
    LEADER_HEADER,
};
pub use env::{
    EnvError, JobEnv, MAX_ENV_NAME_BYTES, MAX_ENV_TOTAL_BYTES, MAX_ENV_VALUE_BYTES, MAX_ENV_VARS,
};
pub use error::{Error, ErrorCode, Result};
pub use follow::{FollowOptions, LogFollower, DEFAULT_POLL_INTERVAL};
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
