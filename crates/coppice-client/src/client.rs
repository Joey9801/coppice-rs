//! The client itself: how a base URL, a token and a timeout become a thing
//! you can call endpoints on.

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::follow::{FollowOptions, LogFollower};
use crate::id::{JobId, NodeId, QuotaEntityId};
use crate::pagination::{JobPager, LogPager, TimelinePager, UsagePager};
use crate::paths;
use crate::types::{
    AbortJobRequest, AbortJobResponse, ConfigureQuotaEntityRequest, ConfigureQuotaEntityResponse,
    DrainNodeResponse, GetAuthConfigResponse, GetAuthorizationResponse, GetClusterOverviewResponse,
    GetCoordinatorStatusResponse, GetJobLogsResponse, GetJobTimelineResponse, GetJobUsageResponse,
    GetNodeResponse, GetNodeUtilizationResponse, GetQuotaEntityResponse, GetSessionResponse,
    HealthzResponse, JobDetail, ListJobsParams, ListJobsResponse, ListNodesResponse,
    ListQuotaEntitiesResponse, LogsParams, QueueStats, RemoveNodeResponse,
    ReplaceJobMetadataRequest, ReplaceJobMetadataResponse, SubmitJobRequest, SubmitJobResponse,
    TimelineParams, UpdateAuthorizationRequest, UpdateAuthorizationResponse,
    UpdateJobMetadataRequest, UpdateJobMetadataResponse, UsageParams,
};

/// The port a coordinator's client API listens on unless configured
/// otherwise.
pub const DEFAULT_PORT: u16 = 7070;

/// The base URL a client dials when nothing says otherwise: [`DEFAULT_PORT`]
/// on loopback.
///
/// Loopback rather than a wildcard address, because this is the address a
/// client *dials*, and the only coordinator a bare call can reasonably mean is
/// one on this machine. Reaching any other cluster is an explicit act.
pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:7070";

/// How long one request may take before it is abandoned.
///
/// `reqwest` imposes no timeout of its own, which turns an unreachable or
/// wedged coordinator into a program that hangs forever with no output. Thirty
/// seconds is comfortably above any bounded read or a write's consensus round
/// trip, and well below anyone's patience for a dead endpoint.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// The response header carrying the serving replica's applied log index.
pub const APPLIED_INDEX_HEADER: &str = "coppice-applied-index";
/// The response header carrying the serving replica's committed log index.
pub const COMMITTED_INDEX_HEADER: &str = "coppice-committed-index";
/// The response header a follower sets to say where the leader is.
pub const LEADER_HEADER: &str = "coppice-leader";

/// How fresh a read has to be (ADR 0007).
///
/// Every read endpoint has its own default — a list is `Bounded`, a
/// configuration read is `Strong`, a derived series is `Eventual` — so most
/// callers never set this. Reach for it when a read must reflect a write you
/// just made, and prefer [`ReadOptions::at_least`] with the write's
/// `log_index` over `Strong`: it is cheaper and says exactly what you mean.
///
/// Client-authored only — this is a request parameter the server never
/// echoes back — so it is closed with no `Unknown` catch-all.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum Consistency {
    /// Linearizable: the serving replica confirms leadership first.
    Strong,
    /// Served from this replica's latest applied view, with the staleness
    /// reported in the response headers.
    Bounded,
    /// Served from whatever derived store backs the endpoint; replica-local.
    Eventual,
}

/// The two read-consistency query parameters every read endpoint accepts.
///
/// Attach them to a client with
/// [`Client::with_read_options`](Client::with_read_options), which returns a
/// scoped clone rather than mutating anything:
///
/// ```no_run
/// # async fn go(client: &coppice_client::Client, request: &coppice_client::SubmitJobRequest)
/// #     -> coppice_client::Result<()> {
/// use coppice_client::ReadOptions;
///
/// // Read-your-writes: pair the write's log index with the next read.
/// let submitted = client.submit_job(request).await?;
/// let detail = client
///     .with_read_options(ReadOptions::at_least(submitted.log_index))
///     .job(submitted.job)
///     .await?;
/// # let _ = detail; Ok(()) }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct ReadOptions {
    /// Override the endpoint's default consistency class.
    pub consistency: Option<Consistency>,
    /// Serve from a view whose applied index is at least this — the
    /// read-your-writes pair for a write response's `log_index`.
    pub min_index: Option<u64>,
}

impl ReadOptions {
    /// No overrides: every endpoint reads at its own default.
    pub fn new() -> ReadOptions {
        ReadOptions::default()
    }

    /// Read linearizably.
    pub fn strong() -> ReadOptions {
        ReadOptions::new().with_consistency(Consistency::Strong)
    }

    /// Read from the replica's latest applied view.
    pub fn bounded() -> ReadOptions {
        ReadOptions::new().with_consistency(Consistency::Bounded)
    }

    /// Read from whatever derived store backs the endpoint.
    pub fn eventual() -> ReadOptions {
        ReadOptions::new().with_consistency(Consistency::Eventual)
    }

    /// Read from a view that has applied at least `index` — the
    /// read-your-writes idiom, paired with a write response's `log_index`.
    pub fn at_least(index: u64) -> ReadOptions {
        ReadOptions::new().with_min_index(index)
    }

    /// Set the consistency class.
    pub fn with_consistency(mut self, consistency: Consistency) -> ReadOptions {
        self.consistency = Some(consistency);
        self
    }

    /// Set the minimum applied index.
    pub fn with_min_index(mut self, index: u64) -> ReadOptions {
        self.min_index = Some(index);
        self
    }

    /// Whether either parameter is set.
    pub fn is_empty(&self) -> bool {
        self.consistency.is_none() && self.min_index.is_none()
    }

    /// The query pairs these options add to a read, `consistency` first.
    pub fn query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        if let Some(consistency) = self.consistency {
            pairs.push(("consistency", consistency.to_string()));
        }
        if let Some(min_index) = self.min_index {
            pairs.push(("min_index", min_index.to_string()));
        }
        pairs
    }
}

/// A read's answer, together with where in the log the replica that served it
/// had got to.
///
/// Every read carries the two indexes, so every read returns one of these
/// rather than a bare value; `Deref` and [`into_inner`](Self::into_inner) mean
/// you can mostly ignore that. The indexes are what make staleness legible:
/// `applied_index` is how far the serving replica has applied, and
/// `committed_index` how far the cluster has committed, so the difference is
/// how far behind this answer is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Versioned<T> {
    /// The decoded body.
    pub value: T,
    /// The serving replica's applied log index, when it reported one.
    pub applied_index: Option<u64>,
    /// The cluster's committed log index as that replica knows it.
    pub committed_index: Option<u64>,
}

impl<T> Versioned<T> {
    /// The body, discarding the indexes.
    pub fn into_inner(self) -> T {
        self.value
    }

    /// How many entries behind the committed frontier this answer is, when
    /// both indexes are known.
    pub fn lag(&self) -> Option<u64> {
        let (applied, committed) = (self.applied_index?, self.committed_index?);
        Some(committed.saturating_sub(applied))
    }

    /// Apply a function to the body, keeping the indexes.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Versioned<U> {
        Versioned {
            value: f(self.value),
            applied_index: self.applied_index,
            committed_index: self.committed_index,
        }
    }
}

impl<T> Deref for Versioned<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

/// Reduce a user-supplied base to the one canonical form.
///
/// Accepts `http://host:7070`, a trailing slash, and a base already ending in
/// `/api/v1` (the form a dev banner prints and people paste), and maps all of
/// them to the same string. The scheme is lowercased because RFC 3986 says
/// schemes are case-insensitive and the TLS decision below compares literally.
///
/// Private: a caller who wants the canonical form of the base they supplied
/// reads it back from [`Client::base_url`], so exposing the function as well
/// would only be a second way to ask the same question.
fn normalize_base_url(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    let canonical = match trimmed.find("://") {
        Some(scheme_end) => {
            let (scheme, rest) = trimmed.split_at(scheme_end);
            format!("{}{rest}", scheme.to_ascii_lowercase())
        }
        None => trimmed.to_string(),
    };

    canonical
        .strip_suffix("/api/v1")
        .unwrap_or(&canonical)
        .trim_end_matches('/')
        .to_string()
}

/// A `reqwest::ClientBuilder` for a *normalized* base, skipping the platform's
/// native root store when the base is not `https://`.
///
/// Loading native roots enumerates the macOS keychain eagerly inside
/// `ClientBuilder::build` — seconds, before any request is made — even for a
/// plain `http://127.0.0.1` base that will never touch TLS. This is the one
/// trick worth keeping from the `coppice` CLI: a plain-HTTP base pays nothing,
/// and an `https://` base keeps the platform trust store automatically under
/// the same scheme check.
///
/// Exposed because a program that builds its own `reqwest::Client` — a
/// readiness poller, say — wants the same behaviour without reimplementing it.
pub fn plain_http_builder(base: &str) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder();
    // Compared as bytes, not sliced as `str`: a base whose first eight bytes
    // straddle a multi-byte character must not panic here.
    let is_https = base
        .as_bytes()
        .get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"https://"));
    // The toggle only exists when a TLS backend is compiled in; without one
    // there are no roots to load, and every builder is already "plain".
    #[cfg(feature = "rustls-tls-native-roots")]
    let builder = if is_https {
        builder
    } else {
        builder.tls_built_in_native_certs(false)
    };
    #[cfg(not(feature = "rustls-tls-native-roots"))]
    let _ = is_https;
    builder
}

/// Everything a [`Client`] shares between its scoped clones.
#[derive(Debug)]
struct Inner {
    base: String,
    http: reqwest::Client,
    token: Option<String>,
}

/// Builds a [`Client`].
///
/// ```no_run
/// use coppice_client::Client;
///
/// let client = Client::builder("https://coppice.example:7070")
///     .token("an-oidc-access-token")
///     .timeout(std::time::Duration::from_secs(10))
///     .build()?;
/// # Ok::<(), coppice_client::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    base: String,
    token: Option<String>,
    timeout: Duration,
    http: Option<reqwest::Client>,
}

impl ClientBuilder {
    /// Start from a base URL. Accepts a bare base (`http://host:7070`), a
    /// trailing slash, or one already ending in `/api/v1`.
    pub fn new(base: impl AsRef<str>) -> ClientBuilder {
        ClientBuilder {
            base: normalize_base_url(base.as_ref()),
            token: None,
            timeout: DEFAULT_TIMEOUT,
            http: None,
        }
    }

    /// Attach `Authorization: Bearer <token>` to every request.
    ///
    /// An empty or whitespace-only token is treated as no token at all: an
    /// environment variable that is set but empty must behave exactly like one
    /// that is unset, and in the open (auth-disabled) posture the header's
    /// mere presence is what must never appear.
    pub fn token(mut self, token: impl Into<String>) -> ClientBuilder {
        let token = token.into();
        self.token = Some(token.trim().to_string()).filter(|t| !t.is_empty());
        self
    }

    /// [`token`](Self::token), for a caller holding an `Option` — a `--token`
    /// flag, typically.
    pub fn token_opt(self, token: Option<impl Into<String>>) -> ClientBuilder {
        match token {
            Some(token) => self.token(token),
            None => self,
        }
    }

    /// Override the [`DEFAULT_TIMEOUT`] for one request.
    pub fn timeout(mut self, timeout: Duration) -> ClientBuilder {
        self.timeout = timeout;
        self
    }

    /// Use a caller-supplied `reqwest::Client` instead of building one.
    ///
    /// The timeout, the TLS posture and the connection pool are then entirely
    /// that client's; nothing here overrides them. Reach for this to share one
    /// pool across several Coppice clients, or to install a proxy, a custom
    /// root store, or a middleware stack.
    pub fn http_client(mut self, http: reqwest::Client) -> ClientBuilder {
        self.http = Some(http);
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<Client> {
        if self.base.is_empty() {
            return Err(Error::InvalidBaseUrl {
                base: self.base,
                reason: "the base URL is empty".to_string(),
            });
        }
        // Parsed here and thrown away: the client formats its URLs as strings,
        // but a base that is not a URL should fail when the client is built,
        // naming the base, rather than on the first request with whatever
        // `reqwest` makes of it.
        reqwest::Url::parse(&format!("{}/api/v1/", self.base)).map_err(|e| {
            Error::InvalidBaseUrl {
                base: self.base.clone(),
                reason: e.to_string(),
            }
        })?;

        let http = match self.http {
            Some(http) => http,
            None => plain_http_builder(&self.base)
                .timeout(self.timeout)
                .build()
                .map_err(Error::Build)?,
        };

        Ok(Client {
            inner: Arc::new(Inner {
                base: self.base,
                http,
                token: self.token,
            }),
            read: ReadOptions::new(),
        })
    }
}

/// A client bound to one coordinator's `/api/v1` surface.
///
/// Cloning is cheap — the connection pool, the base URL and the token are
/// shared — so a `Client` is meant to be passed around by value.
#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<Inner>,
    read: ReadOptions,
}

impl Client {
    /// A [`ClientBuilder`] for `base`.
    pub fn builder(base: impl AsRef<str>) -> ClientBuilder {
        ClientBuilder::new(base)
    }

    /// A client for `base` with no token and the default timeout.
    pub fn new(base: impl AsRef<str>) -> Result<Client> {
        ClientBuilder::new(base).build()
    }

    /// A client for the local coordinator ([`DEFAULT_BASE_URL`]).
    pub fn local() -> Result<Client> {
        Client::new(DEFAULT_BASE_URL)
    }

    /// The normalized base URL, without the `/api/v1` prefix.
    pub fn base_url(&self) -> &str {
        &self.inner.base
    }

    /// Whether a bearer token is attached.
    pub fn has_token(&self) -> bool {
        self.inner.token.is_some()
    }

    /// The absolute URL for an `/api/v1`-relative path (see [`paths`]).
    pub fn url(&self, path: &str) -> String {
        format!("{}/api/v1{path}", self.inner.base)
    }

    /// The absolute URL for a path outside `/api/v1` — [`paths::HEALTHZ`].
    pub fn root_url(&self, path: &str) -> String {
        format!("{}{path}", self.inner.base)
    }

    /// The read options this client applies.
    pub fn read_options(&self) -> &ReadOptions {
        &self.read
    }

    /// A clone of this client whose reads carry `options`.
    ///
    /// Scoped rather than mutating: the original keeps its own options, so a
    /// long-lived shared client can hand out a strongly-consistent view for
    /// one call without anything else noticing.
    pub fn with_read_options(&self, options: ReadOptions) -> Client {
        Client {
            inner: Arc::clone(&self.inner),
            read: options,
        }
    }

    // -- the untyped escape hatch -------------------------------------------

    /// GET an `/api/v1`-relative path and return the body as it arrived.
    ///
    /// This is the forward-compatibility hatch, and the one a `--json` mode
    /// wants: it prints the server's own body, including fields this client is
    /// too old to know about, so a machine-readable rendering can never
    /// disagree with the wire. Use [`paths`] to build `path` and a params
    /// struct's `query_pairs()` to build `query`, and the request is exactly
    /// the one the typed method would have made.
    ///
    /// This client's [`ReadOptions`] are applied, like any other read.
    pub async fn get_value(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Versioned<serde_json::Value>> {
        self.get(path, query).await
    }

    /// POST a JSON body to an `/api/v1`-relative path and return the answer as
    /// it arrived.
    pub async fn post_value(&self, path: &str, body: &impl Serialize) -> Result<serde_json::Value> {
        self.post(path, Some(body)).await
    }

    /// PUT a JSON body to an `/api/v1`-relative path and return the answer as
    /// it arrived.
    pub async fn put_value(&self, path: &str, body: &impl Serialize) -> Result<serde_json::Value> {
        self.put(path, body).await
    }

    // -- session and auth ---------------------------------------------------

    /// `GET /healthz` — is this process serving?
    ///
    /// Outside `/api/v1` and outside authentication: reaching it at all is the
    /// answer. It says nothing about readiness, phase, or cluster health.
    pub async fn healthz(&self) -> Result<HealthzResponse> {
        let request = self.authed(self.inner.http.get(self.root_url(paths::HEALTHZ)));
        let response = request.send().await.map_err(Error::Transport)?;
        Ok(decode(response).await?.0)
    }

    /// `GET /api/v1/session` — who this client's credential proves it is, and
    /// what that identity may do.
    pub async fn session(&self) -> Result<Versioned<GetSessionResponse>> {
        self.get(paths::SESSION, &[]).await
    }

    /// `GET /api/v1/auth/config` — the deployment's public authentication
    /// posture. The one endpoint reachable without a credential, because a
    /// client cannot obtain one without knowing this.
    pub async fn auth_config(&self) -> Result<GetAuthConfigResponse> {
        let config: Versioned<GetAuthConfigResponse> = self.get(paths::AUTH_CONFIG, &[]).await?;
        Ok(config.into_inner())
    }

    /// `GET /api/v1/authorization` — the replicated role bindings and the
    /// groups-claim name. A **strong** read by default: this is the document
    /// an operator edits and PUTs back, and a read-modify-write over a stale
    /// snapshot silently reverts whatever landed in between.
    pub async fn authorization(&self) -> Result<Versioned<GetAuthorizationResponse>> {
        self.get(paths::AUTHORIZATION, &[]).await
    }

    /// `PUT /api/v1/authorization` — replace the bindings list wholesale.
    ///
    /// Full replacement, not a merge: whatever is in `request` is what the
    /// cluster ends up with. Pair the returned `log_index` with
    /// [`ReadOptions::at_least`] to read your own write back.
    pub async fn update_authorization(
        &self,
        request: &UpdateAuthorizationRequest,
    ) -> Result<UpdateAuthorizationResponse> {
        request
            .validate()
            .map_err(|e| Error::InvalidRequest(e.to_string()))?;
        self.put_json(paths::AUTHORIZATION, request).await
    }

    // -- cluster ------------------------------------------------------------

    /// `GET /api/v1/overview` — queue depth and cluster capacity in one read.
    pub async fn overview(&self) -> Result<Versioned<GetClusterOverviewResponse>> {
        self.get(paths::OVERVIEW, &[]).await
    }

    /// `GET /api/v1/queue/stats` — the queue half of the overview on its own.
    pub async fn queue_stats(&self) -> Result<Versioned<QueueStats>> {
        self.get(paths::QUEUE_STATS, &[]).await
    }

    /// `GET /api/v1/coordinators` — this replica's view of the raft cluster.
    pub async fn coordinators(&self) -> Result<Versioned<GetCoordinatorStatusResponse>> {
        self.get(paths::COORDINATORS, &[]).await
    }

    // -- jobs ---------------------------------------------------------------

    /// `GET /api/v1/jobs` — one page of the filtered job list.
    ///
    /// A short page with a non-null `next_cursor` means *continue*, never
    /// *done*. [`list_jobs_paged`](Self::list_jobs_paged) does the walking.
    ///
    /// A filter is checked with [`JobFilter::validate`](crate::JobFilter::validate)
    /// before anything is sent, so a malformed tree is an
    /// [`Error::InvalidRequest`] rather than a round trip to be refused.
    pub async fn list_jobs(&self, params: &ListJobsParams) -> Result<Versioned<ListJobsResponse>> {
        if let Some(filter) = &params.filter {
            filter.validate().map_err(Error::InvalidRequest)?;
        }
        self.get(paths::JOBS, &params.query_pairs()).await
    }

    /// A pager over `GET /api/v1/jobs`.
    pub fn list_jobs_paged(&self, params: ListJobsParams) -> JobPager {
        JobPager::new(self.clone(), params)
    }

    /// `POST /api/v1/jobs` — submit a job.
    ///
    /// The id in `request` is the submission's idempotency identity: retrying
    /// after a timeout, a connection loss, or a leader change with the
    /// identical request resolves to the same job rather than a second one.
    /// Mint a fresh [`JobId`] per logical submission, and reuse it verbatim on
    /// every retry.
    pub async fn submit_job(&self, request: &SubmitJobRequest) -> Result<SubmitJobResponse> {
        request
            .validate()
            .map_err(|e| Error::InvalidRequest(e.to_string()))?;
        self.post_json(paths::JOBS, request).await
    }

    /// `GET /api/v1/jobs/{job}` — one job in full.
    pub async fn job(&self, job: JobId) -> Result<Versioned<JobDetail>> {
        self.get(&paths::job(job), &[]).await
    }

    /// `POST /api/v1/jobs/{job}/abort` — ask for a job to stop.
    ///
    /// This commits a desired-state transition; it does not synchronously stop
    /// the container.
    pub async fn abort_job(
        &self,
        job: JobId,
        request: &AbortJobRequest,
    ) -> Result<AbortJobResponse> {
        self.post_json(&paths::job_abort(job), request).await
    }

    /// `PUT /api/v1/jobs/{job}/metadata` — replace the whole metadata map.
    ///
    /// Sending an empty map clears it, which is the point of the verb. For a
    /// caller that owns only some keys,
    /// [`update_job_metadata`](Self::update_job_metadata) is the patch.
    pub async fn replace_job_metadata(
        &self,
        job: JobId,
        request: &ReplaceJobMetadataRequest,
    ) -> Result<ReplaceJobMetadataResponse> {
        request
            .metadata
            .validate()
            .map_err(|e| Error::InvalidRequest(e.to_string()))?;
        self.put_json(&paths::job_metadata(job), request).await
    }

    /// `POST /api/v1/jobs/{job}/metadata` — merge `set` over the stored map,
    /// then remove the `unset` keys.
    pub async fn update_job_metadata(
        &self,
        job: JobId,
        request: &UpdateJobMetadataRequest,
    ) -> Result<UpdateJobMetadataResponse> {
        request
            .validate()
            .map_err(|e| Error::InvalidRequest(e.to_string()))?;
        self.post_json(&paths::job_metadata(job), request).await
    }

    /// `GET /api/v1/jobs/{job}/timeline` — one page of a job's transitions.
    pub async fn job_timeline(
        &self,
        job: JobId,
        params: &TimelineParams,
    ) -> Result<Versioned<GetJobTimelineResponse>> {
        self.get(&paths::job_timeline(job), &params.query_pairs())
            .await
    }

    /// A pager over `GET /api/v1/jobs/{job}/timeline`.
    pub fn job_timeline_paged(&self, job: JobId, params: TimelineParams) -> TimelinePager {
        TimelinePager::new(self.clone(), job, params)
    }

    /// `GET /api/v1/jobs/{job}/logs` — one page of a job's captured output.
    pub async fn job_logs(
        &self,
        job: JobId,
        params: &LogsParams,
    ) -> Result<Versioned<GetJobLogsResponse>> {
        self.get(&paths::job_logs(job), &params.query_pairs()).await
    }

    /// A pager over `GET /api/v1/jobs/{job}/logs`, stopping at the head.
    pub fn job_logs_paged(&self, job: JobId, params: LogsParams) -> LogPager {
        LogPager::new(self.clone(), job, params)
    }

    /// A follower over `GET /api/v1/jobs/{job}/logs` that keeps going as new
    /// output arrives, until the job is finished. See [`LogFollower`].
    pub fn follow_job_logs(&self, job: JobId, options: FollowOptions) -> LogFollower {
        LogFollower::new(self.clone(), job, options)
    }

    /// `GET /api/v1/jobs/{job}/usage` — one page of a job's resource samples.
    pub async fn job_usage(
        &self,
        job: JobId,
        params: &UsageParams,
    ) -> Result<Versioned<GetJobUsageResponse>> {
        self.get(&paths::job_usage(job), &params.query_pairs())
            .await
    }

    /// A pager over `GET /api/v1/jobs/{job}/usage`.
    pub fn job_usage_paged(&self, job: JobId, params: UsageParams) -> UsagePager {
        UsagePager::new(self.clone(), job, params)
    }

    // -- nodes --------------------------------------------------------------

    /// `GET /api/v1/nodes` — every registered compute node.
    pub async fn list_nodes(&self) -> Result<Versioned<ListNodesResponse>> {
        self.get(paths::NODES, &[]).await
    }

    /// `GET /api/v1/nodes/{node}` — one node in full.
    pub async fn node(&self, node: NodeId) -> Result<Versioned<GetNodeResponse>> {
        self.get(&paths::node(node), &[]).await
    }

    /// `GET /api/v1/nodes/{node}/utilization` — one node's allocated-vs-used
    /// history.
    pub async fn node_utilization(
        &self,
        node: NodeId,
    ) -> Result<Versioned<GetNodeUtilizationResponse>> {
        self.get(&paths::node_utilization(node), &[]).await
    }

    /// `POST /api/v1/nodes/{node}/drain` — cordon a node. New placements stop;
    /// running work continues.
    pub async fn drain_node(&self, node: NodeId) -> Result<DrainNodeResponse> {
        self.post_empty(&paths::node_drain(node)).await
    }

    /// `POST /api/v1/nodes/{node}/undrain` — lift the cordon.
    pub async fn undrain_node(&self, node: NodeId) -> Result<DrainNodeResponse> {
        self.post_empty(&paths::node_undrain(node)).await
    }

    /// `POST /api/v1/nodes/{node}/remove` — evict a node's record. Refused
    /// while the node still accepts placements or holds a live allocation.
    pub async fn remove_node(&self, node: NodeId) -> Result<RemoveNodeResponse> {
        self.post_empty(&paths::node_remove(node)).await
    }

    // -- quota entities -----------------------------------------------------

    /// `GET /api/v1/quota-entities` — the whole quota tree.
    pub async fn list_quota_entities(&self) -> Result<Versioned<ListQuotaEntitiesResponse>> {
        self.get(paths::QUOTA_ENTITIES, &[]).await
    }

    /// `GET /api/v1/quota-entities/{entity}` — one entity, its ancestry, its
    /// children and its subtree stats. A **strong** read by default.
    pub async fn quota_entity(
        &self,
        entity: QuotaEntityId,
    ) -> Result<Versioned<GetQuotaEntityResponse>> {
        self.get(&paths::quota_entity(entity), &[]).await
    }

    /// `POST /api/v1/quota-entities` — create or update an entity.
    ///
    /// An upsert keyed on the client-minted id, so a retry after an unknown
    /// outcome lands on the same entity rather than creating a second one.
    pub async fn configure_quota_entity(
        &self,
        request: &ConfigureQuotaEntityRequest,
    ) -> Result<ConfigureQuotaEntityResponse> {
        self.post_json(paths::QUOTA_ENTITIES, request).await
    }

    // -- plumbing -----------------------------------------------------------

    /// Attach the bearer token, when there is one. When there is not, no
    /// `Authorization` header is sent at all — the header's mere presence is
    /// what an open-mode cluster must never see.
    fn authed(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.inner.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    /// The shared GET half: endpoint query pairs first, then this client's
    /// read options.
    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Versioned<T>> {
        let request = self
            .inner
            .http
            .get(self.url(path))
            .query(query)
            .query(&self.read.query_pairs());
        let response = self
            .authed(request)
            .send()
            .await
            .map_err(Error::Transport)?;
        let (value, indexes) = decode(response).await?;
        Ok(Versioned {
            value,
            applied_index: indexes.applied,
            committed_index: indexes.committed,
        })
    }

    async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<&impl Serialize>,
    ) -> Result<T> {
        let request = self.inner.http.post(self.url(path));
        let request = match body {
            Some(body) => request.json(body),
            None => request,
        };
        let response = self
            .authed(request)
            .send()
            .await
            .map_err(Error::Transport)?;
        Ok(decode(response).await?.0)
    }

    async fn post_json<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize) -> Result<T> {
        self.post(path, Some(body)).await
    }

    /// The three node-admin writes take no body at all: the path names the
    /// node and the route names the intent.
    async fn post_empty<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.post(path, None::<&()>).await
    }

    async fn put<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize) -> Result<T> {
        let request = self.inner.http.put(self.url(path)).json(body);
        let response = self
            .authed(request)
            .send()
            .await
            .map_err(Error::Transport)?;
        Ok(decode(response).await?.0)
    }

    async fn put_json<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize) -> Result<T> {
        self.put(path, body).await
    }
}

/// The two read indexes a response reports, when it reports them.
#[derive(Debug, Default, Clone, Copy)]
struct ReadIndexes {
    applied: Option<u64>,
    committed: Option<u64>,
}

/// Turn a response into a decoded body, or into the right kind of [`Error`].
///
/// The body is read as text and decoded here rather than through
/// `Response::json`, so a decode failure is distinguishable from a transport
/// one and a non-JSON error body survives into the message.
async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<(T, ReadIndexes)> {
    let status = response.status().as_u16();
    let headers = response.headers();
    let header_u64 = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
    };
    let indexes = ReadIndexes {
        applied: header_u64(APPLIED_INDEX_HEADER),
        committed: header_u64(COMMITTED_INDEX_HEADER),
    };
    let leader = headers
        .get(LEADER_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);

    let success = response.status().is_success();
    let body = response.text().await.map_err(Error::Transport)?;

    if !success {
        return Err(api_error(status, &body, leader));
    }
    let value = serde_json::from_str(&body).map_err(Error::Decode)?;
    Ok((value, indexes))
}

/// Read a non-2xx response into the error it describes.
fn api_error(status: u16, body: &str, leader: Option<String>) -> Error {
    #[derive(Deserialize)]
    struct ErrorBody {
        code: String,
        message: String,
    }

    match serde_json::from_str::<ErrorBody>(body) {
        Ok(parsed) => Error::Api {
            status,
            code: parsed
                .code
                .parse()
                .expect("ErrorCode::from_str never fails"),
            message: parsed.message,
            leader,
        },
        Err(_) => Error::UnexpectedStatus {
            status,
            body: body.trim().to_string(),
            leader,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A malformed filter is refused before any request is made: the base
    /// here is unroutable, so reaching the network would be a transport
    /// error instead. The pager goes through the same call.
    #[tokio::test]
    async fn list_jobs_validates_its_filter_locally() {
        let client = Client::new("http://127.0.0.1:1").unwrap();
        let params = ListJobsParams::new().with_filter(crate::JobFilter::all([]));
        let err = client.list_jobs(&params).await.expect_err("empty `all`");
        assert!(matches!(err, Error::InvalidRequest(_)), "{err:?}");
        let err = client
            .list_jobs_paged(params)
            .next_page()
            .await
            .expect_err("empty `all`");
        assert!(matches!(err, Error::InvalidRequest(_)), "{err:?}");
    }

    #[test]
    fn every_accepted_base_normalizes_to_one_form() {
        let want = "http://h:7070";
        for raw in [
            "http://h:7070",
            "http://h:7070/",
            "http://h:7070/api/v1",
            "http://h:7070/api/v1/",
            "  http://h:7070/api/v1/  ",
        ] {
            assert_eq!(normalize_base_url(raw), want, "{raw}");
        }
    }

    /// RFC 3986 says schemes are case-insensitive, and the TLS decision below
    /// compares the scheme literally.
    #[test]
    fn scheme_normalization_is_case_insensitive() {
        assert_eq!(normalize_base_url("HTTPS://h:7070"), "https://h:7070");
        assert_eq!(normalize_base_url("HtTp://h:7070"), "http://h:7070");
    }

    #[test]
    fn url_joins_the_api_prefix() {
        let client = Client::new("http://h:7070/api/v1/").unwrap();
        assert_eq!(client.url("/jobs"), "http://h:7070/api/v1/jobs");
        assert_eq!(client.root_url("/healthz"), "http://h:7070/healthz");
    }

    /// A base whose first eight *bytes* straddle a multi-byte character must
    /// not panic the scheme check.
    #[test]
    fn a_non_ascii_scheme_does_not_panic() {
        for base in ["a\u{e9}\u{e9}\u{e9}\u{e9}://host", "\u{e9}", "short"] {
            let _ = plain_http_builder(base);
            let _ = Client::new(base);
        }
    }

    #[test]
    fn clients_build_for_http_and_https_bases() {
        assert!(Client::new("http://h:7070").is_ok());
        assert!(Client::new("https://h:7070").is_ok());
        assert!(Client::new("HTTPS://h:7070").is_ok());
    }

    #[test]
    fn an_unparseable_base_is_a_typed_error() {
        let err = Client::new("not a url at all").expect_err("no scheme, no host");
        assert!(matches!(err, Error::InvalidBaseUrl { .. }), "{err:?}");
        assert!(Client::new("").is_err());
    }

    /// The default base and the default port are two literals that must name
    /// the same endpoint, and the base must survive its own normalization.
    #[test]
    fn the_default_base_names_the_default_port() {
        assert_eq!(DEFAULT_BASE_URL, format!("http://127.0.0.1:{DEFAULT_PORT}"));
        assert_eq!(normalize_base_url(DEFAULT_BASE_URL), DEFAULT_BASE_URL);
    }

    #[test]
    fn an_empty_token_is_no_token() {
        assert!(!Client::builder("http://h:7070")
            .token("   ")
            .build()
            .unwrap()
            .has_token());
        assert!(!Client::builder("http://h:7070")
            .token_opt(None::<String>)
            .build()
            .unwrap()
            .has_token());
        assert!(Client::builder("http://h:7070")
            .token("t")
            .build()
            .unwrap()
            .has_token());
    }

    #[test]
    fn read_options_render_their_query_pairs() {
        assert!(ReadOptions::new().query_pairs().is_empty());
        assert_eq!(
            ReadOptions::strong().with_min_index(42).query_pairs(),
            vec![
                ("consistency", "strong".to_string()),
                ("min_index", "42".to_string())
            ]
        );
        assert_eq!(
            ReadOptions::at_least(7).query_pairs(),
            vec![("min_index", "7".to_string())]
        );
    }

    /// Scoping must not disturb the client it came from.
    #[test]
    fn with_read_options_returns_a_scoped_clone() {
        let client = Client::new("http://h:7070").unwrap();
        let strong = client.with_read_options(ReadOptions::strong());
        assert_eq!(strong.read_options().consistency, Some(Consistency::Strong));
        assert_eq!(client.read_options().consistency, None);
        assert_eq!(strong.base_url(), client.base_url());
    }

    #[test]
    fn an_error_body_becomes_a_typed_api_error() {
        let err = api_error(404, r#"{"code":"NOT_FOUND","message":"no such job"}"#, None);
        assert!(err.is_not_found());
        assert_eq!(err.status(), Some(404));
        assert_eq!(err.to_string(), "api error (NOT_FOUND): no such job");
    }

    #[test]
    fn a_non_json_error_body_keeps_its_text() {
        let err = api_error(502, "  bad gateway\n", None);
        assert_eq!(err.to_string(), "api error (HTTP 502): bad gateway");
        assert_eq!(err.status(), Some(502));
        assert!(err.code().is_none());
    }

    #[test]
    fn a_leader_hint_reaches_the_message_and_the_predicate() {
        let err = api_error(
            421,
            r#"{"code":"NOT_LEADER","message":"not the leader"}"#,
            Some("10.0.0.2:7070".to_string()),
        );
        assert_eq!(err.leader_hint(), Some("10.0.0.2:7070"));
        assert!(err.is_retryable());
        assert!(err
            .to_string()
            .ends_with("retry against the leader at 10.0.0.2:7070"));
    }
}
