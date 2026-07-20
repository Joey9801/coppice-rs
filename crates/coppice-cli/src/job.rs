//! `coppice job`: the client verbs against a cluster's JSON HTTP API.
//!
//! These commands are a thin transport-and-presentation shell over the
//! coordinator's `/api/v1` surface (ADR 0031): they load a job spec, translate
//! it into the wire request, POST/GET over plain HTTP, and render the JSON
//! response as aligned plain text. All validation, authorization, and durable
//! state transitions belong to the coordinator; nothing here decides anything.
//!
//! The wire shapes are **not** redefined here — every request and response body
//! is a [`coppice_api::http::dto`] type, so the CLI can never drift from the
//! contract the web UI is built on. The one thing this module owns is the
//! *spec file*: a TOML description of a single job, which mirrors the daemon
//! config files' conventions (ADR 0020) — `deny_unknown_fields` so a typo
//! fail-stops, humane duration strings, and byte-size units — and which
//! converts into a [`dto::SubmitJobRequest`].
//!
//! A spec file looks like:
//!
//! ```toml
//! image = "busybox:1.36"
//! command = ["sh", "-c", "echo hello"]
//! # entrypoint = ["/bin/sh", "-c"]   # optional; the image default when absent
//! quota_entity = "quota-00000000-0000-0000-0000-000000000001"
//! priority = 0            # optional, default 0 (a multiplier index)
//! max_runtime = "1h"      # optional humantime duration, whole seconds
//!
//! [resources]
//! cpu_millis = 500
//! memory = "256MiB"
//! disk = "1GiB"
//!
//! [retry]                  # optional
//! max_retries = 3          # default 3
//! retry_user_errors = false # default false
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use coppice_api::http::dto;
use coppice_api::http::COPPICE_LEADER;
use coppice_core::bytes::ByteSize;
use coppice_core::id::{AttemptId, JobId, QuotaEntityId};

// ---------------------------------------------------------------------------
// Spec file
// ---------------------------------------------------------------------------

/// A TOML job spec: everything needed to submit one job.
///
/// `deny_unknown_fields` so a typo (`comand`, `max_runtme`) fail-stops naming
/// the offending key rather than silently defaulting — the same posture the
/// wire [`dto::SubmitJobRequest`] takes on the server side.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    /// Container image reference (`repo:tag` or `repo@digest`).
    pub image: String,
    /// The container command line, pre-tokenized (argv semantics, no shell
    /// parsing). Required and non-empty.
    pub command: Vec<String>,
    /// Entrypoint override; absent runs the image's own entrypoint. When
    /// present, must be non-empty.
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
    /// The quota-entity leaf to charge (`quota-<uuid>`).
    pub quota_entity: QuotaEntityId,
    /// Priority multiplier index; resolved through the replicated multiplier
    /// table (`coppice dev` seeds `-2..=2`). Default 0.
    #[serde(default)]
    pub priority: i32,
    /// Enforced runtime bound. A humane duration string (`"1h"`, `"90s"`);
    /// whole seconds only, since the wire field is whole seconds. Absent = the
    /// policy default charge runtime.
    #[serde(default, with = "humantime_serde")]
    pub max_runtime: Option<Duration>,
    /// Requested resources for scheduling and isolation.
    pub resources: ResourcesSpec,
    /// Retry policy. Absent = the platform default policy.
    #[serde(default)]
    pub retry: Option<RetrySpec>,
}

/// Requested resources. All three dimensions are required — a defaulted request
/// is almost always a mistake, and the server requires them too.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcesSpec {
    /// Milli-CPU units (1000 = one core).
    pub cpu_millis: u64,
    /// Memory limit, as a byte-size string (`"256MiB"`, `"2GB"`).
    pub memory: ByteSize,
    /// Scratch-disk limit, as a byte-size string.
    pub disk: ByteSize,
}

/// Optional retry policy. Present as a `[retry]` table; each field defaults
/// individually, matching `coppice_core::job::RetryPolicy`'s defaults.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrySpec {
    /// Maximum automatic retries. Default 3.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Opt-in to retrying user-error outcomes (nonzero exit, limit breaches).
    /// Default false.
    #[serde(default)]
    pub retry_user_errors: bool,
}

fn default_max_retries() -> u32 {
    3
}

impl JobSpec {
    /// Read, parse, and validate a spec file, wrapping every step with the
    /// file path so the error names both the file and (via serde) the key.
    pub fn load(path: &Path) -> Result<JobSpec> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading job spec {}", path.display()))?;
        let spec: JobSpec =
            toml::from_str(&raw).with_context(|| format!("reading job spec {}", path.display()))?;
        spec.validate()
            .with_context(|| format!("validating job spec {}", path.display()))?;
        Ok(spec)
    }

    /// Reject values serde alone cannot catch, mirroring the server's own
    /// checks so a bad spec fails client-side with a good message rather than
    /// as an opaque `INVALID_ARGUMENT` after a round-trip.
    fn validate(&self) -> Result<()> {
        if self.command.is_empty() {
            bail!("command must not be empty");
        }
        if let Some(entrypoint) = &self.entrypoint {
            if entrypoint.is_empty() {
                bail!("entrypoint, when set, must not be empty");
            }
        }
        if let Some(max_runtime) = self.max_runtime {
            if max_runtime.is_zero() {
                bail!("max_runtime must be greater than zero");
            }
            if max_runtime.subsec_nanos() != 0 {
                bail!(
                    "max_runtime must be a whole number of seconds (the wire field has \
                     no sub-second precision)"
                );
            }
            i64::try_from(max_runtime.as_secs())
                .map_err(|_| anyhow::anyhow!("max_runtime is too large to express in seconds"))?;
        }
        Ok(())
    }

    /// Convert the (validated) spec into the wire request, threading in the
    /// client-minted job id (the idempotency key, ADR 0026).
    pub fn request(&self, job: JobId) -> dto::SubmitJobRequest {
        dto::SubmitJobRequest {
            job,
            image: self.image.clone(),
            command: self.command.clone(),
            entrypoint: self.entrypoint.clone(),
            requests: dto::Resources {
                cpu_millis: self.resources.cpu_millis,
                memory_bytes: self.resources.memory.as_u64(),
                disk_bytes: self.resources.disk.as_u64(),
            },
            priority: self.priority,
            // Validated to fit i64 seconds with no sub-second remainder.
            max_runtime_seconds: self.max_runtime.map(|d| d.as_secs() as i64),
            quota_entity: self.quota_entity,
            retry: self.retry.map(|r| dto::RetryPolicy {
                max_retries: r.max_retries,
                retry_user_errors: r.retry_user_errors,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// CLI surface
// ---------------------------------------------------------------------------

/// `coppice job` argument group. `--api` is global, so it may appear before or
/// after the verb (`coppice job --api URL submit …` and
/// `coppice job submit … --api URL` both work).
#[derive(Debug, clap::Args)]
pub struct JobArgs {
    /// Base URL of the coordinator's client API. Accepts either a bare base
    /// (`http://host:7070`) or one already ending in `/api/v1` (what the
    /// `coppice dev` banner prints), normalized to the same thing.
    #[arg(
        long,
        global = true,
        env = "COPPICE_API",
        default_value = "http://127.0.0.1:7070"
    )]
    api: String,

    #[command(subcommand)]
    pub command: JobCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum JobCommand {
    /// Submit a job from a TOML spec file.
    Submit {
        /// Path to the job spec (`.toml`).
        spec: PathBuf,
        /// Explicit job id for idempotent resubmission (`job-<uuid>`). The
        /// client-minted id is the submission's idempotency key (ADR 0026):
        /// re-submitting with the same id and payload resolves to the same
        /// job. Defaults to a fresh id.
        #[arg(long)]
        job: Option<JobId>,
    },
    /// Show a job's current status.
    Status {
        /// Job id (`job-<uuid>`).
        job: JobId,
    },
    /// Print a job's logs (best-effort, ADR 0034).
    Logs {
        /// Job id (`job-<uuid>`).
        job: JobId,
        /// Restrict to one stream.
        #[arg(long, value_enum)]
        stream: Option<StreamArg>,
        /// Restrict to one attempt (`attempt-<uuid>`).
        #[arg(long)]
        attempt: Option<AttemptId>,
        /// Chronological (`asc`) or newest-first (`desc`); default `asc`.
        #[arg(long, value_enum)]
        order: Option<OrderArg>,
        /// Follow the log as it grows, polling until the job is terminal.
        /// Forces chronological order (conflicts with `--order desc`).
        #[arg(long)]
        follow: bool,
    },
    /// Request a job's abort (a desired-state transition, not a synchronous
    /// stop).
    Abort {
        /// Job id (`job-<uuid>`).
        job: JobId,
        /// Optional reason, recorded in job history and events.
        #[arg(long)]
        reason: Option<String>,
    },
}

/// One of an attempt's two output streams (the `--stream` value).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum StreamArg {
    Stdout,
    Stderr,
}

impl From<StreamArg> for dto::LogStreamName {
    fn from(s: StreamArg) -> dto::LogStreamName {
        match s {
            StreamArg::Stdout => dto::LogStreamName::Stdout,
            StreamArg::Stderr => dto::LogStreamName::Stderr,
        }
    }
}

/// The `--order` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OrderArg {
    Asc,
    Desc,
}

/// How often `--follow` re-polls once caught up to the live head.
const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Run the selected `coppice job` verb.
pub async fn run(args: JobArgs) -> Result<()> {
    let base = normalize_base(&args.api);
    let client = reqwest::Client::new();
    match args.command {
        JobCommand::Submit { spec, job } => submit(&client, &base, &spec, job).await,
        JobCommand::Status { job } => status(&client, &base, job).await,
        JobCommand::Logs {
            job,
            stream,
            attempt,
            order,
            follow,
        } => {
            let order = resolve_order(order, follow)?;
            let stream = stream.map(dto::LogStreamName::from);
            if follow {
                run_follow(&client, &base, job, stream, attempt, FOLLOW_POLL_INTERVAL).await
            } else {
                run_logs(&client, &base, job, stream, attempt, order).await
            }
        }
        JobCommand::Abort { job, reason } => abort(&client, &base, job, reason).await,
    }
}

/// Reduce an `--api` value to a bare base URL. Trims a trailing slash, then a
/// trailing `/api/v1` (the form the dev banner prints and users paste), then
/// any slash that exposes — so every accepted form maps to the same base.
fn normalize_base(raw: &str) -> String {
    let trimmed = raw.trim_end_matches('/');
    trimmed
        .strip_suffix("/api/v1")
        .unwrap_or(trimmed)
        .trim_end_matches('/')
        .to_string()
}

/// Resolve the effective log order. `--follow` streams chronologically, so it
/// forces `asc` and refuses an explicit `--order desc`.
fn resolve_order(order: Option<OrderArg>, follow: bool) -> Result<dto::LogOrder> {
    if follow {
        if order == Some(OrderArg::Desc) {
            bail!("--follow streams chronologically and cannot be combined with --order desc");
        }
        return Ok(dto::LogOrder::Asc);
    }
    Ok(match order {
        Some(OrderArg::Desc) => dto::LogOrder::Desc,
        // The CLI default is chronological — what a terminal reader expects —
        // even though the server default is `desc`.
        Some(OrderArg::Asc) | None => dto::LogOrder::Asc,
    })
}

// ---------------------------------------------------------------------------
// Verbs
// ---------------------------------------------------------------------------

async fn submit(
    client: &reqwest::Client,
    base: &str,
    spec_path: &Path,
    job: Option<JobId>,
) -> Result<()> {
    let spec = JobSpec::load(spec_path)?;
    let job = job.unwrap_or_else(JobId::new);
    let request = spec.request(job);
    let response = client
        .post(format!("{base}/api/v1/jobs"))
        .json(&request)
        .send()
        .await
        .context("submitting job")?;
    if !response.status().is_success() {
        return Err(api_error(response).await);
    }
    let submitted: dto::SubmitJobResponse =
        response.json().await.context("reading submit response")?;
    println!(
        "submitted {} (log index {})",
        submitted.job, submitted.log_index
    );
    Ok(())
}

async fn status(client: &reqwest::Client, base: &str, job: JobId) -> Result<()> {
    let detail = get_job(client, base, job).await?;
    print!("{}", render_status(&detail));
    Ok(())
}

async fn abort(
    client: &reqwest::Client,
    base: &str,
    job: JobId,
    reason: Option<String>,
) -> Result<()> {
    // The path segment is authoritative for the job id; the body omits it.
    let request = dto::AbortJobRequest { job: None, reason };
    let response = client
        .post(format!("{base}/api/v1/jobs/{job}/abort"))
        .json(&request)
        .send()
        .await
        .context("requesting abort")?;
    if !response.status().is_success() {
        return Err(api_error(response).await);
    }
    println!("abort requested for {job}");
    Ok(())
}

/// GET a job's detail, mapping a non-2xx response to a rich error.
async fn get_job(client: &reqwest::Client, base: &str, job: JobId) -> Result<dto::JobDetail> {
    let response = client
        .get(format!("{base}/api/v1/jobs/{job}"))
        .send()
        .await
        .context("fetching job status")?;
    if !response.status().is_success() {
        return Err(api_error(response).await);
    }
    response.json().await.context("reading job detail")
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

/// The non-follow log walk: page through `next_cursor` until it is null, then
/// report any source that was not fully available.
async fn run_logs(
    client: &reqwest::Client,
    base: &str,
    job: JobId,
    stream: Option<dto::LogStreamName>,
    attempt: Option<AttemptId>,
    order: dto::LogOrder,
) -> Result<()> {
    // Attempt multiplicity decides prefixing, and it must be known up front: a
    // page can cover a single attempt even when the job has several (the server
    // ends a page wherever the budget lands), so the walk itself is a late
    // signal. The extra GET also surfaces NOT_FOUND before the first page.
    let mut multi = initial_multi(&get_job(client, base, job).await?, attempt);
    let mut sources: Vec<dto::LogSourceRecord> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = fetch_logs_page(
            client,
            base,
            job,
            stream,
            attempt,
            order,
            cursor.as_deref(),
            None,
        )
        .await?;
        merge_sources(&mut sources, page.sources);
        multi = latch_multi(multi, &sources);
        for entry in &page.entries {
            print_entry(entry, multi);
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    print_source_notes(&sources);
    Ok(())
}

/// Whether lines need an attempt prefix, decided from the job's attempt list:
/// more than one attempt, unless `--attempt` scopes the walk to a single one
/// (scoped output is single-attempt by construction, so a prefix is noise).
fn initial_multi(detail: &dto::JobDetail, attempt: Option<AttemptId>) -> bool {
    attempt.is_none() && detail.attempts.len() > 1
}

/// Safety net behind [`initial_multi`]: if a second attempt appears mid-walk
/// anyway (a retry landing during `--follow`), start prefixing and attribute
/// the lines already printed — they all belong to the previously-sole attempt.
fn latch_multi(multi: bool, sources: &[dto::LogSourceRecord]) -> bool {
    if !multi && sources.len() > 1 {
        if let Some(first) = sources.first() {
            eprintln!(
                "note: earlier lines without an attempt prefix are from attempt {}",
                first.attempt
            );
        }
        return true;
    }
    multi
}

/// Follow state carried across polls. A null cursor loses the exact resume
/// position, so we also remember the last printed entry's instant and how many
/// entries we already printed at that instant — a re-poll passes `from=<last>`
/// (inclusive) and skips that many leading duplicates.
#[derive(Default)]
struct FollowState {
    sources: Vec<dto::LogSourceRecord>,
    multi: bool,
    cursor: Option<String>,
    last_at: Option<String>,
    last_at_count: usize,
}

/// The `--follow` loop: drain to the live head, and once caught up either exit
/// (job terminal, after one final drain for stragglers) or sleep and re-poll.
async fn run_follow(
    client: &reqwest::Client,
    base: &str,
    job: JobId,
    stream: Option<dto::LogStreamName>,
    attempt: Option<AttemptId>,
    interval: Duration,
) -> Result<()> {
    let mut state = FollowState {
        // Same up-front multiplicity decision as the non-follow walk (a page is
        // a late signal); `latch_multi` covers retries that appear mid-follow.
        multi: initial_multi(&get_job(client, base, job).await?, attempt),
        ..FollowState::default()
    };
    loop {
        drain_to_head(client, base, job, stream, attempt, &mut state).await?;
        let detail = get_job(client, base, job).await?;
        if is_terminal(detail.state) {
            // A last drain catches anything written between our final page and
            // the job reaching a terminal state.
            drain_to_head(client, base, job, stream, attempt, &mut state).await?;
            break;
        }
        tokio::time::sleep(interval).await;
    }
    print_source_notes(&state.sources);
    Ok(())
}

/// Fetch and print pages until the walk reaches the live head (`next_cursor`
/// null), resuming by cursor when one is held and by `from`/skip otherwise.
async fn drain_to_head(
    client: &reqwest::Client,
    base: &str,
    job: JobId,
    stream: Option<dto::LogStreamName>,
    attempt: Option<AttemptId>,
    state: &mut FollowState,
) -> Result<()> {
    loop {
        // Resume from the held cursor when we have one; otherwise from the last
        // printed instant (inclusive), skipping already-printed duplicates.
        let (from, skip) = match &state.cursor {
            Some(_) => (None, 0),
            None => (state.last_at.clone(), state.last_at_count),
        };
        let page = fetch_logs_page(
            client,
            base,
            job,
            stream,
            attempt,
            dto::LogOrder::Asc,
            state.cursor.as_deref(),
            from.as_deref(),
        )
        .await?;
        merge_sources(&mut state.sources, page.sources);
        state.multi = latch_multi(state.multi, &state.sources);

        let mut skipped = 0usize;
        for entry in &page.entries {
            let at = entry.at.to_string();
            // Only the first (from-resumed) page skips leading duplicates.
            if from.as_deref() == Some(at.as_str()) && skipped < skip {
                skipped += 1;
                continue;
            }
            print_entry(entry, state.multi);
            if state.last_at.as_deref() == Some(at.as_str()) {
                state.last_at_count += 1;
            } else {
                state.last_at = Some(at);
                state.last_at_count = 1;
            }
        }

        state.cursor = page.next_cursor;
        if state.cursor.is_none() {
            break;
        }
    }
    Ok(())
}

/// One logs GET, mapping a non-2xx response to a rich error.
#[allow(clippy::too_many_arguments)]
async fn fetch_logs_page(
    client: &reqwest::Client,
    base: &str,
    job: JobId,
    stream: Option<dto::LogStreamName>,
    attempt: Option<AttemptId>,
    order: dto::LogOrder,
    cursor: Option<&str>,
    from: Option<&str>,
) -> Result<dto::GetJobLogsResponse> {
    let mut query: Vec<(&str, String)> = vec![("order", order.as_str().to_string())];
    if let Some(stream) = stream {
        query.push(("stream", stream_query(stream).to_string()));
    }
    if let Some(attempt) = attempt {
        query.push(("attempt", attempt.to_string()));
    }
    if let Some(cursor) = cursor {
        query.push(("cursor", cursor.to_string()));
    }
    if let Some(from) = from {
        query.push(("from", from.to_string()));
    }
    let response = client
        .get(format!("{base}/api/v1/jobs/{job}/logs"))
        .query(&query)
        .send()
        .await
        .context("fetching job logs")?;
    if !response.status().is_success() {
        return Err(api_error(response).await);
    }
    response.json().await.context("reading job logs")
}

/// The `stream=` query spelling.
fn stream_query(stream: dto::LogStreamName) -> &'static str {
    match stream {
        dto::LogStreamName::Stdout => "stdout",
        dto::LogStreamName::Stderr => "stderr",
    }
}

/// Print one log line to stdout, prefixing the attempt id only when the walk
/// spans more than one attempt (so a single-attempt job stays uncluttered).
/// An entry whose own text was cut to fit the page byte budget gets a stderr
/// warning — the dropped tail is not retrievable, so silence would present
/// corrupted output as complete.
fn print_entry(entry: &dto::LogEntry, prefix_attempt: bool) {
    let text = entry.text.strip_suffix('\n').unwrap_or(&entry.text);
    let stream = stream_query(entry.stream);
    if prefix_attempt {
        println!("{} {} {} {}", entry.attempt, entry.at, stream, text);
    } else {
        println!("{} {} {}", entry.at, stream, text);
    }
    if entry.truncated {
        eprintln!(
            "note: the entry at {} (attempt {}) was cut to fit the page byte budget; \
             its tail is not retrievable",
            entry.at, entry.attempt
        );
    }
}

/// Merge a page's source records into the running set, keeping insertion order
/// and letting a later record for the same attempt supersede an earlier one —
/// except `truncated`, which is sticky: it is evidence of loss, and a later
/// page over a narrower range (a follow re-poll with `from=`) legitimately
/// reports false without unsaying it. The set stays deduplicated by attempt,
/// so its length is the number of distinct attempts seen.
fn merge_sources(into: &mut Vec<dto::LogSourceRecord>, page: Vec<dto::LogSourceRecord>) {
    for record in page {
        match into.iter_mut().find(|s| s.attempt == record.attempt) {
            Some(existing) => {
                let truncated = existing.truncated || record.truncated;
                *existing = record;
                existing.truncated = truncated;
            }
            None => into.push(record),
        }
    }
}

/// After the walk, report to stderr any source that was not fully available,
/// or whose older lines were pruned — the honesty accounting ADR 0034 requires.
fn print_source_notes(sources: &[dto::LogSourceRecord]) {
    for source in sources {
        if let Some(verdict) = availability_label(source.availability) {
            let reason = source
                .reason
                .as_deref()
                .map(|r| format!(": {r}"))
                .unwrap_or_default();
            eprintln!("note: attempt {} logs {verdict}{reason}", source.attempt);
        }
        if source.truncated {
            eprintln!(
                "note: attempt {} logs truncated (older lines pruned)",
                source.attempt
            );
        }
    }
}

/// The stderr note word for a non-available verdict, or `None` when available.
fn availability_label(availability: dto::LogAvailability) -> Option<&'static str> {
    match availability {
        dto::LogAvailability::Available => None,
        dto::LogAvailability::Expired => Some("expired"),
        dto::LogAvailability::Unreachable => Some("unreachable"),
        dto::LogAvailability::NotStarted => Some("not_started"),
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn is_terminal(state: dto::JobStateKind) -> bool {
    matches!(
        state,
        dto::JobStateKind::Succeeded | dto::JobStateKind::Failed | dto::JobStateKind::Aborted
    )
}

fn job_state_label(state: dto::JobStateKind) -> &'static str {
    use dto::JobStateKind as S;
    match state {
        S::Submitted => "submitted",
        S::Accepted => "accepted",
        S::Queued => "queued",
        S::Attempting => "attempting",
        S::Succeeded => "succeeded",
        S::Failed => "failed",
        S::Aborted => "aborted",
    }
}

fn attempt_state_label(state: dto::AttemptState) -> &'static str {
    use dto::AttemptState as S;
    match state {
        S::Accruing => "accruing",
        S::Ready => "ready",
        S::Dispatching => "dispatching",
        S::Running => "running",
        S::Finalizing => "finalizing",
        S::Terminal => "terminal",
    }
}

fn outcome_kind_label(kind: dto::AttemptOutcomeKind) -> &'static str {
    use dto::AttemptOutcomeKind as K;
    match kind {
        K::Exited => "exited",
        K::MemoryLimitExceeded => "memory_limit_exceeded",
        K::RuntimeLimitExceeded => "runtime_limit_exceeded",
        K::DiskLimitExceeded => "disk_limit_exceeded",
        K::Aborted => "aborted",
        K::Revoked => "revoked",
        K::PullFailed => "pull_failed",
        K::StartFailed => "start_failed",
        K::NodeLost => "node_lost",
        K::AgentError => "agent_error",
    }
}

/// Render a `JobDetail` as aligned plain-text key/value lines plus an attempts
/// section — deliberately modest, skipping the queue/accrual/cost-breakdown
/// depth the web UI shows.
fn render_status(detail: &dto::JobDetail) -> String {
    use std::fmt::Write;

    let spec = &detail.spec;
    let mut out = String::new();
    let mut kv = |key: &str, value: &str| {
        let _ = writeln!(out, "{key:<16}{value}");
    };

    kv("id", &detail.id.to_string());
    kv("state", job_state_label(detail.state));
    kv("image", &spec.image);
    kv("command", &spec.command.join(" "));
    if let Some(entrypoint) = &spec.entrypoint {
        kv("entrypoint", &entrypoint.join(" "));
    }
    kv("quota entity", &spec.quota_entity.to_string());
    kv("priority", &spec.priority.to_string());
    kv(
        "requests",
        &format!(
            "cpu {} mCPU, memory {}, disk {}",
            spec.requests.cpu_millis,
            ByteSize::from_bytes(spec.requests.memory_bytes),
            ByteSize::from_bytes(spec.requests.disk_bytes),
        ),
    );
    match spec.max_runtime_seconds {
        Some(seconds) => kv("max runtime", &format!("{seconds}s")),
        None => kv("max runtime", "unbounded"),
    }
    kv("submitted at", &detail.submitted_at.to_string());
    kv("state since", &detail.state_since.to_string());
    if let Some(terminal_at) = detail.terminal_at {
        kv("terminal at", &terminal_at.to_string());
    }
    kv("retries used", &detail.retries_used.to_string());
    // Gross µCU charged across the job's attempts so far (the one cost figure
    // worth a status line; the full breakdown lives in the web UI).
    kv(
        "cost (charged)",
        &format!("{} uCU", detail.cost.charged_ucu),
    );
    if let Some(abort) = &detail.abort_requested {
        let reason = abort.reason.as_deref().unwrap_or("(none)");
        kv(
            "abort requested",
            &format!("{} at {}", reason, abort.requested_at),
        );
    }

    if detail.attempts.is_empty() {
        let _ = writeln!(out, "attempts        (none)");
    } else {
        let _ = writeln!(out, "attempts:");
        for attempt in &detail.attempts {
            let mut line = format!(
                "  {} {} node {}",
                attempt.id,
                attempt_state_label(attempt.state),
                attempt.node
            );
            if let Some(started) = attempt.started_at {
                let _ = write!(line, " started {started}");
            }
            if let Some(ended) = attempt.ended_at {
                let _ = write!(line, " ended {ended}");
            }
            if let Some(outcome) = &attempt.outcome {
                let _ = write!(line, " outcome {}", outcome_kind_label(outcome.kind));
                if let Some(code) = outcome.exit_code {
                    let _ = write!(line, " (exit {code})");
                }
            }
            let _ = writeln!(out, "{line}");
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The wire error body (ADR 0031). The API's own `ErrorBody` is private and
/// serialize-only, so the client mirrors just the two fields it reads.
#[derive(Debug, Deserialize)]
struct ApiErrorBody {
    code: String,
    message: String,
}

/// Turn a non-2xx response into an `anyhow` error, reading the `{code,
/// message}` body (falling back to raw text) and, on a 421/NOT_LEADER with a
/// `Coppice-Leader` hint, appending where to retry.
async fn api_error(response: reqwest::Response) -> anyhow::Error {
    let status = response.status();
    let leader = response
        .headers()
        .get(COPPICE_LEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response.text().await.unwrap_or_default();

    let mut message = match serde_json::from_str::<ApiErrorBody>(&body) {
        Ok(parsed) => format!("api error ({}): {}", parsed.code, parsed.message),
        Err(_) if !body.trim().is_empty() => {
            format!("api error (HTTP {}): {}", status.as_u16(), body.trim())
        }
        Err(_) => format!("api error (HTTP {})", status.as_u16()),
    };
    if status == reqwest::StatusCode::MISDIRECTED_REQUEST {
        if let Some(leader) = leader {
            message.push_str(&format!("; retry against the leader at {leader}"));
        }
    }
    anyhow::anyhow!(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write as _;
    use std::sync::{Arc, Mutex};

    use axum::extract::{Path as AxumPath, Query, State};
    use axum::http::{HeaderMap, StatusCode as AxumStatus};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use coppice_core::time::Timestamp;

    // -- Spec parsing -------------------------------------------------------

    const MINIMAL_SPEC: &str = r#"
image = "busybox:1.36"
command = ["sh", "-c", "echo hi"]
quota_entity = "quota-00000000-0000-0000-0000-000000000001"

[resources]
cpu_millis = 500
memory = "256MiB"
disk = "1GiB"
"#;

    const FULL_SPEC: &str = r#"
image = "busybox:1.36"
command = ["sh", "-c", "echo hi"]
entrypoint = ["/bin/sh", "-c"]
quota_entity = "quota-00000000-0000-0000-0000-000000000001"
priority = 2
max_runtime = "1h"

[resources]
cpu_millis = 500
memory = "256MiB"
disk = "1GiB"

[retry]
max_retries = 5
retry_user_errors = true
"#;

    fn parse(spec: &str) -> Result<JobSpec> {
        let toml: JobSpec = toml::from_str(spec)?;
        toml.validate()?;
        Ok(toml)
    }

    #[test]
    fn minimal_spec_parses_with_defaults() {
        let spec = parse(MINIMAL_SPEC).expect("minimal spec parses");
        assert_eq!(spec.image, "busybox:1.36");
        assert_eq!(spec.command, ["sh", "-c", "echo hi"]);
        assert!(spec.entrypoint.is_none());
        assert_eq!(spec.priority, 0);
        assert!(spec.max_runtime.is_none());
        assert!(spec.retry.is_none());
        assert_eq!(spec.resources.cpu_millis, 500);
        assert_eq!(spec.resources.memory, ByteSize::from_mib(256));
        assert_eq!(spec.resources.disk, ByteSize::from_gib(1));
    }

    #[test]
    fn full_spec_parses_every_field() {
        let spec = parse(FULL_SPEC).expect("full spec parses");
        assert_eq!(
            spec.entrypoint.as_deref(),
            Some(&["/bin/sh".to_string(), "-c".to_string()][..])
        );
        assert_eq!(spec.priority, 2);
        assert_eq!(spec.max_runtime, Some(Duration::from_secs(3600)));
        let retry = spec.retry.expect("retry present");
        assert_eq!(retry.max_retries, 5);
        assert!(retry.retry_user_errors);
    }

    #[test]
    fn unknown_top_level_key_is_rejected() {
        let bad = format!("{MINIMAL_SPEC}\nimagee = \"x\"\n");
        assert!(toml::from_str::<JobSpec>(&bad).is_err());
    }

    #[test]
    fn unknown_nested_key_is_rejected() {
        // A typo inside `[resources]` must fail, not silently default.
        let bad = MINIMAL_SPEC.replace("cpu_millis = 500", "cpu_milis = 500");
        assert!(toml::from_str::<JobSpec>(&bad).is_err());
    }

    #[test]
    fn bare_integer_duration_is_rejected() {
        let bad = format!("{MINIMAL_SPEC}\nmax_runtime = 3600\n");
        assert!(toml::from_str::<JobSpec>(&bad).is_err());
    }

    #[test]
    fn bare_integer_memory_is_rejected() {
        let bad = MINIMAL_SPEC.replace("memory = \"256MiB\"", "memory = 268435456");
        assert!(toml::from_str::<JobSpec>(&bad).is_err());
    }

    #[test]
    fn sub_second_max_runtime_is_rejected() {
        // Insert before `[resources]` so it stays a top-level key.
        let bad = MINIMAL_SPEC.replace("[resources]", "max_runtime = \"1500ms\"\n\n[resources]");
        let err = parse(&bad).expect_err("sub-second runtime rejected");
        assert!(format!("{err:#}").contains("whole number of seconds"));
    }

    #[test]
    fn zero_max_runtime_is_rejected() {
        let bad = MINIMAL_SPEC.replace("[resources]", "max_runtime = \"0s\"\n\n[resources]");
        let err = parse(&bad).expect_err("zero runtime rejected");
        assert!(format!("{err:#}").contains("greater than zero"));
    }

    #[test]
    fn empty_command_is_rejected() {
        let bad = MINIMAL_SPEC.replace(r#"command = ["sh", "-c", "echo hi"]"#, "command = []");
        let err = parse(&bad).expect_err("empty command rejected");
        assert!(format!("{err:#}").contains("command"));
    }

    #[test]
    fn empty_entrypoint_is_rejected() {
        let bad = MINIMAL_SPEC.replace("[resources]", "entrypoint = []\n\n[resources]");
        let err = parse(&bad).expect_err("empty entrypoint rejected");
        assert!(format!("{err:#}").contains("entrypoint"));
    }

    #[test]
    fn retry_defaults_apply_when_table_is_bare() {
        let spec = parse(&(MINIMAL_SPEC.to_string() + "\n[retry]\n")).expect("bare retry parses");
        let retry = spec.retry.expect("retry present");
        assert_eq!(retry.max_retries, 3);
        assert!(!retry.retry_user_errors);
    }

    #[test]
    fn request_threads_id_and_converts_units() {
        let spec = parse(FULL_SPEC).unwrap();
        let job = JobId::new();
        let request = spec.request(job);
        assert_eq!(request.job, job);
        assert_eq!(request.image, "busybox:1.36");
        assert_eq!(request.max_runtime_seconds, Some(3600));
        assert_eq!(request.requests.cpu_millis, 500);
        assert_eq!(request.requests.memory_bytes, 256 * 1024 * 1024);
        assert_eq!(request.requests.disk_bytes, 1024 * 1024 * 1024);
        assert_eq!(request.priority, 2);
        let retry = request.retry.expect("retry present");
        assert_eq!(retry.max_retries, 5);
        assert!(retry.retry_user_errors);
    }

    // -- API base normalization --------------------------------------------

    #[test]
    fn api_base_normalizes_to_one_form() {
        let want = "http://h:7070";
        for raw in [
            "http://h:7070",
            "http://h:7070/",
            "http://h:7070/api/v1",
            "http://h:7070/api/v1/",
        ] {
            assert_eq!(normalize_base(raw), want, "{raw}");
        }
    }

    // -- HTTP round-trips ---------------------------------------------------

    /// Spawn `router` on an ephemeral loopback port and return its base URL.
    async fn spawn(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn client() -> reqwest::Client {
        reqwest::Client::new()
    }

    /// Build the wire error body (no shared Rust type exists for it — the API's
    /// own `ErrorBody` is private).
    fn error_body(code: &str, message: &str) -> serde_json::Value {
        serde_json::json!({ "code": code, "message": message })
    }

    fn write_spec(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    #[tokio::test]
    async fn submit_posts_the_dto_and_prints_the_id() {
        let captured: Arc<Mutex<Vec<dto::SubmitJobRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let router = Router::new()
            .route(
                "/api/v1/jobs",
                post(
                    |State(captured): State<Arc<Mutex<Vec<dto::SubmitJobRequest>>>>,
                     Json(req): Json<dto::SubmitJobRequest>| async move {
                        let response = dto::SubmitJobResponse {
                            job: req.job,
                            log_index: 42,
                        };
                        captured.lock().unwrap().push(req);
                        Json(serde_json::to_value(response).unwrap())
                    },
                ),
            )
            .with_state(captured.clone());
        let base = spawn(router).await;

        let spec = write_spec(FULL_SPEC);
        let job = JobId::new();
        submit(&client(), &base, spec.path(), Some(job))
            .await
            .expect("submit succeeds");

        // The server received the real DTO, converted from the spec.
        let received = captured.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].job, job);
        assert_eq!(received[0].image, "busybox:1.36");
        assert_eq!(received[0].max_runtime_seconds, Some(3600));
        assert_eq!(received[0].requests.memory_bytes, 256 * 1024 * 1024);
    }

    #[tokio::test]
    async fn submit_surfaces_an_invalid_argument_body() {
        let router = Router::new().route(
            "/api/v1/jobs",
            post(|| async {
                (
                    AxumStatus::BAD_REQUEST,
                    Json(error_body(
                        "INVALID_ARGUMENT",
                        "priority 9 has no multiplier",
                    )),
                )
            }),
        );
        let base = spawn(router).await;
        let spec = write_spec(MINIMAL_SPEC);

        let err = submit(&client(), &base, spec.path(), None)
            .await
            .expect_err("submit fails");
        let message = format!("{err:#}");
        assert!(message.contains("INVALID_ARGUMENT"), "{message}");
        assert!(message.contains("no multiplier"), "{message}");
    }

    #[tokio::test]
    async fn abort_posts_and_succeeds() {
        let captured: Arc<Mutex<Vec<dto::AbortJobRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let router = Router::new()
            .route(
                "/api/v1/jobs/:job/abort",
                post(
                    |State(captured): State<Arc<Mutex<Vec<dto::AbortJobRequest>>>>,
                     Json(req): Json<dto::AbortJobRequest>| async move {
                        captured.lock().unwrap().push(req);
                        Json(serde_json::to_value(dto::AbortJobResponse {}).unwrap())
                    },
                ),
            )
            .with_state(captured.clone());
        let base = spawn(router).await;

        let job = JobId::new();
        abort(&client(), &base, job, Some("cleanup".to_string()))
            .await
            .expect("abort succeeds");

        let received = captured.lock().unwrap();
        assert_eq!(received.len(), 1);
        // The path is authoritative; the body carries no job, only the reason.
        assert!(received[0].job.is_none());
        assert_eq!(received[0].reason.as_deref(), Some("cleanup"));
    }

    #[tokio::test]
    async fn not_leader_surfaces_the_leader_hint() {
        let router = Router::new().route(
            "/api/v1/jobs/:job/abort",
            post(|| async {
                let mut headers = HeaderMap::new();
                headers.insert(COPPICE_LEADER, "10.0.0.3:7070".parse().unwrap());
                (
                    AxumStatus::MISDIRECTED_REQUEST,
                    headers,
                    Json(error_body("NOT_LEADER", "not the leader")),
                )
            }),
        );
        let base = spawn(router).await;

        let err = abort(&client(), &base, JobId::new(), None)
            .await
            .expect_err("not-leader fails");
        let message = format!("{err:#}");
        assert!(message.contains("NOT_LEADER"), "{message}");
        assert!(message.contains("10.0.0.3:7070"), "{message}");
    }

    /// A minimal but complete `JobDetail`, built from the real DTO types.
    fn sample_job_detail(job: JobId, state: dto::JobStateKind) -> dto::JobDetail {
        let ts = Timestamp::from_micros(1_000_000).unwrap();
        let requests = dto::Resources {
            cpu_millis: 500,
            memory_bytes: 256 * 1024 * 1024,
            disk_bytes: 1024 * 1024 * 1024,
        };
        dto::JobDetail {
            id: job,
            state,
            spec: dto::JobSpecView {
                image: "busybox:1.36".to_string(),
                command: vec!["sh".to_string(), "-c".to_string(), "echo hi".to_string()],
                entrypoint: None,
                requests,
                priority: 0,
                max_runtime_seconds: Some(3600),
                quota_entity: "quota-00000000-0000-0000-0000-000000000001"
                    .parse()
                    .unwrap(),
                retry: dto::RetryPolicy {
                    max_retries: 3,
                    retry_user_errors: false,
                },
            },
            submitted_at: ts,
            state_since: ts,
            terminal_at: is_terminal(state).then_some(ts),
            retries_used: 0,
            abort_requested: None,
            entity_chain: Vec::new(),
            attempts: Vec::new(),
            queue: None,
            accrual: None,
            cost: dto::CostReport {
                rate_ucu_per_second: 10,
                rate_breakdown: dto::RateBreakdown {
                    cpu: 10,
                    memory: 0,
                    disk: 0,
                },
                priority_multiplier: 1.0,
                unbounded_multiplier: 1.0,
                effective_rate_ucu_per_second: 10,
                charge_window_seconds: 3600,
                charge_window_is_default: false,
                estimated_ucu: 36000,
                charged_ucu: 1234,
                refund_fraction: 0.0,
                actual_ucu: None,
                true_up: None,
            },
        }
    }

    #[tokio::test]
    async fn status_renders_the_job_detail() {
        let job: JobId = "job-00000000-0000-0000-0000-000000000001".parse().unwrap();
        let detail = sample_job_detail(job, dto::JobStateKind::Attempting);
        let rendered = render_status(&detail);
        assert!(
            rendered.contains(&format!("id              {job}")),
            "{rendered}"
        );
        assert!(
            rendered.contains("state           attempting"),
            "{rendered}"
        );
        assert!(rendered.contains("cost (charged)  1234 uCU"), "{rendered}");
    }

    fn log_entry(attempt: AttemptId, at_us: i64, text: &str) -> dto::LogEntry {
        dto::LogEntry {
            attempt,
            at: Timestamp::from_micros(at_us).unwrap(),
            stream: dto::LogStreamName::Stdout,
            text: text.to_string(),
            truncated: false,
        }
    }

    fn available_source(attempt: AttemptId) -> dto::LogSourceRecord {
        dto::LogSourceRecord {
            attempt,
            node: Some("node-00000000-0000-0000-0000-000000000001".parse().unwrap()),
            availability: dto::LogAvailability::Available,
            truncated: false,
            earliest_available_at: None,
            reason: None,
        }
    }

    #[test]
    fn merged_source_truncation_is_sticky() {
        let attempt: AttemptId = "attempt-00000000-0000-0000-0000-000000000001"
            .parse()
            .unwrap();
        let mut truncated = available_source(attempt);
        truncated.truncated = true;
        let mut sources = Vec::new();
        merge_sources(&mut sources, vec![truncated]);
        // A later record for the same attempt reports no truncation (e.g. a
        // narrower follow re-poll) — the evidence of loss must survive it.
        merge_sources(&mut sources, vec![available_source(attempt)]);
        assert_eq!(sources.len(), 1);
        assert!(sources[0].truncated);
    }

    #[tokio::test]
    async fn logs_paginate_across_two_pages() {
        let attempt: AttemptId = "attempt-00000000-0000-0000-0000-000000000001"
            .parse()
            .unwrap();
        let page_one = dto::GetJobLogsResponse {
            entries: vec![log_entry(attempt, 1_000_000, "line one")],
            sources: vec![available_source(attempt)],
            next_cursor: Some("page2".to_string()),
        };
        let page_two = dto::GetJobLogsResponse {
            entries: vec![log_entry(attempt, 2_000_000, "line two")],
            sources: vec![available_source(attempt)],
            next_cursor: None,
        };
        type SeenCursors = Arc<Mutex<Vec<Option<String>>>>;
        let seen: SeenCursors = Arc::new(Mutex::new(Vec::new()));
        let pages = Arc::new((page_one, page_two, seen.clone()));
        let router = Router::new()
            .route(
                "/api/v1/jobs/:job/logs",
                get(
                    #[allow(clippy::type_complexity)]
                    |State(pages): State<
                        Arc<(dto::GetJobLogsResponse, dto::GetJobLogsResponse, SeenCursors)>,
                    >,
                     Query(params): Query<std::collections::HashMap<String, String>>| async move {
                        let cursor = params.get("cursor").cloned();
                        pages.2.lock().unwrap().push(cursor.clone());
                        let page = if cursor.as_deref() == Some("page2") {
                            &pages.1
                        } else {
                            &pages.0
                        };
                        Json(serde_json::to_value(page).unwrap())
                    },
                ),
            )
            // The walk fetches the job detail first (attempt multiplicity
            // decides prefixing up front).
            .route(
                "/api/v1/jobs/:job",
                get(|AxumPath(job): AxumPath<String>| async move {
                    let id: JobId = job.parse().unwrap();
                    Json(
                        serde_json::to_value(sample_job_detail(id, dto::JobStateKind::Succeeded))
                            .unwrap(),
                    )
                }),
            )
            .with_state(pages);
        let base = spawn(router).await;

        run_logs(
            &client(),
            &base,
            JobId::new(),
            None,
            None,
            dto::LogOrder::Asc,
        )
        .await
        .expect("logs walk completes");

        // The walk made exactly two requests: the first with no cursor, the
        // second resuming with the cursor page one returned.
        let seen = seen.lock().unwrap();
        assert_eq!(*seen, [None, Some("page2".to_string())]);
    }

    #[tokio::test]
    async fn follow_terminates_when_the_job_is_terminal() {
        let attempt: AttemptId = "attempt-00000000-0000-0000-0000-000000000001"
            .parse()
            .unwrap();
        let job = JobId::new();
        // One log page (drained to head immediately), and a job that already
        // reads terminal — so follow does its final drain and exits.
        let page = Arc::new(dto::GetJobLogsResponse {
            entries: vec![log_entry(attempt, 1_000_000, "only line")],
            sources: vec![available_source(attempt)],
            next_cursor: None,
        });
        let router = Router::new()
            .route(
                "/api/v1/jobs/:job/logs",
                get(
                    |State(page): State<Arc<dto::GetJobLogsResponse>>| async move {
                        Json(serde_json::to_value(&*page).unwrap())
                    },
                ),
            )
            .route(
                "/api/v1/jobs/:job",
                get(move |AxumPath(job): AxumPath<String>| async move {
                    let id: JobId = job.parse().unwrap();
                    Json(
                        serde_json::to_value(sample_job_detail(id, dto::JobStateKind::Succeeded))
                            .unwrap(),
                    )
                }),
            )
            .with_state(page);
        let base = spawn(router).await;

        // A tiny interval keeps the test fast; the job is already terminal, so
        // the loop should not actually sleep.
        tokio::time::timeout(
            Duration::from_secs(5),
            run_follow(&client(), &base, job, None, None, Duration::from_millis(5)),
        )
        .await
        .expect("follow terminates promptly")
        .expect("follow succeeds");
    }
}
