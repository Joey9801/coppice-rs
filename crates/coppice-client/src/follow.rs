//! Following a job's logs as it runs.
//!
//! The logs endpoint is a paginated read, not a stream, so "follow" is a
//! polling loop — and the loop has two subtleties worth having in a library
//! rather than re-derived per caller.
//!
//! **Two cursors, not one.** A page carries `next_cursor` while there is more
//! history to walk *now*, and `resume_cursor` — an ascending high-water mark —
//! which stays valid once the walk has caught up with the live head. So the
//! follower walks on `next_cursor` until a page has none, then holds
//! `resume_cursor` and polls from there. A page with neither token (a job with
//! no attempt yet) leaves the position alone; the follower never rewinds.
//!
//! **The job's state, not the page, says when to stop.** A page's `live` flag
//! is best-effort, so the follower asks the job whether it is finished, and
//! when it is, drains once more before stopping — that last drain is what
//! catches output written between the final page and the job going terminal.
//!
//! ```no_run
//! # async fn tail(client: &coppice_client::Client, job: coppice_client::JobId)
//! #     -> coppice_client::Result<()> {
//! use coppice_client::FollowOptions;
//!
//! let mut follower = client.follow_job_logs(job, FollowOptions::new());
//! while let Some(page) = follower.next_page().await? {
//!     for entry in &page.entries {
//!         println!("[{}] {}", entry.stream, entry.text);
//!     }
//! }
//! // Anything expired, unreachable or truncated is reported per attempt.
//! for source in follower.sources() {
//!     if source.availability != coppice_client::LogAvailability::Available {
//!         eprintln!("{}: {}", source.attempt, source.availability);
//!     }
//! }
//! # Ok(()) }
//! ```

use std::time::Duration;

use crate::client::Client;
use crate::error::Result;
use crate::id::{AttemptId, JobId};
use crate::pagination::LogCursor;
use crate::types::{GetJobLogsResponse, LogOrder, LogSourceRecord, LogStreamName, LogsParams};

/// How often a follower polls once it has caught up with the live head.
///
/// One second: fast enough that a tail feels live, slow enough that following
/// a long job is not a load problem for the coordinator.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How a [`LogFollower`] should behave.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FollowOptions {
    /// How long to wait between polls once caught up.
    pub poll_interval: Duration,
    /// Follow only one of the two streams; `None` follows both.
    pub stream: Option<LogStreamName>,
    /// Follow only one attempt; `None` follows every attempt the job makes,
    /// including retries that start after following began.
    pub attempt: Option<AttemptId>,
    /// Page size to ask for. `None` takes the server's default.
    pub limit: Option<u32>,
    /// Where to resume from — a `resume_cursor` kept from an earlier follow.
    /// `None` starts at the beginning of the job's output.
    pub cursor: Option<LogCursor>,
}

impl Default for FollowOptions {
    fn default() -> FollowOptions {
        FollowOptions {
            poll_interval: DEFAULT_POLL_INTERVAL,
            stream: None,
            attempt: None,
            limit: None,
            cursor: None,
        }
    }
}

impl FollowOptions {
    /// The defaults: poll every second, both streams, every attempt, from the
    /// beginning.
    pub fn new() -> FollowOptions {
        FollowOptions::default()
    }

    /// Poll at this interval once caught up.
    pub fn with_poll_interval(mut self, interval: Duration) -> FollowOptions {
        self.poll_interval = interval;
        self
    }

    /// Follow only one stream.
    pub fn with_stream(mut self, stream: LogStreamName) -> FollowOptions {
        self.stream = Some(stream);
        self
    }

    /// Follow only one attempt.
    pub fn with_attempt(mut self, attempt: AttemptId) -> FollowOptions {
        self.attempt = Some(attempt);
        self
    }

    /// Ask for this many entries per page.
    pub fn with_limit(mut self, limit: u32) -> FollowOptions {
        self.limit = Some(limit);
        self
    }

    /// Resume from a cursor kept from an earlier follow.
    pub fn with_cursor(mut self, cursor: LogCursor) -> FollowOptions {
        self.cursor = Some(cursor);
        self
    }
}

/// Walks a job's logs and keeps walking until the job is finished.
///
/// Built by [`Client::follow_job_logs`](crate::Client::follow_job_logs). Each
/// [`next_page`](Self::next_page) makes exactly one logs request and returns
/// its page, so a caller stays in control: drop the follower to stop, or
/// wrap the call in a `tokio::select!` to cancel it.
///
/// Pages can be empty. While the job is running and quiet, the follower keeps
/// returning empty pages at its poll interval rather than blocking
/// indefinitely, which is what lets a caller interleave other work.
#[derive(Debug, Clone)]
pub struct LogFollower {
    client: Client,
    job: JobId,
    options: FollowOptions,
    cursor: Option<LogCursor>,
    sources: Vec<LogSourceRecord>,
    /// Set once the job's own state says it is finished. One more drain runs
    /// after that, which is what catches the last lines.
    job_finished: bool,
    /// Set when the walk is over and `next_page` should answer `None`.
    finished: bool,
    /// Set when the last page reached the head and the job was still running,
    /// so the next poll waits first.
    waiting: bool,
}

impl LogFollower {
    pub(crate) fn new(client: Client, job: JobId, options: FollowOptions) -> LogFollower {
        LogFollower {
            cursor: options.cursor.clone(),
            client,
            job,
            options,
            sources: Vec::new(),
            job_finished: false,
            finished: false,
            waiting: false,
        }
    }

    /// The next page, or `None` once the job is finished and its output fully
    /// drained.
    pub async fn next_page(&mut self) -> Result<Option<GetJobLogsResponse>> {
        if self.finished {
            return Ok(None);
        }
        if self.waiting {
            tokio::time::sleep(self.options.poll_interval).await;
            self.waiting = false;
        }

        let page = self
            .client
            .job_logs(self.job, &self.params())
            .await?
            .into_inner();
        merge_sources(&mut self.sources, &page.sources);

        // Never rewind: a page carrying neither token keeps the position we
        // already hold.
        if let Some(next) = page
            .next_cursor
            .clone()
            .or_else(|| page.resume_cursor.clone())
        {
            self.cursor = Some(next);
        }

        if page.next_cursor.is_none() {
            // The walk has caught up. Either we are done, or we wait.
            if self.job_finished {
                self.finished = true;
            } else if self.client.job(self.job).await?.state.is_terminal() {
                // One more drain before stopping.
                self.job_finished = true;
            } else {
                self.waiting = true;
            }
        }

        Ok(Some(page))
    }

    /// The per-attempt availability records seen so far, merged across every
    /// page: one entry per attempt, latest verdict wins, and `truncated` is
    /// sticky once set.
    ///
    /// Worth reading when the follow ends — an `expired` or `unreachable`
    /// attempt means output is missing, and nothing else in the stream says
    /// so.
    pub fn sources(&self) -> &[LogSourceRecord] {
        &self.sources
    }

    /// The position the next poll will resume from. Keep it to resume a follow
    /// in a later process.
    pub fn cursor(&self) -> Option<&LogCursor> {
        self.cursor.as_ref()
    }

    /// Whether the follow is over.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// The job being followed.
    pub fn job(&self) -> JobId {
        self.job
    }

    /// The request for the next poll. Ascending always: following means
    /// oldest-first, and the resume cursor's own order segment has to agree
    /// with the `order=` on every request or the server refuses it.
    fn params(&self) -> LogsParams {
        let mut params = LogsParams::new().with_order(LogOrder::Asc);
        if let Some(cursor) = &self.cursor {
            params = params.with_cursor(cursor.clone());
        }
        if let Some(stream) = self.options.stream.clone() {
            params = params.with_stream(stream);
        }
        if let Some(attempt) = self.options.attempt {
            params = params.with_attempt(attempt);
        }
        if let Some(limit) = self.options.limit {
            params = params.with_limit(limit);
        }
        params
    }
}

/// Merge a page's source records into the running set: one record per
/// attempt, the later verdict wins, and `truncated` never un-sets — once
/// output has been pruned, saying so remains true for the whole follow.
fn merge_sources(into: &mut Vec<LogSourceRecord>, page: &[LogSourceRecord]) {
    for record in page {
        match into.iter_mut().find(|held| held.attempt == record.attempt) {
            Some(held) => {
                let truncated = held.truncated || record.truncated;
                *held = record.clone();
                held.truncated = truncated;
            }
            None => into.push(record.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogAvailability;

    fn record(
        attempt: AttemptId,
        truncated: bool,
        availability: LogAvailability,
    ) -> LogSourceRecord {
        LogSourceRecord {
            attempt,
            node: None,
            availability,
            truncated,
            earliest_available_at: None,
            reason: None,
        }
    }

    #[test]
    fn merging_keeps_one_record_per_attempt_and_latches_truncation() {
        let a = AttemptId::new();
        let b = AttemptId::new();
        let mut held = Vec::new();
        merge_sources(&mut held, &[record(a, true, LogAvailability::Available)]);
        merge_sources(
            &mut held,
            &[
                record(a, false, LogAvailability::Expired),
                record(b, false, LogAvailability::Available),
            ],
        );
        assert_eq!(held.len(), 2);
        // Latest verdict wins…
        assert_eq!(held[0].availability, LogAvailability::Expired);
        // …but truncation, once true, stays true.
        assert!(held[0].truncated);
        assert!(!held[1].truncated);
    }

    #[test]
    fn options_default_to_a_one_second_poll() {
        let options = FollowOptions::new();
        assert_eq!(options.poll_interval, DEFAULT_POLL_INTERVAL);
        assert_eq!(options.stream, None);
        assert_eq!(options.attempt, None);
    }
}
