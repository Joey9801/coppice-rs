//! Cursors, and the pagers that walk them.
//!
//! Four `/api/v1` endpoints paginate — the job list, a job's timeline, its
//! logs, and its usage samples — and all four do it the same way: a response
//! carries `next_cursor`, and the caller sends that token back to continue.
//!
//! **The contract that trips people up**: a short page with a non-null
//! `next_cursor` means *continue*, never *done*. The server ends a page early
//! for reasons that have nothing to do with the caller's `limit` — an RPC
//! budget, a byte cap, a source cap — so "fewer rows than I asked for" is not
//! an end condition. The only end condition is `next_cursor == null`.
//!
//! The cursor types here are opaque newtypes over the server's token. They
//! deliberately do **not** parse it: the format is the server's business and
//! versioned (`v1:…`) so it can change. What they do give you is a
//! `FromStr`/`Display` round trip, so a CLI can accept one on a `--cursor`
//! flag and hand it straight back.
//!
//! The pagers below are the "just give me everything" path. Each owns a
//! [`Client`] clone and its request parameters, and each `next_page` call
//! sends one request:
//!
//! ```no_run
//! # async fn walk(client: &coppice_client::Client, job: coppice_client::JobId)
//! #     -> coppice_client::Result<()> {
//! let mut pages = client.job_timeline_paged(job, Default::default());
//! while let Some(page) = pages.next_page().await? {
//!     for event in &page.events {
//!         println!("{} {}", event.index, event.at);
//!     }
//! }
//! # Ok(()) }
//! ```

use serde::{Deserialize, Serialize};

use crate::client::Client;
use crate::error::Result;
use crate::id::JobId;
use crate::types::{
    GetJobLogsResponse, GetJobTimelineResponse, GetJobUsageResponse, ListJobsParams,
    ListJobsResponse, LogsParams, TimelineParams, UsageParams,
};

/// Define an opaque pagination cursor: a newtype over the server's token.
macro_rules! cursor {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// The token as the server spelled it.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// The token, consumed.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(token: String) -> $name {
                $name(token)
            }
        }

        impl From<&str> for $name {
            fn from(token: &str) -> $name {
                $name(token.to_string())
            }
        }

        impl std::str::FromStr for $name {
            /// Never fails: the token is opaque, so there is nothing here to
            /// reject. The server validates it.
            type Err = std::convert::Infallible;

            fn from_str(token: &str) -> std::result::Result<$name, Self::Err> {
                Ok($name(token.to_string()))
            }
        }
    };
}

cursor!(
    /// Continues a `GET /api/v1/jobs` scan.
    JobCursor
);
cursor!(
    /// Continues a `GET /api/v1/jobs/{job}/timeline` scan.
    TimelineCursor
);
cursor!(
    /// Continues a `GET /api/v1/jobs/{job}/logs` walk.
    ///
    /// Logs have a second token as well: a page's `resume_cursor` is an
    /// ascending high-water mark that stays valid once the walk has caught up
    /// with the live head, which is what makes following possible. See
    /// [`LogFollower`](crate::LogFollower).
    LogCursor
);
cursor!(
    /// Continues a `GET /api/v1/jobs/{job}/usage` walk.
    UsageCursor
);

/// Walks `GET /api/v1/jobs`, one page per [`next_page`](Self::next_page).
#[derive(Debug, Clone)]
pub struct JobPager {
    client: Client,
    params: ListJobsParams,
    done: bool,
}

impl JobPager {
    pub(crate) fn new(client: Client, params: ListJobsParams) -> JobPager {
        JobPager {
            client,
            params,
            done: false,
        }
    }

    /// The next page, or `None` once the scan is complete.
    pub async fn next_page(&mut self) -> Result<Option<ListJobsResponse>> {
        if self.done {
            return Ok(None);
        }
        let page = self.client.list_jobs(&self.params).await?.into_inner();
        match &page.next_cursor {
            Some(cursor) => self.params.cursor = Some(cursor.clone()),
            None => self.done = true,
        }
        Ok(Some(page))
    }
}

/// Walks `GET /api/v1/jobs/{job}/timeline`.
#[derive(Debug, Clone)]
pub struct TimelinePager {
    client: Client,
    job: JobId,
    params: TimelineParams,
    done: bool,
}

impl TimelinePager {
    pub(crate) fn new(client: Client, job: JobId, params: TimelineParams) -> TimelinePager {
        TimelinePager {
            client,
            job,
            params,
            done: false,
        }
    }

    /// The next page, or `None` once the scan is complete.
    pub async fn next_page(&mut self) -> Result<Option<GetJobTimelineResponse>> {
        if self.done {
            return Ok(None);
        }
        let page = self
            .client
            .job_timeline(self.job, &self.params)
            .await?
            .into_inner();
        match &page.next_cursor {
            Some(cursor) => self.params.cursor = Some(cursor.clone()),
            None => self.done = true,
        }
        Ok(Some(page))
    }
}

/// Walks `GET /api/v1/jobs/{job}/logs` to the head and stops.
///
/// For a walk that keeps going as new output arrives, use
/// [`LogFollower`](crate::LogFollower) instead.
#[derive(Debug, Clone)]
pub struct LogPager {
    client: Client,
    job: JobId,
    params: LogsParams,
    done: bool,
}

impl LogPager {
    pub(crate) fn new(client: Client, job: JobId, params: LogsParams) -> LogPager {
        LogPager {
            client,
            job,
            params,
            done: false,
        }
    }

    /// The next page, or `None` once the walk has reached the head.
    pub async fn next_page(&mut self) -> Result<Option<GetJobLogsResponse>> {
        if self.done {
            return Ok(None);
        }
        let page = self
            .client
            .job_logs(self.job, &self.params)
            .await?
            .into_inner();
        match &page.next_cursor {
            Some(cursor) => self.params.cursor = Some(cursor.clone()),
            None => self.done = true,
        }
        Ok(Some(page))
    }
}

/// Walks `GET /api/v1/jobs/{job}/usage`.
#[derive(Debug, Clone)]
pub struct UsagePager {
    client: Client,
    job: JobId,
    params: UsageParams,
    done: bool,
}

impl UsagePager {
    pub(crate) fn new(client: Client, job: JobId, params: UsageParams) -> UsagePager {
        UsagePager {
            client,
            job,
            params,
            done: false,
        }
    }

    /// The next page, or `None` once the walk is complete.
    pub async fn next_page(&mut self) -> Result<Option<GetJobUsageResponse>> {
        if self.done {
            return Ok(None);
        }
        let page = self
            .client
            .job_usage(self.job, &self.params)
            .await?
            .into_inner();
        match &page.next_cursor {
            Some(cursor) => self.params.cursor = Some(cursor.clone()),
            None => self.done = true,
        }
        Ok(Some(page))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_round_trips_through_a_flag() {
        let token = "v1:job-00000000-0000-0000-0000-000000000007";
        let cursor: JobCursor = token.parse().unwrap();
        assert_eq!(cursor.to_string(), token);
        assert_eq!(cursor.as_str(), token);
        assert_eq!(
            serde_json::to_value(&cursor).unwrap(),
            serde_json::json!(token)
        );
    }
}
