# coppice-client

An async, strongly-typed Rust client for [Coppice](https://github.com/Joey9801/coppice-rs),
a distributed batch job scheduler for containerized workloads.

Coppice runs jobs as Docker images across a fleet of compute nodes, with
resource requests, priorities and quotas, behind a Raft-replicated control
plane. Its coordinator serves a JSON HTTP API under `/api/v1`; this crate is
that API in Rust — typed ids, typed request and response bodies, a typed error
vocabulary, pagination, a log follower, and a subscription to a set of jobs
that keeps itself connected.

It is a standalone crate: it depends on no other `coppice-*` crate, and carries
its own copy of every wire type. A contract test inside the server round-trips
every server value through this crate's copy and holds the JSON on both sides
to structural equality — every key, value and enum spelling, compared as
parsed JSON rather than as text.

## Install

```toml
[dependencies]
coppice-client = "0.0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Quick start

```rust,no_run
use coppice_client::{Client, FollowOptions, JobId, ReadOptions, Resources, SubmitJobRequest};

#[tokio::main]
async fn main() -> coppice_client::Result<()> {
    let client = Client::builder("http://127.0.0.1:7070")
        .token_opt(std::env::var("COPPICE_TOKEN").ok())
        .build()?;

    // Pick the quota entity to charge, then submit. The id is minted here and
    // is the submission's idempotency identity: resend it verbatim on a retry.
    let entity = client.list_quota_entities().await?.entities[0].id;
    let request = SubmitJobRequest::new(
        JobId::new(),
        "alpine:3",
        ["sh", "-c", "echo hello && sleep 5"],
        Resources::new(1_000, 512 * 1024 * 1024, 0),
        entity,
    );
    let submitted = client.submit_job(&request).await?;

    // Read your own write: pair the write's log index with the next read.
    let job = client
        .with_read_options(ReadOptions::at_least(submitted.log_index))
        .job(submitted.job)
        .await?;
    println!("{} is {}", job.id, job.state);

    // Follow its output until it finishes.
    let mut logs = client.follow_job_logs(submitted.job, FollowOptions::new());
    while let Some(page) = logs.next_page().await? {
        for entry in &page.entries {
            print!("{}", entry.text);
        }
    }
    Ok(())
}
```

## Watching a set of jobs

Polling every job a service owns is almost all redundant requests. Instead,
subscribe: `Client::watch_jobs` takes a filter naming an **open set** — every
job matching it, including ones submitted later — lists it a page at a time,
subscribes from the first page's applied index, and keeps one connection open.

```rust,no_run
use coppice_client::{Client, JobFilter, JobWatchItem, WatchOptions};

# async fn go(client: &Client) -> coppice_client::Result<()> {
let filter = JobFilter::metadata_equals("owner", "batch-service");
let mut watch = client.watch_jobs(filter, WatchOptions::new());
let mut live = std::collections::HashMap::new();

while let Some(item) = watch.next_item().await? {
    match item {
        // The live set, one page per item. `first` says start reconciling
        // afresh — the opening snapshot, or the one a gap forced — and
        // `last` says the run is complete.
        JobWatchItem::SnapshotPage { jobs, index, first, last } => {
            if first {
                live.clear();
            }
            for job in jobs {
                live.insert(job.id, job.state);
            }
            if last {
                println!("{} live jobs as of {index:?}", live.len());
            }
        }
        // One command's transitions, never split across items.
        JobWatchItem::Batch(batch) => {
            for event in &batch.events {
                println!("{}.{} {:?}", event.index, event.ordinal, event.body);
            }
        }
        // The enum is `#[non_exhaustive]`: a later release may add an item.
        _ => {}
    }
}
# Ok(()) }
```

What it promises, and what it does not:

- The snapshot is the **live set**, not the archive. The list asks for your
  filter AND a non-terminal phase, so its size follows the cluster's working
  set rather than how much history it keeps; `SnapshotScope::All` opts out and
  pays for it. A job you track that is **absent** from a snapshot has left the
  live set — it finished or was evicted — and finding out which is a read of
  that job (`ReadOptions::at_least(page_index)` if freshness matters). Same at
  startup: a process resuming from its own state polls the old jobs it is
  unsure about rather than waiting for a stream that will never mention them.
- A snapshot run is **not a consistent cut**: each page carries its own applied
  index, all at or above the first page's, and pages after the first are read
  pinned to that index. Nothing is lost, because the subscription resumes from
  the first page's index.
- **Per job it is clean.** The watcher remembers which page index carried each
  job's row and drops from each batch every event whose job was read at or
  above that batch's index; a batch left empty is not delivered. So after a row
  for job `J`, you see only events for `J` newer than that row. Ordinals are
  never renumbered.
- The subscription **reconnects itself** — a stream ends when the credential
  that opened it expires, the replica drains, or it goes silent for longer
  than `DEFAULT_STREAM_IDLE_TIMEOUT` (the server bookmarks progress, and thus
  produces a keepalive byte, at least every 15 s) — resuming from the last
  event id and dropping the batches the resume replays.
- A **gap**, meaning delivery was discontinuous, is never swallowed: it starts
  a fresh snapshot run here (read no older than the gap's
  `earliest_available`), and surfaces as `JobEventItem::Gap` on the
  lower-level `Client::watch_job_events`. A walk that takes longer than the
  replica's reconnection ring window can meet a gap the moment it subscribes
  and start over — one more reason the default snapshot is the live set.
- Events are **thin** — identity, stamp, kind, scope ids, no job snapshot. Read
  the job with `ReadOptions::at_least(event.index)` when you need more.
- The filter is **restricted** to the leaves that say which job this is
  (`metadata`, `entity`, `id`, `submitted_by`, under `all`/`any`/`not`); the
  rest are refused by name before a request is sent.

## Authentication

There is no login flow: a cluster that requires authentication expects an
out-of-band bearer token. With no credential at all, no `Authorization` header
is sent, which is what a cluster running in open mode (a local dev cluster,
say) needs. `Client::auth_config` reports which posture a deployment is in
without needing a credential.

There are two ways to supply one:

- **`ClientBuilder::token`** — one fixed token. The convenience for a CLI or
  any other short-lived tool, whose process does not outlive its credential.
- **`ClientBuilder::token_provider`** — a `TokenProvider` the client asks
  once per request, immediately before sending. This is what a long-running
  process needs, because its credential expires under it. The client caches
  nothing, so refresh, caching and single-flight locking belong to the
  provider.

Either way the token is held as a `BearerToken`, which redacts itself in
`Debug` output, and the header it builds is marked sensitive.

## Feature flags

- **`rustls-tls-native-roots`** *(default)* — enables `reqwest`'s `rustls-tls`
  and `rustls-tls-native-roots`, so both the Mozilla bundle and the platform
  trust store are trusted. Turn default features off for a plain-HTTP
  coordinator, or when supplying your own `reqwest::Client`.

## Minimum supported Rust version

1.86, checked in CI.

## Stability

Coppice is early-stage and this crate is `0.0.x`: anything here may change in
any release, and there are no compatibility shims. Responses are built to
tolerate a newer server — unknown fields are ignored, and a string-valued enum
keeps an unrecognized value verbatim — so an older client degrades rather than
failing. The one thing genuinely lost is the payload of a timeline event whose
kind this client does not know: `TimelineEventBody` is a tagged union, so an
unknown event decodes to a bare `Unknown`. Read those through
`Client::get_value`, which returns the server's own body.

## License

Apache-2.0.
