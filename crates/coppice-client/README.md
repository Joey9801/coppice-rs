# coppice-client

An async, strongly-typed Rust client for [Coppice](https://github.com/Joey9801/coppice-rs),
a distributed batch job scheduler for containerized workloads.

Coppice runs jobs as Docker images across a fleet of compute nodes, with
resource requests, priorities and quotas, behind a Raft-replicated control
plane. Its coordinator serves a JSON HTTP API under `/api/v1`; this crate is
that API in Rust — typed ids, typed request and response bodies, a typed error
vocabulary, pagination, and a log follower.

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
