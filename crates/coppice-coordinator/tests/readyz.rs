//! `/readyz` over real HTTP (ADR 0037 §7), driven through the same
//! `bootstrap::serve_runtime` path the daemon uses. The [`RunningCoordinator`]
//! harness binds the client API listener on an ephemeral port and serves the
//! `coppice_api::http` router with the coordinator's `ReadyzState` attached, so
//! these tests exercise the whole surface — router wiring, the gate, and the
//! JSON body — not just the pure `evaluate` matrix (which is unit-tested in
//! `coppice_coordinator::readyz`).
//!
//! - a **parked** replica answers 503 with phase `waiting`;
//! - a **formed** single-node replica answers 200 with phase `voter`, and its
//!   `?require=formed` flips to 503 because one voter is short of the default
//!   `cluster_size = 3` (membership cardinality, ADR 0037 §7);
//! - `?require=healthy` on that under-redundant leader is 503 **without**
//!   `health_unknown` — the leader knows its own health, it is simply not
//!   redundant;
//! - an unknown `?require=` value is 400.

mod common;

use std::time::{Duration, Instant};

use coppice_core::id::ClusterId;
use serde_json::Value;

use common::{Ca, RunningCoordinator};

const DEADLINE: Duration = Duration::from_secs(20);

/// GET `<base><path>` and return the status code plus the parsed JSON body.
async fn get_readyz(base: &str, path: &str) -> (reqwest::StatusCode, Value) {
    let resp = reqwest::Client::new()
        .get(format!("{base}{path}"))
        .send()
        .await
        .expect("GET /readyz");
    let status = resp.status();
    let body: Value = resp.json().await.expect("/readyz body is JSON");
    (status, body)
}

/// A parked replica (booted, never formed) reports 503 + phase `waiting`: alive
/// and serving, but deliberately not ready (ADR 0037 §1/§7).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_replica_is_503_waiting() {
    let ca = Ca::new();
    let coord = RunningCoordinator::start_parked(ClusterId::new(), &ca).await;
    let base = coord.client_endpoint.clone();

    // Give the API listener a moment to accept, then assert the parked answer.
    let (status, body) = poll_readyz(&base, "/readyz", |status, body| {
        status == reqwest::StatusCode::SERVICE_UNAVAILABLE && body["phase"] == "waiting"
    })
    .await;

    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["phase"], "waiting");
    assert_eq!(body["is_leader"], false);
    // The body is always present, carrying the identities and the counts.
    assert_eq!(body["cluster_size"], 3);
    assert_eq!(body["formed"], false);
    assert!(body["instance_uuid"].is_string());

    coord.shutdown().await;
}

/// A formed single-node replica is node-ready (200, phase `voter`), but its
/// membership is a single voter of the default `cluster_size = 3`, so it is not
/// `formed` and not `healthy`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn formed_single_node_is_ready_but_not_formed_or_healthy() {
    let ca = Ca::new();
    let coord = RunningCoordinator::start(ClusterId::new(), &ca).await;
    let base = coord.client_endpoint.clone();

    // Wait for the single-node cluster to elect itself leader and for the
    // convergence loop to publish the settled `voter` phase; plain `/readyz` is
    // then 200 (node-ready) with the voter phase.
    let (status, body) = poll_readyz(&base, "/readyz", |status, body| {
        status == reqwest::StatusCode::OK && body["phase"] == "voter"
    })
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["phase"], "voter");
    assert_eq!(body["is_leader"], true);
    assert_eq!(body["voters"], 1);
    assert_eq!(body["formed"], false);
    // A leader reports its own `voters_live` (itself): not null, but short of
    // the cluster size.
    assert_eq!(body["voters_live"], 1);
    // The leader's own lag is reported as null.
    assert!(body["replication_lag"].is_null());

    // `?require=formed`: membership cardinality is not met (1 < 3) → 503.
    let (status, body) = get_readyz(&base, "/readyz?require=formed").await;
    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["formed"], false);

    // `?require=healthy`: the leader knows its health (voters_live = 1 < 3), so
    // this is a plain 503 — NOT `health_unknown`.
    let (status, body) = get_readyz(&base, "/readyz?require=healthy").await;
    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body.get("reason").is_none() || body["reason"].is_null(),
        "leader health is known, not health_unknown: {body}"
    );

    // An unknown `?require=` value is a 400.
    let status = reqwest::Client::new()
        .get(format!("{base}/readyz?require=bogus"))
        .send()
        .await
        .expect("GET /readyz?require=bogus")
        .status();
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);

    coord.shutdown().await;
}

/// Poll `/readyz` until `pred` holds or the deadline elapses, returning the
/// final (status, body). Readiness is reached asynchronously (election,
/// convergence), so the tests wait rather than sleep a fixed amount.
async fn poll_readyz(
    base: &str,
    path: &str,
    pred: impl Fn(reqwest::StatusCode, &Value) -> bool,
) -> (reqwest::StatusCode, Value) {
    let start = Instant::now();
    loop {
        let (status, body) = get_readyz(base, path).await;
        if pred(status, &body) {
            return (status, body);
        }
        if start.elapsed() >= DEADLINE {
            panic!("timed out waiting for /readyz{path}; last: {status} {body}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
