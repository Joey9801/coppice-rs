//! Test-only scaffolding shared by the client verbs' contract tests.
//!
//! Every verb module tests itself the same way: stand up an in-process axum
//! server serving real DTO JSON on the real route paths, point an
//! [`ApiClient`](crate::client::ApiClient) at it, run the verb, and assert on
//! both what the verb sent and what it did with the answer. The two pieces of
//! that setup with no per-module content — spawning the server and building a
//! wire error body — live here so the modules do not each grow their own.

use axum::Router;

/// Spawn `router` on an ephemeral loopback port and return its base URL.
pub async fn spawn(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}

/// Build the ADR 0031 wire error body. No shared Rust type exists for it — the
/// API's own `ErrorBody` is private and serialize-only — so tests construct the
/// two fields directly, exactly as the server renders them.
pub fn error_body(code: &str, message: &str) -> serde_json::Value {
    serde_json::json!({ "code": code, "message": message })
}

/// A `Coppice-Leader` hint header, for the 421/NOT_LEADER case every write verb
/// must surface.
pub fn leader_hint(addr: &str) -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(coppice_api::http::COPPICE_LEADER, addr.parse().unwrap());
    headers
}
