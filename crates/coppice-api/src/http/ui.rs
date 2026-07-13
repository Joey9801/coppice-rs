//! Serving the embedded web UI (ADR 0031, "Serving the UI").
//!
//! The same client listener that hosts `/api/v1` serves `web/dist`:
//! static assets at `/`, SPA fallback to `index.html` for client routes —
//! same-origin with the API, no CORS. `web/dist` is a gitignored npm
//! build product: release builds embed whatever was built when the binary
//! compiled; debug builds read the folder from disk at request time (the
//! rust-embed default), so `coppice dev` picks up a fresh
//! `npm --prefix web run build` without recompiling. When no build
//! exists, UI paths answer 404 with the command to run.

use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

use super::error::HttpError;

// `COPPICE_WEB_DIST` is set by build.rs: `web/dist` when a build exists,
// an empty `OUT_DIR` placeholder otherwise — never a write into the source
// tree, which may be read-only.
#[derive(RustEmbed)]
#[folder = "$COPPICE_WEB_DIST"]
struct Assets;

/// Whether a UI build is present (embedded or, in debug, on disk) — lets
/// `coppice dev` print an honest ready banner.
pub fn ui_available() -> bool {
    Assets::get("index.html").is_some()
}

/// Router fallback: everything no explicit route claimed.
///
/// `/api/*` misses keep the JSON error contract; anything else is the UI —
/// an exact asset when one matches, otherwise the SPA shell (`index.html`)
/// so client-side routes like `/jobs/<id>` deep-link correctly.
pub(super) async fn fallback(uri: Uri) -> Response {
    let path = uri.path();
    if path.starts_with("/api/") {
        return HttpError::not_found("no such route").into_response();
    }
    let trimmed = path.trim_start_matches('/');
    if !trimmed.is_empty() {
        if let Some(file) = Assets::get(trimmed) {
            return asset_response(trimmed, file);
        }
    }
    match Assets::get("index.html") {
        Some(file) => asset_response("index.html", file),
        None => (
            StatusCode::NOT_FOUND,
            "web UI not built: run `npm --prefix web run build`, then reload \
             (debug builds serve it immediately; release builds embed it at compile time)",
        )
            .into_response(),
    }
}

fn asset_response(path: &str, file: rust_embed::EmbeddedFile) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    // Vite emits content-hashed filenames under assets/ — cache those
    // forever; the mutable entry points (index.html, favicons) revalidate.
    let cache = if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    (
        [(CONTENT_TYPE, mime.as_ref()), (CACHE_CONTROL, cache)],
        file.data,
    )
        .into_response()
}
