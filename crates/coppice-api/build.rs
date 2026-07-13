//! Resolve the folder the embedded web UI is built from.
//!
//! `web/dist` is a gitignored npm build product, so a clean checkout must
//! still compile without Node — and possibly from a **read-only** source
//! tree (packaging, reproducible builds), so this script never writes into
//! the checkout. When `web/dist` exists it is embedded (and, in debug
//! builds, read from disk at request time); when it doesn't, rust-embed is
//! pointed at an empty directory under `OUT_DIR` and the UI routes answer
//! with a "run npm build" hint instead (`src/http/ui.rs`). The indirection
//! is the `COPPICE_WEB_DIST` env var interpolated by the rust-embed derive.

use std::path::PathBuf;

fn main() {
    let dist = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../web/dist");
    // Rebuild when a build lands or changes, so a binary re-points/re-embeds
    // fresh assets. Watching `index.html` (a file, not just the directory)
    // is what makes the appear/disappear transitions reliable: vite rewrites
    // it with a fresh mtime on every build, and while it's absent cargo
    // treats the missing watched path as always-dirty, so the script keeps
    // re-running until a build shows up and then re-points at it. The
    // directory watch alone can miss the transition (e.g. a restored dist
    // with preserved old mtimes).
    println!("cargo:rerun-if-changed={}", dist.display());
    println!(
        "cargo:rerun-if-changed={}",
        dist.join("index.html").display()
    );

    let folder = if dist.is_dir() {
        dist
    } else {
        let empty =
            PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("empty-web-dist");
        std::fs::create_dir_all(&empty).expect("creating the empty web-dist placeholder");
        empty
    };
    println!("cargo:rustc-env=COPPICE_WEB_DIST={}", folder.display());
}
