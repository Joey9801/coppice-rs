//! Ensure the embedded UI folder exists before rust-embed derives over it.
//!
//! `web/dist` is a gitignored npm build product, so a clean checkout must
//! still compile without Node: rust-embed fails the build on a missing
//! folder, so we create it (empty) here. An empty folder embeds nothing and
//! the UI routes answer with a "run npm build" hint instead
//! (`src/http/ui.rs`).

use std::path::PathBuf;

fn main() {
    let dist = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../web/dist");
    // Rebuild when a fresh `npm run build` lands, so release binaries
    // re-embed the new assets (debug builds read from disk at runtime and
    // don't need the rebuild, but the stanza is harmless there).
    println!("cargo:rerun-if-changed={}", dist.display());
    if let Err(e) = std::fs::create_dir_all(&dist) {
        println!("cargo:warning=could not create {}: {e}", dist.display());
    }
}
