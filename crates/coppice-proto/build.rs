//! Compiles the canonical schema corpus in `proto/` (ADR 0003).
//!
//! Uses `protox` (a pure-Rust protobuf compiler) so the build needs no
//! system `protoc`, then hands the descriptor set to `prost-build`. The
//! descriptor set is also written to `OUT_DIR` — the breaking-change gate
//! (`tests/breaking.rs`) diffs it against the committed baseline in
//! `proto/baseline.binpb`.

use std::error::Error;
use std::fs;
use std::path::PathBuf;

use prost::Message;

fn main() -> Result<(), Box<dyn Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let proto_root = proto_root.canonicalize()?;
    println!("cargo:rerun-if-changed={}", proto_root.display());

    let mut files = Vec::new();
    collect_protos(&proto_root, &mut files)?;
    files.sort();

    let descriptors = protox::compile(&files, [&proto_root])?;

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor_bytes = descriptors.encode_to_vec();
    fs::write(out_dir.join("descriptor.binpb"), &descriptor_bytes)?;

    prost_build::Config::new().compile_fds(descriptors)?;

    // No JSON (serde) codegen: the HTTP edge serializes handwritten DTOs
    // (`coppice-api::http::dto`, ADR 0031 as amended), not these types, so
    // the prost structs need only their binary encoding. serde must never
    // become a parallel encoding of a replicated format (schema-style.md).
    Ok(())
}

fn collect_protos(dir: &PathBuf, files: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_protos(&path, files)?;
        } else if path.extension().is_some_and(|e| e == "proto") {
            files.push(path);
        }
    }
    Ok(())
}
