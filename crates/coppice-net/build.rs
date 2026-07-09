//! Generates the tonic service glue for every Coppice gRPC surface: the
//! coordinator Raft transport and membership admin (`coppice.raft.v1`,
//! ADR 0002/0016) and the agent↔coordinator session (`coppice.agent.v1`,
//! ADR 0009/0011).
//!
//! Compiles the whole `proto/` corpus with `protox` (pure-Rust, no system
//! `protoc`), exactly like `crates/coppice-proto/build.rs`, then hands the
//! descriptor set to `tonic-build`. Every message package is `extern_path`ed
//! to `coppice_proto::pb`, so prost regenerates none of the message types —
//! the only output is the client/server code for the services, which
//! references the message types owned by `coppice-proto`.

use std::error::Error;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let proto_root = proto_root.canonicalize()?;
    println!("cargo:rerun-if-changed={}", proto_root.display());

    let mut files = Vec::new();
    collect_protos(&proto_root, &mut files)?;
    files.sort();

    let descriptors = protox::compile(&files, [&proto_root])?;

    // Point every message package at coppice-proto's generated types so no
    // message struct is regenerated here — only the service glue is emitted.
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .extern_path(".coppice.core.v1", "::coppice_proto::pb::core::v1")
        .extern_path(".coppice.command.v1", "::coppice_proto::pb::command::v1")
        .extern_path(".coppice.raft.v1", "::coppice_proto::pb::raft::v1")
        .extern_path(".coppice.storage.v1", "::coppice_proto::pb::storage::v1")
        .extern_path(".coppice.agent.v1", "::coppice_proto::pb::agent::v1")
        .extern_path(".coppice.api.v1", "::coppice_proto::pb::api::v1")
        .compile_fds(descriptors)?;

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
