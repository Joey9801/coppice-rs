//! The coordinator↔coordinator Raft transport (ADR 0002/0011/0016/0018).
//!
//! openraft owns replication, elections, and snapshot installs; this module is
//! the network seam it drives them through. The [`client`] side implements
//! openraft's [`RaftNetworkFactory`](openraft::network::RaftNetworkFactory) and
//! [`RaftNetwork`](openraft::network::RaftNetwork) over the generated tonic gRPC
//! stubs (`coppice-net`); the [`server`] side implements the generated
//! service trait over the local [`Raft`](openraft::Raft) handle.
//!
//! Two invariants run through both sides:
//!
//! - **Our own representations, on the wire and on disk (ADR 0018).** Every
//!   Raft value is converted with the same [`storage::raftpb`](crate::storage::raftpb)
//!   converters the durable log uses ([`convert`] reuses them), so openraft's
//!   serde forms never travel and the wire/disk encodings cannot drift.
//! - **Cluster identity on every request (ADR 0016).** The client stamps the
//!   16-byte cluster UUID; the server refuses a mismatch before touching Raft
//!   state. The mTLS channel underneath is mutually authenticated (ADR 0011).
//!
//! Only the two endpoints and the wire chunk size are exported; the
//! conversions are an implementation detail.

mod client;
mod convert;
mod server;

pub use client::GrpcNetworkFactory;
pub use server::RaftTransportHandler;
