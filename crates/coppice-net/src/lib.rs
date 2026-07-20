//! # coppice-net
//!
//! tonic client/server stubs for every Coppice gRPC surface: the
//! coordinator↔coordinator Raft transport and membership admin surface
//! (`coppice.raft.v1`, ADR 0002/0016) and the agent↔coordinator session
//! (`coppice.agent.v1`, ADR 0009/0011).
//!
//! Message types are NOT generated here — they come from [`coppice_proto`]
//! (the single owner of the schema corpus, ADR 0003); this crate generates
//! only the service glue, with `extern_path` pointing every message at
//! `coppice_proto::pb`. That split is load-bearing: `coppice-proto` is a
//! prost-only dependency of the deterministic core (state machine, storage
//! formats), and the tonic/hyper transport stack enters the build graph only
//! through this crate, i.e. only for processes that actually open sockets.
//! Domain conversions stay with the endpoints (`coppice-consensus` for
//! openraft, the coordinator gateway and agent for the session), keeping
//! transport-library types out of this crate entirely.
//!
//! The generated code lives in [`pb`] (raw tonic module names, one module per
//! proto package); [`transport`], [`admin`], and [`session`] re-export the
//! client, server, and server-trait types under stable, readable aliases.

/// The raw tonic output, one module per proto package that defines a
/// service. Message types resolve to [`coppice_proto::pb`] via `extern_path`,
/// so nothing here redefines a schema type.
pub mod pb {
    /// `coppice.raft.v1`: the Raft transport and membership admin services.
    pub mod raft {
        include!(concat!(env!("OUT_DIR"), "/coppice.raft.v1.rs"));
    }
    /// `coppice.agent.v1`: the agent session and node services.
    pub mod agent {
        include!(concat!(env!("OUT_DIR"), "/coppice.agent.v1.rs"));
    }
}

/// The coordinator Raft transport service (`RaftTransportService`):
/// AppendEntries, Vote, and streaming InstallSnapshot (ADR 0002).
pub mod transport {
    pub use crate::pb::raft::raft_transport_service_client::RaftTransportServiceClient as Client;
    pub use crate::pb::raft::raft_transport_service_server::{
        RaftTransportService, RaftTransportServiceServer as Server,
    };
}

/// The membership admin service (`RaftAdminService`): add-learner,
/// promote-voter, remove-node, and cluster-status (ADR 0016).
pub mod admin {
    pub use crate::pb::raft::raft_admin_service_client::RaftAdminServiceClient as Client;
    pub use crate::pb::raft::raft_admin_service_server::{
        RaftAdminService, RaftAdminServiceServer as Server,
    };
}

/// The agent session service (`AgentService`): one long-lived bidirectional
/// stream per agent — reports up, commands down (ADR 0009).
pub mod session {
    pub use crate::pb::agent::agent_service_client::AgentServiceClient as Client;
    pub use crate::pb::agent::agent_service_server::{AgentService, AgentServiceServer as Server};
}

/// The agent-hosted node service (`NodeService`, ADR 0034): coordinators dial
/// *in* to fetch job logs (`FetchLogs`), the inverse direction of the agent's
/// outbound [`session`] stream. Read-only; any replica may serve as client.
pub mod node_service {
    pub use crate::pb::agent::node_service_client::NodeServiceClient as Client;
    pub use crate::pb::agent::node_service_server::{NodeService, NodeServiceServer as Server};
}
