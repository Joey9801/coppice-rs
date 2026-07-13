//! Generated protobuf types, one module per versioned package.
//!
//! The source of truth is the `.proto` corpus in `proto/` at the workspace
//! root; evolution rules live in `docs/architecture/schema-style.md`. These
//! are the *only* types for anything that crosses a process or hits disk —
//! domain types convert at the boundary (see [`crate::convert`]).

pub mod core {
    // pbjson's generated visitors trip a clippy lint newer than the
    // generator (`write!(.., &FIELDS)`); allow it on generated code only.
    #[allow(clippy::useless_borrows_in_formatting)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/coppice.core.v1.rs"));
        // proto3 JSON serde impls (pbjson): public-API packages only.
        include!(concat!(env!("OUT_DIR"), "/coppice.core.v1.serde.rs"));
    }
}

pub mod command {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/coppice.command.v1.rs"));
    }
}

pub mod raft {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/coppice.raft.v1.rs"));
    }
}

pub mod storage {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/coppice.storage.v1.rs"));
    }
}

pub mod agent {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/coppice.agent.v1.rs"));
    }
}

pub mod api {
    // See `core`: lint allowance for pbjson-generated code.
    #[allow(clippy::useless_borrows_in_formatting)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/coppice.api.v1.rs"));
        // proto3 JSON serde impls (pbjson): public-API packages only.
        include!(concat!(env!("OUT_DIR"), "/coppice.api.v1.serde.rs"));
    }
}
