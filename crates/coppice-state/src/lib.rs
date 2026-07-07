//! # coppice-state
//!
//! The deterministic replicated state machine that sits behind Raft.
//!
//! This crate defines the authoritative control-plane state and the set of
//! commands that mutate it. Application of a command **must be deterministic**:
//! given the same sequence of committed commands, every replica must reach the
//! same state. That rules out wall-clock reads, randomness, network calls,
//! expensive scheduling computation, and iteration over unordered maps during
//! apply. See `docs/architecture/high-availability.md` and
//! `docs/architecture/state-model.md`.
//!
//! Commands commit *decisions, not computations*. A scheduling command says
//! "assign these jobs to these nodes under this expected state version", never
//! "run the scheduler now".

use std::collections::BTreeMap;

use coppice_core::id::{JobId, NodeId};
use coppice_core::job::Job;
use coppice_core::node::Node;
use serde::{Deserialize, Serialize};

pub mod command;

pub use command::Command;

/// The authoritative, replicated control-plane state.
///
/// Only durable semantic state required for correctness lives here. Derived
/// state (indexes, queue projections, UI aggregates) is rebuilt from this.
/// `BTreeMap` is used throughout to keep iteration deterministic.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StateMachine {
    pub jobs: BTreeMap<JobId, Job>,
    pub nodes: BTreeMap<NodeId, Node>,
    /// Version of the committed state, bumped on every applied command.
    pub version: u64,
}

/// Errors that can arise while validating a command against current state.
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error("job {0} not found")]
    UnknownJob(JobId),
    #[error("command references stale state version")]
    StaleVersion,
}

impl StateMachine {
    /// Deterministically apply a committed command.
    ///
    /// This is the only entry point that mutates authoritative state, and it is
    /// invoked on every replica from the Raft apply loop.
    pub fn apply(&mut self, command: Command) -> Result<(), ApplyError> {
        // TODO: implement per-command application. Skeleton only.
        let _ = command;
        self.version += 1;
        Ok(())
    }
}
