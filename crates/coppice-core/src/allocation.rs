//! Allocations: an attempt's claim on one node's resources.
//!
//! An allocation may be committed *before* the node has space
//! ([`AllocationState::Accruing`]) and accumulate capacity as it frees —
//! this is Coppice's reservation mechanism; there is no standalone
//! reservation object. Funding is deterministic bookkeeping in the apply
//! loop: freed node capacity is pledged to that node's accruing allocations
//! in commit order. Decided in
//! `docs/decisions/0014-accruing-allocations-replace-reservations.md`.

use serde::{Deserialize, Serialize};

use crate::id::{AllocationId, AttemptId, JobId, NodeId};
use crate::resource::Resources;

/// A claim on one node's resources. Authoritative, Raft-replicated state —
/// including `funded`, which must survive failover because accrual progress
/// is exactly what a reservation is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Allocation {
    pub id: AllocationId,
    pub job: JobId,
    pub attempt: AttemptId,
    pub node: NodeId,
    /// The full request this allocation must reach before it is funded.
    pub requested: Resources,
    /// How much has been pledged so far. Equals `requested` once funded.
    pub funded: Resources,
    pub state: AllocationState,
}

/// The allocation state machine: `Accruing → Funded → Active → Released`,
/// with early release from `Accruing` (revocation, abort) and `Funded`
/// (abort). Revocation is legal only while accruing; funded allocations are
/// stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AllocationState {
    /// Committed but not fully funded; holds capacity as it frees. Revocable
    /// by scheduler command at no cost to the job.
    Accruing,
    /// Fully funded and stable; the attempt's `Ready` barrier watches this.
    Funded,
    /// The container is consuming the resources.
    Active,
    /// Terminal: capacity returned to the node and pledged onward to any
    /// accruing allocations there, in commit order.
    Released,
}

impl AllocationState {
    pub fn is_terminal(self) -> bool {
        matches!(self, AllocationState::Released)
    }

    pub fn may_transition_to(self, next: AllocationState) -> bool {
        use AllocationState::*;
        matches!(
            (self, next),
            (Accruing, Funded) | (Funded, Active) | (Accruing | Funded | Active, Released)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::AllocationState::*;

    #[test]
    fn released_is_terminal_and_reachable_from_everywhere() {
        for from in [Accruing, Funded, Active] {
            assert!(from.may_transition_to(Released));
        }
        for to in [Accruing, Funded, Active, Released] {
            assert!(!Released.may_transition_to(to));
        }
    }

    #[test]
    fn funding_cannot_be_skipped_or_reversed() {
        assert!(!Accruing.may_transition_to(Active));
        assert!(!Funded.may_transition_to(Accruing));
        assert!(!Active.may_transition_to(Funded));
    }
}
