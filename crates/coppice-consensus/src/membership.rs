//! The replicated node record and the self-converging membership rules
//! (ADR 0037 §4/§5/§6).
//!
//! Two things live here. [`CoordinatorNode`] is this crate's openraft `Node`
//! binding — it replaces openraft's `BasicNode` so that each membership seat
//! carries, atomically with the membership change that admits it, the
//! **machine identity** it is bound to (the CA-attested subject of the mTLS
//! certificate the admission arrived under) and a `superseded` marker. Because
//! the record is part of the replicated log and every snapshot's membership
//! meta, the one-seat-per-installation invariant and promotion-coupled removal
//! are decided from replicated state rather than a leader's local memory.
//!
//! The rest of the module is the **pure decision core** the adapter drives:
//! [`decide_add_learner`] (the idempotent AddLearner contract and the seat
//! rules of §6) and [`decide_promotion_voters`] (the replacement/overflow
//! removal rules of §5). Factoring them out as pure functions over a snapshot
//! of membership lets the whole contract be unit-tested without a live raft.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::CoordinatorId;

/// This crate's openraft `Node` binding (ADR 0037 §6), replacing openraft's
/// `BasicNode`.
///
/// # Durable-format note
///
/// Adding fields here is a deliberate change to the durable membership format:
/// the record is encoded into `coppice.raft.v1.RaftMember` on both the segment
/// log (membership log entries) and every snapshot's membership meta
/// (`crate::storage::raftpb`). The two new fields are additive protobuf tags,
/// so an older record decodes with `machine_identity = ""` and
/// `superseded = false` — i.e. "unbound, not superseded". Pre-1.0 there is no
/// migration; the format is versioned only by the proto corpus.
///
/// openraft (built without its `serde` feature, ADR 0002) requires a `Node`
/// to be `Clone + Debug + Default + Eq + Send + Sync + 'static`; `Display` is
/// not required but implemented for logging.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CoordinatorNode {
    /// The `host:port` peers dial to reach this node.
    pub addr: String,
    /// The machine identity bound to this seat: the CA-attested subject of the
    /// mTLS certificate the admission (`AddLearner`) or formation
    /// (`InitializeCluster`) arrived under (ADR 0037 §6). Verified by the TLS
    /// layer, never claimed in a request body. Empty only for records written
    /// before this field existed.
    pub machine_identity: String,
    /// Set when this voter's machine identity has admitted a replacement
    /// learner (ADR 0037 §5): the promotion that grants the replacement's vote
    /// retires this seat in the same joint change. "Superseded" is a marking,
    /// never a mechanism — only the promotion actually retires the vote.
    pub superseded: bool,
}

impl CoordinatorNode {
    /// A fresh, non-superseded record for `addr` bound to `machine_identity`.
    pub fn new(addr: impl Into<String>, machine_identity: impl Into<String>) -> Self {
        CoordinatorNode {
            addr: addr.into(),
            machine_identity: machine_identity.into(),
            superseded: false,
        }
    }
}

impl std::fmt::Display for CoordinatorNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)?;
        if !self.machine_identity.is_empty() {
            write!(f, " [{}]", self.machine_identity)?;
        }
        if self.superseded {
            write!(f, " (superseded)")?;
        }
        Ok(())
    }
}

/// Node-local membership policy (ADR 0037 §5). Not replicated: convergence
/// consults `cluster_size` before replicated state is reachable, exactly like
/// `cluster_id` (ADR 0020's node-config litmus). Wired from config by a later
/// package; [`Default`] carries the ADR's defaults until then.
#[derive(Debug, Clone, Copy)]
pub struct MembershipPolicy {
    /// Expected voter count; the overflow-removal threshold and the
    /// formation-complete signal (§5/§7).
    pub cluster_size: usize,
    /// How long the leader's replication to a voter must have been failing
    /// before that voter is eligible for an evidence-gated overflow removal.
    pub removal_grace: Duration,
    /// How long a pending learner may be unreachable / make no replication
    /// progress before a newcomer for the same machine identity may evict it
    /// (§6).
    pub replacement_grace: Duration,
}

impl Default for MembershipPolicy {
    fn default() -> Self {
        MembershipPolicy {
            cluster_size: 3,
            removal_grace: Duration::from_secs(60),
            replacement_grace: Duration::from_secs(60),
        }
    }
}

/// A discovery backend that can attest instance liveness (ADR 0037 §5). Only
/// backends with liveness semantics (e.g. `ec2-asg`) implement it meaningfully;
/// the adapter holds `Option<Arc<dyn LivenessAttestor>>` and a `None` default
/// contributes nothing to removal decisions, so a stale registration file or
/// an unedited static list can never block a legitimate removal.
pub trait LivenessAttestor: Send + Sync {
    /// Whether the backend can attest that `node` — whose membership record
    /// advertises `addr` — is genuinely gone from the group. `false` means
    /// present-or-unknown; only `true` strengthens an overflow-removal
    /// decision. The address comes from the leader's own membership record, so
    /// backends keyed on network location (e.g. `ec2-asg` private IPs) need no
    /// side-channel id mapping.
    fn is_absent(&self, node: CoordinatorId, addr: &str) -> bool;
}

/// One membership seat, as the decision core sees it — a flattened view of a
/// [`CoordinatorNode`] plus whether it is currently a voter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub id: CoordinatorId,
    pub addr: String,
    pub machine_identity: String,
    pub superseded: bool,
    pub voter: bool,
}

/// The outcome of the idempotent AddLearner contract + seat rules (§4/§6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddLearnerDecision {
    /// `id` is already a learner/voter at the requested address — no-op success.
    Noop,
    /// `id` is already in membership at a *different* address — refuse; there
    /// is no silent repointing.
    RefuseSameIdDifferentAddress { existing_addr: String },
    /// A different, still-live pending learner holds this machine identity's
    /// replacement slot — refuse (non-retryable while it stays live).
    RefuseMachineSeatPending { incumbent: CoordinatorId },
    /// Admit as a fresh learner: no voter and no pending learner bound to this
    /// machine identity.
    AdmitFresh,
    /// Admit as a replacement: a voter bound to this machine identity exists;
    /// mark it superseded in the same admission.
    AdmitReplacingVoter { predecessor: CoordinatorId },
    /// Admit after atomically evicting a *stale* pending learner that held the
    /// slot (unreachable / no progress for `replacement_grace`).
    AdmitEvictingStaleLearner { stale: CoordinatorId },
}

/// Decide AddLearner against the current membership snapshot (ADR 0037 §4/§6).
///
/// `is_stale(id)` is the leader's own observation that a pending learner has
/// been unreachable / made no replication progress for `replacement_grace`;
/// the adapter supplies it from its per-follower progress tracking.
pub fn decide_add_learner(
    members: &[NodeRecord],
    new_id: CoordinatorId,
    new_addr: &str,
    machine_identity: &str,
    is_stale: impl Fn(CoordinatorId) -> bool,
) -> AddLearnerDecision {
    // (1) State short-circuit BEFORE any seat gate (§4): if the id is already
    // known, it is either the same address (idempotent no-op) or a conflicting
    // repoint (refused).
    if let Some(existing) = members.iter().find(|m| m.id == new_id) {
        if existing.addr == new_addr {
            return AddLearnerDecision::Noop;
        }
        return AddLearnerDecision::RefuseSameIdDifferentAddress {
            existing_addr: existing.addr.clone(),
        };
    }

    // (2) Seat rules for the machine identity (§6). At most one voter and one
    // pending learner may be bound to a given identity. Records with an empty
    // machine identity (legacy / initial voter before binding) never collide.
    if !machine_identity.is_empty() {
        // A different pending learner already holding the slot is the
        // deterministic winner unless it has gone stale.
        if let Some(pending) = members
            .iter()
            .find(|m| !m.voter && m.machine_identity == machine_identity && m.id != new_id)
        {
            if is_stale(pending.id) {
                return AddLearnerDecision::AdmitEvictingStaleLearner { stale: pending.id };
            }
            return AddLearnerDecision::RefuseMachineSeatPending {
                incumbent: pending.id,
            };
        }
        // A bound voter means this is a replacement: admit and supersede it.
        if let Some(voter) = members
            .iter()
            .find(|m| m.voter && m.machine_identity == machine_identity)
        {
            return AddLearnerDecision::AdmitReplacingVoter {
                predecessor: voter.id,
            };
        }
    }

    AddLearnerDecision::AdmitFresh
}

/// Why a promotion's required removal could not be satisfied (ADR 0037 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionRefusal {
    /// Removing the one peer this ADR permits would still leave > cluster_size.
    VoterSetFull,
    /// An overflow removal is needed but no voter qualifies as dead, or the
    /// resulting set would lack a live majority.
    NoRemovablePeer,
}

impl std::fmt::Display for PromotionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PromotionRefusal::VoterSetFull => f.write_str(
                "voter set full — removing the one permitted peer still leaves more than \
                 cluster_size voters",
            ),
            PromotionRefusal::NoRemovablePeer => {
                f.write_str("no removable peer — no dead voter qualifies for overflow removal")
            }
        }
    }
}

/// Inputs to the promotion-coupled removal decision (ADR 0037 §5).
pub struct PromotionInputs<'a> {
    /// Expected voter count (`MembershipPolicy::cluster_size`).
    pub cluster_size: usize,
    /// The voter-id set *before* this promotion.
    pub current_voters: &'a BTreeSet<CoordinatorId>,
    /// The caught-up learner being promoted.
    pub promoting: CoordinatorId,
    /// The promoting node's machine identity's superseded predecessor, if one
    /// is still in membership (the mandatory replacement removal).
    pub superseded_predecessor: Option<CoordinatorId>,
    /// Voters the leader's replication has been failing to reach for longer
    /// than `removal_grace` (and, where a `LivenessAttestor` applies, absent
    /// from discovery). The adapter has already applied any attestation.
    pub dead_voters: &'a BTreeSet<CoordinatorId>,
    /// Voters the leader currently reaches within the promotion-lag threshold
    /// (its own vantage), for the live-majority postcondition.
    pub live_voters: &'a BTreeSet<CoordinatorId>,
}

/// Compute the final voter set a promotion must install, applying the
/// mandatory replacement removal first and an evidence-gated overflow removal
/// second (ADR 0037 §5). Returns the set to hand to `ReplaceAllVoters`, or a
/// machine-readable refusal.
///
/// Pure: no raft, no clock — the adapter resolves "dead" / "live" / the
/// predecessor from live metrics and hands the resolved sets in, so this can
/// be exhaustively unit-tested.
pub fn decide_promotion_voters(
    inputs: PromotionInputs<'_>,
) -> Result<BTreeSet<CoordinatorId>, PromotionRefusal> {
    let mut set: BTreeSet<CoordinatorId> = inputs.current_voters.clone();
    set.insert(inputs.promoting);

    // Replacement removal — unconditional, regardless of voter count (§5).
    if let Some(pred) = inputs.superseded_predecessor {
        set.remove(&pred);
    }

    // Overflow removal — evaluated second, only if still oversized (§5).
    if set.len() > inputs.cluster_size {
        // At most ONE additional dead voter may be dropped, and never the node
        // being promoted (it just caught up; it is not dead).
        let candidate = inputs
            .dead_voters
            .iter()
            .copied()
            .filter(|v| set.contains(v) && *v != inputs.promoting)
            .min();
        match candidate {
            Some(dead) => {
                set.remove(&dead);
            }
            None => return Err(PromotionRefusal::NoRemovablePeer),
        }
        // Only one removal is permitted; if that is not enough, refuse rather
        // than commit an oversized configuration.
        if set.len() > inputs.cluster_size {
            return Err(PromotionRefusal::VoterSetFull);
        }
    }

    // Postcondition: a live majority of the resulting set from the leader's
    // vantage (§5). The promoting node is live (it just caught up), but guard
    // anyway rather than commit a config that cannot make progress.
    let live = set
        .iter()
        .filter(|v| inputs.live_voters.contains(v))
        .count();
    if live * 2 <= set.len() {
        return Err(PromotionRefusal::NoRemovablePeer);
    }

    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: u64, addr: &str, mi: &str, superseded: bool, voter: bool) -> NodeRecord {
        NodeRecord {
            id,
            addr: addr.into(),
            machine_identity: mi.into(),
            superseded,
            voter,
        }
    }

    fn never_stale(_: CoordinatorId) -> bool {
        false
    }

    // ---- AddLearner idempotency + seat rules (§4/§6) ----

    #[test]
    fn add_learner_same_id_same_addr_is_noop() {
        let members = vec![rec(1, "a:1", "m1", false, false)];
        assert_eq!(
            decide_add_learner(&members, 1, "a:1", "m1", never_stale),
            AddLearnerDecision::Noop
        );
    }

    #[test]
    fn add_learner_same_id_different_addr_is_refused() {
        let members = vec![rec(1, "a:1", "m1", false, true)];
        assert_eq!(
            decide_add_learner(&members, 1, "b:2", "m1", never_stale),
            AddLearnerDecision::RefuseSameIdDifferentAddress {
                existing_addr: "a:1".into()
            }
        );
    }

    #[test]
    fn add_learner_fresh_identity_admits() {
        let members = vec![rec(1, "a:1", "m1", false, true)];
        assert_eq!(
            decide_add_learner(&members, 2, "b:2", "m2", never_stale),
            AddLearnerDecision::AdmitFresh
        );
    }

    #[test]
    fn add_learner_with_bound_voter_admits_as_replacement() {
        let members = vec![rec(1, "a:1", "m1", false, true)];
        assert_eq!(
            decide_add_learner(&members, 2, "b:2", "m1", never_stale),
            AddLearnerDecision::AdmitReplacingVoter { predecessor: 1 }
        );
    }

    #[test]
    fn add_learner_with_live_pending_learner_is_refused() {
        // m1 has a bound voter (1) and a pending replacement learner (2). A
        // third request for m1 loses to the pending learner.
        let members = vec![
            rec(1, "a:1", "m1", true, true),
            rec(2, "b:2", "m1", false, false),
        ];
        assert_eq!(
            decide_add_learner(&members, 3, "c:3", "m1", never_stale),
            AddLearnerDecision::RefuseMachineSeatPending { incumbent: 2 }
        );
    }

    #[test]
    fn add_learner_evicts_a_stale_pending_learner() {
        let members = vec![
            rec(1, "a:1", "m1", true, true),
            rec(2, "b:2", "m1", false, false),
        ];
        // Learner 2 is stale → the newcomer may evict it and take the slot.
        assert_eq!(
            decide_add_learner(&members, 3, "c:3", "m1", |id| id == 2),
            AddLearnerDecision::AdmitEvictingStaleLearner { stale: 2 }
        );
    }

    #[test]
    fn add_learner_empty_identity_never_collides() {
        // The initial voter is bound to "" before machine identities are wired;
        // a fresh learner with its own empty identity must still admit.
        let members = vec![rec(1, "a:1", "", false, true)];
        assert_eq!(
            decide_add_learner(&members, 2, "b:2", "", never_stale),
            AddLearnerDecision::AdmitFresh
        );
    }

    // ---- Promotion-coupled removal (§5) ----

    fn set(ids: &[u64]) -> BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    #[test]
    fn pure_promotion_underfilled_no_removal() {
        // Two of three voters, add a third: allowed, no removal.
        let voters = set(&[1, 2]);
        let out = decide_promotion_voters(PromotionInputs {
            cluster_size: 3,
            current_voters: &voters,
            promoting: 3,
            superseded_predecessor: None,
            dead_voters: &set(&[]),
            live_voters: &set(&[1, 2, 3]),
        })
        .expect("underfilled promotion allowed");
        assert_eq!(out, set(&[1, 2, 3]));
    }

    #[test]
    fn replacement_in_underfilled_cluster_retires_predecessor() {
        // Two voters (1 = superseded predecessor, 2), promote replacement 3.
        // The predecessor is retired in the same change regardless of count.
        let voters = set(&[1, 2]);
        let out = decide_promotion_voters(PromotionInputs {
            cluster_size: 3,
            current_voters: &voters,
            promoting: 3,
            superseded_predecessor: Some(1),
            dead_voters: &set(&[]),
            live_voters: &set(&[2, 3]),
        })
        .expect("replacement promotion allowed");
        assert_eq!(out, set(&[2, 3]));
    }

    #[test]
    fn replacement_in_full_cluster_removes_only_the_predecessor() {
        // Three voters, one (1) superseded; promoting 4 retires exactly 1 —
        // cardinality is satisfied by the replacement, no second removal.
        let voters = set(&[1, 2, 3]);
        let out = decide_promotion_voters(PromotionInputs {
            cluster_size: 3,
            current_voters: &voters,
            promoting: 4,
            superseded_predecessor: Some(1),
            // 2 is also dead, but must NOT be removed — replacement already
            // satisfied cardinality.
            dead_voters: &set(&[2]),
            live_voters: &set(&[2, 3, 4]),
        })
        .expect("full-cluster replacement allowed");
        assert_eq!(out, set(&[2, 3, 4]));
    }

    #[test]
    fn overflow_removes_one_dead_voter() {
        // Three voters, none superseded, promote a fourth: 4 > cluster_size,
        // one dead voter (3) qualifies for overflow removal.
        let voters = set(&[1, 2, 3]);
        let out = decide_promotion_voters(PromotionInputs {
            cluster_size: 3,
            current_voters: &voters,
            promoting: 4,
            superseded_predecessor: None,
            dead_voters: &set(&[3]),
            live_voters: &set(&[1, 2, 4]),
        })
        .expect("overflow removal allowed");
        assert_eq!(out, set(&[1, 2, 4]));
    }

    #[test]
    fn overflow_with_no_dead_voter_is_refused() {
        let voters = set(&[1, 2, 3]);
        let err = decide_promotion_voters(PromotionInputs {
            cluster_size: 3,
            current_voters: &voters,
            promoting: 4,
            superseded_predecessor: None,
            dead_voters: &set(&[]),
            live_voters: &set(&[1, 2, 3, 4]),
        })
        .unwrap_err();
        assert_eq!(err, PromotionRefusal::NoRemovablePeer);
    }

    #[test]
    fn overflow_too_large_for_one_removal_is_voter_set_full() {
        // Pre-existing overflow: 4 voters at cluster_size 3, promote a fifth.
        // One removal is permitted; even after it the set is still > 3.
        let voters = set(&[1, 2, 3, 4]);
        let err = decide_promotion_voters(PromotionInputs {
            cluster_size: 3,
            current_voters: &voters,
            promoting: 5,
            superseded_predecessor: None,
            dead_voters: &set(&[3, 4]),
            live_voters: &set(&[1, 2, 5]),
        })
        .unwrap_err();
        assert_eq!(err, PromotionRefusal::VoterSetFull);
    }
}
