//! # coppice-state
//!
//! The deterministic replicated state machine that sits behind Raft.
//!
//! This crate defines the authoritative control-plane state and the set of
//! commands that mutate it. Application of a command **must be deterministic**:
//! given the same sequence of committed commands, every replica must reach the
//! same state. That rules out wall-clock reads, randomness, network calls,
//! expensive scheduling computation, and iteration over unordered maps during
//! apply. See `docs/architecture/high-availability.md` and the full catalog
//! and apply contract in `docs/architecture/command-catalog.md`.
//!
//! Commands commit *decisions, not computations*. Every timestamp rides in
//! the command, every id is minted by the proposer, and a command that fails
//! validation applies as a deterministic no-op recording a
//! [`RejectionReason`] — it was already committed to the log on every
//! replica, so refusing it must be just as reproducible as applying it.

use std::collections::{BTreeMap, BTreeSet};

use imbl::OrdMap;

use coppice_core::allocation::Allocation;
use coppice_core::attempt::{Attempt, AttemptState};
use coppice_core::id::{
    AllocationId, AttemptId, EnrollTokenId, GroupId, JobId, MachineId, NodeId, QuotaEntityId,
};
use coppice_core::job::{Job, JobState};
use coppice_core::node::Node;
use coppice_core::quota::{
    ChargeRecord, CostUnits, CostWeights, DecayPolicy, PriorityMultiplier, UsageState,
    DEFAULT_PENALTY_EXPONENT_MILLI, DEFAULT_REFUND_FRACTION_MILLI,
    DEFAULT_UNBOUNDED_RUNTIME_MULTIPLIER,
};
use coppice_core::time::{Duration, Timestamp};

mod apply;
pub mod command;

pub use command::Command;

/// The authoritative, replicated control-plane state.
///
/// Only durable semantic state required for correctness lives here. Derived
/// state (indexes, queue projections, UI aggregates) is rebuilt from this.
/// Every map iterates in key order to keep apply deterministic, and every
/// field is `PartialEq` so the determinism harness can assert replica
/// equivalence structurally.
///
/// The maps that scale with job count — `jobs`, `attempts`, `allocations`,
/// and the derived `accrual_queue`, together millions of entries — are
/// `imbl::OrdMap`, whose structural sharing makes cloning the whole state
/// O(1). That is what lets the apply task hand a fresh state to view
/// publication and snapshot capture without deep-copying it (KOI-5). `nodes`
/// and `quota_entities` stay `BTreeMap`: they are bounded (~1k nodes; quota
/// entities scale with accounts, not jobs) and cheap to deep-clone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateMachine {
    pub jobs: OrdMap<JobId, JobRecord>,
    pub attempts: OrdMap<AttemptId, AttemptRecord>,
    pub allocations: OrdMap<AllocationId, AllocationRecord>,
    pub nodes: BTreeMap<NodeId, NodeRecord>,
    /// The quota-entity tree (ADR 0005) with each entity's replicated usage
    /// accumulator.
    pub quota_entities: BTreeMap<QuotaEntityId, QuotaEntity>,
    /// Exactly the allocations in state `Accruing`, keyed `(node, seq)` so a
    /// range scan yields one node's accruing allocations in commit order —
    /// the funding order of ADR 0014.
    ///
    /// Never iterate accruals by `AllocationId`: UUID order is meaningless
    /// across histories. Derived from the allocation map, so it is not
    /// snapshotted: the proto snapshot path (`coppice_proto::convert`)
    /// rebuilds it from the Accruing `AllocationRecord`s at load.
    pub accrual_queue: OrdMap<(NodeId, u64), AllocationId>,
    /// Commit-order sequence for allocations.
    ///
    /// Part of replicated state so it is a pure function of the command
    /// history.
    pub next_allocation_seq: u64,
    /// Replicated cluster policy (ADR 0020: never in node config files).
    pub policy: PolicyConfig,
    /// Semantic feature gate (ADR 0003), bumped only by `BumpClusterVersion`.
    pub cluster_version: u32,
    /// The cluster CA certificate bundle (ADR 0037 §4), or `None` before the
    /// cluster is formed. Deliberately **not** in [`PolicyConfig`]: that type
    /// is pure scheduler/accounting policy, whereas these are cluster-identity
    /// facts (see the `Cluster PKI / identity` catalog section). The CA
    /// *private key* never lives in replicated state — no field of
    /// [`CaCertificate`] could hold it.
    pub ca: Option<CaCertificate>,
    /// Coordinator machine identity → raft seat bindings (ADR 0037 §7).
    /// Bounded (~cluster size), so a plain `BTreeMap`, not `imbl::OrdMap`.
    pub machine_bindings: BTreeMap<MachineId, MachineBinding>,
    /// Live and revoked enrollment tokens by id (ADR 0037 §5). Bounded.
    pub enroll_tokens: BTreeMap<EnrollTokenId, EnrollToken>,
    /// Identities refused renewal (ADR 0037 §5 eviction). Bounded set.
    pub revoked_identities: BTreeSet<RevokedIdentity>,
    /// Raft node id → the instant that node confirmed durable receipt of the
    /// CA key (ADR 0037 §4). The replicated *fact* of possession; the key
    /// itself is never replicated. Bounded (~cluster size).
    pub key_confirmations: BTreeMap<u64, Timestamp>,
    /// Coordinator machine identity → the instant it first enrolled and
    /// received a leaf (ADR 0037 §4). Records enrollment only; binding to a
    /// raft seat is [`machine_bindings`](Self::machine_bindings). Bounded
    /// (~cluster size), so a plain `BTreeMap`.
    pub enrolled_identities: BTreeMap<MachineId, Timestamp>,
    /// Count of applied log entries, accepted or rejected.
    ///
    /// Bumped on every applied command so it is a stable coordinate for
    /// `expected_version` and read-consistency cursors.
    pub version: u64,
}

impl StateMachine {
    /// The raft seat a machine identity is bound to, if any (ADR 0037 §7).
    pub fn machine_binding(&self, machine: &MachineId) -> Option<&MachineBinding> {
        self.machine_bindings.get(machine)
    }

    /// The machine identity bound to a raft node id, if any. A linear scan —
    /// bindings are bounded (~cluster size), so this stays cheap.
    pub fn machine_for_raft_node(&self, raft_node_id: u64) -> Option<&MachineId> {
        self.machine_bindings
            .iter()
            .find(|(_, b)| b.raft_node_id == raft_node_id)
            .map(|(id, _)| id)
    }

    /// Whether an identity has been revoked (ADR 0037 §5).
    pub fn is_identity_revoked(&self, identity: &RevokedIdentity) -> bool {
        self.revoked_identities.contains(identity)
    }

    /// Enrollment tokens that are usable at `now`: not revoked and not expired
    /// (a token with no `expires_at` never expires). Iterates in id order.
    pub fn live_enroll_tokens(
        &self,
        now: Timestamp,
    ) -> impl Iterator<Item = (&EnrollTokenId, &EnrollToken)> {
        self.enroll_tokens
            .iter()
            .filter(move |(_, t)| !t.revoked && t.expires_at.map_or(true, |exp| exp > now))
    }

    /// Whether a node has confirmed durable receipt of the CA key (ADR 0037
    /// §4).
    pub fn has_key_confirmation(&self, raft_node_id: u64) -> bool {
        self.key_confirmations.contains_key(&raft_node_id)
    }

    /// Whether a coordinator machine identity has enrolled (ADR 0037 §4).
    pub fn is_identity_enrolled(&self, machine: &MachineId) -> bool {
        self.enrolled_identities.contains_key(machine)
    }
}

/// A validated PEM bundle of X.509 **CA certificates** — public material only
/// (ADR 0037 §4).
///
/// The CA private key must never enter replicated state, and this type
/// enforces that *by construction*: the only way in is [`CaCertBundle::parse`],
/// which requires every PEM block to carry the `CERTIFICATE` label **and**
/// DER-parse as a real X.509 certificate with the CA basic constraint — so a
/// private-key PEM, a bundle with a key appended, or key DER relabeled as a
/// certificate are all refused before they can ride a command into the Raft
/// log (validating at apply would be too late; the payload would already be
/// replicated and snapshotted). The inner string is private so no other
/// construction path exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaCertBundle {
    pem: String,
}

/// A candidate CA bundle failed [`CaCertBundle::parse`] validation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidCaBundle {
    #[error("bundle contains no CERTIFICATE PEM block")]
    Empty,
    #[error("bundle contains a non-certificate PEM block: {0:?}")]
    ForbiddenBlock(String),
    #[error("malformed PEM structure: {0}")]
    Malformed(&'static str),
    #[error("CERTIFICATE block {index} does not parse as an X.509 certificate")]
    NotACertificate { index: usize },
    #[error("certificate {index} is not a CA (missing the CA basic constraint)")]
    NotACa { index: usize },
}

impl CaCertBundle {
    /// Validate `pem` as one-or-more CA-certificate PEM blocks (a chain is one
    /// bundle) with nothing else in the payload: no other block types, no
    /// content outside blocks, and every block's body must base64-decode and
    /// DER-parse as an X.509 certificate whose basic constraints say CA — the
    /// bundle is the cluster's trust-anchor set, so a leaf has no business
    /// here, and a private key relabeled `CERTIFICATE` fails the DER parse.
    pub fn parse(pem: impl Into<String>) -> Result<CaCertBundle, InvalidCaBundle> {
        use base64::Engine as _;
        use x509_parser::prelude::{FromDer, X509Certificate};

        let pem = pem.into();
        let mut blocks = 0usize;
        let mut body: Option<String> = None;
        for raw in pem.lines() {
            let line = raw.trim();
            if line.is_empty() {
                continue;
            }
            if let Some(rest) = line.strip_prefix("-----BEGIN ") {
                if body.is_some() {
                    return Err(InvalidCaBundle::Malformed("BEGIN inside an open block"));
                }
                let label = rest
                    .strip_suffix("-----")
                    .ok_or(InvalidCaBundle::Malformed("unterminated BEGIN marker"))?;
                if label != "CERTIFICATE" {
                    return Err(InvalidCaBundle::ForbiddenBlock(label.to_string()));
                }
                body = Some(String::new());
            } else if let Some(rest) = line.strip_prefix("-----END ") {
                let label = rest
                    .strip_suffix("-----")
                    .ok_or(InvalidCaBundle::Malformed("unterminated END marker"))?;
                let base64_body = match body.take() {
                    Some(b) if label == "CERTIFICATE" => b,
                    _ => return Err(InvalidCaBundle::Malformed("END without matching BEGIN")),
                };
                // The label is only a claim; the DER is the fact. Decode and
                // parse the block as a whole X.509 certificate (no trailing
                // bytes), then require the CA basic constraint.
                let der = base64::engine::general_purpose::STANDARD
                    .decode(base64_body)
                    .map_err(|_| InvalidCaBundle::NotACertificate { index: blocks })?;
                let cert = match X509Certificate::from_der(&der) {
                    Ok(([], cert)) => cert,
                    _ => return Err(InvalidCaBundle::NotACertificate { index: blocks }),
                };
                let is_ca = matches!(cert.basic_constraints(), Ok(Some(bc)) if bc.value.ca);
                if !is_ca {
                    return Err(InvalidCaBundle::NotACa { index: blocks });
                }
                blocks += 1;
            } else {
                match body.as_mut() {
                    None => return Err(InvalidCaBundle::Malformed("content outside a PEM block")),
                    Some(b) => b.push_str(line),
                }
            }
        }
        if body.is_some() {
            return Err(InvalidCaBundle::Malformed("unterminated CERTIFICATE block"));
        }
        if blocks == 0 {
            return Err(InvalidCaBundle::Empty);
        }
        Ok(CaCertBundle { pem })
    }

    /// The validated PEM text.
    pub fn pem(&self) -> &str {
        &self.pem
    }
}

/// The cluster CA certificate bundle — **public** material only (ADR 0037 §4).
///
/// A [`CaCertBundle`] can hold a chain (a future re-root). The CA private key
/// is created on the forming voter's disk and normally resides only on voter
/// disks; it never enters replicated state, and this type is shaped so it
/// *cannot* — the bundle newtype refuses anything but certificate blocks, and
/// there is no other field it could occupy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaCertificate {
    /// Validated public certificate material (never a private key).
    pub bundle: CaCertBundle,
    pub recorded_at: Timestamp,
}

/// One coordinator installation's binding to a raft seat (ADR 0037 §7).
///
/// Invariant: one machine identity is bound to at most one raft node id, ever
/// — a binding is never rewritten to a different node id. A re-admission of
/// the same `(machine, raft_node_id)` pair at the same address is an accepted
/// no-op; at a different address it is refused (ADR 0037 §7 — address changes
/// go through the operator set-address path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineBinding {
    /// The u64 openraft node id (minted by coppice-consensus; distinct from a
    /// compute [`NodeId`]).
    pub raft_node_id: u64,
    pub address: String,
    /// When the identity was first bound; preserved verbatim across a same-pair
    /// re-admission.
    pub bound_at: Timestamp,
    /// When learner GC marked this binding dead (ADR 0037 §7 stale-learner
    /// GC). `None` while live. A retired identity is never re-admitted — this
    /// is a mark, never a delete, and the binding invariant (one machine
    /// identity ↔ at most one raft node id, ever) extends past retirement:
    /// the seat that was retired stays retired, one seat ever.
    pub retired_at: Option<Timestamp>,
}

/// The role an enrollment token grants (ADR 0037 §5) — never both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrollRole {
    Coordinator,
    Agent,
}

/// A replicated enrollment-token record (ADR 0037 §5).
///
/// `hash` is the opaque PHC string produced by `coppice-tls::pki::token`; the
/// hashing lives there, not here (this crate takes no argon2 dependency), and
/// the secret is never derivable from the record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrollToken {
    pub hash: String,
    pub role: EnrollRole,
    pub label: String,
    /// `None` means the token never expires.
    pub expires_at: Option<Timestamp>,
    pub minted_at: Timestamp,
    pub revoked: bool,
}

/// An identity refused renewal (ADR 0037 §5 eviction).
///
/// `Ord` so it can key a `BTreeSet`; the derived order is (variant, inner id).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum RevokedIdentity {
    Machine(MachineId),
    Node(NodeId),
}

/// A job's replicated record: the submitted spec plus lifecycle bookkeeping
/// owned by the apply loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRecord {
    pub spec: Job,
    pub state: JobState,
    /// The Q32.32 priority multiplier resolved by the API at proposal time
    /// (ADR 0019: apply never sees the raw `i32` in arithmetic).
    pub multiplier: PriorityMultiplier,
    pub submitted_at: Timestamp,
    /// When the job reached its terminal state, stamped from the resolving
    /// command's proposer timestamp: abort's `requested_at`, an outcome
    /// or reconcile report's `observed_at`, or a loss declaration's
    /// `declared_at`. `None` while the job is live.
    ///
    /// The retention clock for `EvictTerminalJobs` runs from this, never
    /// from `submitted_at` (ADR 0012): a job may legitimately queue far
    /// longer than the retention interval before it ever runs.
    pub terminal_at: Option<Timestamp>,
    /// Retries consumed.
    ///
    /// `Revoked` outcomes requeue without touching this.
    pub retries_used: u32,
    /// Every attempt this job has had, in creation order. The attempt in
    /// flight, when there is one, is carried by `state`
    /// ([`JobState::Attempting`]) rather than stored separately (ADR 0030);
    /// [`current_attempt`](JobRecord::current_attempt) derives it from there.
    pub attempts: Vec<AttemptId>,
}

impl JobRecord {
    /// The attempt this job is currently pursuing, if any.
    ///
    /// A derived view of `state` — `Some` exactly while
    /// [`JobState::Attempting`] — kept as a method so call sites read as they
    /// did before the field was removed (ADR 0030). It cannot disagree with
    /// the state, which is the point of folding the link into the enum.
    pub fn current_attempt(&self) -> Option<AttemptId> {
        self.state.attempt()
    }
}

/// An attempt's replicated record: the attempt itself plus the charge that
/// placement committed, kept for true-up at terminal resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    pub attempt: Attempt,
    /// The placement group sharing the `Ready` barrier.
    ///
    /// v1: the job's id.
    pub group: GroupId,
    pub charge: ChargeRecord,
    /// Rate and multiplier the charge used, so true-up never repriced by a
    /// later policy edit (ADR 0019).
    pub rate_ucu_per_second: u64,
    pub multiplier: PriorityMultiplier,
    /// Set when the attempt is observed `Running`.
    ///
    /// An attempt that never started has actual cost zero at true-up.
    pub started_at: Option<Timestamp>,
}

/// An allocation's replicated record plus its commit-order sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationRecord {
    pub allocation: Allocation,
    /// Commit order: assigned from `next_allocation_seq` at creation.
    ///
    /// Funding iterates ascending `seq`, never id order.
    pub seq: u64,
}

/// A node's replicated record: descriptor plus its fencing epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub node: Node,
    /// Bumped on (re)registration and on loss declaration; invalidates all
    /// coordinator→agent commands issued under earlier epochs (ADR 0009).
    pub epoch: u64,
}

/// One node of the quota-entity tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaEntity {
    pub parent: Option<QuotaEntityId>,
    pub name: String,
    /// The soft quota as a *stock* in µCU (ADR 0019); config tooling converts
    /// human rates.
    pub quota: CostUnits,
    pub usage: UsageState,
    /// When this entity was first configured, from the creating command's
    /// proposer stamp; preserved verbatim across every later reconfigure.
    pub created_at: Timestamp,
    /// The most recent `ConfigureQuotaEntity`'s proposer stamp. Equal to
    /// `created_at` on a freshly created entity.
    pub updated_at: Timestamp,
}

/// Maximum quota-tree depth.
///
/// Bounds the ancestor walk during charging so no command can turn apply
/// into unbounded work.
pub const QUOTA_TREE_DEPTH_CAP: u32 = 32;

/// Replicated cluster policy (ADR 0020).
///
/// Everything here would diverge scheduling or accounting if replicas
/// disagreed, so none of it may appear in a node config file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyConfig {
    pub cost_weights: CostWeights,
    pub decay: DecayPolicy,
    pub penalty_exponent_milli: u32,
    /// Maps the user-facing `priority: i32` to a Q32.32 cost multiplier.
    ///
    /// The API resolves through this table at proposal time.
    pub priority_multipliers: BTreeMap<i32, PriorityMultiplier>,
    /// K: at most this many jobs hold accruing allocations at once
    /// (ADR 0014, default 4).
    pub accrual_limit: u32,
    /// Charge-time runtime for jobs with no enforced `max_runtime`, seconds.
    pub default_charge_runtime_s: u64,
    /// Q32.32 multiplier folded into the effective priority multiplier of
    /// any job placed without an enforced `max_runtime` (ADR 0029).
    /// Validated ≥ 1.0 at policy commit; 1.0 disables the surcharge.
    pub unbounded_runtime_multiplier: PriorityMultiplier,
    /// Parts-per-thousand of the unused charge refunded at true-up, applied
    /// to job-attributable outcomes of attempts that ran with a declared
    /// `max_runtime` (ADR 0029). Captured on the charge record at placement;
    /// validated ≤ 1000 at policy commit; 1000 restores full refunds.
    pub refund_fraction_milli: u32,
    /// Terminal jobs are eligible for `EvictTerminalJobs` this long after
    /// terminal state (ADR 0012).
    ///
    /// Consulted by the proposer, never by apply.
    pub terminal_retention: Duration,
    /// Default SIGTERM→SIGKILL grace for aborts.
    pub abort_grace: Duration,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        PolicyConfig {
            cost_weights: CostWeights::default(),
            decay: DecayPolicy::DEFAULT,
            penalty_exponent_milli: DEFAULT_PENALTY_EXPONENT_MILLI,
            priority_multipliers: BTreeMap::new(),
            accrual_limit: 4,
            default_charge_runtime_s: 86_400,
            unbounded_runtime_multiplier: DEFAULT_UNBOUNDED_RUNTIME_MULTIPLIER,
            refund_fraction_milli: DEFAULT_REFUND_FRACTION_MILLI,
            terminal_retention: Duration::from_hours(72),
            abort_grace: Duration::from_secs(30),
        }
    }
}

/// Why a committed command was refused.
///
/// The rejection is part of the deterministic apply result: every replica
/// computes the identical reason, state changes only by the `version` bump,
/// and the proposer observes it through the leader's apply result. See the
/// taxonomy table in `docs/architecture/command-catalog.md`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RejectionReason {
    #[error("job {0} not found")]
    UnknownJob(JobId),
    #[error("node {0} not found")]
    UnknownNode(NodeId),
    #[error("attempt {0} not found")]
    UnknownAttempt(AttemptId),
    #[error("allocation {0} not found")]
    UnknownAllocation(AllocationId),
    #[error("quota entity {0} not found")]
    UnknownQuotaEntity(QuotaEntityId),
    #[error("job {0} already exists with a different spec")]
    SubmitSpecMismatch(JobId),
    #[error("attempt {0} already exists")]
    DuplicateAttempt(AttemptId),
    #[error("allocation {0} already exists")]
    DuplicateAllocation(AllocationId),
    #[error("job {0} is terminal")]
    JobTerminal(JobId),
    #[error("job {0} is not queued")]
    JobNotQueued(JobId),
    #[error("job {0} is not terminal")]
    JobNotTerminal(JobId),
    #[error("attempt {0} already passed this transition")]
    StaleAttemptState(AttemptId),
    #[error("attempt {attempt} is not on node {node}")]
    AttemptNotOnNode { attempt: AttemptId, node: NodeId },
    #[error("allocation {0} is not accruing")]
    AllocationNotAccruing(AllocationId),
    #[error("node {0} is not schedulable")]
    NodeNotSchedulable(NodeId),
    #[error("observed set for node {node} carries epoch {got}, current is {current}")]
    StaleNodeEpoch {
        node: NodeId,
        current: u64,
        got: u64,
    },
    #[error("allocation {0} requests more than the node's total capacity")]
    RequestExceedsNodeCapacity(AllocationId),
    #[error("batch would leave more than {limit} jobs accruing")]
    AccrualLimitExceeded { limit: u32 },
    #[error("placement shape unsupported in v1 (one allocation, singleton group)")]
    UnsupportedPlacementShape,
    #[error("quota entity {0} parent chain would cycle or exceed the depth cap")]
    QuotaEntityCycle(QuotaEntityId),
    #[error("invalid policy: {0}")]
    InvalidPolicy(String),
    #[error("cluster version {requested} is not above current {current}")]
    ClusterVersionNotMonotonic { current: u32, requested: u32 },
    #[error("machine {machine} / raft node {raft_node_id} binding conflicts with an existing one")]
    MachineIdentityConflict {
        machine: MachineId,
        raft_node_id: u64,
    },
    #[error(
        "machine {machine} / raft node {raft_node_id} is already bound at a different address; \
         address changes go through the operator set-address path (ADR 0037 §7)"
    )]
    MachineAddressConflict {
        machine: MachineId,
        raft_node_id: u64,
    },
    #[error(
        "raft node {raft_node_id} carries no machine-identity binding; rebind repoints an \
         existing binding, it never creates one (ADR 0037 §6)"
    )]
    UnknownMachineBinding { raft_node_id: u64 },
    #[error(
        "machine {0} carries no machine-identity binding; retire marks an existing binding, it \
         never creates one (ADR 0037 §7 learner GC)"
    )]
    UnknownMachineIdentity(MachineId),
    #[error(
        "machine {machine} was retired by learner GC and is never re-admitted (ADR 0037 §7 \
         one-seat-ever)"
    )]
    MachineIdentityRetired { machine: MachineId },
    #[error("enrollment token {0} already exists")]
    DuplicateEnrollToken(EnrollTokenId),
    #[error("enrollment token {0} not found")]
    UnknownEnrollToken(EnrollTokenId),
    #[error("command shape invalid: {0}")]
    InvalidCommand(String),
    #[error("batch rejected; per-item diagnostics attached")]
    InvalidBatch(Vec<Rejection>),
}

/// One item's rejection within an otherwise-processed batch: the failing
/// item's position and why it was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    /// Zero-based position of the offending item within the submitted batch.
    pub item_index: u32,
    /// Why the item at `item_index` was refused.
    pub reason: RejectionReason,
}

/// Change events produced by an accepted command — derived output for the
/// event fanout (ADR 0008) and the coordinator runtime, never read back by
/// apply.
///
/// Every attempt- and allocation-scoped event carries its owning job and node
/// ids as **scope keys**, stamped during apply while the association is
/// authoritative. Scoped subscriptions (ADR 0008) filter on these directly;
/// the fanout never has to look the owner up in mutable state that may have
/// moved on by delivery time (KOI-3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    JobSubmitted {
        job: JobId,
    },
    JobStateChanged {
        job: JobId,
        from: JobState,
        to: JobState,
    },
    AttemptStateChanged {
        attempt: AttemptId,
        job: JobId,
        node: NodeId,
        state: AttemptState,
    },
    AllocationFunded {
        allocation: AllocationId,
        job: JobId,
        node: NodeId,
    },
    /// An abort needs a `StopJob` sent to this node — apply does no I/O; the
    /// runtime acts on this.
    StopRequested {
        node: NodeId,
        allocation: AllocationId,
        job: JobId,
    },
    NodeEpochBumped {
        node: NodeId,
        epoch: u64,
    },
    JobEvicted {
        job: JobId,
    },
    QuotaEntityConfigured {
        entity: QuotaEntityId,
    },
    PolicyUpdated,
    ClusterVersionBumped {
        to: u32,
    },
}

/// The successful result of applying one command.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    pub events: Vec<Event>,
}
