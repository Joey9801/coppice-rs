//! Cluster CA re-rooting (ADR 0037 §4).
//!
//! ADR 0037 makes every disk that has ever held the cluster CA key
//! root-equivalent, and names **re-rooting** as the answer to a suspected
//! compromise of any of them. This module is that answer's executable half;
//! [`docs/operations/re-rooting.md`](../../../docs/operations/re-rooting.md) is
//! its runbook.
//!
//! # Why this is a local-socket verb
//!
//! Re-rooting mints a new cluster trust root and replaces the one every
//! machine-plane credential chains to. That is root-equivalent authority, so
//! it rides the same plane as [`crate::formation`]'s `init` and
//! `issue-operator-cert`: the daemon's own Unix socket, where local access
//! **is** the authorization (ADR 0037 §3). Putting it on the network admin
//! service would have meant an operator certificate — issued by the very CA
//! being replaced — could re-root the cluster from anywhere it could dial,
//! which is a strictly larger blast radius than the design accepts. It also
//! has to run *here* for a mechanical reason: the new key must be written to
//! a voter's disk, and the only disk this process can write is its own.
//!
//! # The invariant
//!
//! > **A root never becomes bundle position 0 — the active signing root —
//! > until its private key is durably held by every current voter.**
//!
//! Everything below is that sentence made operational. It exists because the
//! obvious implementation is unrecoverable: mint a root, commit it as active,
//! then write its key out. A leader that crashes between the commit and the
//! first durable write leaves a cluster whose recorded active root has **no
//! private half anywhere**. Nothing can sign — not an enrollment, not a
//! renewal, not the operator certificate you would use to fix it — and no
//! re-run of any verb can recover it, because the key that would have to be
//! recovered never touched a disk. There is no quorum of anything to consult;
//! the bytes are gone. So the ordering is not a nicety, it is the difference
//! between a rotation that survives a `kill -9` and a cluster that has to be
//! rebuilt from backup.
//!
//! # The phases
//!
//! [`RotationPhase`] names them, and a rotation only ever moves forwards
//! through them:
//!
//! 1. **Stage.** `begin` mints the incoming root and writes its key *and*
//!    certificate to this leader's disk ([`pki::stage_ca_material`], the
//!    owner-only atomic write), and only then commits the bundle
//!    `[outgoing (active, still signing), incoming (pending)]`. Dual trust
//!    starts here — both roots are anchors from the commit — but **signing
//!    does not move**. A crash anywhere in or after this phase is safe by
//!    construction: the outgoing root is still active and every voter that
//!    ever held its key still holds it.
//! 2. **Distribute.** The staged key is pushed to every other current voter
//!    over the shipped `TransferCaKey` RPC. Each recipient durably persists it
//!    (to its own staged path) and adopts the recorded anchors **before**
//!    acknowledging, and the leader records each confirmed receipt in
//!    *replicated* state ([`Command::ConfirmStagedKeyPossession`]), preceded by
//!    an intent ([`Command::RecordStagedKeyTransferIntent`]) committed before
//!    the bytes leave this disk. Replicated, because the resume path must run
//!    on *any* leader: a new leader has to know exactly who holds the staged
//!    key, and a local note on a dead machine cannot tell it.
//! 3. **Activate, gated on total coverage.** Only when every current voter has
//!    a replicated staged-key confirmation does `begin` commit the reordered
//!    bundle `[incoming (active), outgoing]` and then promote its own staged
//!    key to the live path. If any voter lacks a confirmation the verb
//!    **refuses to activate** ([`COVERAGE_INCOMPLETE`]), names the missing
//!    voters, and leaves the rotation parked in the staged phase — which is a
//!    working state indefinitely: the outgoing root still signs, both roots
//!    verify. The operator fixes membership (replace or remove the dead voter)
//!    and re-runs `begin`, which re-checks coverage against the **current**
//!    voter set.
//! 4. **Complete.** `complete` drops the outgoing root, and *that* is the
//!    instant an un-renewed leaf stops being accepted anywhere.
//!
//! # Resuming
//!
//! `begin` is the only verb an operator re-runs, and it resolves what to do
//! from replicated state plus this disk:
//!
//! - **A pending root is recorded and this leader holds its key** (the staged
//!   key on disk is the private half of the bundle's pending root): reuse it.
//!   Re-running distribution costs nothing on voters that already confirmed.
//! - **A pending root is recorded and this leader does not hold its key** — a
//!   leader change mid-rotation: **replace the pending entry.** Mint a fresh
//!   root, stage it here, and propose a bundle that swaps it in for the old
//!   pending one. This is safe precisely because a pending root has never
//!   signed anything: no leaf chains to it, so dropping it from the bundle
//!   costs nothing. It also *un*-anchors the discarded root, which is what
//!   makes any copy of its key on another disk worthless rather than a
//!   silently root-equivalent leftover.
//! - **The bundle is multi-rooted with no pending root**: activation already
//!   committed. Promote the staged key locally if this daemon has not yet done
//!   so ([`promote_staged_if_activated`]) and report done.
//!
//! # Trust before signature
//!
//! Every replica verifies its peers against the bundle in its own
//! `[tls] ca_path`, and a replica that cannot verify the leader cannot dial
//! it — so it cannot renew, and renewal is the only thing that would otherwise
//! hand it the new anchors. A rotation that switched signing before the fleet
//! trusted the incoming root would therefore not merely be *slow* to converge;
//! it would be **stuck**, with the route to the new anchors running through a
//! connection the missing anchors forbid. The phase order is what rules that
//! out: the anchors are recorded at stage time, every voter adopts them during
//! distribution, and signing moves last. Adopting anchors is a local write of
//! replicated state ([`adopt_anchors`]): no signature, no dial, no leader,
//! which is exactly why it can happen first. Nodes outside the voter set
//! (learners, agents) reach the same state on their own, without dialing
//! anyone, from [`crate::tasks::renewal`]'s re-root fast path.
//!
//! Bundle order is load-bearing in exactly one direction: position 0 is the
//! **active signing root**, because [`coppice_tls::pki::load_ca_key`] and
//! `CaSigner::load` pair a key against the bundle's first certificate.
//! Verification is order-independent; issuance is not.
//!
//! # What this module deliberately does not build
//!
//! No new replication machinery, and no second custody ledger. Distributing
//! the staged key reuses the shipped `TransferCaKey` RPC — the same one a
//! promotion uses to key a candidate — which routes an incoming key by *which
//! recorded root it matches*, so the staged key lands on the staged path with
//! no change to the RPC surface or its authorization. The staged-custody
//! commands mirror the shape of the live ones (`RecordKeyTransferIntent` /
//! `ConfirmKeyPossession`) rather than reinventing it, and at activation their
//! entries **merge into** the live maps: those disks hold the active root's
//! key from that instant, and `key_holders` must say so.

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};

use coppice_consensus::{Consensus, NodeHandle};
use coppice_core::time::Timestamp;
use coppice_state::command::{
    ConfirmStagedKeyPossession, RecordCaCertificate, RecordStagedKeyTransferIntent,
};
use coppice_state::{CaCertBundle, Command, StagedRoot};
use coppice_tls::{pki, TlsStore};

use coppice_proto::pb::raft::v1 as pb;

/// How long to keep retrying a per-peer key push while the peer catches up to
/// the bundle the rotation just recorded. A push that arrives before the
/// recipient has applied the new `RecordCaCertificate` is refused by its own
/// custody check (the key will match neither root it believes in), which is
/// correct — it just means "not yet", so this waits rather than failing the
/// rotation on a replication lag measured in milliseconds.
const PEER_PUSH_DEADLINE: Duration = Duration::from_secs(30);

/// Gap between per-peer push attempts within [`PEER_PUSH_DEADLINE`].
const PEER_PUSH_RETRY: Duration = Duration::from_secs(1);

/// The basename the outgoing CA key is preserved under, plus a microsecond
/// stamp. Rotation never destroys the key it replaces: an operator who
/// discovers mid-rotation that they rotated the wrong cluster, or that the new
/// key's disk is the compromised one, needs the outgoing key to still exist
/// (it is still a trust anchor until `complete`). It is exactly as
/// root-equivalent as it was before the rotation, and the runbook says so.
const KEY_BACKUP_PREFIX: &str = "ca.key.superseded-";

/// The machine-readable marker on a `begin` that staged and distributed but
/// **refused to activate** because some current voter has no replicated
/// staged-key confirmation.
///
/// A marker rather than prose because this is the one refusal an operator
/// automates around: it is not an error in the rotation, it is the rotation
/// correctly declining to break its own invariant, and the fix
/// (`replace-voter` or `remove-node` for the unreachable voter, then re-run
/// `begin`) is scriptable. [`BeginReport::missing_voters`] carries the list.
pub const COVERAGE_INCOMPLETE: &str = "ROTATION_COVERAGE_INCOMPLETE";

/// The machine-readable marker on a `begin` that cannot even stage, because
/// this leader cannot read the CA key it is supposed to be rotating away from.
pub const KEY_UNAVAILABLE: &str = "ROTATION_KEY_UNAVAILABLE";

/// A `TransferCaKey` recipient could not durably persist the key it was
/// handed (its own disk refused the write). Carried as the message prefix of
/// the `Internal` status the recipient answers with, so the pushing leader can
/// tell *this* — a fact about the recipient's disk, which no retry will
/// change — apart from the transport and h2 failures tonic also surfaces as
/// `Internal`, which are "not yet" and are retried to the push deadline.
pub const KEY_PERSIST_FAILED: &str = "ROTATION_KEY_PERSIST_FAILED";

// ---------------------------------------------------------------------------
// Test-only crash injection
// ---------------------------------------------------------------------------
//
// Each stages one of the crash windows this redesign exists to make survivable,
// deterministically. They abort the *verb* at an exact point; the integration
// test then kills the daemon, so what is exercised is the real "leader died
// here" state and not an approximation of it. Same fire-once, env-var-armed
// contract as `admin.rs`'s two — read that module's notes on why the latch is
// process-global before adding a test that arms one.

/// Abort `begin` immediately after the stage commit publishes, before any
/// distribution. Blocker 1's window: the bundle is two-rooted, the outgoing
/// root still signs, and only this leader holds the staged key.
pub const ROTATE_AFTER_STAGE_COMMIT: &str = "rotate-after-stage-commit";
static ROTATE_AFTER_STAGE_COMMIT_FIRED: AtomicBool = AtomicBool::new(false);

/// Abort `begin` after the first peer's staged-key confirmation publishes.
/// Proves the replicated acks survive a leader change, so a resume neither
/// re-mints nor re-pushes to the peer that already confirmed.
pub const ROTATE_AFTER_FIRST_DISTRIBUTION: &str = "rotate-after-first-distribution";
static ROTATE_AFTER_FIRST_DISTRIBUTION_FIRED: AtomicBool = AtomicBool::new(false);

/// Abort `begin` between the activation commit and the local signing swap —
/// the window that is only survivable *because* activation is gated on total
/// coverage: every voter already holds the key that is now active, so any
/// daemon can promote it on its own.
pub const ROTATE_AFTER_ACTIVATION_COMMIT: &str = "rotate-after-activation-commit";
static ROTATE_AFTER_ACTIVATION_COMMIT_FIRED: AtomicBool = AtomicBool::new(false);

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// Where a rotation has got to.
///
/// The first three values are facts about the **cluster** (they are computed
/// from replicated state alone, so every replica agrees). The last two are
/// facts about **this daemon**: whether the key on this disk is the private
/// half of the now-active root. That split is deliberate — activation is a
/// cluster event, promoting the staged key to the live path is a local one,
/// and the whole point of gating activation on total coverage is that the
/// local half can then be completed independently, by anyone, at any time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RotationPhase {
    /// A single-root bundle: no rotation in flight.
    None,
    /// A pending root is recorded and no peer has confirmed the staged key
    /// yet. The outgoing root still signs.
    Staged,
    /// A pending root is recorded and at least one peer has confirmed. Still
    /// the outgoing root that signs; [`RotationStatus::missing_voters`] says
    /// what activation is waiting for (empty means the next `begin` activates).
    Distributing,
    /// The incoming root is active in the recorded bundle, but **this
    /// daemon's** live key is not its private half yet. Self-healing: the
    /// renewal task promotes the staged key on its next pass.
    ActivePendingSwap,
    /// The incoming root is active and this daemon signs under it. `complete`
    /// is what remains, once its clock gate allows.
    CompleteEligible,
}

/// One root certificate in the recorded bundle, as an operator sees it.
///
/// Identified by its **subject key identifier** and **serial**, both rendered
/// as lowercase hex, because those are what `openssl x509 -noout -ext
/// subjectKeyIdentifier -serial` prints for a certificate file — so the
/// runbook's verification step is a string comparison against material on
/// disk, not a matter of trusting this command's own account of itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootInfo {
    /// Position in the bundle. `0` is the active signing root.
    pub position: usize,
    /// The certificate's subject key identifier, lowercase hex, or `null` when
    /// the certificate carries no such extension.
    pub subject_key_id: Option<String>,
    /// The certificate's serial number, lowercase hex.
    pub serial: String,
    /// The subject common name.
    pub common_name: Option<String>,
    /// Whether new leaves are signed under this root (position 0).
    pub active: bool,
    /// Whether this is the rotation's **pending** root: a trust anchor already,
    /// but not yet signing and not yet everywhere. Never true at position 0 —
    /// that is the invariant.
    pub pending: bool,
}

/// The rotation state of this cluster, as this daemon sees it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotationStatus {
    /// Where the rotation has got to. See [`RotationPhase`] on which values
    /// are cluster facts and which are local ones.
    pub phase: RotationPhase,
    /// Every root in the recorded bundle, in bundle order.
    pub roots: Vec<RootInfo>,
    /// When the current bundle was recorded (microseconds since the epoch).
    /// During a rotation this is the instant the last bundle-changing step
    /// committed — the clock the leaf-turnover wait is measured from.
    pub recorded_at_us: i64,
    /// True while the recorded bundle holds more than one root.
    pub rotation_in_progress: bool,
    /// The pending root's serial, while one is staged.
    pub staged_root_serial: Option<String>,
    /// Whether this daemon's on-disk `[tls] ca_path` bundle is the one the
    /// cluster replicates — i.e. whether it *trusts* what the cluster records.
    ///
    /// Anchors and leaves move independently: a rotation records the incoming
    /// root as an anchor at stage time and every replica adopts it from
    /// replicated state without dialing anyone, so this can be `true` on a
    /// replica still serving a leaf under the outgoing root.
    /// [`Self::leaf_under_active_root`] is the other half, and turnover means
    /// both.
    pub installed_matches_replicated: bool,
    /// Whether the leaf this daemon is *serving* chains to the recorded
    /// bundle's active (position 0) root.
    ///
    /// This is the half `rotate-ca complete` can strand: an un-renewed leaf
    /// stops authenticating the instant the outgoing root retires. It is
    /// computed from the live [`TlsStore`], not from the files behind it.
    pub leaf_under_active_root: bool,
    /// Whether this host's `ca.key` is the private half of the **active**
    /// (position 0) root — i.e. whether this host can sign new leaves.
    pub local_key_signs_active_root: bool,
    /// Whether this host's staged key file is the private half of the recorded
    /// **pending** root. During the staged phase every voter must reach `true`
    /// before activation is allowed.
    pub local_holds_staged_key: bool,
    /// The earliest instant `complete` will run without `--force`: one full
    /// leaf lifetime after `recorded_at_us`. `null` outside a rotation.
    pub earliest_complete_us: Option<i64>,
    /// The leaf lifetime that bound is derived from, in seconds.
    pub leaf_lifetime_secs: i64,
    /// The current voter set — the population the coverage gate is measured
    /// against, and the reason a rotation parked on a dead voter is fixed by a
    /// *membership* change.
    pub voters: Vec<u64>,
    /// Nodes with a replicated confirmation that they hold the **active** CA
    /// key — the root-equivalent disk count (ADR 0037 §4).
    pub key_holders: Vec<u64>,
    /// Unresolved key-transfer intents: disks that may hold the active key.
    pub pending_key_transfers: Vec<u64>,
    /// Nodes with a replicated confirmation that they durably hold the
    /// **staged** key. Root-equivalent too, and listed separately only because
    /// the root they are equivalent to is not yet the signing one: the pending
    /// root is already a trust anchor, so a leaf minted with this key already
    /// verifies fleet-wide.
    pub staged_key_holders: Vec<u64>,
    /// Unresolved staged-key transfer intents: disks that may hold it.
    pub staged_key_transfers: Vec<u64>,
    /// Current voters with no staged-key confirmation — exactly what
    /// activation is waiting on. Empty outside the staged phase.
    pub missing_voters: Vec<u64>,
    /// This daemon's raft id, and the leader it sees.
    pub local_id: u64,
    pub leader: Option<u64>,
}

/// The outcome of pushing the staged key to one peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerOutcome {
    pub node_id: u64,
    pub addr: String,
    /// `true` when the peer acknowledged a durable write **and** the
    /// confirmation replicated. Nothing short of both counts: the ack alone is
    /// knowledge held by one process, and the gate has to be readable by the
    /// next leader.
    pub installed: bool,
    /// `true` when this peer already held a replicated staged-key confirmation
    /// and nothing was pushed. The resume path's no-op.
    pub already_held: bool,
    /// The refusal, when `installed` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// What `rotate-ca begin` reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeginReport {
    pub status: RotationStatus,
    /// Where the outgoing CA key was preserved on this host. `None` until
    /// activation actually swaps the signing key — the staged phase does not
    /// touch it.
    pub key_backup: Option<String>,
    /// One entry per other current voter.
    pub distribution: Vec<PeerOutcome>,
    /// True when this call found a rotation already recorded rather than
    /// staging a fresh one.
    pub resumed: bool,
    /// True when this call minted a **replacement** pending root because the
    /// recorded one's key was not on this leader's disk. The previous pending
    /// root left the bundle in the same commit, so it is no longer an anchor.
    pub replaced_pending_root: bool,
    /// True when the incoming root is now the active signing root.
    pub activated: bool,
    /// Current voters with no staged-key confirmation. Non-empty exactly when
    /// `activated` is false for the coverage reason.
    pub missing_voters: Vec<u64>,
    /// [`COVERAGE_INCOMPLETE`] when activation was refused; `None` otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
}

/// What `rotate-ca complete` reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteReport {
    pub status: RotationStatus,
    /// The roots that are no longer trust anchors anywhere.
    pub retired: Vec<RootInfo>,
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Everything a rotation verb needs from the running daemon.
pub(crate) struct RotationContext<'a, C: Consensus> {
    pub(crate) consensus: &'a C,
    pub(crate) handle: &'a NodeHandle,
    /// This daemon's serving material, used to dial peers for the key push.
    pub(crate) tls: &'a TlsStore,
    pub(crate) data_dir: &'a Path,
}

impl<C: Consensus> RotationContext<'_, C> {
    /// The CA bundle the cluster currently replicates, and the pending root
    /// recorded alongside it.
    fn recorded_ca(&self) -> Result<(String, Timestamp, Option<StagedRoot>)> {
        let view = self.consensus.views().latest();
        let state = view.state();
        let staged = state.staged_root.clone();
        state
            .ca
            .as_ref()
            .map(|ca| (ca.bundle.pem().to_string(), ca.recorded_at, staged))
            .ok_or_else(|| {
                anyhow!(
                    "this cluster records no CA certificate, so there is no root to rotate. \
                     A cluster whose material was provisioned externally (the pre-ADR-0037 \
                     model) has no cluster-owned key and re-rooting does not apply to it."
                )
            })
    }

    /// The current voter set, sorted.
    fn voters(&self) -> Vec<u64> {
        let mut voters: Vec<u64> = self
            .handle
            .cluster_summary()
            .members
            .iter()
            .filter(|m| m.voter)
            .map(|m| m.id)
            .collect();
        voters.sort_unstable();
        voters.dedup();
        voters
    }

    /// Refuse anywhere but the leader.
    ///
    /// Not a mere convenience over the `NotLeader` a proposal would return: a
    /// rotation *must* run on the leader, because the same call both commits
    /// the bundle and writes the staged key to the disk it is running on, and
    /// only the leader can then drive the transfer protocol that puts that key
    /// on everyone else.
    fn require_leader(&self) -> Result<()> {
        let summary = self.handle.cluster_summary();
        match summary.leader {
            Some(leader) if leader == summary.local_id => Ok(()),
            Some(leader) => bail!(
                "this daemon (node {}) is not the leader; node {leader} is. Re-rooting stages \
                 the incoming CA key on the disk it runs on and drives the transfer that puts \
                 it on every other voter — run this on the leader's host.",
                summary.local_id
            ),
            None => bail!(
                "this cluster has no leader right now, so a re-root cannot be committed. \
                 Retry once `coppice coordinator admin status` reports one."
            ),
        }
    }

    /// Assemble the operator-facing status document.
    fn status(&self) -> Result<RotationStatus> {
        let (pem, recorded_at, staged) = self.recorded_ca()?;
        let roots = describe_roots(pem.as_bytes(), staged.as_ref())?;
        let rotation_in_progress = roots.len() > 1;
        let summary = self.handle.cluster_summary();
        let view = self.consensus.views().latest();
        let state = view.state();

        let installed = self.tls.current();
        let installed_matches_replicated =
            same_anchor_set(installed.ca_pem(), pem.as_bytes()).unwrap_or(false);
        let leaf_under_active_root = nth_cert_pem(pem.as_bytes(), 0)
            .map(|active| pki::verify_leaf(active.as_bytes(), installed.cert_pem()).is_ok())
            .unwrap_or(false);
        let local_key_signs_active_root = pki::load_ca_key(self.data_dir, pem.as_bytes()).is_ok();
        let local_holds_staged_key = staged
            .as_ref()
            .and_then(|s| pending_cert_pem(pem.as_bytes(), s).ok())
            .map(|cert| pki::load_staged_ca_key(self.data_dir, cert.as_bytes()).is_ok())
            .unwrap_or(false);

        let leaf_lifetime_secs = pki::LEAF_LIFETIME.whole_seconds();
        let earliest_complete_us =
            rotation_in_progress.then(|| recorded_at.as_micros() + leaf_lifetime_secs * 1_000_000);

        let voters = self.voters();
        let missing_voters = if staged.is_some() {
            state.staged_key_coverage_gap(&voters)
        } else {
            Vec::new()
        };

        let phase = match (&staged, roots.len()) {
            (_, 0 | 1) => RotationPhase::None,
            (Some(s), _) if s.holders.len() <= 1 => RotationPhase::Staged,
            (Some(_), _) => RotationPhase::Distributing,
            (None, _) if local_key_signs_active_root => RotationPhase::CompleteEligible,
            (None, _) => RotationPhase::ActivePendingSwap,
        };

        let sorted = |m: &std::collections::BTreeMap<u64, Timestamp>| {
            let mut v: Vec<u64> = m.keys().copied().collect();
            v.sort_unstable();
            v
        };

        Ok(RotationStatus {
            phase,
            roots,
            recorded_at_us: recorded_at.as_micros(),
            rotation_in_progress,
            staged_root_serial: staged.as_ref().map(|s| s.serial.clone()),
            installed_matches_replicated,
            leaf_under_active_root,
            local_key_signs_active_root,
            local_holds_staged_key,
            earliest_complete_us,
            leaf_lifetime_secs,
            voters,
            key_holders: sorted(&state.key_confirmations),
            pending_key_transfers: sorted(&state.key_transfer_intents),
            staged_key_holders: staged
                .as_ref()
                .map(|s| sorted(&s.holders))
                .unwrap_or_default(),
            staged_key_transfers: staged
                .as_ref()
                .map(|s| sorted(&s.intents))
                .unwrap_or_default(),
            missing_voters,
            local_id: summary.local_id,
            leader: summary.leader,
        })
    }

    /// Propose a command and wait for this daemon's own published view to
    /// carry it. Read-your-writes is not optional anywhere in a rotation: every
    /// step reasons about state the previous step committed.
    async fn commit(&self, command: Command, what: &str) -> Result<u64> {
        let applied = self
            .consensus
            .propose(command)
            .await
            .with_context(|| what.to_string())?;
        applied
            .outcome
            .map_err(|reason| anyhow!("{what} was rejected: {reason}"))?;
        self.consensus
            .views()
            .at_least(applied.log_index)
            .await
            .with_context(|| format!("waiting for {what} to publish"))?;
        Ok(applied.log_index)
    }
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

/// `rotate-ca status`: read-only, and answerable on any replica.
///
/// Deliberately not leader-gated. Several of the questions it answers are
/// *about this replica* — has it renewed onto the recorded bundle, does it
/// hold the staged key, can it sign — and those are exactly what an operator
/// walks the fleet asking during a rotation.
pub(crate) fn status<C: Consensus>(ctx: &RotationContext<'_, C>) -> Result<RotationStatus> {
    ctx.status()
}

// ---------------------------------------------------------------------------
// begin
// ---------------------------------------------------------------------------

/// `rotate-ca begin`: stage an incoming root, put its key on every current
/// voter, and — only once all of them hold it — make it the active one.
///
/// Idempotent in the way that matters operationally: re-running it resumes,
/// and the module doc's "Resuming" section is the whole decision table. The
/// operator's mental model is "run `begin` until it says activated", not
/// "reason about which half happened".
pub(crate) async fn begin<C: Consensus>(ctx: &RotationContext<'_, C>) -> Result<BeginReport> {
    ctx.require_leader()?;
    let (current_pem, _, staged) = ctx.recorded_ca()?;
    let current_roots = describe_roots(current_pem.as_bytes(), staged.as_ref())?;

    // --- Already activated: finish the local half and report done ---------
    //
    // A multi-rooted bundle with no pending root means the activation commit
    // landed. Nothing about the cluster is outstanding; only this disk might
    // be, and that is repaired idempotently (the renewal task does the same on
    // every replica, so this is belt-and-braces for an operator who re-runs
    // the verb rather than waiting).
    if current_roots.len() > 1 && staged.is_none() {
        let key_backup = promote_staged_if_activated(ctx.data_dir, current_pem.as_bytes(), None)
            .context("promoting this host's staged CA key after an activated rotation")?;
        adopt_anchors(ctx.tls, current_pem.as_bytes())
            .await
            .context("installing the recorded bundle in this host's own trust store")?;
        return Ok(BeginReport {
            status: ctx.status()?,
            key_backup: key_backup.map(|p| p.display().to_string()),
            distribution: Vec::new(),
            resumed: true,
            replaced_pending_root: false,
            activated: true,
            missing_voters: Vec::new(),
            refusal: None,
        });
    }

    // --- Phase 1: stage (or resume/replace an existing staging) -----------
    let (staged_serial, staged_key_pem, resumed, replaced) =
        stage_or_resume(ctx, &current_pem, staged.as_ref()).await?;

    if fires(ROTATE_AFTER_STAGE_COMMIT, &ROTATE_AFTER_STAGE_COMMIT_FIRED) {
        bail!("rotate-ca aborted at the {ROTATE_AFTER_STAGE_COMMIT} failpoint (test-only)");
    }

    // --- Phase 2: distribute, recording each receipt in replicated state ---
    let distribution = distribute_staged_key(ctx, &staged_serial, &staged_key_pem).await?;

    // --- Phase 3: activate, gated on TOTAL coverage -----------------------
    //
    // Re-read the gap from the published view rather than tallying the loop
    // above: the loop's own confirmations are in there, and so is anything a
    // concurrent promotion keyed. The voter set is re-read for the same
    // reason — an operator who removed the dead voter between two `begin`
    // runs must see the gate close.
    let voters = ctx.voters();
    let missing = ctx
        .consensus
        .views()
        .latest()
        .state()
        .staged_key_coverage_gap(&voters);
    if !missing.is_empty() {
        tracing::warn!(
            missing = ?missing,
            "rotate-ca: refusing to activate — some current voters do not hold the staged key \
             (ADR 0037 §4)"
        );
        return Ok(BeginReport {
            status: ctx.status()?,
            key_backup: None,
            distribution,
            resumed,
            replaced_pending_root: replaced,
            activated: false,
            missing_voters: missing,
            refusal: Some(COVERAGE_INCOMPLETE.to_string()),
        });
    }

    // Re-read the recorded bundle rather than reusing the one this call opened
    // with: staging committed a new one (and a resume that replaced the
    // pending root committed a different one again), so `current_pem` is the
    // *pre-stage* bundle and does not carry the root about to be activated.
    let (staged_pem, _, _) = ctx.recorded_ca()?;
    let activated_pem = activate(ctx, &staged_pem, &staged_serial).await?;

    if fires(
        ROTATE_AFTER_ACTIVATION_COMMIT,
        &ROTATE_AFTER_ACTIVATION_COMMIT_FIRED,
    ) {
        bail!("rotate-ca aborted at the {ROTATE_AFTER_ACTIVATION_COMMIT} failpoint (test-only)");
    }

    // --- Phase 3b: the local half of activation ---------------------------
    let key_backup = promote_staged_if_activated(ctx.data_dir, activated_pem.as_bytes(), None)
        .context("promoting this host's staged CA key to the live signing path")?;
    adopt_anchors(ctx.tls, activated_pem.as_bytes())
        .await
        .context("installing the activated bundle in this host's own trust store")?;
    tracing::info!("rotate-ca: the incoming root is active and this host signs under it");

    Ok(BeginReport {
        status: ctx.status()?,
        key_backup: key_backup.map(|p| p.display().to_string()),
        distribution,
        resumed,
        replaced_pending_root: replaced,
        activated: true,
        missing_voters: Vec::new(),
        refusal: None,
    })
}

/// Phase 1. Returns `(staged root serial, staged key PEM, resumed, replaced)`.
///
/// The three cases are the module doc's "Resuming" table. What unifies them is
/// that the staged key is on **this** disk before the function returns, and
/// the bundle the cluster records names the root it belongs to — in that
/// order, always, so a crash between the two leaves a disk holding a key for a
/// root nobody trusts, which is inert.
async fn stage_or_resume<C: Consensus>(
    ctx: &RotationContext<'_, C>,
    current_pem: &str,
    staged: Option<&StagedRoot>,
) -> Result<(String, Vec<u8>, bool, bool)> {
    if let Some(staged) = staged {
        let pending = pending_cert_pem(current_pem.as_bytes(), staged)?;
        if let Ok(key_pem) = pki::load_staged_ca_key(ctx.data_dir, pending.as_bytes()) {
            tracing::info!(
                serial = %staged.serial,
                "rotate-ca: resuming the recorded staging; this leader holds the staged key"
            );
            adopt_anchors(ctx.tls, current_pem.as_bytes())
                .await
                .context("installing the recorded bundle in this host's own trust store")?;
            self_confirm_staged(ctx, &staged.serial).await?;
            return Ok((staged.serial.clone(), key_pem, true, false));
        }

        // The staged key is not here. Replace the pending root rather than
        // refuse: a pending root has never signed anything, so nothing chains
        // to it, and the replacement bundle drops it — which un-anchors it and
        // makes any copy of its key on any disk worthless. Refusing instead
        // would park the cluster on a rotation only a dead machine could
        // finish.
        tracing::warn!(
            superseded = %staged.serial,
            "rotate-ca: the recorded pending root's key is not on this leader's disk; minting a \
             replacement pending root (the superseded one leaves the bundle, so it is no longer \
             a trust anchor)"
        );
        let outgoing = nth_cert_pem(current_pem.as_bytes(), 0)?;
        let (serial, key_pem) = mint_and_stage(ctx, &outgoing).await?;
        return Ok((serial, key_pem, true, true));
    }

    // Fresh rotation. Refuse before minting anything if this leader cannot
    // read the key it is rotating away from: it is the one disk that must be
    // able to sign throughout the staged phase, and a rotation begun from a
    // host that cannot sign is a rotation with no signing host at all.
    if let Err(e) = pki::load_ca_key(ctx.data_dir, current_pem.as_bytes()) {
        bail!(
            "{KEY_UNAVAILABLE}: this leader cannot read the CA key of the root it would rotate \
             away from: {e}. The outgoing root keeps signing for the whole staged phase, so a \
             rotation cannot be begun here. Repair custody on this node's data directory, or \
             run `rotate-ca begin` on a voter whose `rotate-ca status` reports \
             `local_key_signs_active_root`."
        );
    }
    let (serial, key_pem) = mint_and_stage(ctx, current_pem).await?;
    Ok((serial, key_pem, false, false))
}

/// Mint an incoming root, put it on this disk, and only then record it as the
/// bundle's pending entry.
///
/// `keep_pem` is the bundle the pending root is appended to — the outgoing
/// root alone, so a replacement staging drops the superseded pending root in
/// the same commit that adds its successor.
async fn mint_and_stage<C: Consensus>(
    ctx: &RotationContext<'_, C>,
    keep_pem: &str,
) -> Result<(String, Vec<u8>)> {
    let minted = pki::mint_root_ca().context("minting the incoming cluster root CA")?;
    let new_cert = String::from_utf8(minted.cert_pem.clone())
        .context("the minted root certificate is not UTF-8")?;

    // Disk before log. This is the ordering the whole redesign is about: the
    // key exists durably, owner-only, on a voter's disk *before* the cluster
    // records that the root it belongs to is anything at all.
    pki::stage_ca_material(ctx.data_dir, &minted.cert_pem, &minted.key_pem)
        .context("staging the incoming CA key and certificate on this host's disk")?;

    let bundle_pem = format!(
        "{}{}",
        ensure_trailing_newline(keep_pem),
        ensure_trailing_newline(&new_cert)
    );
    let bundle = CaCertBundle::parse(bundle_pem.clone())
        .map_err(|e| anyhow!("the staged bundle this rotation assembled is invalid: {e}"))?;
    let serial = bundle
        .serials()
        .last()
        .cloned()
        .ok_or_else(|| anyhow!("the staged bundle carries no certificate serial"))?;

    let log_index = ctx
        .commit(
            Command::RecordCaCertificate(RecordCaCertificate {
                bundle,
                staged_root_serial: Some(serial.clone()),
                recorded_at: Timestamp::now(),
            }),
            "recording the staged dual-root CA bundle",
        )
        .await?;
    tracing::info!(
        log_index,
        serial = %serial,
        "rotate-ca: staged bundle recorded; both roots are trust anchors and the OUTGOING root \
         still signs"
    );

    // Trust the incoming root here before anyone else is asked to. Adopting
    // the bundle is a local write of replicated state — no signature, no dial,
    // no leader.
    adopt_anchors(ctx.tls, bundle_pem.as_bytes())
        .await
        .context("installing the staged bundle in this host's own trust store")?;

    self_confirm_staged(ctx, &serial).await?;
    Ok((serial, minted.key_pem))
}

/// Record this leader's own staged-key possession.
///
/// The leader is a voter and therefore part of the coverage set, so its own
/// possession has to be a *replicated* fact like everyone else's — otherwise
/// the gate could never close on a single-voter cluster, and a resume on a
/// different leader would misread the coverage of this one.
async fn self_confirm_staged<C: Consensus>(
    ctx: &RotationContext<'_, C>,
    serial: &str,
) -> Result<()> {
    let local = ctx.handle.cluster_summary().local_id;
    if ctx
        .consensus
        .views()
        .latest()
        .state()
        .has_staged_key_confirmation(local)
    {
        return Ok(());
    }
    ctx.commit(
        Command::ConfirmStagedKeyPossession(ConfirmStagedKeyPossession {
            raft_node_id: local,
            root_serial: serial.to_string(),
            confirmed_at: Timestamp::now(),
        }),
        "recording this leader's own staged-key possession",
    )
    .await?;
    Ok(())
}

/// Phase 3. Commit the reordered bundle `[incoming (active), outgoing…]` and
/// return its PEM.
///
/// `staged_root_serial: None` on this commit is what tells apply that the
/// staged root has been *promoted* rather than abandoned: it sees the staged
/// serial arrive at position 0 and merges the staged custody entries into the
/// live ones, because those disks now hold the active root's key and
/// `key_holders` must say so.
async fn activate<C: Consensus>(
    ctx: &RotationContext<'_, C>,
    current_pem: &str,
    staged_serial: &str,
) -> Result<String> {
    let blocks = cert_blocks(current_pem.as_bytes())?;
    let serials = CaCertBundle::parse(current_pem.to_string())
        .map_err(|e| anyhow!("the recorded bundle is invalid: {e}"))?
        .serials();
    let position = serials
        .iter()
        .position(|s| s == staged_serial)
        .ok_or_else(|| {
            anyhow!("the recorded bundle no longer carries the staged root {staged_serial}")
        })?;

    let mut ordered = vec![blocks[position].clone()];
    ordered.extend(
        blocks
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != position)
            .map(|(_, b)| b.clone()),
    );
    let bundle_pem: String = ordered.iter().map(|b| ensure_trailing_newline(b)).collect();
    let bundle = CaCertBundle::parse(bundle_pem.clone())
        .map_err(|e| anyhow!("the activated bundle this rotation assembled is invalid: {e}"))?;

    let log_index = ctx
        .commit(
            Command::RecordCaCertificate(RecordCaCertificate {
                bundle,
                staged_root_serial: None,
                recorded_at: Timestamp::now(),
            }),
            "recording the activated CA bundle",
        )
        .await?;
    tracing::info!(
        log_index,
        serial = %staged_serial,
        "rotate-ca: the incoming root is now the active signing root; every current voter holds \
         its key"
    );
    Ok(bundle_pem)
}

/// Push the staged key to every other current voter, recording each confirmed
/// receipt in replicated state.
///
/// The recipient set is **exactly the current voters**: they are the set the
/// activation gate is measured against, and a rotation is not an occasion to
/// widen custody to anyone else.
///
/// A per-peer failure is reported, not fatal — but unlike the previous design
/// it is also not *forgiven*: a peer with no confirmation keeps the coverage
/// gap open and activation does not happen. The honest operator surface is a
/// per-peer account they can act on, plus a rotation that stays parked in a
/// state where the old root still signs.
async fn distribute_staged_key<C: Consensus>(
    ctx: &RotationContext<'_, C>,
    serial: &str,
    key_pem: &[u8],
) -> Result<Vec<PeerOutcome>> {
    let summary = ctx.handle.cluster_summary();
    let mut outcomes = Vec::new();
    let mut pushed_any = false;

    for member in summary
        .members
        .iter()
        .filter(|m| m.voter && m.id != summary.local_id)
    {
        if ctx
            .consensus
            .views()
            .latest()
            .state()
            .has_staged_key_confirmation(member.id)
        {
            outcomes.push(PeerOutcome {
                node_id: member.id,
                addr: member.addr.clone(),
                installed: true,
                already_held: true,
                error: None,
            });
            continue;
        }

        // Intent before bytes (ADR 0037 §4): from this entry on, whatever
        // crashes, the peer stays visible to custody accounting as a possible
        // holder of a root-equivalent key. The pending root is already a trust
        // anchor, so "possible holder" is not hypothetical authority.
        if let Err(e) = ctx
            .commit(
                Command::RecordStagedKeyTransferIntent(RecordStagedKeyTransferIntent {
                    raft_node_id: member.id,
                    root_serial: serial.to_string(),
                    intended_at: Timestamp::now(),
                }),
                "recording the staged-key transfer intent",
            )
            .await
        {
            outcomes.push(PeerOutcome {
                node_id: member.id,
                addr: member.addr.clone(),
                installed: false,
                already_held: false,
                error: Some(format!("{e:#}")),
            });
            continue;
        }

        let result = push_key_to(ctx, &member.addr, key_pem).await;
        match result {
            Ok(()) => {
                match ctx
                    .commit(
                        Command::ConfirmStagedKeyPossession(ConfirmStagedKeyPossession {
                            raft_node_id: member.id,
                            root_serial: serial.to_string(),
                            confirmed_at: Timestamp::now(),
                        }),
                        "recording a peer's staged-key possession",
                    )
                    .await
                {
                    Ok(_) => {
                        tracing::info!(
                            node = member.id,
                            "rotate-ca: peer durably holds the staged CA key"
                        );
                        outcomes.push(PeerOutcome {
                            node_id: member.id,
                            addr: member.addr.clone(),
                            installed: true,
                            already_held: false,
                            error: None,
                        });
                        pushed_any = true;
                    }
                    Err(e) => outcomes.push(PeerOutcome {
                        node_id: member.id,
                        addr: member.addr.clone(),
                        installed: false,
                        already_held: false,
                        error: Some(format!("{e:#}")),
                    }),
                }
            }
            Err(e) => {
                tracing::error!(
                    node = member.id,
                    error = %format!("{e:#}"),
                    "rotate-ca: peer did NOT take the staged CA key; the incoming root cannot \
                     be activated until it does (ADR 0037 §4)"
                );
                outcomes.push(PeerOutcome {
                    node_id: member.id,
                    addr: member.addr.clone(),
                    installed: false,
                    already_held: false,
                    error: Some(format!("{e:#}")),
                });
            }
        }

        if pushed_any
            && fires(
                ROTATE_AFTER_FIRST_DISTRIBUTION,
                &ROTATE_AFTER_FIRST_DISTRIBUTION_FIRED,
            )
        {
            bail!(
                "rotate-ca aborted at the {ROTATE_AFTER_FIRST_DISTRIBUTION} failpoint (test-only)"
            );
        }
    }
    Ok(outcomes)
}

/// One peer's push, retried **only while the peer is merely behind**.
///
/// The RPC is the shipped `TransferCaKey`. The recipient routes the key by
/// which recorded root it matches — active root to the live path, pending root
/// to the staged path — so the staged transfer needs no new verb, no new
/// authorization rule, and no second code path on the receiving side.
///
/// The retry classification matters more here than it did before this became a
/// *gate*. Two failures wear the same shape from the outside and must not be
/// treated alike:
///
/// - **The peer has not applied the new bundle yet.** It refuses because the
///   key it was handed matches neither root it currently believes in, or the
///   dial fails while it restarts. That is "not yet", measured in
///   milliseconds, and waiting is right.
/// - **The peer applied the bundle and could not keep the key** — a full disk,
///   a permissions failure, anything its custody write reports. Waiting cannot
///   help, and retrying would be worse than useless: it turns a durable
///   refusal into a `PEER_PUSH_DEADLINE`-long stall, and — since a later
///   attempt might succeed for an unrelated reason — it can convert a voter
///   that genuinely failed to persist the key into an apparent success. The
///   coverage gate is only as honest as this distinction.
///
/// So a server-side `Internal`/`DataLoss`/`ResourceExhausted` (the recipient
/// speaking about its own disk, which it marks with [`KEY_PERSIST_FAILED`])
/// returns immediately. Everything else — a transport failure, `Unavailable`,
/// the `InvalidArgument` a lagging peer answers with, and crucially any
/// *unmarked* `Internal` (tonic reports h2/TLS/connection hiccups under that
/// code too) — is a "not yet" and is retried to the deadline. Classifying by
/// gRPC code alone turned one transient blip on a loaded host into a missing
/// acknowledgement, a correctly refused activation, and a rotation parked for
/// no reason the operator could see.
async fn push_key_to<C: Consensus>(
    ctx: &RotationContext<'_, C>,
    addr: &str,
    key_pem: &[u8],
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + PEER_PUSH_DEADLINE;
    let history_id = ctx.handle.history_id().to_vec();
    loop {
        let material = ctx.tls.current();
        let attempt = async {
            let mut client = crate::admin::admin_channel(
                addr,
                material.ca_pem(),
                material.cert_pem(),
                material.key_pem(),
            )
            .await
            .map_err(|e| (false, e))?;
            client
                .transfer_ca_key(pb::TransferCaKeyRequest {
                    history_id: history_id.clone(),
                    ca_key_pem: key_pem.to_vec(),
                })
                .await
                .map_err(|s| {
                    let durable = crate::admin::has_marker(s.message(), KEY_PERSIST_FAILED);
                    (durable, anyhow!("{}: {}", s.code(), s.message()))
                })?;
            Ok::<(), (bool, anyhow::Error)>(())
        }
        .await;

        match attempt {
            Ok(()) => return Ok(()),
            Err((durable_refusal, e)) => {
                if durable_refusal || tokio::time::Instant::now() >= deadline {
                    return Err(e);
                }
                tokio::time::sleep(PEER_PUSH_RETRY).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The local half of activation, everywhere
// ---------------------------------------------------------------------------

/// Promote this host's staged key to the live signing path when the recorded
/// bundle says the staged root is now the active one — idempotently, from
/// anywhere.
///
/// This is the completeness half of the redesign. Activation is one commit
/// followed by one local file swap, and a crash between them must not need the
/// crashed machine to come back: the coverage gate guarantees every voter
/// already holds the key that just became active, so **any** daemon can finish
/// its own half by itself. [`crate::tasks::renewal`] calls this on every pass,
/// which means every replica repairs itself at startup and within one
/// re-evaluate interval thereafter; `begin` calls it too so an operator who
/// re-runs the verb does not have to wait for a timer.
///
/// Returns the path the outgoing key was preserved at, when a promotion
/// happened. Does nothing (and returns `Ok(None)`) when a rotation is still
/// staged, when this host already signs the active root, or when this host has
/// no staged key — the three ordinary cases.
///
/// `staged` is the recorded pending root, if any. Passing `Some` short-circuits
/// to a no-op: a staged rotation has not been activated, so there is nothing to
/// promote.
pub fn promote_staged_if_activated(
    data_dir: &Path,
    ca_bundle_pem: &[u8],
    staged: Option<&StagedRoot>,
) -> Result<Option<PathBuf>> {
    if staged.is_some() {
        return Ok(None);
    }
    // Already signing the active root. Any staged leftovers are a superseded
    // pending root's key: root-equivalent bytes with no accounting entry, so
    // they go.
    if pki::load_ca_key(data_dir, ca_bundle_pem).is_ok() {
        pki::discard_staged_ca_material(data_dir)
            .context("discarding this host's superseded staged CA material")?;
        return Ok(None);
    }
    let active = nth_cert_pem(ca_bundle_pem, 0)?;
    let Ok(staged_key) = pki::load_staged_ca_key(data_dir, active.as_bytes()) else {
        // No staged key for the active root. Ordinary on a replica that was
        // never keyed, and the honest answer on one that was not covered.
        return Ok(None);
    };

    let backup = preserve_outgoing_key(data_dir)?;
    pki::write_ca_key(data_dir, &staged_key).with_context(|| {
        format!(
            "promoting the staged CA key to the live path in {}",
            data_dir.display()
        )
    })?;
    pki::discard_staged_ca_material(data_dir)
        .context("discarding this host's staged CA material after promoting it")?;
    tracing::info!(
        backup = ?backup.as_ref().map(|p| p.display().to_string()),
        "rotate-ca: promoted the staged CA key to this host's live signing path (ADR 0037 §4)"
    );
    Ok(backup)
}

/// Copy the live `ca.key` aside before it is replaced.
///
/// The backup is written and fsynced *before* the live key is overwritten:
/// rotation never destroys the key it replaces. The outgoing root is still a
/// trust anchor until `complete`, so an operator who discovers mid-rotation
/// that they rotated the wrong thing needs that key to still exist. Returns
/// `None` when there was no live key to preserve, which is the case on a
/// replica that only ever held the staged one.
fn preserve_outgoing_key(data_dir: &Path) -> Result<Option<PathBuf>> {
    let live = data_dir.join(pki::CA_KEY_FILE);
    let outgoing = match std::fs::read(&live) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", live.display())),
    };
    let backup = data_dir.join(format!(
        "{KEY_BACKUP_PREFIX}{}",
        Timestamp::now().as_micros()
    ));
    write_private(&backup, &outgoing)
        .with_context(|| format!("preserving the outgoing CA key at {}", backup.display()))?;
    Ok(Some(backup))
}

/// Owner-only write. Mirrors `pki::write_ca_key`'s posture for the backup copy,
/// which is every bit as root-equivalent as the file it copies.
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Trust anchors
// ---------------------------------------------------------------------------

/// How many times [`adopt_anchors`] re-reads before giving up, and the gap
/// between attempts. The store skips a swap whose read straddled a concurrent
/// write (`TlsStore::force_reload` returns `Ok(false)` on a torn read), and a
/// renewal installing a leaf beside these bytes is exactly such a writer — so
/// "adopted" is asserted by re-reading the store, not by the write returning.
const ADOPT_ATTEMPTS: usize = 5;
const ADOPT_RETRY: Duration = Duration::from_millis(50);

/// Make `bundle_pem` this host's on-disk trust anchor set and confirm the live
/// [`TlsStore`] serves it.
///
/// The replicated bundle *is* the cluster's anchor set, so adopting it needs
/// no signature, no dial and no leader — which is what lets a replica repair
/// its own trust before it has any way to reach a peer that already signs
/// under the incoming root. Only the anchors move: the leaf and key are left
/// alone, so a replica whose leaf is still under the outgoing root keeps
/// serving it (the outgoing root is a trust anchor for the whole window) while
/// its own view of who to trust moves ahead.
///
/// Returns whether anything changed. Confirmation is the point: the callers in
/// [`begin`] use the return to sequence a rotation, so "the bytes were written"
/// is not a strong enough postcondition — the store must be serving them.
pub(crate) async fn adopt_anchors(tls: &TlsStore, bundle_pem: &[u8]) -> Result<bool> {
    if same_anchor_set(tls.current().ca_pem(), bundle_pem).unwrap_or(false) {
        return Ok(false);
    }
    pki::install_ca_bundle(tls.paths(), bundle_pem)
        .with_context(|| format!("writing the trust anchors {}", tls.paths().ca.display()))?;
    for attempt in 0..ADOPT_ATTEMPTS {
        tls.force_reload()
            .context("reloading TLS material after installing new trust anchors")?;
        if same_anchor_set(tls.current().ca_pem(), bundle_pem).unwrap_or(false) {
            tracing::info!(
                ca_path = %tls.paths().ca.display(),
                "trust anchors: adopted the bundle the cluster records (ADR 0037 §4)"
            );
            return Ok(true);
        }
        if attempt + 1 < ADOPT_ATTEMPTS {
            tokio::time::sleep(ADOPT_RETRY).await;
        }
    }
    bail!(
        "the trust anchors were written to {} but this daemon's TLS store is still serving a \
         different anchor set; a concurrent writer is racing the reload",
        tls.paths().ca.display()
    )
}

// ---------------------------------------------------------------------------
// complete
// ---------------------------------------------------------------------------

/// `rotate-ca complete`: drop the outgoing root, ending the dual-trust window.
///
/// The gate is a clock, and it is the honest one. There is no cluster-wide
/// observable for "no leaf signed by the outgoing root remains in use": an
/// agent that has not dialed in for a week still holds a valid leaf, and
/// nothing replicated records which root any given leaf was signed under. What
/// *is* known exactly is that every leaf the outgoing root ever signed was
/// signed before the activation commit, and no leaf outlives
/// [`pki::LEAF_LIFETIME`] — so one full leaf lifetime after that commit there
/// can be no unexpired leaf under the outgoing root, whatever any node was
/// doing in the meantime.
///
/// `--force` exists because that bound is conservative by a wide margin in the
/// common case (a fleet whose renewal fast path has already moved everything
/// onto the new root within minutes), and because refusing to let an operator
/// respond to an active compromise on their own judgement would be the wrong
/// default for a compromise-response verb. It is not a shortcut past
/// verification — it is the verb an operator runs *after* verifying, and the
/// runbook says what verifying means.
pub(crate) async fn complete<C: Consensus>(
    ctx: &RotationContext<'_, C>,
    force: bool,
) -> Result<CompleteReport> {
    ctx.require_leader()?;
    let (current_pem, recorded_at, staged) = ctx.recorded_ca()?;
    let roots = describe_roots(current_pem.as_bytes(), staged.as_ref())?;

    if roots.len() < 2 {
        bail!(
            "no re-root is in progress: the recorded bundle holds a single root, which is \
             already the only trust anchor. Run `rotate-ca begin` first."
        );
    }
    // A staged rotation has not activated: position 0 is still the OUTGOING
    // root. Completing here would retire the *incoming* root and leave the
    // compromised one as the sole anchor — the exact inverse of the operator's
    // intent, and the reason this check is on the pending marker rather than
    // on the root count.
    if let Some(staged) = &staged {
        bail!(
            "this rotation is still staged: the recorded bundle's active root is the OUTGOING \
             one and {} is only pending, so completing now would retire the incoming root and \
             leave the outgoing one as the cluster's sole trust anchor. Run `rotate-ca begin` \
             until it reports the rotation activated — `rotate-ca status` names any voter still \
             missing the staged key.",
            staged.serial
        );
    }
    if pki::load_ca_key(ctx.data_dir, current_pem.as_bytes()).is_err() {
        bail!(
            "this host's ca.key is not the private half of the active root, so completing here \
             would leave the cluster with a sole trust anchor this leader cannot sign under. \
             If the rotation activated very recently this is transient and needs no action: \
             every voter holds the incoming key (activation is gated on exactly that), and \
             each promotes it to its live signing path on its next renewal pass — one \
             `[pacing] renewal_reevaluate_interval`, 15s by default. Re-check with \
             `rotate-ca status`: this host is ready when it reports \
             `signing key signs the active root`. If it stays this way, custody on this data \
             directory needs repair."
        );
    }

    let leaf_lifetime_us = pki::LEAF_LIFETIME.whole_seconds() * 1_000_000;
    let earliest = recorded_at.as_micros() + leaf_lifetime_us;
    let now = Timestamp::now().as_micros();
    if now < earliest && !force {
        let remaining = Duration::from_micros((earliest - now).max(0) as u64);
        bail!(
            "the dual-trust window has {} left to run. Every leaf the outgoing root signed was \
             signed before this rotation began, and no leaf outlives {}, so after that point \
             none can remain — completing now can strand any node that has not yet renewed \
             (an agent that cannot verify the cluster retries forever; it does not re-enrol). \
             Verify turnover as the re-rooting runbook describes and re-run with --force to \
             complete early.",
            humantime_serde::re::humantime::format_duration(Duration::from_secs(
                remaining.as_secs()
            )),
            humantime_serde::re::humantime::format_duration(Duration::from_secs(
                pki::LEAF_LIFETIME.whole_seconds().max(0) as u64
            )),
        );
    }

    let active_pem = nth_cert_pem(current_pem.as_bytes(), 0)?;
    let bundle = CaCertBundle::parse(active_pem.clone())
        .map_err(|e| anyhow!("the single-root bundle this rotation assembled is invalid: {e}"))?;
    let log_index = ctx
        .commit(
            Command::RecordCaCertificate(RecordCaCertificate {
                bundle,
                staged_root_serial: None,
                recorded_at: Timestamp::now(),
            }),
            "recording the completed single-root CA bundle",
        )
        .await?;
    tracing::info!(
        log_index,
        "rotate-ca: the outgoing root is no longer a trust anchor"
    );
    // Make the retirement true *here* before reporting it, rather than leaving
    // this host trusting the retired root until its own renewal loop next
    // looks: `complete` is the verb an operator runs to end a compromise, and
    // "the outgoing root is no longer trusted" must be a fact about the daemon
    // answering the call, not a promise about its next timer tick.
    adopt_anchors(ctx.tls, active_pem.as_bytes())
        .await
        .context("installing the completed single-root bundle in this host's own trust store")?;

    Ok(CompleteReport {
        status: ctx.status()?,
        retired: roots.into_iter().skip(1).collect(),
    })
}

// ---------------------------------------------------------------------------
// Certificate inspection
// ---------------------------------------------------------------------------

/// Describe every certificate in a bundle, in bundle order.
fn describe_roots(pem: &[u8], staged: Option<&StagedRoot>) -> Result<Vec<RootInfo>> {
    let ders = cert_ders(pem)?;
    ders.iter()
        .enumerate()
        .map(|(position, der)| {
            let (_, cert) = x509_parser::parse_x509_certificate(der.as_slice())
                .map_err(|e| anyhow!("parsing CA certificate {position}: {e}"))?;
            let common_name = cert
                .subject()
                .iter_common_name()
                .next()
                .and_then(|cn| cn.as_str().ok())
                .map(|s| s.to_string());
            let subject_key_id = cert
                .get_extension_unique(
                    &x509_parser::oid_registry::OID_X509_EXT_SUBJECT_KEY_IDENTIFIER,
                )
                .ok()
                .flatten()
                .and_then(|ext| match ext.parsed_extension() {
                    x509_parser::extensions::ParsedExtension::SubjectKeyIdentifier(ski) => {
                        Some(crate::formation::hex(ski.0))
                    }
                    _ => None,
                });
            let serial = crate::formation::hex(cert.raw_serial());
            let pending = staged.is_some_and(|s| s.serial == serial);
            Ok(RootInfo {
                position,
                subject_key_id,
                serial,
                common_name,
                active: position == 0,
                pending,
            })
        })
        .collect()
}

/// The DER of every certificate in a PEM bundle.
fn cert_ders(pem: &[u8]) -> Result<Vec<Vec<u8>>> {
    let ders = rustls_pemfile::certs(&mut std::io::Cursor::new(pem))
        .map(|der| der.map(|d| d.to_vec()))
        .collect::<Result<Vec<_>, _>>()
        .context("parsing the CA bundle")?;
    if ders.is_empty() {
        bail!("the recorded CA bundle contains no certificates");
    }
    Ok(ders)
}

/// The recorded bundle's pending root as a standalone PEM block.
fn pending_cert_pem(pem: &[u8], staged: &StagedRoot) -> Result<String> {
    pending_cert_pem_for(pem, &staged.serial)
}

/// The certificate of `pem` carrying `serial`, as a standalone PEM block.
///
/// `pub(crate)` for [`crate::admin`]'s `TransferCaKey` handler, which has to
/// answer "is this pushed key the private half of the root the cluster
/// currently stages?" — and the only honest form of that question is against
/// the certificate the cluster records, never against anything the pusher sent.
pub(crate) fn pending_cert_pem_for(pem: &[u8], serial: &str) -> Result<String> {
    let bundle = CaCertBundle::parse(
        std::str::from_utf8(pem)
            .context("CA bundle is not UTF-8")?
            .to_string(),
    )
    .map_err(|e| anyhow!("the recorded CA bundle is invalid: {e}"))?;
    let position = bundle
        .serials()
        .iter()
        .position(|s| s == serial)
        .ok_or_else(|| {
            anyhow!("the recorded bundle carries no certificate with serial {serial}")
        })?;
    nth_cert_pem(pem, position)
}

/// Every `BEGIN`…`END CERTIFICATE` block of a bundle, verbatim and standalone.
fn cert_blocks(pem: &[u8]) -> Result<Vec<String>> {
    let text = std::str::from_utf8(pem).context("CA bundle is not UTF-8")?;
    let mut blocks: Vec<String> = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed == "-----BEGIN CERTIFICATE-----" {
            current = Some(String::new());
        }
        if let Some(block) = current.as_mut() {
            block.push_str(trimmed);
            block.push('\n');
        }
        if trimmed == "-----END CERTIFICATE-----" {
            if let Some(block) = current.take() {
                blocks.push(block);
            }
        }
    }
    if blocks.is_empty() {
        bail!("the recorded CA bundle contains no certificates");
    }
    Ok(blocks)
}

/// The `n`th certificate of a bundle as a standalone PEM block.
///
/// The original block text, verbatim: the bytes the cluster already validated
/// and recorded, so reordering or completing a rotation cannot perturb the very
/// anchor it is keeping. [`CaCertBundle::parse`] re-validates it regardless.
pub(crate) fn nth_cert_pem(pem: &[u8], n: usize) -> Result<String> {
    cert_blocks(pem)?
        .into_iter()
        .nth(n)
        .ok_or_else(|| anyhow!("the recorded CA bundle has no certificate at position {n}"))
}

/// Whether two PEM bundles carry the same set of certificates, order aside.
fn same_anchor_set(a: &[u8], b: &[u8]) -> Result<bool> {
    let mut a = cert_ders(a)?;
    let mut b = cert_ders(b)?;
    a.sort();
    b.sort();
    Ok(a == b)
}

/// PEM blocks concatenate only if the first ends in a newline.
fn ensure_trailing_newline(pem: &str) -> String {
    if pem.ends_with('\n') {
        pem.to_string()
    } else {
        format!("{pem}\n")
    }
}

/// Consult a fire-once, env-var-armed test failpoint. Always `false` in a real
/// deployment; see [`crate::admin::failpoint_fires`].
fn fires(name: &str, latch: &AtomicBool) -> bool {
    crate::admin::failpoint_fires(name, latch)
}
