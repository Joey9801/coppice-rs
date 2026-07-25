//! Explicit cluster formation and the daemon phase it publishes (ADR 0037 §3).
//!
//! Formation is the one privileged ceremony in a cluster's lifetime, and its
//! authority is **local**: it is not a network RPC, it is a verb on the
//! daemon's own Unix socket ([`crate::localadmin`]). This module owns the
//! seven steps that verb executes, the marker semantics that make a partial
//! formation identifiable, and [`PhaseState`] — the small shared cell every
//! surface reads to decide what it may answer.
//!
//! # The marker, and why everything hangs off it
//!
//! Step 2 stamps a **formation intent** into the manifest; step 7 stamps the
//! **`formation_complete` marker**. Between them the daemon's external
//! surface stays closed: `ProbeCluster` does not report `initialized`, no
//! client API is served, and membership verbs are refused. That closure is
//! what confines a failed formation to the one node that attempted it — no
//! peer can have joined a cluster that never announced itself — which in turn
//! is what makes "wipe exactly one data directory and re-run `init`" a
//! complete recovery.
//!
//! A crash anywhere in the seven steps therefore restarts into
//! [`Phase::Failed`]: intent without marker, fail-stop, no resume path. That
//! is deliberate (ADR 0037 §3): a resumable formation state machine was
//! considered and rejected — it existed only to auto-heal a rare window in a
//! restartable one-time operation.

use std::path::Path;
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, bail, Context, Result};

use coppice_api::http::{ReadyzPhase, ReadyzReport, ReadyzVoter};
use coppice_consensus::fs::RealFs;
use coppice_consensus::storage::{self, StorageOptions};
use coppice_consensus::{
    Consensus, NodeHandle, OpenraftConsensus, StartIntent, StartedNode, StateViews,
};
use coppice_core::id::{ClusterId, MachineId};
use coppice_core::time::Timestamp;
use coppice_state::command::{BindMachineIdentity, RecordCaCertificate};
use coppice_state::{CaCertBundle, Command};
use coppice_tls::pki;
use coppice_tls::{TlsPaths, TlsStore};

use crate::config::Config;
use crate::policy::FormationPolicy;

/// The common name the day-0 operator certificate carries when the operator
/// does not choose one (ADR 0022's operator profile supplies the `OU`).
pub(crate) const DEFAULT_OPERATOR_CN: &str = "day0-operator";

// ---------------------------------------------------------------------------
// Phase
// ---------------------------------------------------------------------------

/// What this daemon is currently able to serve (ADR 0037 §1).
enum Phase {
    /// No manifest, no cluster: the admin socket and `/readyz` are up and
    /// nothing else is. Left only by a local `init` (and, from chunk 05, by
    /// an initialized cluster appearing in discovery).
    Waiting,
    /// This directory records a formation intent with no completion marker.
    /// Fail-stop; the intent's timestamp is the diagnostic.
    Failed { intent_at_us: i64 },
    /// A formed cluster: everything is served.
    Formed(Formed),
}

/// The pieces of a formed replica the status surfaces read.
#[derive(Clone)]
struct Formed {
    handle: NodeHandle,
    views: StateViews,
}

/// The daemon's phase, shared by every surface that must answer differently
/// before and after formation: `/readyz`, `ProbeCluster`, the admin socket,
/// and the membership verbs.
///
/// A plain `RwLock` rather than a watch channel: readers want the current
/// value synchronously inside request handlers, the write happens at most
/// twice in a process's life, and no reader awaits while holding it.
///
/// Deliberately no stored history id: the stamp travels with the
/// [`NodeHandle`] once formed, and an unformed daemon has none to report —
/// formation mints a fresh history per cluster lifetime (ADR 0037 §3), so
/// config-derived values must never masquerade as one.
pub(crate) struct PhaseState {
    cluster_id: ClusterId,
    cluster_size: usize,
    /// How stale a leader's quorum acknowledgment may be before this replica
    /// stops reporting itself ready (ADR 0037 §9). Derived from the election
    /// timeout: past it, a partitioned follower would have started an
    /// election of its own.
    contact_staleness: std::time::Duration,
    phase: RwLock<Phase>,
}

impl PhaseState {
    /// A daemon that has not formed: parked, or fail-stopped.
    pub(crate) fn unformed(
        cluster_id: ClusterId,
        cluster_size: usize,
        contact_staleness: std::time::Duration,
        marks: storage::FormationMarks,
    ) -> Arc<PhaseState> {
        let phase = match marks.intent_at_us {
            Some(intent_at_us) if marks.complete_at_us.is_none() => Phase::Failed { intent_at_us },
            _ => Phase::Waiting,
        };
        Arc::new(PhaseState {
            cluster_id,
            cluster_size,
            contact_staleness,
            phase: RwLock::new(phase),
        })
    }

    /// Publish a formed replica. Every surface that was refusing starts
    /// answering from the next read.
    pub(crate) fn publish_formed(&self, handle: NodeHandle, views: StateViews) {
        *self.phase.write().expect("phase lock") = Phase::Formed(Formed { handle, views });
    }

    /// Publish the fail-stop. Reached when a live `init` attempt dies after
    /// stamping its intent: the directory is already in the failed state, so
    /// the running daemon says so rather than pretending it is still parked
    /// and inviting a retry that cannot succeed.
    pub(crate) fn publish_failed(&self, intent_at_us: i64) {
        *self.phase.write().expect("phase lock") = Phase::Failed { intent_at_us };
    }

    /// Whether this daemon is serving a formed cluster — the single predicate
    /// behind "the external surface stays closed until the marker exists".
    pub(crate) fn is_formed(&self) -> bool {
        matches!(&*self.phase.read().expect("phase lock"), Phase::Formed(_))
    }

    /// The formation-intent timestamp when this daemon is fail-stopped.
    pub(crate) fn failed_at(&self) -> Option<i64> {
        match &*self.phase.read().expect("phase lock") {
            Phase::Failed { intent_at_us } => Some(*intent_at_us),
            _ => None,
        }
    }

    pub(crate) fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    /// The current `/readyz` body (ADR 0037 §9).
    pub(crate) fn readyz(&self) -> ReadyzReport {
        let cluster = self.cluster_id.to_string();
        let formed = match &*self.phase.read().expect("phase lock") {
            Phase::Waiting => {
                return ReadyzReport::unformed(
                    cluster,
                    ReadyzPhase::Waiting,
                    self.cluster_size,
                    Some(
                        "no cluster formed on this node and none found: run \
                         `coppice coordinator init` on one daemon to form the cluster \
                         (ADR 0037 §3)"
                            .to_string(),
                    ),
                )
            }
            Phase::Failed { intent_at_us } => {
                return ReadyzReport::unformed(
                    cluster,
                    ReadyzPhase::FormationFailed,
                    self.cluster_size,
                    Some(failed_diagnostic(*intent_at_us)),
                )
            }
            Phase::Formed(formed) => formed.clone(),
        };

        let summary = formed.handle.cluster_summary();
        let applied_index = formed.views.latest().applied_index();
        let is_voter = summary
            .members
            .iter()
            .any(|m| m.id == summary.local_id && m.voter);
        let is_leader = summary.leader == Some(summary.local_id);
        let voter_count = summary.members.iter().filter(|m| m.voter).count();
        // Contact with the cluster (ADR 0037 §9): local lag alone cannot see
        // a partition, because a cut-off replica's applied and known-committed
        // indexes freeze together and its lag reads zero forever. A leader
        // proves contact through a fresh quorum acknowledgment (openraft's
        // lease metric); a follower proves it by still knowing a leader — a
        // partitioned voter's election timeout fires, it becomes a candidate,
        // and the leader it knew is gone with the old term. A sole voter is
        // its own quorum and has no one to lose contact with.
        let leader_contact_stale = if voter_count <= 1 {
            false
        } else if is_leader {
            !summary
                .millis_since_quorum_ack
                .is_some_and(|ms| std::time::Duration::from_millis(ms) <= self.contact_staleness)
        } else {
            summary.leader.is_none()
        };
        let mut voters: Vec<ReadyzVoter> = summary
            .members
            .iter()
            .filter(|m| m.voter)
            .map(|m| ReadyzVoter {
                node_id: m.id,
                address: m.addr.clone(),
            })
            .collect();
        voters.sort_by_key(|v| v.node_id);

        let reason = if leader_contact_stale {
            Some(
                "this replica has lost contact with its cluster: its local indexes are \
                 current only against a frozen frontier (ADR 0037 §9)"
                    .to_string(),
            )
        } else {
            (!is_voter).then(|| "this replica is not a voter".to_string())
        };
        ReadyzReport {
            cluster_id: cluster,
            history_id: Some(hex(&formed.handle.history_id())),
            node_id: Some(summary.local_id),
            instance_uuid: Some(hex(&formed.handle.instance_uuid())),
            // `joining` belongs to the convergence loop (chunk 05); a formed
            // replica here is a voter, or a learner if membership says so.
            phase: if is_voter {
                ReadyzPhase::Voter
            } else {
                ReadyzPhase::Learner
            },
            leader: summary.leader,
            is_leader,
            applied_index,
            committed_index: summary.known_committed,
            replication_lag: summary.known_committed.saturating_sub(applied_index),
            leader_contact_stale,
            voters,
            cluster_size: self.cluster_size,
            reason,
        }
    }

    /// The current `ProbeCluster` answer (ADR 0037 §3).
    ///
    /// `initialized` is true only in [`Phase::Formed`]: a parked, forming, or
    /// failed daemon is discoverable but not joinable.
    pub(crate) fn probe(&self) -> ProbeAnswer {
        match &*self.phase.read().expect("phase lock") {
            Phase::Waiting | Phase::Failed { .. } => ProbeAnswer {
                cluster_id: self.cluster_id.to_string(),
                history_id: Vec::new(),
                initialized: false,
                node_id: None,
                leader_hint: None,
                voters: Vec::new(),
            },
            Phase::Formed(formed) => {
                let summary = formed.handle.cluster_summary();
                let mut voters: Vec<(u64, String)> = summary
                    .members
                    .iter()
                    .filter(|m| m.voter)
                    .map(|m| (m.id, m.addr.clone()))
                    .collect();
                voters.sort_by_key(|(id, _)| *id);
                ProbeAnswer {
                    cluster_id: self.cluster_id.to_string(),
                    history_id: formed.handle.history_id().to_vec(),
                    initialized: true,
                    node_id: Some(summary.local_id),
                    leader_hint: summary.leader,
                    voters,
                }
            }
        }
    }
}

/// A daemon's answer to `ProbeCluster`, in domain terms.
pub(crate) struct ProbeAnswer {
    pub(crate) cluster_id: String,
    pub(crate) history_id: Vec<u8>,
    pub(crate) initialized: bool,
    pub(crate) node_id: Option<u64>,
    pub(crate) leader_hint: Option<u64>,
    pub(crate) voters: Vec<(u64, String)>,
}

/// The operator-facing message for a fail-stopped daemon: what happened, and
/// the one documented recovery.
pub(crate) fn failed_diagnostic(intent_at_us: i64) -> String {
    let when = Timestamp::from_micros(intent_at_us)
        .map(|t| t.to_string())
        .unwrap_or_else(|| format!("{intent_at_us}us"));
    format!(
        "formation started at {when} never completed: the data directory records a formation \
         intent with no formation_complete marker, so this node's cluster is partial and is \
         never resumed (ADR 0037 §3). No peer can have joined it — the external surface stayed \
         closed. Recovery: stop the daemon, wipe the data directory, restart (it parks), and \
         re-run `coppice coordinator init`."
    )
}

// ---------------------------------------------------------------------------
// Startup: what the data directory says
// ---------------------------------------------------------------------------

/// What a data directory's manifest says about how this daemon must start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartupState {
    /// No manifest: a new instance. With no `--bootstrap`/`--join` it parks
    /// and waits for `init`.
    Empty,
    /// A manifest with no dangling formation intent: resume the instance,
    /// under the raft history the stamp records.
    Resume {
        /// The stamped history id. For a directory formation created this is
        /// the value `init` minted — the stamp, not config, is the authority,
        /// because a re-formed cluster keeps its `cluster_id` but carries a
        /// new history (ADR 0037 §3).
        history_id: [u8; 16],
    },
    /// A formation intent with no completion marker: fail-stop.
    FormationFailed { intent_at_us: i64 },
}

/// Read the data directory's identity stamp and classify the startup
/// (ADR 0037 §1).
///
/// Deliberately does not open the store: a directory in the failed state must
/// never have consensus brought up on it.
pub(crate) fn inspect(data_dir: &Path) -> Result<(StartupState, storage::FormationMarks)> {
    let fs = RealFs::new(data_dir.to_path_buf());
    let stamp = storage::read_manifest_stamp(&fs)
        .with_context(|| format!("reading the manifest in {}", data_dir.display()))?;
    Ok(match stamp {
        None => (StartupState::Empty, storage::FormationMarks::default()),
        Some(stamp) if stamp.formation.failed() => (
            StartupState::FormationFailed {
                intent_at_us: stamp.formation.intent_at_us.unwrap_or_default(),
            },
            stamp.formation,
        ),
        Some(stamp) => (
            StartupState::Resume {
                history_id: stamp.history_id,
            },
            stamp.formation,
        ),
    })
}

/// The history a resumed directory serves, cross-checked against config
/// exactly as far as the stamp allows (ADR 0016 / ADR 0037 §3).
///
/// A **legacy** directory (no formation markers — stamped by the
/// `--bootstrap`/`--join` flags) recorded the config `cluster_id`'s bytes as
/// its history, so config-vs-stamp is a real wrong-volume check and a
/// mismatch fail-stops here with the ADR 0016 message. A **formed** directory
/// carries a minted history that config cannot know; the stamp is the
/// authority, and the cross-cluster protection lives where the ADR puts it —
/// peers compare stamped history ids on every RPC, and the convergence loop
/// (chunk 05) fail-stops a resumed replica whose history disagrees with the
/// cluster its discovery finds.
pub(crate) fn resumed_history(
    cfg: &Config,
    stamped: [u8; 16],
    marks: storage::FormationMarks,
) -> Result<[u8; 16]> {
    let config_bytes = *cfg.cluster_id.0.as_bytes();
    if marks.complete_at_us.is_none() && stamped != config_bytes {
        bail!(
            "the data directory {} is stamped for history {} but this config's cluster_id \
             derives {} — wrong volume or cross-cluster mixup; refusing to start (ADR 0016)",
            cfg.data_dir.display(),
            hex(&stamped),
            hex(&config_bytes),
        );
    }
    Ok(stamped)
}

// ---------------------------------------------------------------------------
// Formation
// ---------------------------------------------------------------------------

/// Everything `init` needs that the daemon already has (or can rebuild).
pub(crate) struct FormationContext {
    pub(crate) config: Config,
    pub(crate) advertise_addr: String,
    /// The hot-reload store, when the daemon had TLS material at startup.
    /// `None` is the genuinely fresh installation (ADR 0037 §4: a minimal
    /// deployment provisions no certificates at all) — formation mints the
    /// first material and loads the store from the files it just wrote.
    pub(crate) tls: Option<Arc<TlsStore>>,
    /// Test-only crash injection; `None` in every real daemon.
    pub(crate) failpoint: Option<Failpoint>,
}

/// What the operator asked `init` to do beyond forming.
#[derive(Debug, Default)]
pub(crate) struct FormRequest {
    /// Bootstrap policy TOML (ADR 0020's `cluster init --policy`).
    pub(crate) policy: Option<Vec<u8>>,
    /// An operator CSR to sign; absent means mint the keypair locally and
    /// print both halves.
    pub(crate) operator_csr: Option<Vec<u8>>,
    /// The common name the operator certificate carries.
    pub(crate) operator_cn: Option<String>,
}

/// The day-0 operator credential `init` prints (ADR 0037 §3 step 5).
#[derive(Debug, Clone)]
pub(crate) struct OperatorCredential {
    pub(crate) cert_pem: String,
    /// Present only on the no-CSR path, where the cluster minted the keypair.
    pub(crate) key_pem: Option<String>,
    pub(crate) ca_pem: String,
}

/// A completed formation: the running replica, and what to print.
pub(crate) struct Formation {
    pub(crate) started: StartedNode,
    pub(crate) operator: OperatorCredential,
    pub(crate) machine: MachineId,
    /// The store now serving the cluster-minted material — the one the
    /// daemon's remaining listeners must be built over. Freshly created here
    /// when the daemon started certless.
    pub(crate) tls_store: Arc<TlsStore>,
}

/// Where a test may abort formation, to prove the marker semantics.
///
/// The two interesting points are either side of `raft.initialize`, because
/// that is the boundary a resumable design would have had to reason about —
/// and the marker makes both sides identical: `formation-failed`. The daemon
/// never sets this; it exists so an in-crate test can drive [`form`] to each
/// boundary without staging the on-disk state by hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum Failpoint {
    /// Abort after the intent is stamped, before the raft history exists.
    BeforeRaftInitialize,
    /// Abort after the raft history exists, before the marker.
    AfterRaftInitialize,
}

/// Run the seven steps of ADR 0037 §3 on a parked daemon.
///
/// The order is the ADR's and is load-bearing: the intent is stamped before
/// anything else durable (step 2), the replicated writes happen only after
/// `raft.initialize` (step 4→6), and the marker is last (step 7) — after the
/// operator certificate and the bootstrap policy, so that a formation missing
/// its day-0 state is identified as failed rather than mistaken for healthy.
pub(crate) async fn form(ctx: FormationContext, req: FormRequest) -> Result<Formation> {
    let FormationContext {
        config: cfg,
        advertise_addr,
        tls,
        failpoint,
    } = ctx;
    let cfg = &cfg;
    let data_dir = cfg.data_dir.clone();

    // Everything that can be rejected on its own merits is rejected before
    // the first durable act. A malformed policy file is an operator typo, and
    // a typo must not cost a data directory: past step 2 the only exit is
    // wipe-and-retry.
    //
    // Parsing alone is not enough for that promise. A syntactically valid
    // policy can still describe a quota hierarchy with a dangling parent or a
    // cycle, and `commands` only discovers that when it topologically orders
    // the entities. Formation always starts from an empty state machine, so
    // that ordering is fully decidable now — run it against the default state
    // and discard the result, purely to fail here rather than at step 6.
    let policy = req
        .policy
        .as_deref()
        .map(FormationPolicy::parse_toml)
        .transpose()
        .context("parsing the bootstrap policy")?;
    if let Some(policy) = &policy {
        policy
            .commands(&coppice_state::StateMachine::default(), Timestamp::now())
            .context("validating the bootstrap policy")?;
    }

    // --- Step 1: one round of discovery + probe. -------------------------
    refuse_if_cluster_exists(cfg, &advertise_addr).await?;

    // --- Step 2: mint and stamp identity, recording the intent. ----------
    //
    // The history id is MINTED here, not derived from config (ADR 0037 §3):
    // `cluster_id` is the operator-chosen logical name and survives a
    // wipe-and-re-form, while the history id names one raft history and must
    // not — a fresh id per formation is exactly what lets volumes from a
    // previous history be told apart from the new one instead of merging
    // into it.
    let history_id = *uuid::Uuid::new_v4().as_bytes();
    let now = Timestamp::now();
    let fs = RealFs::new(data_dir.clone());
    let node_id = storage::init_forming(&fs, &StorageOptions::new(history_id), now.as_micros())
        .with_context(|| {
            format!(
                "stamping a formation intent into the data directory {}",
                data_dir.display()
            )
        })?;
    tracing::info!(
        node_id,
        history_id = %hex(&history_id),
        "formation: minted history, stamped node identity and formation intent"
    );

    // --- Step 3: the cluster root CA and this node's own coordinator leaf.
    // The key file lands in the data directory (owner-only) and never enters
    // replicated state; the leaf lands in the `[tls]` paths, so the hot-reload
    // store picks it up without a restart (ADR 0037 §4).
    let ca = pki::mint_root_ca().context("minting the cluster root CA")?;
    pki::write_ca_key(&data_dir, &ca.key_pem).context("writing the cluster CA key")?;
    let signer = pki::CaSigner::load(&ca.cert_pem, &ca.key_pem)
        .context("loading the freshly minted cluster CA for signing")?;

    let machine = pki::mint_machine_identity();
    pki::persist_machine_identity(&data_dir, &machine)
        .context("persisting this coordinator's machine identity")?;

    let (leaf_cert, leaf_key) = pki::mint_coordinator_local(&signer, &machine, &leaf_sans(cfg))
        .context("issuing this coordinator's own leaf certificate")?;
    let tls_paths = TlsPaths {
        cert: cfg.tls.cert_path.clone(),
        key: cfg.tls.key_path.clone(),
        ca: cfg.tls.ca_path.clone(),
    };
    pki::install_leaf_material(&tls_paths, &ca.cert_pem, &leaf_cert, &leaf_key)
        .context("installing the cluster-minted leaf into the configured [tls] paths")?;
    // The daemon may have started with no TLS material at all (the ADR's
    // minimal deployment): the material now exists, so the store does too.
    let tls_store = match tls {
        Some(store) => {
            store
                .force_reload()
                .context("reloading TLS material after installing the cluster-minted leaf")?;
            store
        }
        None => TlsStore::load(tls_paths)
            .context("loading the TLS store from the freshly minted material")?,
    };
    tracing::info!(%machine, "formation: minted cluster CA and this node's coordinator leaf");

    if failpoint == Some(Failpoint::BeforeRaftInitialize) {
        bail!("formation aborted at the BeforeRaftInitialize failpoint (test-only)");
    }

    // --- Step 4: open the freshly stamped directory and create the single
    // -- voter cluster. `Restart` is correct here: step 2 already stamped the
    // -- manifest, so this is a resume of a directory this process just made.
    let options = crate::bootstrap::node_options(
        cfg,
        history_id,
        advertise_addr.clone(),
        Arc::clone(&tls_store),
    );
    let started = coppice_consensus::start(options, StartIntent::Restart)
        .await
        .context("starting consensus on the freshly stamped data directory")?;
    started
        .handle
        .initialize_single_voter(advertise_addr.clone())
        .await
        .context("creating the single-voter cluster")?;
    tracing::info!(node_id, "formation: raft history initialized");

    // From here on the replica is live, so every remaining failure must stop
    // it before it propagates: the daemon's answer to a failed formation is to
    // serve the closed surface until an operator wipes the directory, and a
    // raft core still ticking and writing into that directory in the meantime
    // is not what "fail-stop" should mean.
    let rest = finish_formation(FinishInputs {
        advertise_addr: &advertise_addr,
        failpoint,
        req: &req,
        started: &started,
        ca: &ca,
        signer: &signer,
        machine,
        node_id,
        policy,
    })
    .await;
    match rest {
        Ok(operator) => Ok(Formation {
            started,
            operator,
            machine,
            tls_store,
        }),
        Err(e) => {
            if let Err(stop) = started.handle.shutdown().await {
                tracing::warn!(error = %stop, "formation: the aborted replica did not shut down cleanly");
            }
            Err(e)
        }
    }
}

/// Steps 5–7, plus the replicated half of step 3 — everything that happens
/// once the raft history exists. Split out so [`form`] has one place to stop
/// the replica when any of it fails.
struct FinishInputs<'a> {
    advertise_addr: &'a str,
    failpoint: Option<Failpoint>,
    req: &'a FormRequest,
    started: &'a StartedNode,
    ca: &'a pki::CaMaterial,
    signer: &'a pki::CaSigner,
    machine: MachineId,
    node_id: u64,
    policy: Option<FormationPolicy>,
}

async fn finish_formation(inputs: FinishInputs<'_>) -> Result<OperatorCredential> {
    let FinishInputs {
        advertise_addr,
        failpoint,
        req,
        started,
        ca,
        signer,
        machine,
        node_id,
        policy,
    } = inputs;
    if failpoint == Some(Failpoint::AfterRaftInitialize) {
        bail!("formation aborted at the AfterRaftInitialize failpoint (test-only)");
    }

    // --- Step 3 (replicated half): the CA certificate is public material
    // -- every node needs, and this node's machine identity binds to its raft
    // -- seat. Both wait for the history to exist, hence their position here.
    let bundle = CaCertBundle::parse(
        std::str::from_utf8(&ca.cert_pem).context("cluster CA certificate is not UTF-8")?,
    )
    .map_err(|e| anyhow!("the minted cluster CA is not a valid CA bundle: {e}"))?;
    let recorded_at = Timestamp::now();
    crate::policy::propose_all(
        &started.consensus,
        vec![
            Command::RecordCaCertificate(RecordCaCertificate {
                bundle,
                recorded_at,
            }),
            Command::BindMachineIdentity(BindMachineIdentity {
                machine,
                raft_node_id: node_id,
                address: advertise_addr.to_string(),
                bound_at: recorded_at,
            }),
        ],
    )
    .await
    .context("recording the cluster CA and this node's machine identity")?;

    // --- Step 5: the day-0 operator certificate. -------------------------
    let operator = issue_operator_credential(signer, &ca.cert_pem, req)?;

    // --- Step 6: the bootstrap policy (idempotent puts). -----------------
    //
    // Enrollment tokens are the other thing a `--policy` file will carry
    // (ADR 0037 §5). The replicated command exists, but the token schema and
    // its "print the secret exactly once" semantics belong with the
    // enrollment endpoint, so `[[enroll_token]]` is deliberately not accepted
    // yet: `FormationPolicy` rejects unknown tables, which means a file
    // written for it fails loudly here rather than being silently ignored.
    if let Some(policy) = policy {
        let commands = {
            let view = started.views.latest();
            policy.commands(view.state(), Timestamp::now())?
        };
        let count = commands.len();
        crate::policy::propose_all(&started.consensus, commands)
            .await
            .context("applying the bootstrap policy")?;
        tracing::info!(commands = count, "formation: bootstrap policy applied");
    }

    // --- Step 7: the marker. Formation has happened only now. ------------
    started
        .formation
        .mark_formation_complete(Timestamp::now().as_micros())
        .context("stamping the formation_complete marker")?;
    tracing::info!(node_id, "formation: complete");

    Ok(operator)
}

/// Sign (or mint) the operator certificate `init` prints (ADR 0037 §3 step 5).
///
/// Also the whole body of `issue-operator-cert`, whose only difference is
/// that it loads the CA from disk instead of having just minted it — which is
/// what makes it a day-0 recovery for a lost `init` output.
pub(crate) fn issue_operator_credential(
    signer: &pki::CaSigner,
    ca_pem: &[u8],
    req: &FormRequest,
) -> Result<OperatorCredential> {
    let cn = req.operator_cn.as_deref().unwrap_or(DEFAULT_OPERATOR_CN);
    let ca_pem = String::from_utf8(ca_pem.to_vec()).context("cluster CA bundle is not UTF-8")?;

    let (cert_pem, key_pem) = match &req.operator_csr {
        Some(csr) => (
            pki::issue_operator(signer, csr, cn).context("signing the operator CSR")?,
            None,
        ),
        None => {
            let (cert, key) =
                pki::mint_operator_local(signer, cn).context("minting an operator keypair")?;
            (cert, Some(key))
        }
    };

    Ok(OperatorCredential {
        cert_pem: String::from_utf8(cert_pem)
            .context("issued operator certificate is not UTF-8")?,
        key_pem: key_pem
            .map(|k| String::from_utf8(k).context("minted operator key is not UTF-8"))
            .transpose()?,
        ca_pem,
    })
}

/// Load the cluster CA for signing on a formed node: the certificate from
/// replicated state (public material every node has), the key from this
/// node's own disk (ADR 0037 §4 — it is never replicated).
pub(crate) fn load_cluster_ca(
    data_dir: &Path,
    consensus: &OpenraftConsensus,
) -> Result<(pki::CaSigner, Vec<u8>)> {
    let ca_pem = consensus
        .views()
        .latest()
        .state()
        .ca
        .as_ref()
        .map(|ca| ca.bundle.pem().to_string())
        .ok_or_else(|| {
            anyhow!(
                "this cluster has no cluster-owned CA recorded: it was not formed by \
                 `coppice coordinator init` (ADR 0037 §4), so there is no key here to sign with"
            )
        })?;
    let key_pem = pki::load_ca_key(data_dir, ca_pem.as_bytes()).with_context(|| {
        format!(
            "loading the cluster CA key from {} — signing runs on a voter, which is \
             the only disk that holds it (ADR 0037 §4)",
            data_dir.display()
        )
    })?;
    let signer = pki::CaSigner::load(ca_pem.as_bytes(), &key_pem)
        .context("loading the cluster CA for signing")?;
    Ok((signer, ca_pem.into_bytes()))
}

/// Step 1: refuse to form when a discovered candidate already reports an
/// initialized cluster with this `cluster_id`.
///
/// A guard against accidental double-init, not a safety proof (ADR 0037 §3):
/// unreachable candidates are skipped, because probing is a search for the
/// cluster, not a census. The daemon's own address is excluded — several
/// backends (`file` foremost) list this very process, and a daemon can never
/// be the existing cluster its own formation must not duplicate.
///
/// **The guard fails closed when it cannot run.** The probe plane is mTLS,
/// and the ADR's flow gives every prober credentials before it probes: a
/// certless daemon enrolls first (§1) and only then probes, so by the time a
/// mistaken `init` could reach a daemon in a fleet with a live cluster, that
/// daemon holds a leaf and the guard sees the cluster. Until enrollment
/// exists, a certless daemon whose discovery names candidates has no way to
/// ask them anything — and "cannot probe" must not be read as "no cluster
/// exists", because that silently disables a mandated formation step in
/// exactly the deployment (minimal, certless) it most protects. So it
/// refuses, naming both ways out; forming with an empty seed set remains the
/// legitimate certless path.
async fn refuse_if_cluster_exists(cfg: &Config, advertise_addr: &str) -> Result<()> {
    let mut candidates = crate::discovery::build(&cfg.discovery)
        .context("building the discovery backend for the pre-formation probe")?
        .candidates()
        .await;
    candidates.retain(|candidate| candidate != advertise_addr);
    if candidates.is_empty() {
        tracing::info!("formation: discovery found no candidates to probe");
        return Ok(());
    }

    let have_creds = [&cfg.tls.ca_path, &cfg.tls.cert_path, &cfg.tls.key_path]
        .iter()
        .all(|p| p.exists());
    if !have_creds {
        bail!(
            "refusing to form: discovery found {} candidate(s) ({}) but this daemon holds \
             no TLS material, so the pre-formation double-init guard cannot ask them \
             whether a cluster already exists (ADR 0037 §3 step 1). If this is genuinely \
             the first formation, run `init` with an empty discovery seed set (or \
             provision [tls] material and re-run); once enrollment lands, a parked daemon \
             acquires a leaf before formation and this guard runs as designed.",
            candidates.len(),
            candidates.join(", "),
        );
    }

    let probes = crate::probe::probe_all(cfg, &candidates).await?;
    for (target, answer) in probes {
        if answer.initialized && answer.cluster_id == cfg.cluster_id.to_string() {
            bail!(
                "refusing to form: {target} already reports an initialized cluster \
                 {} (history {}). Formation happens exactly once per cluster lifetime — \
                 this daemon should join the existing cluster, not create a second one \
                 (ADR 0037 §3).",
                answer.cluster_id,
                hex(&answer.history_id),
            );
        }
    }
    Ok(())
}

/// The SANs this node's own coordinator leaf carries: the address peers dial
/// it on. `advertise_host` is resolved by `config::load` before any reader.
fn leaf_sans(cfg: &Config) -> Vec<String> {
    let mut sans = Vec::new();
    if let Some(host) = cfg.listen.advertise_host.as_deref() {
        sans.push(host.to_string());
    }
    // Every deployment reaches its own node locally at some point (the admin
    // client's default target, a health probe); a leaf that cannot serve
    // `localhost` makes those need a second certificate.
    for extra in ["localhost", "127.0.0.1", "::1"] {
        if !sans.iter().any(|s| s == extra) {
            sans.push(extra.to_string());
        }
    }
    sans
}

/// Lowercase hex, for operator-facing identity strings.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_consensus::storage::FormationMarks;

    fn marks(intent: Option<i64>, complete: Option<i64>) -> FormationMarks {
        FormationMarks {
            intent_at_us: intent,
            complete_at_us: complete,
        }
    }

    fn state(marks: FormationMarks) -> Arc<PhaseState> {
        PhaseState::unformed(
            ClusterId::new(),
            3,
            std::time::Duration::from_secs(1),
            marks,
        )
    }

    #[test]
    fn intent_without_marker_is_the_failed_phase() {
        let s = state(marks(Some(1_000), None));
        assert_eq!(s.failed_at(), Some(1_000));
        assert_eq!(s.readyz().phase, ReadyzPhase::FormationFailed);
        assert!(!s.is_formed());
    }

    #[test]
    fn a_completed_formation_restarts_as_a_plain_instance() {
        // Both markers present: nothing to fail-stop on. (This directory then
        // resumes through the normal path, which publishes `Formed`.)
        let s = state(marks(Some(1_000), Some(2_000)));
        assert_eq!(s.failed_at(), None);
        assert_eq!(s.readyz().phase, ReadyzPhase::Waiting);
    }

    #[test]
    fn a_fresh_directory_parks() {
        let s = state(marks(None, None));
        assert_eq!(s.readyz().phase, ReadyzPhase::Waiting);
        assert!(s.readyz().reason.unwrap().contains("coordinator init"));
    }

    #[test]
    fn an_unformed_daemon_never_reports_initialized_to_a_prober() {
        for m in [marks(None, None), marks(Some(1), None)] {
            let answer = state(m).probe();
            assert!(!answer.initialized);
            assert!(answer.history_id.is_empty());
            assert!(answer.node_id.is_none());
        }
    }

    #[test]
    fn the_failed_diagnostic_names_the_recovery() {
        let msg = failed_diagnostic(Timestamp::now().as_micros());
        assert!(msg.contains("wipe the data directory"), "{msg}");
        assert!(msg.contains("coppice coordinator init"), "{msg}");
    }

    #[test]
    fn inspect_reads_an_absent_manifest_as_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (state, marks) = inspect(dir.path()).expect("inspect");
        assert_eq!(state, StartupState::Empty);
        assert_eq!(marks, FormationMarks::default());
    }

    // ---- `form` end to end, against a real data directory ----------------
    //
    // These reach past the daemon: they call the seven steps directly, so the
    // assertions can be about replicated state and the on-disk artifacts that
    // `tests/formation.rs` can only observe through a surface.

    /// A throwaway CA plus a leaf, the material a daemon starts with before
    /// the cluster mints its own.
    fn seed_tls(dir: &Path) -> TlsPaths {
        use rcgen::{CertificateParams, DnType, KeyPair};
        let mut ca_params = CertificateParams::new(vec![]).expect("ca params");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "seed-ca");
        let ca_key = KeyPair::generate().expect("ca key");
        let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign");

        let mut leaf_params =
            CertificateParams::new(vec!["localhost".to_string()]).expect("leaf params");
        leaf_params
            .distinguished_name
            .push(DnType::CommonName, "seed-leaf");
        leaf_params.use_authority_key_identifier_extension = true;
        leaf_params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let leaf_key = KeyPair::generate().expect("leaf key");
        let leaf = leaf_params
            .signed_by(&leaf_key, &ca_cert, &ca_key)
            .expect("sign leaf");

        let paths = TlsPaths {
            cert: dir.join("node.crt"),
            key: dir.join("node.key"),
            ca: dir.join("ca.crt"),
        };
        std::fs::write(&paths.cert, leaf.pem()).expect("write cert");
        std::fs::write(&paths.key, leaf_key.serialize_pem()).expect("write key");
        std::fs::write(&paths.ca, ca_cert.pem()).expect("write ca");
        paths
    }

    /// A config file for a node that will never bind anything: `form` opens
    /// storage and raft, but the listeners belong to the daemon.
    fn seed_config(root: &Path, cluster_id: ClusterId) -> Config {
        let paths = seed_tls(root);
        let data_dir = root.join("data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        let config_path = root.join("coordinator.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"cluster_id = "{cluster_id}"
data_dir = "{data_dir}"

[discovery]
backend = "static"
cluster_size = 1

[discovery.static]
addrs = []

[listen]
raft_addr = "127.0.0.1:0"
advertise_host = "localhost"

[raft]
election_timeout = "300ms"
heartbeat_interval = "100ms"

[tls]
cert_path = "{cert}"
key_path = "{key}"
ca_path = "{ca}"

[observability]
log_level = "warn"
"#,
                data_dir = data_dir.display(),
                cert = paths.cert.display(),
                key = paths.key.display(),
                ca = paths.ca.display(),
            ),
        )
        .expect("write config");

        crate::config::load(&config_path, crate::config::CliOverrides::default())
            .expect("load config")
            .into_config()
    }

    fn context(config: Config, failpoint: Option<Failpoint>) -> FormationContext {
        let tls_store = TlsStore::load(TlsPaths {
            cert: config.tls.cert_path.clone(),
            key: config.tls.key_path.clone(),
            ca: config.tls.ca_path.clone(),
        })
        .expect("load tls store");
        FormationContext {
            config,
            advertise_addr: "localhost:17071".to_string(),
            tls: Some(tls_store),
            failpoint,
        }
    }

    #[tokio::test]
    async fn form_records_the_ca_the_binding_the_policy_and_the_marker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cluster_id = ClusterId::new();
        let config = seed_config(dir.path(), cluster_id);
        let data_dir = config.data_dir.clone();
        let entity = coppice_core::id::QuotaEntityId::new();

        let formation = form(
            context(config, None),
            FormRequest {
                policy: Some(
                    format!("[[quota_entity]]\nid = \"{entity}\"\nname = \"seeded\"\nquota = 5\n")
                        .into_bytes(),
                ),
                operator_csr: None,
                operator_cn: None,
            },
        )
        .await
        .expect("formation succeeds");

        // Read at the index the last proposal committed at: view publication
        // has its own cadence, so `latest()` may still trail the apply.
        let index = formation
            .started
            .consensus
            .read_index()
            .await
            .expect("read index");
        let state = formation
            .started
            .views
            .at_least(index)
            .await
            .expect("await the applied index");
        let state = state.state();

        // The CA certificate is replicated (public material); the key is not.
        let ca = state.ca.as_ref().expect("the CA certificate is recorded");
        assert!(ca.bundle.pem().contains("BEGIN CERTIFICATE"));
        assert!(
            data_dir.join(pki::CA_KEY_FILE).exists(),
            "the CA key belongs on this voter's disk, and only there"
        );
        assert!(data_dir.join(pki::MACHINE_IDENTITY_FILE).exists());

        // This node's machine identity binds to its raft seat (ADR 0037 §7).
        let binding = state
            .machine_binding(&formation.machine)
            .expect("machine identity bound");
        assert_eq!(binding.raft_node_id, formation.started.handle.node_id());

        // The bootstrap policy landed.
        assert!(state.quota_entities.contains_key(&entity));

        // The operator credential exists nowhere but the reply.
        assert!(formation.operator.key_pem.is_some());
        let verified = pki::verify_leaf(
            formation.operator.ca_pem.as_bytes(),
            formation.operator.cert_pem.as_bytes(),
        )
        .expect("the operator leaf verifies against the cluster CA");
        assert_eq!(
            verified.profile,
            pki::Profile::Operator {
                cn: DEFAULT_OPERATOR_CN.to_string()
            }
        );

        // The history is minted, not derived: it must differ from the value
        // the legacy flags would have stamped from config.
        assert_ne!(
            formation.started.handle.history_id(),
            *cluster_id.0.as_bytes(),
            "formation must mint a fresh history id (ADR 0037 §3)"
        );

        // And the marker is stamped last.
        let marks = storage::read_formation_marks(&RealFs::new(data_dir))
            .expect("read marks")
            .expect("manifest");
        assert!(marks.intent_at_us.is_some());
        assert!(marks.complete_at_us.is_some());

        formation
            .started
            .handle
            .shutdown()
            .await
            .expect("shut down");
    }

    /// Both sides of `raft.initialize` leave the same identifiable state: an
    /// intent with no marker. That equivalence is the whole point of putting
    /// the marker after the operator certificate and the policy.
    async fn assert_failpoint_leaves_an_incomplete_formation(failpoint: Failpoint) {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = seed_config(dir.path(), ClusterId::new());
        let data_dir = config.data_dir.clone();

        let err = form(context(config, Some(failpoint)), FormRequest::default())
            .await
            .map(|_| ())
            .expect_err("the failpoint aborts formation");
        assert!(format!("{err:#}").contains("failpoint"), "{err:#}");

        let marks = storage::read_formation_marks(&RealFs::new(data_dir))
            .expect("read marks")
            .expect("the intent was stamped before anything else durable");
        assert!(marks.failed());
        assert_eq!(
            inspect(dir.path().join("data").as_path())
                .expect("inspect")
                .0,
            StartupState::FormationFailed {
                intent_at_us: marks.intent_at_us.expect("intent")
            }
        );
    }

    #[tokio::test]
    async fn a_failpoint_before_raft_initialize_leaves_an_incomplete_formation() {
        assert_failpoint_leaves_an_incomplete_formation(Failpoint::BeforeRaftInitialize).await;
    }

    #[tokio::test]
    async fn a_failpoint_after_raft_initialize_leaves_an_incomplete_formation() {
        assert_failpoint_leaves_an_incomplete_formation(Failpoint::AfterRaftInitialize).await;
    }

    #[tokio::test]
    async fn a_malformed_policy_is_refused_before_anything_durable_happens() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = seed_config(dir.path(), ClusterId::new());
        let data_dir = config.data_dir.clone();

        let err = form(
            context(config, None),
            FormRequest {
                policy: Some(b"[[quota_entity]]\nnot_a_field = 1\n".to_vec()),
                ..FormRequest::default()
            },
        )
        .await
        .map(|_| ())
        .expect_err("a malformed policy is rejected");
        assert!(format!("{err:#}").contains("bootstrap policy"), "{err:#}");
        assert!(
            storage::read_formation_marks(&RealFs::new(data_dir))
                .expect("read marks")
                .is_none(),
            "an operator typo must not cost a data directory"
        );
    }
}
