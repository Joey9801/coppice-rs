//! The self-converging membership loop (ADR 0037 §1/§6).
//!
//! Joining a cluster stops being an operator dance and becomes a loop the new
//! replica runs *against the cluster itself*, as an ordinary client of the
//! admin surface, presenting its own machine certificate. It replaces the
//! hand-driven read-the-node-id-out-of-the-log / `add-learner` / `promote`
//! sequence, and it is re-entered from the top on every retryable failure and
//! on every restart — which is what makes `Restart=always` the entire recovery
//! story (ADR 0037 §6).
//!
//! # Two halves, because a daemon converges from two different places
//!
//! [`PreStart`] runs **inside park**, before this installation has any
//! identity at all: enroll if there is no usable leaf, run discovery, probe,
//! and — the moment an initialized cluster with a matching `cluster_id`
//! answers — start the consensus replica under [`StartIntent::Join`], stamped
//! with **the history id the probe reported**. It never self-bootstraps; a
//! round that finds nothing simply cycles, and park's own `select!` races it
//! against a local `init`.
//!
//! [`spawn`] runs the **post-start** loop, for every started replica — resumed,
//! joined, or freshly formed. It no-ops when this identity is already a
//! caught-up voter, and otherwise drives `AddLearner` → catch-up →
//! `PromoteVoter` against the leader. Because the membership verbs are
//! idempotent by contract (ADR 0037 §6), a process killed at any step
//! converges after respawn with no cleanup, and the two halves overlap
//! harmlessly: a replica that [`PreStart`] admitted still runs the post-start
//! loop, which finds the work already done.
//!
//! # Why discovery is consulted last, not first
//!
//! Discovery is **seed-only** (ADR 0037 §2): it answers "whom might I dial
//! first?" *before* this replica is in the cluster. The moment the replica has
//! a membership view — admitted as a learner, or resumed with replicated state
//! — [`dial_targets`](Convergence::dial_targets) routes from LOCAL knowledge
//! instead (the believed leader, then every other known member), consulting
//! discovery only when local knowledge is empty. That ordering is what
//! guarantees discovery can never *wedge* convergence: an admitted learner
//! already carries replicated membership and leader information, so it keeps
//! converging even if discovery goes empty or names only a since-dead leader,
//! and a leader change mid-join costs one tick because the next tick
//! re-derives its targets from the by-then-updated local view.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use coppice_api::http::ReadyzReport;
use coppice_consensus::{ClusterSummary, NodeHandle, StartIntent, StartedNode, PROMOTION_LAG_MAX};
use coppice_enroll::{Claim, EnrollmentConfig};
use coppice_proto::pb::raft::v1 as pb;
use coppice_tls::pki::machine;
use coppice_tls::{TlsPaths, TlsStore};

use crate::admin::{self, admin_channel_from_store};
use crate::config::{Config, PacingConfig};
use crate::formation::PhaseState;
use coppice_discovery::Discovery;

// Every interval this loop sleeps for is `[pacing]` node configuration
// ([`crate::config::PacingConfig`]), whose defaults are the values these were
// as constants. They are pure liveness knobs — a shorter interval costs dials,
// a longer one costs convergence latency — and the reason they are
// configurable is that a test fleet has no reason to pay production pacing to
// form.

// ---------------------------------------------------------------------------
// (a) Pre-start convergence, inside park
// ---------------------------------------------------------------------------

/// The parked half of the loop: enroll, discover, probe, and join (ADR 0037
/// §1).
///
/// Holds no consensus state, because there is none yet — a parked daemon has
/// no manifest, no node id, and quite possibly no certificate. That last point
/// shapes the whole type: TLS material is loaded **lazily**, at the first
/// round that finds it on disk, because in the ADR's minimal deployment the
/// material does not exist until this loop's own enroll step puts it there.
pub(crate) struct PreStart<'a> {
    config: &'a Config,
    advertise_addr: &'a str,
    /// The store, once there is material for one. Starts as whatever the
    /// daemon loaded at startup — `None` for a certless installation.
    tls: Option<Arc<TlsStore>>,
    backoff: Duration,
}

impl<'a> PreStart<'a> {
    pub(crate) fn new(
        config: &'a Config,
        advertise_addr: &'a str,
        tls: Option<Arc<TlsStore>>,
    ) -> PreStart<'a> {
        PreStart {
            config,
            advertise_addr,
            tls,
            backoff: config.pacing.park_interval_min,
        }
    }

    /// Cycle until an initialized cluster with this `cluster_id` answers, then
    /// join it.
    ///
    /// Returns only on success: every failure in a round — discovery empty,
    /// enrollment refused, nothing initialized answering, consensus declining
    /// to start — is logged and retried, because none of them is a reason for
    /// a parked daemon to stop being a parked daemon (ADR 0037 §1). Park's
    /// `select!` supplies the two exits that are not "found a cluster": a
    /// local `init`, and shutdown. Cancellation at any await is safe — this
    /// holds only client dials until the instant it hands back a started
    /// replica.
    pub(crate) async fn run(&mut self) -> (StartedNode, Arc<TlsStore>) {
        loop {
            match self.round().await {
                Ok(Some(joined)) => return joined,
                Ok(None) => {}
                // Escalate once the backoff has maxed out: the first failed
                // rounds of a fleet boot are routine (peers still binding),
                // but a daemon that has been failing for the whole backoff
                // ramp is *stuck*, and its reason must be visible at the
                // default log level — a parked daemon has no other surface
                // that says why (ADR 0037 §9's spirit).
                Err(e) if self.backoff >= self.config.pacing.park_interval_max => {
                    tracing::warn!(
                        error = %format!("{e:#}"),
                        "convergence: rounds keep failing at maximum backoff; still parked, \
                         will retry"
                    );
                }
                Err(e) => {
                    tracing::info!(
                        error = %format!("{e:#}"),
                        "convergence: round did not complete; still parked, will retry"
                    );
                }
            }
            tokio::time::sleep(jittered(self.backoff)).await;
            self.backoff = (self.backoff * 2).min(self.config.pacing.park_interval_max);
        }
    }

    /// One round: enroll if needed, discover, probe, and join what answers.
    async fn round(&mut self) -> Result<Option<(StartedNode, Arc<TlsStore>)>> {
        self.enroll_if_needed().await;

        let mut candidates = coppice_discovery::build(&self.config.discovery.seed_config())
            .context("building the discovery backend")?
            .candidates()
            .await;
        // Several backends (`file` foremost) list this very process. A daemon
        // is never the cluster it is looking for.
        candidates.retain(|candidate| candidate != self.advertise_addr);
        if candidates.is_empty() {
            tracing::debug!("convergence: discovery named no candidates");
            return Ok(None);
        }

        // The probe plane is mTLS. Without material there is nothing to ask
        // with — which is not an error, it is the state enrollment exists to
        // leave, and the next round tries again.
        if !leaf_present(self.config) {
            tracing::debug!(
                candidates = candidates.len(),
                "convergence: no TLS material yet, so no candidate can be probed"
            );
            return Ok(None);
        }

        let answers = crate::probe::probe_all(self.config, &candidates).await?;
        let cluster_id = self.config.cluster_id.to_string();
        let Some((target, answer)) = answers
            .into_iter()
            .find(|(_, a)| a.initialized && a.cluster_id == cluster_id)
        else {
            tracing::debug!(
                candidates = candidates.len(),
                "convergence: no candidate reports an initialized cluster; staying parked"
            );
            return Ok(None);
        };

        // The history is the CLUSTER's, learned at admission (ADR 0037 §3) —
        // never the config-derived value the removed `--join` flag stamped. A
        // cluster that was wiped and re-formed keeps its `cluster_id` and
        // carries a new history, and stamping the wrong one here would be the
        // cross-cluster mixup the stamp exists to catch.
        let history_id: [u8; 16] = answer.history_id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!(
                "{target} reports an initialized cluster with a {}-byte history id; expected 16 \
                 (ADR 0037 §3)",
                answer.history_id.len()
            )
        })?;

        let store = self.store()?;
        tracing::info!(
            %target,
            history_id = %crate::formation::hex(&history_id),
            "convergence: found an initialized cluster; joining it as a new learner \
             (ADR 0037 §6)"
        );
        let options = crate::bootstrap::node_options(
            self.config,
            history_id,
            self.advertise_addr.to_string(),
            Arc::clone(&store),
        );
        let started = coppice_consensus::start(options, StartIntent::Join)
            .await
            .context("starting consensus to join the discovered cluster")?;
        // `start` stamps the directory as formed on the way in (ADR 0037 §3):
        // the history above is the *cluster's*, not one this config could
        // derive, and the marker is what tells the next start not to read that
        // as a wrong-volume mixup (`crate::formation::resumed_history`).
        Ok(Some((started, store)))
    }

    /// Obtain a cluster-signed leaf if this installation has none (ADR 0037
    /// §5), minting and persisting the machine identity it claims first.
    ///
    /// Never fatal: an enrollment endpoint that is down, a leader mid-election,
    /// a token not yet seeded — all of them are reasons to try again next
    /// round, not reasons to stop. The machine identity is minted **once** and
    /// persisted beside the manifest, so a restart re-presents the same
    /// identity and the cluster's one-identity-one-seat rule (§7) holds across
    /// crashes.
    async fn enroll_if_needed(&self) {
        let Some(enrollment) = &self.config.enrollment else {
            return;
        };
        let paths = crate::bootstrap::tls_paths(self.config);
        if coppice_enroll::client::has_usable_leaf(&paths) {
            return;
        }
        if let Err(e) = self.enroll(enrollment, &paths).await {
            // Same escalation rule as the round loop: a first refusal is
            // routine fleet-boot noise, but enrollment still failing once the
            // backoff has maxed out is the thing an operator (or a CI log)
            // needs to see without turning the log level up.
            if self.backoff >= self.config.pacing.park_interval_max {
                tracing::warn!(
                    endpoint = %enrollment.endpoint,
                    error = %format!("{e:#}"),
                    "convergence: enrollment keeps failing at maximum backoff; will retry"
                );
            } else {
                tracing::info!(
                    endpoint = %enrollment.endpoint,
                    error = %format!("{e:#}"),
                    "convergence: enrollment did not succeed; will retry"
                );
            }
        }
    }

    async fn enroll(&self, enrollment: &EnrollmentConfig, paths: &TlsPaths) -> Result<()> {
        let data_dir = &self.config.data_dir;
        let machine = match machine::load_machine_identity(data_dir)
            .context("reading this installation's machine identity")?
        {
            Some(machine) => machine,
            None => {
                let machine = machine::mint_machine_identity();
                machine::persist_machine_identity(data_dir, &machine)
                    .context("persisting this installation's machine identity")?;
                machine
            }
        };
        // The SANs are this daemon's own serving addresses, declared rather
        // than derivable: the cluster cannot know where a machine it has never
        // met listens. They are checked, not trusted — the leader dial-back-
        // verifies the advertised address at admission (ADR 0037 §6).
        coppice_enroll::ensure_enrolled(
            paths,
            enrollment,
            Claim::Machine(machine),
            &crate::formation::leaf_sans(self.config),
        )
        .await
        .context("enrolling for a cluster-signed coordinator leaf")?;
        // A certless daemon that just enrolled can serve `ProbeCluster` from
        // the next round on; say so, because "parked and invisible" and
        // "parked and dialable" look identical from outside otherwise.
        tracing::info!(%machine, "convergence: enrolled; this daemon now holds a cluster leaf");
        Ok(())
    }

    /// The hot-reload store, loaded on first use.
    ///
    /// A daemon that started certless has no store; the material appears only
    /// when enrollment installs it, which is inside this loop. Loading here
    /// (rather than at startup) is the whole reason enrollment can precede
    /// having credentials at all.
    fn store(&mut self) -> Result<Arc<TlsStore>> {
        if let Some(store) = &self.tls {
            return Ok(Arc::clone(store));
        }
        let store = TlsStore::load(crate::bootstrap::tls_paths(self.config))
            .context("loading the TLS material this daemon enrolled for")?;
        self.tls = Some(Arc::clone(&store));
        Ok(store)
    }
}

/// Whether all three `[tls]` files exist — the cheap precondition for dialing
/// the mTLS probe plane, distinct from `has_usable_leaf`'s expiry check
/// because a probe with an expired leaf still fails informatively.
fn leaf_present(cfg: &Config) -> bool {
    [&cfg.tls.cert_path, &cfg.tls.key_path, &cfg.tls.ca_path]
        .iter()
        .all(|p| p.exists())
}

// ---------------------------------------------------------------------------
// (b) Post-start convergence
// ---------------------------------------------------------------------------

/// Everything the post-start loop needs to drive itself against the cluster.
pub(crate) struct Convergence {
    /// This replica's own handle, for reading local membership each tick.
    pub(crate) handle: NodeHandle,
    /// The `host:port` this replica advertises to peers — the address it asks
    /// to be admitted at, and the one the leader dial-back-verifies (§6).
    pub(crate) advertise_addr: String,
    /// The operator-chosen logical cluster name, matched against probe
    /// answers (ADR 0020/0037 §3).
    pub(crate) cluster_id: String,
    /// The discovery backend, consulted only pre-admission (ADR 0037 §2).
    pub(crate) discovery: Arc<dyn Discovery>,
    /// The shared mTLS store: the loop dials the admin surface through it,
    /// presenting this daemon's own machine certificate, which is what the
    /// §7 self-service grant is keyed on.
    pub(crate) tls: Arc<TlsStore>,
    /// Where terminal admission refusals are published for `/readyz` (§9).
    pub(crate) phase: Arc<PhaseState>,
    /// How fast this loop ticks: the `[pacing]` section of node config,
    /// carried by value because the loop outlives the borrow of `Config`.
    pub(crate) pacing: PacingConfig,
    /// The cluster's **public** client edge, as this daemon's `[enrollment]`
    /// table names it — the second supersession channel (see
    /// [`Convergence::watch_for_supersession`]). `None` when no `[enrollment]`
    /// table is configured, which simply leaves that channel silent.
    pub(crate) public_edge: Option<PublicEdge>,
    /// How this loop stops the whole daemon when it establishes that this
    /// volume's history has been superseded (ADR 0037 §3).
    pub(crate) failstop: FailStop,
    /// Where this daemon's join pipeline stops dead, if its config armed a
    /// failpoint (ADR 0037 §6). Disarmed in every real deployment, and
    /// unarmable in a release build — see [`crate::failpoints`]. Carried on
    /// the loop rather than read from a global so that arming one daemon of a
    /// single-process test fleet cannot arm its leader.
    pub(crate) failpoints: crate::failpoints::Failpoints,
    /// Consecutive public-edge supersession observations, and the superseding
    /// history they all named. Interior mutability because the loop's tick
    /// takes `&self`; a plain `Mutex` because it is written once per tick and
    /// never held across an await.
    pub(crate) supersession: Mutex<Option<(String, u32)>>,
}

/// How many **consecutive** rounds a public-edge supersession observation must
/// repeat — naming the same superseding history each time — before this daemon
/// fail-stops. See [`Convergence::watch_for_supersession`] for why the machine
/// plane needs no such corroboration and this channel does.
const SUPERSESSION_ROUNDS: u32 = 3;

/// How long the terminal `/readyz` stays readable after the fail-stop verdict
/// is published and before the daemon starts draining. Not pacing-derived: it
/// is a bound on how long a *reader* has, and it is deliberately the same on a
/// test fleet as in production so nothing depends on the tempo.
const SUPERSESSION_READYZ_GRACE: Duration = Duration::from_secs(1);

/// The marker every history-superseded refusal message begins with, matching
/// the `/readyz` phase name ([`ReadyzPhase::HistorySuperseded`]) so a log line
/// and a status document are greppable by the same string.
pub(crate) const HISTORY_SUPERSEDED: &str = "history-superseded";

/// The daemon-wide stop this loop pulls on a superseded history.
///
/// Fail-stop, not park: a process that keeps running would keep answering raft
/// and client traffic out of a history the cluster has abandoned. The reason is
/// stashed alongside the trigger so `bootstrap::run_with` can exit **nonzero**
/// with it after the ordinary shutdown order has drained — a clean stop with an
/// alarming exit status, which under `Restart=always` becomes a restart loop
/// that re-reaches the same refusal. That loop is the intended posture: ADR
/// 0037's parked-fleet consequence is that a fleet which cannot legitimately
/// join anything must alarm rather than quietly serve something wrong.
#[derive(Clone)]
pub(crate) struct FailStop {
    reason: Arc<OnceLock<String>>,
    shutdown: watch::Sender<bool>,
}

impl FailStop {
    pub(crate) fn new(shutdown: watch::Sender<bool>) -> FailStop {
        FailStop {
            reason: Arc::new(OnceLock::new()),
            shutdown,
        }
    }

    /// Record why the daemon is stopping and start the drain. Idempotent: the
    /// first reason wins, and a second trigger cannot overwrite it.
    fn trigger(&self, reason: String) {
        let _ = self.reason.set(reason);
        let _ = self.shutdown.send(true);
    }

    /// The recorded reason, if this daemon is stopping because of a fail-stop
    /// rather than a signal.
    pub(crate) fn reason(&self) -> Option<String> {
        self.reason.get().cloned()
    }
}

/// A client for the cluster's public client-listener edge — the surface
/// `[enrollment]` already names, dialed under exactly the posture enrollment
/// declares (`https` verified against system roots, or plain `http` only with
/// the explicit `insecure` flag, ADR 0037 §4).
///
/// `GET /readyz` there answers with the responder's `cluster_id` and
/// `history_id`, which is all supersession detection needs.
pub(crate) struct PublicEdge {
    /// The `[enrollment] endpoint` base URL, trailing slash trimmed.
    endpoint: String,
    insecure: bool,
    /// Built on first use, not at startup: a healthy fleet never dials this,
    /// and assembling a root store costs real time in a debug build.
    http: OnceLock<reqwest::Client>,
}

impl PublicEdge {
    /// Build the edge client for a daemon whose config declares one.
    ///
    /// Returns `None` for an `[enrollment]` table whose posture
    /// [`validate_endpoint`](coppice_enroll::client::validate_endpoint)
    /// rejects. That is unreachable through config load (which validates the
    /// same table), and if it ever were reachable the right answer is a silent
    /// channel rather than a daemon that refuses to start over a diagnostic.
    pub(crate) fn from_config(cfg: Option<&EnrollmentConfig>) -> Option<PublicEdge> {
        let cfg = cfg?;
        if let Err(e) = coppice_enroll::client::validate_endpoint(&cfg.endpoint, cfg.insecure) {
            tracing::warn!(
                endpoint = %cfg.endpoint,
                error = %e,
                "convergence: the [enrollment] endpoint cannot be dialed for supersession \
                 detection; that channel stays silent"
            );
            return None;
        }
        Some(PublicEdge {
            endpoint: cfg.endpoint.trim_end_matches('/').to_string(),
            insecure: cfg.insecure,
            http: OnceLock::new(),
        })
    }

    /// One bounded `GET /readyz`. `None` for anything that did not produce a
    /// well-formed report — unreachable, timed out, a proxy error page, a
    /// version that does not serve the route. A silent channel never
    /// fail-stops anything.
    async fn readyz(&self) -> Option<ReadyzReport> {
        let http = match self.http.get() {
            Some(http) => http,
            None => {
                let built = reqwest::Client::builder()
                    // The same two transport rules the enrollment client sets,
                    // for the same reasons: a redirect is a different host's
                    // answer, and an unbounded dial is a hung tick.
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(crate::probe::PROBE_TIMEOUT)
                    .use_rustls_tls()
                    .tls_built_in_root_certs(!self.insecure)
                    .build();
                match built {
                    Ok(http) => {
                        let _ = self.http.set(http);
                        self.http.get()?
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "convergence: could not build the public-edge client"
                        );
                        return None;
                    }
                }
            }
        };
        let response = http
            .get(format!("{}/readyz", self.endpoint))
            .send()
            .await
            .ok()?;
        // `/readyz` answers 503 in every phase but a ready voter, and a
        // superseding cluster's leader is exactly as likely to be 200 as a
        // follower is to be 503 — so the status is deliberately not consulted,
        // only the body.
        response.json::<ReadyzReport>().await.ok()
    }
}

/// Spawn the post-start convergence loop over a started replica.
///
/// Runs for *every* replica, converged or not: the first tick of a caught-up
/// voter — the resumed and the freshly-formed case — is a membership read and
/// a sleep, which is exactly the "no-ops when already a caught-up voter"
/// ADR 0037 §1 asks for, and keeps one code path instead of two.
///
/// The returned handle is aborted at shutdown alongside the runtime's other
/// tasks. That is safe at any await: the loop holds only client dials and the
/// verbs it drives are idempotent, so an aborted tick is indistinguishable
/// from a tick that never happened.
pub(crate) fn spawn(convergence: Convergence) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let wait = convergence.step().await;
            tokio::time::sleep(jittered(wait)).await;
        }
    })
}

/// What one pass against a leader achieved, and therefore how long to wait.
enum JoinStep {
    /// Promotion succeeded: this replica is now a voter.
    Promoted,
    /// Admitted (or already) a learner, still catching up — keep polling.
    Learner,
    /// The voter set is full, or the leader could not fold in a removal
    /// (`no-removable-peer`, `quorum-at-risk`) (ADR 0037 §7). Deliberately
    /// *not* terminal: this replica stays a caught-up learner and keeps
    /// polling, because it is then either the `new_node_id` of a pending
    /// `ReplaceVoter` or waiting on the evidence-gated removal of a dead
    /// predecessor. The server's machine-readable reason is carried so §7's
    /// "visible in status output" holds — an operator reading `/readyz` can
    /// tell a routine full set from a promotion the leader refused because
    /// no dead peer qualified.
    VoterSetFull(String),
    /// A retryable failure — not the leader, an election in flight, an
    /// unreachable dial. Re-probe and try again.
    Retry,
    /// A refusal no amount of waiting fixes: a duplicated machine identity, or
    /// this node id already in membership at a different address (ADR 0037
    /// §7). Recorded as the last admission refusal and backed off hard.
    Refused(String),
    /// The leader's dial-back verification of this daemon's advertised
    /// endpoint failed (ADR 0037 §6 step 3). The middle path between the two
    /// arms above: **surfaced** like a refusal — a permanently wrong
    /// `advertise_addr` would otherwise wedge this node silently, and status
    /// is where an operator finds that out (§9) — but **retried** at the fast
    /// cadence, because during a fleet boot it is routinely transient (the
    /// leader dials back before the joiner's listener binds) and a hard
    /// backoff would tax every cold start to punish a misconfiguration.
    EndpointUnverified(String),
}

impl Convergence {
    /// Run one tick; return how long to wait before the next.
    async fn step(&self) -> Duration {
        let summary = self.handle.cluster_summary();
        let me = summary.members.iter().find(|m| m.id == summary.local_id);

        // Already a voter: converged as far as *membership* goes, so all the
        // admission machinery below this line is a no-op for the
        // overwhelmingly common case (a restart of a healthy replica).
        //
        // One question survives that short-circuit, and only for a voter that
        // has lost contact with its cluster: is this replica's whole history
        // still the cluster's? A resuming voter cannot answer it from local
        // state — its membership, its stamp and its log are all perfectly
        // self-consistent — which is precisely how an old volume ends up
        // serving a history the fleet re-initialized past (ADR 0037 §3).
        if me.is_some_and(|m| m.voter) {
            // Including the notices: this replica holds a seat, so whatever
            // the pre-promotion churn left in `/readyz` is stale by
            // definition. The `Promoted` arm below clears them too, but only
            // for the promotion *this* loop drove — a replica that restarts
            // already promoted, or one a new leader promoted out from under
            // an in-flight attempt, converges through here and never through
            // that arm (ADR 0037 §6/§9).
            self.phase.clear_convergence_notices();
            self.watch_for_supersession(&summary).await;
            return self.pacing.settled_interval;
        }

        let targets = self.dial_targets(&summary).await;
        let Some((leader_addr, leader_history)) = self.find_leader(&targets).await else {
            tracing::debug!(
                targets = targets.len(),
                "convergence: no leader found this tick; retrying"
            );
            return self.pacing.probe_interval;
        };

        // Compare histories BEFORE asking to be admitted (ADR 0016 / ADR 0037
        // §3): a mismatch is the wrong-volume / wiped-and-re-formed-cluster
        // case, and waiting cannot fix it — two raft histories never merge.
        // Without this check the loop would hammer `AddLearner` at 300ms and
        // eat the server's cross-history refusal forever; with it, the reason
        // reaches `/readyz last_admission_refusal` and the loop backs off
        // hard. (The pre-start half never hits this: it stamps the history the
        // probe reported, by design. Only a *stamped* replica can disagree
        // with the cluster it found.)
        let stamped = self.handle.history_id();
        if leader_history.as_slice() != stamped.as_slice() {
            let message = cross_history_refusal(&leader_addr, &leader_history, &stamped);
            tracing::error!(
                node_id = summary.local_id,
                refusal = %message,
                "convergence: the discovered cluster carries a different raft history; \
                 joining can never succeed (ADR 0016 / ADR 0037 §3)"
            );
            self.phase.record_admission_refusal(message);
            return self.pacing.refusal_backoff;
        }

        match self.attempt_join(&summary, &leader_addr).await {
            JoinStep::Promoted => {
                tracing::info!(
                    node_id = summary.local_id,
                    "convergence: promoted to voter; this replica has converged (ADR 0037 §6)"
                );
                // A converged voter has nothing pending; leaving a stale
                // refusal or hold in `/readyz` would read as a live problem.
                self.phase.clear_convergence_notices();
                self.pacing.settled_interval
            }
            JoinStep::Learner => self.pacing.probe_interval,
            JoinStep::VoterSetFull(message) => {
                tracing::debug!(
                    node_id = summary.local_id,
                    hold = %message,
                    "convergence: no voter seat is available; remaining a caught-up learner \
                     and continuing to poll (ADR 0037 §7)"
                );
                // §7: "promotion is refused with a machine-readable reason —
                // the learner keeps polling and the situation is visible in
                // status output". The reason lands in `/readyz
                // promotion_hold` — deliberately NOT `last_admission_refusal`,
                // which stays reserved for operator-actionable refusals —
                // without changing the polling cadence.
                self.phase.record_promotion_hold(message);
                self.pacing.settled_interval
            }
            JoinStep::Retry => self.pacing.probe_interval,
            JoinStep::EndpointUnverified(message) => {
                tracing::info!(
                    node_id = summary.local_id,
                    refusal = %message,
                    "convergence: the leader could not verify this daemon's advertised \
                     endpoint; surfacing it and retrying at the probe cadence (ADR 0037 §6)"
                );
                self.phase.record_admission_refusal(message);
                self.pacing.probe_interval
            }
            JoinStep::Refused(message) => {
                tracing::error!(
                    node_id = summary.local_id,
                    refusal = %message,
                    "convergence: admission refused for a reason waiting cannot fix; see \
                     /readyz last_admission_refusal (ADR 0037 §7)"
                );
                self.phase.record_admission_refusal(message);
                self.pacing.refusal_backoff
            }
        }
    }

    /// Decide whether this voter's raft history has been **superseded** by a
    /// re-init of a cluster wearing the same `cluster_id`, and fail-stop the
    /// daemon if so (ADR 0037 §3 Consequences).
    ///
    /// # The state being detected
    ///
    /// `history_id` is minted exactly once per formation, so *one* situation
    /// produces "a formed cluster answering to my `cluster_id` with a history
    /// that is not mine": somebody re-inited. This volume then holds state from
    /// before that act. Two raft histories never merge, so there is nothing to
    /// converge toward and nothing worth serving — the ADR's answer is a
    /// fail-stop, and the operator's is restore-or-wipe.
    ///
    /// # Three guards against fail-stopping a healthy voter
    ///
    /// A false positive here takes down a working replica, so the trigger is
    /// *exclusively* a positive observation of a formed, same-`cluster_id`,
    /// different-`history_id` cluster. Everything else — empty discovery, an
    /// unreachable peer, a malformed answer, no `[enrollment]` table — leaves
    /// the daemon exactly as it was.
    ///
    /// 1. **In-contact voters never look.** A voter that still has its
    ///    cluster (a follower that knows a leader, a leader with a fresh
    ///    quorum acknowledgment) is healthy by definition and returns before
    ///    dialing anything. That also keeps discovery off the hot path of a
    ///    healthy fleet entirely: this costs nothing until a voter is already
    ///    alone.
    /// 2. **Our own history wins outright.** If any peer answers carrying the
    ///    history this replica is stamped for, the counter resets and the
    ///    round ends — a partition is not a supersession, and the two look
    ///    identical from the inside until somebody answers.
    /// 3. **Corroboration where the channel is weaker.** See below.
    ///
    /// Guard 1 has a known boundary: a **sole voter** is its own quorum and can
    /// never register lost contact, so a single-voter cluster's old volume is
    /// not detected here. That is deliberate rather than overlooked — the
    /// alternative is polling the public edge forever on every single-node
    /// deployment — and it is the narrow case: a one-voter cluster whose volume
    /// survives a re-init has no peers to disagree with it in the first place.
    ///
    /// # Two channels, because a re-init breaks the strong one
    ///
    /// **(1) The machine plane** — `ProbeCluster` over mTLS. The strongest
    /// signal available: the responder proved possession of a leaf signed by
    /// *this replica's own* trust root and vice versa, so an answer here is
    /// structural and one observation is conclusive.
    ///
    /// It is also, for the case that matters most, silent. Formation mints a
    /// brand-new root CA (`formation::form`, ADR 0037 §3/§4), so a fleet
    /// re-inited after losing its volumes is not merely on a new history — it
    /// is on a new trust root, and the handshake fails before either side says
    /// anything. That is not a signal: a refused handshake and a dead peer are
    /// the same observation.
    ///
    /// **(2) The public client edge** — `GET /readyz` at the `[enrollment]`
    /// endpoint. Deliberately the one plane designed to work *across* a trust
    /// discontinuity, which is exactly why enrollment lives there (§4): it is
    /// verified against system roots, not the cluster's own CA, so it still
    /// answers after a re-root or a re-init. Its answer is unauthenticated
    /// with respect to *this* cluster, so it is corroborated instead: the same
    /// superseding history must be observed on [`SUPERSESSION_ROUNDS`]
    /// consecutive rounds, and any round that observes something else — our
    /// own history, a different superseding history, nothing at all — resets
    /// the count to zero.
    async fn watch_for_supersession(&self, summary: &ClusterSummary) {
        // Already fail-stopped and draining; do not re-log the same verdict
        // every tick until the task is aborted.
        if self.failstop.reason().is_some() {
            return;
        }
        // Guard 1: a voter still in contact with its cluster has its answer.
        if !self.phase.readyz().leader_contact_stale {
            self.forget_supersession();
            return;
        }

        let stamped = crate::formation::hex(&self.handle.history_id());

        // Channel 1: every peer this replica knows of, plus whatever discovery
        // names. Discovery is additive here and never load-bearing — a voter
        // out of contact is exactly the case where its replicated membership
        // may name only addresses that no longer exist.
        let mut targets = self.dial_targets(summary).await;
        for candidate in self.discovery.candidates().await {
            if candidate != self.advertise_addr && !targets.contains(&candidate) {
                targets.push(candidate);
            }
        }
        for target in &targets {
            let Some(answer) = self.probe(target).await else {
                continue;
            };
            let observed = crate::formation::hex(&answer.history_id);
            // Guard 2.
            if observed == stamped {
                self.forget_supersession();
                return;
            }
            self.fail_stop(supersession_refusal(
                target,
                "the mutually authenticated machine plane",
                &observed,
                &stamped,
            ))
            .await;
            return;
        }

        // Channel 2.
        let Some(edge) = &self.public_edge else {
            self.forget_supersession();
            return;
        };
        let Some(report) = edge.readyz().await else {
            self.forget_supersession();
            return;
        };
        // A responder that names another `cluster_id`, or that has no history
        // to report (parked, or fail-stopped itself), says nothing about ours.
        let observed = match report.history_id {
            Some(history) if report.cluster_id == self.cluster_id => history,
            _ => {
                self.forget_supersession();
                return;
            }
        };
        // Guard 2, on this channel: the endpoint is usually a load-balanced
        // name in front of *our own* cluster, and that is what it answers with
        // whenever the cluster is fine.
        if observed == stamped {
            self.forget_supersession();
            return;
        }
        // Guard 3.
        let seen = self.note_supersession(&observed);
        if seen < SUPERSESSION_ROUNDS {
            tracing::warn!(
                endpoint = %edge.endpoint,
                observed_history = %observed,
                stamped_history = %stamped,
                round = seen,
                of = SUPERSESSION_ROUNDS,
                "convergence: the cluster's public endpoint reports a different raft history \
                 than this replica is stamped for; corroborating before fail-stopping \
                 (ADR 0037 §3)"
            );
            return;
        }
        self.fail_stop(supersession_refusal(
            &edge.endpoint,
            "the cluster's public client edge, on three consecutive rounds",
            &observed,
            &stamped,
        ))
        .await;
    }

    /// Count one more consecutive observation of `observed`, returning the run
    /// length. A different history restarts the run rather than extending it:
    /// three answers naming three histories corroborate nothing.
    fn note_supersession(&self, observed: &str) -> u32 {
        let mut seen = self.supersession.lock().expect("supersession lock");
        let count = match seen.take() {
            Some((history, count)) if history == observed => count + 1,
            _ => 1,
        };
        *seen = Some((observed.to_string(), count));
        count
    }

    fn forget_supersession(&self) {
        *self.supersession.lock().expect("supersession lock") = None;
    }

    /// Publish the verdict and stop the daemon (ADR 0037 §3/§9).
    ///
    /// All three surfaces the ADR names, in the order an operator meets them:
    /// an ERROR log line naming both histories and what produced them, a
    /// terminal `/readyz` phase carrying the same text, and a nonzero exit
    /// through the ordinary shutdown drain — which stops client and raft
    /// traffic on the way out.
    async fn fail_stop(&self, message: String) {
        tracing::error!(
            refusal = %message,
            "convergence: this replica's raft history has been superseded by a re-init of a \
             cluster with the same cluster_id; there is no path back into it, so this daemon \
             is fail-stopping rather than serving state the fleet has abandoned (ADR 0037 §3)"
        );
        self.phase.publish_superseded(message.clone());
        // Publish, *then* stop — with a window in between wide enough that the
        // terminal state can actually be read. ADR 0037 §9 makes status the
        // surface an operator (and a load balancer, and a scrape) learns this
        // from, and a phase that exists for a millisecond before the listener
        // goes away is a phase nobody ever sees. Bounded and short: this is a
        // grace period for readers, not a retry.
        tokio::time::sleep(SUPERSESSION_READYZ_GRACE).await;
        self.failstop.trigger(message);
    }

    /// The ordered addresses to look for a leader at this tick.
    ///
    /// **Local knowledge first** (ADR 0037 §2/§6): the currently-believed
    /// leader from replicated membership, then every other known member.
    /// A stale leader belief costs one skipped dial — the next member probed
    /// reports the real leader, and the next tick re-derives from the updated
    /// view. Discovery is the fallback, consulted only when local knowledge is
    /// empty: the genuinely pre-admission case, which is precisely what
    /// seeding a first dial is for.
    async fn dial_targets(&self, summary: &ClusterSummary) -> Vec<String> {
        let mut targets: Vec<String> = Vec::new();
        if let Some(leader) = summary.leader {
            if let Some(m) = summary.members.iter().find(|m| m.id == leader) {
                targets.push(m.addr.clone());
            }
        }
        for m in &summary.members {
            if m.id != summary.local_id && !targets.contains(&m.addr) {
                targets.push(m.addr.clone());
            }
        }
        if targets.is_empty() {
            targets = self.discovery.candidates().await;
            targets.retain(|t| *t != self.advertise_addr);
        }
        targets
    }

    /// Probe candidates for a leader of an initialized cluster with our
    /// `cluster_id` (ADR 0037 §3/§6).
    ///
    /// Unreachable candidates are skipped, not reported: probing is a search
    /// for the leader, not a census.
    ///
    /// **The whole round's answers are collected before anything is chosen**,
    /// then examined strongest evidence first, and an endpoint is only ever
    /// returned on the strength of *its own* answer — never on another
    /// replica's belief about it:
    ///
    /// 1. **Self-evidence.** A candidate whose own answer names itself as
    ///    leader (`leader_hint == node_id`) is taken outright: that is the
    ///    strongest claim any remote can make.
    /// 2. **Validated hints.** Otherwise the distinct endpoints the answers'
    ///    `leader_hint`s resolve to (through each answer's own voter list) are
    ///    probed in turn, and one is returned only if it answers *and* claims
    ///    leadership for itself under the hinted node id. A follower's hint is
    ///    a belief, and a stale or partitioned follower can go on believing in
    ///    a dead former leader indefinitely — returning its hint unvalidated
    ///    would let whichever candidate happens to be listed first pin every
    ///    tick to a corpse while the real leader sat unconsidered later in the
    ///    list. A dead hinted endpoint is skipped, never returned.
    /// 3. **Fallback.** No hint validated: fall back to a reachable candidate
    ///    that is itself in a reported voter set (else one that at least
    ///    reports a non-empty voter set) — worth dialing because its refusal
    ///    names the real leader and the next tick retargets. An unadmitted
    ///    joiner reports an empty voter set, and is exactly the answer this
    ///    must not settle for: a replica that has started consensus under
    ///    `Join` but has not been admitted answers `initialized` too, and two
    ///    joiners seeded with each other would otherwise spin on "no leader
    ///    currently known" forever.
    ///
    /// The chosen endpoint's reported `history_id` rides along, so the caller
    /// can refuse a cross-history join *before* asking to be admitted
    /// (ADR 0016 / ADR 0037 §3).
    async fn find_leader(&self, candidates: &[String]) -> Option<(String, Vec<u8>)> {
        // Collect the round: every reachable candidate's answer, deduplicated.
        let mut answers: Vec<(String, pb::ProbeClusterResponse)> = Vec::new();
        for candidate in candidates {
            if answers.iter().any(|(addr, _)| addr == candidate) {
                continue;
            }
            if let Some(answer) = self.probe(candidate).await {
                answers.push((candidate.clone(), answer));
            }
        }

        // (1) A candidate that is the leader by its own account.
        if let Some((addr, answer)) = answers.iter().find(|(_, a)| claims_leadership(a)) {
            return Some((addr.clone(), answer.history_id.clone()));
        }

        // (2) The distinct hinted endpoints, in answer order, each validated
        // by asking it directly.
        let mut hinted: Vec<(u64, String)> = Vec::new();
        for (_, answer) in &answers {
            let Some(hint) = answer.leader_hint else {
                continue;
            };
            let Some(voter) = answer.voters.iter().find(|v| v.node_id == hint) else {
                continue;
            };
            if !hinted.iter().any(|(_, addr)| *addr == voter.address) {
                hinted.push((hint, voter.address.clone()));
            }
        }
        for (hint, addr) in hinted {
            // Already answered this round without claiming leadership: its own
            // word is fresher than anyone's belief about it. Skip the re-dial.
            if answers.iter().any(|(a, _)| *a == addr) {
                continue;
            }
            let Some(answer) = self.probe(&addr).await else {
                continue; // a dead hint is a dead hint
            };
            if answer.node_id == Some(hint) && claims_leadership(&answer) {
                let history = answer.history_id.clone();
                return Some((addr, history));
            }
        }

        // (3) Nothing validated: a member candidate whose refusal will name
        // the real leader.
        answers
            .iter()
            .find(|(_, a)| {
                a.node_id
                    .is_some_and(|id| a.voters.iter().any(|v| v.node_id == id))
            })
            .or_else(|| answers.iter().find(|(_, a)| !a.voters.is_empty()))
            .map(|(addr, answer)| (addr.clone(), answer.history_id.clone()))
    }

    /// One bounded `ProbeCluster` against `addr`, filtered to an initialized
    /// answer for this `cluster_id`. `None` for everything else — unreachable,
    /// timed out, uninitialized, or another cluster entirely.
    async fn probe(&self, addr: &str) -> Option<pb::ProbeClusterResponse> {
        let attempt = async {
            let mut client = admin_channel_from_store(addr, &self.tls).await.ok()?;
            client
                .probe_cluster(pb::ProbeClusterRequest {
                    cluster_id: self.cluster_id.clone(),
                })
                .await
                .ok()
                .map(|resp| resp.into_inner())
        };
        // Bounded for the same reason `probe.rs` bounds its round: a
        // black-holed address must cost one timeout, not a hung tick.
        let resp = tokio::time::timeout(crate::probe::PROBE_TIMEOUT, attempt)
            .await
            .ok()??;
        (resp.initialized && resp.cluster_id == self.cluster_id).then_some(resp)
    }

    /// One `AddLearner` → catch-up check → `PromoteVoter` pass against
    /// `leader_addr` (ADR 0037 §6).
    ///
    /// Every verb is idempotent by contract, so a pass is safe to repeat from
    /// the top however the last one ended — which is the whole reason this can
    /// be a stateless loop rather than a resumable state machine.
    async fn attempt_join(&self, summary: &ClusterSummary, leader_addr: &str) -> JoinStep {
        let node_id = summary.local_id;
        let history_id = self.handle.history_id();
        let mut client = match admin_channel_from_store(leader_addr, &self.tls).await {
            Ok(client) => client,
            Err(e) => {
                tracing::debug!(
                    leader = %leader_addr,
                    error = %format!("{e:#}"),
                    "convergence: could not dial the leader's admin surface; retrying"
                );
                return JoinStep::Retry;
            }
        };

        // Step 3: admission. The leader binds the machine identity from the
        // mTLS session this call arrives under and dial-back-verifies the
        // advertised address before admitting (ADR 0037 §6/§7).
        //
        // The two failpoints around this call bracket the admission RPC from
        // both sides — stamped-but-never-asked, and asked-but-never-answered.
        // Both are no-ops unless this daemon's own config armed them.
        self.failpoints
            .halt_if_armed(crate::failpoints::JOIN_BEFORE_ADD_LEARNER)
            .await;
        let admitted = client
            .add_learner(pb::AddLearnerRequest {
                history_id: history_id.to_vec(),
                node_id,
                address: self.advertise_addr.clone(),
            })
            .await;
        // Deliberately between the `await` and the inspection below: an armed
        // daemon halts holding an answer it never looks at, which is exactly
        // the state a crash on the wire produces — the leader may have
        // committed the admission, and this replica has no idea.
        self.failpoints
            .halt_if_armed(crate::failpoints::JOIN_ADD_LEARNER_ISSUED)
            .await;
        if let Err(status) = admitted {
            // Debug rather than warn: a redirect to the real leader and an
            // election in flight both land here, and both are ordinary. The
            // refusals an operator must see are published to `/readyz` by the
            // caller's `Refused` arm instead.
            tracing::debug!(
                leader = %leader_addr,
                node_id,
                code = ?status.code(),
                refusal = status.message(),
                "convergence: AddLearner was refused"
            );
            return classify(&status);
        }

        // Step 4: catch-up. The leader's own replication view is the authority
        // on how far behind this learner is — the local `known_committed` is
        // not, because a learner that has not yet received a single append
        // reads zero lag against its own frozen frontier.
        match client
            .cluster_status(pb::ClusterStatusRequest {
                history_id: history_id.to_vec(),
            })
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                let floor = resp.last_applied_index.saturating_sub(PROMOTION_LAG_MAX);
                let caught_up = resp
                    .replication
                    .iter()
                    .any(|p| p.node_id == node_id && p.matched_index >= floor);
                if !caught_up {
                    return JoinStep::Learner;
                }
            }
            Err(status) => return classify(&status),
        }

        // Step 5: promotion. The request names no removal — it has no field
        // for one — because a machine credential may never remove anyone
        // (ADR 0037 §7). Where a removal is warranted the leader folds it in
        // itself, under its own replication evidence.
        let promoted = client
            .promote_voter(pb::PromoteVoterRequest {
                history_id: history_id.to_vec(),
                promote_node_id: node_id,
            })
            .await;
        // The in-flight promotion instant, for the same reason and in the same
        // place as the admission one above: the seat may already be this
        // replica's while this replica still believes it is a learner.
        self.failpoints
            .halt_if_armed(crate::failpoints::JOIN_PROMOTE_VOTER_ISSUED)
            .await;
        match promoted {
            Ok(_) => JoinStep::Promoted,
            Err(status) => classify(&status),
        }
    }
}

/// Whether a probe answer is the responder's own claim to be the leader:
/// it knows its raft identity and its `leader_hint` names that very node.
/// The strongest leadership evidence a remote can offer, and the only kind
/// [`Convergence::find_leader`] accepts without corroboration.
fn claims_leadership(answer: &pb::ProbeClusterResponse) -> bool {
    answer.node_id.is_some() && answer.leader_hint == answer.node_id
}

/// The client-side cross-history refusal (ADR 0016), published to `/readyz`
/// when the cluster a stamped replica found carries a different raft history.
/// Prefixed with [`admin::HISTORY_CONFLICT`] — the same marker the server's
/// `history_conflict_status` uses — so status output reads consistently
/// whichever side detected the mismatch first, and [`classify`] would route
/// the server's version of this refusal identically.
fn cross_history_refusal(leader_addr: &str, cluster_history: &[u8], stamped: &[u8; 16]) -> String {
    format!(
        "{}: the cluster at {leader_addr} carries history {}, but this replica is stamped for \
         history {} — a cross-history join can never succeed; this is a wrong data volume or a \
         wiped-and-re-formed cluster, not something waiting fixes (ADR 0016 / ADR 0037 §3)",
        admin::HISTORY_CONFLICT,
        crate::formation::hex(cluster_history),
        crate::formation::hex(stamped),
    )
}

/// The history-superseded fail-stop message: what was observed, on which
/// channel, and what an operator does about it.
///
/// Both histories are named in full, because the first thing anyone reading
/// this will want is to check the surviving one against a backup. The two
/// remedies are spelled out rather than implied: this is the one refusal in the
/// lifecycle where *doing nothing* silently costs the fleet its data, since the
/// cluster it belongs to no longer exists to be rejoined.
fn supersession_refusal(observed_at: &str, channel: &str, observed: &str, stamped: &str) -> String {
    format!(
        "{HISTORY_SUPERSEDED}: {observed_at} reports a formed cluster with this \
         daemon's cluster_id but raft history {observed}, while this data directory is \
         stamped for history {stamped} — observed over {channel}. A history id is minted \
         once per formation, so this means the cluster was deliberately re-initialized and \
         the state on this volume predates it; the two histories can never merge (ADR 0016 / \
         ADR 0037 §3). Either restore the fleet from a backup of history {stamped}, or wipe \
         this data directory so this daemon enrolls into history {observed} as a new \
         installation — but not both, and not neither."
    )
}

/// Classify a membership refusal into a convergence step.
///
/// The admin surface flattens every refusal into a `tonic::Status`, and gRPC
/// has no code that distinguishes "you are behind, keep waiting" from "this
/// request is wrong and always will be" — so every membership refusal the
/// server constructs **begins with a stable marker** from the shared
/// vocabulary in [`crate::admin`], and the discrimination here is a prefix
/// match on those constants, never on ad-hoc prose. The marker set:
///
/// - [`admin::MACHINE_IDENTITY_CONFLICT`], [`admin::MACHINE_ADDRESS_CONFLICT`],
///   [`admin::ADDRESS_CONFLICT`], [`admin::UNKNOWN_NODE`],
///   [`admin::HISTORY_CONFLICT`] — terminal for the tick: surfaced as the
///   last admission refusal and backed off hard. (`unknown-node` on a
///   promotion means our own `AddLearner` was refused or reverted, not that
///   we are slow; `history-conflict` is the ADR 0016 wrong-volume case, which
///   no retry changes.)
/// - [`admin::VOTER_SET_FULL`], [`admin::NO_REMOVABLE_PEER`],
///   [`admin::QUORUM_AT_RISK`] — the seat is not available yet; stay a
///   caught-up learner and keep polling on the settled interval.
/// - [`admin::NO_KEY_HOLDER`], [`admin::KEY_UNAVAILABLE`],
///   [`admin::IDENTITY_RETIRED`] — terminal for the tick: CA-key custody
///   needs operator repair (ADR 0037 §4), or this identity was retired with
///   its seat and is never re-admitted (§7).
/// - [`admin::LEARNER_BEHIND`] — still catching up, poll at the fast cadence.
/// - [`admin::ENDPOINT_UNVERIFIED`] — surfaced *and* retried fast: transient
///   during fleet boot, permanent when `advertise_addr` is wrong, and only
///   status can tell an operator which (ADR 0037 §6/§9).
///
/// Codes are deliberately not consulted for the marked cases (the markers are
/// unambiguous, and e.g. `unknown-node` travels as `NOT_FOUND` while the rest
/// are `FAILED_PRECONDITION`); anything unmarked falls through to a retry,
/// which is the safe default because the verbs are idempotent.
fn classify(status: &tonic::Status) -> JoinStep {
    let message = status.message();

    // `no-removable-peer` and `quorum-at-risk` are the same situation from
    // the learner's side as a full voter set: the seat is not available *yet*,
    // nothing about the learner is wrong, and re-offering on the settled
    // interval is the expected behaviour (ADR 0037 §7).
    if [
        admin::VOTER_SET_FULL,
        admin::NO_REMOVABLE_PEER,
        admin::QUORUM_AT_RISK,
    ]
    .iter()
    .any(|marker| admin::has_marker(message, marker))
    {
        return JoinStep::VoterSetFull(message.to_string());
    }
    if [
        admin::MACHINE_IDENTITY_CONFLICT,
        admin::MACHINE_ADDRESS_CONFLICT,
        admin::ADDRESS_CONFLICT,
        admin::UNKNOWN_NODE,
        admin::HISTORY_CONFLICT,
        // Custody repair conditions and the one-seat-ever refusal: all
        // operator problems, none of them things polling resolves
        // (ADR 0037 §4/§7).
        admin::NO_KEY_HOLDER,
        admin::KEY_UNAVAILABLE,
        admin::IDENTITY_RETIRED,
    ]
    .iter()
    .any(|marker| admin::has_marker(message, marker))
    {
        return JoinStep::Refused(message.to_string());
    }
    if admin::has_marker(message, admin::ENDPOINT_UNVERIFIED) {
        return JoinStep::EndpointUnverified(message.to_string());
    }
    if admin::has_marker(message, admin::LEARNER_BEHIND) {
        return JoinStep::Learner;
    }
    JoinStep::Retry
}

/// Spread out ticks so a fleet that booted together does not dial the leader
/// in lockstep. Derived from the clock rather than any identity, because a
/// converging daemon may not have an identity yet.
fn jittered(base: Duration) -> Duration {
    let spread = base.as_millis() as u64 / 4;
    if spread == 0 {
        return base;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    base + Duration::from_millis(now % spread)
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_consensus::ConsensusError;
    use coppice_core::id::MachineId;

    // Every fixture below is built through the SAME formatting the server
    // uses (`admin::consensus_error_to_status` and the `admin::*_status`
    // helpers), never through hand-written prose: classification is a
    // contract between two halves of this crate, and building both sides
    // from one function is what stops them drifting apart again.

    #[test]
    fn a_full_voter_set_keeps_the_learner_polling() {
        // ADR 0037 §7: not terminal — the learner stays and waits for a
        // `ReplaceVoter` or an evidence-gated removal.
        let status = admin::consensus_error_to_status(ConsensusError::VoterSetFull {
            node: 7,
            voters: 3,
            cluster_size: 3,
        });
        assert!(matches!(classify(&status), JoinStep::VoterSetFull(_)));
    }

    #[test]
    fn an_address_conflict_is_terminal_and_surfaces_its_message() {
        let status = admin::consensus_error_to_status(ConsensusError::AddressConflict {
            node: 7,
            current: "a:1".into(),
            requested: "b:2".into(),
        });
        match classify(&status) {
            JoinStep::Refused(message) => assert!(message.contains("a:1"), "{message}"),
            _ => panic!("an address conflict must be terminal for the tick"),
        }
    }

    #[test]
    fn the_machine_binding_conflicts_are_terminal_and_surface_their_messages() {
        // ADR 0037 §7: a duplicated identity and a same-pair-new-address
        // re-admission are both operator problems, not things to poll at.
        let machine = MachineId::new();
        for status in [
            admin::machine_identity_conflict_status(machine, 7),
            admin::machine_address_conflict_status(machine, 7),
        ] {
            match classify(&status) {
                JoinStep::Refused(message) => {
                    assert!(message.contains(&machine.to_string()), "{message}")
                }
                _ => panic!("a machine-binding conflict must be terminal for the tick"),
            }
        }
    }

    #[test]
    fn a_full_set_with_no_dead_peer_also_keeps_the_learner_polling() {
        // ADR 0037 §7: "no removable peer" is the launch-before-terminate
        // steady state — the learner is fine, the seat simply is not free,
        // and it re-offers on the settled interval exactly as for a full
        // voter set. Same for a momentary loss of live-majority evidence.
        for status in [
            admin::consensus_error_to_status(ConsensusError::NoRemovablePeer {
                node: 7,
                voters: 3,
                cluster_size: 3,
            }),
            admin::consensus_error_to_status(ConsensusError::QuorumAtRisk {
                live: 1,
                continuing: 3,
            }),
        ] {
            assert!(
                matches!(classify(&status), JoinStep::VoterSetFull(_)),
                "must keep polling: {}",
                status.message()
            );
        }
    }

    #[test]
    fn custody_repair_and_retirement_are_terminal_for_the_tick() {
        // ADR 0037 §4/§7: a cluster that cannot keep a confirmed key holder,
        // a leader that cannot read its own key, and an identity retired with
        // its seat are all operator problems — polling resolves none of them.
        let key_unavailable = tonic::Status::failed_precondition(format!(
            "{}: this leader cannot read its own CA key (ADR 0037 §4)",
            admin::KEY_UNAVAILABLE
        ));
        for status in [
            admin::consensus_error_to_status(ConsensusError::NoKeyHolder),
            key_unavailable,
            admin::identity_retired_status(MachineId::new()),
        ] {
            assert!(
                matches!(classify(&status), JoinStep::Refused(_)),
                "must be terminal for the tick: {}",
                status.message()
            );
        }
    }

    #[test]
    fn an_unknown_node_on_promotion_is_terminal() {
        // Our own AddLearner was refused or reverted — nothing is replicating
        // to us, so polling the promotion would spin forever.
        let status = admin::consensus_error_to_status(ConsensusError::UnknownNode { node: 7 });
        assert!(matches!(classify(&status), JoinStep::Refused(_)));
    }

    #[test]
    fn a_cross_history_refusal_is_terminal_from_either_side() {
        // ADR 0016: two raft histories never merge, so both detections — the
        // server refusing a mismatched request, and the client's own pre-check
        // before AddLearner — must take the hard-backoff Refused path, never
        // fall through to the 300ms retry that used to hide this forever.
        let cluster = [0xAAu8; 16];
        let stamped = [0xBBu8; 16];

        let server = admin::history_conflict_status(&cluster, &stamped);
        match classify(&server) {
            JoinStep::Refused(message) => {
                assert!(message.contains(&"aa".repeat(16)), "{message}");
                assert!(message.contains(&"bb".repeat(16)), "{message}");
            }
            _ => panic!("the server's cross-history refusal must be terminal for the tick"),
        }

        // The client-side message wears the same marker, so a status page
        // reads consistently whichever side saw the mismatch first — and so
        // it would classify identically if it ever traveled as a status.
        let client = cross_history_refusal("c1:7071", &cluster, &stamped);
        assert!(
            admin::has_marker(&client, admin::HISTORY_CONFLICT),
            "{client}"
        );
        assert!(matches!(
            classify(&tonic::Status::failed_precondition(client)),
            JoinStep::Refused(_)
        ));
    }

    #[test]
    fn leadership_is_claimed_only_by_a_node_naming_itself() {
        let answer = |node_id: Option<u64>, leader_hint: Option<u64>| pb::ProbeClusterResponse {
            cluster_id: "c".into(),
            history_id: vec![0; 16],
            initialized: true,
            node_id,
            leader_hint,
            voters: Vec::new(),
        };
        // The self-claim find_leader validates hints against.
        assert!(claims_leadership(&answer(Some(3), Some(3))));
        // A follower pointing elsewhere is a belief, not evidence.
        assert!(!claims_leadership(&answer(Some(2), Some(3))));
        // No leader known, and no identity at all: neither can claim anything.
        assert!(!claims_leadership(&answer(Some(2), None)));
        assert!(!claims_leadership(&answer(None, None)));
    }

    #[test]
    fn an_unverified_endpoint_is_surfaced_but_keeps_the_fast_cadence() {
        // ADR 0037 §6: transient during fleet boot, permanent when
        // `advertise_addr` is wrong — so it must reach status without paying
        // the hard refusal backoff.
        let status = admin::endpoint_unverified_status("dialing c1:7071 to probe it: refused");
        match classify(&status) {
            JoinStep::EndpointUnverified(message) => {
                assert!(message.contains("c1:7071"), "{message}")
            }
            _ => panic!("an unverified endpoint takes the surfaced-but-fast middle path"),
        }
    }

    #[test]
    fn a_behind_learner_keeps_polling_and_a_redirect_retries() {
        let behind =
            admin::consensus_error_to_status(ConsensusError::LearnerNotCaughtUp { lag: 12 });
        assert!(matches!(classify(&behind), JoinStep::Learner));

        let not_leader =
            admin::consensus_error_to_status(ConsensusError::NotLeader { leader: Some(3) });
        assert!(matches!(classify(&not_leader), JoinStep::Retry));

        // An unrecognized refusal must retry, never wedge: the verbs are
        // idempotent, so a wrong guess costs one dial.
        let unknown = tonic::Status::unavailable("connection reset");
        assert!(matches!(classify(&unknown), JoinStep::Retry));
    }

    #[test]
    fn a_marker_in_a_human_tail_does_not_classify() {
        // The prefix discipline: markers count only at the head of the
        // message, so prose that merely mentions one cannot misroute a tick.
        let status = tonic::Status::failed_precondition(
            "the last attempt ended voter-set-full: retrying later",
        );
        assert!(matches!(classify(&status), JoinStep::Retry));
    }

    #[test]
    fn jitter_stays_within_a_quarter_of_the_base() {
        let pacing = PacingConfig::default();
        for base in [
            pacing.probe_interval,
            pacing.settled_interval,
            pacing.refusal_backoff,
        ] {
            let jittered = jittered(base);
            assert!(jittered >= base);
            assert!(jittered < base + base / 4 + Duration::from_millis(1));
        }
    }
}
