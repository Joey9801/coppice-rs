//! Per-daemon crash injection for the join pipeline (ADR 0037 §6).
//!
//! # Why this exists at all
//!
//! ADR 0037 §6's claim is that an interrupted join needs no cleanup: kill the
//! joiner anywhere in `AddLearner` → catch-up → `PromoteVoter` and a restart
//! re-presents the same identity and converges. Testing "anywhere" means being
//! able to stop a daemon **at a named line of the loop**, and two of those
//! lines cannot be reached by racing a poller against a healthy cluster: the
//! instant after an RPC has been issued but before its outcome is observed is,
//! under a leader that answers in microseconds, not a state any external
//! observer can catch.
//!
//! # Why not the `COPPICE_TEST_FAILPOINT` env var
//!
//! The older mechanism ([`crate::admin`]'s, and `rotate`'s) arms a
//! **process-global, fire-once** latch from an environment variable. That is
//! serviceable for a test that stages one crash in one daemon and then ends,
//! which is what `key_custody.rs` and `rotate_ca_durability.rs` do. It is
//! unusable here: the integration harness runs a whole fleet inside one test
//! process, so a process-global latch is armed for *every* daemon at once
//! (including the leader, which drives the very RPCs the joiner is being
//! stopped around), and fire-once means the second iteration of a looping
//! parameterized test silently stages nothing.
//!
//! So a failpoint here is **carried by the daemon's own config** — the one
//! thing in this architecture that is already per-daemon and already threaded
//! everywhere a daemon's behaviour is decided. Arming a joiner cannot arm its
//! leader, and re-arming a fresh daemon in the next loop iteration is a fresh
//! config, not a latch to reset.
//!
//! # Why it can never load in a production build
//!
//! `[pacing]` and `[token_kdf]` are test-*shaped* but production-legal: their
//! extremes are bad settings, not impossible ones. A failpoint is different in
//! kind — there is no deployment for which "stop converging, permanently" is a
//! setting — so the section is refused outright unless the binary was built
//! with `debug_assertions`, at config load, before anything binds
//! ([`crate::config::TestFailpointConfig::validate`]). A release coordinator
//! handed a config carrying `[test_failpoints]` fail-stops naming the section.
//! `debug_assertions` rather than a cargo feature because the integration
//! suites that need this are ordinary `cargo test` targets of this same crate:
//! a feature would have to be enabled by a dev-dependency on the crate itself,
//! or hidden behind `required-features`, which would take these suites out of
//! a default `cargo test -p coppice-coordinator` run — precisely the tests
//! least worth making opt-in.
//!
//! # What "halt" means
//!
//! The armed daemon **stops converging, forever, at that exact await**: it
//! writes a marker file naming the failpoint and then parks on a future that
//! never completes. Nothing after the failpoint's line runs — the response
//! sitting in the local variable is never inspected, and no later tick is ever
//! taken — which is the property the surrounding tests need and the one a
//! sleep-and-hope staging cannot promise.
//!
//! The marker file (not a log line, not a `/readyz` field) is the harness's
//! evidence, for two reasons: it costs the production surfaces nothing, and it
//! is *durable*, so the observing side has no race to lose. The rest of the
//! daemon keeps serving, so a harness that sees the marker can read `/readyz`
//! for the state at the halt and then kill the process abruptly with its
//! ordinary crash-injection primitive — a real teardown at a provable line,
//! rather than a graceful drain.

use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Halt with the identity stamped and consensus running under `Join`, before
/// the joiner has issued `AddLearner` at all: the cluster has never heard of
/// this replica, and its own disk already commits it to one identity.
pub const JOIN_BEFORE_ADD_LEARNER: &str = "join-before-add-learner";

/// Halt with `AddLearner` **issued and answered but never observed** — the
/// admission may be committed cluster-side while the joiner holds no record of
/// asking. The half of ADR 0037 §6's idempotency claim that says a second
/// `AddLearner` for the same identity at the same address is a no-op success,
/// staged from the only side that can stage it.
pub const JOIN_ADD_LEARNER_ISSUED: &str = "join-add-learner-issued";

/// The same instant one verb later: `PromoteVoter` issued and answered, its
/// outcome never observed. The in-flight promotion — between "a caught-up
/// learner asked" and "this replica knows it is a voter".
pub const JOIN_PROMOTE_VOTER_ISSUED: &str = "join-promote-voter-issued";

/// Every name `[test_failpoints] halt_at` accepts. An unknown name is a config
/// error rather than a silently inert setting: a test whose failpoint was
/// renamed out from under it must fail loudly, not pass having staged nothing.
pub const ALL: [&str; 3] = [
    JOIN_BEFORE_ADD_LEARNER,
    JOIN_ADD_LEARNER_ISSUED,
    JOIN_PROMOTE_VOTER_ISSUED,
];

/// Hold a job submission in the gap between the API layer's authorization
/// pre-check and the proposal that follows it (ADR 0023).
///
/// A **gate**, not a halt: the daemon parks at this line until the test
/// releases it, then carries on and proposes normally. It exists because
/// ADR 0023's revocation-race guarantee — a command whose pre-check passed
/// against bindings that were then replaced is refused at apply, in log
/// order — is a claim about an interleaving no external observer can stage.
/// The pre-check reads an *eventual* view and the proposal lands wherever the
/// leader puts it; "pre-check first, revocation second, proposal third" is
/// otherwise a timing hope, and a sleep long enough to make it likely is both
/// slow and still not a proof.
///
/// With the gate, the test holds the window open explicitly: it waits for the
/// reached marker (so the pre-check has demonstrably run and passed), commits
/// the `UpdateAuthorization` and reads back *its* log index, then releases —
/// so the submission's own log position is provably later than the
/// revocation's.
pub const API_SUBMIT_BEFORE_PROPOSE: &str = "api-submit-before-propose";

/// Every name `[test_failpoints] gate_at` accepts, on the same terms as
/// [`ALL`]: an unknown name is a config error, never a silently inert setting.
pub const ALL_GATES: [&str; 1] = [API_SUBMIT_BEFORE_PROPOSE];

/// Where a halted daemon records that it reached `name`, inside its own data
/// directory. Public so the integration harness computes the same path from
/// the same constant instead of copying a format string.
pub fn halt_marker(data_dir: &Path, name: &str) -> PathBuf {
    data_dir.join(format!("failpoint-{name}.halted"))
}

/// Where a daemon parked at the gate `name` records that it got there — the
/// file the test waits on before staging anything behind its back.
pub fn gate_reached_marker(data_dir: &Path, name: &str) -> PathBuf {
    data_dir.join(format!("failpoint-{name}.reached"))
}

/// The file whose existence releases a daemon parked at the gate `name` — the
/// test writes it when it is done staging.
pub fn gate_release_marker(data_dir: &Path, name: &str) -> PathBuf {
    data_dir.join(format!("failpoint-{name}.release"))
}

/// How often a parked daemon looks for its release file. Short enough that
/// the release is not itself a source of test latency; a poll rather than a
/// notification because the observer is another *process's* view of a
/// directory in the general case, and inotify-shaped machinery would be a
/// dependency bought for a debug-only path.
const GATE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// How long a parked daemon waits before giving up and carrying on. A gate
/// that is never released is a broken test, and the failure it should produce
/// is that test's own assertion — not a wedged CI job whose log says nothing.
const GATE_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// One daemon's armed failpoints, as its config declared them.
///
/// [`Failpoints::default`] is the disarmed value every real deployment gets
/// (and every release build gets unconditionally, since the section cannot
/// load there), and it holds no allocation: the check on the hot path is one
/// `Option` discriminant.
///
/// Public only because it crosses the `bootstrap::serve_runtime*` seams on its
/// way from config to the write path; nothing outside this crate can arm one,
/// since [`Failpoints::new`] is crate-private and the config section that
/// calls it refuses to load in a release build.
#[derive(Clone, Debug, Default)]
pub struct Failpoints(Option<Arc<Armed>>);

#[derive(Debug)]
struct Armed {
    /// `[test_failpoints] halt_at` — park forever.
    halt_at: Vec<String>,
    /// `[test_failpoints] gate_at` — park until released.
    gate_at: Vec<String>,
    data_dir: PathBuf,
}

impl Failpoints {
    /// The armed set for a daemon whose config carries `[test_failpoints]`.
    /// Two empty lists are disarmed — the section exists, but names nothing.
    pub(crate) fn new(halt_at: &[String], gate_at: &[String], data_dir: &Path) -> Failpoints {
        if halt_at.is_empty() && gate_at.is_empty() {
            return Failpoints(None);
        }
        Failpoints(Some(Arc::new(Armed {
            halt_at: halt_at.to_vec(),
            gate_at: gate_at.to_vec(),
            data_dir: data_dir.to_path_buf(),
        })))
    }

    fn armed(&self, name: &str) -> Option<&Armed> {
        self.0
            .as_deref()
            .filter(|armed| armed.halt_at.iter().any(|n| n == name))
    }

    fn gate_armed(&self, name: &str) -> Option<&Armed> {
        self.0
            .as_deref()
            .filter(|armed| armed.gate_at.iter().any(|n| n == name))
    }

    /// Whether `name` would fire, without firing it — the only way to assert
    /// on an armed set, since the firing path never returns.
    #[cfg(test)]
    pub(crate) fn is_armed_for_tests(&self, name: &str) -> bool {
        self.armed(name).is_some()
    }

    /// Whether the gate `name` would fire, without firing it.
    #[cfg(test)]
    pub(crate) fn is_gate_armed_for_tests(&self, name: &str) -> bool {
        self.gate_armed(name).is_some()
    }

    /// Park here until the test releases this gate, if `name` is armed;
    /// otherwise return immediately.
    ///
    /// Unlike [`halt_if_armed`](Self::halt_if_armed) this *does* return, and
    /// everything after the call site runs exactly as it would have — the gate
    /// moves a line's execution later in wall-clock time and changes nothing
    /// else. One request at a time: a second caller arriving while the first
    /// is parked would share the same two files, which is why the only gate
    /// that exists sits on a path a test drives with a single in-flight
    /// request.
    pub(crate) async fn gate_if_armed(&self, name: &'static str) {
        let Some(armed) = self.gate_armed(name) else {
            return;
        };
        let reached = gate_reached_marker(&armed.data_dir, name);
        let release = gate_release_marker(&armed.data_dir, name);

        // A release left behind by an earlier pass would wave this one
        // straight through, which is the one way this mechanism could fail
        // *silently* — the test would see a released gate it never released
        // and conclude an interleaving it never staged.
        let _ = std::fs::remove_file(&release);
        if let Err(e) = std::fs::write(&reached, name) {
            tracing::error!(
                failpoint = name,
                marker = %reached.display(),
                error = %e,
                "test failpoint: could not write the gate's reached marker"
            );
        }
        tracing::warn!(
            failpoint = name,
            "test failpoint: parked at a gate until released (test-only; \
             [test_failpoints] cannot load in a release build)"
        );

        let deadline = std::time::Instant::now() + GATE_MAX_WAIT;
        while !release.exists() {
            if std::time::Instant::now() >= deadline {
                tracing::error!(
                    failpoint = name,
                    "test failpoint: gate never released; carrying on so the \
                     test fails on its own assertion rather than hanging"
                );
                break;
            }
            tokio::time::sleep(GATE_POLL_INTERVAL).await;
        }
        let _ = std::fs::remove_file(&reached);
    }

    /// Stop this daemon's convergence permanently if `name` is armed;
    /// otherwise return immediately.
    ///
    /// Never returns when it fires. Callers therefore place it *exactly* where
    /// the crash is meant to land — in particular, after an RPC's `.await` and
    /// before the result is looked at, which is what makes "the request was
    /// issued and its outcome was never observed" a reachable state rather
    /// than a timing hope.
    pub(crate) async fn halt_if_armed(&self, name: &'static str) {
        let Some(armed) = self.armed(name) else {
            return;
        };
        let marker = halt_marker(&armed.data_dir, name);
        if let Err(e) = std::fs::write(&marker, name) {
            // The harness waits on this file, so failing to write it turns a
            // deterministic staging into a hang; say so at a level nobody can
            // filter out before parking anyway.
            tracing::error!(
                failpoint = name,
                marker = %marker.display(),
                error = %e,
                "test failpoint: could not write the halt marker"
            );
        }
        tracing::error!(
            failpoint = name,
            "test failpoint: halting convergence here, permanently (test-only; \
             [test_failpoints] cannot load in a release build)"
        );
        std::future::pending::<()>().await;
    }
}
