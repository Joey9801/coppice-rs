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

/// Where a halted daemon records that it reached `name`, inside its own data
/// directory. Public so the integration harness computes the same path from
/// the same constant instead of copying a format string.
pub fn halt_marker(data_dir: &Path, name: &str) -> PathBuf {
    data_dir.join(format!("failpoint-{name}.halted"))
}

/// One daemon's armed failpoints, as its config declared them.
///
/// [`Failpoints::default`] is the disarmed value every real deployment gets
/// (and every release build gets unconditionally, since the section cannot
/// load there), and it holds no allocation: the check on the hot path is one
/// `Option` discriminant.
#[derive(Clone, Debug, Default)]
pub(crate) struct Failpoints(Option<Arc<Armed>>);

#[derive(Debug)]
struct Armed {
    names: Vec<String>,
    data_dir: PathBuf,
}

impl Failpoints {
    /// The armed set for a daemon whose config carries `[test_failpoints]`.
    /// An empty list is disarmed — the section exists, but names nothing.
    pub(crate) fn new(names: &[String], data_dir: &Path) -> Failpoints {
        if names.is_empty() {
            return Failpoints(None);
        }
        Failpoints(Some(Arc::new(Armed {
            names: names.to_vec(),
            data_dir: data_dir.to_path_buf(),
        })))
    }

    fn armed(&self, name: &str) -> Option<&Armed> {
        self.0
            .as_deref()
            .filter(|armed| armed.names.iter().any(|n| n == name))
    }

    /// Whether `name` would fire, without firing it — the only way to assert
    /// on an armed set, since the firing path never returns.
    #[cfg(test)]
    pub(crate) fn is_armed_for_tests(&self, name: &str) -> bool {
        self.armed(name).is_some()
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
