//! The agent's liveness and readiness state (ADR 0041).
//!
//! A single shared snapshot the [`session`](crate::session) writes and the
//! operational listener ([`metrics_server`](crate::metrics_server)) reads.
//! The dependency runs one way: the session knows nothing about HTTP, and the
//! server knows nothing about the session. The handle is `None` for every
//! session that was never given one (the unit suite, `coppice dev`).
//!
//! `/healthz` is the *liveness* probe and answers 200 unconditionally while
//! the process is serving. `/readyz` is the *readiness* probe; its phase is
//! decided by this fixed precedence, first arm that holds wins:
//!
//! 1. **`draining`** — the shutdown flag is set. Wins over everything else,
//!    including a healthy registered session, since `running` is the work a
//!    lifecycle hook polls down to zero.
//! 2. **`starting` / `reconnecting`** — not registered on a live stream;
//!    `starting` before the first registration of this process's life,
//!    `reconnecting` after.
//! 3. **`docker-unavailable`** — registered, but the most recent `observe()`
//!    failed.
//! 4. **`ready`** — registered on a live session with a reachable daemon. The
//!    only phase that answers 200.

use std::sync::{Arc, Mutex};

use coppice_core::id::NodeId;

/// The shared health snapshot: a handle the session updates and the probe
/// endpoints read. Cloning shares one state.
#[derive(Clone)]
pub struct AgentHealth(Arc<Mutex<State>>);

#[derive(Debug)]
struct State {
    node: NodeId,
    registered: bool,
    ever_registered: bool,
    docker_ok: bool,
    draining: bool,
    running: usize,
}

/// One rendered readiness answer. Serialized as the `/readyz` body; `phase`
/// also decides the status code ([`Readiness::is_ready`]).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Readiness {
    pub phase: Phase,
    pub node_id: NodeId,
    pub registered: bool,
    pub docker_ok: bool,
    pub draining: bool,
    /// Allocations this agent is still accountable for — the number a drain
    /// waits down to zero.
    pub running: usize,
    /// One human sentence saying why this phase, for an operator reading a
    /// failed probe out of a load balancer's logs.
    pub reason: &'static str,
}

/// The readiness phases of ADR 0041's agent table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    Starting,
    Ready,
    Reconnecting,
    DockerUnavailable,
    Draining,
}

impl Readiness {
    /// Whether this phase is a 200. Only `ready` is.
    pub fn is_ready(&self) -> bool {
        self.phase == Phase::Ready
    }
}

impl AgentHealth {
    /// A fresh handle for `node`, in the `starting` phase: nothing registered
    /// yet, and Docker assumed reachable (startup already failed if it was
    /// not, so the first `observe()` is the first thing that can say
    /// otherwise).
    pub fn new(node: NodeId) -> AgentHealth {
        AgentHealth(Arc::new(Mutex::new(State {
            node,
            registered: false,
            ever_registered: false,
            docker_ok: true,
            draining: false,
            running: 0,
        })))
    }

    fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut state)
    }

    /// Registration completed on a live stream (`RegisterAccepted`), or the
    /// session was lost. The first `true` is also what separates `starting`
    /// from `reconnecting` for the rest of the process's life.
    pub fn set_registered(&self, registered: bool) {
        self.with(|s| {
            s.registered = registered;
            s.ever_registered |= registered;
        });
    }

    /// The outcome of the most recent `observe()` — the agent's only continuous
    /// evidence that its container runtime is still there.
    pub fn set_docker_ok(&self, ok: bool) {
        self.with(|s| s.docker_ok = ok);
    }

    /// The shutdown announcement (ADR 0041); set once, at the flip.
    pub fn set_draining(&self, draining: bool) {
        self.with(|s| s.draining = draining);
    }

    /// How much accountable live work is outstanding
    /// ([`crate::session::outstanding_live_work`]).
    pub fn set_running(&self, running: usize) {
        self.with(|s| s.running = running);
    }

    /// Render the current readiness answer under the precedence documented at
    /// the top of this module.
    pub fn readiness(&self) -> Readiness {
        self.with(|s| {
            let (phase, reason) = if s.draining {
                (
                    Phase::Draining,
                    "shutting down: draining, not accepting new placements",
                )
            } else if !s.registered {
                if s.ever_registered {
                    (
                        Phase::Reconnecting,
                        "session lost; the reconnect loop is running",
                    )
                } else {
                    (
                        Phase::Starting,
                        "not yet registered with a coordinator since startup",
                    )
                }
            } else if !s.docker_ok {
                (
                    Phase::DockerUnavailable,
                    "registered, but the last container observation failed",
                )
            } else {
                (
                    Phase::Ready,
                    "registered on a live session; container runtime reachable",
                )
            };
            Readiness {
                phase,
                node_id: s.node,
                registered: s.registered,
                docker_ok: s.docker_ok,
                draining: s.draining,
                running: s.running,
                reason,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health() -> AgentHealth {
        AgentHealth::new(NodeId::new())
    }

    #[test]
    fn a_fresh_agent_is_starting_and_never_reconnecting() {
        let health = health();
        assert_eq!(health.readiness().phase, Phase::Starting);
        assert!(!health.readiness().is_ready());
    }

    #[test]
    fn registration_makes_it_ready_and_a_lost_session_reconnecting() {
        let health = health();
        health.set_registered(true);
        assert_eq!(health.readiness().phase, Phase::Ready);
        assert!(health.readiness().is_ready());

        // The same "not registered" stream state as boot, told apart only by
        // having registered once already.
        health.set_registered(false);
        assert_eq!(health.readiness().phase, Phase::Reconnecting);
    }

    #[test]
    fn a_failed_observation_is_reported_only_while_registered() {
        let health = health();
        health.set_docker_ok(false);
        // Not registered yet: the session state is the more useful answer.
        assert_eq!(health.readiness().phase, Phase::Starting);
        health.set_registered(true);
        assert_eq!(health.readiness().phase, Phase::DockerUnavailable);
        health.set_docker_ok(true);
        assert_eq!(health.readiness().phase, Phase::Ready);
    }

    #[test]
    fn draining_wins_over_every_other_phase() {
        let health = health();
        health.set_registered(true);
        health.set_draining(true);
        health.set_running(2);
        let readiness = health.readiness();
        assert_eq!(readiness.phase, Phase::Draining);
        assert_eq!(readiness.running, 2);
        assert!(readiness.registered, "a draining agent is still registered");

        // ...including over a lost session and a dead daemon.
        health.set_registered(false);
        health.set_docker_ok(false);
        assert_eq!(health.readiness().phase, Phase::Draining);
    }

    #[test]
    fn phases_serialize_kebab_case() {
        let health = health();
        health.set_registered(true);
        health.set_docker_ok(false);
        let body = serde_json::to_value(health.readiness()).expect("serialize readiness");
        assert_eq!(body["phase"], "docker-unavailable");
        assert_eq!(body["registered"], true);
        assert_eq!(body["docker_ok"], false);
        assert_eq!(body["draining"], false);
        assert_eq!(body["running"], 0);
        assert!(body["reason"].is_string());
        assert!(body["node_id"].is_string());
    }
}
