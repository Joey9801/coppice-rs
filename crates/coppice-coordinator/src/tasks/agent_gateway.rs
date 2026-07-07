//! Agent gateway: the session manager plus (future) per-session tasks.
//!
//! Per `docs/architecture/coordinator-runtime.md` ("Agent gateway"),
//! sessions terminate on the leader only: a follower has nothing useful to
//! do with an agent connection, since every inbound report must be
//! normalized and proposed by the leader anyway. The manager task here
//! self-gates *new session acceptance* on leadership implicitly (there is no
//! accept loop yet — see [`run_session`]) and drops every live session the
//! moment leadership is lost.
//!
//! No transport dependency has landed, so the accept loop that would
//! terminate real sockets is not built: [`run_session`] is a stub showing
//! the shape a real accept loop will spawn once one exists.

use std::collections::BTreeMap;

use tokio::sync::{mpsc, watch};

use coppice_consensus::ConsensusStatus;
use coppice_core::id::NodeId;
use coppice_proto::pb::agent::v1::{AgentCommand, AgentReport};

use crate::limits::COMMAND_ROUTER_CAPACITY;

/// One command routed to a node's agent session.
pub struct RouteCommand {
    pub node: NodeId,
    pub command: AgentCommand,
}

/// The command router is gone: the session manager task has shut down.
#[derive(Debug, Clone, Copy)]
pub struct RouterClosed;

impl std::fmt::Display for RouterClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "agent command router is closed")
    }
}

impl std::error::Error for RouterClosed {}

/// Cloneable handle to the session manager's command router
/// (`COMMAND_ROUTER_CAPACITY`, `send().await` — producers are leader-only
/// loops that tolerate this backpressure per the channel inventory).
#[derive(Clone)]
pub struct RouterHandle {
    tx: mpsc::Sender<RouteCommand>,
}

impl RouterHandle {
    pub async fn send(&self, command: RouteCommand) -> Result<(), RouterClosed> {
        self.tx.send(command).await.map_err(|_| RouterClosed)
    }
}

/// A report from a node agent, decoded off the wire and handed to
/// ingestion. Constructed by [`run_session`] once a real transport exists.
#[allow(dead_code)] // shape for the future accept loop; see `run_session`.
pub struct InboundReport {
    pub node: NodeId,
    pub report: AgentReport,
}

/// The manager's per-session bookkeeping. Only the outbound half is kept
/// here: inbound reports flow into the shared inbound channel directly
/// (never back through the manager).
#[allow(dead_code)] // constructed by a future accept loop; see `run_session`.
struct SessionHandle {
    outbound: mpsc::Sender<AgentCommand>,
}

/// Spawn the session manager. `inbound` is the shared sender side of the
/// agent-inbound channel (`AGENT_INBOUND_CAPACITY`, created by
/// `crate::runtime`): the manager holds it so a future accept loop can clone
/// it into each session it spawns via [`run_session`]. Returns the router
/// handle other leader-only tasks route agent commands through, plus the
/// manager's `JoinHandle`.
pub fn spawn(
    inbound: mpsc::Sender<InboundReport>,
    status: watch::Receiver<ConsensusStatus>,
    shutdown: watch::Receiver<bool>,
) -> (RouterHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(COMMAND_ROUTER_CAPACITY);
    let handle = RouterHandle { tx };
    let join = tokio::spawn(run_manager(rx, inbound, status, shutdown));
    (handle, join)
}

async fn run_manager(
    mut router_rx: mpsc::Receiver<RouteCommand>,
    // Held for a future accept loop to clone into new sessions; the manager
    // itself never sends on it.
    _inbound: mpsc::Sender<InboundReport>,
    mut status: watch::Receiver<ConsensusStatus>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut sessions: BTreeMap<NodeId, SessionHandle> = BTreeMap::new();

    loop {
        tokio::select! {
            biased;
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            result = status.changed() => {
                if result.is_err() {
                    break;
                }
                if !status.borrow().role.is_leader() && !sessions.is_empty() {
                    tracing::info!(
                        count = sessions.len(),
                        "agent gateway: leadership lost, dropping sessions"
                    );
                    sessions.clear();
                }
            }
            routed = router_rx.recv() => {
                let Some(routed) = routed else { break };
                match sessions.get(&routed.node) {
                    Some(session) if session.outbound.try_send(routed.command).is_ok() => {}
                    Some(_) => {
                        tracing::warn!(
                            node = %routed.node,
                            "agent gateway: outbound queue full, disconnecting session"
                        );
                        sessions.remove(&routed.node);
                    }
                    None => {
                        tracing::debug!(
                            node = %routed.node,
                            "agent gateway: no session for node, dropping command"
                        );
                    }
                }
            }
        }
    }
    tracing::info!("agent gateway: shutting down, dropping sessions");
    // Dropping `sessions` here closes every outbound channel.
}

/// The shape a real accept loop will spawn per accepted connection, once a
/// transport exists. Not called today, so `run_manager`'s session registry
/// stays empty until a real accept loop is wired in.
#[allow(dead_code)]
async fn run_session(
    node: NodeId,
    inbound: mpsc::Sender<InboundReport>,
    mut outbound: mpsc::Receiver<AgentCommand>,
) {
    // Read half: decode wire reports and hand them to ingestion. The
    // `send().await` here is itself the "agent inbound" channel's
    // backpressure policy: a full channel stalls this socket read, which
    // stalls the agent over TCP — never apply
    // (`docs/architecture/coordinator-runtime.md`).
    let report: Option<AgentReport> = decode_next_report();
    if let Some(report) = report {
        if inbound.send(InboundReport { node, report }).await.is_err() {
            return;
        }
    }

    // Write half: drain the outbound mpsc (`AGENT_OUTBOUND_CAPACITY`) onto
    // the socket.
    while let Some(command) = outbound.recv().await {
        write_to_socket(command);
    }
}

/// Deferred: decoding a wire `AgentReport` needs a real socket to read from.
#[allow(dead_code)]
fn decode_next_report() -> Option<AgentReport> {
    todo!("decode a wire AgentReport off the session socket")
}

/// Deferred: encoding needs a real socket to write to.
#[allow(dead_code)]
fn write_to_socket(command: AgentCommand) {
    let _ = command;
    todo!("encode and write an AgentCommand to the session socket")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn route_to_unknown_node_is_a_no_op() {
        let (_inbound_tx, _inbound_rx) = mpsc::channel(1);
        let (status_tx, status_rx) = watch::channel(ConsensusStatus {
            id: 1,
            role: coppice_consensus::Role::Leader { term: 1 },
            last_applied: 0,
            known_committed: 0,
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (router, join) = spawn(_inbound_tx, status_rx, shutdown_rx);

        // No session is registered for this node (no transport yet), so
        // routing to it is dropped rather than panicking or blocking.
        let node = NodeId::new();
        let command = AgentCommand { header: None, body: None };
        router.send(RouteCommand { node, command }).await.expect("router accepts the send");

        let _ = shutdown_tx.send(true);
        let _ = status_tx; // keep the sender alive until shutdown is observed
        join.await.expect("manager task shuts down cleanly");
    }
}
