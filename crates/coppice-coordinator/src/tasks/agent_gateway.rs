//! Agent gateway: the session manager, the per-session pumps, and the tonic
//! `AgentService` handler.
//!
//! Per `docs/architecture/coordinator-runtime.md` ("Agent gateway"), sessions
//! terminate on the leader only: a follower has nothing useful to do with an
//! agent connection, since every inbound report must be normalized and
//! proposed by the leader anyway. The handler refuses a non-leader with a
//! leader hint; the manager drops every live session the moment leadership is
//! lost.
//!
//! One bidirectional stream per agent (`coppice.agent.v1.AgentService`, ADR
//! 0009/0011): reports up, commands down. The manager is the single writer of
//! the per-node `command_seq` and the fencing-token stamper — a
//! [`RouteCommand`] arrives header-less and leaves stamped with
//! `(leader_term, node_epoch, command_seq)` (ADR 0009).

// tonic's service trait and the accept path return `Result<_, Status>`;
// `Status` is a large error type, so its `Err` variant is unavoidably big.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

use coppice_consensus::{ConsensusStatus, Role, StateViews};
use coppice_core::id::NodeId;
use coppice_net::session::AgentService;
use coppice_proto::pb::agent::v1::{AgentCommand, AgentReport, CommandHeader, FencingToken};

use crate::limits::{AGENT_OUTBOUND_CAPACITY, COMMAND_ROUTER_CAPACITY, SESSION_CONTROL_CAPACITY};

/// One command routed to a node's agent session. `command.header` is `None`
/// on the way in — the manager stamps the fencing token as it routes.
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

/// Cloneable handle to the session manager's command router.
///
/// Capacity: `COMMAND_ROUTER_CAPACITY`; uses `send().await` — producers are
/// leader-only loops that tolerate this backpressure per the channel inventory.
#[derive(Clone)]
pub struct RouterHandle {
    tx: mpsc::Sender<RouteCommand>,
}

impl RouterHandle {
    pub async fn send(&self, command: RouteCommand) -> Result<(), RouterClosed> {
        self.tx.send(command).await.map_err(|_| RouterClosed)
    }

    /// A router handle and its receiver, for tests that capture routed commands
    /// without spinning up the session manager.
    #[cfg(test)]
    pub(crate) fn channel_for_test() -> (RouterHandle, mpsc::Receiver<RouteCommand>) {
        let (tx, rx) = mpsc::channel(COMMAND_ROUTER_CAPACITY);
        (RouterHandle { tx }, rx)
    }
}

/// A report from a node agent, decoded off the wire and handed to ingestion.
pub struct InboundReport {
    pub node: NodeId,
    pub report: AgentReport,
}

/// Session lifecycle registrations from the per-session pumps to the manager.
///
/// The manager is the sole owner of the session registry; the pumps only ask
/// it to open or close an entry. `session_id` guards `Close` so a replaced
/// session's late close cannot evict its successor.
enum SessionControl {
    Open {
        node: NodeId,
        session_id: u64,
        outbound: mpsc::Sender<AgentCommand>,
    },
    Close {
        node: NodeId,
        session_id: u64,
    },
}

/// The manager's per-node bookkeeping: the current session's outbound queue
/// and the id identifying which session owns it.
struct Session {
    session_id: u64,
    outbound: mpsc::Sender<AgentCommand>,
}

/// The spawned gateway's handles.
pub struct Gateway {
    /// Route agent commands to sessions (used by dispatch and ingestion).
    pub router: RouterHandle,
    /// Accept and register agent sessions (used by the tonic service).
    pub authority: SessionAuthority,
    /// The session manager task.
    pub join: JoinHandle<()>,
}

/// Cloneable handle the tonic `AgentService` uses to accept sessions: it
/// carries the shared inbound sender, the session-control channel, the status
/// watch (for the leader check and fencing term), and the session-id counter.
#[derive(Clone)]
pub struct SessionAuthority {
    inbound: mpsc::Sender<InboundReport>,
    control: mpsc::Sender<SessionControl>,
    status: watch::Receiver<ConsensusStatus>,
    next_session_id: Arc<AtomicU64>,
}

/// Spawn the session manager.
///
/// `inbound` is the shared sender side of the agent-inbound channel
/// (`AGENT_INBOUND_CAPACITY`, created by `crate::runtime`): the authority
/// clones it into each session it accepts. `views` lets the manager read each
/// node's current epoch when it stamps a fencing token. Returns the router,
/// the accept authority, and the manager's `JoinHandle`.
pub fn spawn(
    inbound: mpsc::Sender<InboundReport>,
    views: StateViews,
    status: watch::Receiver<ConsensusStatus>,
    shutdown: watch::Receiver<bool>,
) -> Gateway {
    let (router_tx, router_rx) = mpsc::channel(COMMAND_ROUTER_CAPACITY);
    let (control_tx, control_rx) = mpsc::channel(SESSION_CONTROL_CAPACITY);
    let router = RouterHandle { tx: router_tx };
    let authority = SessionAuthority {
        inbound,
        control: control_tx,
        status: status.clone(),
        next_session_id: Arc::new(AtomicU64::new(1)),
    };
    let join = tokio::spawn(run_manager(router_rx, control_rx, views, status, shutdown));
    Gateway {
        router,
        authority,
        join,
    }
}

async fn run_manager(
    mut router_rx: mpsc::Receiver<RouteCommand>,
    mut control_rx: mpsc::Receiver<SessionControl>,
    views: StateViews,
    mut status: watch::Receiver<ConsensusStatus>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut sessions: BTreeMap<NodeId, Session> = BTreeMap::new();
    // Per-node `command_seq`, monotone within a leadership term and reset when
    // the term changes so it restarts at 1 per node in the next term (ADR
    // 0009). `term` tracks the term the seqs belong to; a watch may coalesce a
    // step-down and re-election into one wakeup, so a Leader→Leader term change
    // must reset the seqs even without an observed non-leader state in between.
    let mut seqs: BTreeMap<NodeId, u64> = BTreeMap::new();
    let mut term: Option<u64> = None;

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
                match status.borrow().role {
                    Role::Leader { term: new_term } => {
                        if term != Some(new_term) {
                            seqs.clear();
                            term = Some(new_term);
                        }
                    }
                    _ => {
                        if !sessions.is_empty() {
                            tracing::info!(
                                count = sessions.len(),
                                "agent gateway: leadership lost, dropping sessions"
                            );
                        }
                        // Dropping the sessions closes every outbound channel.
                        sessions.clear();
                        seqs.clear();
                        term = None;
                    }
                }
            }
            control = control_rx.recv() => {
                let Some(control) = control else { break };
                match control {
                    SessionControl::Open { node, session_id, outbound } => {
                        // A reconnect REPLACES the previous handle for the node.
                        sessions.insert(node, Session { session_id, outbound });
                    }
                    SessionControl::Close { node, session_id } => {
                        if sessions.get(&node).is_some_and(|s| s.session_id == session_id) {
                            sessions.remove(&node);
                        }
                    }
                }
            }
            routed = router_rx.recv() => {
                let Some(routed) = routed else { break };
                stamp_and_route(&mut sessions, &mut seqs, &views, &status, routed);
            }
        }
    }
    tracing::debug!("agent gateway shutting down; dropping sessions");
    // Dropping `sessions` here closes every outbound channel.
}

/// Stamp a routed command's fencing header and forward it to the node's live
/// session. Single-writer of the per-node seq (ADR 0009).
fn stamp_and_route(
    sessions: &mut BTreeMap<NodeId, Session>,
    seqs: &mut BTreeMap<NodeId, u64>,
    views: &StateViews,
    status: &watch::Receiver<ConsensusStatus>,
    routed: RouteCommand,
) {
    let RouteCommand { node, mut command } = routed;

    // No live session: nothing to route to; reconciliation heals on reconnect.
    if !sessions.contains_key(&node) {
        tracing::debug!(node = %node, "agent gateway: no session for node, dropping command");
        return;
    }

    // Only the current leader may stamp a token; a deposed leader's commands
    // must never reach an agent (ADR 0009 term check is the agent-side backstop).
    let term = match status.borrow().role {
        Role::Leader { term } => term,
        _ => {
            tracing::warn!(node = %node, "agent gateway: not leader, dropping command");
            return;
        }
    };

    // The epoch is read from the current view; an unknown node cannot be fenced.
    let view = views.latest();
    let Some(node_record) = view.state().nodes.get(&node) else {
        tracing::warn!(node = %node, "agent gateway: node unknown in view, dropping command");
        return;
    };
    let node_epoch = node_record.epoch;

    // Consume a seq now that the command is certain to be stamped.
    let seq = seqs.entry(node).or_insert(0);
    *seq += 1;
    command.header = Some(CommandHeader {
        token: Some(FencingToken {
            leader_term: term,
            node_epoch,
        }),
        command_seq: *seq,
    });

    let session = sessions.get(&node).expect("presence checked above");
    if session.outbound.try_send(command).is_err() {
        tracing::warn!(
            node = %node,
            "agent gateway: outbound queue full, disconnecting session"
        );
        sessions.remove(&node);
    }
}

/// The tonic `AgentService` handler: one bidirectional stream per agent.
pub struct AgentSessionService {
    authority: SessionAuthority,
}

impl AgentSessionService {
    pub fn new(authority: SessionAuthority) -> Self {
        AgentSessionService { authority }
    }
}

/// The server-streaming response half of a session: the node's command queue.
type SessionCommandStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<AgentCommand, Status>> + Send>>;

#[tonic::async_trait]
impl AgentService for AgentSessionService {
    type SessionStream = SessionCommandStream;

    async fn session(
        &self,
        request: Request<Streaming<AgentReport>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        self.authority.accept(request).await
    }
}

impl SessionAuthority {
    /// Accept one agent session (leader-only). Authenticates the mTLS leaf CN
    /// against the claimed NodeId, registers the session with the manager, and
    /// returns the outbound command stream.
    async fn accept(
        &self,
        request: Request<Streaming<AgentReport>>,
    ) -> Result<Response<SessionCommandStream>, Status> {
        // Leader-only: refuse a non-leader with a leader hint when known.
        let role = self.status.borrow().role.clone();
        if !role.is_leader() {
            let mut status = Status::failed_precondition("not leader");
            if let Role::Follower { leader: Some(id) } = role {
                if let Ok(hint) = MetadataValue::try_from(id.to_string()) {
                    status.metadata_mut().insert("x-coppice-leader-hint", hint);
                }
            }
            return Err(status);
        }

        // mTLS identity: the client leaf's subject CN. Certs are already
        // chain-validated by the TLS layer; here we only bind CN to NodeId
        // (ADR 0011 — the CA issues a leaf with CN = node UUID).
        let cn = {
            let peer = request.peer_certs();
            match peer.as_ref().and_then(|certs| certs.first()) {
                Some(cert) => common_name_of(cert.as_ref())?,
                None => {
                    return Err(Status::unauthenticated(
                        "client certificate required (ADR 0011)",
                    ))
                }
            }
        };

        // The claimed NodeId comes from the FIRST report (ADR 0009 step 2: the
        // agent registers, then reports its ObservedSet). Every report carries
        // it; a later report with a different node ends the session.
        let mut reports = request.into_inner();
        let first = reports
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("agent session opened with no report"))?;
        let claimed = node_of(&first)
            .ok_or_else(|| Status::invalid_argument("first report is missing a node id"))?;

        if cn != claimed.to_string() {
            return Err(Status::unauthenticated(format!(
                "client certificate CN {cn} does not match claimed node {claimed} (ADR 0011)"
            )));
        }

        // Register the session; a reconnect replaces any prior handle.
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let (outbound_tx, outbound_rx) = mpsc::channel(AGENT_OUTBOUND_CAPACITY);
        if self
            .control
            .send(SessionControl::Open {
                node: claimed,
                session_id,
                outbound: outbound_tx,
            })
            .await
            .is_err()
        {
            return Err(Status::unavailable("agent gateway is shutting down"));
        }

        // Inbound pump: forward `first`, then the rest of the stream.
        tokio::spawn(pump_inbound(
            claimed,
            session_id,
            first,
            reports,
            self.inbound.clone(),
            self.control.clone(),
        ));

        // Outbound half: the session's command queue as the response stream.
        let stream = ReceiverStream::new(outbound_rx).map(Ok::<AgentCommand, Status>);
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Read half of one session: forward each report to ingestion, then close.
///
/// The `send().await` here is the "agent inbound" channel's backpressure
/// policy: a full channel stalls this socket read, which stalls the agent over
/// TCP — it never touches apply (`docs/architecture/coordinator-runtime.md`).
async fn pump_inbound(
    node: NodeId,
    session_id: u64,
    first: AgentReport,
    mut reports: Streaming<AgentReport>,
    inbound: mpsc::Sender<InboundReport>,
    control: mpsc::Sender<SessionControl>,
) {
    if inbound
        .send(InboundReport {
            node,
            report: first,
        })
        .await
        .is_err()
    {
        close(&control, node, session_id).await;
        return;
    }

    loop {
        match reports.message().await {
            Ok(Some(report)) => {
                // Identity is fixed for the connection (ADR 0009): a report for
                // a different node ends the session.
                if node_of(&report) != Some(node) {
                    break;
                }
                if inbound.send(InboundReport { node, report }).await.is_err() {
                    break;
                }
            }
            Ok(None) => break, // the agent closed its half
            Err(_) => break,   // stream / transport error
        }
    }

    close(&control, node, session_id).await;
}

/// Ask the manager to close this session (guarded by `session_id`).
async fn close(control: &mpsc::Sender<SessionControl>, node: NodeId, session_id: u64) {
    let _ = control
        .send(SessionControl::Close { node, session_id })
        .await;
}

/// The NodeId a report claims, if present and well-formed.
fn node_of(report: &AgentReport) -> Option<NodeId> {
    report.node.clone().and_then(|n| NodeId::try_from(n).ok())
}

/// Extract the subject CN from a DER-encoded X.509 leaf (ADR 0011).
fn common_name_of(der: &[u8]) -> Result<String, Status> {
    let (_, cert) = x509_parser::parse_x509_certificate(der)
        .map_err(|_| Status::unauthenticated("client certificate is not valid DER"))?;
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|attr| attr.as_str().ok())
        .ok_or_else(|| Status::unauthenticated("client certificate has no subject CN"))?;
    Ok(cn.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap as Map;

    use coppice_consensus::{ViewPublisher, ViewPublisherConfig};
    use coppice_core::node::Node;
    use coppice_core::resource::Resources;
    use coppice_state::{NodeRecord, StateMachine};

    fn leader(term: u64) -> ConsensusStatus {
        ConsensusStatus {
            id: 1,
            role: Role::Leader { term },
            last_applied: 0,
            known_committed: 0,
        }
    }

    /// A published view holding one node at `epoch`.
    fn views_with_node(node: NodeId, epoch: u64) -> StateViews {
        let mut sm = StateMachine::default();
        sm.nodes.insert(
            node,
            NodeRecord {
                node: Node {
                    id: node,
                    capacity: Resources::ZERO,
                    labels: Map::new(),
                    schedulable: true,
                },
                epoch,
            },
        );
        // The publisher may drop: `views.latest()` borrows the last-published
        // value, which survives the sender being gone.
        let (_publisher, views) = ViewPublisher::new(sm, 0, ViewPublisherConfig::default());
        views
    }

    fn header_of(command: &AgentCommand) -> (u64, u64, u64) {
        let header = command.header.expect("command is stamped");
        let token = header.token.expect("token present");
        (token.leader_term, token.node_epoch, header.command_seq)
    }

    fn bare_command() -> AgentCommand {
        AgentCommand {
            header: None,
            body: None,
        }
    }

    #[tokio::test]
    async fn stamps_term_epoch_and_monotone_seq() {
        let node = NodeId::new();
        let views = views_with_node(node, 7);
        let (status_tx, status_rx) = watch::channel(leader(3));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (router_tx, router_rx) = mpsc::channel(8);
        let (control_tx, control_rx) = mpsc::channel(8);
        let manager = tokio::spawn(run_manager(
            router_rx,
            control_rx,
            views,
            status_rx,
            shutdown_rx,
        ));

        let (out_tx, mut out_rx) = mpsc::channel(8);
        control_tx
            .send(SessionControl::Open {
                node,
                session_id: 1,
                outbound: out_tx,
            })
            .await
            .unwrap();

        for _ in 0..2 {
            router_tx
                .send(RouteCommand {
                    node,
                    command: bare_command(),
                })
                .await
                .unwrap();
        }

        let first = out_rx.recv().await.expect("first stamped command");
        assert_eq!(header_of(&first), (3, 7, 1));
        let second = out_rx.recv().await.expect("second stamped command");
        assert_eq!(header_of(&second), (3, 7, 2));

        let _ = shutdown_tx.send(true);
        let _ = status_tx;
        manager.await.unwrap();
    }

    #[tokio::test]
    async fn unknown_node_in_view_drops_the_command() {
        let known = NodeId::new();
        let views = views_with_node(known, 1);
        let (status_tx, status_rx) = watch::channel(leader(1));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (router_tx, router_rx) = mpsc::channel(8);
        let (control_tx, control_rx) = mpsc::channel(8);
        let manager = tokio::spawn(run_manager(
            router_rx,
            control_rx,
            views,
            status_rx,
            shutdown_rx,
        ));

        // A session exists for a node that is NOT in the view: stamping cannot
        // read an epoch, so the command is dropped.
        let ghost = NodeId::new();
        let (out_tx, mut out_rx) = mpsc::channel(8);
        control_tx
            .send(SessionControl::Open {
                node: ghost,
                session_id: 1,
                outbound: out_tx,
            })
            .await
            .unwrap();
        router_tx
            .send(RouteCommand {
                node: ghost,
                command: bare_command(),
            })
            .await
            .unwrap();

        // Nothing is delivered; drive a round-trip through the manager to be
        // sure it processed the route before we assert emptiness.
        let _ = shutdown_tx.send(true);
        let _ = status_tx;
        manager.await.unwrap();
        assert!(out_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn reconnect_replaces_session_and_seq_continues() {
        let node = NodeId::new();
        let views = views_with_node(node, 2);
        let (status_tx, status_rx) = watch::channel(leader(5));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (router_tx, router_rx) = mpsc::channel(8);
        let (control_tx, control_rx) = mpsc::channel(8);
        let manager = tokio::spawn(run_manager(
            router_rx,
            control_rx,
            views,
            status_rx,
            shutdown_rx,
        ));

        // First session.
        let (a_tx, mut a_rx) = mpsc::channel(8);
        control_tx
            .send(SessionControl::Open {
                node,
                session_id: 1,
                outbound: a_tx,
            })
            .await
            .unwrap();
        router_tx
            .send(RouteCommand {
                node,
                command: bare_command(),
            })
            .await
            .unwrap();
        let a1 = a_rx.recv().await.expect("first session command");
        assert_eq!(header_of(&a1).2, 1);

        // Reconnect: a new session replaces the old handle.
        let (b_tx, mut b_rx) = mpsc::channel(8);
        control_tx
            .send(SessionControl::Open {
                node,
                session_id: 2,
                outbound: b_tx,
            })
            .await
            .unwrap();
        // A late close for the OLD session must not evict the new one.
        control_tx
            .send(SessionControl::Close {
                node,
                session_id: 1,
            })
            .await
            .unwrap();
        router_tx
            .send(RouteCommand {
                node,
                command: bare_command(),
            })
            .await
            .unwrap();
        let b2 = b_rx.recv().await.expect("replacement session command");
        // Seq is per-node and continues monotone across the replace.
        assert_eq!(header_of(&b2).2, 2);

        let _ = shutdown_tx.send(true);
        let _ = status_tx;
        manager.await.unwrap();
    }

    #[tokio::test]
    async fn seq_resets_across_leadership_loss() {
        let node = NodeId::new();
        let views = views_with_node(node, 4);
        let (status_tx, status_rx) = watch::channel(leader(5));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (router_tx, router_rx) = mpsc::channel(8);
        let (control_tx, control_rx) = mpsc::channel(8);
        let manager = tokio::spawn(run_manager(
            router_rx,
            control_rx,
            views,
            status_rx,
            shutdown_rx,
        ));

        let (a_tx, mut a_rx) = mpsc::channel(8);
        control_tx
            .send(SessionControl::Open {
                node,
                session_id: 1,
                outbound: a_tx,
            })
            .await
            .unwrap();
        router_tx
            .send(RouteCommand {
                node,
                command: bare_command(),
            })
            .await
            .unwrap();
        assert_eq!(header_of(&a_rx.recv().await.unwrap()).2, 1);

        // Lose leadership: sessions and seqs clear.
        status_tx
            .send(ConsensusStatus {
                role: Role::Follower { leader: Some(9) },
                ..leader(5)
            })
            .unwrap();
        // Regain leadership under a new term.
        status_tx.send(leader(6)).unwrap();

        // Re-open and route: the seq restarts at 1 and the term is the new one.
        let (c_tx, mut c_rx) = mpsc::channel(8);
        control_tx
            .send(SessionControl::Open {
                node,
                session_id: 2,
                outbound: c_tx,
            })
            .await
            .unwrap();
        router_tx
            .send(RouteCommand {
                node,
                command: bare_command(),
            })
            .await
            .unwrap();
        let c1 = c_rx.recv().await.expect("post-recovery command");
        assert_eq!(header_of(&c1), (6, 4, 1));

        let _ = shutdown_tx.send(true);
        manager.await.unwrap();
    }

    #[test]
    fn node_of_reads_the_report_node() {
        let node = NodeId::new();
        let report = AgentReport {
            node: Some(node.into()),
            node_epoch: 0,
            body: None,
        };
        assert_eq!(node_of(&report), Some(node));
    }
}
