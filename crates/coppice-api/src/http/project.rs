//! Read-model projections: `StateMachine` → JSON DTOs ([`super::dto`]).
//!
//! These are pure functions of the replicated state, run at read time in
//! the handler (never in apply). Aggregations that scan the allocation or
//! attempt maps are handler-scoped throwaway memos, never stored on the
//! state machine.

use std::collections::BTreeMap;

use coppice_core::allocation::AllocationState;
use coppice_core::attempt::AttemptState;
use coppice_core::id::NodeId;
use coppice_core::resource::Resources;
use coppice_state::{AttemptRecord, StateMachine};

use super::dto;

#[derive(Default)]
struct NodeMemo {
    allocated: Resources,
    running_count: u32,
    accruing_count: u32,
}

fn build_node_memos(state: &StateMachine) -> BTreeMap<NodeId, NodeMemo> {
    let mut memos: BTreeMap<NodeId, NodeMemo> = BTreeMap::new();

    for (_, alloc_record) in &state.allocations {
        let alloc = &alloc_record.allocation;
        if alloc.state.is_terminal() {
            continue;
        }
        let memo = memos.entry(alloc.node).or_default();
        memo.allocated = memo.allocated.saturating_add(&alloc.funded);
        if matches!(alloc.state, AllocationState::Accruing) {
            memo.accruing_count += 1;
        }
    }

    for (_, attempt_record) in &state.attempts {
        if matches!(attempt_record.attempt.state, AttemptState::Running) {
            memos
                .entry(attempt_record.attempt.node)
                .or_default()
                .running_count += 1;
        }
    }

    memos
}

fn node_summary(
    node_id: &NodeId,
    record: &coppice_state::NodeRecord,
    memo: &NodeMemo,
) -> dto::NodeSummary {
    dto::NodeSummary {
        id: *node_id,
        capacity: (&record.node.capacity).into(),
        allocated: (&memo.allocated).into(),
        used: (&Resources::ZERO).into(),
        labels: record.node.labels.clone(),
        schedulable: record.node.schedulable,
        // Health has no reliable input yet: the replicated state records no
        // loss flag (`DeclareNodeLost` bumps the epoch and clears
        // `schedulable`, indistinguishable from an operator drain) and
        // heartbeat liveness is not wired, so every node reports `Unknown`
        // rather than a fabricated `Healthy`.
        health: dto::NodeHealth::Unknown,
        epoch: record.epoch,
        last_heartbeat_us: None,
        running_count: memo.running_count,
        accruing_count: memo.accruing_count,
    }
}

pub fn list_nodes(state: &StateMachine) -> dto::ListNodesResponse {
    let memos = build_node_memos(state);
    let empty = NodeMemo::default();

    let nodes = state
        .nodes
        .iter()
        .map(|(id, record)| {
            let memo = memos.get(id).unwrap_or(&empty);
            node_summary(id, record, memo)
        })
        .collect();

    dto::ListNodesResponse { nodes }
}

pub fn get_node(state: &StateMachine, id: &NodeId) -> Option<dto::GetNodeResponse> {
    let record = state.nodes.get(id)?;
    let memos = build_node_memos(state);
    let empty = NodeMemo::default();
    let memo = memos.get(id).unwrap_or(&empty);

    let summary = node_summary(id, record, memo);

    let active_attempts = state
        .attempts
        .iter()
        .filter(|(_, ar)| {
            ar.attempt.node == *id
                && matches!(
                    ar.attempt.state,
                    AttemptState::Dispatching | AttemptState::Running | AttemptState::Finalizing
                )
        })
        .map(|(_, ar)| attempt_view(ar))
        .collect();

    let accrual_queue = state
        .accrual_queue
        .iter()
        .filter(|((node, _), _)| *node == *id)
        .filter_map(|((_, _), alloc_id)| {
            let alloc_record = state.allocations.get(alloc_id)?;
            let alloc = &alloc_record.allocation;
            Some(dto::AccrualView {
                allocation: dto::AllocationView {
                    id: alloc.id,
                    job: alloc.job,
                    attempt: alloc.attempt,
                    node: alloc.node,
                    requested: (&alloc.requested).into(),
                    funded: (&alloc.funded).into(),
                    state: alloc.state.into(),
                    seq: alloc_record.seq,
                },
                funded_fraction: funded_fraction(&alloc.funded, &alloc.requested),
                projected_start_us: None,
            })
        })
        .collect();

    Some(dto::GetNodeResponse {
        summary,
        active_attempts,
        accrual_queue,
    })
}

fn attempt_view(ar: &AttemptRecord) -> dto::AttemptView {
    dto::AttemptView {
        id: ar.attempt.id,
        job: ar.attempt.job,
        node: ar.attempt.node,
        allocation: ar.attempt.allocation,
        state: (&ar.attempt.state).into(),
        outcome: match &ar.attempt.state {
            AttemptState::Terminal(outcome) => Some(outcome.into()),
            _ => None,
        },
        started_at_us: ar.started_at_us,
        ended_at_us: None,
        rate_ucu_per_second: ar.rate_ucu_per_second,
        charged_ucu: ar.charge.amount.0,
    }
}

fn funded_fraction(funded: &Resources, requested: &Resources) -> dto::FundedFraction {
    let frac = |funded: u64, requested: u64| -> f64 {
        if requested == 0 {
            1.0
        } else {
            funded as f64 / requested as f64
        }
    };
    dto::FundedFraction {
        cpu: frac(funded.cpu_millis, requested.cpu_millis),
        memory: frac(funded.memory_bytes, requested.memory_bytes),
        disk: frac(funded.disk_bytes, requested.disk_bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::allocation::Allocation;
    use coppice_core::attempt::Attempt;
    use coppice_core::id::GroupId;
    use coppice_core::id::{AllocationId, AttemptId, JobId};
    use coppice_core::node::Node;
    use coppice_core::quota::{ChargeRecord, CostUnits, PriorityMultiplier, FULL_REFUND_MILLI};
    use coppice_state::{AllocationRecord, AttemptRecord, NodeRecord};

    fn test_node(id: NodeId) -> NodeRecord {
        NodeRecord {
            node: Node {
                id,
                capacity: Resources {
                    cpu_millis: 4000,
                    memory_bytes: 8_000_000_000,
                    disk_bytes: 100_000_000_000,
                },
                labels: BTreeMap::new(),
                schedulable: true,
            },
            epoch: 1,
        }
    }

    fn test_attempt(id: AttemptId, job: JobId, node: NodeId, state: AttemptState) -> AttemptRecord {
        AttemptRecord {
            attempt: Attempt {
                id,
                job,
                allocation: AllocationId::new(),
                node,
                state,
            },
            group: GroupId(job.0),
            charge: ChargeRecord {
                amount: CostUnits(1000),
                charged_at_us: 0,
                refund_fraction_milli: FULL_REFUND_MILLI,
            },
            rate_ucu_per_second: 100,
            multiplier: PriorityMultiplier::ONE,
            started_at_us: Some(1000),
        }
    }

    fn test_allocation(
        id: AllocationId,
        job: JobId,
        attempt: AttemptId,
        node: NodeId,
        state: AllocationState,
    ) -> AllocationRecord {
        AllocationRecord {
            allocation: Allocation {
                id,
                job,
                attempt,
                node,
                requested: Resources {
                    cpu_millis: 1000,
                    memory_bytes: 1_000_000,
                    disk_bytes: 0,
                },
                funded: Resources {
                    cpu_millis: 1000,
                    memory_bytes: 1_000_000,
                    disk_bytes: 0,
                },
                state,
            },
            seq: 1,
        }
    }

    #[test]
    fn list_nodes_returns_empty_for_no_nodes() {
        let state = StateMachine::default();
        let response = list_nodes(&state);
        assert!(response.nodes.is_empty());
    }

    #[test]
    fn list_nodes_includes_all_registered_nodes() {
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let mut state = StateMachine::default();
        state.nodes.insert(n1, test_node(n1));
        state.nodes.insert(n2, test_node(n2));

        let response = list_nodes(&state);
        assert_eq!(response.nodes.len(), 2);
    }

    #[test]
    fn list_nodes_counts_running_and_accruing() {
        let node = NodeId::new();
        let job = JobId::new();
        let attempt_running = AttemptId::new();
        let attempt_accruing = AttemptId::new();
        let alloc_active = AllocationId::new();
        let alloc_accruing = AllocationId::new();

        let mut state = StateMachine::default();
        state.nodes.insert(node, test_node(node));

        state.attempts.insert(
            attempt_running,
            test_attempt(attempt_running, job, node, AttemptState::Running),
        );
        state.attempts.insert(
            attempt_accruing,
            test_attempt(attempt_accruing, job, node, AttemptState::Accruing),
        );
        state.allocations.insert(
            alloc_active,
            test_allocation(
                alloc_active,
                job,
                attempt_running,
                node,
                AllocationState::Active,
            ),
        );
        state.allocations.insert(
            alloc_accruing,
            test_allocation(
                alloc_accruing,
                job,
                attempt_accruing,
                node,
                AllocationState::Accruing,
            ),
        );

        let response = list_nodes(&state);
        assert_eq!(response.nodes.len(), 1);
        let summary = &response.nodes[0];
        assert_eq!(summary.running_count, 1);
        assert_eq!(summary.accruing_count, 1);
    }

    #[test]
    fn nodes_report_unknown_health_until_liveness_exists() {
        let node = NodeId::new();
        let mut state = StateMachine::default();
        state.nodes.insert(node, test_node(node));

        let response = list_nodes(&state);
        assert_eq!(response.nodes[0].health, dto::NodeHealth::Unknown);
    }

    #[test]
    fn get_node_returns_none_for_missing() {
        let state = StateMachine::default();
        assert!(get_node(&state, &NodeId::new()).is_none());
    }

    #[test]
    fn get_node_returns_active_attempts_and_accrual_queue() {
        let node = NodeId::new();
        let job = JobId::new();
        let attempt = AttemptId::new();
        let alloc = AllocationId::new();

        let mut state = StateMachine::default();
        state.nodes.insert(node, test_node(node));
        state.attempts.insert(
            attempt,
            test_attempt(attempt, job, node, AttemptState::Running),
        );
        state.allocations.insert(
            alloc,
            test_allocation(alloc, job, attempt, node, AllocationState::Active),
        );

        let response = get_node(&state, &node).unwrap();
        assert_eq!(response.active_attempts.len(), 1);
        assert_eq!(response.active_attempts[0].rate_ucu_per_second, 100);
        assert_eq!(response.active_attempts[0].outcome, None);
    }

    #[test]
    fn funded_fraction_handles_zero_requested() {
        let ff = funded_fraction(&Resources::ZERO, &Resources::ZERO);
        assert_eq!(ff.cpu, 1.0);
        assert_eq!(ff.memory, 1.0);
        assert_eq!(ff.disk, 1.0);
    }
}
