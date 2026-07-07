//! The determinism property-test harness.
//!
//! Generates valid-ish command sequences — interleaved per-job lifecycle
//! chains plus cluster-level commands, so sequences reach deep states while
//! still producing plenty of rejections — and asserts the apply contract's
//! replay guarantees:
//!
//! - **Replica equivalence**: the same sequence into two `StateMachine`s
//!   yields identical apply results and identical final state.
//! - **Snapshot equivalence**: apply k commands, roundtrip the state through
//!   the serde path (the snapshot path for now, per ADR 0003), apply the
//!   rest — indistinguishable from applying everything on the original.
//!
//! Rejections are as much a part of the contract as acceptances: a rejected
//! command must be a deterministic no-op on every replica, so generated
//! sequences deliberately include stale, duplicate, and invalid commands.

mod common;

use std::collections::BTreeMap;

use common::*;
use coppice_core::attempt::AttemptOutcome;
use coppice_core::job::RetryPolicy;
use coppice_core::quota::CostUnits;
use coppice_core::resource::Resources;
use coppice_state::command::{
    BumpClusterVersion, ConfigureQuotaEntity, DeclareNodeLost, EvictTerminalJobs, LostAttempt,
    ReconcileNode, RegisterNode, SetNodeSchedulable,
};
use coppice_state::{Command, StateMachine};
use proptest::prelude::*;

const MAX_JOBS: u64 = 5;
const NODES: u64 = 3;

fn node_of(ix: usize) -> coppice_core::id::NodeId {
    nid(1 + (ix as u128 % NODES as u128))
}

fn arb_ts() -> impl Strategy<Value = i64> {
    // A day's spread around the fixture base, deliberately not monotone:
    // the clock-skew clamp must make regressed timestamps harmless.
    (TS - 43_200_000_000)..(TS + 43_200_000_000)
}

fn arb_outcome() -> impl Strategy<Value = AttemptOutcome> {
    prop_oneof![
        Just(AttemptOutcome::Exited { code: 0 }),
        Just(AttemptOutcome::Exited { code: 1 }),
        Just(AttemptOutcome::OomKilled),
        Just(AttemptOutcome::MaxRuntimeExceeded),
        Just(AttemptOutcome::Aborted),
        Just(AttemptOutcome::PullFailed { user_error: true }),
        Just(AttemptOutcome::StartFailed { user_error: false }),
        Just(AttemptOutcome::NodeLost),
        Just(AttemptOutcome::AgentError),
    ]
}

/// One job's lifecycle chain, truncated at `progress` and with an optional
/// abort spliced in — so sequences cover early endings, races, and
/// stale-report rejections, not just happy paths.
fn arb_job_chain(i: u64) -> impl Strategy<Value = Vec<Command>> {
    (
        1usize..=6,
        proptest::option::of(0usize..=6),
        arb_outcome(),
        0usize..NODES as usize,
        500u64..12_000,
        proptest::option::of(60u64..7_200),
        arb_ts(),
    )
        .prop_map(move |(progress, abort_at, outcome, node_ix, cpu_millis, max_rt, ts)| {
            let job = jid(1_000 + i as u128);
            let attempt = aid(2_000 + i as u128);
            let alloc = alid(3_000 + i as u128);
            let mut chain = vec![
                submit_cmd(job, cpu(cpu_millis), max_rt, RetryPolicy::default()),
                place_cmd(placement(job, attempt, alloc, node_of(node_ix), cpu(cpu_millis)), ts),
                dispatch_cmd(attempt, ts),
                started_cmd(attempt, ts),
                exited_cmd(attempt, ts),
                outcome_cmd(attempt, outcome, 30, ts),
            ];
            chain.truncate(progress);
            if let Some(at) = abort_at {
                let at = at.min(chain.len());
                chain.insert(at, abort_cmd(job, ts));
            }
            chain
        })
}

/// Cluster-level commands referencing the same id pools, so some are valid
/// and some reject — both must be deterministic.
fn arb_global() -> impl Strategy<Value = Command> {
    prop_oneof![
        (0usize..NODES as usize, 4_000u64..32_000, arb_ts()).prop_map(|(n, cpu_millis, ts)| {
            Command::RegisterNode(RegisterNode {
                node: node_of(n),
                capacity: cpu(cpu_millis),
                labels: BTreeMap::new(),
                registered_at_us: ts,
            })
        }),
        (0usize..NODES as usize, arb_ts()).prop_map(|(n, ts)| {
            Command::DeclareNodeLost(DeclareNodeLost { node: node_of(n), declared_at_us: ts })
        }),
        (0usize..NODES as usize, any::<bool>(), arb_ts()).prop_map(|(n, schedulable, ts)| {
            Command::SetNodeSchedulable(SetNodeSchedulable {
                node: node_of(n),
                schedulable,
                updated_at_us: ts,
            })
        }),
        (any::<u8>(), arb_ts()).prop_map(|(mask, ts)| {
            Command::EvictTerminalJobs(EvictTerminalJobs {
                jobs: (0..MAX_JOBS)
                    .filter(|k| mask & (1 << k) != 0)
                    .map(|k| jid(1_000 + k as u128))
                    .collect(),
                evicted_at_us: ts,
            })
        }),
        (0u128..4, arb_ts()).prop_map(|(e, ts)| {
            Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
                entity: qid(0xE0 + e),
                parent: Some(ROOT),
                name: "team".into(),
                quota: CostUnits(1_000_000_000),
                updated_at_us: ts,
            })
        }),
        (1u32..6, arb_ts()).prop_map(|(to, ts)| {
            Command::BumpClusterVersion(BumpClusterVersion { to, bumped_at_us: ts })
        }),
        (0u32..5, ).prop_map(|(k,)| update_policy_cmd(test_policy(k))),
        (0usize..NODES as usize, 1u64..4, any::<u8>(), any::<u8>(), arb_ts()).prop_map(
            |(n, epoch, adopt_mask, lost_mask, ts)| {
                Command::ReconcileNode(ReconcileNode {
                    node: node_of(n),
                    node_epoch: epoch,
                    adopted: (0..MAX_JOBS)
                        .filter(|k| adopt_mask & (1 << k) != 0)
                        .map(|k| aid(2_000 + k as u128))
                        .collect(),
                    lost: (0..MAX_JOBS)
                        .filter(|k| lost_mask & (1 << k) != 0)
                        .map(|k| LostAttempt {
                            attempt: aid(2_000 + k as u128),
                            outcome: AttemptOutcome::AgentError,
                            actual_runtime_us: 0,
                        })
                        .collect(),
                    observed_at_us: ts,
                })
            }
        ),
    ]
}

/// Merge chains preserving intra-chain order; the pick sequence chooses
/// which chain advances next.
fn interleave(chains: Vec<Vec<Command>>, picks: Vec<prop::sample::Index>) -> Vec<Command> {
    let mut chains: Vec<std::collections::VecDeque<Command>> =
        chains.into_iter().map(Into::into).collect();
    let mut out = Vec::new();
    for pick in picks {
        let live: Vec<usize> =
            (0..chains.len()).filter(|&i| !chains[i].is_empty()).collect();
        if live.is_empty() {
            break;
        }
        let i = live[pick.index(live.len())];
        if let Some(cmd) = chains[i].pop_front() {
            out.push(cmd);
        }
    }
    for chain in &mut chains {
        while let Some(cmd) = chain.pop_front() {
            out.push(cmd);
        }
    }
    out
}

#[derive(Debug, Clone)]
struct Scenario {
    commands: Vec<Command>,
    split: usize,
}

fn arb_scenario() -> impl Strategy<Value = Scenario> {
    let chains = (
        arb_job_chain(0),
        arb_job_chain(1),
        arb_job_chain(2),
        arb_job_chain(3),
        arb_job_chain(4),
    );
    (
        chains,
        1usize..=MAX_JOBS as usize,
        proptest::collection::vec(arb_global(), 0..6),
        proptest::collection::vec(any::<prop::sample::Index>(), 0..64),
        any::<prop::sample::Index>(),
    )
        .prop_map(|((c0, c1, c2, c3, c4), njobs, globals, picks, split)| {
            let mut chains: Vec<Vec<Command>> =
                vec![c0, c1, c2, c3, c4].into_iter().take(njobs).collect();
            for g in globals {
                chains.push(vec![g]);
            }
            // Deterministic bootstrap prefix so chains can reach deep states.
            let mut commands = vec![
                configure_entity_cmd(ROOT, None),
                update_policy_cmd(test_policy(4)),
            ];
            for n in 0..NODES {
                commands.push(register_node_cmd(node_of(n as usize), cpu(16_000), TS));
            }
            commands.extend(interleave(chains, picks));
            let split = split.index(commands.len() + 1);
            Scenario { commands, split }
        })
}

proptest! {
    /// Same committed sequence ⇒ identical apply results and identical
    /// state, on every replica.
    #[test]
    fn replicas_converge(scenario in arb_scenario()) {
        let mut a = StateMachine::default();
        let mut b = StateMachine::default();
        for cmd in &scenario.commands {
            let ra = a.apply(cmd);
            let rb = b.apply(cmd);
            prop_assert_eq!(ra, rb, "apply results diverged on {:?}", cmd);
        }
        prop_assert_eq!(a, b);
    }

    /// Snapshot-then-replay: serializing mid-stream and continuing on the
    /// restored copy is indistinguishable from never snapshotting. Exercises
    /// the serde path standing in for the proto snapshot (ADR 0003); `split`
    /// ranges over the whole sequence including the ends.
    #[test]
    fn snapshot_roundtrip_is_transparent(scenario in arb_scenario()) {
        let mut direct = StateMachine::default();
        for cmd in &scenario.commands {
            let _ = direct.apply(cmd);
        }

        let mut prefix = StateMachine::default();
        for cmd in &scenario.commands[..scenario.split] {
            let _ = prefix.apply(cmd);
        }
        let snapshot = serde_json::to_vec(&prefix).expect("state must serialize");
        let mut restored: StateMachine =
            serde_json::from_slice(&snapshot).expect("snapshot must deserialize");
        prop_assert_eq!(&restored, &prefix, "roundtrip must be lossless");

        let mut resumed_results = Vec::new();
        let mut original_results = Vec::new();
        for cmd in &scenario.commands[scenario.split..] {
            resumed_results.push(restored.apply(cmd));
            original_results.push(prefix.apply(cmd));
        }
        prop_assert_eq!(resumed_results, original_results);
        prop_assert_eq!(&restored, &prefix);
        prop_assert_eq!(restored, direct);
    }

    /// Applying any generated sequence never panics — bad input is a
    /// rejection, not a crash. (Implicit in the other properties; explicit
    /// here so a future shrink points straight at the offending command.)
    #[test]
    fn apply_never_panics(scenario in arb_scenario()) {
        let mut sm = StateMachine::default();
        for (i, cmd) in scenario.commands.iter().enumerate() {
            let _ = sm.apply(cmd);
            prop_assert_eq!(sm.version, i as u64 + 1, "version counts every applied entry");
        }
    }
}

/// Resource arithmetic in apply saturates instead of panicking, even at the
/// extremes the generators above won't hit.
#[test]
fn extreme_resources_never_panic() {
    let mut sm = setup();
    let huge = Resources { cpu_millis: u64::MAX, memory_bytes: u64::MAX, disk_bytes: u64::MAX };
    apply_ok(&mut sm, register_node_cmd(nid(9), huge, TS));
    apply_ok(&mut sm, submit_cmd(jid(9), huge, Some(u64::MAX / 2_000_000), RetryPolicy::default()));
    apply_ok(&mut sm, place_cmd(placement(jid(9), aid(9), alid(9), nid(9), huge), TS));
    apply_ok(&mut sm, dispatch_cmd(aid(9), TS));
    apply_ok(&mut sm, started_cmd(aid(9), TS));
    apply_ok(&mut sm, outcome_cmd(aid(9), AttemptOutcome::Exited { code: 0 }, u64::MAX / 2_000_000, TS + 1));
}
