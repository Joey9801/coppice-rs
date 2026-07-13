//! Synthetic `StateMachine` generation for storage benchmarks and the
//! determinism suite.
//!
//! ADR 0018 gates the snapshot encode/decode path on benchmarks at the
//! 1M-live-job scale, and the determinism suite wants realistic states to
//! replay commands against. Both need the *same* generator: a fully
//! populated, internally consistent [`StateMachine`] built deterministically
//! from a seed, so a benchmark regression or a determinism failure is
//! reproducible from a logged number. Every id and random choice therefore
//! flows through [`crate::rng::Rng`] — never `Uuid::new_v4` — or seeds stop
//! reproducing.
//!
//! The generator does not go through [`StateMachine::apply`]: at 1M jobs,
//! replaying commands would be far slower than the benchmark it feeds, and
//! would need its own scheduler and agent stand-ins. Instead it constructs
//! records directly, choosing state combinations that mirror the legal
//! `JobState`/`AttemptState`/`AllocationState` triples in `coppice-core`'s
//! transition tables (see `job.rs`, `attempt.rs`, `allocation.rs`, and
//! `coppice-state/src/apply.rs`), so consumers see the same shapes apply
//! would have produced.

use std::collections::BTreeMap;

use coppice_core::allocation::{Allocation, AllocationState};
use coppice_core::attempt::{Attempt, AttemptOutcome, AttemptState};
use coppice_core::id::{AllocationId, AttemptId, GroupId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{AbortRequest, Job, JobState, RetryPolicy};
use coppice_core::node::Node;
use coppice_core::quota::{self, ChargeRecord, CostUnits, PriorityMultiplier, UsageState};
use coppice_core::resource::Resources;
use coppice_state::{
    AllocationRecord, AttemptRecord, JobRecord, NodeRecord, PolicyConfig, QuotaEntity, StateMachine,
};

use crate::rng::Rng;

/// Cluster shape and scale for synthetic state generation.
#[derive(Debug, Clone)]
pub struct SynthConfig {
    pub seed: u64,
    pub jobs: usize,
    pub nodes: usize,
    pub quota_entities: usize,
}

impl SynthConfig {
    /// A plausible cluster shape for `jobs` live+historical jobs: enough
    /// nodes to spread the load thinly (roughly 256 jobs per node) and a
    /// three-level quota-entity tree sized so leaves see a few thousand jobs
    /// each. Seed 0 — callers wanting a different draw of the same shape set
    /// `seed` after construction.
    pub fn with_jobs(jobs: usize) -> SynthConfig {
        SynthConfig {
            seed: 0,
            jobs,
            nodes: (jobs / 256).max(4),
            quota_entities: (jobs / 2000).max(8),
        }
    }
}

/// Build a fully populated, internally consistent `StateMachine` with
/// `cfg.jobs` jobs, deterministic from `cfg.seed`.
pub fn synth_state(cfg: &SynthConfig) -> StateMachine {
    let mut rng = Rng::new(cfg.seed);
    // Fixed rather than wall-clock: reproducibility is the entire point.
    let base_time_us: i64 = 1_700_000_000_000_000;

    let quota_tree = build_quota_tree(&mut rng, cfg.quota_entities, base_time_us);
    let nodes = build_nodes(&mut rng, cfg.nodes.max(1));
    let node_ids: Vec<NodeId> = nodes.keys().copied().collect();

    let policy = PolicyConfig {
        priority_multipliers: build_priority_table(),
        ..PolicyConfig::default()
    };
    let priorities: Vec<i32> = policy.priority_multipliers.keys().copied().collect();

    let mut jobs_buf: Vec<(JobId, JobRecord)> = Vec::with_capacity(cfg.jobs);
    let mut bufs = Buffers {
        attempts: Vec::with_capacity(cfg.jobs * 3 / 2),
        allocations: Vec::with_capacity(cfg.jobs * 3 / 2),
        accrual: Vec::new(),
    };
    let mut next_seq: u64 = 0;

    for _ in 0..cfg.jobs {
        let job_id = JobId(next_uuid(&mut rng));
        let leaf = *rng.pick(&quota_tree.leaves);
        let priority = *rng.pick(&priorities);
        let multiplier = policy.priority_multipliers[&priority];
        let requested = Resources {
            cpu_millis: rng.range(250, 8_000),
            memory_bytes: rng.range(256 << 20, 16 << 30),
            disk_bytes: rng.range(0, 50 << 30).max(1),
        };
        let max_runtime_us = if rng.chance(70, 100) {
            // 5 minutes .. 24 hours, in microseconds.
            Some(rng.range(300_000_000, 86_400_000_000))
        } else {
            None
        };
        let submitted_at_us = base_time_us - rng.range(0, 90 * 24 * 3_600 * 1_000_000) as i64;
        let retry = RetryPolicy {
            max_retries: rng.range(1, 6) as u32,
            retry_user_errors: rng.chance(1, 3),
        };
        let rate = quota::resource_rate(&requested, &policy.cost_weights);
        let charge_amount = quota::cost_from_rate(
            rate,
            max_runtime_us
                .map(quota::runtime_seconds_ceil)
                .unwrap_or(policy.default_charge_runtime_s),
            multiplier,
        );
        let ctx = AttemptCtx {
            job: job_id,
            requested,
            rate,
            multiplier,
            charge_amount,
        };

        let bucket = rng.below(100);
        let (job_state, current_attempt, mut attempt_ids, abort_eligible) = if bucket < 55 {
            // Running.
            let node = *rng.pick(&node_ids);
            let charged_at = submitted_at_us + rng.range(0, 60_000_000) as i64;
            let mut ids = gen_history(
                &mut rng,
                &ctx,
                &node_ids,
                submitted_at_us,
                &mut next_seq,
                &mut bufs,
            );
            let seq = next_seq;
            next_seq += 1;
            let id = build_attempt(
                &mut rng,
                &ctx,
                node,
                charged_at,
                AttemptKind::Running,
                seq,
                &mut bufs,
            );
            ids.push(id);
            (JobState::Running, Some(id), ids, true)
        } else if bucket < 70 {
            // Queued: no current attempt, possibly a retry history.
            let ids = gen_history(
                &mut rng,
                &ctx,
                &node_ids,
                submitted_at_us,
                &mut next_seq,
                &mut bufs,
            );
            (JobState::Queued, None, ids, false)
        } else if bucket < 80 {
            // Preparing: current attempt is accruing, ready, or dispatching.
            let node = *rng.pick(&node_ids);
            let charged_at = submitted_at_us + rng.range(0, 60_000_000) as i64;
            let mut ids = gen_history(
                &mut rng,
                &ctx,
                &node_ids,
                submitted_at_us,
                &mut next_seq,
                &mut bufs,
            );
            let kind = match rng.below(3) {
                0 => AttemptKind::Accruing,
                1 => AttemptKind::Ready,
                _ => AttemptKind::Dispatching,
            };
            let seq = next_seq;
            next_seq += 1;
            let id = build_attempt(&mut rng, &ctx, node, charged_at, kind, seq, &mut bufs);
            ids.push(id);
            (JobState::Preparing, Some(id), ids, true)
        } else if bucket < 85 {
            // Finalizing: exit observed, resolution not yet committed.
            let node = *rng.pick(&node_ids);
            let charged_at = submitted_at_us + rng.range(0, 60_000_000) as i64;
            let mut ids = gen_history(
                &mut rng,
                &ctx,
                &node_ids,
                submitted_at_us,
                &mut next_seq,
                &mut bufs,
            );
            let seq = next_seq;
            next_seq += 1;
            let id = build_attempt(
                &mut rng,
                &ctx,
                node,
                charged_at,
                AttemptKind::Finalizing,
                seq,
                &mut bufs,
            );
            ids.push(id);
            (JobState::Finalizing, Some(id), ids, true)
        } else {
            // Terminal: Succeeded, Failed, or Aborted. A terminal job's
            // `current_attempt` is always `None` — the real apply loop
            // clears it on every terminal resolution (see
            // `StateMachine::resolve_job`) — even though the job's last
            // attempt (if any) is still listed in `attempts`.
            let r2 = rng.below(100);
            let (state, outcome) = if r2 < 70 {
                (
                    JobState::Succeeded,
                    Some(AttemptOutcome::Exited { code: 0 }),
                )
            } else if r2 < 90 {
                (
                    JobState::Failed,
                    Some(random_terminal_failure_outcome(&mut rng)),
                )
            } else if rng.chance(1, 2) {
                // Aborted while still queued: no attempt ever existed.
                (JobState::Aborted, None)
            } else {
                (JobState::Aborted, Some(AttemptOutcome::Aborted))
            };
            let mut ids = if outcome.is_some() {
                gen_history(
                    &mut rng,
                    &ctx,
                    &node_ids,
                    submitted_at_us,
                    &mut next_seq,
                    &mut bufs,
                )
            } else {
                Vec::new()
            };
            if let Some(outcome) = outcome {
                let node = *rng.pick(&node_ids);
                let charged_at = submitted_at_us + rng.range(0, 60_000_000) as i64;
                let seq = next_seq;
                next_seq += 1;
                let id = build_attempt(
                    &mut rng,
                    &ctx,
                    node,
                    charged_at,
                    AttemptKind::Terminal(outcome),
                    seq,
                    &mut bufs,
                );
                ids.push(id);
            }
            (state, None, ids, false)
        };

        // `retries_used` counts terminal attempts before the live/current
        // one; a job with no current attempt yet (Queued via requeue) has
        // already had every listed attempt counted the same way.
        let retries_used = attempt_ids.len() as u32 - u32::from(current_attempt.is_some());

        let abort_requested = if job_state == JobState::Aborted {
            Some(AbortRequest {
                reason: if rng.chance(1, 2) {
                    Some("user requested cancellation".to_string())
                } else {
                    None
                },
                requested_at_us: submitted_at_us + rng.range(0, 3_600_000_000) as i64,
            })
        } else if abort_eligible && rng.chance(3, 100) {
            // Legal in every non-terminal state: an abort can be in flight
            // while the attempt is still winding down.
            Some(AbortRequest {
                reason: None,
                requested_at_us: submitted_at_us + rng.range(0, 3_600_000_000) as i64,
            })
        } else {
            None
        };

        let (command, entrypoint) = gen_command(&mut rng);
        let spec = Job {
            id: job_id,
            image: gen_image(&mut rng),
            command,
            entrypoint,
            requests: requested,
            priority,
            max_runtime_us,
            quota_entity: leaf,
            retry,
            abort_requested,
        };
        // Stamped like the real terminal path: a job aborted before any
        // attempt existed terminates in the abort apply itself, so its
        // terminal time is the request's; every other terminal job resolves
        // at some later report time.
        let terminal_at_us = if !job_state.is_terminal() {
            None
        } else if let (true, Some(a)) = (attempt_ids.is_empty(), &spec.abort_requested) {
            Some(a.requested_at_us)
        } else {
            Some(submitted_at_us + rng.range(0, 3_600_000_000) as i64)
        };

        attempt_ids.shrink_to_fit();
        jobs_buf.push((
            job_id,
            JobRecord {
                spec,
                state: job_state,
                multiplier,
                submitted_at_us,
                terminal_at_us,
                retries_used,
                current_attempt,
                attempts: attempt_ids,
            },
        ));
    }

    jobs_buf.sort_unstable_by_key(|(id, _)| *id);
    bufs.attempts.sort_unstable_by_key(|(id, _)| *id);
    bufs.allocations.sort_unstable_by_key(|(id, _)| *id);
    bufs.accrual.sort_unstable_by_key(|(key, _)| *key);
    let mut entities = quota_tree.entities;
    entities.sort_unstable_by_key(|(id, _)| *id);

    StateMachine {
        jobs: jobs_buf.into_iter().collect(),
        attempts: bufs.attempts.into_iter().collect(),
        allocations: bufs.allocations.into_iter().collect(),
        nodes,
        quota_entities: entities.into_iter().collect(),
        accrual_queue: bufs.accrual.into_iter().collect(),
        next_allocation_seq: next_seq,
        policy,
        cluster_version: 1,
        version: cfg.jobs as u64 * 8,
    }
}

/// Mint a fresh, deterministic UUID from the testkit RNG. Never
/// `Uuid::new_v4`: that would break seed reproducibility.
fn next_uuid(rng: &mut Rng) -> uuid::Uuid {
    uuid::Uuid::from_u64_pair(rng.next_u64(), rng.next_u64())
}

/// The per-job inputs shared by every attempt the job gets (current and
/// historical): same resource request, same charge rate, same job-scoped
/// quota multiplier.
#[derive(Debug, Clone, Copy)]
struct AttemptCtx {
    job: JobId,
    requested: Resources,
    rate: u64,
    multiplier: PriorityMultiplier,
    charge_amount: CostUnits,
}

/// What an attempt/allocation pair being built should look like. Mirrors the
/// legal `AttemptState` × `AllocationState` combinations from
/// `coppice-state/src/apply.rs`: `Accruing`/`Ready`/`Dispatching` all keep
/// the job `Preparing`, funding only reaches `Active` once the attempt is
/// observed `Running`, and only a terminal attempt releases its allocation.
enum AttemptKind {
    Accruing,
    Ready,
    Dispatching,
    Running,
    Finalizing,
    Terminal(AttemptOutcome),
}

/// Accumulators for the flat record lists, sorted and bulk-collected into
/// the state maps once generation finishes — far cheaper at 1M scale than
/// inserting into the maps one record at a time.
struct Buffers {
    attempts: Vec<(AttemptId, AttemptRecord)>,
    allocations: Vec<(AllocationId, AllocationRecord)>,
    accrual: Vec<((NodeId, u64), AllocationId)>,
}

/// Build one attempt and its allocation, push them into `bufs`, and return
/// the new attempt's id.
fn build_attempt(
    rng: &mut Rng,
    ctx: &AttemptCtx,
    node: NodeId,
    charged_at_us: i64,
    kind: AttemptKind,
    seq: u64,
    bufs: &mut Buffers,
) -> AttemptId {
    let attempt_id = AttemptId(next_uuid(rng));
    let allocation_id = AllocationId(next_uuid(rng));

    let (attempt_state, alloc_state, funded, started_at_us, accruing) = match kind {
        AttemptKind::Accruing => {
            // Partially funded by construction: this is what distinguishes
            // Accruing from Funded in the real apply loop.
            let frac = rng.range(0, 90);
            let funded = Resources {
                cpu_millis: ctx.requested.cpu_millis * frac / 100,
                memory_bytes: ctx.requested.memory_bytes * frac / 100,
                disk_bytes: ctx.requested.disk_bytes * frac / 100,
            };
            (
                AttemptState::Accruing,
                AllocationState::Accruing,
                funded,
                None,
                true,
            )
        }
        AttemptKind::Ready => (
            AttemptState::Ready,
            AllocationState::Funded,
            ctx.requested,
            None,
            false,
        ),
        AttemptKind::Dispatching => (
            AttemptState::Dispatching,
            AllocationState::Funded,
            ctx.requested,
            None,
            false,
        ),
        AttemptKind::Running => {
            let started = charged_at_us + rng.range(1_000_000, 60_000_000) as i64;
            (
                AttemptState::Running,
                AllocationState::Active,
                ctx.requested,
                Some(started),
                false,
            )
        }
        AttemptKind::Finalizing => {
            let started = charged_at_us + rng.range(1_000_000, 60_000_000) as i64;
            (
                AttemptState::Finalizing,
                AllocationState::Active,
                ctx.requested,
                Some(started),
                false,
            )
        }
        AttemptKind::Terminal(ref outcome) => {
            // An attempt revoked or turned back before it ever dispatched
            // never started; every other outcome observed a running
            // container first.
            let started = match outcome {
                AttemptOutcome::Revoked
                | AttemptOutcome::PullFailed { .. }
                | AttemptOutcome::StartFailed { .. } => None,
                _ => Some(charged_at_us + rng.range(1_000_000, 60_000_000) as i64),
            };
            (
                AttemptState::Terminal(outcome.clone()),
                AllocationState::Released,
                ctx.requested,
                started,
                false,
            )
        }
    };

    let attempt = Attempt {
        id: attempt_id,
        job: ctx.job,
        allocation: allocation_id,
        node,
        state: attempt_state,
    };
    bufs.attempts.push((
        attempt_id,
        AttemptRecord {
            attempt,
            // v1 groups are singletons keyed by the job id.
            group: GroupId(ctx.job.0),
            charge: ChargeRecord {
                amount: ctx.charge_amount,
                charged_at_us,
                refund_fraction_milli: quota::FULL_REFUND_MILLI,
            },
            rate_ucu_per_second: ctx.rate,
            multiplier: ctx.multiplier,
            started_at_us,
        },
    ));
    let allocation = Allocation {
        id: allocation_id,
        job: ctx.job,
        attempt: attempt_id,
        node,
        requested: ctx.requested,
        funded,
        state: alloc_state,
    };
    bufs.allocations
        .push((allocation_id, AllocationRecord { allocation, seq }));
    if accruing {
        bufs.accrual.push(((node, seq), allocation_id));
    }
    attempt_id
}

/// ~20% of jobs carry 1-2 earlier terminal attempts (prior retries): each
/// gets its own allocation, released, in an outcome that plausibly
/// preceded a retry.
fn gen_history(
    rng: &mut Rng,
    ctx: &AttemptCtx,
    node_ids: &[NodeId],
    submitted_at_us: i64,
    next_seq: &mut u64,
    bufs: &mut Buffers,
) -> Vec<AttemptId> {
    let mut ids = Vec::new();
    if rng.chance(20, 100) {
        let count = rng.range(1, 3);
        for _ in 0..count {
            let node = *rng.pick(node_ids);
            let charged_at = submitted_at_us + rng.range(0, 60_000_000) as i64;
            let outcome = random_early_outcome(rng);
            let seq = *next_seq;
            *next_seq += 1;
            let id = build_attempt(
                rng,
                ctx,
                node,
                charged_at,
                AttemptKind::Terminal(outcome),
                seq,
                bufs,
            );
            ids.push(id);
        }
    }
    ids
}

/// A plausible reason an earlier attempt ended and the job was retried.
fn random_early_outcome(rng: &mut Rng) -> AttemptOutcome {
    match rng.below(5) {
        0 => AttemptOutcome::Revoked,
        1 => AttemptOutcome::NodeLost,
        2 => AttemptOutcome::AgentError,
        3 => AttemptOutcome::PullFailed {
            user_error: rng.chance(1, 2),
        },
        _ => AttemptOutcome::Exited {
            code: rng.range(1, 255) as i32,
        },
    }
}

/// A plausible reason a job's final attempt ended in `Failed`. Excludes
/// `Aborted` (that outcome only produces `JobState::Aborted`) and `Revoked`
/// (which always requeues rather than terminating).
fn random_terminal_failure_outcome(rng: &mut Rng) -> AttemptOutcome {
    match rng.below(6) {
        0 => AttemptOutcome::Exited {
            code: rng.range(1, 255) as i32,
        },
        1 => AttemptOutcome::OomKilled,
        2 => AttemptOutcome::MaxRuntimeExceeded,
        3 => AttemptOutcome::PullFailed {
            user_error: rng.chance(1, 2),
        },
        4 => AttemptOutcome::StartFailed {
            user_error: rng.chance(1, 2),
        },
        _ => AttemptOutcome::NodeLost,
    }
}

/// A small, plausible priority ladder: 6 tiers from 0.5x to 3x, Q32.32.
fn build_priority_table() -> BTreeMap<i32, PriorityMultiplier> {
    let mut table = BTreeMap::new();
    for priority in -2i32..=3 {
        let n = (priority + 3) as u64; // 1..=6
        table.insert(priority, PriorityMultiplier((n << 32) / 2));
    }
    table
}

/// The quota-entity tree plus its leaves (every job charges exactly one
/// leaf; every ancestor above it is charged too — ADR 0005).
struct QuotaTree {
    entities: Vec<(QuotaEntityId, QuotaEntity)>,
    leaves: Vec<QuotaEntityId>,
}

/// Build a 3-level quota-entity tree (root orgs -> teams -> leaves) with
/// `total` entities spread across the levels, each carrying a nonzero usage
/// accumulator.
fn build_quota_tree(rng: &mut Rng, total: usize, base_time_us: i64) -> QuotaTree {
    let total = total.max(3);
    let roots = (total / 20).max(1);
    let teams = (total / 5).max(roots + 1);
    let leaves_n = total.saturating_sub(roots + teams).max(1);

    let mut entities = Vec::with_capacity(roots + teams + leaves_n);

    let mut root_ids = Vec::with_capacity(roots);
    for i in 0..roots {
        let id = QuotaEntityId(next_uuid(rng));
        let quota = CostUnits(rng.range(1_000_000_000, 100_000_000_000_000));
        let usage = UsageState {
            usage: CostUnits(rng.range(0, quota.0.max(2))),
            last_update_us: base_time_us,
        };
        entities.push((
            id,
            QuotaEntity {
                parent: None,
                name: format!("org-{i}"),
                quota,
                usage,
            },
        ));
        root_ids.push(id);
    }

    let mut team_ids = Vec::with_capacity(teams);
    for i in 0..teams {
        let parent = *rng.pick(&root_ids);
        let id = QuotaEntityId(next_uuid(rng));
        let quota = CostUnits(rng.range(100_000_000, 10_000_000_000_000));
        let usage = UsageState {
            usage: CostUnits(rng.range(0, quota.0.max(2))),
            last_update_us: base_time_us,
        };
        entities.push((
            id,
            QuotaEntity {
                parent: Some(parent),
                name: format!("team-{i}"),
                quota,
                usage,
            },
        ));
        team_ids.push(id);
    }

    let mut leaf_ids = Vec::with_capacity(leaves_n);
    for i in 0..leaves_n {
        let parent = *rng.pick(&team_ids);
        let id = QuotaEntityId(next_uuid(rng));
        let quota = CostUnits(rng.range(10_000_000, 1_000_000_000_000));
        let usage = UsageState {
            usage: CostUnits(rng.range(0, quota.0.max(2))),
            last_update_us: base_time_us,
        };
        entities.push((
            id,
            QuotaEntity {
                parent: Some(parent),
                name: format!("leaf-{i}"),
                quota,
                usage,
            },
        ));
        leaf_ids.push(id);
    }

    QuotaTree {
        entities,
        leaves: leaf_ids,
    }
}

/// Build `count` nodes with plausible capacity, a few labels each, and ~5%
/// unschedulable (drained/maintenance).
fn build_nodes(rng: &mut Rng, count: usize) -> BTreeMap<NodeId, NodeRecord> {
    const ZONES: [&str; 4] = ["us-east-1a", "us-east-1b", "us-west-2a", "eu-west-1a"];
    const POOLS: [&str; 4] = ["default", "spot", "gpu", "batch"];
    const INSTANCE_TYPES: [&str; 4] = ["m6i.4xlarge", "m6i.8xlarge", "c6i.16xlarge", "r6i.4xlarge"];

    let mut pairs: Vec<(NodeId, NodeRecord)> = Vec::with_capacity(count);
    for _ in 0..count {
        let id = NodeId(next_uuid(rng));
        let capacity = Resources {
            cpu_millis: rng.range(16_000, 128_000),
            memory_bytes: rng.range(64 << 30, 512 << 30),
            disk_bytes: rng.range(500 << 30, 4_000 << 30),
        };
        let mut labels = BTreeMap::new();
        labels.insert("zone".to_string(), (*rng.pick(&ZONES)).to_string());
        labels.insert("pool".to_string(), (*rng.pick(&POOLS)).to_string());
        labels.insert(
            "instance-type".to_string(),
            (*rng.pick(&INSTANCE_TYPES)).to_string(),
        );
        let schedulable = rng.chance(95, 100);
        let epoch = rng.range(1, 6);
        pairs.push((
            id,
            NodeRecord {
                node: Node {
                    id,
                    capacity,
                    labels,
                    schedulable,
                },
                epoch,
            },
        ));
    }
    pairs.sort_unstable_by_key(|(id, _)| *id);
    pairs.into_iter().collect()
}

/// A realistic-looking image reference: `registry/name:tag@sha256:<hex>`,
/// roughly 40-70 characters.
fn gen_image(rng: &mut Rng) -> String {
    const REGISTRIES: [&str; 4] = [
        "registry.example.com",
        "ghcr.io/coppice",
        "docker.io/coppice",
        "us.pkg.dev/coppice",
    ];
    const NAMES: [&str; 8] = [
        "worker",
        "ingest",
        "embed-server",
        "batch-runner",
        "api-gateway",
        "video-encode",
        "ml-train",
        "feature-extract",
    ];
    let registry = rng.pick(&REGISTRIES);
    let name = rng.pick(&NAMES);
    let major = rng.range(0, 9);
    let minor = rng.range(0, 30);
    let patch = rng.range(0, 50);
    // 48 bits -> 12 hex chars, like a truncated sha256 digest.
    let sha = rng.next_u64() & 0xFFFF_FFFF_FFFF;
    format!("{registry}/{name}:v{major}.{minor}.{patch}@sha256:{sha:012x}")
}

/// A non-empty tokenized command line, plus an occasional entrypoint
/// override (both uphold the `Job` invariants the conversion boundary
/// enforces: command never empty, override argv never empty).
fn gen_command(rng: &mut Rng) -> (Vec<String>, Option<Vec<String>>) {
    const PROGRAMS: [&str; 5] = ["train", "ingest", "encode", "score", "compact"];
    const FLAGS: [&str; 6] = [
        "--epochs",
        "--shards",
        "--batch-size",
        "--workers",
        "--seed",
        "--timeout-s",
    ];
    let mut command = vec![rng.pick(&PROGRAMS).to_string()];
    for _ in 0..rng.range(0, 4) {
        command.push(rng.pick(&FLAGS).to_string());
        command.push(rng.range(1, 4096).to_string());
    }
    let entrypoint = rng
        .chance(1, 4)
        .then(|| vec!["/usr/local/bin/launch".to_string()]);
    (command, entrypoint)
}

/// Assert the cross-reference and accrual-queue invariants documented on
/// [`StateMachine`]. Shared with the determinism suite, which runs it on
/// replayed states as well as synthetic ones.
pub fn check_consistency(sm: &StateMachine) {
    // `next_allocation_seq` and seq uniqueness/ordering.
    let mut seqs: Vec<u64> = sm.allocations.values().map(|r| r.seq).collect();
    seqs.sort_unstable();
    for pair in seqs.windows(2) {
        assert!(
            pair[0] < pair[1],
            "allocation seq must be unique and strictly ordered"
        );
    }
    match seqs.last() {
        Some(&max_seq) => assert_eq!(sm.next_allocation_seq, max_seq + 1),
        None => assert_eq!(sm.next_allocation_seq, 0),
    }

    // The accrual queue is exactly the Accruing allocations, keyed (node, seq).
    let expected: imbl::OrdMap<(NodeId, u64), AllocationId> = sm
        .allocations
        .values()
        .filter(|r| r.allocation.state == AllocationState::Accruing)
        .map(|r| ((r.allocation.node, r.seq), r.allocation.id))
        .collect();
    assert_eq!(
        sm.accrual_queue, expected,
        "accrual_queue must match Accruing allocations exactly"
    );

    for (job_id, jr) in &sm.jobs {
        match jr.state {
            JobState::Queued | JobState::Succeeded | JobState::Failed | JobState::Aborted => {
                assert!(
                    jr.current_attempt.is_none(),
                    "job {job_id} in {:?} must have no current attempt",
                    jr.state
                );
            }
            JobState::Preparing | JobState::Running | JobState::Finalizing => {
                assert!(
                    jr.current_attempt.is_some(),
                    "live job {job_id} must have a current attempt"
                );
            }
            JobState::Submitted | JobState::Accepted => {}
        }
        if let Some(cur) = jr.current_attempt {
            assert!(
                jr.attempts.contains(&cur),
                "job {job_id}'s attempts must list its current attempt"
            );
        }
        assert!(
            sm.quota_entities.contains_key(&jr.spec.quota_entity),
            "job {job_id} references an unknown quota entity"
        );
        for aid in &jr.attempts {
            let ar = sm
                .attempts
                .get(aid)
                .unwrap_or_else(|| panic!("job {job_id} lists unknown attempt {aid}"));
            assert_eq!(
                ar.attempt.job, *job_id,
                "attempt {aid} back-reference must match its job"
            );
        }
        if let Some(cur) = jr.current_attempt {
            let ar = &sm.attempts[&cur];
            let alloc = &sm.allocations[&ar.attempt.allocation];
            assert_eq!(alloc.allocation.job, *job_id);
            assert_eq!(alloc.allocation.attempt, cur);
            assert_eq!(alloc.allocation.node, ar.attempt.node);
            let legal = matches!(
                (jr.state, &ar.attempt.state, alloc.allocation.state),
                (
                    JobState::Preparing,
                    AttemptState::Accruing,
                    AllocationState::Accruing
                ) | (
                    JobState::Preparing,
                    AttemptState::Ready,
                    AllocationState::Funded
                ) | (
                    JobState::Preparing,
                    AttemptState::Dispatching,
                    AllocationState::Funded
                ) | (
                    JobState::Running,
                    AttemptState::Running,
                    AllocationState::Active
                ) | (
                    JobState::Finalizing,
                    AttemptState::Finalizing,
                    AllocationState::Active
                )
            );
            assert!(
                legal,
                "illegal live combo for job {job_id}: job={:?} attempt={:?} allocation={:?}",
                jr.state, ar.attempt.state, alloc.allocation.state
            );
        }
    }

    for (aid, ar) in &sm.attempts {
        assert!(
            sm.jobs.contains_key(&ar.attempt.job),
            "attempt {aid} references unknown job"
        );
        assert!(
            sm.nodes.contains_key(&ar.attempt.node),
            "attempt {aid} references unknown node"
        );
        let alloc = sm
            .allocations
            .get(&ar.attempt.allocation)
            .unwrap_or_else(|| panic!("attempt {aid} references unknown allocation"));
        assert_eq!(
            alloc.allocation.attempt, *aid,
            "allocation back-reference must match its attempt"
        );
        assert_eq!(alloc.allocation.job, ar.attempt.job);
        assert_eq!(alloc.allocation.node, ar.attempt.node);
        if ar.attempt.state.is_terminal() {
            assert_eq!(
                alloc.allocation.state,
                AllocationState::Released,
                "a terminal attempt's allocation must be released"
            );
        }
    }

    for r in sm.allocations.values() {
        assert!(
            sm.nodes.contains_key(&r.allocation.node),
            "allocation references unknown node"
        );
    }

    for entity in sm.quota_entities.values() {
        if let Some(parent) = entity.parent {
            assert!(
                sm.quota_entities.contains_key(&parent),
                "quota entity references unknown parent"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_config_is_deterministic() {
        let cfg = SynthConfig::with_jobs(2_000);
        assert_eq!(synth_state(&cfg), synth_state(&cfg));
    }

    #[test]
    fn different_seeds_differ() {
        let mut cfg = SynthConfig::with_jobs(2_000);
        let a = synth_state(&cfg);
        cfg.seed = 1;
        let b = synth_state(&cfg);
        assert_ne!(a, b);
    }

    #[test]
    fn consistent_at_10k() {
        let cfg = SynthConfig::with_jobs(10_000);
        let state = synth_state(&cfg);
        assert_eq!(state.jobs.len(), 10_000);
        check_consistency(&state);
    }

    #[test]
    #[ignore = "1M-scale; run in release"]
    fn consistent_at_1m() {
        let cfg = SynthConfig::with_jobs(1_000_000);
        let start = std::time::Instant::now();
        let state = synth_state(&cfg);
        let elapsed = start.elapsed();
        eprintln!("synth_state(1_000_000 jobs) took {elapsed:?}");
        assert_eq!(state.jobs.len(), 1_000_000);
        check_consistency(&state);
    }

    /// The KOI-5 clone bound: view publication and snapshot capture clone the
    /// whole state on the apply task, so the clone must stay structurally
    /// shared (ADR 0028), never a deep copy. A deep copy at this scale runs
    /// hundreds of milliseconds; the bound would catch any regression to one.
    #[test]
    #[ignore = "1M-scale; run in release"]
    fn clone_at_1m_is_structurally_shared() {
        let cfg = SynthConfig::with_jobs(1_000_000);
        let state = synth_state(&cfg);
        let start = std::time::Instant::now();
        let cloned = state.clone();
        let elapsed = start.elapsed();
        eprintln!("clone of a 1M-job state took {elapsed:?}");
        assert_eq!(cloned.version, state.version);
        assert!(
            elapsed < std::time::Duration::from_millis(50),
            "full-state clone took {elapsed:?}; expected O(1) structural sharing"
        );
    }
}
