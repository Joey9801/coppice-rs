/**
 * UI-facing domain types.
 *
 * These mirror the Rust model (crates/coppice-core, coppice-state,
 * coppice-scheduler, coppice-consensus) by name and semantics so that
 * swapping the mock client for the real HTTP client is a pure transport
 * change. Conventions:
 *
 * - Ids are typed `<prefix>-<uuidv7>` strings (ADR 0024): `job-…`,
 *   `node-…`, `alloc-…`, `attempt-…`, `quota-…`. Coordinators are Raft
 *   ids (plain small integers), not uuid-typed ids.
 * - Timestamps are microseconds since the Unix epoch, suffixed `Us`
 *   (matching the Rust `*_us: i64` fields). Durations are also `Us`.
 * - Costs are µCU (micro cost units, `CostUnits` in Rust), suffixed `Ucu`.
 *   1 CU = 1_000_000 µCU (see lib/format.ts).
 * - All numbers are plain `number`. The future real client owns the
 *   proto3-JSON wire mapping (i64-as-string etc.) at its boundary; these
 *   types never change for that.
 */

export type JobId = string
export type NodeId = string
export type AttemptId = string
export type AllocationId = string
export type QuotaEntityId = string
/** Raft identity of a coordinator replica (`CoordinatorId = u64` in Rust). */
export type CoordinatorId = number

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

/** Mirrors `coppice_core::Resources { cpu_millis, memory_bytes, disk_bytes }`. */
export interface Resources {
  cpuMillis: number
  memoryBytes: number
  diskBytes: number
}

// ---------------------------------------------------------------------------
// Job lifecycle (mirrors coppice-core job.rs / attempt.rs / allocation.rs)
// ---------------------------------------------------------------------------

/**
 * `JobState`: Submitted → Accepted → Queued → Attempting(attempt) →
 * {Succeeded, Failed, Aborted}, with `Attempting → Queued` on retry (ADR
 * 0030). `Attempting` structurally carries the in-flight attempt id — there
 * is no separate `currentAttempt` field anywhere in this file;
 * `jobAttemptId` is the derived accessor and can never disagree with the
 * state. The job enum no longer distinguishes preparing/running/finalizing:
 * that detail comes only from joining `Attempting` with its attempt's
 * `AttemptState` (see `JobPhase`/`derivePhase` below).
 */
export type JobState =
  | { kind: 'Submitted' }
  | { kind: 'Accepted' }
  | { kind: 'Queued' }
  | { kind: 'Attempting'; attempt: AttemptId }
  | { kind: 'Succeeded' }
  | { kind: 'Failed' }
  | { kind: 'Aborted' }

export type JobStateKind = JobState['kind']

export const JOB_STATE_KINDS: readonly JobStateKind[] = [
  'Submitted',
  'Accepted',
  'Queued',
  'Attempting',
  'Succeeded',
  'Failed',
  'Aborted',
]

export const TERMINAL_JOB_STATE_KINDS: readonly JobStateKind[] = ['Succeeded', 'Failed', 'Aborted']

export function isTerminalJobState(state: JobState): boolean {
  return (TERMINAL_JOB_STATE_KINDS as readonly JobStateKind[]).includes(state.kind)
}

/** Derived accessor mirroring the Rust `JobState::attempt()` — cannot disagree with the state. */
export function jobAttemptId(state: JobState): AttemptId | null {
  return state.kind === 'Attempting' ? state.attempt : null
}

/** Human label for a `JobState`, e.g. for timelines; `Attempting` includes the attempt id. */
export function jobStateLabel(state: JobState): string {
  return state.kind === 'Attempting' ? `Attempting(${state.attempt})` : state.kind
}

/** `AttemptState`: one execution of a job (retries mint a new attempt). */
export type AttemptState =
  'Accruing' | 'Ready' | 'Dispatching' | 'Running' | 'Finalizing' | 'Terminal'

/**
 * Flat display "phase" for a job (ADR 0030 observability note): a read-time
 * join of `Attempting(id)` with that attempt's `AttemptState`, never
 * replicated or stored. Every UI surface that shows a single job status —
 * state pills, list filters, timelines, the overview breakdown — renders
 * this instead of the raw `JobState`, so the flat vocabulary users already
 * see (queued/preparing/running/finalizing/…) is unchanged even though the
 * job machine that produces it collapsed.
 */
export type JobPhase =
  | 'Submitted'
  | 'Accepted'
  | 'Queued'
  | 'Preparing'
  | 'Running'
  | 'Finalizing'
  | 'Succeeded'
  | 'Failed'
  | 'Aborted'

export const JOB_PHASES: readonly JobPhase[] = [
  'Submitted',
  'Accepted',
  'Queued',
  'Preparing',
  'Running',
  'Finalizing',
  'Succeeded',
  'Failed',
  'Aborted',
]

export const TERMINAL_JOB_PHASES: readonly JobPhase[] = ['Succeeded', 'Failed', 'Aborted']

/**
 * Join a job state with its current attempt's state (null when the job
 * carries no attempt, e.g. `Queued`) into the flat phase the UI shows.
 * `Accruing`/`Ready`/`Dispatching` all read as `Preparing` — the UI has
 * never distinguished them; a `Terminal` attempt means resolution is
 * completing in the same apply, which reads as `Finalizing` a beat longer.
 */
export function derivePhase(state: JobState, attemptState: AttemptState | null): JobPhase {
  if (state.kind !== 'Attempting') return state.kind
  switch (attemptState) {
    case 'Accruing':
    case 'Ready':
    case 'Dispatching':
      return 'Preparing'
    case 'Running':
      return 'Running'
    case 'Finalizing':
    case 'Terminal':
    case null:
      return 'Finalizing'
  }
}

/** `AttemptOutcome` — why an attempt reached `Terminal`. */
export type AttemptOutcomeKind =
  | 'Exited'
  | 'OomKilled'
  | 'MaxRuntimeExceeded'
  | 'Aborted'
  | 'Revoked'
  | 'PullFailed'
  | 'StartFailed'
  | 'NodeLost'
  | 'AgentError'

/** `OutcomeClass` — who "owns" the outcome (drives retry policy). */
export type OutcomeClass = 'Success' | 'UserError' | 'UserRequest' | 'Platform'

export interface AttemptOutcome {
  kind: AttemptOutcomeKind
  /** Present when kind is `Exited`. */
  exitCode?: number
  class: OutcomeClass
}

/**
 * `AllocationState`: an attempt's claim on one node. `funded` grows toward
 * `requested` (in commit order) as capacity frees; `Funded` means fully
 * funded but not yet active on the agent.
 */
export type AllocationState = 'Accruing' | 'Funded' | 'Active' | 'Released'

export interface AllocationView {
  id: AllocationId
  job: JobId
  attempt: AttemptId
  node: NodeId
  requested: Resources
  funded: Resources
  state: AllocationState
  /** Commit order — drives funding priority within a node. */
  seq: number
}

export interface AttemptView {
  id: AttemptId
  job: JobId
  node: NodeId
  allocation: AllocationId
  state: AttemptState
  /** Present iff state is `Terminal`. */
  outcome: AttemptOutcome | null
  startedAtUs: number | null
  endedAtUs: number | null
  /** µCU per second while running (from cost weights × requested resources). */
  rateUcuPerSecond: number
  /** Upfront charge for this attempt (trued-up at finalization). */
  chargedUcu: number
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

/**
 * Mirrors `coppice_core::Job` (the immutable submitted spec).
 *
 * NOTE: `env` is not yet on the Rust `Job` — it is the UI's proposal for
 * the environment overlay that lands with the Docker executor; reconcile
 * when `coppice_core::job` grows it.
 */
export interface JobSpec {
  image: string
  /**
   * The container command line, pre-tokenized (argv semantics, no shell).
   * Required and never empty. May be large — render lazily.
   */
  command: string[]
  /** Entrypoint override; null runs the image's own entrypoint. */
  entrypoint: string[] | null
  /** Environment overlay. May be large — render lazily. */
  env: Record<string, string>
  requests: Resources
  /** Small integer priority class, mapped to a multiplier by policy. */
  priority: number
  maxRuntimeUs: number | null
  quotaEntity: QuotaEntityId
  retry: {
    maxRetries: number
    retryUserErrors: boolean
  }
}

export interface JobSummary {
  id: JobId
  state: JobState
  image: string
  quotaEntity: QuotaEntityId
  quotaEntityName: string
  priority: number
  submittedAtUs: number
  terminalAtUs: number | null
  /** Node of the current attempt, when one exists. */
  node: NodeId | null
  /**
   * State of the attempt `state` points at, when one exists — lets list
   * rows derive a phase (`derivePhase`) without a second fetch.
   */
  attemptState: AttemptState | null
  /** 1-based queue rank; only for `Queued` jobs. */
  queueRank: number | null
  /** Min funded/requested fraction across dims; only while accruing. */
  fundingFraction: number | null
  /** Charged so far (upfront charges, trued-up when terminal). */
  costUcu: number
  /** Outcome of the last attempt, when terminal. */
  outcome: AttemptOutcome | null
}

/**
 * Queue-position explainer for a `Queued` job (ADR 0021):
 * `score = multiplier / penaltyProduct + wAge * ageUs / ageHorizonUs`.
 */
export interface QueuePositionExplainer {
  /** 1-based position in the ranked queue. */
  rank: number
  queueDepth: number
  score: number
  /** Priority multiplier m(j) from the job's priority class. */
  multiplier: number
  /** One entry per quota entity from leaf to root. */
  penaltyChain: Array<{
    entity: QuotaEntityId
    name: string
    usageUcu: number
    quotaUcu: number
    /** usage/quota above 1.0 counts against you. */
    overQuotaRatio: number
    /** This entity's multiplicative penalty ≥ 1. */
    penalty: number
  }>
  /** Product of the chain penalties, P(j). */
  penaltyProduct: number
  ageUs: number
  ageHorizonUs: number
  wAge: number
  /** The additive aging term, wAge * ageUs / ageHorizonUs. */
  ageBonus: number
}

/** Funding progress of an accruing allocation (ADR 0027). */
export interface AccrualView {
  allocation: AllocationView
  /** Fraction funded per dimension, 0..1. */
  fundedFraction: {
    cpu: number
    memory: number
    disk: number
  }
  /**
   * Earliest guaranteed full-funding time from committed capacity
   * releases; null means unbounded (no guaranteed release covers it).
   */
  projectedStartUs: number | null
}

export interface CostReport {
  /** µCU/second while running, from policy cost weights × requested. */
  rateUcuPerSecond: number
  /** Upfront estimate: rate × (maxRuntime or the policy default runtime). */
  estimatedUcu: number
  /** True iff the estimate used the policy default because maxRuntime is unset. */
  estimateUsedDefaultRuntime: boolean
  /** Total charged across attempts so far. */
  chargedUcu: number
  /** Final cost after true-up; only when the job is terminal. */
  actualUcu: number | null
  /** Refund or surcharge applied at finalization. */
  trueUp: { kind: 'Refund' | 'Surcharge'; amountUcu: number } | null
}

export interface JobDetail {
  id: JobId
  state: JobState
  spec: JobSpec
  submittedAtUs: number
  /**
   * When the job entered its current state (µs). Server-derived from the
   * event history; drives "in this state for …" displays.
   */
  stateSinceUs: number
  terminalAtUs: number | null
  retriesUsed: number
  abortRequested: { reason: string | null; requestedAtUs: number } | null
  /** Quota-entity ancestry, root first, leaf (the owning entity) last. */
  entityChain: QuotaEntityView[]
  attempts: AttemptView[]
  /** Present iff state is `Queued`. */
  queue: QueuePositionExplainer | null
  /** Present iff the current attempt is accruing. */
  accrual: AccrualView | null
  cost: CostReport
}

/** The `AttemptView` `job.state` currently points at, if any (derived, ADR 0030). */
export function jobCurrentAttempt(job: Pick<JobDetail, 'state' | 'attempts'>): AttemptView | null {
  const id = jobAttemptId(job.state)
  return id ? (job.attempts.find((a) => a.id === id) ?? null) : null
}

export interface ListJobsFilter {
  /** Filters by the displayed phase (`derivePhase`), not the raw `JobState`. */
  states?: JobPhase[]
  /** Matches the entity's whole subtree (a leaf matches just itself). */
  quotaEntity?: QuotaEntityId
  node?: NodeId
  /** Substring match on job id or image. */
  search?: string
  limit?: number
}

export interface JobList {
  jobs: JobSummary[]
  /** Total matching before `limit` was applied. */
  total: number
}

// ---------------------------------------------------------------------------
// Timeline events (mirrors the `Event` enum in coppice-state)
// ---------------------------------------------------------------------------

export type TimelineEvent = { atUs: number } & (
  | { kind: 'JobSubmitted'; job: JobId }
  | { kind: 'JobStateChanged'; job: JobId; from: JobState; to: JobState }
  | {
      kind: 'AttemptStateChanged'
      attempt: AttemptId
      job: JobId
      node: NodeId
      state: AttemptState
    }
  | { kind: 'AllocationFunded'; allocation: AllocationId; job: JobId; node: NodeId }
  | { kind: 'StopRequested'; job: JobId; reason: string | null }
  | { kind: 'NodeEpochBumped'; node: NodeId; epoch: number }
  | { kind: 'JobEvicted'; job: JobId; node: NodeId }
)

// ---------------------------------------------------------------------------
// Usage / utilization series
// ---------------------------------------------------------------------------

export interface UsageSample {
  tUs: number
  cpuMillis: number
  memoryBytes: number
  diskBytes: number
}

export interface JobUsage {
  /**
   * The attempt these samples belong to (usage is measured per attempt);
   * null when the job has no attempts yet. When the request named no
   * attempt, the server picks the current (else latest) one.
   */
  attempt: AttemptId | null
  /** What the job asked for — chart ceilings. */
  requested: Resources
  samples: UsageSample[]
}

export interface UtilizationSample {
  tUs: number
  /** Actually consumed. */
  used: Resources
  /** Funded/reserved by allocations at that instant. */
  allocated: Resources
}

export interface NodeUtilization {
  capacity: Resources
  samples: UtilizationSample[]
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/** Liveness, derived from agent heartbeats (epoch fencing per ADR 0009). */
export type NodeHealth = 'Healthy' | 'Lost'

export interface NodeSummary {
  id: NodeId
  capacity: Resources
  /** Sum of funded resources of non-Released allocations. */
  allocated: Resources
  /** Actual measured consumption. */
  used: Resources
  labels: Record<string, string>
  /** False = draining: no new placements, running work continues. */
  schedulable: boolean
  health: NodeHealth
  /** Bumps on (re)registration or loss; fences stale agent commands. */
  epoch: number
  lastHeartbeatUs: number | null
  runningCount: number
  accruingCount: number
}

/** A finished attempt in the node's recent history. */
export interface NodeHistoryEntry {
  attempt: AttemptId
  job: JobId
  image: string
  outcome: AttemptOutcome
  startedAtUs: number | null
  endedAtUs: number
}

export interface NodeDetail {
  summary: NodeSummary
  /** Attempts currently running/dispatching/finalizing on this node. */
  activeAttempts: AttemptView[]
  /** Accruing allocations queued against this node, in funding order. */
  accrualQueue: AccrualView[]
}

// ---------------------------------------------------------------------------
// Quota entities
// ---------------------------------------------------------------------------

export interface QuotaEntityView {
  id: QuotaEntityId
  name: string
  parent: QuotaEntityId | null
  quotaUcu: number
  /** Decayed usage as of "now" (24h half-life by default). */
  usageUcu: number
  overQuotaRatio: number
  /** Multiplicative scheduling penalty ≥ 1 derived from the ratio. */
  penalty: number
}

/**
 * How an entity came to exist. `sso` marks the auto-populated user tree
 * (ADR 0022): the coordinator mints an entity under the reserved `users`
 * root the first time it sees an OIDC principal, named after the `sub`
 * claim. Auto-minted entities stay editable (quota, child sub-queues) but
 * their name and position are owned by the identity, not the admin.
 */
export type QuotaEntityOrigin = 'configured' | 'sso'

/** One node of the quota-entity tree, as listed by the explorer. */
export interface QuotaEntityNode {
  id: QuotaEntityId
  /** Full display path, slash-separated ("Acme/Eng/Platform"). */
  name: string
  parent: QuotaEntityId | null
  origin: QuotaEntityOrigin
  /** OIDC `sub` the entity was auto-minted for; only on `sso` entities. */
  principal: string | null
  quotaUcu: number
  /** Decayed usage as of "now". */
  usageUcu: number
  overQuotaRatio: number
  penalty: number
  createdAtUs: number
  updatedAtUs: number
  /** Live job counts over this entity's subtree (itself + descendants). */
  queuedCount: number
  runningCount: number
}

/** Subtree-inclusive stats for one quota entity. */
export interface QuotaEntityStats {
  /** Tallied by displayed phase (`derivePhase`), not the raw `JobState`. */
  byState: Record<JobPhase, number>
  oldestQueuedAgeUs: number | null
  /** Σ µCU/s of currently running attempts in the subtree. */
  burnRateUcuPerSecond: number
  /** µCU charged to this entity in the trailing 24h (pre-decay). */
  chargedUcu24h: number
  /** Recent decayed-usage samples for sparklines, oldest first. */
  usageHistory: Array<{ tUs: number; usageUcu: number }>
}

export interface QuotaEntityDetail {
  entity: QuotaEntityNode
  /** Ancestry, root first, this entity last. */
  chain: QuotaEntityView[]
  children: QuotaEntityNode[]
  stats: QuotaEntityStats
}

/**
 * Mirrors the `ConfigureQuotaEntity` command: a create-or-update upsert,
 * no delete in v1 (entities with historical charges stay). Updates
 * preserve accumulated usage — reconfiguration is not an amnesty.
 */
export interface ConfigureQuotaEntityInput {
  /** Null proposes a create; the server mints the id. */
  entity: QuotaEntityId | null
  parent: QuotaEntityId | null
  name: string
  quotaUcu: number
}

// ---------------------------------------------------------------------------
// Cluster overview / queue stats
// ---------------------------------------------------------------------------

export interface QueueStats {
  /** Jobs currently in `Queued`. */
  depth: number
  /** Jobs leaving the queue (placed) per minute, recent window. */
  drainRatePerMinute: number
  /** Jobs entering the queue per minute, recent window. */
  arrivalRatePerMinute: number
  oldestQueuedAgeUs: number | null
  /** Tallied by displayed phase (`derivePhase`), not the raw `JobState`. */
  byState: Record<JobPhase, number>
  /** Recent history for sparklines, oldest first. */
  history: Array<{
    tUs: number
    depth: number
    drainedPerMinute: number
    arrivedPerMinute: number
  }>
}

export interface ClusterCapacity {
  nodes: { total: number; schedulable: number; lost: number }
  capacity: Resources
  allocated: Resources
  used: Resources
}

export interface ClusterOverview {
  clusterId: string
  queue: QueueStats
  capacity: ClusterCapacity
  /** Most recent cluster events, newest first. */
  recentEvents: TimelineEvent[]
}

// ---------------------------------------------------------------------------
// Coordinators (mirrors ConsensusStatus / ClusterSummary in coppice-consensus)
// ---------------------------------------------------------------------------

export type CoordinatorRole = 'Leader' | 'Follower' | 'Learner'

export interface CoordinatorMember {
  id: CoordinatorId
  /** Dial address peers use (host:port). */
  addr: string
  role: CoordinatorRole
  voter: boolean
  /** Highest applied Raft log index on this member. */
  lastApplied: number
  /** Entries behind the leader's committed index. */
  replicationLagEntries: number
  /** Host resource use of the coordinator process's machine, 0..1. */
  host: { cpuFraction: number; memoryFraction: number; diskFraction: number }
  lastSeenUs: number
}

export interface CoordinatorStatus {
  clusterId: string
  leader: CoordinatorId | null
  term: number
  /** Highest committed log index known cluster-wide. */
  knownCommitted: number
  /** Highest applied log index on the serving replica. */
  lastApplied: number
  /** State-machine command count (distinct from the Raft log index). */
  stateVersion: number
  snapshot: {
    sizeBytes: number
    /** Log index the last snapshot covers. */
    lastIncludedIndex: number
    takenAtUs: number
    entriesSinceSnapshot: number
  }
  /** Object counts in the replicated state machine. */
  stateCounts: {
    jobs: number
    attempts: number
    allocations: number
    nodes: number
    quotaEntities: number
  }
  members: CoordinatorMember[]
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------
// NOTE: no log storage/streaming exists in the backend yet — this shape is
// the UI's proposal for that future API (cursor-paged, ADR 0008 style).

export type LogLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error'

export interface LogEntry {
  tUs: number
  level: LogLevel
  /** Module path / component that emitted the line. */
  target: string
  message: string
}

export interface LogChunk {
  entries: LogEntry[]
  /** Pass back to fetch older entries; null when history is exhausted. */
  nextCursor: string | null
}

// ---------------------------------------------------------------------------
// Session (auth stub — real SSO lands with ADRs 0022/0023)
// ---------------------------------------------------------------------------

export interface Session {
  subject: string
  name: string
  email: string | null
  roles: string[]
}
