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
 * `JobState`: Submitted → Accepted → Queued → Preparing → Running →
 * Finalizing → terminal. NOTE: "accruing" is not a job state — a job sits
 * in `Preparing` while its attempt/allocation accrues capacity.
 */
export type JobState =
  | 'Submitted'
  | 'Accepted'
  | 'Queued'
  | 'Preparing'
  | 'Running'
  | 'Finalizing'
  | 'Succeeded'
  | 'Failed'
  | 'Aborted'

export const JOB_STATES: readonly JobState[] = [
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

export const TERMINAL_JOB_STATES: readonly JobState[] = ['Succeeded', 'Failed', 'Aborted']

/** `AttemptState`: one execution of a job (retries mint a new attempt). */
export type AttemptState =
  'Accruing' | 'Ready' | 'Dispatching' | 'Running' | 'Finalizing' | 'Terminal'

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

/** Mirrors `coppice_core::Job` (the immutable submitted spec). */
export interface JobSpec {
  image: string
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
  terminalAtUs: number | null
  retriesUsed: number
  abortRequested: { reason: string | null; requestedAtUs: number } | null
  /** Quota-entity ancestry, root first, leaf (the owning entity) last. */
  entityChain: QuotaEntityView[]
  attempts: AttemptView[]
  currentAttempt: AttemptId | null
  /** Present iff state is `Queued`. */
  queue: QueuePositionExplainer | null
  /** Present iff the current attempt is accruing. */
  accrual: AccrualView | null
  cost: CostReport
}

export interface ListJobsFilter {
  states?: JobState[]
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
  byState: Record<JobState, number>
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
