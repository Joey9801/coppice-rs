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
 * - Instants are `Date`, never a bare number: a `Date` carries its epoch and
 *   its unit, where a number carries neither and silently invites the
 *   thousand-fold mistake (reading µs as ms). This mirrors the Rust side,
 *   whose internal type is a `Timestamp` wrapping `chrono::DateTime<Utc>`.
 *   The wire carries ISO 8601 / RFC 3339 strings
 *   (`"2026-07-16T09:30:00.000000Z"`, always UTC, µs precision); parsing
 *   them into `Date` is the client's job at its boundary, like every other
 *   transport mapping below.
 * - Durations are whole seconds as a plain `number`, suffixed `Seconds` —
 *   a duration has no epoch or timezone to lose, which is what motivates
 *   `Date` for instants, and arithmetic on it needs no parser.
 * - Costs are µCU (micro cost units, `CostUnits` in Rust), suffixed `Ucu`.
 *   1 CU = 1_000_000 µCU (see lib/format.ts).
 * - All other numbers are plain `number`. The future real client owns the
 *   wire mapping (ISO strings, i64-as-string etc.) at its boundary; these
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
  | 'MemoryLimitExceeded'
  | 'RuntimeLimitExceeded'
  | 'DiskLimitExceeded'
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
  startedAt: Date | null
  endedAt: Date | null
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
  maxRuntimeSeconds: number | null
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
  submittedAt: Date
  terminalAt: Date | null
  /** Node of the current attempt, when one exists. */
  node: NodeId | null
  /**
   * State of the attempt `state` points at, when one exists — lets list
   * rows derive a phase (`derivePhase`) without a second fetch.
   */
  attemptState: AttemptState | null
  /** Min funded/requested fraction across dims; only while accruing. */
  fundingFraction: number | null
  /**
   * Gross µCU charged across attempts (upfront placement charges). NOT
   * trued-up when terminal — replicated state does not retain the per-job
   * settlement; the net figure is `JobDetail.cost.actualUcu`.
   */
  costUcu: number
  /** Outcome of the last attempt, when terminal. */
  outcome: AttemptOutcome | null
}

/**
 * Queue-position explainer for a `Queued` job (ADR 0021):
 * `score = multiplier / penaltyProduct + wAge * ageSeconds / ageHorizonSeconds`.
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
  ageSeconds: number
  ageHorizonSeconds: number
  wAge: number
  /** The additive aging term, wAge * ageSeconds / ageHorizonSeconds. */
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
  projectedStart: Date | null
}

export interface CostReport {
  /**
   * Base µCU/second from policy cost weights × requested resources, before any
   * multiplier. `rateBreakdown` sums to this.
   */
  rateUcuPerSecond: number
  /** Per-dimension split of the base rate (µCU/second), summing to `rateUcuPerSecond`. */
  rateBreakdown: { cpu: number; memory: number; disk: number }
  /** Priority-class multiplier (≥1) mapped from `spec.priority` by policy (ADR 0021). */
  priorityMultiplier: number
  /**
   * Runtime penalty folded in because the job declared no `max_runtime`
   * (ADR 0029, default 2×); 1 when the job is bounded.
   */
  unboundedMultiplier: number
  /** The µCU/second actually priced: `rateUcuPerSecond × priority × unbounded`. */
  effectiveRateUcuPerSecond: number
  /**
   * Duration the upfront placement charge covers: the job's `max_runtime`, or
   * the policy default charge runtime when `max_runtime` is unset.
   */
  chargeWindowSeconds: number
  /** True iff `chargeWindowSeconds` is the policy default (job declared no `max_runtime`). */
  chargeWindowIsDefault: boolean
  /** Upfront charge taken at placement: `effectiveRate × chargeWindow`. */
  estimatedUcu: number
  /** Total charged across attempts so far; 0 before the job is placed. */
  chargedUcu: number
  /**
   * Fraction (0..1) of the unused charge a true-up refunds (ADR 0029): the
   * policy default for a declared bound, or 1 (full) for unbounded jobs and
   * platform-attributable outcomes.
   */
  refundFraction: number
  /** Final cost after true-up; only when the job is terminal. */
  actualUcu: number | null
  /** Refund or surcharge applied at finalization. */
  trueUp: { kind: 'Refund' | 'Surcharge'; amountUcu: number } | null
}

export interface JobDetail {
  id: JobId
  state: JobState
  spec: JobSpec
  submittedAt: Date
  /**
   * When the job entered its current state. Server-derived from the
   * event history; drives "in this state for …" displays.
   */
  stateSince: Date
  terminalAt: Date | null
  retriesUsed: number
  abortRequested: { reason: string | null; requestedAt: Date } | null
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

/**
 * Recursive job-filter AST for `listJobs`, mirroring the server's
 * externally-tagged JSON filter (ListJobs v1 wire contract). Every node is a
 * one-key object; the client maps this camelCase/PascalCase shape to the
 * snake_case wire form at its boundary. Absent filter ⇒ match every job.
 *
 * Leaves:
 * - `phase`: matches the displayed phase (`derivePhase`), not the raw
 *   `JobState`; `in` is non-empty (empty is invalid).
 * - `entity`: matches jobs owned by a quota entity. `scope` defaults to
 *   `'subtree'` (the entity plus all descendants); `'exact'` matches only the
 *   named entity. An unknown entity id matches nothing (not an error).
 * - `node`: the current attempt's node. Unknown ⇒ matches nothing.
 * - `image`: exactly one of `contains` / `equals` (both or neither invalid).
 * - `id`: `in` is a non-empty set of job ids (empty is invalid).
 * - `search`: case-insensitive substring over the job id string OR the image
 *   string.
 * - `submitted`: at least one bound; `after` is inclusive (≥), `before` is
 *   exclusive (<); `after > before` is invalid.
 * - `requests`: a resource dimension with at least one of `min`/`max`, both
 *   inclusive; `min > max` is invalid.
 *
 * Caps (violation is invalid): max nesting depth 8, max 64 total nodes
 * (combinators + leaves). Empty `all`/`any`/`in` arrays are invalid.
 */
export type JobFilter =
  | { all: JobFilter[] }
  | { any: JobFilter[] }
  | { not: JobFilter }
  | { phase: { in: JobPhase[] } }
  | { entity: { id: QuotaEntityId; scope?: 'subtree' | 'exact' } }
  | { node: NodeId }
  | { image: { contains: string } | { equals: string } }
  | { id: { in: JobId[] } }
  | { search: string }
  | { submitted: { after?: Date; before?: Date } }
  | {
      requests: { resource: 'cpuMillis' | 'memoryBytes' | 'diskBytes'; min?: number; max?: number }
    }

/**
 * A single `listJobs` page request. `cursor` is the opaque token from a prior
 * response's `nextCursor` (continue from where that page left off); `limit` is
 * the page size (server default 100, valid range 1..=1000).
 */
export interface ListJobsRequest {
  filter?: JobFilter
  cursor?: string
  limit?: number
}

export interface JobList {
  jobs: JobSummary[]
  /**
   * Keyset cursor to continue from (feed back as `cursor`); `null` iff the
   * listing is exhausted. A short page with a NON-null cursor legitimately
   * means "continue" — the server may stop early on a scan budget, so never
   * treat a short page as "done" (only `nextCursor === null` means done).
   */
  nextCursor: string | null
}

// ---------------------------------------------------------------------------
// Timeline events (ADR 0032's one shared wire shape: identity + advisory
// stamp + a body mirroring the `Event` enum in coppice-state arm for arm)
// ---------------------------------------------------------------------------

/**
 * The event payload: kind plus the scope keys apply stamped while the
 * association was authoritative. `from`/`to` are state *kinds* — the wire
 * flattens `Attempting`'s attempt id away (it travels on attempt-scoped
 * events instead).
 */
export type TimelineEventBody =
  | { kind: 'JobSubmitted'; job: JobId }
  | { kind: 'JobStateChanged'; job: JobId; from: JobStateKind; to: JobStateKind }
  | {
      kind: 'AttemptStateChanged'
      attempt: AttemptId
      job: JobId
      node: NodeId
      state: AttemptState
    }
  | { kind: 'AllocationFunded'; allocation: AllocationId; job: JobId; node: NodeId }
  | { kind: 'StopRequested'; node: NodeId; allocation: AllocationId; job: JobId }
  | { kind: 'NodeEpochBumped'; node: NodeId; epoch: number }
  | { kind: 'JobEvicted'; job: JobId }
  | { kind: 'QuotaEntityConfigured'; entity: QuotaEntityId }
  | { kind: 'PolicyUpdated' }
  | { kind: 'ClusterVersionBumped'; to: number }

export type TimelineEvent = {
  /** The producing command's Raft log index. */
  index: number
  /**
   * The event's position within that command's full batch, assigned before
   * any filtering. `(index, ordinal)` is the event's identity — the only
   * valid ordering and deduplication key. Scoped streams may show ordinal
   * gaps within an index; they never renumber.
   */
  ordinal: number
  /**
   * Advisory proposer stamp ("when the proposer asserted this fact").
   * Stamps come from different replicas' clocks, so this may run backwards
   * as `index` advances: render it, never sort or deduplicate by it.
   */
  at: Date
} & TimelineEventBody

/**
 * A bounded most-recent window of events (ADR 0032, tier 1). `floorIndex`
 * is an exclusive coverage cursor: the window is complete for every index
 * strictly above it and claims nothing at or below. Empty `events` with a
 * high cursor is a freshly restarted coordinator, not a quiet cluster.
 */
export interface RecentEventsWindow {
  floorIndex: number
  events: TimelineEvent[]
}

// ---------------------------------------------------------------------------
// Usage / utilization series
// ---------------------------------------------------------------------------

export interface UsageSample {
  t: Date
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

// ---------------------------------------------------------------------------
// Job usage metrics — the real GET /api/v1/jobs/{job}/usage contract
// ---------------------------------------------------------------------------
// Mirrors the Rust DTOs in crates/coppice-api/src/http/dto.rs
// (GetJobUsageResponse / UsagePoint / UsageSourceRecord / UsageAvailability),
// the metrics twin of the job-logs pipeline. Supersedes the invented
// `JobUsage`/`UsageSample` proposal above (kept for the mock world and its
// tests) exactly as the real log DTOs superseded this file's invented
// `LogChunk`. Cursor-paged over the job's attempts with a per-attempt
// availability accounting; `order` defaults to `asc` (chart order).
//
// Counters are cumulative — a client differences consecutive samples to derive
// rates. CPU totals are integer microseconds (`...Us`), not whole seconds: a
// deliberate divergence from the `_seconds` duration convention so sub-second
// deltas between adjacent samples survive.

export type UsageAvailability = 'available' | 'expired' | 'unreachable' | 'not_started'

export interface UsagePoint {
  attempt: AttemptId
  at: Date
  cpuUsageTotalUs: number
  cpuThrottledTotalUs: number
  memoryUsedBytes: number
  memoryPeakBytes: number
  diskWritableBytes: number
  diskImageBytes: number
  netRxBytesTotal: number
  netTxBytesTotal: number
  blkioReadBytesTotal: number
  blkioWriteBytesTotal: number
}

export interface UsageSourceRecord {
  attempt: AttemptId
  /** Null only when the attempt record is missing from replicated state. */
  node: NodeId | null
  availability: UsageAvailability
  /** True when older samples the client asked for had been pruned. */
  truncated: boolean
  earliestAvailableAt: Date | null
  /** Detail for an expired/unreachable/not_started verdict; null when available. */
  reason: string | null
}

export interface GetJobUsageResponse {
  samples: UsagePoint[]
  sources: UsageSourceRecord[]
  /** Pass back to continue; null when the walk is complete. */
  nextCursor: string | null
}

export interface UtilizationSample {
  t: Date
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

/**
 * Liveness, derived from agent heartbeats (epoch fencing per ADR 0009).
 * `Unknown` is what the real API reports until heartbeat liveness is
 * wired server-side — the replicated state records no health input, and
 * a `DeclareNodeLost` is indistinguishable from an operator drain there.
 */
export type NodeHealth = 'Unknown' | 'Healthy' | 'Lost'

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
  lastHeartbeat: Date | null
  runningCount: number
  accruingCount: number
}

/** A finished attempt in the node's recent history. */
export interface NodeHistoryEntry {
  attempt: AttemptId
  job: JobId
  image: string
  outcome: AttemptOutcome
  startedAt: Date | null
  endedAt: Date
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
  createdAt: Date
  updatedAt: Date
  /** Live job counts over this entity's subtree (itself + descendants). */
  queuedCount: number
  runningCount: number
}

/** Subtree-inclusive stats for one quota entity. */
export interface QuotaEntityStats {
  /** Tallied by displayed phase (`derivePhase`), not the raw `JobState`. */
  byState: Record<JobPhase, number>
  oldestQueuedAgeSeconds: number | null
  /** Σ µCU/s of currently running attempts in the subtree. */
  burnRateUcuPerSecond: number
  /** µCU charged to this entity in the trailing 24h (pre-decay). */
  chargedUcu24h: number
  /** Recent decayed-usage samples for sparklines, oldest first. */
  usageHistory: Array<{ t: Date; usageUcu: number }>
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
  /**
   * Jobs leaving the queue (placed) per minute, recent window. `null` means
   * the serving replica has no windowed coverage yet (fresh restart or a
   * lost event stream) — a gap, never "nothing is draining" (ADR 0032).
   */
  drainRatePerMinute: number | null
  /** Jobs entering the queue per minute, recent window; `null` as above. */
  arrivalRatePerMinute: number | null
  oldestQueuedAgeSeconds: number | null
  /** Tallied by displayed phase (`derivePhase`), not the raw `JobState`. */
  byState: Record<JobPhase, number>
  /** Recent history for sparklines, oldest first. */
  history: Array<{
    t: Date
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
  /** Most recent cluster events, newest first, with its coverage cursor. */
  recentEvents: RecentEventsWindow
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
  lastSeen: Date
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
    takenAt: Date
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
  t: Date
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
