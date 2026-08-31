import { getAccessToken } from '@/auth/oidc'
import type { CoppiceApi } from './client'
import { ApiError } from './client'
import type { ApiErrorCode } from './client'
import {
  JOB_PHASES,
  type AccrualView,
  type AllocationState,
  type AllocationView,
  type AttemptId,
  type AttemptOutcome,
  type AttemptOutcomeKind,
  type AttemptState,
  type AttemptView,
  type ClusterOverview,
  type ConfigureQuotaEntityInput,
  type CoordinatorMember,
  type CoordinatorRole,
  type CoordinatorStatus,
  type CostReport,
  type GetJobUsageResponse,
  type JobDetail,
  type JobFilter,
  type JobId,
  type JobList,
  type JobPhase,
  type JobSpec,
  type JobState,
  type JobStateKind,
  type JobSummary,
  type ListJobsRequest,
  type LogChunk,
  type LogEntry,
  type NodeDetail,
  type NodeHealth,
  type NodeId,
  type NodeSummary,
  type OutcomeClass,
  type QueuePositionExplainer,
  type QueueStats,
  type QuotaEntityDetail,
  type QuotaEntityId,
  type QuotaEntityNode,
  type QuotaEntityStats,
  type QuotaEntityView,
  type Resources,
  type Session,
  type TimelineEvent,
  type TimelineEventBody,
} from './types'

/** `CostReport['trueUp']['kind']` has no standalone exported name. */
type TrueUpKind = NonNullable<CostReport['trueUp']>['kind']

/**
 * The real `CoppiceApi` implementation: `fetch` against `/api/v1/...` on the
 * coordinator's client listener (ADR 0031). Every method here owns exactly
 * one thing beyond the HTTP call — the wire ↔ domain mapping described in
 * `web/CLAUDE.md`'s "How to replace a mock endpoint with a real one":
 *
 * - snake_case keys ↔ camelCase fields
 * - lower_snake_case enum strings ↔ the PascalCase string-union values
 *   `types.ts` uses
 * - ISO 8601 instant strings ↔ `Date`
 * - bare typed-string / numeric ids pass through unchanged
 * - `{ code, message }` error bodies ↔ `ApiError`
 *
 * Only endpoints the server actually implements are exported here; the
 * delegation table in `./index.ts` decides, per method, whether a real call
 * or the mock backs it — nothing here is reachable for a still-`501` route.
 *
 * A handful of `types.ts` fields have no source in the current wire
 * contract (documented inline at each call site, e.g. quota-entity SSO
 * `origin`/`principal`, per-member coordinator `host`/`lastSeen`, and the
 * `QueuePositionExplainer` ranking fields the server deliberately does not
 * compute — see `crates/coppice-api/src/http/dto.rs`). Those are filled
 * with an honest, documented default rather than left to crash; when the
 * server grows the data, only the mapping here needs to change.
 */
export function createRealClient(): CoppiceApi {
  return {
    getSession: () => getJson('/session', mapSession),

    getClusterOverview: () => getJson('/overview', mapClusterOverview),
    getQueueStats: () => getJson('/queue/stats', mapQueueStats),

    listJobs: (request) => listJobs(request),
    getJob: (id) => getJson(`/jobs/${encodeURIComponent(id)}`, mapJobDetail),
    getJobTimeline: (id) =>
      getJson(`/jobs/${encodeURIComponent(id)}/timeline`, (body: WireGetJobTimelineResponse) =>
        body.events.map(mapTimelineEvent),
      ),
    getJobUsage: (id, attempt) => getJobUsage(id, attempt ?? null),
    getJobLogs: (id, cursor) => getJobLogs(id, cursor),

    listNodes: () =>
      getJson('/nodes', (body: WireListNodesResponse) => body.nodes.map(mapNodeSummary)),
    getNode: (id) => getJson(`/nodes/${encodeURIComponent(id)}`, mapNodeDetail),
    getNodeUtilization: () => {
      throw new Error('getNodeUtilization has no real implementation — route through the mock')
    },
    getNodeHistory: () => {
      throw new Error('getNodeHistory has no real implementation — route through the mock')
    },
    getNodeLogs: () => {
      throw new Error('getNodeLogs has no real implementation — route through the mock')
    },

    getCoordinatorStatus: () => getJson('/coordinators', mapCoordinatorStatus),
    getCoordinatorLogs: () => {
      throw new Error('getCoordinatorLogs has no real implementation — route through the mock')
    },

    listQuotaEntities: () =>
      getJson('/quota-entities', (body: WireListQuotaEntitiesResponse) =>
        body.entities.map(mapQuotaEntityNode),
      ),
    getQuotaEntity: (id) =>
      getJson(`/quota-entities/${encodeURIComponent(id)}`, mapQuotaEntityDetail),
    configureQuotaEntity: (input) => configureQuotaEntity(input),
  }
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

/** The `{ code, message }` shape every failure leaving the HTTP layer carries. */
interface WireErrorBody {
  code: string
  message: string
}

const WIRE_ERROR_CODES: Record<string, ApiErrorCode> = {
  INVALID_ARGUMENT: 'InvalidArgument',
  UNAUTHENTICATED: 'Unauthenticated',
  PERMISSION_DENIED: 'PermissionDenied',
  NOT_FOUND: 'NotFound',
  REJECTED: 'Rejected',
  NOT_LEADER: 'NotLeader',
  UNAVAILABLE: 'Unavailable',
  UNIMPLEMENTED: 'Unimplemented',
  INTERNAL: 'Internal',
}

/**
 * Fallback when the body is missing, not JSON, or carries a code this client
 * does not know: not every 401 the browser sees comes from the coordinator's
 * error layer (a reverse proxy or an auth gateway in front of it can answer
 * with an HTML page, or nothing at all), and an `Unauthenticated` that
 * degrades to `Internal` silently disables the centralized re-login in
 * `query-client.ts`. Only the two statuses the UI actually branches on are
 * mapped here — a credential problem (401) and an authority problem (403);
 * everything else stays `Internal`, since nothing downstream distinguishes
 * those and inventing a domain code from a proxy's status would be a guess.
 */
const STATUS_ERROR_CODES: Record<number, ApiErrorCode> = {
  401: 'Unauthenticated',
  403: 'PermissionDenied',
}

/**
 * Every call carries the bearer token whenever one is held (ADR 0022). It is
 * read per request rather than captured at client construction so a token
 * refresh takes effect immediately; when no token is held — open mode, or
 * before the first login — the header is simply absent and the server
 * decides. An explicit `Authorization` in `init.headers` still wins.
 */
async function request(path: string, init?: RequestInit): Promise<Response> {
  let response: Response
  const token = getAccessToken()
  try {
    response = await fetch(`/api/v1${path}`, {
      ...init,
      headers: {
        Accept: 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...init?.headers,
      },
    })
  } catch (err) {
    throw new ApiError('Unavailable', err instanceof Error ? err.message : 'network request failed')
  }
  if (!response.ok) {
    let body: WireErrorBody | null = null
    try {
      body = (await response.json()) as WireErrorBody
    } catch {
      // Non-JSON error body (e.g. a proxy's HTML error page) — fall through
      // to the status-derived code below.
    }
    const code =
      (body && WIRE_ERROR_CODES[body.code]) ?? STATUS_ERROR_CODES[response.status] ?? 'Internal'
    const message = body?.message ?? `request failed with status ${response.status}`
    const leaderHint = response.headers.get('coppice-leader') ?? undefined
    throw new ApiError(code, message, leaderHint)
  }
  return response
}

async function getJson<Wire, T>(path: string, map: (body: Wire) => T): Promise<T> {
  const response = await request(path)
  const body = (await response.json()) as Wire
  return map(body)
}

async function postJson<Wire, T>(path: string, body: unknown, map: (body: Wire) => T): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  const wire = (await response.json()) as Wire
  return map(wire)
}

// ---------------------------------------------------------------------------
// Shared scalar mapping
// ---------------------------------------------------------------------------

function toDate(iso: string): Date {
  return new Date(iso)
}

function toDateOrNull(iso: string | null): Date | null {
  return iso === null ? null : toDate(iso)
}

/** `"some_thing"` → `"SomeThing"` — every wire enum's snake_case → PascalCase. */
function snakeToPascal(s: string): string {
  return s
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('')
}

interface WireResources {
  cpu_millis: number
  memory_bytes: number
  disk_bytes: number
}

function mapResources(r: WireResources): Resources {
  return { cpuMillis: r.cpu_millis, memoryBytes: r.memory_bytes, diskBytes: r.disk_bytes }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

type WireBindingRole = 'submitter' | 'operator' | 'admin'

interface WireSessionBinding {
  role: WireBindingRole
  /** Subtree root the role is scoped to; `null` = cluster-wide. */
  scope: QuotaEntityId | null
}

interface WireGetSessionResponse {
  principal: string
  groups: string[]
  auth_method: 'bearer' | 'operator_cert' | 'open'
  name: string | null
  email: string | null
  bindings: WireSessionBinding[]
  implicit_admin: boolean
}

/**
 * The ADR 0023 authority summary → the flat `Session` the UI consumes.
 *
 * `roles` is the deduplicated set of roles from the matching bindings, in the
 * closed role set's own order rather than the wire's, so it reads the same
 * however the bindings happen to be stored. The per-binding `scope` is
 * deliberately dropped here: no UI consumes subtree scoping yet (see
 * `canConfigureEntities`), and inventing a scoped shape before there is a
 * consumer would be guessing at the eventual one.
 *
 * `name` is presentation-only and frequently absent — the `name` claim, else
 * the `email` claim, else the opaque principal, which is always something to
 * render (`cert:<CN>` for an operator certificate, `anonymous` in open mode).
 */
function mapSession(s: WireGetSessionResponse): Session {
  const roles: WireBindingRole[] = ['submitter', 'operator', 'admin']
  return {
    subject: s.principal,
    name: s.name ?? s.email ?? s.principal,
    email: s.email,
    roles: roles.filter((role) => s.bindings.some((b) => b.role === role)),
    implicitAdmin: s.implicit_admin,
  }
}

// ---------------------------------------------------------------------------
// Jobs / attempts / allocations
// ---------------------------------------------------------------------------

type WireJobStateKind =
  'submitted' | 'accepted' | 'queued' | 'attempting' | 'succeeded' | 'failed' | 'aborted'

function mapJobStateKind(s: WireJobStateKind): JobStateKind {
  return snakeToPascal(s) as JobStateKind
}

/** `JobSummary` carries its `attempt` id alongside the flattened state kind. */
function mapJobState(state: WireJobStateKind, attempt: AttemptId | null): JobState {
  const kind = mapJobStateKind(state)
  if (kind === 'Attempting') {
    if (attempt === null) {
      throw new Error(`wire job state "attempting" carried no attempt id`)
    }
    return { kind: 'Attempting', attempt }
  }
  return { kind } as JobState
}

type WireAttemptState = 'accruing' | 'ready' | 'dispatching' | 'running' | 'finalizing' | 'terminal'

function mapAttemptState(s: WireAttemptState): AttemptState {
  return snakeToPascal(s) as AttemptState
}

type WireAttemptOutcomeKind =
  | 'exited'
  | 'memory_limit_exceeded'
  | 'runtime_limit_exceeded'
  | 'disk_limit_exceeded'
  | 'aborted'
  | 'revoked'
  | 'pull_failed'
  | 'start_failed'
  | 'node_lost'
  | 'agent_error'

type WireOutcomeClass = 'success' | 'user_error' | 'user_request' | 'platform'

interface WireAttemptOutcome {
  kind: WireAttemptOutcomeKind
  exit_code?: number | null
  class: WireOutcomeClass
}

function mapAttemptOutcome(o: WireAttemptOutcome): AttemptOutcome {
  return {
    kind: snakeToPascal(o.kind) as AttemptOutcomeKind,
    ...(o.exit_code != null ? { exitCode: o.exit_code } : {}),
    class: snakeToPascal(o.class) as OutcomeClass,
  }
}

interface WireAttemptView {
  id: AttemptId
  job: JobId
  node: NodeId
  allocation: string
  state: WireAttemptState
  outcome: WireAttemptOutcome | null
  started_at: string | null
  ended_at: string | null
  rate_ucu_per_second: number
  charged_ucu: number
}

function mapAttemptView(a: WireAttemptView): AttemptView {
  return {
    id: a.id,
    job: a.job,
    node: a.node,
    allocation: a.allocation,
    state: mapAttemptState(a.state),
    outcome: a.outcome ? mapAttemptOutcome(a.outcome) : null,
    startedAt: toDateOrNull(a.started_at),
    endedAt: toDateOrNull(a.ended_at),
    rateUcuPerSecond: a.rate_ucu_per_second,
    chargedUcu: a.charged_ucu,
  }
}

type WireAllocationState = 'accruing' | 'funded' | 'active' | 'released'

interface WireAllocationView {
  id: string
  job: JobId
  attempt: AttemptId
  node: NodeId
  requested: WireResources
  funded: WireResources
  state: WireAllocationState
  seq: number
}

function mapAllocationView(a: WireAllocationView): AllocationView {
  return {
    id: a.id,
    job: a.job,
    attempt: a.attempt,
    node: a.node,
    requested: mapResources(a.requested),
    funded: mapResources(a.funded),
    state: snakeToPascal(a.state) as AllocationState,
    seq: a.seq,
  }
}

interface WireAccrualView {
  allocation: WireAllocationView
  funded_fraction: { cpu: number; memory: number; disk: number }
  projected_start: string | null
}

function mapAccrualView(a: WireAccrualView): AccrualView {
  return {
    allocation: mapAllocationView(a.allocation),
    fundedFraction: a.funded_fraction,
    projectedStart: toDateOrNull(a.projected_start),
  }
}

type WireJobPhase =
  | 'submitted'
  | 'accepted'
  | 'queued'
  | 'preparing'
  | 'running'
  | 'finalizing'
  | 'succeeded'
  | 'failed'
  | 'aborted'

/** `Record<JobPhase, number>` keyed lower_snake_case on the wire. */
function mapPhaseCounts(byState: Record<string, number>): Record<JobPhase, number> {
  const out = {} as Record<JobPhase, number>
  for (const phase of JOB_PHASES) {
    out[phase] = byState[phase.toLowerCase()] ?? 0
  }
  return out
}

// ---------------------------------------------------------------------------
// Cluster overview / queue stats
// ---------------------------------------------------------------------------

interface WireQueueSample {
  t: string
  depth: number
  drained_per_minute: number
  arrived_per_minute: number
}

interface WireQueueStats {
  depth: number
  drain_rate_per_minute: number | null
  arrival_rate_per_minute: number | null
  oldest_queued_age_seconds: number | null
  by_state: Record<WireJobPhase, number>
  history: WireQueueSample[]
}

function mapQueueStats(q: WireQueueStats): QueueStats {
  return {
    depth: q.depth,
    drainRatePerMinute: q.drain_rate_per_minute,
    arrivalRatePerMinute: q.arrival_rate_per_minute,
    oldestQueuedAgeSeconds: q.oldest_queued_age_seconds,
    byState: mapPhaseCounts(q.by_state),
    history: q.history.map((h) => ({
      t: toDate(h.t),
      depth: h.depth,
      drainedPerMinute: h.drained_per_minute,
      arrivedPerMinute: h.arrived_per_minute,
    })),
  }
}

type WireTimelineEventBody =
  | { kind: 'job_submitted'; job: JobId }
  | { kind: 'job_state_changed'; job: JobId; from: WireJobStateKind; to: WireJobStateKind }
  | {
      kind: 'attempt_state_changed'
      attempt: AttemptId
      job: JobId
      node: NodeId
      state: WireAttemptState
    }
  | { kind: 'allocation_funded'; allocation: string; job: JobId; node: NodeId }
  | { kind: 'stop_requested'; node: NodeId; allocation: string; job: JobId }
  | { kind: 'node_epoch_bumped'; node: NodeId; epoch: number }
  | { kind: 'job_evicted'; job: JobId }
  | { kind: 'quota_entity_configured'; entity: QuotaEntityId }
  | { kind: 'policy_updated' }
  | { kind: 'authorization_updated' }
  | { kind: 'cluster_version_bumped'; to: number }

interface WireTimelineEvent {
  index: number
  ordinal: number
  at: string
  kind: WireTimelineEventBody['kind']
  [key: string]: unknown
}

function mapTimelineEventBody(e: WireTimelineEvent): TimelineEventBody {
  switch (e.kind) {
    case 'job_submitted':
      return { kind: 'JobSubmitted', job: e.job as JobId }
    case 'job_state_changed':
      return {
        kind: 'JobStateChanged',
        job: e.job as JobId,
        from: mapJobStateKind(e.from as WireJobStateKind),
        to: mapJobStateKind(e.to as WireJobStateKind),
      }
    case 'attempt_state_changed':
      return {
        kind: 'AttemptStateChanged',
        attempt: e.attempt as AttemptId,
        job: e.job as JobId,
        node: e.node as NodeId,
        state: mapAttemptState(e.state as WireAttemptState),
      }
    case 'allocation_funded':
      return {
        kind: 'AllocationFunded',
        allocation: e.allocation as string,
        job: e.job as JobId,
        node: e.node as NodeId,
      }
    case 'stop_requested':
      return {
        kind: 'StopRequested',
        node: e.node as NodeId,
        allocation: e.allocation as string,
        job: e.job as JobId,
      }
    case 'node_epoch_bumped':
      return { kind: 'NodeEpochBumped', node: e.node as NodeId, epoch: e.epoch as number }
    case 'job_evicted':
      return { kind: 'JobEvicted', job: e.job as JobId }
    case 'quota_entity_configured':
      return { kind: 'QuotaEntityConfigured', entity: e.entity as QuotaEntityId }
    case 'policy_updated':
      return { kind: 'PolicyUpdated' }
    case 'authorization_updated':
      return { kind: 'AuthorizationUpdated' }
    case 'cluster_version_bumped':
      return { kind: 'ClusterVersionBumped', to: e.to as number }
  }
}

function mapTimelineEvent(e: WireTimelineEvent): TimelineEvent {
  return {
    index: e.index,
    ordinal: e.ordinal,
    at: toDate(e.at),
    ...mapTimelineEventBody(e),
  }
}

interface WireGetJobTimelineResponse {
  events: WireTimelineEvent[]
  floor_index: number
  next_cursor: string | null
}

interface WireNodeCounts {
  total: number
  schedulable: number
  lost: number
}

interface WireClusterCapacity {
  nodes: WireNodeCounts
  capacity: WireResources
  allocated: WireResources
  used: WireResources
}

interface WireRecentEventsWindow {
  floor_index: number
  events: WireTimelineEvent[]
}

interface WireGetClusterOverviewResponse {
  cluster_id: string
  queue: WireQueueStats
  capacity: WireClusterCapacity
  recent_events: WireRecentEventsWindow
}

function mapClusterOverview(o: WireGetClusterOverviewResponse): ClusterOverview {
  return {
    clusterId: o.cluster_id,
    queue: mapQueueStats(o.queue),
    capacity: {
      nodes: o.capacity.nodes,
      capacity: mapResources(o.capacity.capacity),
      allocated: mapResources(o.capacity.allocated),
      used: mapResources(o.capacity.used),
    },
    recentEvents: {
      floorIndex: o.recent_events.floor_index,
      events: o.recent_events.events.map(mapTimelineEvent),
    },
  }
}

// ---------------------------------------------------------------------------
// Jobs list / detail
// ---------------------------------------------------------------------------

interface WireJobSummary {
  id: JobId
  state: WireJobStateKind
  attempt: AttemptId | null
  image: string
  quota_entity: QuotaEntityId
  quota_entity_name: string
  priority: number
  submitted_at: string
  terminal_at: string | null
  node: NodeId | null
  attempt_state: WireAttemptState | null
  funding_fraction: number | null
  cost_ucu: number
  outcome: WireAttemptOutcome | null
}

function mapJobSummary(j: WireJobSummary): JobSummary {
  return {
    id: j.id,
    state: mapJobState(j.state, j.attempt),
    image: j.image,
    quotaEntity: j.quota_entity,
    quotaEntityName: j.quota_entity_name,
    priority: j.priority,
    submittedAt: toDate(j.submitted_at),
    terminalAt: toDateOrNull(j.terminal_at),
    node: j.node,
    attemptState: j.attempt_state ? mapAttemptState(j.attempt_state) : null,
    fundingFraction: j.funding_fraction,
    costUcu: j.cost_ucu,
    outcome: j.outcome ? mapAttemptOutcome(j.outcome) : null,
  }
}

interface WireListJobsResponse {
  jobs: WireJobSummary[]
  next_cursor: string | null
}

const JOB_PHASE_WIRE: Record<JobPhase, string> = {
  Submitted: 'submitted',
  Accepted: 'accepted',
  Queued: 'queued',
  Preparing: 'preparing',
  Running: 'running',
  Finalizing: 'finalizing',
  Succeeded: 'succeeded',
  Failed: 'failed',
  Aborted: 'aborted',
}

const REQUESTS_RESOURCE_WIRE: Record<string, string> = {
  cpuMillis: 'cpu_millis',
  memoryBytes: 'memory_bytes',
  diskBytes: 'disk_bytes',
}

/**
 * `JobFilter` (types.ts) is already externally-tagged one-key-object JSON
 * matching the wire `JobFilter` enum shape one for one — the only
 * conversions needed are `JobPhase`/`RequestsResource` casing and `Date` →
 * ISO string on the `submitted` leaf.
 */
function filterToWire(f: JobFilter): unknown {
  if ('all' in f) return { all: f.all.map(filterToWire) }
  if ('any' in f) return { any: f.any.map(filterToWire) }
  if ('not' in f) return { not: filterToWire(f.not) }
  if ('phase' in f) return { phase: { in: f.phase.in.map((p) => JOB_PHASE_WIRE[p]) } }
  if ('entity' in f) return { entity: f.entity }
  if ('node' in f) return { node: f.node }
  if ('image' in f) return { image: f.image }
  if ('id' in f) return { id: f.id }
  if ('search' in f) return { search: f.search }
  if ('submitted' in f) {
    return {
      submitted: {
        ...(f.submitted.after ? { after: f.submitted.after.toISOString() } : {}),
        ...(f.submitted.before ? { before: f.submitted.before.toISOString() } : {}),
      },
    }
  }
  return {
    requests: {
      ...f.requests,
      resource: REQUESTS_RESOURCE_WIRE[f.requests.resource] ?? f.requests.resource,
    },
  }
}

async function listJobs(req: ListJobsRequest): Promise<JobList> {
  const params = new URLSearchParams()
  if (req.filter) params.set('filter', JSON.stringify(filterToWire(req.filter)))
  if (req.cursor) params.set('cursor', req.cursor)
  if (req.limit) params.set('limit', String(req.limit))
  const qs = params.toString()
  return getJson(`/jobs${qs ? `?${qs}` : ''}`, (body: WireListJobsResponse) => ({
    jobs: body.jobs.map(mapJobSummary),
    nextCursor: body.next_cursor,
  }))
}

interface WireJobSpecView {
  image: string
  command: string[]
  entrypoint: string[] | null
  requests: WireResources
  priority: number
  max_runtime_seconds: number | null
  quota_entity: QuotaEntityId
  retry: { max_retries: number; retry_user_errors: boolean }
}

function mapJobSpec(s: WireJobSpecView): JobSpec {
  return {
    image: s.image,
    command: s.command,
    entrypoint: s.entrypoint,
    // No source on the wire yet (`coppice_core::job::Job` carries no env
    // overlay — see the dto.rs `JobSpecView` deviation note); an empty
    // overlay is the honest default until the domain type grows one.
    env: {},
    requests: mapResources(s.requests),
    priority: s.priority,
    maxRuntimeSeconds: s.max_runtime_seconds,
    quotaEntity: s.quota_entity,
    retry: { maxRetries: s.retry.max_retries, retryUserErrors: s.retry.retry_user_errors },
  }
}

/**
 * `over_quota_ratio`/`penalty` are `f64` on the server and serialize as JSON
 * `null` when infinite (a zero-quota entity with nonzero usage — see the
 * dto.rs `QuotaEntityNode` doc comment); JSON has no infinity. Modeled as
 * `number | null` here and mapped to `Number.POSITIVE_INFINITY` below so the
 * app-side type stays a plain `number` with the honest in-memory value.
 */
interface WirePenaltyLink {
  entity: QuotaEntityId
  name: string
  usage_ucu: number
  quota_ucu: number
  over_quota_ratio: number | null
  penalty: number | null
}

interface WireQueuePositionExplainer {
  multiplier: number
  penalty_chain: WirePenaltyLink[]
  /** Product of the chain penalties — also `null` when infinite; see above. */
  penalty_product: number | null
  age_seconds: number
}

/** `null` (non-finite on the wire) becomes the honest in-memory `Infinity`. */
function orInfinity(n: number | null): number {
  return n ?? Number.POSITIVE_INFINITY
}

/**
 * The server deliberately omits `rank`/`queueDepth`/`score`/`wAge`/
 * `ageHorizonSeconds`/`ageBonus` (an O(queue) ranking scan the dto.rs
 * `QueuePositionExplainer` comment says was cut rather than served badly).
 * `types.ts` still requires them, so this fills the rankless fields with 0
 * (an honest "not computed", not a fabricated position) and computes
 * `score` from only the terms the server does provide (no age bonus).
 */
function mapQueuePositionExplainer(q: WireQueuePositionExplainer): QueuePositionExplainer {
  return {
    rank: 0,
    queueDepth: 0,
    // `null` penalty_product means infinite (see `orInfinity`): the job's
    // effective score is then 0 (multiplier / Infinity), the lowest
    // priority, not the `q.multiplier` fallback used for the finite
    // non-positive case (penalty_product not yet computed).
    score:
      q.penalty_product === null
        ? 0
        : q.penalty_product > 0
          ? q.multiplier / q.penalty_product
          : q.multiplier,
    multiplier: q.multiplier,
    penaltyChain: q.penalty_chain.map((p) => ({
      entity: p.entity,
      name: p.name,
      usageUcu: p.usage_ucu,
      quotaUcu: p.quota_ucu,
      overQuotaRatio: orInfinity(p.over_quota_ratio),
      penalty: orInfinity(p.penalty),
    })),
    penaltyProduct: orInfinity(q.penalty_product),
    ageSeconds: q.age_seconds,
    ageHorizonSeconds: 0,
    wAge: 0,
    ageBonus: 0,
  }
}

interface WireCostReport {
  rate_ucu_per_second: number
  rate_breakdown: { cpu: number; memory: number; disk: number }
  priority_multiplier: number
  unbounded_multiplier: number
  effective_rate_ucu_per_second: number
  charge_window_seconds: number
  charge_window_is_default: boolean
  estimated_ucu: number
  charged_ucu: number
  refund_fraction: number
  actual_ucu: number | null
  true_up: { kind: 'refund' | 'surcharge'; amount_ucu: number } | null
}

function mapCostReport(c: WireCostReport): CostReport {
  return {
    rateUcuPerSecond: c.rate_ucu_per_second,
    rateBreakdown: c.rate_breakdown,
    priorityMultiplier: c.priority_multiplier,
    unboundedMultiplier: c.unbounded_multiplier,
    effectiveRateUcuPerSecond: c.effective_rate_ucu_per_second,
    chargeWindowSeconds: c.charge_window_seconds,
    chargeWindowIsDefault: c.charge_window_is_default,
    estimatedUcu: c.estimated_ucu,
    chargedUcu: c.charged_ucu,
    refundFraction: c.refund_fraction,
    actualUcu: c.actual_ucu,
    trueUp: c.true_up
      ? { kind: snakeToPascal(c.true_up.kind) as TrueUpKind, amountUcu: c.true_up.amount_ucu }
      : null,
  }
}

/** `over_quota_ratio`/`penalty` nullability: see `WirePenaltyLink` above. */
interface WireQuotaEntityView {
  id: QuotaEntityId
  name: string
  parent: QuotaEntityId | null
  quota_ucu: number
  usage_ucu: number
  over_quota_ratio: number | null
  penalty: number | null
}

function mapQuotaEntityView(v: WireQuotaEntityView): QuotaEntityView {
  return {
    id: v.id,
    name: v.name,
    parent: v.parent,
    quotaUcu: v.quota_ucu,
    usageUcu: v.usage_ucu,
    overQuotaRatio: orInfinity(v.over_quota_ratio),
    penalty: orInfinity(v.penalty),
  }
}

interface WireJobDetail {
  id: JobId
  state: WireJobStateKind
  spec: WireJobSpecView
  submitted_at: string
  state_since: string
  terminal_at: string | null
  retries_used: number
  abort_requested: { reason: string | null; requested_at: string } | null
  entity_chain: WireQuotaEntityView[]
  attempts: WireAttemptView[]
  queue: WireQueuePositionExplainer | null
  accrual: WireAccrualView | null
  cost: WireCostReport
}

/**
 * Unlike `JobSummary`, the job-detail DTO does not carry a separate
 * "current attempt" id alongside its flattened `state` (dto.rs's `JobDetail`
 * projects `record.state.into()`, which drops the `Attempting(attempt)`
 * payload). At most one of a job's attempts is ever non-terminal at a time,
 * so the current attempt — when `state` is `attempting` — is recovered as
 * the one non-`Terminal` attempt, falling back to the last attempt in
 * submission order if that invariant ever doesn't hold.
 */
function resolveCurrentAttempt(attempts: WireAttemptView[]): AttemptId | null {
  const live = attempts.find((a) => a.state !== 'terminal')
  if (live) return live.id
  return attempts.length > 0 ? attempts[attempts.length - 1]!.id : null
}

function mapJobDetail(j: WireJobDetail): JobDetail {
  const attempt = j.state === 'attempting' ? resolveCurrentAttempt(j.attempts) : null
  return {
    id: j.id,
    state: mapJobState(j.state, attempt),
    spec: mapJobSpec(j.spec),
    submittedAt: toDate(j.submitted_at),
    stateSince: toDate(j.state_since),
    terminalAt: toDateOrNull(j.terminal_at),
    retriesUsed: j.retries_used,
    abortRequested: j.abort_requested
      ? { reason: j.abort_requested.reason, requestedAt: toDate(j.abort_requested.requested_at) }
      : null,
    entityChain: j.entity_chain.map(mapQuotaEntityView),
    attempts: j.attempts.map(mapAttemptView),
    queue: j.queue ? mapQueuePositionExplainer(j.queue) : null,
    accrual: j.accrual ? mapAccrualView(j.accrual) : null,
    cost: mapCostReport(j.cost),
  }
}

// ---------------------------------------------------------------------------
// Job usage
// ---------------------------------------------------------------------------

type WireUsageAvailability = 'available' | 'expired' | 'unreachable' | 'not_started'

interface WireUsagePoint {
  attempt: AttemptId
  at: string
  cpu_usage_total_us: number
  cpu_throttled_total_us: number
  memory_used_bytes: number
  memory_peak_bytes: number
  disk_writable_bytes: number
  disk_image_bytes: number
  net_rx_bytes_total: number
  net_tx_bytes_total: number
  blkio_read_bytes_total: number
  blkio_write_bytes_total: number
}

interface WireUsageSourceRecord {
  attempt: AttemptId
  node: NodeId | null
  availability: WireUsageAvailability
  truncated: boolean
  earliest_available_at: string | null
  reason: string | null
}

interface WireGetJobUsageResponse {
  samples: WireUsagePoint[]
  sources: WireUsageSourceRecord[]
  next_cursor: string | null
}

function getJobUsage(id: JobId, attempt: AttemptId | null): Promise<GetJobUsageResponse> {
  const params = new URLSearchParams()
  if (attempt) params.set('attempt', attempt)
  const qs = params.toString()
  return getJson(
    `/jobs/${encodeURIComponent(id)}/usage${qs ? `?${qs}` : ''}`,
    (body: WireGetJobUsageResponse): GetJobUsageResponse => ({
      samples: body.samples.map((s) => ({
        attempt: s.attempt,
        at: toDate(s.at),
        cpuUsageTotalUs: s.cpu_usage_total_us,
        cpuThrottledTotalUs: s.cpu_throttled_total_us,
        memoryUsedBytes: s.memory_used_bytes,
        memoryPeakBytes: s.memory_peak_bytes,
        diskWritableBytes: s.disk_writable_bytes,
        diskImageBytes: s.disk_image_bytes,
        netRxBytesTotal: s.net_rx_bytes_total,
        netTxBytesTotal: s.net_tx_bytes_total,
        blkioReadBytesTotal: s.blkio_read_bytes_total,
        blkioWriteBytesTotal: s.blkio_write_bytes_total,
      })),
      sources: body.sources.map((s) => ({
        attempt: s.attempt,
        node: s.node,
        availability: s.availability,
        truncated: s.truncated,
        earliestAvailableAt: toDateOrNull(s.earliest_available_at),
        reason: s.reason,
      })),
      nextCursor: body.next_cursor,
    }),
  )
}

// ---------------------------------------------------------------------------
// Job logs
// ---------------------------------------------------------------------------

type WireLogStreamName = 'stdout' | 'stderr'
type WireLogAvailability = 'available' | 'expired' | 'unreachable' | 'not_started'

interface WireLogEntry {
  attempt: AttemptId
  at: string
  stream: WireLogStreamName
  text: string
  truncated: boolean
}

interface WireLogSourceRecord {
  attempt: AttemptId
  node: NodeId | null
  availability: WireLogAvailability
  truncated: boolean
  earliest_available_at: string | null
  reason: string | null
}

interface WireGetJobLogsResponse {
  entries: WireLogEntry[]
  sources: WireLogSourceRecord[]
  next_cursor: string | null
}

/**
 * `LogChunk`/`LogEntry` in `types.ts` predate the real log DTO (ADR 0034)
 * and use a generic `{ t, level, target, message }` shape that has no
 * `attempt`/`stream`/per-attempt `sources` on it. Rather than widen that
 * type here (out of scope for this pass — see the "Logs are invented" note
 * in CLAUDE.md), each wire entry is mapped losslessly enough to render: the
 * stream becomes a level (`stderr` reads as `error`, `stdout` as `info`)
 * and the attempt id becomes the `target`. A follow-up that threads
 * `attempt`/`stream`/`sources` through `LogChunk` should replace this.
 */
function mapLogEntry(e: WireLogEntry): LogEntry {
  return {
    t: toDate(e.at),
    level: e.stream === 'stderr' ? 'error' : 'info',
    target: e.attempt,
    message: e.text,
  }
}

function getJobLogs(id: JobId, cursor: string | null): Promise<LogChunk> {
  const params = new URLSearchParams()
  if (cursor) params.set('cursor', cursor)
  const qs = params.toString()
  return getJson(
    `/jobs/${encodeURIComponent(id)}/logs${qs ? `?${qs}` : ''}`,
    (body: WireGetJobLogsResponse): LogChunk => ({
      entries: body.entries.map(mapLogEntry),
      nextCursor: body.next_cursor,
    }),
  )
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

type WireNodeHealth = 'unknown' | 'healthy' | 'lost'

interface WireNodeSummary {
  id: NodeId
  capacity: WireResources
  allocated: WireResources
  used: WireResources
  labels: Record<string, string>
  schedulable: boolean
  health: WireNodeHealth
  epoch: number
  last_heartbeat: string | null
  running_count: number
  accruing_count: number
}

function mapNodeSummary(n: WireNodeSummary): NodeSummary {
  return {
    id: n.id,
    capacity: mapResources(n.capacity),
    allocated: mapResources(n.allocated),
    used: mapResources(n.used),
    labels: n.labels,
    schedulable: n.schedulable,
    health: snakeToPascal(n.health) as NodeHealth,
    epoch: n.epoch,
    lastHeartbeat: toDateOrNull(n.last_heartbeat),
    runningCount: n.running_count,
    accruingCount: n.accruing_count,
  }
}

interface WireListNodesResponse {
  nodes: WireNodeSummary[]
}

interface WireGetNodeResponse {
  summary: WireNodeSummary
  active_attempts: WireAttemptView[]
  accrual_queue: WireAccrualView[]
}

function mapNodeDetail(n: WireGetNodeResponse): NodeDetail {
  return {
    summary: mapNodeSummary(n.summary),
    activeAttempts: n.active_attempts.map(mapAttemptView),
    accrualQueue: n.accrual_queue.map(mapAccrualView),
  }
}

// ---------------------------------------------------------------------------
// Quota entities
// ---------------------------------------------------------------------------

/** `over_quota_ratio`/`penalty` nullability: see `WirePenaltyLink` above. */
interface WireQuotaEntityNode {
  id: QuotaEntityId
  name: string
  parent: QuotaEntityId | null
  quota_ucu: number
  usage_ucu: number
  over_quota_ratio: number | null
  penalty: number | null
  created_at: string
  updated_at: string
  queued_count: number
  running_count: number
}

/**
 * `origin`/`principal` (ADR 0022 SSO provenance) have no source: replicated
 * state records no SSO origin, so the server omits both fields entirely
 * (dto.rs's `QuotaEntityNode` doc comment). `'configured'`/`null` is the
 * honest default until an identity subsystem backs them.
 */
function mapQuotaEntityNode(n: WireQuotaEntityNode): QuotaEntityNode {
  return {
    id: n.id,
    name: n.name,
    parent: n.parent,
    origin: 'configured',
    principal: null,
    quotaUcu: n.quota_ucu,
    usageUcu: n.usage_ucu,
    overQuotaRatio: orInfinity(n.over_quota_ratio),
    penalty: orInfinity(n.penalty),
    createdAt: toDate(n.created_at),
    updatedAt: toDate(n.updated_at),
    queuedCount: n.queued_count,
    runningCount: n.running_count,
  }
}

interface WireListQuotaEntitiesResponse {
  entities: WireQuotaEntityNode[]
}

interface WireQuotaEntityStats {
  by_state: Record<WireJobPhase, number>
  oldest_queued_age_seconds: number | null
  burn_rate_ucu_per_second: number
  charged_ucu_24h: number | null
  usage_history: { t: string; usage_ucu: number }[]
}

function mapQuotaEntityStats(s: WireQuotaEntityStats): QuotaEntityStats {
  return {
    byState: mapPhaseCounts(s.by_state),
    oldestQueuedAgeSeconds: s.oldest_queued_age_seconds,
    burnRateUcuPerSecond: s.burn_rate_ucu_per_second,
    // No charge ledger exists to measure a trailing-24h total yet (dto.rs
    // documents this as always null); 0 is the honest "nothing recorded".
    chargedUcu24h: s.charged_ucu_24h ?? 0,
    usageHistory: s.usage_history.map((h) => ({ t: toDate(h.t), usageUcu: h.usage_ucu })),
  }
}

interface WireGetQuotaEntityResponse {
  entity: WireQuotaEntityNode
  chain: WireQuotaEntityView[]
  children: WireQuotaEntityNode[]
  stats: WireQuotaEntityStats
}

function mapQuotaEntityDetail(d: WireGetQuotaEntityResponse): QuotaEntityDetail {
  return {
    entity: mapQuotaEntityNode(d.entity),
    chain: d.chain.map(mapQuotaEntityView),
    children: d.children.map(mapQuotaEntityNode),
    stats: mapQuotaEntityStats(d.stats),
  }
}

interface WireConfigureQuotaEntityResponse {
  entity: QuotaEntityId
  log_index: number
}

/**
 * The write returns only the echoed id + `log_index` (dto.rs's documented
 * deviation from `types.ts`'s sketch of a full node echo — a committed
 * decision carries no fresh projected view). `CoppiceApi.configureQuotaEntity`
 * still promises a `QuotaEntityNode`, so this follows the write with a
 * strong, read-your-writes `GET` pinned to the write's `log_index`
 * (ADR 0007), exactly as the dto.rs comment prescribes.
 */
async function configureQuotaEntity(input: ConfigureQuotaEntityInput): Promise<QuotaEntityNode> {
  const entity = input.entity ?? mintQuotaEntityId()
  const wireBody = {
    entity,
    parent: input.parent,
    name: input.name,
    quota_ucu: input.quotaUcu,
  }
  const { logIndex } = await postJson(
    '/quota-entities',
    wireBody,
    (body: WireConfigureQuotaEntityResponse) => ({ entity: body.entity, logIndex: body.log_index }),
  )
  return getJson(
    `/quota-entities/${encodeURIComponent(entity)}?min_index=${logIndex}&consistency=strong`,
    (body: WireGetQuotaEntityResponse) => mapQuotaEntityNode(body.entity),
  )
}

/** Client-mints a `quota-<uuid>` id (ADR 0024) for a create. Any valid UUID
 * parses server-side (`coppice_core::id` does not enforce a version). */
function mintQuotaEntityId(): QuotaEntityId {
  return `quota-${crypto.randomUUID()}`
}

// ---------------------------------------------------------------------------
// Coordinators
// ---------------------------------------------------------------------------

interface WireCoordinatorSnapshot {
  size_bytes: number | null
  last_included_index: number
  taken_at: string | null
  entries_since_snapshot: number
}

interface WireCoordinatorStateCounts {
  jobs: number
  attempts: number
  allocations: number
  nodes: number
  quota_entities: number
}

type WireCoordinatorRole = 'leader' | 'follower' | 'learner'

interface WireCoordinatorMember {
  id: number
  addr: string
  role: WireCoordinatorRole
  voter: boolean
  last_applied: number | null
  replication_lag_entries: number | null
}

/**
 * `host` (per-member cpu/memory/disk fractions) and `lastSeen` have no wire
 * source — no inter-coordinator host/liveness reporting exists yet (dto.rs's
 * `CoordinatorMember` doc comment: "the web mock invents both"). Zeros and
 * "now" are the least-misleading defaults: a real 0% bar reads as "unknown",
 * and stamping `lastSeen` as the read time avoids a fabricated staleness
 * warning. `lastApplied`/`replicationLagEntries` are `null` on every member
 * but the serving replica itself; 0 is the same "not known" default.
 */
function mapCoordinatorMember(m: WireCoordinatorMember, now: Date): CoordinatorMember {
  return {
    id: m.id,
    addr: m.addr,
    role: snakeToPascal(m.role) as CoordinatorRole,
    voter: m.voter,
    lastApplied: m.last_applied ?? 0,
    replicationLagEntries: m.replication_lag_entries ?? 0,
    host: { cpuFraction: 0, memoryFraction: 0, diskFraction: 0 },
    lastSeen: now,
  }
}

interface WireGetCoordinatorStatusResponse {
  cluster_id: string
  leader: number | null
  term: number
  known_committed: number
  last_applied: number
  state_version: number
  snapshot: WireCoordinatorSnapshot | null
  state_counts: WireCoordinatorStateCounts
  members: WireCoordinatorMember[]
}

function mapCoordinatorStatus(c: WireGetCoordinatorStatusResponse): CoordinatorStatus {
  const now = new Date()
  return {
    clusterId: c.cluster_id,
    leader: c.leader,
    term: c.term,
    knownCommitted: c.known_committed,
    lastApplied: c.last_applied,
    stateVersion: c.state_version,
    snapshot: c.snapshot
      ? {
          sizeBytes: c.snapshot.size_bytes ?? 0,
          lastIncludedIndex: c.snapshot.last_included_index,
          takenAt: toDateOrNull(c.snapshot.taken_at) ?? new Date(0),
          entriesSinceSnapshot: c.snapshot.entries_since_snapshot,
        }
      : // No snapshot taken yet on this replica — zeroed, not fabricated.
        {
          sizeBytes: 0,
          lastIncludedIndex: 0,
          takenAt: new Date(0),
          entriesSinceSnapshot: c.last_applied,
        },
    stateCounts: {
      jobs: c.state_counts.jobs,
      attempts: c.state_counts.attempts,
      allocations: c.state_counts.allocations,
      nodes: c.state_counts.nodes,
      quotaEntities: c.state_counts.quota_entities,
    },
    members: c.members.map((m) => mapCoordinatorMember(m, now)),
  }
}
