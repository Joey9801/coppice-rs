/**
 * MockWorld — one deterministic, seeded simulation of a Coppice cluster.
 *
 * It holds rich internal records (jobs, attempts, allocations, nodes, a quota
 * tree, coordinators) and projects them into the read-only view types in
 * `../types.ts`. The client (`mock-client.ts`) owns a singleton and advances
 * it lazily via `advanceTo(nowUs)`; nothing here reads the wall clock, so
 * construction is fully reproducible from `{ seed, nowUs }`.
 *
 * Coherence invariants (asserted by world.test.ts) are maintained at
 * construction and preserved by every `tick`:
 *  - every attempt/allocation references an existing job and node
 *  - funded ≤ requested per dimension
 *  - Σ funded over non-Released allocations on a node ≤ node capacity
 *  - a Running job has one current attempt Running + an Active allocation
 *  - Queued jobs hold no allocation; terminal jobs have an outcome + time
 *  - queue ranks are 1..depth, consistent with descending score
 */

import type {
  AccrualView,
  AllocationState,
  AllocationView,
  AttemptOutcome,
  AttemptOutcomeKind,
  AttemptState,
  AttemptView,
  ClusterOverview,
  CoordinatorMember,
  CoordinatorStatus,
  CostReport,
  JobDetail,
  JobList,
  JobPhase,
  JobState,
  JobStateKind,
  JobSummary,
  JobUsage,
  ListJobsFilter,
  LogChunk,
  LogEntry,
  LogLevel,
  NodeDetail,
  NodeHealth,
  NodeHistoryEntry,
  NodeSummary,
  NodeUtilization,
  OutcomeClass,
  ConfigureQuotaEntityInput,
  QueuePositionExplainer,
  QueueStats,
  QuotaEntityDetail,
  QuotaEntityNode,
  QuotaEntityOrigin,
  QuotaEntityStats,
  QuotaEntityView,
  RecentEventsWindow,
  Resources,
  TimelineEvent,
  TimelineEventBody,
  UtilizationSample,
  UsageSample,
} from '../types'
import { derivePhase, isTerminalJobState, jobAttemptId, JOB_PHASES } from '../types'
import {
  GIB,
  hashSeed,
  mintCommand,
  mintEnv,
  mintImage,
  ORG_NAME,
  Rng,
  TEAMS,
  TIB,
} from './generate'

// ---------------------------------------------------------------------------
// Constants: time, policy, cost weights
// ---------------------------------------------------------------------------

const SECOND_US = 1_000_000
const MINUTE_US = 60 * SECOND_US
const HOUR_US = 60 * MINUTE_US

const TICK_US = SECOND_US
/** Cap on ticks processed in one advanceTo, so a long-idle tab can't hang. */
const MAX_TICKS_PER_ADVANCE = 4000

// The simulation clock is microseconds throughout — it is a pinned pseudo-clock
// (`advanceTo(nowUs)`), never a wall clock, and integer µs keeps every tick
// reproducible. The *view* types are `Date`/seconds (see ../types.ts), so these
// two convert at the projection boundary and nowhere else.

/** Simulation µs → the `Date` a view type carries. */
const at = (us: number): Date => new Date(us / 1000)

/** A simulation µs span → the whole seconds a view type carries. */
const secondsOf = (us: number): number => Math.trunc(us / SECOND_US)

/** Priority class → scheduling multiplier m(j) (ADR 0021). */
const PRIORITY_MULTIPLIER: Record<number, number> = { 0: 1, 1: 2, 2: 4 }
const W_AGE = 0.5
const AGE_HORIZON_US = 24 * HOUR_US
/**
 * Charge policy (ADR 0029), mirroring `coppice_state::Policy` defaults:
 * - the upfront charge covers `max_runtime`, or this default window when the
 *   job declared no bound (`default_charge_runtime_s: 86_400` = 24h);
 * - unbounded jobs price at an elevated multiplier to encourage real bounds;
 * - a true-up refunds this fraction of the unused charge for bounded jobs
 *   (unbounded / platform outcomes refund in full).
 */
const DEFAULT_CHARGE_RUNTIME_US = 24 * HOUR_US
const UNBOUNDED_RUNTIME_MULTIPLIER = 2
const REFUND_FRACTION = 0.75

/**
 * Cost weights, configured the way an operator reasons about them: as
 * reciprocals — "how much of a resource one CU buys per hour" — anchored on
 * CPU and scaled off a typical 8 GiB/core node shape (so a node's cores and
 * its memory cost the same). Chosen so a 4-core / 16 GiB job still costs ~3
 * CU/hour, and so each dimension reciprocates to a clean whole number the cost
 * breakdown can surface (2 core-hours, 16 GiB-hours, 128 TiB-hours per CU).
 * rate = cpuMillis*W_CPU + (mem/GiB)*W_MEM + (disk/TiB)*W_DISK  (µCU/second).
 */
const cuPerHourToUcuPerSecond = (cuPerHour: number): number => (cuPerHour * 1_000_000) / 3600
/** 2 core-hours per CU → 0.5 CU/hour/core, per millicore-second. */
const W_CPU = cuPerHourToUcuPerSecond(1 / 2) / 1000
/** 16 GiB-hours per CU, per GiB-second. */
const W_MEM = cuPerHourToUcuPerSecond(1 / 16)
/** 128 TiB-hours per CU (disk priced far cheaper), per TiB-second. */
const W_DISK = cuPerHourToUcuPerSecond(1 / 128)

// Kept low so admission is visibly gated: with a big accrual pool the queue
// would drain into Preparing the moment jobs arrive (mirrors policy
// `accrual_limit`, ADR 0014).
const ACCRUAL_LIMIT_PER_NODE = 2

/** Per-node utilization history ring: 30s buckets covering ~1h. */
const UTIL_BUCKET_US = 30 * SECOND_US
const UTIL_BUCKETS = 120
const USAGE_RING = 120
const QUEUE_BUCKET_US = 30 * SECOND_US
const QUEUE_HISTORY_BUCKETS = 60
const EVENTS_RING = 200

// ---------------------------------------------------------------------------
// Quota tree: decay, history, users
// ---------------------------------------------------------------------------

/** Usage decays exponentially with a 24h half-life (policy default). */
const QUOTA_HALF_LIFE_US = 24 * HOUR_US
/** Per-tick decay multiplier (applied once per 1s tick to every entity). */
const QUOTA_DECAY_PER_TICK = 0.5 ** (TICK_US / QUOTA_HALF_LIFE_US)
/** Usage-history ring shares the node util cadence: 30s buckets over ~1h. */
const QUOTA_BUCKET_US = UTIL_BUCKET_US
const QUOTA_HISTORY_BUCKETS = UTIL_BUCKETS
/** Trailing window for `chargedUcu24h` and the charge log. */
const CHARGE_WINDOW_US = 24 * HOUR_US
/** Ancestor-walk / tree-depth cap, matching `charge_ancestors` in the Rust. */
const MAX_QUOTA_DEPTH = 32
/** 1 CU = 1e6 µCU (see buildQuotaTree quota seeds). */
const CU_UCU = 1_000_000

/** Reserved root the coordinator auto-populates from OIDC principals. */
const USERS_ROOT_NAME = 'users'
const USERS_ROOT_QUOTA_UCU = 2_000 * CU_UCU
const USER_DEFAULT_QUOTA_UCU = 50 * CU_UCU
/** Hard cap on auto-minted identities (seeded + live). */
const MAX_USERS = 15
/** Deterministic pool of OIDC `sub` claims for the auto-populated user tree. */
const USER_SUBS = [
  'alice.chen@acme.dev',
  'bo.svensson@acme.dev',
  'carlos.mendez@acme.dev',
  'dmitry.volkov@acme.dev',
  'emma.johansson@acme.dev',
  'fatima.al-rashid@acme.dev',
  'grace.kim@acme.dev',
  'hiro.tanaka@acme.dev',
  'ingrid.larsen@acme.dev',
  'julia.rossi@acme.dev',
  'kwame.osei@acme.dev',
  'liang.wei@acme.dev',
  'maria.silva@acme.dev',
  'noah.goldberg@acme.dev',
  'olga.petrova@acme.dev',
] as const

// ---------------------------------------------------------------------------
// Resource helpers
// ---------------------------------------------------------------------------

function res(cpuMillis: number, memoryBytes: number, diskBytes: number): Resources {
  return { cpuMillis, memoryBytes, diskBytes }
}

function zeroRes(): Resources {
  return res(0, 0, 0)
}

function addRes(a: Resources, b: Resources): Resources {
  return res(a.cpuMillis + b.cpuMillis, a.memoryBytes + b.memoryBytes, a.diskBytes + b.diskBytes)
}

function subRes(a: Resources, b: Resources): Resources {
  return res(a.cpuMillis - b.cpuMillis, a.memoryBytes - b.memoryBytes, a.diskBytes - b.diskBytes)
}

function scaleRes(a: Resources, f: number): Resources {
  return res(
    Math.round(a.cpuMillis * f),
    Math.round(a.memoryBytes * f),
    Math.round(a.diskBytes * f),
  )
}

function fitsRes(need: Resources, free: Resources): boolean {
  return (
    need.cpuMillis <= free.cpuMillis &&
    need.memoryBytes <= free.memoryBytes &&
    need.diskBytes <= free.diskBytes
  )
}

function minFraction(part: Resources, whole: Resources): number {
  const f = (a: number, b: number) => (b > 0 ? a / b : 1)
  return Math.min(
    f(part.cpuMillis, whole.cpuMillis),
    f(part.memoryBytes, whole.memoryBytes),
    f(part.diskBytes, whole.diskBytes),
  )
}

/** Per-dimension cost rate (µCU/second), the weighted terms of `computeRate`. */
function rateTerms(r: Resources): { cpu: number; memory: number; disk: number } {
  return {
    cpu: r.cpuMillis * W_CPU,
    memory: (r.memoryBytes / GIB) * W_MEM,
    disk: (r.diskBytes / TIB) * W_DISK,
  }
}

function computeRate(r: Resources): number {
  const t = rateTerms(r)
  return Math.round(t.cpu + t.memory + t.disk)
}

// ---------------------------------------------------------------------------
// Internal records (richer than the view types; never handed out directly)
// ---------------------------------------------------------------------------

interface QEntity {
  id: string
  name: string
  parent: string | null
  quotaUcu: number
  /** Decayed usage as of `nowUs`; a float internally (rounded at the edge). */
  usageUcu: number
  depth: number
  origin: QuotaEntityOrigin
  /** OIDC `sub` for auto-minted `sso` entities; null otherwise. */
  principal: string | null
  createdAtUs: number
  updatedAtUs: number
  /**
   * Stored decayed-usage ring (oldest first), appended every QUOTA_BUCKET_US
   * and seeded at construction — a stable recording, never regenerated.
   */
  usageHistory: Array<{ tUs: number; usageUcu: number }>
  /** Charge log (full cost charged to this entity), pruned to trailing 24h. */
  charges: Array<{ tUs: number; amountUcu: number }>
}

interface MNode {
  id: string
  capacity: Resources
  labels: Record<string, string>
  schedulable: boolean
  health: NodeHealth
  epoch: number
  lastHeartbeatUs: number | null
  /**
   * Stored utilization ring (oldest first), appended every UTIL_BUCKET_US
   * tick and seeded at construction — history must be a stable recording,
   * never regenerated per request.
   */
  utilHistory: UtilizationSample[]
}

interface MAlloc {
  id: string
  job: string
  attempt: string
  node: string
  requested: Resources
  funded: Resources
  state: AllocationState
  seq: number
}

interface MAttempt {
  id: string
  job: string
  node: string
  allocation: string
  state: AttemptState
  outcome: AttemptOutcome | null
  startedAtUs: number | null
  endedAtUs: number | null
  rateUcuPerSecond: number
  chargedUcu: number
  /** Usage samples recorded while this attempt ran (bounded ring). */
  usage: UsageSample[]
}

interface JobSpecInternal {
  image: string
  entrypoint: string[] | null
  command: string[]
  env: Record<string, string>
  requests: Resources
  priority: number
  maxRuntimeUs: number | null
  quotaEntity: string
  retry: { maxRetries: number; retryUserErrors: boolean }
}

interface MJob {
  id: string
  spec: JobSpecInternal
  /** The current attempt id, when there is one, lives inside `Attempting` (ADR 0030). */
  state: JobState
  submittedAtUs: number
  terminalAtUs: number | null
  retriesUsed: number
  abortRequested: { reason: string | null; requestedAtUs: number } | null
  attempts: string[]
  actualUcu: number | null
  trueUp: { kind: 'Refund' | 'Surcharge'; amountUcu: number } | null
  projectedStartUs: number | null
  /** Simulation clock at last state promotion (drives Submitted→Queued flow). */
  lastTransitionUs: number
}

interface MCoordinator {
  id: number
  addr: string
  role: 'Leader' | 'Follower' | 'Learner'
  voter: boolean
  lagEntries: number
  host: { cpuFraction: number; memoryFraction: number; diskFraction: number }
  lastSeenUs: number
}

interface QueueBucket {
  tUs: number
  depth: number
  drainedPerMinute: number
  arrivedPerMinute: number
}

// ---------------------------------------------------------------------------
// Job shape pool
// ---------------------------------------------------------------------------

interface Shape {
  cpuMillis: number
  memGiB: number
  diskGiB: number
}

// Weighted mean ≈ 7 cores/job: sized against the ~520-core fleet so the
// steady-state running set (~75–85 jobs, see tickArrivals/tickRunningJobs
// rates) saturates CPU and queueing/accrual behavior stays observable.
const SHAPES: ReadonlyArray<readonly [Shape, number]> = [
  [{ cpuMillis: 1000, memGiB: 4, diskGiB: 20 }, 3],
  [{ cpuMillis: 2000, memGiB: 8, diskGiB: 40 }, 3],
  [{ cpuMillis: 4000, memGiB: 16, diskGiB: 100 }, 4],
  [{ cpuMillis: 8000, memGiB: 32, diskGiB: 200 }, 3],
  [{ cpuMillis: 16000, memGiB: 64, diskGiB: 400 }, 2],
  [{ cpuMillis: 4000, memGiB: 64, diskGiB: 100 }, 1],
  [{ cpuMillis: 32000, memGiB: 128, diskGiB: 800 }, 1],
]

function shapeToResources(s: Shape): Resources {
  return res(s.cpuMillis, s.memGiB * GIB, s.diskGiB * GIB)
}

/** Failure outcomes for Failed jobs: [kind, class, weight]. */
const FAILURE_POOL: ReadonlyArray<readonly [AttemptOutcomeKind, OutcomeClass, number]> = [
  ['OomKilled', 'UserError', 3],
  ['Exited', 'UserError', 3],
  ['MaxRuntimeExceeded', 'UserError', 2],
  ['PullFailed', 'Platform', 1],
  ['StartFailed', 'Platform', 1],
  ['NodeLost', 'Platform', 1],
]

/** Platform-class transient outcomes used for earlier (retried) attempts. */
const PLATFORM_POOL: ReadonlyArray<readonly [AttemptOutcomeKind, number]> = [
  ['NodeLost', 2],
  ['PullFailed', 2],
  ['StartFailed', 1],
  ['AgentError', 1],
]

// ---------------------------------------------------------------------------
// MockWorld
// ---------------------------------------------------------------------------

const DEFAULT_SEED = 0x00c0ff1c

export class MockWorld {
  private rng: Rng
  private nowUs: number
  private lastTickUs: number

  private entities = new Map<string, QEntity>()
  /** Childless entities — the leaf set (maintained across create/reparent). */
  private leafIds: string[] = []
  /**
   * The pool jobs are actually submitted under, weighted so the org tree
   * keeps most traffic. Seeded at construction and extended by auto-minted
   * SSO identities; admin-configured entities do NOT auto-receive traffic.
   */
  private jobLeaves: Array<readonly [string, number]> = []
  private usersRootId = ''
  private usedSubs = new Set<string>()
  private lastQuotaBucketUs = 0
  private nodes = new Map<string, MNode>()
  private jobs = new Map<string, MJob>()
  private attempts = new Map<string, MAttempt>()
  private allocs = new Map<string, MAlloc>()
  private coordinators: MCoordinator[] = []

  private events: TimelineEvent[] = []
  private jobEvents = new Map<string, TimelineEvent[]>()
  /**
   * Stand-in for the Raft log index that stamps every real event's
   * identity (ADR 0032). The mock emits one event per "command", so
   * ordinals are always 0.
   */
  private nextEventIndex = 1
  private queueHistory: QueueBucket[] = []

  private seqCounter = 0
  private lastBucketUs = 0
  private lastUtilBucketUs = 0
  private arrivalsThisBucket = 0
  private drainsThisBucket = 0

  private clusterId = 'coppice-prod-1'
  private raftIndex = 0
  private stateVersion = 0
  private snapshotIndex = 0
  private snapshotAtUs = 0

  constructor(nowUs: number, seed: number = DEFAULT_SEED) {
    this.rng = new Rng(seed)
    this.nowUs = nowUs
    this.lastTickUs = nowUs
    this.raftIndex = 42000
    this.stateVersion = 61000
    this.build()
  }

  // ---- construction --------------------------------------------------------

  private build(): void {
    this.buildQuotaTree()
    this.buildNodes()
    this.buildCoordinators()
    this.buildJobs()
    this.seedQueueHistory()
    this.seedUtilHistories()
    this.seedQuotaHistories()
    this.recomputeQueueRanks()
  }

  /**
   * Backfill each node's utilization ring so charts are full on first load.
   * Walk backward from the node's REAL current allocation, occasionally
   * stepping by a job-sized chunk (placements/releases), with `used`
   * wobbling smoothly beneath `allocated` — so the allocated line is
   * piecewise-constant over time instead of flat, and the most recent
   * sample agrees with live state.
   */
  private seedUtilHistories(): void {
    for (const node of this.nodes.values()) {
      const rng = new Rng(hashSeed(node.id + 'util-seed'))
      let alloc = this.nodeAllocated(node.id)
      let factor = rng.range(0.55, 0.8)
      const backward: UtilizationSample[] = []
      for (let i = 0; i < UTIL_BUCKETS; i += 1) {
        const tUs = this.nowUs - i * UTIL_BUCKET_US
        factor = Math.min(0.9, Math.max(0.4, factor + rng.range(-0.04, 0.04)))
        const used = res(
          Math.round(alloc.cpuMillis * factor),
          Math.round(alloc.memoryBytes * Math.min(1, factor + 0.1)),
          Math.round(alloc.diskBytes * Math.min(1, factor + 0.15)),
        )
        backward.push({ t: at(tUs), used, allocated: { ...alloc } })
        if (rng.bool(0.12)) {
          // A placement or release happened at this point (walking backward).
          const sign = rng.bool() ? 1 : -1
          const chunk = res(
            rng.int(1, 8) * 1000,
            rng.int(4, 32) * GIB,
            Math.round(rng.range(0.05, 0.5) * TIB),
          )
          const step = (a: number, c: number, cap: number) =>
            Math.min(cap, Math.max(0, a + sign * c))
          alloc = res(
            step(alloc.cpuMillis, chunk.cpuMillis, node.capacity.cpuMillis),
            step(alloc.memoryBytes, chunk.memoryBytes, node.capacity.memoryBytes),
            step(alloc.diskBytes, chunk.diskBytes, node.capacity.diskBytes),
          )
        }
      }
      node.utilHistory = backward.reverse()
    }
    this.lastUtilBucketUs = this.nowUs
  }

  /**
   * Backfill each entity's decayed-usage ring and a few synthetic charge-log
   * entries so the trailing-24h charge total isn't zero at boot. The ring
   * wobbles around the current usage (reverse-decay drift) so sparklines are
   * full and the newest sample agrees with live `usageUcu`.
   */
  private seedQuotaHistories(): void {
    // Reverse of one bucket's decay: older samples were marginally higher.
    const reverseStep = 2 ** (QUOTA_BUCKET_US / QUOTA_HALF_LIFE_US)
    for (const ent of this.entities.values()) {
      const rng = new Rng(hashSeed(ent.id + 'quota-seed'))
      const backward: Array<{ tUs: number; usageUcu: number }> = []
      let val = ent.usageUcu
      for (let i = 0; i < QUOTA_HISTORY_BUCKETS; i += 1) {
        const tUs = this.nowUs - i * QUOTA_BUCKET_US
        backward.push({ tUs, usageUcu: Math.max(0, Math.round(val)) })
        val = val * reverseStep * (1 + rng.range(-0.03, 0.03))
      }
      ent.usageHistory = backward.reverse()

      // A handful of synthetic charges spread across the trailing 24h,
      // summing to roughly a quarter of current usage.
      if (ent.usageUcu > 0) {
        const n = rng.int(3, 6)
        for (let i = 0; i < n; i += 1) {
          const tUs = this.nowUs - Math.round(rng.range(0, CHARGE_WINDOW_US))
          const amountUcu = Math.round((ent.usageUcu / n) * rng.range(0.1, 0.4))
          ent.charges.push({ tUs, amountUcu })
        }
        ent.charges.sort((a, b) => a.tUs - b.tUs)
      }
    }
    this.lastQuotaBucketUs = this.nowUs
  }

  /** Allocate a bare QEntity record with the shared defaults filled in. */
  private newEntity(fields: {
    name: string
    parent: string | null
    quotaUcu: number
    usageUcu: number
    depth: number
    origin: QuotaEntityOrigin
    principal?: string | null
    id?: string
  }): QEntity {
    const ent: QEntity = {
      id: fields.id ?? this.rng.mintId('quota'),
      name: fields.name,
      parent: fields.parent,
      quotaUcu: fields.quotaUcu,
      usageUcu: fields.usageUcu,
      depth: fields.depth,
      origin: fields.origin,
      principal: fields.principal ?? null,
      createdAtUs: this.nowUs,
      updatedAtUs: this.nowUs,
      usageHistory: [],
      charges: [],
    }
    this.entities.set(ent.id, ent)
    return ent
  }

  private buildQuotaTree(): void {
    const root = this.newEntity({
      name: ORG_NAME,
      parent: null,
      quotaUcu: 0,
      usageUcu: 0,
      depth: 0,
      origin: 'configured',
    })

    // Pre-pick which leaves are meaningfully over quota (2–3 of them).
    const leafSpecs: Array<{ division: string; team: string }> = []
    for (const division of Object.keys(TEAMS)) {
      for (const team of TEAMS[division] ?? []) leafSpecs.push({ division, team })
    }
    const overIdx = new Set(
      this.rng.shuffle(leafSpecs.map((_, i) => i)).slice(0, this.rng.int(2, 3)),
    )

    const divisionEntities = new Map<string, QEntity>()
    for (const division of Object.keys(TEAMS)) {
      const ent = this.newEntity({
        name: `${ORG_NAME}/${division}`,
        parent: root.id,
        quotaUcu: 0,
        usageUcu: 0,
        depth: 1,
        origin: 'configured',
      })
      divisionEntities.set(division, ent)
    }

    leafSpecs.forEach((spec, i) => {
      const parent = divisionEntities.get(spec.division)
      if (!parent) return
      const quotaUcu = this.rng.int(300, 1500) * CU_UCU
      const overQuota = overIdx.has(i)
      const ratio = overQuota ? this.rng.range(1.3, 2.4) : this.rng.range(0.2, 0.9)
      const leaf = this.newEntity({
        name: `${ORG_NAME}/${spec.division}/${spec.team}`,
        parent: parent.id,
        quotaUcu,
        usageUcu: Math.round(quotaUcu * ratio),
        depth: 2,
        origin: 'configured',
      })
      this.leafIds.push(leaf.id)
      // Org leaves carry the bulk of steady-state traffic.
      this.jobLeaves.push([leaf.id, 8])
    })

    // Roll usage/quota up: ancestor usage ≈ sum of descendants (ADR 0021 tree).
    for (const division of divisionEntities.values()) {
      const children = [...this.entities.values()].filter((e) => e.parent === division.id)
      division.quotaUcu = children.reduce((s, c) => s + c.quotaUcu, 0)
      division.usageUcu = children.reduce((s, c) => s + c.usageUcu, 0)
    }
    root.quotaUcu = [...divisionEntities.values()].reduce((s, c) => s + c.quotaUcu, 0)
    root.usageUcu = [...divisionEntities.values()].reduce((s, c) => s + c.usageUcu, 0)

    this.buildUsersTree()
  }

  /**
   * The reserved `users` root and its auto-populated SSO identities (ADR
   * 0022): the coordinator mints an entity per OIDC principal the first time
   * it sees one. Two users get admin-configured child sub-queues.
   */
  private buildUsersTree(): void {
    const usersRoot = this.newEntity({
      name: USERS_ROOT_NAME,
      parent: null,
      quotaUcu: USERS_ROOT_QUOTA_UCU,
      usageUcu: 0,
      depth: 0,
      origin: 'sso',
    })
    this.usersRootId = usersRoot.id

    const seeded = 7
    // Two of the seeded users get sub-queues (and thus are not leaves).
    const withSubqueues = new Set(this.rng.shuffle([0, 1, 2, 3, 4, 5, 6]).slice(0, 2))
    const overIdx = new Set(this.rng.shuffle([0, 1, 2, 3, 4, 5, 6]).slice(0, 2))

    for (let i = 0; i < seeded; i += 1) {
      const sub = USER_SUBS[i]!
      const quotaUcu = this.rng.int(30, 80) * CU_UCU
      const ratio = overIdx.has(i) ? this.rng.range(1.1, 1.8) : this.rng.range(0.1, 0.7)
      const user = this.mintUserEntity(sub, quotaUcu, Math.round(quotaUcu * ratio))

      if (withSubqueues.has(i)) {
        for (const leaf of ['batch', 'experiments']) {
          const subQuota = this.rng.int(10, 25) * CU_UCU
          const subRatio = this.rng.range(0.2, 1.1)
          const sq = this.newEntity({
            name: `${user.name}/${leaf}`,
            parent: user.id,
            quotaUcu: subQuota,
            usageUcu: Math.round(subQuota * subRatio),
            depth: 2,
            origin: 'configured',
          })
          this.leafIds.push(sq.id)
          this.jobLeaves.push([sq.id, 1])
        }
      } else {
        // A childless user is itself a leaf and receives modest traffic.
        this.leafIds.push(user.id)
        this.jobLeaves.push([user.id, 2])
      }
    }

    // Roll the seeded user usage up into the users root.
    usersRoot.usageUcu = [...this.entities.values()]
      .filter((e) => e.parent === usersRoot.id)
      .reduce((s, c) => s + c.usageUcu, 0)
  }

  /** Mint one SSO user entity under the users root and record its `sub`. */
  private mintUserEntity(sub: string, quotaUcu: number, usageUcu: number): QEntity {
    this.usedSubs.add(sub)
    return this.newEntity({
      name: `${USERS_ROOT_NAME}/${sub}`,
      parent: this.usersRootId,
      quotaUcu,
      usageUcu,
      depth: 1,
      origin: 'sso',
      principal: sub,
    })
  }

  private buildNodes(): void {
    const zones = ['a', 'b', 'c']
    const pools = ['general', 'highmem', 'compute'] as const
    const coreOptions = [8, 16, 32, 48, 64, 96]
    const count = 16
    for (let i = 0; i < count; i += 1) {
      const cores = this.rng.pick(coreOptions)
      const pool = this.rng.pick(pools)
      const memPerCore = pool === 'highmem' ? 12 : pool === 'compute' ? 2 : 4
      const memGiB = cores * memPerCore
      const diskTiB = this.rng.range(0.5, 8)
      const draining = i === 3 // one node draining (schedulable false, still runs work)
      const lost = i === 7 // one node Lost (stale heartbeat)
      const node: MNode = {
        id: this.rng.mintId('node'),
        capacity: res(cores * 1000, memGiB * GIB, Math.round(diskTiB * TIB)),
        labels: { zone: this.rng.pick(zones), pool },
        schedulable: !draining,
        health: lost ? 'Lost' : 'Healthy',
        epoch: lost ? this.rng.int(4, 9) : this.rng.int(1, 3),
        lastHeartbeatUs: lost
          ? this.nowUs - this.rng.range(18 * MINUTE_US, 22 * MINUTE_US)
          : this.nowUs - this.rng.range(SECOND_US, 15 * SECOND_US),
        utilHistory: [],
      }
      this.nodes.set(node.id, node)
    }
  }

  private buildCoordinators(): void {
    for (let id = 1; id <= 3; id += 1) {
      this.coordinators.push({
        id,
        addr: `coord-${id}.internal:7071`,
        role: id === 1 ? 'Leader' : 'Follower',
        voter: true,
        lagEntries: id === 1 ? 0 : this.rng.int(0, 2),
        host: {
          cpuFraction: this.rng.range(0.1, 0.6),
          memoryFraction: this.rng.range(0.2, 0.7),
          diskFraction: this.rng.range(0.05, 0.4),
        },
        lastSeenUs: this.nowUs - this.rng.range(SECOND_US, 4 * SECOND_US),
      })
    }
    this.snapshotIndex = this.raftIndex - this.rng.int(1200, 2500)
    this.snapshotAtUs = this.nowUs - this.rng.range(50 * MINUTE_US, 70 * MINUTE_US)
  }

  /** Placeable nodes = healthy (draining node still hosts existing work). */
  private placeableNodes(): MNode[] {
    return [...this.nodes.values()].filter((n) => n.health === 'Healthy')
  }

  private nodeFreeCapacity(): Map<string, Resources> {
    const free = new Map<string, Resources>()
    for (const node of this.nodes.values()) free.set(node.id, { ...node.capacity })
    for (const alloc of this.allocs.values()) {
      if (alloc.state === 'Released') continue
      const cur = free.get(alloc.node)
      if (cur) free.set(alloc.node, subRes(cur, alloc.funded))
    }
    return free
  }

  private makeSpec(): JobSpecInternal {
    const shape = this.rng.weighted(SHAPES)
    const priority = this.rng.weighted([
      [0, 6],
      [1, 3],
      [2, 1],
    ] as const)
    const hasMaxRuntime = this.rng.bool(0.6)
    const command = mintCommand(this.rng)
    return {
      image: mintImage(this.rng),
      entrypoint: command.entrypoint,
      command: command.command,
      env: mintEnv(this.rng),
      requests: shapeToResources(shape),
      priority,
      maxRuntimeUs: hasMaxRuntime ? this.rng.int(1, 8) * HOUR_US : null,
      quotaEntity: this.rng.weighted(this.jobLeaves),
      retry: { maxRetries: this.rng.int(0, 3), retryUserErrors: this.rng.bool(0.3) },
    }
  }

  private buildJobs(): void {
    // Order matters: capacity-consuming states first so free capacity is real.
    // The running count starts the cluster near CPU saturation (~65 × ~7
    // cores against the ~520-core fleet) so the seeded queue doesn't
    // instantly drain into the free capacity of a half-idle cluster.
    this.buildRunningJobs(65)
    this.buildFinalizingJobs(4)
    this.buildPreparingJobs(16)
    this.buildQueuedJobs(25)
    this.buildPipelineJobs({ kind: 'Accepted' }, 4)
    this.buildPipelineJobs({ kind: 'Submitted' }, 3)
    this.buildTerminalJobs(150)
  }

  private newJob(state: JobState, submittedAtUs: number): MJob {
    const job: MJob = {
      id: this.rng.mintId('job'),
      spec: this.makeSpec(),
      state,
      submittedAtUs,
      terminalAtUs: null,
      retriesUsed: 0,
      abortRequested: null,
      attempts: [],
      actualUcu: null,
      trueUp: null,
      projectedStartUs: null,
      lastTransitionUs: submittedAtUs,
    }
    this.jobs.set(job.id, job)
    return job
  }

  private newAttempt(job: MJob, node: string, state: AttemptState): MAttempt {
    const rate = computeRate(job.spec.requests)
    const attempt: MAttempt = {
      id: this.rng.mintId('attempt'),
      job: job.id,
      node,
      allocation: '',
      state,
      outcome: null,
      startedAtUs: null,
      endedAtUs: null,
      rateUcuPerSecond: rate,
      chargedUcu: 0,
      usage: [],
    }
    this.attempts.set(attempt.id, attempt)
    job.attempts.push(attempt.id)
    return attempt
  }

  private newAlloc(
    job: MJob,
    attempt: MAttempt,
    node: string,
    funded: Resources,
    state: AllocationState,
  ): MAlloc {
    this.seqCounter += 1
    const alloc: MAlloc = {
      id: this.rng.mintId('alloc'),
      job: job.id,
      attempt: attempt.id,
      node,
      requested: { ...job.spec.requests },
      funded,
      state,
      seq: this.seqCounter,
    }
    this.allocs.set(alloc.id, alloc)
    attempt.allocation = alloc.id
    return alloc
  }

  /** The attempt `job.state` points at, if any (ADR 0030: no separate link field). */
  private currentAttempt(job: MJob): MAttempt | undefined {
    const id = jobAttemptId(job.state)
    return id ? this.attempts.get(id) : undefined
  }

  /** The flat display phase for a job, joining `Attempting` with its attempt's state. */
  private jobPhase(job: MJob): JobPhase {
    return derivePhase(job.state, this.currentAttempt(job)?.state ?? null)
  }

  private buildRunningJobs(target: number): void {
    const free = this.nodeFreeCapacity()
    const placeable = this.placeableNodes()
    let made = 0
    let attempts = 0
    while (made < target && attempts < target * 6) {
      attempts += 1
      const submittedAtUs = this.nowUs - this.rng.range(5 * MINUTE_US, 20 * HOUR_US)
      const job = this.newJob({ kind: 'Queued' }, submittedAtUs)
      const nodeId = this.findFit(job.spec.requests, free, placeable)
      if (!nodeId) {
        this.jobs.delete(job.id)
        job.attempts = []
        continue
      }
      const runningForUs = this.rng.range(2 * MINUTE_US, 6 * HOUR_US)
      const startedAtUs = this.nowUs - runningForUs
      // Optionally give this job a prior retried attempt (transient failure).
      this.maybeAddRetries(job, startedAtUs)
      const attempt = this.newAttempt(job, nodeId, 'Running')
      attempt.startedAtUs = startedAtUs
      attempt.chargedUcu = this.jobChargeModel(job).upfrontUcu
      const alloc = this.newAlloc(job, attempt, nodeId, { ...job.spec.requests }, 'Active')
      job.state = { kind: 'Attempting', attempt: attempt.id }
      const cur = free.get(nodeId)
      if (cur) free.set(nodeId, subRes(cur, alloc.funded))
      this.seedUsage(attempt, job.spec.requests, startedAtUs, this.nowUs)
      made += 1
    }
  }

  private buildFinalizingJobs(target: number): void {
    const free = this.nodeFreeCapacity()
    const placeable = this.placeableNodes()
    let made = 0
    let tries = 0
    while (made < target && tries < target * 6) {
      tries += 1
      const submittedAtUs = this.nowUs - this.rng.range(20 * MINUTE_US, 8 * HOUR_US)
      const job = this.newJob({ kind: 'Queued' }, submittedAtUs)
      const nodeId = this.findFit(job.spec.requests, free, placeable)
      if (!nodeId) {
        this.jobs.delete(job.id)
        continue
      }
      const runningForUs = this.rng.range(10 * MINUTE_US, 4 * HOUR_US)
      const startedAtUs = this.nowUs - runningForUs
      const attempt = this.newAttempt(job, nodeId, 'Finalizing')
      attempt.startedAtUs = startedAtUs
      attempt.chargedUcu = this.jobChargeModel(job).upfrontUcu
      const alloc = this.newAlloc(job, attempt, nodeId, { ...job.spec.requests }, 'Active')
      job.state = { kind: 'Attempting', attempt: attempt.id }
      const cur = free.get(nodeId)
      if (cur) free.set(nodeId, subRes(cur, alloc.funded))
      this.seedUsage(attempt, job.spec.requests, startedAtUs, this.nowUs)
      made += 1
    }
  }

  private buildPreparingJobs(target: number): void {
    const free = this.nodeFreeCapacity()
    const placeable = this.placeableNodes()
    for (let i = 0; i < target; i += 1) {
      const submittedAtUs = this.nowUs - this.rng.range(30 * SECOND_US, 15 * MINUTE_US)
      const job = this.newJob({ kind: 'Queued' }, submittedAtUs)
      // Accrue against the node with the most headroom (partial funding).
      const nodeId = this.pickAccrualNode(placeable, free)
      if (!nodeId) {
        // Fall back to Queued if nothing can host an accrual (already set).
        continue
      }
      const attempt = this.newAttempt(job, nodeId, 'Accruing')
      attempt.startedAtUs = null
      // Charged in full at placement, even while still accruing capacity.
      attempt.chargedUcu = this.jobChargeModel(job).upfrontUcu
      const frac = this.rng.range(0.2, 0.8)
      const funded = this.clampFunded(scaleRes(job.spec.requests, frac), free.get(nodeId))
      const alloc = this.newAlloc(job, attempt, nodeId, funded, 'Accruing')
      job.state = { kind: 'Attempting', attempt: attempt.id }
      job.projectedStartUs = this.rng.bool(0.75)
        ? this.nowUs + this.rng.range(1 * MINUTE_US, 25 * MINUTE_US)
        : null
      const cur = free.get(nodeId)
      if (cur) free.set(nodeId, subRes(cur, alloc.funded))
    }
  }

  private buildQueuedJobs(target: number): void {
    for (let i = 0; i < target; i += 1) {
      const submittedAtUs = this.nowUs - this.rng.range(10 * SECOND_US, 40 * MINUTE_US)
      this.newJob({ kind: 'Queued' }, submittedAtUs)
    }
  }

  private buildPipelineJobs(state: JobState, target: number): void {
    for (let i = 0; i < target; i += 1) {
      const submittedAtUs = this.nowUs - this.rng.range(1 * SECOND_US, 20 * SECOND_US)
      this.newJob(state, submittedAtUs)
    }
  }

  private buildTerminalJobs(target: number): void {
    for (let i = 0; i < target; i += 1) {
      // Spread submissions across the last ~72h of history.
      const submittedAtUs = this.nowUs - this.rng.range(30 * MINUTE_US, 72 * HOUR_US)
      const outcomeRoll = this.rng.float()
      let state: JobState
      if (outcomeRoll < 0.1) state = { kind: 'Failed' }
      else if (outcomeRoll < 0.14) state = { kind: 'Aborted' }
      else state = { kind: 'Succeeded' }
      const job = this.newJob(state, submittedAtUs)

      const runForUs = this.rng.range(1 * MINUTE_US, 5 * HOUR_US)
      const startedAtUs = submittedAtUs + this.rng.range(2 * SECOND_US, 3 * MINUTE_US)
      const endedAtUs = Math.min(this.nowUs - MINUTE_US, startedAtUs + runForUs)
      job.terminalAtUs = endedAtUs

      this.maybeAddRetries(job, startedAtUs)

      const node = this.rng.pick([...this.nodes.values()])
      const attempt = this.newAttempt(job, node.id, 'Terminal')
      attempt.startedAtUs = startedAtUs
      attempt.endedAtUs = endedAtUs
      attempt.outcome = this.finalOutcome(state, job)
      attempt.chargedUcu = this.jobChargeModel(job).upfrontUcu
      this.seedUsage(attempt, job.spec.requests, startedAtUs, endedAtUs)
      // Released allocation: recorded for history, excluded from capacity.
      this.newAlloc(job, attempt, node.id, { ...job.spec.requests }, 'Released')

      this.applyTrueUp(job, attempt, state)
    }
  }

  /**
   * Prepend earlier retried attempts (each with a transient Platform
   * outcome), never exceeding the job's retry budget — total attempts must
   * stay ≤ maxRetries + 1 or views like "attempts 2 of 1" turn nonsensical.
   */
  private maybeAddRetries(job: MJob, currentStartUs: number): void {
    if (job.spec.retry.maxRetries === 0 || !this.rng.bool(0.15)) return
    const retries = this.rng.int(1, job.spec.retry.maxRetries)
    const node = this.rng.pick([...this.nodes.values()])
    for (let r = 0; r < retries; r += 1) {
      const startedAtUs =
        currentStartUs - (retries - r) * this.rng.range(2 * MINUTE_US, 30 * MINUTE_US)
      const endedAtUs = startedAtUs + this.rng.range(30 * SECOND_US, 8 * MINUTE_US)
      const attempt = this.newAttempt(job, node.id, 'Terminal')
      attempt.startedAtUs = startedAtUs
      attempt.endedAtUs = endedAtUs
      const kind = this.rng.weighted(PLATFORM_POOL)
      attempt.outcome = { kind, class: 'Platform' }
      // Platform-attributable outcomes refund in full → net zero charge.
      attempt.chargedUcu = 0
      // Pull/start failures never ran a container, so they record no usage.
      if (kind !== 'PullFailed' && kind !== 'StartFailed') {
        this.seedUsage(attempt, job.spec.requests, startedAtUs, endedAtUs)
      }
      this.newAlloc(job, attempt, node.id, { ...job.spec.requests }, 'Released')
      job.retriesUsed += 1
    }
  }

  private finalOutcome(state: JobState, job: MJob): AttemptOutcome {
    if (state.kind === 'Succeeded') return { kind: 'Exited', exitCode: 0, class: 'Success' }
    if (state.kind === 'Aborted') return { kind: 'Aborted', class: 'UserRequest' }
    // A job with no runtime bound can never time out on it.
    const pool = FAILURE_POOL.filter(
      (f) => f[0] !== 'MaxRuntimeExceeded' || job.spec.maxRuntimeUs !== null,
    )
    const [kind, cls] = this.rng.weighted(pool.map((f) => [[f[0], f[1]] as const, f[2]] as const))
    if (kind === 'Exited') return { kind, exitCode: this.rng.pick([1, 2, 127, 137]), class: cls }
    return { kind, class: cls }
  }

  private applyTrueUp(job: MJob, attempt: MAttempt, state: JobState): void {
    const charged = job.attempts.reduce((s, id) => s + (this.attempts.get(id)?.chargedUcu ?? 0), 0)
    // The upfront charge covered the whole window at the effective rate; the
    // tail the job never used is refunded (partly for a declared bound, fully
    // for an unbounded job or a platform-attributable outcome).
    const model = this.jobChargeModel(job)
    const ranUs = Math.max(
      0,
      (attempt.endedAtUs ?? this.nowUs) - (attempt.startedAtUs ?? this.nowUs),
    )
    const unusedUs = Math.max(0, model.chargeWindowUs - ranUs)
    const fraction = attempt.outcome?.class === 'Platform' ? 1 : model.refundFraction
    const refund = Math.min(
      charged,
      Math.round(model.effectiveRate * (unusedUs / SECOND_US) * fraction),
    )
    job.actualUcu = Math.max(0, charged - refund)
    job.trueUp = refund > 0 ? { kind: 'Refund', amountUcu: refund } : null
    if (state.kind === 'Aborted') {
      job.abortRequested = {
        reason: this.rng.pick(['user requested', 'superseded', 'cost cap', null]),
        requestedAtUs:
          (attempt.endedAtUs ?? this.nowUs) - this.rng.range(SECOND_US, 30 * SECOND_US),
      }
    }
  }

  private findFit(
    need: Resources,
    free: Map<string, Resources>,
    placeable: MNode[],
  ): string | null {
    const order = this.rng.shuffle([...placeable])
    for (const node of order) {
      const f = free.get(node.id)
      if (f && fitsRes(need, f)) return node.id
    }
    return null
  }

  private pickAccrualNode(placeable: MNode[], free: Map<string, Resources>): string | null {
    // Prefer nodes under the accrual limit; pick the one with most cpu headroom.
    const counts = this.accrualCounts()
    let best: string | null = null
    let bestCpu = -1
    for (const node of placeable) {
      if ((counts.get(node.id) ?? 0) >= ACCRUAL_LIMIT_PER_NODE) continue
      const f = free.get(node.id)
      const cpu = f ? f.cpuMillis : 0
      if (cpu > bestCpu) {
        bestCpu = cpu
        best = node.id
      }
    }
    return best
  }

  private accrualCounts(): Map<string, number> {
    const counts = new Map<string, number>()
    for (const alloc of this.allocs.values()) {
      if (alloc.state === 'Accruing') counts.set(alloc.node, (counts.get(alloc.node) ?? 0) + 1)
    }
    return counts
  }

  private clampFunded(funded: Resources, free: Resources | undefined): Resources {
    if (!free) return funded
    return res(
      Math.min(funded.cpuMillis, Math.max(0, free.cpuMillis)),
      Math.min(funded.memoryBytes, Math.max(0, free.memoryBytes)),
      Math.min(funded.diskBytes, Math.max(0, free.diskBytes)),
    )
  }

  /** Backfill an attempt's usage ring over its [start, end] run window. */
  private seedUsage(attempt: MAttempt, requested: Resources, startUs: number, endUs: number): void {
    const span = Math.max(0, endUs - startUs)
    const step = Math.max(SECOND_US, Math.floor(span / USAGE_RING))
    const samples: UsageSample[] = []
    const seedRng = new Rng(hashSeed(attempt.id))
    for (let t = startUs; t <= endUs && samples.length < USAGE_RING; t += step) {
      samples.push(this.usageSample(t, requested, seedRng))
    }
    attempt.usage = samples
  }

  private usageSample(tUs: number, requested: Resources, rng: Rng): UsageSample {
    // Usage wobbles under requested; memory occasionally near the ceiling.
    const cpuFrac = rng.range(0.35, 0.95)
    const memFrac = rng.bool(0.15) ? rng.range(0.9, 0.99) : rng.range(0.4, 0.85)
    const diskFrac = rng.range(0.2, 0.7)
    return {
      t: at(tUs),
      cpuMillis: Math.round(requested.cpuMillis * cpuFrac),
      memoryBytes: Math.round(requested.memoryBytes * memFrac),
      diskBytes: Math.round(requested.diskBytes * diskFrac),
    }
  }

  private seedQueueHistory(): void {
    // Pre-fill the ~30-minute sparkline window so it is full on first load.
    const start = this.nowUs - QUEUE_HISTORY_BUCKETS * QUEUE_BUCKET_US
    const depth = this.countByState('Queued')
    const histRng = new Rng(hashSeed(this.clusterId))
    for (let i = 0; i < QUEUE_HISTORY_BUCKETS; i += 1) {
      const tUs = start + i * QUEUE_BUCKET_US
      const wobble = histRng.int(-4, 4)
      this.queueHistory.push({
        tUs,
        depth: Math.max(0, depth + wobble),
        // Roughly the live steady-state rates (see tickArrivals) so the
        // sparklines don't jump when real ticks take over from the seed.
        drainedPerMinute: histRng.int(7, 13),
        arrivedPerMinute: histRng.int(7, 14),
      })
    }
    this.lastBucketUs = this.nowUs
  }

  // ---- simulation ----------------------------------------------------------

  /** Process elapsed 1s ticks up to `nowUs`, then pin the clock to `nowUs`. */
  advanceTo(nowUs: number): void {
    if (nowUs <= this.nowUs) {
      this.nowUs = Math.max(this.nowUs, nowUs)
      return
    }
    let steps = 0
    while (this.lastTickUs + TICK_US <= nowUs && steps < MAX_TICKS_PER_ADVANCE) {
      this.lastTickUs += TICK_US
      this.nowUs = this.lastTickUs
      this.tick()
      steps += 1
    }
    this.lastTickUs = nowUs
    this.nowUs = nowUs
    this.recomputeQueueRanks()
  }

  private tick(): void {
    this.raftIndex += this.rng.int(1, 4)
    this.stateVersion += this.rng.int(1, 3)
    this.tickQuotaDecay()
    this.tickRunningJobs()
    this.tickAccruals()
    this.tickAdmissions()
    this.tickPipeline()
    this.tickArrivals()
    this.tickAutoMintUsers()
    this.tickCoordinators()
    this.tickUtilHistory()
    this.tickQuotaHistory()
    this.rollQueueBucket()
  }

  /** Decay every entity's usage once per tick with the 24h half-life. */
  private tickQuotaDecay(): void {
    for (const ent of this.entities.values()) {
      ent.usageUcu *= QUOTA_DECAY_PER_TICK
    }
  }

  /**
   * Charge `amountUcu` (the FULL cost) to every ancestor of `entityId`,
   * leaf→root, depth-capped like `charge_ancestors`. Grows live usage and
   * appends to each entity's 30s-bucketed charge log.
   */
  private chargeAncestors(entityId: string, amountUcu: number): void {
    if (amountUcu <= 0) return
    let cur: QEntity | undefined = this.entities.get(entityId)
    let depth = 0
    while (cur && depth < MAX_QUOTA_DEPTH) {
      cur.usageUcu += amountUcu
      const last = cur.charges[cur.charges.length - 1]
      if (last && this.nowUs - last.tUs < QUOTA_BUCKET_US) {
        last.amountUcu += amountUcu
      } else {
        cur.charges.push({ tUs: this.nowUs, amountUcu })
        const cutoff = this.nowUs - CHARGE_WINDOW_US
        while (cur.charges.length > 0 && cur.charges[0]!.tUs < cutoff) cur.charges.shift()
      }
      cur = cur.parent ? this.entities.get(cur.parent) : undefined
      depth += 1
    }
  }

  /** Apply a terminal true-up to the job's ancestor chain (saturating ≥ 0). */
  /**
   * Simulate a previously-unseen OIDC principal submitting its first job:
   * with a small per-tick probability, auto-mint a new SSO user under the
   * users root (until the cap) and add it to the traffic pool.
   */
  private tickAutoMintUsers(): void {
    if (this.usedSubs.size >= MAX_USERS) return
    // ~one new identity per 5–10 minutes of sim time.
    if (!this.rng.bool(0.0025)) return
    const sub = USER_SUBS.find((s) => !this.usedSubs.has(s))
    if (!sub) return
    const user = this.mintUserEntity(sub, USER_DEFAULT_QUOTA_UCU, 0)
    this.leafIds.push(user.id)
    this.jobLeaves.push([user.id, 2])
  }

  /** Append a decayed-usage sample to every entity's ring on bucket cadence. */
  private tickQuotaHistory(): void {
    if (this.nowUs - this.lastQuotaBucketUs < QUOTA_BUCKET_US) return
    for (const ent of this.entities.values()) {
      ent.usageHistory.push({ tUs: this.nowUs, usageUcu: Math.max(0, Math.round(ent.usageUcu)) })
      if (ent.usageHistory.length > QUOTA_HISTORY_BUCKETS) ent.usageHistory.shift()
    }
    this.lastQuotaBucketUs = this.nowUs
  }

  private tickRunningJobs(): void {
    for (const job of this.jobs.values()) {
      const attempt = this.currentAttempt(job)
      if (!attempt || (attempt.state !== 'Running' && attempt.state !== 'Finalizing')) continue
      // Append a bounded usage sample. The job's own charge was taken upfront
      // at placement (see tickAdmissions); the per-tick charge to ancestors is
      // the entity-usage simulation that drives quota sparklines and penalties.
      const seedRng = new Rng(hashSeed(attempt.id + this.nowUs))
      attempt.usage.push(this.usageSample(this.nowUs, job.spec.requests, seedRng))
      if (attempt.usage.length > USAGE_RING) attempt.usage.shift()
      const delta = Math.round(attempt.rateUcuPerSecond * (TICK_US / SECOND_US))
      this.chargeAncestors(job.spec.quotaEntity, delta)

      if (attempt.state === 'Running') {
        const overRuntime =
          job.spec.maxRuntimeUs !== null &&
          attempt.startedAtUs !== null &&
          this.nowUs - attempt.startedAtUs > job.spec.maxRuntimeUs
        // p ≈ 0.0035/s → mean runtime ~5min. Tuned together with the arrival
        // rate in tickArrivals so demand roughly matches effective service
        // at saturation and a visible queue + accruals persist at steady
        // state without growing unboundedly. No job-level transition here —
        // the job stays `Attempting(attempt)`; only the attempt's state
        // moves, which is what the timeline now shows (ADR 0030).
        if (overRuntime || this.rng.bool(0.0035)) {
          attempt.state = 'Finalizing'
          this.pushEvent({
            atUs: this.nowUs,
            kind: 'AttemptStateChanged',
            attempt: attempt.id,
            job: job.id,
            node: attempt.node,
            state: 'Finalizing',
          })
        }
      } else if (this.rng.bool(0.25)) {
        this.finishJob(job, attempt)
      }
    }
  }

  private finishJob(job: MJob, attempt: MAttempt): void {
    const failed = this.rng.bool(0.08)
    const state: JobState = failed ? { kind: 'Failed' } : { kind: 'Succeeded' }
    attempt.state = 'Terminal'
    attempt.endedAtUs = this.nowUs
    attempt.outcome = this.finalOutcome(state, job)
    job.terminalAtUs = this.nowUs
    this.applyTrueUp(job, attempt, state)
    // Entity usage is charged pay-as-you-go per running tick (see
    // tickRunningJobs) and already tracks actual consumption, so the job's
    // upfront-model refund is not replayed onto ancestors — doing so would
    // double-count and could zero out an entity's usage on a big finish.
    // Release the allocation, freeing node capacity.
    const alloc = this.allocs.get(attempt.allocation)
    if (alloc) alloc.state = 'Released'
    this.transition(job, state)
    this.pushEvent({
      atUs: this.nowUs,
      kind: 'AttemptStateChanged',
      attempt: attempt.id,
      job: job.id,
      node: attempt.node,
      state: 'Terminal',
    })
    this.drainsThisBucket += 0 // finishing running work isn't a queue drain
  }

  private tickAccruals(): void {
    const free = this.nodeFreeCapacity()
    for (const job of this.jobs.values()) {
      const attempt = this.currentAttempt(job)
      if (!attempt || attempt.state !== 'Accruing') continue
      const alloc = this.allocs.get(attempt.allocation)
      if (!alloc || alloc.state !== 'Accruing') continue
      // A projection that has come and gone without funding was optimistic;
      // re-project the way a scheduler pass would recompute the bound.
      if (job.projectedStartUs !== null && job.projectedStartUs <= this.nowUs) {
        job.projectedStartUs = this.nowUs + this.rng.range(1 * MINUTE_US, 15 * MINUTE_US)
      }
      const nodeFree = free.get(alloc.node) ?? zeroRes()
      // Fund up toward requested, bounded by remaining node headroom.
      const gain = this.rng.range(0.05, 0.3)
      const target = scaleRes(alloc.requested, minFraction(alloc.funded, alloc.requested) + gain)
      const grow = this.clampFunded(
        res(
          Math.min(target.cpuMillis, alloc.requested.cpuMillis),
          Math.min(target.memoryBytes, alloc.requested.memoryBytes),
          Math.min(target.diskBytes, alloc.requested.diskBytes),
        ),
        addRes(alloc.funded, nodeFree),
      )
      const delta = subRes(grow, alloc.funded)
      if (delta.cpuMillis > 0 || delta.memoryBytes > 0 || delta.diskBytes > 0) {
        alloc.funded = grow
        free.set(alloc.node, subRes(nodeFree, delta))
      }
      if (fitsRes(alloc.requested, alloc.funded)) {
        // Fully funded → Active → Running. No job-level transition — the
        // job stays `Attempting(attempt)`; only the attempt moves (ADR 0030).
        alloc.funded = { ...alloc.requested }
        alloc.state = 'Active'
        attempt.state = 'Running'
        attempt.startedAtUs = this.nowUs
        this.seedUsage(attempt, job.spec.requests, this.nowUs - SECOND_US, this.nowUs)
        this.pushEvent({
          atUs: this.nowUs,
          kind: 'AttemptStateChanged',
          attempt: attempt.id,
          job: job.id,
          node: alloc.node,
          state: 'Running',
        })
        this.pushEvent({
          atUs: this.nowUs,
          kind: 'AllocationFunded',
          allocation: alloc.id,
          job: job.id,
          node: alloc.node,
        })
      }
    }
  }

  private tickAdmissions(): void {
    const free = this.nodeFreeCapacity()
    const counts = this.accrualCounts()
    const placeable = this.placeableNodes()
    const queued = [...this.jobs.values()].filter((j) => j.state.kind === 'Queued')
    // Admit the best-ranked queued jobs first, at most one per tick
    // (mirrors `max_placements_per_cycle`) — jobs must visibly dwell in the
    // queue instead of being whisked into Preparing on arrival.
    queued.sort((a, b) => this.score(b) - this.score(a))
    let placements = 0
    for (const job of queued) {
      if (placements >= 1) break
      if (!this.rng.bool(0.4)) continue

      // Free-fit first (mirrors the real scheduler's try_free_fit): seat the
      // job fully funded on the node with the tightest fit. Only when no
      // node can hold it outright does it open a partially-funded accrual —
      // otherwise accrual slots pin big jobs to one node while free capacity
      // idles elsewhere and the whole cluster wedges.
      let fitNode: string | null = null
      let fitCpu = Number.MAX_SAFE_INTEGER
      let accrualNode: string | null = null
      let accrualCpu = -1
      for (const node of placeable) {
        const f = free.get(node.id)
        if (!f) continue
        if (fitsRes(job.spec.requests, f) && f.cpuMillis < fitCpu) {
          fitCpu = f.cpuMillis
          fitNode = node.id
        }
        if ((counts.get(node.id) ?? 0) < ACCRUAL_LIMIT_PER_NODE && f.cpuMillis > accrualCpu) {
          accrualCpu = f.cpuMillis
          accrualNode = node.id
        }
      }
      const placedNode = fitNode ?? accrualNode
      if (!placedNode) break
      const attempt = this.newAttempt(job, placedNode, 'Accruing')
      // Placement charges the full window upfront (trued up at finalization).
      attempt.chargedUcu = this.jobChargeModel(job).upfrontUcu
      const funded = fitNode
        ? { ...job.spec.requests }
        : this.clampFunded(
            scaleRes(job.spec.requests, this.rng.range(0.15, 0.5)),
            free.get(placedNode),
          )
      const alloc = this.newAlloc(job, attempt, placedNode, funded, 'Accruing')
      job.projectedStartUs = this.nowUs + this.rng.range(1 * MINUTE_US, 20 * MINUTE_US)
      this.transition(job, { kind: 'Attempting', attempt: attempt.id })
      counts.set(placedNode, (counts.get(placedNode) ?? 0) + 1)
      const f = free.get(placedNode)
      if (f) free.set(placedNode, subRes(f, alloc.funded))
      this.drainsThisBucket += 1
      placements += 1
    }
  }

  private tickPipeline(): void {
    for (const job of this.jobs.values()) {
      if (job.state.kind === 'Submitted' && this.nowUs - job.lastTransitionUs > 2 * SECOND_US) {
        this.transition(job, { kind: 'Accepted' })
      } else if (
        job.state.kind === 'Accepted' &&
        this.nowUs - job.lastTransitionUs > 3 * SECOND_US
      ) {
        this.transition(job, { kind: 'Queued' })
        this.arrivalsThisBucket += 1
      }
    }
  }

  private tickArrivals(): void {
    // Demand is elastic around a target queue depth (~30): pressure eases
    // as the backlog grows and swells when it shrinks, so the cluster
    // hovers near saturation with a persistent, bounded queue instead of
    // either draining to zero or growing forever. The ±30% sinusoid (~18min
    // cycle) gives the drain/arrival sparklines visible shape.
    const depth = this.countByState('Queued')
    const pressure = Math.min(1.5, Math.max(0.4, 1.5 - depth / 40))
    const swell = 1 + 0.3 * Math.sin((2 * Math.PI * this.nowUs) / (18 * MINUTE_US))
    if (!this.rng.bool(0.2 * pressure * swell)) return
    const job = this.newJob({ kind: 'Submitted' }, this.nowUs)
    this.pushEvent({ atUs: this.nowUs, kind: 'JobSubmitted', job: job.id })
  }

  private tickCoordinators(): void {
    for (const c of this.coordinators) {
      const wob = (v: number) => Math.min(0.95, Math.max(0.03, v + this.rng.range(-0.03, 0.03)))
      c.host = {
        cpuFraction: wob(c.host.cpuFraction),
        memoryFraction: wob(c.host.memoryFraction),
        diskFraction: wob(c.host.diskFraction),
      }
      c.lastSeenUs = this.nowUs - this.rng.range(SECOND_US, 3 * SECOND_US)
      if (c.role !== 'Leader') c.lagEntries = this.rng.int(0, 2)
    }
    if (this.raftIndex - this.snapshotIndex > 3000) {
      this.snapshotIndex = this.raftIndex - this.rng.int(50, 150)
      this.snapshotAtUs = this.nowUs
    }
  }

  /** Append a real (allocated, used) sample to every node's ring. */
  private tickUtilHistory(): void {
    if (this.nowUs - this.lastUtilBucketUs < UTIL_BUCKET_US) return
    for (const node of this.nodes.values()) {
      node.utilHistory.push({
        t: at(this.nowUs),
        used: this.nodeUsed(node.id),
        allocated: this.nodeAllocated(node.id),
      })
      if (node.utilHistory.length > UTIL_BUCKETS) node.utilHistory.shift()
    }
    this.lastUtilBucketUs = this.nowUs
  }

  private rollQueueBucket(): void {
    if (this.nowUs - this.lastBucketUs < QUEUE_BUCKET_US) return
    const windowMin = QUEUE_BUCKET_US / MINUTE_US
    this.queueHistory.push({
      tUs: this.nowUs,
      depth: this.countByState('Queued'),
      drainedPerMinute: Math.round(this.drainsThisBucket / windowMin),
      arrivedPerMinute: Math.round(this.arrivalsThisBucket / windowMin),
    })
    if (this.queueHistory.length > QUEUE_HISTORY_BUCKETS) this.queueHistory.shift()
    this.lastBucketUs = this.nowUs
    this.arrivalsThisBucket = 0
    this.drainsThisBucket = 0
  }

  private transition(job: MJob, to: JobState): void {
    const from = job.state
    if (from.kind === to.kind && jobAttemptId(from) === jobAttemptId(to)) return
    job.state = to
    job.lastTransitionUs = this.nowUs
    if (isTerminalJobState(to)) job.terminalAtUs = job.terminalAtUs ?? this.nowUs
    // The wire event carries state *kinds*: `Attempting`'s attempt id is
    // flattened away, exactly like `coppice_state::Event` on the wire.
    this.pushEvent({
      atUs: this.nowUs,
      kind: 'JobStateChanged',
      job: job.id,
      from: from.kind,
      to: to.kind,
    })
  }

  /// Callers pass the simulation clock (`atUs`); the view's `at` is stamped
  /// here, with identity, so every caller stays in simulation µs.
  private pushEvent({ atUs, ...body }: { atUs: number } & TimelineEventBody): void {
    // Identity is stamped here, once, like the real apply loop: the mock
    // emits one event per simulated command, so ordinals are always 0.
    const ev: TimelineEvent = {
      index: this.nextEventIndex++,
      ordinal: 0,
      at: at(atUs),
      ...body,
    }
    this.events.push(ev)
    if (this.events.length > EVENTS_RING) this.events.shift()
    const jobId = 'job' in ev ? (ev.job as string) : null
    if (jobId) {
      const list = this.jobEvents.get(jobId) ?? []
      list.push(ev)
      this.jobEvents.set(jobId, list)
    }
  }

  // ---- scoring -------------------------------------------------------------

  private multiplier(priority: number): number {
    return PRIORITY_MULTIPLIER[priority] ?? 1
  }

  private penaltyChain(entityId: string): Array<{
    entity: string
    name: string
    usageUcu: number
    quotaUcu: number
    overQuotaRatio: number
    penalty: number
  }> {
    const chain = []
    let cur: QEntity | undefined = this.entities.get(entityId)
    while (cur) {
      const ratio = cur.quotaUcu > 0 ? cur.usageUcu / cur.quotaUcu : 0
      chain.push({
        entity: cur.id,
        name: cur.name,
        usageUcu: cur.usageUcu,
        quotaUcu: cur.quotaUcu,
        overQuotaRatio: ratio,
        penalty: Math.max(1, ratio * ratio),
      })
      cur = cur.parent ? this.entities.get(cur.parent) : undefined
    }
    return chain
  }

  private score(job: MJob): number {
    const chain = this.penaltyChain(job.spec.quotaEntity)
    const penaltyProduct = chain.reduce((p, c) => p * c.penalty, 1)
    const ageUs = this.nowUs - job.submittedAtUs
    return this.multiplier(job.spec.priority) / penaltyProduct + (W_AGE * ageUs) / AGE_HORIZON_US
  }

  private recomputeQueueRanks(): void {
    const queued = [...this.jobs.values()].filter((j) => j.state.kind === 'Queued')
    queued.sort((a, b) => this.score(b) - this.score(a))
    this.queuedRank.clear()
    queued.forEach((job, i) => this.queuedRank.set(job.id, i + 1))
    this.queueDepth = queued.length
  }

  private queuedRank = new Map<string, number>()
  private queueDepth = 0

  // ---- lookups -------------------------------------------------------------

  private countByState(kind: JobStateKind): number {
    let n = 0
    for (const job of this.jobs.values()) if (job.state.kind === kind) n += 1
    return n
  }

  private entityName(id: string): string {
    return this.entities.get(id)?.name ?? id
  }

  private childrenOf(id: string): QEntity[] {
    return [...this.entities.values()].filter((e) => e.parent === id)
  }

  /** The entity itself plus all descendants (BFS, depth-capped). */
  private subtreeEntityIds(id: string): Set<string> {
    const out = new Set<string>()
    if (!this.entities.has(id)) return out
    const queue = [id]
    while (queue.length > 0) {
      const cur = queue.shift()!
      if (out.has(cur)) continue
      out.add(cur)
      for (const child of this.childrenOf(cur)) queue.push(child.id)
    }
    return out
  }

  /** Rebuild the leaf set after a structural change (create/reparent). */
  private recomputeLeaves(): void {
    const hasChild = new Set<string>()
    for (const ent of this.entities.values()) {
      if (ent.parent) hasChild.add(ent.parent)
    }
    this.leafIds = [...this.entities.values()].filter((e) => !hasChild.has(e.id)).map((e) => e.id)
  }

  /** Recompute every entity's cached depth by walking to its root. */
  private recomputeDepths(): void {
    for (const ent of this.entities.values()) {
      let depth = 0
      let cur: QEntity | undefined = ent.parent ? this.entities.get(ent.parent) : undefined
      while (cur && depth < MAX_QUOTA_DEPTH) {
        depth += 1
        cur = cur.parent ? this.entities.get(cur.parent) : undefined
      }
      ent.depth = depth
    }
  }

  private jobOrThrow(id: string): MJob {
    const job = this.jobs.get(id)
    if (!job) throw new NotFound(`job ${id}`)
    return job
  }

  private nodeOrThrow(id: string): MNode {
    const node = this.nodes.get(id)
    if (!node) throw new NotFound(`node ${id}`)
    return node
  }

  // ---- node aggregates -----------------------------------------------------

  private nodeAllocated(nodeId: string): Resources {
    let total = zeroRes()
    for (const alloc of this.allocs.values()) {
      if (alloc.node === nodeId && alloc.state !== 'Released') total = addRes(total, alloc.funded)
    }
    return total
  }

  private nodeUsed(nodeId: string): Resources {
    let total = zeroRes()
    for (const job of this.jobs.values()) {
      const attempt = this.currentAttempt(job)
      if (!attempt || (attempt.state !== 'Running' && attempt.state !== 'Finalizing')) continue
      if (attempt.node !== nodeId) continue
      const last = attempt.usage[attempt.usage.length - 1]
      if (last) total = addRes(total, res(last.cpuMillis, last.memoryBytes, last.diskBytes))
    }
    return total
  }

  private nodeCounts(nodeId: string): { running: number; accruing: number } {
    let running = 0
    let accruing = 0
    for (const alloc of this.allocs.values()) {
      if (alloc.node !== nodeId) continue
      if (alloc.state === 'Active') running += 1
      else if (alloc.state === 'Accruing') accruing += 1
    }
    return { running, accruing }
  }

  // ===========================================================================
  // View builders (public API surface). All return freshly built objects.
  // ===========================================================================

  buildClusterOverview(): ClusterOverview {
    let capacity = zeroRes()
    let allocated = zeroRes()
    let used = zeroRes()
    let schedulable = 0
    let lost = 0
    for (const node of this.nodes.values()) {
      if (node.health === 'Lost') {
        lost += 1
      } else {
        capacity = addRes(capacity, node.capacity)
        if (node.schedulable) schedulable += 1
      }
      allocated = addRes(allocated, this.nodeAllocated(node.id))
      used = addRes(used, this.nodeUsed(node.id))
    }
    return {
      clusterId: this.clusterId,
      queue: this.buildQueueStats(),
      capacity: {
        nodes: { total: this.nodes.size, schedulable, lost },
        capacity,
        allocated,
        used,
      },
      recentEvents: this.buildRecentEvents(20),
    }
  }

  /**
   * The bounded most-recent window with its exclusive coverage cursor
   * (ADR 0032): complete strictly above `floorIndex`, which rises when the
   * ring evicted or the limit truncated.
   */
  private buildRecentEvents(limit: number): RecentEventsWindow {
    const events = [...this.events]
      .reverse()
      .slice(0, limit)
      .map((e) => ({ ...e }))
    const oldest = events[events.length - 1]
    const floorIndex = oldest !== undefined ? oldest.index - 1 : this.nextEventIndex - 1
    return { floorIndex, events }
  }

  buildQueueStats(): QueueStats {
    const byState = {} as Record<JobPhase, number>
    for (const s of JOB_PHASES) byState[s] = 0
    let oldestQueuedAgeUs: number | null = null
    for (const job of this.jobs.values()) {
      byState[this.jobPhase(job)] += 1
      if (job.state.kind === 'Queued') {
        const age = this.nowUs - job.submittedAtUs
        if (oldestQueuedAgeUs === null || age > oldestQueuedAgeUs) oldestQueuedAgeUs = age
      }
    }
    const recent = this.queueHistory.slice(-6)
    const avg = (pick: (b: QueueBucket) => number) =>
      recent.length ? recent.reduce((s, b) => s + pick(b), 0) / recent.length : 0
    return {
      depth: byState.Queued,
      drainRatePerMinute: Math.round(avg((b) => b.drainedPerMinute)),
      arrivalRatePerMinute: Math.round(avg((b) => b.arrivedPerMinute)),
      oldestQueuedAgeSeconds: oldestQueuedAgeUs === null ? null : secondsOf(oldestQueuedAgeUs),
      byState,
      history: this.queueHistory.map((b) => ({
        t: at(b.tUs),
        depth: b.depth,
        drainedPerMinute: b.drainedPerMinute,
        arrivedPerMinute: b.arrivedPerMinute,
      })),
    }
  }

  listJobs(filter: ListJobsFilter): JobList {
    const wanted = filter.states ? new Set(filter.states) : null
    const search = filter.search?.toLowerCase()
    // quotaEntity matches the whole subtree (entity + descendants).
    const subtree = filter.quotaEntity ? this.subtreeEntityIds(filter.quotaEntity) : null
    let matches = [...this.jobs.values()].filter((job) => {
      if (wanted && !wanted.has(this.jobPhase(job))) return false
      if (subtree && !subtree.has(job.spec.quotaEntity)) return false
      if (filter.node) {
        const attempt = this.currentAttempt(job)
        if (!attempt || attempt.node !== filter.node) return false
      }
      if (search) {
        const hay = `${job.id} ${job.spec.image}`.toLowerCase()
        if (!hay.includes(search)) return false
      }
      return true
    })
    // Non-terminal first (submittedAt desc), then terminal (terminalAt desc).
    matches = matches.sort((a, b) => {
      const at = this.isTerminal(a)
      const bt = this.isTerminal(b)
      if (at !== bt) return at ? 1 : -1
      if (!at) return b.submittedAtUs - a.submittedAtUs
      return (b.terminalAtUs ?? 0) - (a.terminalAtUs ?? 0)
    })
    const total = matches.length
    const limit = filter.limit ?? 100
    return {
      total,
      jobs: matches.slice(0, limit).map((job) => this.jobSummary(job)),
    }
  }

  private isTerminal(job: MJob): boolean {
    return isTerminalJobState(job.state)
  }

  private jobSummary(job: MJob): JobSummary {
    const attempt = this.currentAttempt(job)
    const alloc = attempt ? this.allocs.get(attempt.allocation) : undefined
    const fundingFraction =
      attempt?.state === 'Accruing' && alloc ? minFraction(alloc.funded, alloc.requested) : null
    return {
      id: job.id,
      state: job.state,
      image: job.spec.image,
      quotaEntity: job.spec.quotaEntity,
      quotaEntityName: this.entityName(job.spec.quotaEntity),
      priority: job.spec.priority,
      submittedAt: at(job.submittedAtUs),
      terminalAt: job.terminalAtUs === null ? null : at(job.terminalAtUs),
      node: attempt ? attempt.node : null,
      attemptState: attempt ? attempt.state : null,
      queueRank: job.state.kind === 'Queued' ? (this.queuedRank.get(job.id) ?? null) : null,
      fundingFraction,
      // Settled net cost once terminal (after the true-up refund); the gross
      // upfront charge while the job is still holding it.
      costUcu:
        this.isTerminal(job) && job.actualUcu != null ? job.actualUcu : this.totalCharged(job),
      outcome: this.isTerminal(job) ? this.lastOutcome(job) : null,
    }
  }

  private totalCharged(job: MJob): number {
    return job.attempts.reduce((s, id) => s + (this.attempts.get(id)?.chargedUcu ?? 0), 0)
  }

  private lastOutcome(job: MJob): AttemptOutcome | null {
    const last = job.attempts[job.attempts.length - 1]
    return last ? (this.attempts.get(last)?.outcome ?? null) : null
  }

  buildJobDetail(id: string): JobDetail {
    const job = this.jobOrThrow(id)
    const attempt = this.currentAttempt(job)
    return {
      id: job.id,
      state: job.state,
      spec: {
        image: job.spec.image,
        entrypoint: job.spec.entrypoint ? [...job.spec.entrypoint] : null,
        command: [...job.spec.command],
        env: { ...job.spec.env },
        requests: { ...job.spec.requests },
        priority: job.spec.priority,
        maxRuntimeSeconds: job.spec.maxRuntimeUs === null ? null : secondsOf(job.spec.maxRuntimeUs),
        quotaEntity: job.spec.quotaEntity,
        retry: { ...job.spec.retry },
      },
      submittedAt: at(job.submittedAtUs),
      stateSince: at(this.stateSince(job, attempt)),
      terminalAt: job.terminalAtUs === null ? null : at(job.terminalAtUs),
      retriesUsed: job.retriesUsed,
      abortRequested: job.abortRequested
        ? {
            reason: job.abortRequested.reason,
            requestedAt: at(job.abortRequested.requestedAtUs),
          }
        : null,
      entityChain: this.entityChain(job.spec.quotaEntity),
      attempts: job.attempts.map((aid) => this.attemptView(aid)),
      queue: job.state.kind === 'Queued' ? this.queueExplainer(job) : null,
      accrual: attempt && attempt.state === 'Accruing' ? this.accrualView(attempt) : null,
      cost: this.costReport(job),
    }
  }

  /** Project one entity to the read-only view (usage rounded at the edge). */
  private entityView(ent: QEntity): QuotaEntityView {
    const ratio = ent.quotaUcu > 0 ? ent.usageUcu / ent.quotaUcu : 0
    return {
      id: ent.id,
      name: ent.name,
      parent: ent.parent,
      quotaUcu: ent.quotaUcu,
      usageUcu: Math.round(ent.usageUcu),
      overQuotaRatio: ratio,
      penalty: Math.max(1, ratio * ratio),
    }
  }

  /**
   * When the job entered its current state. Seeded jobs are built directly
   * into a state (no event history), so fall back to the best per-state
   * anchor: attempt start for Running, terminal time for terminal states,
   * last recorded transition otherwise.
   */
  private stateSince(job: MJob, attempt: MAttempt | undefined): number {
    if (this.isTerminal(job) && job.terminalAtUs !== null) return job.terminalAtUs
    if (attempt?.state === 'Running' && attempt.startedAtUs != null) return attempt.startedAtUs
    return job.lastTransitionUs
  }

  private entityChain(leafId: string): JobDetail['entityChain'] {
    // Build leaf→root then reverse to root→leaf per the contract.
    const chain: QuotaEntityView[] = []
    let cur: QEntity | undefined = this.entities.get(leafId)
    let depth = 0
    while (cur && depth < MAX_QUOTA_DEPTH) {
      chain.push(this.entityView(cur))
      cur = cur.parent ? this.entities.get(cur.parent) : undefined
      depth += 1
    }
    return chain.reverse()
  }

  private attemptView(id: string): AttemptView {
    const a = this.attempts.get(id)
    if (!a) throw new NotFound(`attempt ${id}`)
    return {
      id: a.id,
      job: a.job,
      node: a.node,
      allocation: a.allocation,
      state: a.state,
      outcome: a.outcome ? { ...a.outcome } : null,
      startedAt: a.startedAtUs === null ? null : at(a.startedAtUs),
      endedAt: a.endedAtUs === null ? null : at(a.endedAtUs),
      rateUcuPerSecond: a.rateUcuPerSecond,
      chargedUcu: a.chargedUcu,
    }
  }

  private allocView(id: string): AllocationView {
    const a = this.allocs.get(id)
    if (!a) throw new NotFound(`allocation ${id}`)
    return {
      id: a.id,
      job: a.job,
      attempt: a.attempt,
      node: a.node,
      requested: { ...a.requested },
      funded: { ...a.funded },
      state: a.state,
      seq: a.seq,
    }
  }

  private queueExplainer(job: MJob): QueuePositionExplainer {
    const chain = this.penaltyChain(job.spec.quotaEntity)
    const penaltyProduct = chain.reduce((p, c) => p * c.penalty, 1)
    const multiplier = this.multiplier(job.spec.priority)
    const ageUs = this.nowUs - job.submittedAtUs
    const ageBonus = (W_AGE * ageUs) / AGE_HORIZON_US
    return {
      rank: this.queuedRank.get(job.id) ?? 1,
      queueDepth: this.queueDepth,
      score: multiplier / penaltyProduct + ageBonus,
      multiplier,
      penaltyChain: chain,
      penaltyProduct,
      ageSeconds: secondsOf(ageUs),
      ageHorizonSeconds: secondsOf(AGE_HORIZON_US),
      wAge: W_AGE,
      ageBonus,
    }
  }

  private accrualView(attempt: MAttempt): AccrualView {
    const alloc = this.allocs.get(attempt.allocation)
    if (!alloc) throw new NotFound(`allocation ${attempt.allocation}`)
    const job = this.jobs.get(attempt.job)
    const frac = (a: number, b: number) => (b > 0 ? Math.min(1, a / b) : 1)
    return {
      allocation: this.allocView(alloc.id),
      fundedFraction: {
        cpu: frac(alloc.funded.cpuMillis, alloc.requested.cpuMillis),
        memory: frac(alloc.funded.memoryBytes, alloc.requested.memoryBytes),
        disk: frac(alloc.funded.diskBytes, alloc.requested.diskBytes),
      },
      projectedStart: job && job.projectedStartUs !== null ? at(job.projectedStartUs) : null,
    }
  }

  /**
   * The upfront-charge model for a job (ADR 0005/0029) — the single source of
   * every cost number shown. The `max_runtime` window (or the policy default
   * when the job declared no bound) is priced in full at the effective rate at
   * placement; the unused tail is (partly) refunded at true-up.
   */
  private jobChargeModel(job: MJob) {
    const terms = rateTerms(job.spec.requests)
    const base = computeRate(job.spec.requests)
    const bounded = job.spec.maxRuntimeUs !== null
    const priorityMultiplier = this.multiplier(job.spec.priority)
    const unboundedMultiplier = bounded ? 1 : UNBOUNDED_RUNTIME_MULTIPLIER
    const effectiveRate = base * priorityMultiplier * unboundedMultiplier
    const chargeWindowUs = job.spec.maxRuntimeUs ?? DEFAULT_CHARGE_RUNTIME_US
    const upfrontUcu = Math.round(effectiveRate * (chargeWindowUs / SECOND_US))
    const refundFraction = bounded ? REFUND_FRACTION : 1
    return {
      base,
      terms,
      bounded,
      priorityMultiplier,
      unboundedMultiplier,
      effectiveRate,
      chargeWindowUs,
      upfrontUcu,
      refundFraction,
    }
  }

  private costReport(job: MJob): CostReport {
    const m = this.jobChargeModel(job)
    return {
      rateUcuPerSecond: m.base,
      // Unrounded per-dimension µCU/s: disk weights are sub-µCU/s per GiB, so
      // rounding here would zero out the disk term and its derived per-unit rate.
      rateBreakdown: { cpu: m.terms.cpu, memory: m.terms.memory, disk: m.terms.disk },
      priorityMultiplier: m.priorityMultiplier,
      unboundedMultiplier: m.unboundedMultiplier,
      effectiveRateUcuPerSecond: m.effectiveRate,
      chargeWindowSeconds: secondsOf(m.chargeWindowUs),
      chargeWindowIsDefault: job.spec.maxRuntimeUs === null,
      estimatedUcu: m.upfrontUcu,
      chargedUcu: this.totalCharged(job),
      refundFraction: m.refundFraction,
      actualUcu: this.isTerminal(job) ? job.actualUcu : null,
      trueUp: this.isTerminal(job) && job.trueUp ? { ...job.trueUp } : null,
    }
  }

  buildJobTimeline(id: string): TimelineEvent[] {
    this.jobOrThrow(id)
    const list = this.jobEvents.get(id) ?? []
    // Always include the synthetic submission at the head. Index 0 sits
    // below every real index, so identity ordering keeps it first.
    const submit: TimelineEvent = {
      index: 0,
      ordinal: 0,
      at: at(this.jobs.get(id)!.submittedAtUs),
      kind: 'JobSubmitted',
      job: id,
    }
    const merged = [submit, ...list.filter((e) => e.kind !== 'JobSubmitted')]
    // Order by identity `(index, ordinal)`, never by `atUs` (ADR 0032: the
    // stamp is advisory and may run backwards across proposers).
    return merged.sort((a, b) => a.index - b.index || a.ordinal - b.ordinal).map((e) => ({ ...e }))
  }

  /** Usage for one attempt; null = the current (else latest) attempt. */
  buildJobUsage(id: string, attemptId: string | null = null): JobUsage {
    const job = this.jobOrThrow(id)
    const chosen =
      attemptId ?? jobAttemptId(job.state) ?? job.attempts[job.attempts.length - 1] ?? null
    if (chosen === null) {
      return { attempt: null, requested: { ...job.spec.requests }, samples: [] }
    }
    const attempt = this.attempts.get(chosen)
    if (!attempt || attempt.job !== id) throw new NotFound(`attempt ${chosen} of job ${id}`)
    return {
      attempt: attempt.id,
      requested: { ...job.spec.requests },
      samples: attempt.usage.map((s) => ({ ...s })),
    }
  }

  // ---- quota entities ------------------------------------------------------

  /** Subtree-inclusive Queued/Running job counts for every entity, one pass. */
  private subtreeJobCounts(): { queued: Map<string, number>; running: Map<string, number> } {
    const queued = new Map<string, number>()
    const running = new Map<string, number>()
    for (const id of this.entities.keys()) {
      queued.set(id, 0)
      running.set(id, 0)
    }
    for (const job of this.jobs.values()) {
      const phase = this.jobPhase(job)
      if (phase !== 'Queued' && phase !== 'Running') continue
      const map = phase === 'Queued' ? queued : running
      let cur: QEntity | undefined = this.entities.get(job.spec.quotaEntity)
      let depth = 0
      while (cur && depth < MAX_QUOTA_DEPTH) {
        map.set(cur.id, (map.get(cur.id) ?? 0) + 1)
        cur = cur.parent ? this.entities.get(cur.parent) : undefined
        depth += 1
      }
    }
    return { queued, running }
  }

  private buildQuotaEntityNode(
    ent: QEntity,
    queuedCount: number,
    runningCount: number,
  ): QuotaEntityNode {
    const ratio = ent.quotaUcu > 0 ? ent.usageUcu / ent.quotaUcu : 0
    return {
      id: ent.id,
      name: ent.name,
      parent: ent.parent,
      origin: ent.origin,
      principal: ent.principal,
      quotaUcu: ent.quotaUcu,
      usageUcu: Math.max(0, Math.round(ent.usageUcu)),
      overQuotaRatio: ratio,
      penalty: Math.max(1, ratio * ratio),
      createdAt: at(ent.createdAtUs),
      updatedAt: at(ent.updatedAtUs),
      queuedCount,
      runningCount,
    }
  }

  private buildQuotaEntityNodeFor(id: string): QuotaEntityNode {
    const { queued, running } = this.subtreeJobCounts()
    return this.buildQuotaEntityNode(
      this.entities.get(id)!,
      queued.get(id) ?? 0,
      running.get(id) ?? 0,
    )
  }

  listQuotaEntities(): QuotaEntityNode[] {
    const { queued, running } = this.subtreeJobCounts()
    return [...this.entities.values()]
      .map((e) => this.buildQuotaEntityNode(e, queued.get(e.id) ?? 0, running.get(e.id) ?? 0))
      .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
  }

  buildQuotaEntityDetail(id: string): QuotaEntityDetail {
    const ent = this.entities.get(id)
    if (!ent) throw new NotFound(`quota entity ${id}`)
    const { queued, running } = this.subtreeJobCounts()
    const entity = this.buildQuotaEntityNode(ent, queued.get(id) ?? 0, running.get(id) ?? 0)
    const children = this.childrenOf(id)
      .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
      .map((c) => this.buildQuotaEntityNode(c, queued.get(c.id) ?? 0, running.get(c.id) ?? 0))
    return {
      entity,
      chain: this.entityChain(id),
      children,
      stats: this.buildQuotaStats(ent),
    }
  }

  private buildQuotaStats(ent: QEntity): QuotaEntityStats {
    const subtree = this.subtreeEntityIds(ent.id)
    const byState = {} as Record<JobPhase, number>
    for (const s of JOB_PHASES) byState[s] = 0
    let oldestQueuedAgeUs: number | null = null
    let burnRateUcuPerSecond = 0
    for (const job of this.jobs.values()) {
      if (!subtree.has(job.spec.quotaEntity)) continue
      const phase = this.jobPhase(job)
      byState[phase] += 1
      if (phase === 'Queued') {
        const age = this.nowUs - job.submittedAtUs
        if (oldestQueuedAgeUs === null || age > oldestQueuedAgeUs) oldestQueuedAgeUs = age
      } else if (phase === 'Running') {
        const a = this.currentAttempt(job)
        if (a && a.state === 'Running') burnRateUcuPerSecond += a.rateUcuPerSecond
      }
    }
    const cutoff = this.nowUs - CHARGE_WINDOW_US
    const chargedUcu24h = ent.charges
      .filter((c) => c.tUs >= cutoff)
      .reduce((s, c) => s + c.amountUcu, 0)
    return {
      byState,
      oldestQueuedAgeSeconds: oldestQueuedAgeUs === null ? null : secondsOf(oldestQueuedAgeUs),
      burnRateUcuPerSecond,
      chargedUcu24h,
      usageHistory: ent.usageHistory.map((h) => ({ t: at(h.tUs), usageUcu: h.usageUcu })),
    }
  }

  /**
   * Create-or-update upsert mirroring the `ConfigureQuotaEntity` command
   * (ADR 0005/0019): no delete in v1, updates preserve accumulated usage,
   * cycle/depth validation walks up from the proposed parent.
   */
  configureQuotaEntity(input: ConfigureQuotaEntityInput): QuotaEntityNode {
    const name = input.name.trim()
    if (name.length === 0) throw new MockInvalid('name must be non-empty')
    if (
      !Number.isFinite(input.quotaUcu) ||
      !Number.isInteger(input.quotaUcu) ||
      input.quotaUcu < 0
    ) {
      throw new MockInvalid('quotaUcu must be a non-negative integer')
    }
    const parentId = input.parent
    if (parentId !== null && !this.entities.has(parentId)) {
      throw new MockInvalid(`UnknownQuotaEntity: ${parentId}`)
    }

    if (input.entity !== null) {
      // ---- update ----
      const ent = this.entities.get(input.entity)
      if (!ent) throw new NotFound(`quota entity ${input.entity}`)
      if (ent.origin === 'sso' && (name !== ent.name || parentId !== ent.parent)) {
        throw new MockInvalid('an SSO identity owns its name and parent; only quota may change')
      }
      if (parentId !== ent.parent) this.assertReparentable(ent, parentId)
      ent.name = name
      ent.parent = parentId
      ent.quotaUcu = input.quotaUcu
      ent.updatedAtUs = this.nowUs
      // usageUcu deliberately preserved — reconfiguration is not an amnesty.
      this.recomputeDepths()
      this.recomputeLeaves()
      return this.buildQuotaEntityNodeFor(ent.id)
    }

    // ---- create ----
    const depth = parentId ? this.entities.get(parentId)!.depth + 1 : 0
    if (depth > MAX_QUOTA_DEPTH) {
      throw new MockInvalid(`quota entity depth exceeds cap ${MAX_QUOTA_DEPTH}`)
    }
    const ent = this.newEntity({
      name,
      parent: parentId,
      quotaUcu: input.quotaUcu,
      usageUcu: 0,
      depth,
      origin: 'configured',
    })
    this.seedEmptyQuotaHistory(ent)
    this.recomputeLeaves()
    return this.buildQuotaEntityNodeFor(ent.id)
  }

  /** Validate a reparent: no cycle, and the moved subtree fits the depth cap. */
  private assertReparentable(ent: QEntity, newParentId: string | null): void {
    let cur: QEntity | undefined = newParentId ? this.entities.get(newParentId) : undefined
    let steps = 0
    while (cur) {
      if (cur.id === ent.id) throw new MockInvalid(`QuotaEntityCycle at ${ent.id}`)
      steps += 1
      if (steps > MAX_QUOTA_DEPTH) {
        throw new MockInvalid(`quota entity depth exceeds cap ${MAX_QUOTA_DEPTH}`)
      }
      cur = cur.parent ? this.entities.get(cur.parent) : undefined
    }
    const newDepth = newParentId ? this.entities.get(newParentId)!.depth + 1 : 0
    if (newDepth + this.subtreeHeight(ent.id) > MAX_QUOTA_DEPTH) {
      throw new MockInvalid(`quota entity depth exceeds cap ${MAX_QUOTA_DEPTH}`)
    }
  }

  /** Height (max edges to a descendant leaf) of the subtree rooted at `id`. */
  private subtreeHeight(id: string): number {
    let height = 0
    const stack: Array<readonly [string, number]> = [[id, 0]]
    while (stack.length > 0) {
      const [cur, d] = stack.pop()!
      if (d > height) height = d
      for (const child of this.childrenOf(cur)) stack.push([child.id, d + 1])
    }
    return height
  }

  private seedEmptyQuotaHistory(ent: QEntity): void {
    const hist: Array<{ tUs: number; usageUcu: number }> = []
    for (let i = QUOTA_HISTORY_BUCKETS - 1; i >= 0; i -= 1) {
      hist.push({ tUs: this.nowUs - i * QUOTA_BUCKET_US, usageUcu: 0 })
    }
    ent.usageHistory = hist
    ent.charges = []
  }

  buildNodeSummaries(): NodeSummary[] {
    return [...this.nodes.values()].map((n) => this.nodeSummary(n))
  }

  private nodeSummary(node: MNode): NodeSummary {
    const counts = this.nodeCounts(node.id)
    return {
      id: node.id,
      capacity: { ...node.capacity },
      allocated: this.nodeAllocated(node.id),
      used: this.nodeUsed(node.id),
      labels: { ...node.labels },
      schedulable: node.schedulable,
      health: node.health,
      epoch: node.epoch,
      lastHeartbeat: node.lastHeartbeatUs === null ? null : at(node.lastHeartbeatUs),
      runningCount: counts.running,
      accruingCount: counts.accruing,
    }
  }

  buildNodeDetail(id: string): NodeDetail {
    const node = this.nodeOrThrow(id)
    const activeAttempts: AttemptView[] = []
    const accrualQueue: AccrualView[] = []
    for (const attempt of this.attempts.values()) {
      if (attempt.node !== node.id) continue
      if (
        attempt.state === 'Running' ||
        attempt.state === 'Dispatching' ||
        attempt.state === 'Finalizing'
      ) {
        activeAttempts.push(this.attemptView(attempt.id))
      } else if (attempt.state === 'Accruing') {
        accrualQueue.push(this.accrualView(attempt))
      }
    }
    accrualQueue.sort((a, b) => a.allocation.seq - b.allocation.seq)
    return { summary: this.nodeSummary(node), activeAttempts, accrualQueue }
  }

  buildNodeUtilization(id: string): NodeUtilization {
    const node = this.nodeOrThrow(id)
    return {
      capacity: { ...node.capacity },
      samples: node.utilHistory.map((s) => ({
        t: s.t,
        used: { ...s.used },
        allocated: { ...s.allocated },
      })),
    }
  }

  buildNodeHistory(id: string): NodeHistoryEntry[] {
    this.nodeOrThrow(id)
    const entries: NodeHistoryEntry[] = []
    for (const attempt of this.attempts.values()) {
      if (attempt.node !== id || attempt.state !== 'Terminal' || !attempt.outcome) continue
      if (attempt.endedAtUs === null) continue
      const job = this.jobs.get(attempt.job)
      entries.push({
        attempt: attempt.id,
        job: attempt.job,
        image: job?.spec.image ?? 'unknown',
        outcome: { ...attempt.outcome },
        startedAt: attempt.startedAtUs === null ? null : at(attempt.startedAtUs),
        // Non-null: the loop skips attempts that have not ended.
        endedAt: at(attempt.endedAtUs),
      })
    }
    return entries.sort((a, b) => b.endedAt.getTime() - a.endedAt.getTime()).slice(0, 50)
  }

  buildCoordinatorStatus(): CoordinatorStatus {
    const leader = this.coordinators.find((c) => c.role === 'Leader') ?? null
    const members: CoordinatorMember[] = this.coordinators.map((c) => ({
      id: c.id,
      addr: c.addr,
      role: c.role,
      voter: c.voter,
      lastApplied: this.raftIndex - c.lagEntries,
      replicationLagEntries: c.lagEntries,
      host: { ...c.host },
      lastSeen: at(c.lastSeenUs),
    }))
    return {
      clusterId: this.clusterId,
      leader: leader ? leader.id : null,
      term: 7,
      knownCommitted: this.raftIndex,
      lastApplied: this.raftIndex - (leader ? leader.lagEntries : 0),
      stateVersion: this.stateVersion,
      snapshot: {
        sizeBytes: 40 * 1024 * 1024,
        lastIncludedIndex: this.snapshotIndex,
        takenAt: at(this.snapshotAtUs),
        entriesSinceSnapshot: this.raftIndex - this.snapshotIndex,
      },
      stateCounts: {
        jobs: this.jobs.size,
        attempts: this.attempts.size,
        allocations: this.allocs.size,
        nodes: this.nodes.size,
        quotaEntities: this.entities.size,
      },
      members,
    }
  }

  // ---- logs ----------------------------------------------------------------

  buildJobLogs(id: string, cursor: string | null): LogChunk {
    const job = this.jobOrThrow(id)
    return pageLogs(this.jobLogLines(job), cursor)
  }

  buildNodeLogs(id: string, cursor: string | null): LogChunk {
    const node = this.nodeOrThrow(id)
    return pageLogs(this.nodeLogLines(node), cursor)
  }

  buildCoordinatorLogs(id: number, cursor: string | null): LogChunk {
    const c = this.coordinators.find((m) => m.id === id)
    if (!c) throw new NotFound(`coordinator ${id}`)
    return pageLogs(this.coordinatorLogLines(c), cursor)
  }

  private jobLogLines(job: MJob): LogEntry[] {
    const rng = new Rng(hashSeed(job.id + 'log'))
    const lines: LogEntry[] = []
    const push = (tUs: number, level: LogLevel, target: string, message: string) =>
      lines.push({ t: at(tUs), level, target, message })

    // Every job at least acknowledges submission to the coordinator.
    push(job.submittedAtUs, 'info', 'coordinator.admission', 'job submitted, awaiting admission')

    // Container-runtime lines only exist once the current attempt has actually
    // started a container. Submitted/Accepted/Queued have no attempt at all,
    // and a Preparing job's attempt is still Accruing (startedAtUs === null) —
    // none of them have pulled an image or started an entrypoint yet.
    const attempt = this.currentAttempt(job)
    if (!attempt || attempt.startedAtUs === null) return lines

    const start = attempt.startedAtUs
    const end = job.terminalAtUs ?? this.nowUs
    // Anchor the pull/start sequence to when the container actually started,
    // so it reflects the current attempt rather than the submission time.
    push(start - 9 * SECOND_US, 'info', 'agent.puller', `pulling image ${job.spec.image}`)
    push(start - 5 * SECOND_US, 'info', 'agent.puller', 'image present, extracting layers')
    push(start, 'info', 'agent.runtime', 'created container, starting entrypoint')
    const appLines = rng.int(40, 90)
    let t = start + 3 * SECOND_US
    for (let i = 0; i < appLines && t < end; i += 1) {
      t += rng.range(2 * SECOND_US, 90 * SECOND_US)
      const level = rng.weighted([
        ['info', 8],
        ['debug', 3],
        ['warn', 1],
      ] as const) as LogLevel
      push(t, level, 'app', APP_LINES[rng.int(0, APP_LINES.length - 1)] ?? 'working')
    }
    const outcome = this.lastOutcome(job)
    if (outcome) {
      if (outcome.kind === 'Exited' && outcome.exitCode === 0) {
        push(end, 'info', 'app', 'done; flushing outputs')
        push(end, 'info', 'agent.runtime', 'container exited code 0')
      } else if (outcome.kind === 'OomKilled') {
        push(end, 'error', 'agent.runtime', 'container OOM-killed (memory.max exceeded)')
      } else {
        push(end, 'error', 'agent.runtime', `container terminated: ${outcome.kind}`)
      }
    }
    return lines
  }

  private nodeLogLines(node: MNode): LogEntry[] {
    const rng = new Rng(hashSeed(node.id + 'log'))
    const lines: LogEntry[] = []
    const count = 100
    let t = this.nowUs - count * 15 * SECOND_US
    for (let i = 0; i < count; i += 1) {
      t += rng.range(8 * SECOND_US, 20 * SECOND_US)
      const roll = rng.float()
      if (roll < 0.55) {
        lines.push({
          t: at(t),
          level: 'debug',
          target: 'agent.heartbeat',
          message: 'heartbeat ack',
        })
      } else if (roll < 0.8) {
        lines.push({
          t: at(t),
          level: 'info',
          target: 'agent.reconcile',
          message: 'reconciled desired allocations',
        })
      } else if (roll < 0.95) {
        lines.push({
          t: at(t),
          level: 'info',
          target: 'agent.alloc',
          message: 'allocation funded → active',
        })
      } else {
        lines.push({
          t: at(t),
          level: 'warn',
          target: 'agent.reconcile',
          message: 'drift detected, resyncing',
        })
      }
    }
    if (node.health === 'Lost') {
      lines.push({
        t: at(this.nowUs),
        level: 'error',
        target: 'agent.heartbeat',
        message: 'heartbeat timeout; marking node Lost',
      })
    }
    return lines
  }

  private coordinatorLogLines(c: MCoordinator): LogEntry[] {
    const rng = new Rng(hashSeed(String(c.id) + 'coordlog'))
    const lines: LogEntry[] = []
    const count = 100
    let t = this.nowUs - count * 10 * SECOND_US
    let idx = this.raftIndex - count * 3
    for (let i = 0; i < count; i += 1) {
      t += rng.range(4 * SECOND_US, 14 * SECOND_US)
      idx += rng.int(1, 5)
      const roll = rng.float()
      if (roll < 0.6) {
        lines.push({
          t: at(t),
          level: 'debug',
          target: 'raft.log',
          message: `appended entries up to index ${idx}`,
        })
      } else if (roll < 0.9) {
        lines.push({
          t: at(t),
          level: 'info',
          target: 'raft.commit',
          message: `committed index ${idx}`,
        })
      } else if (roll < 0.97) {
        lines.push({
          t: at(t),
          level: 'info',
          target: 'raft.snapshot',
          message: 'snapshot trigger: log size threshold',
        })
      } else {
        lines.push({
          t: at(t),
          level: 'warn',
          target: 'raft.election',
          message: `election tick; term ${7}`,
        })
      }
    }
    return lines
  }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

class NotFound extends Error {
  readonly notFound = true
  constructor(what: string) {
    super(`not found: ${what}`)
    this.name = 'MockNotFound'
  }
}

export function isMockNotFound(e: unknown): boolean {
  return typeof e === 'object' && e !== null && 'notFound' in e
}

class MockInvalid extends Error {
  readonly invalid = true
  constructor(message: string) {
    super(message)
    this.name = 'MockInvalid'
  }
}

export function isMockInvalid(e: unknown): boolean {
  return typeof e === 'object' && e !== null && 'invalid' in e
}

const LOG_PAGE = 40

/** Cursor-paged newest-first; cursor is a stringified offset. */
function pageLogs(all: LogEntry[], cursor: string | null): LogChunk {
  const newestFirst = [...all].sort((a, b) => b.t.getTime() - a.t.getTime())
  const offset = cursor ? Math.max(0, Number.parseInt(cursor, 10) || 0) : 0
  const slice = newestFirst.slice(offset, offset + LOG_PAGE)
  const nextOffset = offset + LOG_PAGE
  return {
    entries: slice.map((e) => ({ ...e })),
    nextCursor: nextOffset < newestFirst.length ? String(nextOffset) : null,
  }
}

const APP_LINES = [
  'loaded checkpoint from object store',
  'epoch 3 step 1200 loss=0.4821',
  'validation batch complete, acc=0.913',
  'flushing metrics to collector',
  'processed 10000 records',
  'cache hit ratio 0.87',
  'GC pause 12ms',
  'shard 4/8 rebalanced',
  'retrying upstream request (attempt 2)',
  'wrote 512 MiB to scratch volume',
]
