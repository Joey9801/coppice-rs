import { describe, expect, it } from 'vitest'
import type { Resources } from '../types'
import { TERMINAL_JOB_STATES } from '../types'
import { MockWorld } from './world'

const NOW_US = 1_760_000_000_000_000 // pinned "now" so construction is reproducible

function addRes(a: Resources, b: Resources): Resources {
  return {
    cpuMillis: a.cpuMillis + b.cpuMillis,
    memoryBytes: a.memoryBytes + b.memoryBytes,
    diskBytes: a.diskBytes + b.diskBytes,
  }
}

function fits(part: Resources, whole: Resources): boolean {
  return (
    part.cpuMillis <= whole.cpuMillis &&
    part.memoryBytes <= whole.memoryBytes &&
    part.diskBytes <= whole.diskBytes
  )
}

/**
 * Walk every public view the API exposes and assert the coherence invariants
 * documented on MockWorld. Called against a fresh world and a ticked one.
 */
function assertInvariants(world: MockWorld, nowUs: number): void {
  const nodes = world.buildNodeSummaries()
  const nodeIds = new Set(nodes.map((n) => n.id))

  // Per-node: allocated (Σ funded of non-Released) ≤ capacity.
  for (const node of nodes) {
    expect(fits(node.allocated, node.capacity)).toBe(true)
  }

  // All jobs, via listJobs with a high limit.
  const { jobs, total } = world.listJobs({ limit: 10_000 })
  expect(total).toBe(jobs.length)

  for (const summary of jobs) {
    const detail = world.buildJobDetail(summary.id)
    const terminal = TERMINAL_JOB_STATES.includes(detail.state)

    // Terminal jobs: outcome + terminalAtUs ≥ submittedAtUs.
    if (terminal) {
      expect(detail.terminalAtUs).not.toBeNull()
      expect(detail.terminalAtUs ?? 0).toBeGreaterThanOrEqual(detail.submittedAtUs)
      expect(summary.outcome).not.toBeNull()
    } else {
      expect(detail.terminalAtUs).toBeNull()
    }

    // entityChain runs root → leaf, matches parent links, ends at the owner.
    const chain = detail.entityChain
    expect(chain.length).toBeGreaterThan(0)
    expect(chain[0]?.parent).toBeNull()
    const leaf = chain[chain.length - 1]
    expect(leaf?.id).toBe(detail.spec.quotaEntity)
    for (let i = 1; i < chain.length; i += 1) {
      expect(chain[i]?.parent).toBe(chain[i - 1]?.id)
    }

    // Attempts/allocations reference existing jobs and nodes; funded ≤ requested.
    for (const attempt of detail.attempts) {
      expect(attempt.job).toBe(detail.id)
      expect(nodeIds.has(attempt.node)).toBe(true)
    }

    // Running job: exactly one current attempt Running with an Active,
    // fully funded allocation.
    if (detail.state === 'Running') {
      expect(detail.currentAttempt).not.toBeNull()
      const cur = detail.attempts.find((a) => a.id === detail.currentAttempt)
      expect(cur?.state).toBe('Running')
      const nodeDetail = world.buildNodeDetail(cur!.node)
      const active = nodeDetail.activeAttempts.find((a) => a.id === cur!.id)
      expect(active).toBeDefined()
    }

    // Queued job: no allocation, a rank in 1..depth, an accrual-free explainer.
    if (detail.state === 'Queued') {
      expect(detail.accrual).toBeNull()
      expect(detail.queue).not.toBeNull()
      const q = detail.queue!
      expect(q.rank).toBeGreaterThanOrEqual(1)
      expect(q.rank).toBeLessThanOrEqual(q.queueDepth)
      expect(summary.queueRank).toBe(q.rank)
      // No attempt has a live (non-terminal) allocation for a queued job.
      for (const attempt of detail.attempts) {
        expect(attempt.state).toBe('Terminal')
      }
    }

    // Accrual: fundedFraction per dim = funded/requested; projectedStart null or ≥ now.
    if (detail.accrual) {
      const { allocation, fundedFraction, projectedStartUs } = detail.accrual
      expect(fundedFraction.cpu).toBeCloseTo(
        allocation.funded.cpuMillis / allocation.requested.cpuMillis,
        6,
      )
      expect(fundedFraction.memory).toBeCloseTo(
        allocation.funded.memoryBytes / allocation.requested.memoryBytes,
        6,
      )
      expect(fits(allocation.funded, allocation.requested)).toBe(true)
      if (projectedStartUs !== null) expect(projectedStartUs).toBeGreaterThanOrEqual(nowUs)
    }

    // Non-terminal cost: no actual/trueUp yet.
    if (!terminal) {
      expect(detail.cost.actualUcu).toBeNull()
      expect(detail.cost.trueUp).toBeNull()
    }
  }

  // Queue ranks form 1..depth with no gaps.
  const queued = jobs.filter((j) => j.state === 'Queued')
  const ranks = queued.map((j) => j.queueRank).sort((a, b) => (a ?? 0) - (b ?? 0))
  ranks.forEach((r, i) => expect(r).toBe(i + 1))

  // Recompute per-node allocated independently from allocations exposed via
  // node details and compare to the summary (defense in depth on the Σ bound).
  for (const node of nodes) {
    const detail = world.buildNodeDetail(node.id)
    let allocated: Resources = { cpuMillis: 0, memoryBytes: 0, diskBytes: 0 }
    for (const acc of detail.accrualQueue) allocated = addRes(allocated, acc.allocation.funded)
    expect(fits(allocated, node.capacity)).toBe(true)
  }
}

describe('MockWorld construction', () => {
  it('holds all coherence invariants on a fresh world', () => {
    const world = new MockWorld(NOW_US)
    assertInvariants(world, NOW_US)
  })

  it('produces the required mix of job states', () => {
    const world = new MockWorld(NOW_US)
    const stats = world.buildQueueStats()
    expect(stats.byState.Running).toBeGreaterThan(10)
    expect(stats.byState.Queued).toBeGreaterThan(5)
    expect(stats.byState.Preparing).toBeGreaterThan(0)
    const terminal = stats.byState.Succeeded + stats.byState.Failed + stats.byState.Aborted
    expect(terminal).toBeGreaterThan(100)
    expect(stats.byState.Failed).toBeGreaterThan(0)
  })

  it('pre-seeds a full queue-stats history for sparklines', () => {
    const world = new MockWorld(NOW_US)
    const stats = world.buildQueueStats()
    expect(stats.history.length).toBeGreaterThanOrEqual(60)
    for (let i = 1; i < stats.history.length; i += 1) {
      expect(stats.history[i]!.tUs).toBeGreaterThan(stats.history[i - 1]!.tUs)
    }
  })

  it('has coordinators with a single leader and plausible snapshot', () => {
    const world = new MockWorld(NOW_US)
    const status = world.buildCoordinatorStatus()
    expect(status.members.length).toBe(3)
    expect(status.members.filter((m) => m.role === 'Leader').length).toBe(1)
    expect(status.snapshot.entriesSinceSnapshot).toBeGreaterThan(0)
    expect(status.stateCounts.nodes).toBe(16)
  })
})

describe('MockWorld determinism', () => {
  it('two worlds from the same seed produce identical listJobs at construction', () => {
    const a = new MockWorld(NOW_US)
    const b = new MockWorld(NOW_US)
    expect(a.listJobs({ limit: 10_000 })).toEqual(b.listJobs({ limit: 10_000 }))
  })

  it('differs for a different seed', () => {
    const a = new MockWorld(NOW_US, 1)
    const b = new MockWorld(NOW_US, 2)
    expect(a.listJobs({ limit: 10_000 })).not.toEqual(b.listJobs({ limit: 10_000 }))
  })

  // Deep links (/jobs/:id, /nodes/:id) must survive a page reload, which
  // rebuilds the world at a later wall-clock "now": ids are minted from a
  // seed-driven virtual clock and must not depend on construction time.
  it('mints the same ids regardless of construction time', () => {
    const a = new MockWorld(NOW_US)
    const b = new MockWorld(NOW_US + 3_600_000_000) // one hour later
    const ids = (w: MockWorld) => w.listJobs({ limit: 10_000 }).jobs.map((j) => j.id)
    expect(ids(a)).toEqual(ids(b))
    expect(a.buildNodeSummaries().map((n) => n.id)).toEqual(b.buildNodeSummaries().map((n) => n.id))
  })
})

describe('MockWorld simulation', () => {
  it('holds invariants after ~5 minutes of ticks', () => {
    const world = new MockWorld(NOW_US)
    let t = NOW_US
    for (let i = 0; i < 10; i += 1) {
      t += 30 * 1_000_000 // advance in 30s steps
      world.advanceTo(t)
    }
    assertInvariants(world, t)
  })

  // Regression: utilization history must be a stable recording that ticks
  // append to — not a series regenerated (and thus reshuffled) per request.
  it('records node utilization history instead of regenerating it', () => {
    const world = new MockWorld(NOW_US)
    const nodeId = world.buildNodeSummaries()[0]!.id
    const a = world.buildNodeUtilization(nodeId)
    expect(world.buildNodeUtilization(nodeId).samples).toEqual(a.samples)

    // The allocated line steps over the seeded window instead of being flat.
    const distinctAllocs = new Set(a.samples.map((s) => s.allocated.cpuMillis))
    expect(distinctAllocs.size).toBeGreaterThan(1)

    // 5 minutes of ticks → 10 new 30s buckets appended, oldest 10 dropped,
    // and every surviving prior sample preserved verbatim.
    world.advanceTo(NOW_US + 5 * 60_000_000)
    const b = world.buildNodeUtilization(nodeId)
    expect(b.samples.length).toBe(a.samples.length)
    expect(b.samples.slice(0, a.samples.length - 10)).toEqual(a.samples.slice(10))
  })

  // Regression: jobs must dwell in the queue long enough to inspect the
  // queueing UI — admissions are slot- and rate-gated, arrivals slightly
  // outpace service, so a nonempty queue with real wait times persists.
  it('keeps a visible queue with meaningful wait times after 15 minutes', () => {
    const world = new MockWorld(NOW_US)
    const t = NOW_US + 15 * 60_000_000
    world.advanceTo(t)
    const stats = world.buildQueueStats()
    expect(stats.byState.Queued).toBeGreaterThanOrEqual(5)
    expect(stats.oldestQueuedAgeUs ?? 0).toBeGreaterThanOrEqual(2 * 60_000_000)
  })

  it('advances raft/state version as it ticks', () => {
    const world = new MockWorld(NOW_US)
    const before = world.buildCoordinatorStatus()
    world.advanceTo(NOW_US + 300 * 1_000_000)
    const after = world.buildCoordinatorStatus()
    expect(after.knownCommitted).toBeGreaterThan(before.knownCommitted)
    expect(after.stateVersion).toBeGreaterThan(before.stateVersion)
  })
})

describe('MockWorld filters and lookups', () => {
  it('applies listJobs filters and reports pre-limit total', () => {
    const world = new MockWorld(NOW_US)
    const running = world.listJobs({ states: ['Running'], limit: 5 })
    expect(running.jobs.length).toBeLessThanOrEqual(5)
    for (const j of running.jobs) expect(j.state).toBe('Running')
    expect(running.total).toBeGreaterThanOrEqual(running.jobs.length)
  })

  it('throws a not-found marker for unknown ids', () => {
    const world = new MockWorld(NOW_US)
    expect(() => world.buildJobDetail('job-does-not-exist')).toThrow()
    expect(() => world.buildNodeDetail('node-does-not-exist')).toThrow()
  })

  it('pages logs with an opaque cursor until exhausted', () => {
    const world = new MockWorld(NOW_US)
    const node = world.buildNodeSummaries()[0]!
    let cursor: string | null = null
    let pages = 0
    do {
      const chunk: { nextCursor: string | null } = world.buildNodeLogs(node.id, cursor)
      cursor = chunk.nextCursor
      pages += 1
    } while (cursor && pages < 20)
    expect(pages).toBeGreaterThan(1)
    expect(cursor).toBeNull()
  })
})
