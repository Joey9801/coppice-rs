import type { Resources } from '@/api/types'

/**
 * One instant of a capacity history: what the machines advertise
 * (`capacity`), what allocations have funded (`allocated`) and what was
 * actually measured (`used`). Both the cluster-wide history
 * (`CapacitySample`) and a single node's utilization history are projected
 * onto this shape, so the same charts serve both pages.
 */
export interface CapacityHistorySample {
  t: Date
  capacity: Resources
  allocated: Resources
  /**
   * Measured consumption. `null` means nothing was reporting at that
   * instant (a coverage gap, ADR 0032) — never a fabricated zero.
   */
  used: Resources | null
  /**
   * Cluster samples sum `used` over `reportingNodes` of `totalNodes`, so a
   * value can be partial; a single node's samples carry neither (usage is
   * reported for that node or it is not, which `used: null` already says).
   */
  reportingNodes?: number
  totalNodes?: number
}

/**
 * One dimension of one sample, split into the non-negative bands the chart
 * stacks. The raw `used`/`allocated`/`capacity` readings ride along
 * untouched so tooltips can report what was measured rather than what was
 * derived for drawing.
 *
 * The three raw series are NESTED totals, not additive categories, so they
 * cannot be stacked directly. The bands below are chosen so that stacking
 * them bottom-up reproduces the real quantities as boundaries:
 *
 * - `usedWithinAllocated` + `allocatedUnused` → top is `allocated`
 * - `usedWithinAllocated` + `allocatedUnused` + `usedOverAllocated` → top is
 *   `max(used, allocated)`, i.e. the whole of `used` is drawn (split across
 *   two bands, never truncated) even when it exceeds what was funded
 * - `+ unallocatedCapacity` → top is `capacity` whenever capacity is the
 *   largest of the three; when it is not (an over-committed reading) that
 *   band is zero and the dotted capacity line sits below the stack, which
 *   is the honest picture.
 */
export interface CapacityBandPoint {
  /** Epoch ms: recharts scales a numeric axis; `Date` is rebuilt to format a tick. */
  tMs: number
  /** Measured, as reported; `null` is a coverage gap (renders as a break). */
  used: number | null
  /** Funded by allocations at that instant (replicated state, not measured). */
  allocated: number
  /** Advertised by the node(s) at that instant. */
  capacity: number
  /** Band: `min(used, allocated)`; `null` (a gap) when usage was not reported. */
  usedWithinAllocated: number | null
  /** Band: `max(0, allocated - used)` — funded but idle. */
  allocatedUnused: number | null
  /** Band: `max(0, used - allocated)` — measured consumption past what was funded. */
  usedOverAllocated: number | null
  /** Band: `max(0, capacity - max(allocated, used))` — capacity nobody has funded. */
  unallocatedCapacity: number | null
  /** Nodes contributing a measured `used` here (cluster samples only). */
  reportingNodes: number | null
  totalNodes: number | null
}

/** A dimension's whole history plus the facts the chart chrome needs. */
export interface CapacitySeries {
  points: CapacityBandPoint[]
  /** At least one sample carries a measured `used`. */
  hasUsage: boolean
  /** At least one sample has no measured `used` (a gap the chart must show). */
  hasCoverageGap: boolean
  /** At least one sample had allocation left over its measured usage. */
  hasAllocatedUnused: boolean
  /** At least one sample measured more usage than was allocated. */
  hasOverAllocation: boolean
  /** At least one sample has capacity nobody has allocated. */
  hasUnallocated: boolean
  /** Largest value any series reaches — the axis must cover it. */
  max: number
}

/** Picks one dimension (cpu millis, memory bytes, disk bytes) out of `Resources`. */
export type ResourcePick = (r: Resources) => number

/**
 * Project a capacity history onto one resource dimension's stacked bands.
 *
 * Nothing is clamped or invented: `used` above `allocated` is preserved in
 * full (split across two bands so the excess is visible as such), `used:
 * null` stays `null` all the way to the chart, and an over-committed
 * reading simply leaves no unallocated band.
 */
export function toCapacitySeries(
  history: readonly CapacityHistorySample[],
  pick: ResourcePick,
): CapacitySeries {
  let hasUsage = false
  let hasCoverageGap = false
  let hasAllocatedUnused = false
  let hasOverAllocation = false
  let hasUnallocated = false
  let max = 0

  const points = history.map((sample) => {
    const capacity = pick(sample.capacity)
    const allocated = pick(sample.allocated)
    const used = sample.used === null ? null : pick(sample.used)

    if (used === null) hasCoverageGap = true
    else hasUsage = true

    // Every band is a share of measured usage or of what is left over it,
    // so with no measurement there is no decomposition to draw: the whole
    // stack is a gap at that instant and the three real series keep their
    // lines. Standing `allocated` up as if it were known-idle capacity
    // would be an invention.
    const usedWithinAllocated = used === null ? null : Math.min(used, allocated)
    const allocatedUnused = used === null ? null : Math.max(0, allocated - used)
    const usedOverAllocated = used === null ? null : Math.max(0, used - allocated)
    const committed = Math.max(allocated, used ?? allocated)
    const unallocatedCapacity = used === null ? null : Math.max(0, capacity - committed)

    if (allocatedUnused !== null && allocatedUnused > 0) hasAllocatedUnused = true
    if (usedOverAllocated !== null && usedOverAllocated > 0) hasOverAllocation = true
    if (unallocatedCapacity !== null && unallocatedCapacity > 0) hasUnallocated = true
    max = Math.max(max, capacity, allocated, used ?? 0)

    return {
      tMs: sample.t.getTime(),
      used,
      allocated,
      capacity,
      usedWithinAllocated,
      allocatedUnused,
      usedOverAllocated,
      unallocatedCapacity,
      reportingNodes: sample.reportingNodes ?? null,
      totalNodes: sample.totalNodes ?? null,
    }
  })

  return {
    points,
    hasUsage,
    hasCoverageGap,
    hasAllocatedUnused,
    hasOverAllocation,
    hasUnallocated,
    max,
  }
}
