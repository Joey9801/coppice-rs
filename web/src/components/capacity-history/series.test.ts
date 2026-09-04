import { describe, expect, it } from 'vitest'
import type { Resources } from '@/api/types'
import { toCapacitySeries, type CapacityHistorySample } from './series'

const cpu = (r: Resources) => r.cpuMillis

function res(cpuMillis: number): Resources {
  return { cpuMillis, memoryBytes: cpuMillis * 1024, diskBytes: 0 }
}

function sample(
  capacity: number,
  allocated: number,
  used: number | null,
  extra: Partial<CapacityHistorySample> = {},
): CapacityHistorySample {
  return {
    t: new Date(1_720_000_000_000),
    capacity: res(capacity),
    allocated: res(allocated),
    used: used === null ? null : res(used),
    ...extra,
  }
}

/** The bands are only legible if they stack up to the real boundaries. */
function stackTops(point: {
  usedWithinAllocated: number | null
  allocatedUnused: number | null
  usedOverAllocated: number | null
  unallocatedCapacity: number | null
}) {
  const used = point.usedWithinAllocated ?? 0
  const allocated = used + (point.allocatedUnused ?? 0)
  const committed = allocated + (point.usedOverAllocated ?? 0)
  return { used, allocated, committed, top: committed + (point.unallocatedCapacity ?? 0) }
}

describe('toCapacitySeries', () => {
  it('splits a normal reading into used / allocated-unused / unallocated bands', () => {
    const series = toCapacitySeries([sample(8_000, 5_000, 2_000)], cpu)
    const p = series.points[0]!

    expect(p).toMatchObject({
      used: 2_000,
      allocated: 5_000,
      capacity: 8_000,
      usedWithinAllocated: 2_000,
      allocatedUnused: 3_000,
      usedOverAllocated: 0,
      unallocatedCapacity: 3_000,
    })
    // Stacked, the bands reproduce used → allocated → capacity exactly.
    expect(stackTops(p)).toEqual({ used: 2_000, allocated: 5_000, committed: 5_000, top: 8_000 })
    expect(series).toMatchObject({
      hasUsage: true,
      hasCoverageGap: false,
      hasOverAllocation: false,
      hasUnallocated: true,
      max: 8_000,
    })
  })

  it('leaves the whole stack a gap when usage was not measured', () => {
    // A `used: null` sample must never become a zero-usage reading, and
    // there is no "allocated but unused" claim to make either: every band
    // is a share of measured usage or of what is left over it. The
    // allocated/capacity readings survive as lines.
    const series = toCapacitySeries([sample(8_000, 5_000, null)], cpu)
    const p = series.points[0]!

    expect(p.used).toBeNull()
    expect(p.usedWithinAllocated).toBeNull()
    expect(p.allocatedUnused).toBeNull()
    expect(p.usedOverAllocated).toBeNull()
    expect(p.unallocatedCapacity).toBeNull()
    expect(p.allocated).toBe(5_000)
    expect(p.capacity).toBe(8_000)
    expect(series).toMatchObject({ hasUsage: false, hasCoverageGap: true })
  })

  it('reports partial coverage per sample and mixed coverage across a series', () => {
    const series = toCapacitySeries(
      [
        sample(8_000, 5_000, 2_000, { reportingNodes: 3, totalNodes: 16 }),
        sample(8_000, 5_000, null, { reportingNodes: 0, totalNodes: 16 }),
      ],
      cpu,
    )

    expect(series.points.map((p) => [p.reportingNodes, p.totalNodes])).toEqual([
      [3, 16],
      [0, 16],
    ])
    expect(series.hasUsage).toBe(true)
    expect(series.hasCoverageGap).toBe(true)
  })

  it('leaves no counts on samples that carry none (a single node)', () => {
    const p = toCapacitySeries([sample(8_000, 5_000, 2_000)], cpu).points[0]!
    expect(p.reportingNodes).toBeNull()
    expect(p.totalNodes).toBeNull()
  })

  it('keeps the allocated boundary when allocation exactly equals capacity', () => {
    // Nothing is left over to draw, so the dotted capacity line is the only
    // thing distinguishing the two — the bands must not hide either.
    const p = toCapacitySeries([sample(8_000, 8_000, 8_000)], cpu).points[0]!

    expect(p.unallocatedCapacity).toBe(0)
    expect(p.allocatedUnused).toBe(0)
    expect(p.usedWithinAllocated).toBe(8_000)
    expect(stackTops(p)).toEqual({ used: 8_000, allocated: 8_000, committed: 8_000, top: 8_000 })
  })

  it('shows usage above allocation as its own band, never truncated', () => {
    const series = toCapacitySeries([sample(8_000, 3_000, 4_500)], cpu)
    const p = series.points[0]!

    expect(p.used).toBe(4_500)
    expect(p.usedWithinAllocated).toBe(3_000)
    expect(p.allocatedUnused).toBe(0)
    expect(p.usedOverAllocated).toBe(1_500)
    // The two used bands add back up to the measured value.
    expect(p.usedWithinAllocated! + p.usedOverAllocated!).toBe(4_500)
    expect(p.unallocatedCapacity).toBe(3_500)
    expect(series.hasOverAllocation).toBe(true)
  })

  it('leaves no unallocated band when commitment exceeds capacity', () => {
    // Over-committed (or over-used) past what the node advertises: the
    // dotted capacity line ends up below the stack, which is the truth.
    const series = toCapacitySeries([sample(4_000, 6_000, 7_000)], cpu)
    const p = series.points[0]!

    expect(p.unallocatedCapacity).toBe(0)
    expect(series.hasUnallocated).toBe(false)
    expect(series.max).toBe(7_000)
    expect(stackTops(p).top).toBe(7_000)
  })

  it('handles a zero-capacity node without inventing anything', () => {
    const series = toCapacitySeries([sample(0, 0, 0)], cpu)

    expect(series.points[0]).toMatchObject({
      capacity: 0,
      allocated: 0,
      used: 0,
      usedWithinAllocated: 0,
      allocatedUnused: 0,
      usedOverAllocated: 0,
      unallocatedCapacity: 0,
    })
    expect(series.max).toBe(0)
    expect(series.hasUnallocated).toBe(false)
  })

  it('is empty, not degenerate, for an empty history', () => {
    expect(toCapacitySeries([], cpu)).toEqual({
      points: [],
      hasUsage: false,
      hasCoverageGap: false,
      hasOverAllocation: false,
      hasUnallocated: false,
      max: 0,
    })
  })

  it('projects each dimension independently', () => {
    const history = [sample(8_000, 5_000, 2_000)]
    const memory = toCapacitySeries(history, (r) => r.memoryBytes)
    const disk = toCapacitySeries(history, (r) => r.diskBytes)

    expect(memory.points[0]!.capacity).toBe(8_000 * 1024)
    expect(disk.points[0]!.capacity).toBe(0)
    expect(disk.max).toBe(0)
  })
})
