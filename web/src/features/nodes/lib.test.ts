import { describe, expect, it } from 'vitest'
import type { NodeUtilization, Resources } from '@/api/types'
import { nodeCapacityHistory } from './lib'

function res(cpuMillis: number): Resources {
  return { cpuMillis, memoryBytes: cpuMillis * 1024, diskBytes: cpuMillis * 2 }
}

const utilization: NodeUtilization = {
  capacity: res(8_000),
  samples: [
    { t: new Date(1_720_000_000_000), used: res(2_000), allocated: res(4_000) },
    // The node stopped reporting: `used` is absent, not zero.
    { t: new Date(1_720_000_060_000), used: null, allocated: res(4_000) },
  ],
}

describe('nodeCapacityHistory', () => {
  it("carries the node's advertised capacity onto every sample", () => {
    expect(nodeCapacityHistory(utilization).map((s) => s.capacity)).toEqual([
      res(8_000),
      res(8_000),
    ])
  })

  it('preserves an unreported usage sample as null', () => {
    const history = nodeCapacityHistory(utilization)

    expect(history[0]!.used).toEqual(res(2_000))
    expect(history[1]!.used).toBeNull()
  })

  it('carries no node counts — a single node has no partial coverage to report', () => {
    for (const sample of nodeCapacityHistory(utilization)) {
      expect(sample.reportingNodes).toBeUndefined()
      expect(sample.totalNodes).toBeUndefined()
    }
  })

  it('is empty when the node has no samples yet', () => {
    expect(nodeCapacityHistory({ capacity: res(8_000), samples: [] })).toEqual([])
  })
})
