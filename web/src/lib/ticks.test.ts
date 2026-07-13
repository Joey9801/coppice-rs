import { describe, expect, it } from 'vitest'
import { byteTicks, cpuTicks, linearTicks } from './ticks'

const GIB = 1024 ** 3

function assertCovers(ticks: number[], max: number): void {
  expect(ticks[0]).toBe(0)
  expect(ticks[ticks.length - 1]).toBeGreaterThanOrEqual(max)
  for (let i = 1; i < ticks.length; i += 1) {
    expect(ticks[i]! - ticks[i - 1]!).toBeCloseTo(ticks[1]!, 6)
  }
}

describe('nice ticks', () => {
  it('linearTicks lands on round decimal steps', () => {
    expect(linearTicks(7.3)).toEqual([0, 2, 4, 6, 8])
    expect(linearTicks(0.87)).toEqual([0, 0.25, 0.5, 0.75, 1])
    assertCovers(linearTicks(123), 123)
  })

  it('byteTicks steps in the unit the labels use', () => {
    // Max 15.3 GiB → 5 GiB steps, not "3.83 GiB" steps.
    const ticks = byteTicks(15.3 * GIB)
    expect(ticks).toEqual([0, 5 * GIB, 10 * GIB, 15 * GIB, 20 * GIB])
    assertCovers(ticks, 15.3 * GIB)
    // Small raw-byte scales never produce fractional bytes.
    for (const t of byteTicks(7)) expect(Number.isInteger(t)).toBe(true)
  })

  it('cpuTicks steps in whole/nice core fractions', () => {
    // 6.4 cores → 2-core steps.
    expect(cpuTicks(6400)).toEqual([0, 2000, 4000, 6000, 8000])
    // 800m → 200m steps.
    expect(cpuTicks(800)).toEqual([0, 200, 400, 600, 800])
  })

  it('tolerates zero/negative maxima', () => {
    expect(linearTicks(0)).toEqual([0, 1])
    expect(byteTicks(-5)).toEqual([0, 1])
    expect(cpuTicks(0)).toEqual([0, 1000])
  })
})
