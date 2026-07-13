import { describe, expect, it } from 'vitest'
import { formatUnitPrice } from './format'

const GIB = 2 ** 30
const TIB = 2 ** 40

/** µCU/s for `count` units each priced so `perCu` unit-hours cost 1 CU. */
function ratePerSecond(count: number, perCu: number): number {
  // one unit-hour costs 1/perCu CU; total CU/hour = count / perCu.
  return ((count / perCu) * 1_000_000) / 3600
}

describe('formatUnitPrice', () => {
  it('quotes CPU as a clean core-hours reciprocal', () => {
    // 4 cores priced at 2 core-hours per CU.
    expect(formatUnitPrice(ratePerSecond(4, 2), 'cpu', 4000)).toBe('2 core-hours/CU')
  })

  it('uses the singular unit when one unit-hour is 1 CU', () => {
    expect(formatUnitPrice(ratePerSecond(1, 1), 'cpu', 1000)).toBe('1 core-hour/CU')
  })

  it('detects a binary (GiB) reciprocal', () => {
    // 16 GiB priced at 16 GiB-hours per CU.
    expect(formatUnitPrice(ratePerSecond(16, 16), 'bytes', 16 * GIB)).toBe('16 GiB-hours/CU')
  })

  it('detects an SI (GB) reciprocal without assuming binary', () => {
    // 32 GB priced at 8 GB-hours per CU — not a clean binary count.
    expect(formatUnitPrice(ratePerSecond(32, 8), 'bytes', 32 * 1e9)).toBe('8 GB-hours/CU')
  })

  it('prefers the largest unit (smallest count) when several are clean', () => {
    // 1 TiB-hour per CU also reads as 1024 GiB-hours; TiB should win.
    expect(formatUnitPrice(ratePerSecond(1, 1), 'bytes', TIB)).toBe('1 TiB-hour/CU')
  })

  it('falls back to a direct rate when no reciprocal is clean', () => {
    // 0.072 CU/hour/GiB reciprocates to 13.9 GiB-hours — not whole.
    const rate = (0.072 * 64 * 1_000_000) / 3600 // 64 GiB total
    expect(formatUnitPrice(rate, 'bytes', 64 * GIB)).toBe('0.072 CU/hour / GiB')
  })

  it('does not snap a near-integer reciprocal (e.g. 138.9)', () => {
    // 2 µCU/(TiB·s) → ~139 TiB-hours/CU but not actually whole → direct rate.
    expect(formatUnitPrice(2 * (500 / 1024), 'bytes', 500 * GIB)).toContain('CU/hour / GiB')
  })

  it('returns null when nothing was requested', () => {
    expect(formatUnitPrice(0, 'bytes', 0)).toBeNull()
  })
})
