import { describe, expect, it } from 'vitest'
import { capacityCoverageNote, formatUsedTooltip } from './capacity-coverage'

describe('capacityCoverageNote', () => {
  it('is null when every node is reporting (full coverage)', () => {
    expect(capacityCoverageNote(16, 16)).toBeNull()
  })

  it('reads "no nodes reporting usage" when nothing is reporting', () => {
    expect(capacityCoverageNote(0, 16)).toBe('no nodes reporting usage')
  })

  it('reads "used: n/total nodes reporting" for partial coverage', () => {
    expect(capacityCoverageNote(15, 16)).toBe('used: 15/16 nodes reporting')
  })
})

describe('formatUsedTooltip', () => {
  it('reads "not reported" for a null value (coverage gap)', () => {
    expect(formatUsedTooltip(null, 0, 16)).toBe('not reported')
  })

  it('annotates coverage when the sample is partial', () => {
    expect(formatUsedTooltip(1_500, 3, 16)).toBe('1.5 cores (3/16 nodes reporting)')
  })

  it('has no annotation when coverage is complete', () => {
    expect(formatUsedTooltip(1_500, 16, 16)).toBe('1.5 cores')
  })
})
