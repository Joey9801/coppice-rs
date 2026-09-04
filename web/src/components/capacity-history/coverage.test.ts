import { describe, expect, it } from 'vitest'
import { capacityCoverageNote, coverageSuffix } from './coverage'

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

describe('coverageSuffix', () => {
  it('annotates a partial reading', () => {
    expect(coverageSuffix(3, 16)).toBe('3/16 nodes reporting')
  })

  it('has nothing to say at full coverage', () => {
    expect(coverageSuffix(16, 16)).toBeNull()
  })

  it('has nothing to say for a single node, which carries no counts', () => {
    expect(coverageSuffix(null, null)).toBeNull()
  })
})
