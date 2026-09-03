import { describe, expect, it } from 'vitest'
import type { AttemptView } from '@/api/types'
import { attemptRuntimeSeconds } from './attempt-runtime'

const NOW = new Date('2026-01-01T00:10:00.000Z')
const START = '2026-01-01T00:00:00.000Z'
const END = '2026-01-01T00:05:00.000Z'

function attempt(
  overrides: Partial<Pick<AttemptView, 'state' | 'startedAt' | 'endedAt'>>,
): Pick<AttemptView, 'state' | 'startedAt' | 'endedAt'> {
  return { state: 'Running', startedAt: new Date(START), endedAt: null, ...overrides }
}

describe('attemptRuntimeSeconds', () => {
  it('measures a live attempt up to now, not past its start alone', () => {
    expect(attemptRuntimeSeconds(attempt({ state: 'Running' }), NOW)).toBe(600)
  })

  it('stops at the replicated end stamp for a terminal attempt', () => {
    expect(attemptRuntimeSeconds(attempt({ state: 'Terminal', endedAt: new Date(END) }), NOW)).toBe(
      300,
    )
  })

  it('never uses the browser clock for a terminal attempt without a stamp', () => {
    // Regression: this used to fall back to `now`, silently inflating a
    // finished attempt's runtime as the page stayed open.
    expect(attemptRuntimeSeconds(attempt({ state: 'Terminal', endedAt: null }), NOW)).toBeNull()
  })

  it('reports no runtime for an attempt that never started', () => {
    // Aborted before its attempt started: no start, no nonsensical runtime.
    expect(attemptRuntimeSeconds(attempt({ state: 'Terminal', startedAt: null }), NOW)).toBeNull()
    expect(attemptRuntimeSeconds(null, NOW)).toBeNull()
  })
})
