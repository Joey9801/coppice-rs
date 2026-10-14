import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useNow } from './use-now'

describe('useNow', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('advances after each interval', () => {
    const { result } = renderHook(() => useNow(1000))
    const first = result.current.getTime()
    expect(first).toBe(new Date('2026-01-01T00:00:00Z').getTime())

    act(() => {
      vi.advanceTimersByTime(1000)
    })
    expect(result.current.getTime()).toBe(first + 1000)

    act(() => {
      vi.advanceTimersByTime(2000)
    })
    expect(result.current.getTime()).toBe(first + 3000)
  })

  it('clears its interval on unmount', () => {
    const { unmount } = renderHook(() => useNow(1000))
    expect(vi.getTimerCount()).toBe(1)
    unmount()
    expect(vi.getTimerCount()).toBe(0)
  })

  it('re-subscribes when the interval changes', () => {
    const { result, rerender } = renderHook(({ ms }) => useNow(ms), {
      initialProps: { ms: 1000 },
    })
    rerender({ ms: 5000 })
    expect(vi.getTimerCount()).toBe(1)
    const start = result.current.getTime()
    act(() => {
      vi.advanceTimersByTime(1000)
    })
    expect(result.current.getTime()).toBe(start)
    act(() => {
      vi.advanceTimersByTime(4000)
    })
    expect(result.current.getTime()).toBe(start + 5000)
  })
})
