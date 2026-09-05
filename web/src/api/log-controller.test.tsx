import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { mergeLogs, useLogController } from './log-controller'
import type { LogChunk, LogEntry } from './types'

const entry = (id: string, micros = id): LogEntry => ({
  id,
  at: `2026-01-01T00:00:00.${micros.padStart(6, '0')}Z`,
  t: new Date(0),
  target: 'test',
  message: 'same output',
})
const page = (entries: LogEntry[], extra: Partial<LogChunk> = {}): LogChunk => ({
  entries,
  nextCursor: null,
  live: true,
  ...extra,
})
async function flush() {
  await act(async () => {
    await Promise.resolve()
  })
}
afterEach(() => vi.useRealTimers())

describe('log controller', () => {
  it('merges by identity, preserves duplicate writes and microsecond order', () => {
    expect(mergeLogs([entry('2'), entry('3')], [entry('1'), entry('2')]).map((e) => e.id)).toEqual([
      '1',
      '2',
      '3',
    ])
  })

  it('never replaces a complete chunk with an overlapping truncated prefix', () => {
    const whole = { ...entry('1'), message: 'whole output', truncated: false }
    const prefix = { ...whole, message: 'whole', truncated: true }
    expect(mergeLogs([whole], [prefix])).toEqual([whole])
    expect(mergeLogs([prefix], [whole])).toEqual([whole])
  })

  it('defaults to tail and uses independent older/newer cursors, including after pause', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(page([entry('5'), entry('6')], { nextCursor: 'older' }))
      .mockResolvedValueOnce(page([entry('3'), entry('4')], { nextCursor: 'older2' }))
      .mockResolvedValueOnce(page([entry('6'), entry('7')], { nextCursor: 'newer' }))
      .mockResolvedValueOnce(page([entry('8')], { resumeCursor: 'watermark' }))
    const { result } = renderHook(() => useLogController(fetch))
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(fetch).toHaveBeenCalledWith(null, { order: 'desc', limit: 200 })
    act(() => result.current.togglePlaying())
    expect(result.current.hasOlder).toBe(true)
    expect(result.current.hasNewer).toBe(true)
    await act(() => result.current.loadOlder())
    expect(fetch).toHaveBeenLastCalledWith('older', { order: 'desc', limit: 200 })
    await act(() => result.current.loadNewer())
    expect(fetch).toHaveBeenLastCalledWith(null, { order: 'asc', limit: 200, from: entry('6').at })
    await act(() => result.current.loadNewer())
    expect(fetch).toHaveBeenLastCalledWith('newer', {
      order: 'asc',
      limit: 200,
      from: entry('6').at,
    })
    expect(result.current.data.entries.map((e) => e.id)).toEqual(['3', '4', '5', '6', '7', '8'])
    expect(result.current.hasOlder).toBe(true)
  })

  it('reselecting Tail refreshes the window after reading history', async () => {
    const fetch = vi.fn().mockResolvedValue(page([entry('1')], { live: false }))
    const { result } = renderHook(() => useLogController(fetch))
    await waitFor(() => expect(result.current.loading).toBe(false))
    act(() => result.current.setMode('tail'))
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2))
    expect(fetch).toHaveBeenLastCalledWith(null, { order: 'desc', limit: 200 })
  })

  it('head and page size request a fresh window and stop polling completed sources', async () => {
    const fetch = vi.fn().mockResolvedValue(page([entry('1')], { live: false }))
    const { result } = renderHook(() => useLogController(fetch))
    await waitFor(() => expect(result.current.loading).toBe(false))
    act(() => result.current.setMode('head'))
    await waitFor(() => expect(fetch).toHaveBeenLastCalledWith(null, { order: 'asc', limit: 200 }))
    act(() => result.current.setLimit(50))
    await waitFor(() => expect(fetch).toHaveBeenLastCalledWith(null, { order: 'asc', limit: 50 }))
    expect(result.current.hasOlder).toBe(false)
    expect(result.current.hasNewer).toBe(false)
  })

  it('pauses polling, ignores an in-flight poll, resumes from a high-water mark and never overlaps', async () => {
    vi.useFakeTimers()
    let resolve!: (value: LogChunk) => void
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(page([entry('1')]))
      .mockImplementationOnce(
        () =>
          new Promise<LogChunk>((done) => {
            resolve = done
          }),
      )
      .mockResolvedValue(page([entry('2')], { resumeCursor: 'watermark' }))
    const { result } = renderHook(() => useLogController(fetch))
    await flush()
    await act(async () => vi.advanceTimersByTime(2000))
    await act(async () => vi.advanceTimersByTime(20000))
    expect(fetch).toHaveBeenCalledTimes(2)
    act(() => result.current.togglePlaying())
    await act(async () => resolve(page([entry('2')])))
    expect(result.current.data.entries.map((e) => e.id)).toEqual(['1'])
    await act(async () => vi.advanceTimersByTime(20000))
    expect(fetch).toHaveBeenCalledTimes(2)
    act(() => result.current.togglePlaying())
    await act(async () => vi.advanceTimersByTime(2000))
    expect(result.current.data.entries.map((e) => e.id)).toEqual(['1', '2'])
    await act(async () => vi.advanceTimersByTime(2000))
    expect(fetch).toHaveBeenLastCalledWith('watermark', expect.objectContaining({ order: 'asc' }))
  })

  it('ignores an old source response, preserves data on errors, and stops automatic retries', async () => {
    vi.useFakeTimers()
    let resolve!: (value: LogChunk) => void
    const old = vi.fn(
      () =>
        new Promise<LogChunk>((done) => {
          resolve = done
        }),
    )
    const fresh = vi
      .fn()
      .mockResolvedValueOnce(page([entry('9')]))
      .mockRejectedValue(new Error('offline'))
    const { result, rerender } = renderHook(({ fetch }) => useLogController(fetch), {
      initialProps: { fetch: old },
    })
    rerender({ fetch: fresh })
    await flush()
    await act(async () => resolve(page([entry('1')])))
    expect(result.current.data.entries[0]!.id).toBe('9')
    await act(async () => vi.advanceTimersByTime(2000))
    expect(result.current.error).toBe('offline')
    expect(result.current.data.entries[0]!.id).toBe('9')
    await act(async () => vi.advanceTimersByTime(20000))
    expect(fresh).toHaveBeenCalledTimes(2)
  })
})
