import { useCallback, useEffect, useRef, useState } from 'react'
import type { LogChunk, LogEntry, LogRequest, LogSource } from './types'

export type LogFetcher = (cursor: string | null, request: LogRequest) => Promise<LogChunk>
type Direction = 'initial' | 'older' | 'newer'

/** Merge identities, never content: identical repeated writes are separate entries. */
export function mergeLogs(previous: LogEntry[], incoming: LogEntry[]): LogEntry[] {
  const merged = new Map(previous.map((entry) => [entry.id, entry]))
  for (const entry of incoming) {
    const existing = merged.get(entry.id)
    if (!existing || existing.truncated || !entry.truncated) merged.set(entry.id, entry)
  }
  return [...merged.values()].sort(
    (a, b) =>
      (a.attempt ?? '').localeCompare(b.attempt ?? '') ||
      Date.parse(a.at ?? a.t.toISOString()) - Date.parse(b.at ?? b.t.toISOString()) ||
      (a.at?.match(/\.(\d+)/)?.[1] ?? '')
        .padEnd(6, '0')
        .localeCompare((b.at?.match(/\.(\d+)/)?.[1] ?? '').padEnd(6, '0')) ||
      (a.id ?? '').localeCompare(b.id ?? '', undefined, { numeric: true }),
  )
}

function mergeSources(previous: LogSource[], incoming: LogSource[]): LogSource[] {
  const sources = new Map(previous.map((source) => [source.attempt, source]))
  for (const source of incoming) {
    sources.set(source.attempt, {
      ...source,
      truncated: source.truncated || Boolean(sources.get(source.attempt)?.truncated),
    })
  }
  return [...sources.values()]
}

/** One bounded request at a time. Generation guards isolate source/window changes. */
export function useLogController(fetchPage: LogFetcher) {
  const [mode, setMode] = useState<'head' | 'tail'>('tail')
  const [limit, setLimit] = useState(200)
  const [windowVersion, setWindowVersion] = useState(0)
  const [playing, setPlaying] = useState(true)
  const [data, setData] = useState<LogChunk>({ entries: [], nextCursor: null })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [revision, setRevision] = useState(0)
  const [boundaries, setBoundaries] = useState({ older: false, newer: false })
  const failedDirection = useRef<Direction>('initial')
  const state = useRef({
    generation: 0,
    busy: false,
    older: null as string | null,
    newer: null as string | null,
    from: undefined as string | undefined,
    hasNewer: false,
    playing: true,
    pauseEpoch: 0,
  })

  const load = useCallback(
    async (direction: Direction, automatic = false) => {
      const current = state.current
      if (current.busy) return
      current.busy = true
      const generation = current.generation
      const pauseEpoch = current.pauseEpoch
      setLoading(true)
      setError(null)
      failedDirection.current = direction
      const order =
        direction === 'older' || (direction === 'initial' && mode === 'tail') ? 'desc' : 'asc'
      const cursor =
        direction === 'initial' ? null : direction === 'older' ? current.older : current.newer
      try {
        const page = await fetchPage(cursor, {
          order,
          limit,
          ...(direction === 'newer' && current.from ? { from: current.from } : {}),
        })
        if (
          state.current !== current ||
          generation !== current.generation ||
          (automatic && pauseEpoch !== current.pauseEpoch)
        )
          return
        if (page.entries.some((entry) => !entry.id))
          throw new Error(
            'Stable log identities unavailable. Update the log source to enable this viewer.',
          )
        if (direction === 'initial') {
          current.older = mode === 'tail' ? page.nextCursor : null
          current.newer = mode === 'head' ? (page.nextCursor ?? page.resumeCursor ?? null) : null
          current.from = mode === 'tail' ? page.entries.at(-1)?.at : undefined
          // A tail needs a forward probe even after completion: data may arrive between reads.
          current.hasNewer = mode === 'tail' || page.nextCursor !== null
        } else if (direction === 'older') current.older = page.nextCursor
        else {
          current.newer = page.nextCursor ?? page.resumeCursor ?? current.newer
          current.hasNewer = page.nextCursor !== null
        }
        setBoundaries({ older: current.older !== null, newer: current.hasNewer })
        setData((previous) => ({
          ...page,
          entries:
            direction === 'initial' ? page.entries : mergeLogs(previous.entries, page.entries),
          sources: mergeSources(
            direction === 'initial' ? [] : (previous.sources ?? []),
            page.sources ?? [],
          ),
        }))
      } catch (reason) {
        if (state.current === current && (!automatic || pauseEpoch === current.pauseEpoch))
          setError(reason instanceof Error ? reason.message : 'Could not load logs.')
      } finally {
        if (state.current === current) {
          current.busy = false
          setLoading(false)
          setRevision((n) => n + 1)
        }
      }
    },
    [fetchPage, limit, mode],
  )

  useEffect(() => {
    const current = {
      generation: state.current.generation + 1,
      busy: false,
      older: null,
      newer: null,
      from: undefined,
      hasNewer: false,
      playing: state.current.playing,
      pauseEpoch: 0,
    }
    state.current = current
    // This effect synchronizes a new external source/window with the rendered snapshot.
    // oxlint-disable-next-line react/set-state-in-effect
    setData({ entries: [], nextCursor: null })
    setBoundaries({ older: false, newer: false })
    void load('initial')
    return () => {
      current.generation += 1
    }
  }, [load, windowVersion])

  useEffect(() => {
    if (!playing || loading || error || data.unsupported || (!data.live && !state.current.hasNewer))
      return
    const timer = setTimeout(() => {
      void load('newer', true)
    }, 2000)
    return () => clearTimeout(timer)
  }, [playing, loading, error, data, load, revision])

  const togglePlaying = () => {
    state.current.playing = !state.current.playing
    state.current.pauseEpoch += 1
    setPlaying(state.current.playing)
  }
  return {
    data,
    loading,
    error,
    mode,
    setMode: (next: 'head' | 'tail') => {
      setMode(next)
      setWindowVersion((version) => version + 1)
    },
    limit,
    setLimit,
    playing,
    togglePlaying,
    hasOlder: boundaries.older,
    hasNewer: boundaries.newer || Boolean(data.live),
    loadOlder: () => load('older'),
    loadNewer: () => load('newer'),
    retry: () => load(failedDirection.current),
  }
}
export type LogController = ReturnType<typeof useLogController>
