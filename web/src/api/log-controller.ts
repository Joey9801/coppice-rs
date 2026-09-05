import { useCallback, useEffect, useRef, useState } from 'react'
import type { LogChunk, LogEntry, LogPage, LogRequest, LogSource } from './types'

export type LogFetcher = (cursor: string | null, request: LogRequest) => Promise<LogChunk>
type Direction = 'initial' | 'older' | 'newer'
const emptyPage: LogPage = {
  entries: [],
  sources: [],
  nextCursor: null,
  resumeCursor: null,
  live: false,
}

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
      Date.parse(a.at) - Date.parse(b.at) ||
      // Date.parse drops micros, so compare the fractional part textually.
      (a.at.match(/\.(\d+)/)?.[1] ?? '')
        .padEnd(6, '0')
        .localeCompare((b.at.match(/\.(\d+)/)?.[1] ?? '').padEnd(6, '0')) ||
      a.id.localeCompare(b.id, undefined, { numeric: true }),
  )
}

function mergeSources(previous: LogSource[], incoming: LogSource[]): LogSource[] {
  const sources = new Map(previous.map((source) => [source.attempt, source]))
  for (const source of incoming)
    sources.set(source.attempt, {
      ...source,
      truncated: source.truncated || Boolean(sources.get(source.attempt)?.truncated),
    })
  return [...sources.values()]
}

function newSession(generation: number) {
  return {
    generation,
    pending: null as Promise<void> | null,
    older: null as string | null,
    newer: null as string | null,
    from: undefined as string | undefined,
    hasNewer: false,
    pauseEpoch: 0,
  }
}

/** Serialize page requests; manual loads wait for polls instead of being dropped. */
export function useLogController(fetchPage: LogFetcher) {
  const [mode, setMode] = useState<'head' | 'tail'>('tail')
  const [limit, setLimit] = useState(200)
  const [playing, setPlaying] = useState(true)
  const [data, setData] = useState<LogChunk>(emptyPage)
  const [loading, setLoading] = useState(false)
  const [polling, setPolling] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [revision, setRevision] = useState(0)
  const [boundaries, setBoundaries] = useState({ older: false, newer: false })
  const failedDirection = useRef<Direction>('initial')
  const state = useRef(newSession(0))

  const load = useCallback(
    async (direction: Direction, automatic = false) => {
      const current = state.current
      const generation = current.generation
      if (automatic && current.pending) return
      if (!automatic) {
        setLoading(true)
        // Prioritize the reader's request; don't append a poll while a prepend is queued.
        if (current.pending) current.pauseEpoch += 1
      }
      while (current.pending) await current.pending
      if (state.current !== current || generation !== current.generation) return
      let release!: () => void
      current.pending = new Promise<void>((resolve) => {
        release = resolve
      })
      const pauseEpoch = current.pauseEpoch
      if (automatic) setPolling(true)
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
        if (page.unsupported) {
          setData(page)
          return
        }
        if (direction === 'initial') {
          current.older = mode === 'tail' ? page.nextCursor : null
          current.newer = mode === 'head' ? (page.nextCursor ?? page.resumeCursor) : null
          current.from = mode === 'tail' ? page.entries.at(-1)?.at : undefined
          current.hasNewer = mode === 'tail' ? page.live : page.nextCursor !== null
        } else if (direction === 'older') current.older = page.nextCursor
        else {
          current.newer = page.nextCursor ?? page.resumeCursor ?? current.newer
          current.hasNewer = page.nextCursor !== null
        }
        setBoundaries({ older: current.older !== null, newer: current.hasNewer })
        setData((previous) =>
          direction === 'initial' || previous.unsupported
            ? page
            : {
                ...page,
                entries: mergeLogs(previous.entries, page.entries),
                sources: mergeSources(previous.sources, page.sources),
              },
        )
      } catch (reason) {
        if (
          state.current === current &&
          generation === current.generation &&
          (!automatic || pauseEpoch === current.pauseEpoch)
        )
          setError(reason instanceof Error ? reason.message : 'Could not load logs.')
      } finally {
        current.pending = null
        release()
        if (state.current === current && generation === current.generation) {
          if (automatic) setPolling(false)
          else setLoading(false)
          setRevision((n) => n + 1)
        }
      }
    },
    [fetchPage, limit, mode],
  )

  useEffect(() => {
    const current = newSession(state.current.generation + 1)
    state.current = current
    // Synchronize the new external source/window with its rendered snapshot.
    // oxlint-disable-next-line react/set-state-in-effect
    setData(emptyPage)
    setBoundaries({ older: false, newer: false })
    setPolling(false)
    void load('initial')
    return () => {
      current.generation += 1
    }
  }, [load])

  useEffect(() => {
    if (
      !playing ||
      loading ||
      polling ||
      error ||
      data.unsupported ||
      (!data.live && !boundaries.newer)
    )
      return
    const timer = setTimeout(() => {
      void load('newer', true)
    }, 2000)
    return () => clearTimeout(timer)
  }, [playing, loading, polling, error, data, load, revision, boundaries.newer])

  const togglePlaying = () => {
    state.current.pauseEpoch += 1
    setPlaying((value) => !value)
  }
  return {
    data,
    loading,
    polling,
    error,
    mode,
    setMode,
    limit,
    setLimit,
    playing,
    togglePlaying,
    hasOlder: boundaries.older,
    hasNewer: boundaries.newer || (!data.unsupported && data.live),
    loadOlder: () => load('older'),
    loadNewer: () => load('newer'),
    retry: () => load(failedDirection.current),
  }
}
export type LogController = ReturnType<typeof useLogController>
