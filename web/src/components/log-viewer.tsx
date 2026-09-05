import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import type { LogController } from '@/api/log-controller'
import type { LogLevel } from '@/api/types'
import { formatTimeOfDay } from '@/lib/format'
import { cn } from '@/lib/utils'
import { Button } from './ui/button'
import { Select } from './ui/select'

export interface LogViewerProps {
  controller: LogController
  structured?: boolean
  emptyText?: string
  className?: string
}
const severity: Record<LogLevel, number> = { trace: 0, debug: 1, info: 2, warn: 3, error: 4 }

/** Presentation only: fetching and directional history belong to the controller. */
export function LogViewer({
  controller,
  structured = false,
  emptyText = 'No log entries.',
  className,
}: LogViewerProps) {
  const [timestamps, setTimestamps] = useState(true)
  const [level, setLevel] = useState<LogLevel>('info')
  const [maximized, setMaximized] = useState(false)
  const viewport = useRef<HTMLDivElement>(null)
  const dialog = useRef<HTMLDialogElement>(null)
  const maximize = useRef<HTMLButtonElement>(null)
  const following = useRef(true)
  const anchor = useRef<{ height: number; top: number } | null>(null)
  const lastWindow = useRef('')
  const scrollTop = useRef(0)
  const all = controller.data.unsupported ? [] : controller.data.entries
  const visible = structured
    ? all.filter((entry) => entry.level && severity[entry.level] >= severity[level])
    : all
  const busy = controller.loading
  const { data } = controller
  const unsupported = data.unsupported
  const windowKey = `${controller.mode}:${controller.limit}`

  useLayoutEffect(() => {
    if (data.unsupported) return
    const element = viewport.current
    if (!element) return
    if (lastWindow.current !== windowKey) {
      following.current = controller.mode !== 'head'
      lastWindow.current = windowKey
      anchor.current = null
    }
    if (anchor.current && !busy) {
      element.scrollTop = anchor.current.top + element.scrollHeight - anchor.current.height
      anchor.current = null
    } else if (following.current && controller.playing && controller.mode !== 'head') {
      element.scrollTop = element.scrollHeight
    }
  }, [data, busy, controller.playing, controller.mode, windowKey])

  useEffect(() => {
    if (maximized) {
      dialog.current?.showModal()
      if (viewport.current) viewport.current.scrollTop = scrollTop.current
    } else {
      if (viewport.current) viewport.current.scrollTop = scrollTop.current
    }
  }, [maximized])

  const toggleMaximize = () => {
    scrollTop.current = viewport.current?.scrollTop ?? 0
    setMaximized((value) => !value)
  }
  const restore = () => {
    scrollTop.current = viewport.current?.scrollTop ?? 0
    setMaximized(false)
    requestAnimationFrame(() => maximize.current?.focus())
  }
  const older = () => {
    const element = viewport.current
    if (element) anchor.current = { height: element.scrollHeight, top: element.scrollTop }
    following.current = false
    void controller.loadOlder()
  }
  const content = (
    <section
      aria-label="Log viewer"
      className={cn('min-w-0 rounded-md border bg-background p-3 text-xs', className)}
    >
      <div className="mb-3 flex flex-wrap items-center gap-3">
        <Button
          variant={controller.mode === 'head' ? 'secondary' : 'outline'}
          size="sm"
          disabled={unsupported || busy}
          aria-pressed={controller.mode === 'head'}
          onClick={() => {
            if (controller.mode === 'head') return
            following.current = false
            controller.setMode('head')
          }}
        >
          Head
        </Button>
        <Button
          variant={controller.mode === 'tail' ? 'secondary' : 'outline'}
          size="sm"
          disabled={unsupported || busy}
          aria-pressed={controller.mode === 'tail'}
          onClick={() => {
            if (controller.mode === 'tail') return
            following.current = true
            controller.setMode('tail')
          }}
        >
          Tail
        </Button>
        <label
          className="flex items-center gap-2"
          title="Job output is paged in captured chunks; a chunk may contain multiple lines."
        >
          Entries{' '}
          <Select
            aria-label="Entries per page"
            disabled={unsupported || busy}
            value={controller.limit}
            onChange={(event) => controller.setLimit(Number(event.target.value))}
          >
            {[50, 100, 200, 500, 1000].map((n) => (
              <option key={n}>{n}</option>
            ))}
          </Select>
        </label>
        <Button
          variant="outline"
          size="sm"
          disabled={unsupported || !controller.hasNewer}
          onClick={controller.togglePlaying}
          aria-label={controller.playing ? 'Pause log updates' : 'Resume log updates'}
        >
          {controller.playing ? 'Pause' : 'Play'}
        </Button>
        <label className="flex items-center gap-1">
          <input
            type="checkbox"
            checked={timestamps}
            onChange={(event) => setTimestamps(event.target.checked)}
          />
          Timestamps
        </label>
        {structured && (
          <label
            className="flex items-center gap-2"
            title="Inclusive threshold: warn includes error; info adds info; debug adds debug."
          >
            Verbosity{' '}
            <Select
              aria-label="Verbosity"
              disabled={unsupported}
              value={level}
              onChange={(event) => setLevel(event.target.value as LogLevel)}
            >
              <option value="warn">Warn + error</option>
              <option value="info">Info and above</option>
              <option value="debug">Debug and above</option>
            </Select>
          </label>
        )}
        <Button
          variant="outline"
          size="sm"
          ref={maximize}
          onClick={maximized ? restore : toggleMaximize}
          aria-label={maximized ? 'Restore log viewer' : 'Maximize log viewer'}
        >
          {maximized ? 'Restore' : 'Maximize'}
        </Button>
      </div>
      {unsupported ? (
        <p role="status">Log collection is not available for this source.</p>
      ) : (
        <>
          <div
            ref={viewport}
            tabIndex={0}
            aria-label="Log output"
            className={cn(
              'overflow-auto bg-muted/30 p-2 font-mono leading-relaxed',
              maximized ? 'h-[70dvh]' : 'max-h-96',
            )}
            onScroll={(event) => {
              const element = event.currentTarget
              following.current =
                element.scrollHeight - element.scrollTop - element.clientHeight < 24
            }}
          >
            {controller.hasOlder ? (
              <Button variant="outline" size="sm" disabled={busy} onClick={older}>
                Load older lines
              </Button>
            ) : (
              <p className="text-muted-foreground">Start of available history</p>
            )}
            {visible.length === 0 && !busy && (
              <p className="py-6 text-center">
                {all.length ? 'No entries match this verbosity.' : emptyText}
              </p>
            )}
            <ul className="space-y-0.5">
              {visible.map((entry) => (
                <li key={entry.id} className="flex flex-wrap gap-x-2 sm:flex-nowrap">
                  {timestamps && (
                    <span className="shrink-0 text-muted-foreground">
                      {formatTimeOfDay(new Date(entry.at))}
                    </span>
                  )}
                  {structured && entry.level && (
                    <span
                      className={cn(
                        'shrink-0 uppercase',
                        severity[entry.level] >= 3 && 'text-amber-600',
                      )}
                    >
                      {entry.level}
                    </span>
                  )}
                  <span
                    className="max-w-40 shrink-0 truncate text-muted-foreground"
                    title={entry.attempt ?? entry.target}
                  >
                    {entry.stream ?? entry.target}
                  </span>
                  <span className="w-full min-w-0 whitespace-pre-wrap break-words sm:w-auto sm:flex-1">
                    {entry.message}
                    {entry.truncated && <em> [output truncated]</em>}
                  </span>
                </li>
              ))}
            </ul>
            {controller.hasNewer ? (
              <Button
                variant="outline"
                size="sm"
                disabled={busy}
                onClick={() => void controller.loadNewer()}
              >
                Load newer lines
              </Button>
            ) : (
              <p className="text-muted-foreground">End of available output</p>
            )}
          </div>
          <p role="status" className="mt-2 text-muted-foreground">
            {busy
              ? 'Loading logs…'
              : data.live
                ? controller.playing
                  ? 'Live · polling every 2 seconds'
                  : 'Paused · new output may be available'
                : 'Source finished'}
          </p>
          {controller.error && (
            <p role="alert">
              {controller.error}{' '}
              <Button
                variant="outline"
                size="sm"
                disabled={busy}
                onClick={() => void controller.retry()}
              >
                Retry
              </Button>
            </p>
          )}
          {data.sources.map((source) => (
            <p key={source.attempt} className="mt-1 text-muted-foreground">
              {source.attempt} · {source.availability}
              {source.truncated ? ' · older output expired' : ''}
              {source.reason ? ` · ${source.reason}` : ''}
            </p>
          ))}
        </>
      )}
    </section>
  )
  return maximized ? (
    <dialog
      ref={dialog}
      aria-label="Maximized log viewer"
      className="m-auto w-[96vw] max-w-none bg-background p-2 text-foreground backdrop:bg-black/50"
      onCancel={(event) => {
        event.preventDefault()
        restore()
      }}
    >
      {content}
    </dialog>
  ) : (
    content
  )
}
