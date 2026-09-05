import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import type { LogController } from '@/api/log-controller'
import type { LogEntry, LogLevel } from '@/api/types'
import { formatTimeOfDay } from '@/lib/format'
import { cn } from '@/lib/utils'

export interface LogViewerProps {
  controller?: LogController
  /** Static previews may supply entries directly. */
  entries?: LogEntry[]
  structured?: boolean
  loading?: boolean
  emptyText?: string
  className?: string
}
const severity: Record<LogLevel, number> = { trace: 0, debug: 1, info: 2, warn: 3, error: 4 }

/** Presentation only: fetching and directional history belong to the controller. */
export function LogViewer({
  controller,
  entries = [],
  structured = false,
  loading = false,
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
  const all = controller?.data.entries ?? entries
  const visible = structured
    ? all.filter((entry) => entry.level && severity[entry.level] >= severity[level])
    : all
  const busy = controller?.loading ?? loading
  const unsupported = controller?.data.unsupported
  const windowKey = `${controller?.mode}:${controller?.limit}`

  useLayoutEffect(() => {
    const element = viewport.current
    if (!element) return
    if (lastWindow.current !== windowKey) {
      following.current = controller?.mode !== 'head'
      lastWindow.current = windowKey
      anchor.current = null
    }
    if (anchor.current && !busy) {
      element.scrollTop = anchor.current.top + element.scrollHeight - anchor.current.height
      anchor.current = null
    } else if (following.current && (controller?.playing ?? true) && controller?.mode !== 'head') {
      element.scrollTop = element.scrollHeight
    }
  }, [all, busy, controller?.playing, controller?.mode, windowKey])

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
    void controller?.loadOlder()
  }
  const content = (
    <section
      aria-label="Log viewer"
      className={cn('min-w-0 rounded-md border bg-background p-3 text-xs', className)}
    >
      <div className="mb-3 flex flex-wrap items-center gap-3 [&_button]:rounded [&_button]:border [&_button]:px-2 [&_button]:py-1 [&_button:disabled]:opacity-40 [&_button[aria-pressed=true]]:bg-muted [&_select]:rounded [&_select]:border [&_select]:p-1">
        {controller && (
          <>
            <button
              disabled={unsupported || busy}
              aria-pressed={controller.mode === 'head'}
              onClick={() => {
                following.current = false
                controller.setMode('head')
              }}
            >
              Head
            </button>
            <button
              disabled={unsupported || busy}
              aria-pressed={controller.mode === 'tail'}
              onClick={() => {
                following.current = true
                controller.setMode('tail')
              }}
            >
              Tail
            </button>
            <label title="Job output is paged in captured chunks; a chunk may contain multiple lines.">
              Entries{' '}
              <select
                aria-label="Entries per page"
                disabled={unsupported || busy}
                value={controller.limit}
                onChange={(event) => controller.setLimit(Number(event.target.value))}
              >
                {[50, 100, 200, 500, 1000].map((n) => (
                  <option key={n}>{n}</option>
                ))}
              </select>
            </label>
            <button
              disabled={unsupported || (!controller.data.live && !controller.hasNewer)}
              onClick={controller.togglePlaying}
              aria-label={controller.playing ? 'Pause log updates' : 'Resume log updates'}
            >
              {controller.playing ? 'Pause' : 'Play'}
            </button>
          </>
        )}
        <label className="flex items-center gap-1">
          <input
            type="checkbox"
            checked={timestamps}
            onChange={(event) => setTimestamps(event.target.checked)}
          />
          Timestamps
        </label>
        {structured && (
          <label title="Inclusive threshold: warn includes error; info adds info; debug adds debug.">
            Verbosity{' '}
            <select
              aria-label="Verbosity"
              disabled={unsupported}
              value={level}
              onChange={(event) => setLevel(event.target.value as LogLevel)}
            >
              <option value="warn">Warn + error</option>
              <option value="info">Info and above</option>
              <option value="debug">Debug and above</option>
            </select>
          </label>
        )}
        <button
          ref={maximize}
          onClick={maximized ? restore : toggleMaximize}
          aria-label={maximized ? 'Restore log viewer' : 'Maximize log viewer'}
        >
          {maximized ? 'Restore' : 'Maximize'}
        </button>
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
            {controller &&
              (controller.hasOlder ? (
                <button disabled={busy} onClick={older}>
                  Load older lines
                </button>
              ) : (
                <p className="text-muted-foreground">Start of available history</p>
              ))}
            {visible.length === 0 && !busy && (
              <p className="py-6 text-center">
                {all.length ? 'No entries match this verbosity.' : emptyText}
              </p>
            )}
            <ul className="space-y-0.5">
              {visible.map((entry) => (
                <li
                  key={entry.id ?? `${entry.t.toISOString()}:${entry.target}:${entry.message}`}
                  className="flex flex-wrap gap-x-2 sm:flex-nowrap"
                >
                  {timestamps && (
                    <span className="shrink-0 text-muted-foreground">
                      {formatTimeOfDay(entry.t)}
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
            {controller &&
              (controller.hasNewer ? (
                <button disabled={busy} onClick={() => void controller.loadNewer()}>
                  Load newer lines
                </button>
              ) : (
                <p className="text-muted-foreground">End of available output</p>
              ))}
          </div>
          <p role="status" className="mt-2 text-muted-foreground">
            {busy
              ? 'Loading logs…'
              : controller
                ? controller.playing && controller.data.live
                  ? 'Live · polling every 2 seconds'
                  : controller.data.live
                    ? 'Paused · new output may be available'
                    : 'Source finished'
                : ''}
          </p>
          {controller?.error && (
            <p role="alert">
              {controller.error}{' '}
              <button disabled={busy} onClick={() => void controller.retry()}>
                Retry
              </button>
            </p>
          )}
          {controller?.data.sources?.map((source) => (
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
