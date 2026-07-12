import type { LogEntry, LogLevel } from '@/api/types'
import { formatTimeOfDayUs } from '@/lib/format'
import { cn } from '@/lib/utils'
import { Skeleton } from '@/components/ui/skeleton'

export interface LogViewerProps {
  entries: LogEntry[]
  loading?: boolean
  emptyText?: string
  className?: string
}

const LEVEL_CLASS: Record<LogLevel, string> = {
  error: 'text-red-600 dark:text-red-400',
  warn: 'text-amber-600 dark:text-amber-400',
  info: 'text-foreground',
  debug: 'text-muted-foreground',
  trace: 'text-muted-foreground/70',
}

/** Scrollable mono log block with per-level coloring, loading + empty states. */
export function LogViewer({
  entries,
  loading = false,
  emptyText = 'No log entries.',
  className,
}: LogViewerProps) {
  return (
    <div
      className={cn(
        'max-h-96 overflow-auto rounded-md border bg-muted/30 p-3 font-mono text-xs leading-relaxed',
        className,
      )}
    >
      {loading ? (
        <div className="space-y-1.5">
          {Array.from({ length: 6 }).map((_, i) => (
            <Skeleton key={i} className="h-3.5" style={{ width: `${90 - i * 8}%` }} />
          ))}
        </div>
      ) : entries.length === 0 ? (
        <p className="py-6 text-center text-muted-foreground">{emptyText}</p>
      ) : (
        <ul className="space-y-0.5">
          {entries.map((e, i) => (
            <li key={i} className="flex gap-2">
              <span className="shrink-0 tabular-nums text-muted-foreground">
                {formatTimeOfDayUs(e.tUs)}
              </span>
              <span
                className={cn('w-10 shrink-0 font-medium uppercase', LEVEL_CLASS[e.level])}
                title={e.level}
              >
                {e.level}
              </span>
              <span className="shrink-0 truncate text-muted-foreground/80 max-w-[10rem]">
                {e.target}
              </span>
              <span className="min-w-0 whitespace-pre-wrap break-words text-foreground">
                {e.message}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
