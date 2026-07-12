import { cn } from '@/lib/utils'

export interface ResourceBarProps {
  label: string
  capacity: number
  allocated?: number
  used?: number
  format: (n: number) => string
  className?: string
}

/**
 * Horizontal capacity bar: the track is `capacity`, a translucent fill is
 * `allocated` (reserved), and a solid fill is `used` (measured). Widths are
 * clamped so an over-committed node still reads sensibly.
 */
export function ResourceBar({
  label,
  capacity,
  allocated,
  used,
  format,
  className,
}: ResourceBarProps) {
  const pct = (n: number | undefined) =>
    n == null || capacity <= 0 ? 0 : Math.max(0, Math.min(1, n / capacity)) * 100

  const caption = [
    used != null ? `${format(used)} used` : null,
    allocated != null ? `${format(allocated)} alloc` : null,
    `${format(capacity)} cap`,
  ]
    .filter(Boolean)
    .join(' / ')

  return (
    <div className={cn('space-y-1', className)}>
      <div className="flex items-baseline justify-between gap-2 text-xs">
        <span className="font-medium text-foreground">{label}</span>
        <span className="tabular-nums text-muted-foreground">{caption}</span>
      </div>
      <div className="relative h-2 w-full overflow-hidden rounded-full bg-muted">
        {allocated != null ? (
          <div
            className="absolute inset-y-0 left-0 rounded-full bg-primary/30"
            style={{ width: `${pct(allocated)}%` }}
          />
        ) : null}
        {used != null ? (
          <div
            className="absolute inset-y-0 left-0 rounded-full bg-primary"
            style={{ width: `${pct(used)}%` }}
          />
        ) : null}
      </div>
    </div>
  )
}
