import type { ComponentProps } from 'react'
import { cn } from '@/lib/utils'

/** Simple div-based progress bar. `value` is a fraction in 0..1. */
function Progress({
  className,
  value = 0,
  ...props
}: Omit<ComponentProps<'div'>, 'children'> & { value?: number }) {
  const pct = Math.max(0, Math.min(1, value)) * 100
  return (
    <div
      data-slot="progress"
      role="progressbar"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(pct)}
      className={cn('relative h-2 w-full overflow-hidden rounded-full bg-muted', className)}
      {...props}
    >
      <div
        data-slot="progress-indicator"
        className="h-full rounded-full bg-primary transition-[width]"
        style={{ width: `${pct}%` }}
      />
    </div>
  )
}

export { Progress }
