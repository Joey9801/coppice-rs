import type { ReactNode } from 'react'
import { cn } from '@/lib/utils'

export interface KeyValueGridProps {
  items: Array<{ label: string; value: ReactNode }>
  className?: string
}

/** Two-column definition list for spec / detail panels. */
export function KeyValueGrid({ items, className }: KeyValueGridProps) {
  return (
    <dl
      className={cn(
        'grid grid-cols-[auto_1fr] gap-x-4 gap-y-2.5 text-sm sm:grid-cols-[minmax(0,10rem)_1fr]',
        className,
      )}
    >
      {items.map((item, i) => (
        <div key={i} className="col-span-2 grid grid-cols-subgrid items-baseline">
          <dt className="text-muted-foreground">{item.label}</dt>
          <dd className="min-w-0 text-foreground">{item.value}</dd>
        </div>
      ))}
    </dl>
  )
}
