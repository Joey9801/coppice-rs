import type { ReactNode } from 'react'
import { Card } from '@/components/ui/card'

export interface StatTileProps {
  label: string
  value: ReactNode
  hint?: ReactNode
  children?: ReactNode
}

/** Dashboard KPI card. `children` renders under the value (e.g. a sparkline). */
export function StatTile({ label, value, hint, children }: StatTileProps) {
  return (
    <Card className="flex flex-col gap-1 p-4">
      <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        {label}
      </span>
      <span className="text-2xl font-semibold leading-tight tabular-nums text-foreground">
        {value}
      </span>
      {hint ? <span className="text-xs text-muted-foreground">{hint}</span> : null}
      {children ? <div className="mt-1">{children}</div> : null}
    </Card>
  )
}
