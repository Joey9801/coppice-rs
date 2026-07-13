import type { AccrualView } from '@/api/types'
import { formatBytes, formatCpu, formatPercent, formatTimeUntil, shortId } from '@/lib/format'
import { IdLink, StatePill } from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'

/** Funding progress of the accruing allocation (ADR 0027). */
export function JobAccrualPanel({ accrual }: { accrual: AccrualView }) {
  const { allocation } = accrual

  const dims = [
    {
      label: 'CPU',
      fraction: accrual.fundedFraction.cpu,
      funded: formatCpu(allocation.funded.cpuMillis),
      requested: formatCpu(allocation.requested.cpuMillis),
    },
    {
      label: 'Memory',
      fraction: accrual.fundedFraction.memory,
      funded: formatBytes(allocation.funded.memoryBytes),
      requested: formatBytes(allocation.requested.memoryBytes),
    },
    {
      label: 'Disk',
      fraction: accrual.fundedFraction.disk,
      funded: formatBytes(allocation.funded.diskBytes),
      requested: formatBytes(allocation.requested.diskBytes),
    },
  ]

  return (
    <Card>
      <CardHeader className="p-4 pb-0">
        <CardTitle className="text-sm">Waiting for capacity</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6 p-4">
        <p className="text-sm text-foreground">
          Accruing capacity on <IdLink id={allocation.node} /> — allocation{' '}
          <span className="font-mono text-xs">{shortId(allocation.id)}</span>{' '}
          <StatePill state={allocation.state} />, commit seq{' '}
          <span className="tabular-nums">{allocation.seq}</span>
        </p>

        <div className="space-y-4">
          {dims.map((dim) => (
            <div key={dim.label} className="space-y-1">
              <div className="flex items-baseline justify-between text-xs">
                <span className="font-medium text-foreground">{dim.label}</span>
                <span className="tabular-nums text-muted-foreground">
                  funded {dim.funded} of {dim.requested} ({formatPercent(dim.fraction)})
                </span>
              </div>
              <Progress value={dim.fraction} />
            </div>
          ))}
        </div>

        <div className="text-sm">
          <span className="text-muted-foreground">Projected start: </span>
          {accrual.projectedStartUs != null ? (
            <span className="text-foreground">{formatTimeUntil(accrual.projectedStartUs)}</span>
          ) : (
            <span className="text-amber-600 dark:text-amber-400">
              unbounded — no guaranteed release covers this yet
            </span>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
