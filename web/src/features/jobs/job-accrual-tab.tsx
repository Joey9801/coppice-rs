import type { AccrualView } from '@/api/types'
import { formatBytes, formatCpu, formatPercent, formatTimeUntil, shortId } from '@/lib/format'
import { IdLink, KeyValueGrid, StatePill } from '@/components'
import { Progress } from '@/components/ui/progress'

export function JobAccrualTab({ accrual }: { accrual: AccrualView }) {
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
    <div className="space-y-6">
      <p className="text-sm text-foreground">
        Waiting for capacity on <IdLink id={allocation.node} />
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

      <div>
        <h3 className="mb-2 text-sm font-medium text-foreground">Allocation</h3>
        <KeyValueGrid
          items={[
            {
              label: 'Id',
              value: <span className="font-mono text-xs">{shortId(allocation.id)}</span>,
            },
            { label: 'State', value: <StatePill state={allocation.state} /> },
            { label: 'Commit seq', value: <span className="tabular-nums">{allocation.seq}</span> },
          ]}
        />
      </div>
    </div>
  )
}
