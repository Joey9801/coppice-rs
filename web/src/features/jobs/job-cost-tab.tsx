import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { Gauge } from 'lucide-react'
import type { CostReport, JobId, JobUsage } from '@/api/types'
import { useJobUsage } from '@/api/queries'
import { formatBytes, formatCpu, formatTimeOfDayUs, formatUcu } from '@/lib/format'
import { EmptyState, KeyValueGrid } from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { AXIS_TICK, TOOLTIP_CONTENT_STYLE } from './chart-theme'

/** µCU/second → "X CU/hour". */
function ratePerHour(rateUcuPerSecond: number): string {
  return `${formatUcu(rateUcuPerSecond * 3600)}/hour`
}

export function JobCostTab({ jobId, cost }: { jobId: JobId; cost: CostReport }) {
  const items = [
    { label: 'Rate', value: ratePerHour(cost.rateUcuPerSecond) },
    {
      label: 'Estimated',
      value: (
        <span>
          {formatUcu(cost.estimatedUcu)}
          {cost.estimateUsedDefaultRuntime ? (
            <span className="ml-1.5 text-xs text-muted-foreground">(policy default runtime)</span>
          ) : null}
        </span>
      ),
    },
    { label: 'Charged so far', value: formatUcu(cost.chargedUcu) },
    {
      label: 'Actual',
      value:
        cost.actualUcu != null ? (
          formatUcu(cost.actualUcu)
        ) : (
          <span className="text-muted-foreground">— not final yet</span>
        ),
    },
    {
      label: 'True-up',
      value: cost.trueUp ? (
        <span
          className={
            cost.trueUp.kind === 'Refund'
              ? 'text-emerald-600 dark:text-emerald-400'
              : 'text-amber-600 dark:text-amber-400'
          }
        >
          {cost.trueUp.kind} {formatUcu(cost.trueUp.amountUcu)}
        </span>
      ) : (
        <span className="text-muted-foreground">—</span>
      ),
    },
  ]

  return (
    <div className="space-y-6">
      <KeyValueGrid items={items} />
      <UsageCharts jobId={jobId} />
    </div>
  )
}

function UsageCharts({ jobId }: { jobId: JobId }) {
  const usage = useJobUsage(jobId)

  if (usage.isLoading) {
    return <Skeleton className="h-56" />
  }

  if (!usage.data || usage.data.samples.length === 0) {
    return (
      <EmptyState icon={Gauge} title="No usage samples" description="The job hasn't run yet." />
    )
  }

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <UsageChart
        title="CPU"
        usage={usage.data}
        dataKey="cpuMillis"
        requested={usage.data.requested.cpuMillis}
        format={formatCpu}
        color="var(--chart-1)"
      />
      <UsageChart
        title="Memory"
        usage={usage.data}
        dataKey="memoryBytes"
        requested={usage.data.requested.memoryBytes}
        format={formatBytes}
        color="var(--chart-2)"
      />
    </div>
  )
}

function UsageChart({
  title,
  usage,
  dataKey,
  requested,
  format,
  color,
}: {
  title: string
  usage: JobUsage
  dataKey: 'cpuMillis' | 'memoryBytes'
  requested: number
  format: (n: number) => string
  color: string
}) {
  const gradientId = `usage-fill-${dataKey}`
  const data = usage.samples.map((s) => ({ tUs: s.tUs, value: s[dataKey] }))

  return (
    <Card>
      <CardHeader className="p-4 pb-0">
        <CardTitle className="text-sm">{title}</CardTitle>
      </CardHeader>
      <CardContent className="p-4">
        <ResponsiveContainer width="100%" height={200}>
          <AreaChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={color} stopOpacity={0.35} />
                <stop offset="100%" stopColor={color} stopOpacity={0.02} />
              </linearGradient>
            </defs>
            <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" vertical={false} />
            <XAxis
              dataKey="tUs"
              tickFormatter={formatTimeOfDayUs}
              tick={AXIS_TICK}
              stroke="var(--border)"
              minTickGap={48}
            />
            <YAxis
              tick={AXIS_TICK}
              stroke="var(--border)"
              width={56}
              tickFormatter={format}
              domain={[0, (max: number) => Math.max(max, requested) * 1.1]}
            />
            <Tooltip
              contentStyle={TOOLTIP_CONTENT_STYLE}
              labelFormatter={(t) => formatTimeOfDayUs(Number(t))}
              formatter={(value) => [format(Number(value)), title]}
            />
            <ReferenceLine
              y={requested}
              stroke="var(--muted-foreground)"
              strokeDasharray="4 4"
              label={{
                value: 'requested',
                position: 'insideTopRight',
                fill: 'var(--muted-foreground)',
                fontSize: 11,
              }}
            />
            <Area
              type="monotone"
              dataKey="value"
              stroke={color}
              strokeWidth={2}
              fill={`url(#${gradientId})`}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}
