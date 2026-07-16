import { useState } from 'react'
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
import {
  derivePhase,
  jobAttemptId,
  jobCurrentAttempt,
  type AttemptId,
  type JobDetail,
  type UsageSample,
} from '@/api/types'
import { useJobUsage } from '@/api/queries'
import { formatBytes, formatCpu, formatTimeOfDay, shortId } from '@/lib/format'
import { byteTicks, cpuTicks } from '@/lib/ticks'
import { EmptyState } from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Select } from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { AXIS_TICK, TOOLTIP_CONTENT_STYLE } from './chart-theme'

/**
 * Resource usage of one attempt: CPU / memory / disk charts with an attempt
 * picker (usage is measured per attempt, so retries each have their own
 * series). Jobs that haven't run yet get a state-aware placeholder.
 */
export function JobUsageSection({ job }: { job: JobDetail }) {
  const attempts = job.attempts
  const currentAttemptId = jobAttemptId(job.state)
  const fallback = currentAttemptId ?? attempts[attempts.length - 1]?.id ?? null
  const [picked, setPicked] = useState<AttemptId | null>(null)
  // A stale pick (e.g. the world moved on) falls back to the current attempt.
  const attemptId = picked !== null && attempts.some((a) => a.id === picked) ? picked : fallback
  const usage = useJobUsage(job.id, attemptId)

  return (
    <Card>
      <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-3 p-4 pb-0">
        <CardTitle className="text-sm">Usage</CardTitle>
        {attempts.length > 1 ? (
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            Attempt
            <Select
              value={attemptId ?? ''}
              onChange={(e) => setPicked(e.target.value)}
              className="h-8 font-mono text-xs"
              aria-label="Attempt to chart"
            >
              {[...attempts].reverse().map((a) => (
                <option key={a.id} value={a.id}>
                  {shortId(a.id)} · {a.state}
                  {a.id === currentAttemptId ? ' (current)' : ''}
                </option>
              ))}
            </Select>
          </label>
        ) : null}
      </CardHeader>
      <CardContent className="p-4">
        {usage.isLoading ? (
          <Skeleton className="h-48" />
        ) : !usage.data || usage.data.samples.length === 0 ? (
          <EmptyState icon={Gauge} title="No usage yet" description={placeholderText(job)} />
        ) : (
          <div className="grid gap-4 lg:grid-cols-3">
            <UsageChart
              title="CPU"
              samples={usage.data.samples}
              dataKey="cpuMillis"
              requested={usage.data.requested.cpuMillis}
              format={formatCpu}
              makeTicks={cpuTicks}
              color="var(--chart-1)"
            />
            <UsageChart
              title="Memory"
              samples={usage.data.samples}
              dataKey="memoryBytes"
              requested={usage.data.requested.memoryBytes}
              format={formatBytes}
              makeTicks={byteTicks}
              color="var(--chart-2)"
            />
            <UsageChart
              title="Disk"
              samples={usage.data.samples}
              dataKey="diskBytes"
              requested={usage.data.requested.diskBytes}
              format={formatBytes}
              makeTicks={byteTicks}
              color="var(--chart-3)"
            />
          </div>
        )}
      </CardContent>
    </Card>
  )
}

/** Why there are no samples, in terms of where the job is in its lifecycle. */
function placeholderText(job: JobDetail): string {
  const phase = derivePhase(job.state, jobCurrentAttempt(job)?.state ?? null)
  switch (phase) {
    case 'Submitted':
    case 'Accepted':
      return "The job hasn't started yet — it is still being admitted."
    case 'Queued':
      return job.queue
        ? `The job is #${job.queue.rank} of ${job.queue.queueDepth} in the queue; usage appears once it starts running.`
        : 'The job is queued; usage appears once it starts running.'
    case 'Preparing':
      return 'The job is waiting for capacity on its node; usage appears once it starts running.'
    default:
      return 'This attempt never ran a container, so no usage was recorded.'
  }
}

function UsageChart({
  title,
  samples,
  dataKey,
  requested,
  format,
  makeTicks,
  color,
}: {
  title: string
  samples: UsageSample[]
  dataKey: 'cpuMillis' | 'memoryBytes' | 'diskBytes'
  requested: number
  format: (n: number) => string
  /** Nice-tick generator for this dimension's unit (see lib/ticks.ts). */
  makeTicks: (max: number) => number[]
  color: string
}) {
  const gradientId = `usage-fill-${dataKey}`
  const data = samples.map((s) => ({ tUs: s.t, value: s[dataKey] }))
  const dataMax = data.reduce((m, d) => Math.max(m, d.value), 0)
  const ticks = makeTicks(Math.max(dataMax, requested))
  const top = ticks[ticks.length - 1] ?? requested

  return (
    <div>
      <h4 className="mb-1 text-xs font-medium text-muted-foreground">{title}</h4>
      <ResponsiveContainer width="100%" height={180}>
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
            tickFormatter={formatTimeOfDay}
            tick={AXIS_TICK}
            stroke="var(--border)"
            minTickGap={48}
          />
          <YAxis
            tick={AXIS_TICK}
            stroke="var(--border)"
            width={64}
            tickFormatter={format}
            ticks={ticks}
            interval={0}
            domain={[0, top]}
          />
          <Tooltip
            contentStyle={TOOLTIP_CONTENT_STYLE}
            labelFormatter={(ms) => formatTimeOfDay(new Date(Number(ms)))}
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
    </div>
  )
}
