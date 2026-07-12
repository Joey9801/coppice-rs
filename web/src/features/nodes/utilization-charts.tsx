import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { NodeUtilization } from '@/api/types'
import { formatBytes, formatCpu, formatTimeOfDayUs } from '@/lib/format'

const AXIS_TICK = { fill: 'var(--muted-foreground)', fontSize: 11 } as const

const TOOLTIP_CONTENT_STYLE = {
  background: 'var(--popover)',
  border: '1px solid var(--border)',
  color: 'var(--popover-foreground)',
  borderRadius: 8,
} as const

const LEGEND_STYLE = { fontSize: 11 } as const

interface SeriesPoint {
  tUs: number
  used: number
  allocated: number
}

interface UtilizationAreaChartProps {
  data: SeriesPoint[]
  format: (n: number) => string
  gradientId: string
}

/** One dimension: `used` (chart-1) over `allocated` (chart-2) as stacked areas. */
function UtilizationAreaChart({ data, format, gradientId }: UtilizationAreaChartProps) {
  return (
    <ResponsiveContainer width="100%" height={180}>
      <AreaChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <defs>
          <linearGradient id={`${gradientId}-used`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--chart-1)" stopOpacity={0.35} />
            <stop offset="100%" stopColor="var(--chart-1)" stopOpacity={0.02} />
          </linearGradient>
          <linearGradient id={`${gradientId}-allocated`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--chart-2)" stopOpacity={0.2} />
            <stop offset="100%" stopColor="var(--chart-2)" stopOpacity={0.02} />
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
          width={64}
          tickFormatter={format}
          domain={[0, 'auto']}
        />
        <Tooltip
          contentStyle={TOOLTIP_CONTENT_STYLE}
          labelFormatter={(t) => formatTimeOfDayUs(Number(t))}
          formatter={(value, name) => [format(Number(value)), name]}
        />
        <Legend wrapperStyle={LEGEND_STYLE} />
        <Area
          type="monotone"
          dataKey="allocated"
          name="Allocated"
          stroke="var(--chart-2)"
          strokeWidth={2}
          fill={`url(#${gradientId}-allocated)`}
          isAnimationActive={false}
        />
        <Area
          type="monotone"
          dataKey="used"
          name="Used"
          stroke="var(--chart-1)"
          strokeWidth={2}
          fill={`url(#${gradientId}-used)`}
          isAnimationActive={false}
        />
      </AreaChart>
    </ResponsiveContainer>
  )
}

export function UtilizationCharts({ utilization }: { utilization: NodeUtilization }) {
  const cpu = utilization.samples.map((s) => ({
    tUs: s.tUs,
    used: s.used.cpuMillis,
    allocated: s.allocated.cpuMillis,
  }))
  const memory = utilization.samples.map((s) => ({
    tUs: s.tUs,
    used: s.used.memoryBytes,
    allocated: s.allocated.memoryBytes,
  }))

  return (
    <div className="space-y-6">
      <div>
        <p className="mb-1 text-xs font-medium uppercase tracking-wide text-muted-foreground">
          CPU
        </p>
        <UtilizationAreaChart data={cpu} format={formatCpu} gradientId="node-util-cpu" />
      </div>
      <div>
        <p className="mb-1 text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Memory
        </p>
        <UtilizationAreaChart data={memory} format={formatBytes} gradientId="node-util-mem" />
      </div>
    </div>
  )
}
