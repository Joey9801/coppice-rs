import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { QueueStats } from '@/api/types'
import { formatTimeOfDayUs } from '@/lib/format'

const AXIS_TICK = { fill: 'var(--muted-foreground)', fontSize: 11 } as const

const TOOLTIP_CONTENT_STYLE = {
  background: 'var(--popover)',
  border: '1px solid var(--border)',
  color: 'var(--popover-foreground)',
  borderRadius: 8,
} as const

export interface QueueChartProps {
  history: QueueStats['history']
}

/** Area chart of queue depth over the recent history window. */
export function QueueChart({ history }: QueueChartProps) {
  const data = history.map((h) => ({ tUs: h.tUs, depth: h.depth }))

  return (
    <ResponsiveContainer width="100%" height={240}>
      <AreaChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <defs>
          <linearGradient id="queue-depth-fill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--chart-1)" stopOpacity={0.35} />
            <stop offset="100%" stopColor="var(--chart-1)" stopOpacity={0.02} />
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
          width={32}
          allowDecimals={false}
          domain={[0, 'auto']}
        />
        <Tooltip
          contentStyle={TOOLTIP_CONTENT_STYLE}
          labelFormatter={(t) => formatTimeOfDayUs(Number(t))}
          formatter={(value) => [value, 'Depth']}
        />
        <Area
          type="monotone"
          dataKey="depth"
          stroke="var(--chart-1)"
          strokeWidth={2}
          fill="url(#queue-depth-fill)"
          isAnimationActive={false}
        />
      </AreaChart>
    </ResponsiveContainer>
  )
}
