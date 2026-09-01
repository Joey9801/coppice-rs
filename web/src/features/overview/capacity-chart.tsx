import {
  Line,
  LineChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { CapacitySample } from '@/api/types'
import { formatCpu, formatTimeOfDay } from '@/lib/format'
import { EmptyState } from '@/components'
import { formatUsedTooltip } from './capacity-coverage'

const AXIS_TICK = { fill: 'var(--muted-foreground)', fontSize: 11 } as const

const TOOLTIP_CONTENT_STYLE = {
  background: 'var(--popover)',
  border: '1px solid var(--border)',
  color: 'var(--popover-foreground)',
  borderRadius: 8,
} as const

/**
 * Total/allocated/used CPU (millicores) over the cluster's recent capacity
 * history. `capacity`/`allocated`/`used` each carry three resource
 * dimensions (CPU, memory, disk) with different units — mixing them on one
 * axis would be dishonest, so this chart shows CPU only (the dimension most
 * relevant to scheduling pressure) and labels itself accordingly. `used:
 * null` (no node reporting usage at that instant) renders as a gap, never a
 * fabricated 0 — `connectNulls={false}` keeps the gap visible.
 */
export function CapacityChart({ history }: { history: CapacitySample[] }) {
  if (history.length === 0) {
    return <EmptyState title="No capacity history yet" />
  }

  const data = history.map((h) => ({
    tMs: h.t.getTime(),
    capacity: h.capacity.cpuMillis,
    allocated: h.allocated.cpuMillis,
    used: h.used?.cpuMillis ?? null,
    reportingNodes: h.reportingNodes,
    totalNodes: h.totalNodes,
  }))

  return (
    <div>
      <p className="mb-1 text-xs font-medium uppercase tracking-wide text-muted-foreground">CPU</p>
      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" vertical={false} />
          <XAxis
            dataKey="tMs"
            tickFormatter={(ms) => formatTimeOfDay(new Date(ms))}
            tick={AXIS_TICK}
            stroke="var(--border)"
            minTickGap={48}
          />
          <YAxis
            tick={AXIS_TICK}
            stroke="var(--border)"
            width={64}
            tickFormatter={formatCpu}
            domain={[0, 'auto']}
          />
          <Tooltip
            contentStyle={TOOLTIP_CONTENT_STYLE}
            labelFormatter={(ms) => formatTimeOfDay(new Date(Number(ms)))}
            formatter={(value, name, item) => {
              if (name === 'Used') {
                const datum = item.payload as (typeof data)[number]
                return [
                  formatUsedTooltip(
                    value == null ? null : Number(value),
                    datum.reportingNodes,
                    datum.totalNodes,
                  ),
                  name,
                ]
              }
              return [value == null ? 'not reported' : formatCpu(Number(value)), name]
            }}
          />
          <Line
            type="monotone"
            dataKey="capacity"
            name="Capacity"
            stroke="var(--chart-3)"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="allocated"
            name="Allocated"
            stroke="var(--chart-2)"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="used"
            name="Used"
            stroke="var(--chart-1)"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
            connectNulls={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
