import { useMemo, type ReactNode } from 'react'
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { Resources } from '@/api/types'
import { formatBytes, formatCpu, formatTimeOfDay } from '@/lib/format'
import { byteTicks, cpuTicks } from '@/lib/ticks'
import { cn } from '@/lib/utils'
import { capacityCoverageNote, coverageSuffix } from './coverage'
import {
  toCapacitySeries,
  type CapacityBandPoint,
  type CapacityHistorySample,
  type CapacitySeries,
  type ResourcePick,
} from './series'

const AXIS_TICK = { fill: 'var(--muted-foreground)', fontSize: 11 } as const

/** Colours of the four stacked bands and the three real boundaries. */
const BAND_COLOR = {
  used: 'var(--chart-1)',
  allocated: 'var(--chart-2)',
  overAllocation: 'var(--chart-5)',
  capacity: 'var(--muted-foreground)',
} as const

interface Dimension {
  key: 'cpu' | 'memory' | 'disk'
  label: string
  pick: ResourcePick
  format: (n: number) => string
  ticks: (max: number) => number[]
}

const DIMENSIONS: readonly Dimension[] = [
  {
    key: 'cpu',
    label: 'CPU',
    pick: (r) => r.cpuMillis,
    format: formatCpu,
    ticks: cpuTicks,
  },
  {
    key: 'memory',
    label: 'Memory',
    pick: (r) => r.memoryBytes,
    format: formatBytes,
    ticks: byteTicks,
  },
  {
    key: 'disk',
    label: 'Disk',
    pick: (r) => r.diskBytes,
    format: formatBytes,
    ticks: byteTicks,
  },
]

export interface CapacityHistoryProps {
  /** Oldest first; empty renders the honest "no history yet" state. */
  history: readonly CapacityHistorySample[]
  /** The current reading, shown as text beside each dimension's chart. */
  latest: { capacity: Resources; allocated: Resources; used: Resources | null }
  /**
   * Usage coverage behind `latest.used` for a cluster-wide figure (a sum
   * over reporting nodes). Omit for a single node, whose `used: null`
   * already says whether it reported.
   */
  coverage?: { reportingNodes: number; totalNodes: number } | null
  /** Scopes this instance's SVG gradient ids; unique per mounted panel. */
  idPrefix: string
  className?: string
}

/**
 * CPU / memory / disk capacity history, side by side at wide widths and
 * stacked below the `lg` breakpoint.
 *
 * Each chart stacks derived, non-negative bands (see `series.ts`) so that
 * coincident values stay legible, and then draws the three real series as
 * boundaries on top: measured `used`, funded `allocated`, and advertised
 * `capacity` as a dotted line so it survives equality with `allocated`.
 * Nothing is clamped: usage past allocation gets its own band, and an
 * unreported `used` is a gap in both the band and the line, never a zero.
 */
export function CapacityHistory({
  history,
  latest,
  coverage,
  idPrefix,
  className,
}: CapacityHistoryProps) {
  const series = useMemo(() => DIMENSIONS.map((d) => toCapacitySeries(history, d.pick)), [history])
  const note = coverage ? capacityCoverageNote(coverage.reportingNodes, coverage.totalNodes) : null
  const anyOverAllocation = series.some((s) => s.hasOverAllocation)
  const anyGap = series.some((s) => s.hasCoverageGap)

  return (
    <div className={cn('space-y-3', className)}>
      {note ? <p className="text-xs text-muted-foreground">{note}</p> : null}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        {DIMENSIONS.map((dimension, i) => (
          <DimensionPanel
            key={dimension.key}
            dimension={dimension}
            series={series[i]!}
            latest={latest}
            gradientId={`${idPrefix}-${dimension.key}`}
          />
        ))}
      </div>
      <CapacityLegend showOverAllocation={anyOverAllocation} showGap={anyGap} />
    </div>
  )
}

function DimensionPanel({
  dimension,
  series,
  latest,
  gradientId,
}: {
  dimension: Dimension
  series: CapacitySeries
  latest: CapacityHistoryProps['latest']
  gradientId: string
}) {
  const { format, pick, label } = dimension
  const used = latest.used === null ? null : pick(latest.used)

  return (
    <div>
      <div className="mb-2 flex flex-wrap items-baseline justify-between gap-x-3 gap-y-0.5">
        <h4 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          {label}
        </h4>
        <p className="text-xs tabular-nums text-muted-foreground">
          {used === null ? 'not reported' : `${format(used)} used`} /{' '}
          {format(pick(latest.allocated))} alloc / {format(pick(latest.capacity))} cap
        </p>
      </div>
      {series.points.length === 0 ? (
        <div className="flex h-[180px] items-center justify-center rounded-md border border-dashed border-border">
          <p className="text-sm text-muted-foreground">No history yet</p>
        </div>
      ) : (
        <CapacityDimensionChart
          series={series}
          format={format}
          makeTicks={dimension.ticks}
          gradientId={gradientId}
        />
      )}
    </div>
  )
}

function CapacityDimensionChart({
  series,
  format,
  makeTicks,
  gradientId,
}: {
  series: CapacitySeries
  format: (n: number) => string
  makeTicks: (max: number) => number[]
  gradientId: string
}) {
  const ticks = makeTicks(series.max)
  const top = ticks[ticks.length - 1] ?? series.max

  return (
    <ResponsiveContainer width="100%" height={180}>
      <ComposedChart data={series.points} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <defs>
          <pattern
            id={`${gradientId}-unallocated`}
            width={6}
            height={6}
            patternTransform="rotate(45)"
            patternUnits="userSpaceOnUse"
          >
            <line
              x1="0"
              y1="0"
              x2="0"
              y2="6"
              stroke="var(--muted-foreground)"
              strokeWidth={1}
              strokeOpacity={0.25}
            />
          </pattern>
        </defs>
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
          width={80}
          tickFormatter={format}
          ticks={ticks}
          interval={0}
          domain={[0, top]}
        />
        <Tooltip
          isAnimationActive={false}
          content={(props) => (
            <CapacityTooltip
              active={props.active}
              payload={props.payload as unknown as ReadonlyArray<{ payload?: CapacityBandPoint }>}
              format={format}
            />
          )}
        />
        {/* Bands, bottom-up. Derived from the three real series; see series.ts. */}
        <Area
          type="monotone"
          stackId="bands"
          dataKey="usedWithinAllocated"
          name="Used"
          stroke="none"
          fill={BAND_COLOR.used}
          fillOpacity={0.5}
          isAnimationActive={false}
          connectNulls={false}
          activeDot={false}
        />
        <Area
          type="monotone"
          stackId="bands"
          dataKey="allocatedUnused"
          name="Allocated, unused"
          stroke="none"
          connectNulls={false}
          fill={BAND_COLOR.allocated}
          fillOpacity={0.22}
          isAnimationActive={false}
          activeDot={false}
        />
        <Area
          type="monotone"
          stackId="bands"
          dataKey="usedOverAllocated"
          name="Above allocation"
          stroke="none"
          connectNulls={false}
          fill={BAND_COLOR.overAllocation}
          fillOpacity={0.45}
          isAnimationActive={false}
          activeDot={false}
        />
        <Area
          type="monotone"
          stackId="bands"
          dataKey="unallocatedCapacity"
          name="Unallocated capacity"
          stroke="none"
          connectNulls={false}
          fill={`url(#${gradientId}-unallocated)`}
          isAnimationActive={false}
          activeDot={false}
        />
        {/* The real series as boundaries, drawn over the bands. */}
        <Line
          type="monotone"
          dataKey="allocated"
          name="Allocated"
          stroke={BAND_COLOR.allocated}
          strokeWidth={1.5}
          dot={false}
          activeDot={false}
          isAnimationActive={false}
        />
        <Line
          type="monotone"
          dataKey="used"
          name="Used"
          stroke={BAND_COLOR.used}
          strokeWidth={2}
          dot={false}
          activeDot={false}
          isAnimationActive={false}
          connectNulls={false}
        />
        <Line
          type="monotone"
          dataKey="capacity"
          name="Capacity"
          stroke={BAND_COLOR.capacity}
          strokeWidth={1.5}
          strokeDasharray="4 3"
          dot={false}
          activeDot={false}
          isAnimationActive={false}
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}

/**
 * One reading in full: what was measured, what was funded and what the
 * hardware advertises — plus, when they disagree, the derived excess. The
 * stacked bands are never named as if they were readings.
 */
function CapacityTooltip({
  active,
  payload,
  format,
}: {
  active?: boolean
  payload?: ReadonlyArray<{ payload?: CapacityBandPoint }>
  format: (n: number) => string
}) {
  const point = payload?.[0]?.payload
  if (!active || !point) return null
  const coverage = coverageSuffix(point.reportingNodes, point.totalNodes)

  return (
    <div className="rounded-lg border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md">
      <p className="mb-1 font-medium">{formatTimeOfDay(new Date(point.tMs))}</p>
      <dl className="space-y-0.5">
        <TooltipRow
          swatch={BAND_COLOR.used}
          label="Used"
          value={point.used === null ? 'not reported' : format(point.used)}
          note={
            point.used === null
              ? 'no usage sample at this instant'
              : coverage
                ? `measured · ${coverage}`
                : 'measured'
          }
        />
        <TooltipRow
          swatch={BAND_COLOR.allocated}
          label="Allocated"
          value={format(point.allocated)}
          note="funded by allocations"
        />
        <TooltipRow
          swatch={BAND_COLOR.capacity}
          label="Capacity"
          value={format(point.capacity)}
          note="advertised by nodes"
          dashed
        />
      </dl>
      {point.usedOverAllocated !== null && point.usedOverAllocated > 0 ? (
        <p className="mt-1 text-[11px]" style={{ color: BAND_COLOR.overAllocation }}>
          {format(point.usedOverAllocated)} above allocation (derived)
        </p>
      ) : null}
    </div>
  )
}

function TooltipRow({
  swatch,
  label,
  value,
  note,
  dashed,
}: {
  swatch: string
  label: string
  value: string
  note: string
  dashed?: boolean
}) {
  return (
    <div className="flex items-baseline gap-2">
      <Swatch color={swatch} dashed={dashed} />
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="ml-auto tabular-nums">{value}</dd>
      <dd className="text-[11px] text-muted-foreground">({note})</dd>
    </div>
  )
}

function Swatch({
  color,
  dashed,
  hatched,
}: {
  color?: string
  dashed?: boolean
  hatched?: boolean
}) {
  if (dashed) {
    return (
      <span
        aria-hidden
        className="inline-block h-0 w-4 shrink-0 border-t-2 border-dashed"
        style={{ borderColor: color }}
      />
    )
  }
  return (
    <span
      aria-hidden
      className={cn(
        'inline-block size-2.5 shrink-0 rounded-[2px]',
        hatched && 'border border-border',
      )}
      style={
        hatched
          ? {
              backgroundImage:
                'repeating-linear-gradient(45deg, var(--muted-foreground) 0 1px, transparent 1px 4px)',
              opacity: 0.5,
            }
          : { background: color, opacity: 0.6 }
      }
    />
  )
}

/** What each mark means, and whether it is a reading or derived for drawing. */
function CapacityLegend({
  showOverAllocation,
  showGap,
}: {
  showOverAllocation: boolean
  showGap: boolean
}) {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-muted-foreground">
      <LegendItem swatch={<Swatch color={BAND_COLOR.used} />}>Used (measured)</LegendItem>
      <LegendItem swatch={<Swatch color={BAND_COLOR.allocated} />}>
        Allocated, unused (derived)
      </LegendItem>
      {showOverAllocation ? (
        <LegendItem swatch={<Swatch color={BAND_COLOR.overAllocation} />}>
          Above allocation (measured)
        </LegendItem>
      ) : null}
      <LegendItem swatch={<Swatch hatched />}>Capacity not allocated (derived)</LegendItem>
      <LegendItem swatch={<Swatch color={BAND_COLOR.capacity} dashed />}>
        Capacity (advertised)
      </LegendItem>
      {showGap ? (
        <span>
          Gaps in the bands are instants with no usage sample; allocated and capacity stay drawn as
          lines.
        </span>
      ) : null}
    </div>
  )
}

function LegendItem({ swatch, children }: { swatch: ReactNode; children: ReactNode }) {
  return (
    <span className="flex items-center gap-1.5">
      {swatch}
      {children}
    </span>
  )
}
