import { type ReactNode } from 'react'
import type { CostReport, Resources } from '@/api/types'
import {
  formatBytes,
  formatCpu,
  formatDurationUs,
  formatMultiplier,
  formatPercent,
  formatUcu,
  formatUcuRatePerHour,
  formatUnitPrice,
} from '@/lib/format'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { TrueUpAmount } from './true-up-amount'

/**
 * The single home for a job's cost story (ADR 0005/0029). It always reads
 * top-to-bottom as one build-up — the per-resource rate, then the charge it
 * implies, then the refund — revealing only the parts the job's state has
 * actually produced: a queued job shows what it *will* be charged, a running
 * job shows the upfront charge with the refund still pending, a finished job
 * shows the refund and the final settled cost.
 */
export function JobCostCard({ cost, requests }: { cost: CostReport; requests: Resources }) {
  const hasPenalty = cost.priorityMultiplier !== 1 || cost.unboundedMultiplier !== 1
  const terminal = cost.actualUcu != null
  const charged = cost.chargedUcu > 0

  return (
    <Card>
      <CardHeader className="p-4 pb-0">
        <CardTitle className="text-sm">Cost</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4 p-4">
        <div>
          <div className="flex items-baseline gap-2">
            <span className="text-2xl font-semibold tabular-nums text-foreground">
              {formatUcu(cost.effectiveRateUcuPerSecond * 3600)}
            </span>
            <span className="text-sm text-muted-foreground">
              /hour {hasPenalty ? 'effective rate' : 'rate'}
            </span>
          </div>
          <RateBreakdown cost={cost} requests={requests} hasPenalty={hasPenalty} />
        </div>

        <div className="border-t pt-4">
          <ChargeSection cost={cost} terminal={terminal} charged={charged} />
        </div>
      </CardContent>
    </Card>
  )
}

/** Always-visible itemisation: each resource priced per unit, then the multipliers. */
function RateBreakdown({
  cost,
  requests,
  hasPenalty,
}: {
  cost: CostReport
  requests: Resources
  hasPenalty: boolean
}) {
  return (
    <div className="mt-3 space-y-1.5 rounded-md border bg-muted/30 p-3 text-sm">
      <ResourceRow
        label="CPU"
        requested={formatCpu(requests.cpuMillis)}
        ratePerSecond={cost.rateBreakdown.cpu}
        price={formatUnitPrice(cost.rateBreakdown.cpu, 'cpu', requests.cpuMillis)}
      />
      <ResourceRow
        label="Memory"
        requested={formatBytes(requests.memoryBytes)}
        ratePerSecond={cost.rateBreakdown.memory}
        price={formatUnitPrice(cost.rateBreakdown.memory, 'bytes', requests.memoryBytes)}
      />
      <ResourceRow
        label="Disk"
        requested={formatBytes(requests.diskBytes)}
        ratePerSecond={cost.rateBreakdown.disk}
        price={formatUnitPrice(cost.rateBreakdown.disk, 'bytes', requests.diskBytes)}
      />
      <BuildupRow
        label="Base rate"
        value={formatUcuRatePerHour(cost.rateUcuPerSecond)}
        divide
        strong={!hasPenalty}
      />
      {cost.priorityMultiplier !== 1 ? (
        <BuildupRow
          label="Priority class"
          value={formatMultiplier(cost.priorityMultiplier)}
          muted
        />
      ) : null}
      {cost.unboundedMultiplier !== 1 ? (
        <BuildupRow
          label="No runtime limit"
          value={formatMultiplier(cost.unboundedMultiplier)}
          warn
        />
      ) : null}
      {hasPenalty ? (
        <BuildupRow
          label="Effective rate"
          value={formatUcuRatePerHour(cost.effectiveRateUcuPerSecond)}
          divide
          strong
        />
      ) : null}
    </div>
  )
}

/** One resource line: "Memory · 64 GiB · 16 GiB-hours/CU" on the left, its total on the right. */
function ResourceRow({
  label,
  requested,
  ratePerSecond,
  price,
}: {
  label: string
  requested: string
  ratePerSecond: number
  price: string | null
}) {
  return (
    <div className="flex items-baseline justify-between gap-3">
      <span className="min-w-0 text-muted-foreground">
        <span className="text-foreground">{label}</span> · {requested}
        {price ? ` · ${price}` : ''}
      </span>
      <span className="shrink-0 tabular-nums text-foreground">
        {formatUcuRatePerHour(ratePerSecond)}
      </span>
    </div>
  )
}

/** The charge / refund lines, showing only what the job's state supports. */
function ChargeSection({
  cost,
  terminal,
  charged,
}: {
  cost: CostReport
  terminal: boolean
  charged: boolean
}) {
  const windowLine = (
    <span className="text-xs text-muted-foreground">
      {formatUcuRatePerHour(cost.effectiveRateUcuPerSecond)} ×{' '}
      {formatDurationUs(cost.chargeWindowUs)}{' '}
      {cost.chargeWindowIsDefault ? 'default window' : 'max runtime'}
    </span>
  )

  if (!charged && !terminal) {
    return (
      <div className="space-y-1">
        <BuildupRow label="Will be charged" value={formatUcu(cost.estimatedUcu)} strong />
        {windowLine}
        <p className="pt-1 text-xs text-muted-foreground">
          Charged upfront to your quota when the job is placed on a node.
        </p>
      </div>
    )
  }

  if (!terminal) {
    return (
      <div className="space-y-1">
        <BuildupRow label="Charged at placement" value={formatUcu(cost.chargedUcu)} strong />
        {windowLine}
        <BuildupRow
          label="Refund at finish"
          value={<span className="text-muted-foreground">— still running</span>}
        />
      </div>
    )
  }

  return (
    <div className="space-y-1.5">
      <BuildupRow label="Charged at placement" value={formatUcu(cost.chargedUcu)} />
      <div>
        <BuildupRow
          label="Refund"
          value={
            cost.trueUp ? (
              <TrueUpAmount trueUp={cost.trueUp} />
            ) : (
              <span className="text-muted-foreground">none — ran to its limit</span>
            )
          }
        />
        {cost.trueUp?.kind === 'Refund' ? (
          <p className="text-xs text-muted-foreground">
            {formatPercent(cost.refundFraction)} of the unused runtime
          </p>
        ) : null}
      </div>
      <BuildupRow
        label="Final cost"
        value={formatUcu(cost.actualUcu ?? cost.chargedUcu)}
        divide
        strong
      />
    </div>
  )
}

/** One label/value line in the build-up, with optional divider and emphasis. */
function BuildupRow({
  label,
  value,
  muted,
  strong,
  warn,
  divide,
}: {
  label: string
  value: ReactNode
  muted?: boolean
  strong?: boolean
  warn?: boolean
  divide?: boolean
}) {
  return (
    <div
      className={
        'flex items-baseline justify-between gap-3' +
        (divide ? ' border-t pt-1.5' : '') +
        (warn ? ' text-amber-600 dark:text-amber-400' : '')
      }
    >
      <span className={warn ? '' : muted ? 'text-muted-foreground' : 'text-foreground'}>
        {label}
      </span>
      <span className={'tabular-nums ' + (strong ? 'font-medium text-foreground' : '')}>
        {value}
      </span>
    </div>
  )
}
