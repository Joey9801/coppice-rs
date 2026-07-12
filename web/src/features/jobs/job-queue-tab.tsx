import type { QueuePositionExplainer } from '@/api/types'
import { formatDurationUs, formatUcu } from '@/lib/format'
import { cn } from '@/lib/utils'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

/** Trim to a few significant figures for the score arithmetic. */
function sig(n: number, digits = 4): string {
  if (!Number.isFinite(n)) return '—'
  return Number(n.toPrecision(digits)).toString()
}

export function JobQueueTab({ queue }: { queue: QueuePositionExplainer }) {
  return (
    <div className="space-y-6">
      <div className="flex items-baseline gap-2">
        <span className="text-3xl font-semibold tabular-nums text-foreground">#{queue.rank}</span>
        <span className="text-sm text-muted-foreground">of {queue.queueDepth} queued</span>
      </div>

      <div className="rounded-lg border bg-muted/30 p-4">
        <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1 font-mono text-sm">
          <span className="text-muted-foreground">score =</span>
          <span title="priority multiplier">{sig(queue.multiplier)}</span>
          <span className="text-muted-foreground">÷</span>
          <span title="penalty product">{sig(queue.penaltyProduct)}</span>
          <span className="text-muted-foreground">+</span>
          <span title="age bonus">{sig(queue.ageBonus)}</span>
          <span className="text-muted-foreground">=</span>
          <span className="font-semibold text-foreground">{sig(queue.score)}</span>
        </div>
      </div>

      <div>
        <h3 className="mb-2 text-sm font-medium text-foreground">Penalty chain (leaf → root)</h3>
        <div className="rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Entity</TableHead>
                <TableHead className="text-right">Usage</TableHead>
                <TableHead className="text-right">Quota</TableHead>
                <TableHead className="text-right">Over quota</TableHead>
                <TableHead className="text-right">Penalty</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {queue.penaltyChain.map((link) => (
                <TableRow key={link.entity}>
                  <TableCell className="whitespace-nowrap">{link.name}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    {formatUcu(link.usageUcu)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {formatUcu(link.quotaUcu)}
                  </TableCell>
                  <TableCell
                    className={cn(
                      'text-right tabular-nums',
                      link.overQuotaRatio > 1 && 'text-red-600 dark:text-red-400',
                    )}
                  >
                    ×{sig(link.overQuotaRatio, 3)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">×{sig(link.penalty, 3)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </div>

      <div className="space-y-1 text-sm">
        <p className="text-foreground">
          aged {formatDurationUs(queue.ageUs)} of {formatDurationUs(queue.ageHorizonUs)} horizon ·
          age credit +{sig(queue.ageBonus)}
        </p>
        <p className="text-muted-foreground">
          Queued jobs are ranked by this score; it improves as your entities' usage decays (24h
          half-life) and as the job ages toward the horizon.
        </p>
      </div>
    </div>
  )
}
