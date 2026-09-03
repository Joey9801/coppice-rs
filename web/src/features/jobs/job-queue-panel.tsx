import type { QueuePositionExplainer } from '@/api/types'
import { formatDuration, formatMultiplier, formatUcu } from '@/lib/format'
import { cn } from '@/lib/utils'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

/** Trim to a few significant figures for the secondary score arithmetic. */
function sig(n: number, digits = 4): string {
  if (!Number.isFinite(n)) return '∞'
  return Number(n.toPrecision(digits)).toString()
}

/**
 * Why a queued job sits where it does, in operator terms.
 *
 * The scheduler ranks queued jobs by an effective score with three inputs:
 * the job's priority class, the quota pressure along its entity path, and a
 * small credit that grows as the job waits. This panel explains each input
 * from the terms the server actually provides — it never shows a literal
 * queue position or a "final" score, because the server does not compute
 * one: ranking happens inside the scheduler pass, and the age term's weight
 * is scheduler-local. The quotient below is the priority term only; raw
 * arithmetic is secondary detail, not the headline.
 */
export function JobQueuePanel({ queue }: { queue: QueuePositionExplainer }) {
  const overQuota = queue.penaltyChain.filter((l) => l.overQuotaRatio > 1)
  const pressure = queue.penaltyProduct

  return (
    <Card>
      <CardHeader className="p-4 pb-0">
        <CardTitle className="text-sm">Queue ranking</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6 p-4">
        <p className="text-sm text-muted-foreground">
          The scheduler admits queued jobs by <span className="text-foreground">priority</span>,
          discounted by <span className="text-foreground">quota pressure</span> and lifted by a{' '}
          <span className="text-foreground">waiting credit</span>. This is what it sees for this
          job:
        </p>

        <div className="grid gap-3 sm:grid-cols-3">
          <div className="rounded-lg border bg-muted/30 p-3">
            <div className="text-xs text-muted-foreground">Priority</div>
            <div className="mt-1 text-lg font-semibold tabular-nums text-foreground">
              ×{sig(queue.multiplier)}
            </div>
            <p className="mt-1 text-xs text-muted-foreground">
              The job's priority class multiplies its ranking directly.
            </p>
          </div>
          <div className="rounded-lg border bg-muted/30 p-3">
            <div className="text-xs text-muted-foreground">Quota pressure</div>
            <div
              className={cn(
                'mt-1 text-lg font-semibold tabular-nums',
                pressure > 1 ? 'text-red-600 dark:text-red-400' : 'text-foreground',
              )}
            >
              ÷{sig(pressure)}
            </div>
            <p className="mt-1 text-xs text-muted-foreground">
              {overQuota.length === 0
                ? 'Every entity on the path is within quota — no discount.'
                : overQuota.length === 1 && overQuota[0]
                  ? `${overQuota[0].name} is over quota, discounting the ranking.`
                  : `${overQuota.length} entities are over quota, discounting the ranking.`}
            </p>
          </div>
          <div className="rounded-lg border bg-muted/30 p-3">
            <div className="text-xs text-muted-foreground">Waiting</div>
            <div className="mt-1 text-lg font-semibold tabular-nums text-foreground">
              {formatDuration(queue.ageSeconds)}
            </div>
            <p className="mt-1 text-xs text-muted-foreground">
              Waiting earns a credit that quota pressure cannot reduce, giving this job a permanent
              head start over anything submitted after it. Every queued job's credit grows at the
              same rate, so waiting does not reorder jobs already in the queue — that shifts as
              quota usage decays.
            </p>
          </div>
        </div>

        <div>
          <h3 className="mb-2 text-sm font-medium text-foreground">
            Quota pressure, entity by entity (leaf → root)
          </h3>
          <div className="rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Entity</TableHead>
                  <TableHead className="text-right">Usage</TableHead>
                  <TableHead className="text-right">Quota</TableHead>
                  <TableHead className="text-right">Share of quota used</TableHead>
                  <TableHead className="text-right">Ranking discount</TableHead>
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
                      {formatMultiplier(link.overQuotaRatio)}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatMultiplier(link.penalty)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            Each entity's usage decays over time — halving every configured half-life (24 hours by
            default) — so pressure fades as finished work ages out of the window.
          </p>
        </div>

        <details className="text-sm">
          <summary className="cursor-pointer text-muted-foreground">Score arithmetic</summary>
          <div className="mt-2 rounded-lg border bg-muted/30 p-3 font-mono text-xs">
            priority term = <span title="priority multiplier">{sig(queue.multiplier)}</span>
            <span className="text-muted-foreground"> ÷ </span>
            <span title="penalty product">{sig(pressure)}</span>
            <span className="text-muted-foreground"> = </span>
            <span className="font-semibold text-foreground">
              {sig(queue.multiplier / pressure)}
            </span>
            <span className="text-muted-foreground">
              {' '}
              + waiting credit (computed by the scheduler)
            </span>
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            The scheduler ranks by this priority term plus a waiting credit it computes itself, so
            the total is not shown here. Entities over quota divide the ranking; waiting adds to it.
          </p>
        </details>
      </CardContent>
    </Card>
  )
}
