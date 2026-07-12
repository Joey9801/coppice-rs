import type { ReactNode } from 'react'
import { Link } from '@tanstack/react-router'
import { ArrowLeft, ArrowRight, Boxes, Inbox, ListTree } from 'lucide-react'
import type {
  AccrualView,
  AttemptView,
  NodeDetail,
  NodeHistoryEntry,
  NodeSummary,
} from '@/api/types'
import { useNode, useNodeHistory, useNodeLogs, useNodeUtilization } from '@/api/queries'
import {
  formatDurationUs,
  formatPercent,
  formatTimeUntil,
  formatUcu,
  resourceFractions,
} from '@/lib/format'
import { cn } from '@/lib/utils'
import {
  EmptyState,
  IdLink,
  LogViewer,
  PageHeader,
  ResourceTriple,
  StatePill,
  StatTile,
  TimeAgo,
  outcomePill,
} from '@/components'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { isNotFound, sortedLabels } from './lib'
import { UtilizationCharts } from './utilization-charts'

export function NodeDetailPage({ nodeId }: { nodeId: string }) {
  const { data: detail, isPending, isError, error } = useNode(nodeId)

  return (
    <div>
      <Link
        to="/nodes"
        className="mb-4 inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground"
      >
        <ArrowLeft className="size-4" />
        All nodes
      </Link>

      {isPending ? (
        <DetailSkeleton />
      ) : isError ? (
        <Card>
          <EmptyState
            icon={Boxes}
            title={isNotFound(error) ? 'Node not found' : "Couldn't load node"}
            description={
              isNotFound(error) ? (
                <>
                  No node <span className="font-mono">{nodeId}</span> is registered with the
                  cluster.
                </>
              ) : (
                'The cluster API is unavailable. Retrying automatically.'
              )
            }
          />
        </Card>
      ) : (
        <NodeDetailBody detail={detail} nodeId={nodeId} />
      )}
    </div>
  )
}

function NodeDetailBody({ detail, nodeId }: { detail: NodeDetail; nodeId: string }) {
  const { summary } = detail
  const lost = summary.health === 'Lost'
  const draining = summary.schedulable === false && !lost
  const usedFrac = resourceFractions(summary.used, summary.capacity)

  return (
    <div className="space-y-6">
      <PageHeader
        title={
          <span className="flex flex-wrap items-center gap-3">
            <span className="font-mono text-lg break-all">{summary.id}</span>
            <StatePill state={lost ? 'Lost' : 'Healthy'} />
            {summary.schedulable === false ? <StatePill state="Draining" /> : null}
          </span>
        }
        description={<HeaderDescription summary={summary} />}
        actions={
          <Link
            to="/jobs"
            search={{ node: nodeId }}
            className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline"
          >
            View jobs on this node
            <ArrowRight className="size-4" />
          </Link>
        }
      />

      {lost ? (
        <Banner tone="destructive">
          Agent lost — no heartbeat for{' '}
          {summary.lastHeartbeatUs == null ? (
            'an unknown interval'
          ) : (
            <TimeAgo tUs={summary.lastHeartbeatUs} className="font-medium" />
          )}
          . Epoch fenced at {summary.epoch}; running attempts will be declared NodeLost.
        </Banner>
      ) : draining ? (
        <Banner tone="amber">Draining — no new placements; existing work continues.</Banner>
      ) : null}

      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatTile label="Running attempts" value={summary.runningCount} />
        <StatTile label="Accruing allocations" value={summary.accruingCount} />
        <StatTile label="CPU used" value={formatPercent(usedFrac.cpu)} hint="of capacity" />
        <StatTile label="Memory used" value={formatPercent(usedFrac.memory)} hint="of capacity" />
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle>Capacity</CardTitle>
          </CardHeader>
          <CardContent>
            <ResourceTriple
              capacity={summary.capacity}
              allocated={summary.allocated}
              used={summary.used}
            />
          </CardContent>
        </Card>

        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Utilization history</CardTitle>
          </CardHeader>
          <CardContent>
            <UtilizationSection nodeId={nodeId} />
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Active attempts</CardTitle>
        </CardHeader>
        <CardContent>
          <ActiveAttemptsTable attempts={detail.activeAttempts} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Accrual queue</CardTitle>
        </CardHeader>
        <CardContent>
          <AccrualQueueTable queue={detail.accrualQueue} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Recent history</CardTitle>
        </CardHeader>
        <CardContent>
          <HistorySection nodeId={nodeId} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Agent logs</CardTitle>
        </CardHeader>
        <CardContent>
          <LogsSection nodeId={nodeId} />
        </CardContent>
      </Card>
    </div>
  )
}

function HeaderDescription({ summary }: { summary: NodeSummary }) {
  const labels = sortedLabels(summary.labels)
  return (
    <span className="flex flex-wrap items-center gap-2">
      {labels.map(([k, v]) => (
        <Badge key={k} variant="outline" className="font-mono text-[11px]">
          {k}={v}
        </Badge>
      ))}
      <span className="text-sm text-muted-foreground">
        epoch {summary.epoch} · last heartbeat{' '}
        {summary.lastHeartbeatUs == null ? 'never' : <TimeAgo tUs={summary.lastHeartbeatUs} />}
      </span>
    </span>
  )
}

function Banner({ tone, children }: { tone: 'destructive' | 'amber'; children: ReactNode }) {
  return (
    <div
      className={cn(
        'rounded-lg border px-4 py-3 text-sm',
        tone === 'destructive'
          ? 'border-destructive/30 bg-destructive/5 text-destructive'
          : 'border-amber-500/30 bg-amber-500/10 text-amber-700 dark:text-amber-300',
      )}
    >
      {children}
    </div>
  )
}

function UtilizationSection({ nodeId }: { nodeId: string }) {
  const { data, isPending, isError } = useNodeUtilization(nodeId)
  if (isPending) return <Skeleton className="h-[396px]" />
  if (isError || data.samples.length === 0) {
    return <EmptyState title="No utilization samples" description="No history recorded yet." />
  }
  return <UtilizationCharts utilization={data} />
}

function ActiveAttemptsTable({ attempts }: { attempts: AttemptView[] }) {
  if (attempts.length === 0) {
    return <EmptyState icon={Inbox} title="Nothing running." />
  }
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Attempt</TableHead>
          <TableHead>Job</TableHead>
          <TableHead>State</TableHead>
          <TableHead>Started</TableHead>
          <TableHead className="text-right">Rate</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {attempts.map((a) => (
          <TableRow key={a.id}>
            <TableCell>
              <IdLink id={a.id} />
            </TableCell>
            <TableCell>
              <IdLink id={a.job} />
            </TableCell>
            <TableCell>
              <StatePill state={a.state} />
            </TableCell>
            <TableCell>
              {a.startedAtUs == null ? (
                <span className="text-muted-foreground">—</span>
              ) : (
                <TimeAgo tUs={a.startedAtUs} className="text-sm text-muted-foreground" />
              )}
            </TableCell>
            <TableCell className="text-right tabular-nums">
              {formatUcu(a.rateUcuPerSecond * 3600)}/h
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}

function AccrualQueueTable({ queue }: { queue: AccrualView[] }) {
  if (queue.length === 0) {
    return <EmptyState icon={ListTree} title="No accruing allocations" />
  }
  return (
    <div className="space-y-3">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Job</TableHead>
            <TableHead className="w-72">Funding</TableHead>
            <TableHead>Projected start</TableHead>
            <TableHead className="text-right">Seq</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {queue.map((entry) => (
            <TableRow key={entry.allocation.id}>
              <TableCell>
                <IdLink id={entry.allocation.job} />
              </TableCell>
              <TableCell>
                <div className="space-y-1">
                  <FundingBar label="CPU" fraction={entry.fundedFraction.cpu} />
                  <FundingBar label="Mem" fraction={entry.fundedFraction.memory} />
                  <FundingBar label="Disk" fraction={entry.fundedFraction.disk} />
                </div>
              </TableCell>
              <TableCell>
                {entry.projectedStartUs == null ? (
                  <span className="text-amber-700 dark:text-amber-300">unbounded</span>
                ) : (
                  <span className="tabular-nums">{formatTimeUntil(entry.projectedStartUs)}</span>
                )}
              </TableCell>
              <TableCell className="text-right tabular-nums text-muted-foreground">
                {entry.allocation.seq}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <p className="text-xs text-muted-foreground">
        Allocations fund in commit order as capacity frees (ADR 0027).
      </p>
    </div>
  )
}

function FundingBar({ label, fraction }: { label: string; fraction: number }) {
  return (
    <div className="flex items-center gap-2">
      <span className="w-8 shrink-0 text-xs text-muted-foreground">{label}</span>
      <Progress value={fraction} className="h-1.5 flex-1" />
      <span className="w-9 shrink-0 text-right text-xs tabular-nums text-muted-foreground">
        {formatPercent(fraction)}
      </span>
    </div>
  )
}

function HistorySection({ nodeId }: { nodeId: string }) {
  const { data, isPending, isError } = useNodeHistory(nodeId)
  if (isPending) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-9" />
        ))}
      </div>
    )
  }
  if (isError || data.length === 0) {
    return <EmptyState icon={Inbox} title="No recent history" />
  }
  return <HistoryTable entries={data} />
}

function HistoryTable({ entries }: { entries: NodeHistoryEntry[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Job</TableHead>
          <TableHead>Image</TableHead>
          <TableHead>Outcome</TableHead>
          <TableHead className="text-right">Duration</TableHead>
          <TableHead className="text-right">Ended</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {entries.map((e) => (
          <TableRow key={e.attempt}>
            <TableCell>
              <IdLink id={e.job} />
            </TableCell>
            <TableCell className="max-w-[16rem] truncate font-mono text-xs text-muted-foreground">
              {e.image}
            </TableCell>
            <TableCell>{outcomePill(e.outcome)}</TableCell>
            <TableCell className="text-right tabular-nums">
              {e.startedAtUs == null ? (
                <span className="text-muted-foreground">—</span>
              ) : (
                formatDurationUs(e.endedAtUs - e.startedAtUs)
              )}
            </TableCell>
            <TableCell className="text-right">
              <TimeAgo tUs={e.endedAtUs} className="text-sm text-muted-foreground" />
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}

function LogsSection({ nodeId }: { nodeId: string }) {
  const { data, isPending } = useNodeLogs(nodeId)
  return (
    <div className="space-y-2">
      <LogViewer entries={data?.entries ?? []} loading={isPending} />
      <p className="text-xs text-muted-foreground">
        Mock data — agent log shipping is not designed in the backend yet.
      </p>
    </div>
  )
}

function DetailSkeleton() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-10 w-96" />
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-24" />
        ))}
      </div>
      <Skeleton className="h-64" />
    </div>
  )
}
