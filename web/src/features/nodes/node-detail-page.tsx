import { useMemo, type ReactNode } from 'react'
import { Link } from '@tanstack/react-router'
import { ArrowLeft, ArrowRight, Boxes, Inbox, ListTree } from 'lucide-react'
import type {
  AccrualView,
  AttemptView,
  HostFacts,
  NodeDetail,
  NodeSummary,
  Resources,
} from '@/api/types'
import { useNode, useNodeLogs, useNodeUtilization } from '@/api/queries'
import {
  formatBytes,
  formatCpu,
  formatPercent,
  formatTimeUntil,
  formatUcu,
  resourceFractions,
} from '@/lib/format'
import { cn } from '@/lib/utils'
import {
  CapacityHistory,
  EmptyState,
  IdLink,
  LogViewer,
  PageHeader,
  StatePill,
  StatTile,
  TimeAgo,
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
import { isNotFound, nodeCapacityHistory, sortedLabels } from './lib'
import { formatHostKernel, formatHostOs } from './host-facts'

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
            <StatePill state={summary.health} />
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
          {summary.lastHeartbeat == null ? (
            'an unknown interval'
          ) : (
            <TimeAgo t={summary.lastHeartbeat} className="font-medium" />
          )}
          . Running attempts from registration epoch {summary.epoch} will be marked lost.
        </Banner>
      ) : draining ? (
        <Banner tone="amber">Draining — no new placements; existing work continues.</Banner>
      ) : null}

      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatTile label="Running attempts" value={summary.runningCount} />
        <StatTile label="Accruing allocations" value={summary.accruingCount} />
        <StatTile
          label="CPU used"
          value={usedFrac.cpu != null ? formatPercent(usedFrac.cpu) : '—'}
          hint={usedFrac.cpu != null ? 'of capacity' : 'not reported'}
        />
        <StatTile
          label="Memory used"
          value={usedFrac.memory != null ? formatPercent(usedFrac.memory) : '—'}
          hint={usedFrac.memory != null ? 'of capacity' : 'not reported'}
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Capacity</CardTitle>
        </CardHeader>
        <CardContent>
          <CapacitySection nodeId={nodeId} summary={summary} />
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle>Host</CardTitle>
          </CardHeader>
          <CardContent>
            <HostCard host={detail.host} summary={summary} detected={detail.detectedCapacity} />
          </CardContent>
        </Card>

        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Active attempts</CardTitle>
          </CardHeader>
          <CardContent>
            <ActiveAttemptsTable attempts={detail.activeAttempts} />
          </CardContent>
        </Card>
      </div>

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
          <CardTitle>Agent logs</CardTitle>
        </CardHeader>
        <CardContent>
          <LogsSection nodeId={nodeId} />
        </CardContent>
      </Card>
    </div>
  )
}

/**
 * What the machine is, and — when they disagree — why what it advertises is
 * not what it has.
 *
 * Every fact is best-effort at the agent, so an empty string or a zero count
 * means "not determined" and renders as `unknown` rather than as a confident
 * `0`. A node whose agent reported nothing at all is not an error: older
 * agents and hosts whose readings all failed both land here.
 */
function HostCard({
  host,
  summary,
  detected,
}: {
  host: HostFacts | null
  summary: NodeSummary
  detected: Resources | null
}) {
  if (host === null) {
    return (
      <p className="text-sm text-muted-foreground">
        This node&rsquo;s agent reported no host details.
      </p>
    )
  }

  const cores =
    host.physicalCores > 0 && host.logicalCores > 0
      ? `${host.physicalCores} physical / ${host.logicalCores} logical`
      : host.logicalCores > 0
        ? `${host.logicalCores} logical`
        : null

  // Advertised capacity is what the scheduler places against; detection is
  // what the box reported before `[capacity]` overrides and the system
  // reservation. Showing both only earns its space when they differ.
  const overridden =
    detected !== null &&
    (detected.cpuMillis !== summary.capacity.cpuMillis ||
      detected.memoryBytes !== summary.capacity.memoryBytes ||
      detected.diskBytes !== summary.capacity.diskBytes)

  return (
    <div className="space-y-4">
      <dl className="space-y-2 text-sm">
        <HostFact label="OS" value={formatHostOs(host)} />
        <HostFact label="Kernel" value={formatHostKernel(host)} mono />
        <HostFact label="Arch" value={host.arch} mono />
        <HostFact label="CPU" value={host.cpuModel} />
        <HostFact label="Cores" value={cores} />
        <HostFact
          label="Memory"
          value={host.totalMemoryBytes > 0 ? formatBytes(host.totalMemoryBytes) : null}
        />
        <HostFact
          label="Disk"
          value={host.totalDiskBytes > 0 ? formatBytes(host.totalDiskBytes) : null}
        />
        <HostFact label="Agent" value={host.agentVersion} mono />
      </dl>

      {overridden && detected !== null ? (
        <div className="space-y-2 border-t border-border pt-3">
          <p className="text-xs text-muted-foreground">
            Advertised capacity differs from what this host detected — an operator override or the
            system reservation.
          </p>
          <dl className="space-y-1 text-sm">
            <CapacityDelta
              label="CPU"
              advertised={formatCpu(summary.capacity.cpuMillis)}
              detected={formatCpu(detected.cpuMillis)}
              differs={detected.cpuMillis !== summary.capacity.cpuMillis}
            />
            <CapacityDelta
              label="Memory"
              advertised={formatBytes(summary.capacity.memoryBytes)}
              detected={formatBytes(detected.memoryBytes)}
              differs={detected.memoryBytes !== summary.capacity.memoryBytes}
            />
            <CapacityDelta
              label="Disk"
              advertised={formatBytes(summary.capacity.diskBytes)}
              detected={formatBytes(detected.diskBytes)}
              differs={detected.diskBytes !== summary.capacity.diskBytes}
            />
          </dl>
        </div>
      ) : null}
    </div>
  )
}

/** One host fact; a missing reading reads as `unknown`, never as a value. */
function HostFact({ label, value, mono }: { label: string; value: string | null; mono?: boolean }) {
  return (
    <div className="flex items-baseline justify-between gap-3">
      <dt className="shrink-0 text-muted-foreground">{label}</dt>
      <dd
        className={cn(
          'text-right break-all',
          mono && 'font-mono text-[13px]',
          !value && 'text-muted-foreground italic',
        )}
      >
        {value || 'unknown'}
      </dd>
    </div>
  )
}

function CapacityDelta({
  label,
  advertised,
  detected,
  differs,
}: {
  label: string
  advertised: string
  detected: string
  differs: boolean
}) {
  return (
    <div className="flex items-baseline justify-between gap-3">
      <dt className="shrink-0 text-muted-foreground">{label}</dt>
      <dd className="text-right tabular-nums">
        <span className={cn(differs && 'font-medium')}>{advertised}</span>
        <span className="text-muted-foreground"> of {detected} detected</span>
      </dd>
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
        {summary.lastHeartbeat == null ? 'never' : <TimeAgo t={summary.lastHeartbeat} />}
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

/**
 * The node's capacity in one place: the current advertised/funded/measured
 * figures beside the same three charts the cluster overview uses, over this
 * node's utilization history. A failed or empty history still renders the
 * current figures — they come from the node read, not from this query.
 */
function CapacitySection({ nodeId, summary }: { nodeId: string; summary: NodeSummary }) {
  const { data, isPending, isError } = useNodeUtilization(nodeId)
  const history = useMemo(() => (data ? nodeCapacityHistory(data) : []), [data])

  if (isPending) return <Skeleton className="h-[220px]" />

  return (
    <div className="space-y-3">
      {isError ? (
        <p className="text-xs text-muted-foreground">
          Utilization history is unavailable right now; the figures below are the node&rsquo;s
          latest reading.
        </p>
      ) : null}
      <CapacityHistory
        idPrefix="node-capacity"
        history={history}
        latest={{
          capacity: summary.capacity,
          allocated: summary.allocated,
          used: summary.used,
        }}
      />
    </div>
  )
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
              {a.startedAt == null ? (
                <span className="text-muted-foreground">—</span>
              ) : (
                <TimeAgo t={a.startedAt} className="text-sm text-muted-foreground" />
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
            <TableHead className="text-right">Funding order</TableHead>
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
                {entry.projectedStart == null ? (
                  <span className="text-amber-700 dark:text-amber-300">unbounded</span>
                ) : formatTimeUntil(entry.projectedStart) === 'now' ? (
                  // The guarantee lapsed before apply reaped the release:
                  // say so, never an "imminent" that lingers.
                  <span className="text-amber-700 dark:text-amber-300">
                    due now (release overdue)
                  </span>
                ) : (
                  <span className="tabular-nums">{formatTimeUntil(entry.projectedStart)}</span>
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
        Allocations are funded in sequence as capacity becomes available.
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

function LogsSection({ nodeId }: { nodeId: string }) {
  const { data, isPending } = useNodeLogs(nodeId)
  return (
    <div className="space-y-2">
      <LogViewer entries={data?.entries ?? []} loading={isPending} />
      <p className="text-xs text-muted-foreground">
        Sample data — log collection is not available yet.
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
