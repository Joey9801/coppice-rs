import { useNavigate } from '@tanstack/react-router'
import { Boxes } from 'lucide-react'
import type { NodeSummary } from '@/api/types'
import { useNodes } from '@/api/queries'
import {
  formatDuration,
  formatNodeCapacity,
  formatPercent,
  formatTimestamp,
  resourceFractions,
} from '@/lib/format'
import { cn } from '@/lib/utils'
import { EmptyState, IdLink, PageHeader, StatePill, TimeAgo } from '@/components'
import { Badge } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'
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
import { InterruptionBadge } from './interruption-badge'
import { drainStatus, sortedLabels } from './lib'

export function NodesPage() {
  const { data: nodes, isPending, isError } = useNodes()

  return (
    <div>
      <PageHeader title="Nodes" description={<NodesSummaryLine nodes={nodes} />} />

      {isPending ? (
        <NodesTableSkeleton />
      ) : isError ? (
        <Card>
          <EmptyState
            icon={Boxes}
            title="Couldn't load nodes"
            description="The cluster API is unavailable. Retrying automatically."
          />
        </Card>
      ) : nodes.length === 0 ? (
        <Card>
          <EmptyState
            icon={Boxes}
            title="No nodes registered"
            description="Agents appear here once they register with the coordinator."
          />
        </Card>
      ) : (
        <Card className="overflow-hidden">
          <Table className="min-w-max">
            <TableHeader>
              <TableRow>
                <TableHead>Node</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Labels</TableHead>
                <TableHead>Capacity</TableHead>
                <TableHead className="w-40">CPU alloc</TableHead>
                <TableHead className="w-40">Mem alloc</TableHead>
                <TableHead className="text-right">Used CPU</TableHead>
                <TableHead className="text-right">Run / Accr</TableHead>
                <TableHead className="text-right">Epoch</TableHead>
                <TableHead className="text-right">Heartbeat</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {nodes.map((node) => (
                <NodeRow key={node.id} node={node} />
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
    </div>
  )
}

function NodesSummaryLine({ nodes }: { nodes: NodeSummary[] | undefined }) {
  if (!nodes) return <>Loading nodes…</>
  const open = nodes.filter(
    (n) => n.drain === null && !n.agentDraining && n.health !== 'Lost',
  ).length
  const lost = nodes.filter((n) => n.health === 'Lost').length
  return (
    <>
      {nodes.length} {nodes.length === 1 ? 'node' : 'nodes'} · {open} open · {lost} lost
    </>
  )
}

/**
 * Health, plus the two independent drain facts — the operator's drain record
 * (with its status) and the agent's own shutdown announcement — and the
 * provider interruption the agent reported, if any (ADR 0044 §5).
 */
function StatusCell({ node }: { node: NodeSummary }) {
  // `Unknown` renders as itself: with no heartbeat marks to judge by (a
  // follower serving the read), "Healthy" would be a fabrication.
  const pill =
    node.health === 'Lost' || node.drain === null ? (
      <StatePill state={node.health} />
    ) : (
      <StatePill state="Draining" />
    )
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {pill}
      {node.drain !== null ? (
        <span
          className="text-xs text-muted-foreground"
          title={`Drain requested ${formatTimestamp(node.drain.requestedAt)}${
            node.drain.deadline ? `; deadline ${formatTimestamp(node.drain.deadline)}` : ''
          }${node.drain.noticeAt ? `; notices at ${formatTimestamp(node.drain.noticeAt)}` : ''}${
            node.drain.emptyBy ? `; empty by ${formatTimestamp(node.drain.emptyBy)}` : ''
          }${
            node.drain.maxNoticeShortfallSeconds
              ? `; notice short by up to ${formatDuration(node.drain.maxNoticeShortfallSeconds)}`
              : ''
          }`}
        >
          {drainStatus(node.drain)}
        </span>
      ) : null}
      {node.agentDraining ? (
        <Badge
          variant="outline"
          className="text-[10px]"
          title="The agent announced its own shutdown — it admits nothing"
        >
          agent draining
        </Badge>
      ) : null}
      {node.preemptibleOnly ? <SpotBadge /> : null}
      {node.interruptionDeadline != null ? (
        <InterruptionBadge deadline={node.interruptionDeadline} className="text-[10px]" />
      ) : null}
    </div>
  )
}

function NodeRow({ node }: { node: NodeSummary }) {
  const navigate = useNavigate()
  const lost = node.health === 'Lost'
  const allocFrac = resourceFractions(node.allocated, node.capacity)
  const usedFrac = resourceFractions(node.used, node.capacity)
  const labels = sortedLabels(node.labels)

  return (
    <TableRow
      onClick={() => navigate({ to: '/nodes/$nodeId', params: { nodeId: node.id } })}
      className={cn('cursor-pointer', lost && 'bg-destructive/5')}
    >
      <TableCell onClick={stop}>
        <IdLink id={node.id} />
      </TableCell>
      <TableCell>
        <StatusCell node={node} />
      </TableCell>
      <TableCell>
        {labels.length === 0 ? (
          <span className="text-muted-foreground">—</span>
        ) : (
          <div className="flex flex-wrap gap-1">
            {labels.map(([k, v]) => (
              <Badge key={k} variant="outline" className="font-mono text-[11px]">
                {k}={v}
              </Badge>
            ))}
          </div>
        )}
      </TableCell>
      <TableCell className="whitespace-nowrap tabular-nums text-muted-foreground">
        {formatNodeCapacity(node.capacity)}
      </TableCell>
      <TableCell>
        <MiniBar fraction={allocFrac.cpu} />
      </TableCell>
      <TableCell>
        <MiniBar fraction={allocFrac.memory} />
      </TableCell>
      <TableCell className="text-right tabular-nums">
        {usedFrac.cpu != null ? (
          formatPercent(usedFrac.cpu)
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </TableCell>
      <TableCell className="text-right tabular-nums">
        {node.runningCount} / {node.accruingCount}
      </TableCell>
      <TableCell className="text-right tabular-nums text-muted-foreground">{node.epoch}</TableCell>
      <TableCell className="text-right">
        {node.lastHeartbeat == null ? (
          <span className="text-muted-foreground">never</span>
        ) : (
          <TimeAgo
            t={node.lastHeartbeat}
            className={cn(
              'text-sm tabular-nums',
              lost ? 'text-destructive' : 'text-muted-foreground',
            )}
          />
        )}
      </TableCell>
    </TableRow>
  )
}

function SpotBadge() {
  return (
    <Badge
      variant="outline"
      className="text-[10px]"
      title="Spot capacity — accepts only effectively preemptible work"
    >
      spot
    </Badge>
  )
}

function MiniBar({ fraction }: { fraction: number }) {
  return (
    <div className="flex items-center gap-2">
      <Progress value={fraction} className="h-1.5 flex-1" />
      <span className="w-9 shrink-0 text-right text-xs tabular-nums text-muted-foreground">
        {formatPercent(fraction)}
      </span>
    </div>
  )
}

/** Stop row-navigation when interacting with a cell's own links/buttons. */
function stop(event: { stopPropagation: () => void }) {
  event.stopPropagation()
}

function NodesTableSkeleton() {
  return (
    <Card className="p-4">
      <div className="space-y-3">
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} className="h-9" />
        ))}
      </div>
    </Card>
  )
}
