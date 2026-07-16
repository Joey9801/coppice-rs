import { useNavigate } from '@tanstack/react-router'
import { Boxes } from 'lucide-react'
import type { NodeSummary } from '@/api/types'
import { useNodes } from '@/api/queries'
import { formatBytes, formatCpu, formatPercent, resourceFractions } from '@/lib/format'
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
import { sortedLabels } from './lib'

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
          <Table>
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
  const schedulable = nodes.filter((n) => n.schedulable && n.health !== 'Lost').length
  const lost = nodes.filter((n) => n.health === 'Lost').length
  return (
    <>
      {nodes.length} {nodes.length === 1 ? 'node' : 'nodes'} · {schedulable} schedulable · {lost}{' '}
      lost
    </>
  )
}

function statusPill(node: NodeSummary) {
  if (node.health === 'Lost') return <StatePill state="Lost" />
  if (!node.schedulable) return <StatePill state="Draining" />
  return <StatePill state="Healthy" />
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
      <TableCell>{statusPill(node)}</TableCell>
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
        {formatCpu(node.capacity.cpuMillis)} · {formatBytes(node.capacity.memoryBytes)}
      </TableCell>
      <TableCell>
        <MiniBar fraction={allocFrac.cpu} />
      </TableCell>
      <TableCell>
        <MiniBar fraction={allocFrac.memory} />
      </TableCell>
      <TableCell className="text-right tabular-nums">{formatPercent(usedFrac.cpu)}</TableCell>
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
