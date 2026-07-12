import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { ChevronDown, ChevronRight, Network, Plus, Search } from 'lucide-react'
import type { QuotaEntityNode } from '@/api/types'
import { useQuotaEntities } from '@/api/queries'
import { canConfigureEntities, useSession } from '@/auth/session'
import { formatUcu } from '@/lib/format'
import { cn } from '@/lib/utils'
import { EmptyState, PageHeader } from '@/components'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { EntityForm } from './entity-form'
import { type EntityTreeNode, buildEntityTree, isUsersRoot, lastSegment, matchingIds } from './lib'

export function EntitiesPage() {
  const { data: entities, isPending, isError } = useQuotaEntities()
  const { data: session } = useSession()
  const canEdit = canConfigureEntities(session)

  const [creating, setCreating] = useState(false)
  const [filter, setFilter] = useState('')
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())
  const seeded = useRef(false)

  const tree = useMemo(() => buildEntityTree(entities ?? []), [entities])

  // Seed expansion to the first two levels once, then leave it to the user —
  // 2s refetches must not collapse the tree back.
  useEffect(() => {
    if (seeded.current || !entities || entities.length === 0) return
    seeded.current = true
    const open = new Set<string>()
    const walk = (n: EntityTreeNode) => {
      if (n.depth <= 1 && n.children.length > 0) open.add(n.node.id)
      n.children.forEach(walk)
    }
    tree.forEach(walk)
    setExpanded(open)
  }, [entities, tree])

  const filtering = filter.trim() !== ''
  const visible = useMemo(
    () => (filtering ? matchingIds(entities ?? [], filter) : null),
    [filtering, entities, filter],
  )

  const rows = useMemo(
    () => flattenRows(tree, { visible, expanded, filtering }),
    [tree, visible, expanded, filtering],
  )

  const toggle = (id: string) =>
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })

  return (
    <div>
      <PageHeader
        title="Entities"
        description={<SummaryLine entities={entities} />}
        actions={
          canEdit ? (
            <Button size="sm" onClick={() => setCreating((v) => !v)}>
              <Plus className="size-4" />
              New entity
            </Button>
          ) : undefined
        }
      />

      {canEdit && creating ? (
        <Card className="mb-6 p-6">
          <EntityForm
            mode="create"
            title="New entity"
            allEntities={entities ?? []}
            onDone={() => setCreating(false)}
          />
        </Card>
      ) : null}

      <div className="mb-4 relative w-72 max-w-full">
        <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          className="pl-8"
          placeholder="Filter by name or principal…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
      </div>

      {isPending ? (
        <TreeSkeleton />
      ) : isError ? (
        <Card>
          <EmptyState
            icon={Network}
            title="Couldn't load entities"
            description="The cluster API is unavailable. Retrying automatically."
          />
        </Card>
      ) : (entities?.length ?? 0) === 0 ? (
        <Card>
          <EmptyState
            icon={Network}
            title="No quota entities"
            description="The quota tree is empty. Create a root entity to get started."
          />
        </Card>
      ) : rows.length === 0 ? (
        <Card>
          <EmptyState
            icon={Search}
            title="No entities match this filter"
            description="Try a different name or principal."
          />
        </Card>
      ) : (
        <Card className="overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead className="w-64">Usage</TableHead>
                <TableHead className="text-right">Quota</TableHead>
                <TableHead className="text-right">Penalty</TableHead>
                <TableHead className="text-right">Queued</TableHead>
                <TableHead className="text-right">Running</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <EntityRow
                  key={row.tree.node.id}
                  row={row}
                  expanded={filtering || expanded.has(row.tree.node.id)}
                  onToggle={toggle}
                />
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
    </div>
  )
}

interface Row {
  tree: EntityTreeNode
  hasChildren: boolean
}

function flattenRows(
  tree: EntityTreeNode[],
  opts: { visible: Set<string> | null; expanded: Set<string>; filtering: boolean },
): Row[] {
  const out: Row[] = []
  const walk = (n: EntityTreeNode) => {
    if (opts.visible && !opts.visible.has(n.node.id)) return
    const kids = opts.visible ? n.children.filter((c) => opts.visible?.has(c.node.id)) : n.children
    out.push({ tree: n, hasChildren: kids.length > 0 })
    const open = opts.filtering || opts.expanded.has(n.node.id)
    if (open) kids.forEach(walk)
  }
  tree.forEach(walk)
  return out
}

function SummaryLine({ entities }: { entities: QuotaEntityNode[] | undefined }) {
  if (!entities) return <>Loading entities…</>
  const sso = entities.filter((e) => e.origin === 'sso').length
  return (
    <>
      Quota-entity tree — soft quotas, decayed usage, and scheduling penalties · {entities.length}{' '}
      {entities.length === 1 ? 'entity' : 'entities'} · {entities.length - sso} configured · {sso}{' '}
      SSO
    </>
  )
}

function EntityRow({
  row,
  expanded,
  onToggle,
}: {
  row: Row
  expanded: boolean
  onToggle: (id: string) => void
}) {
  const navigate = useNavigate()
  const { node, depth } = row.tree
  const over = node.overQuotaRatio > 1

  return (
    <TableRow
      onClick={() => navigate({ to: '/entities/$entityId', params: { entityId: node.id } })}
      className="cursor-pointer"
    >
      <TableCell>
        <div className="flex items-center gap-1.5" style={{ paddingLeft: depth * 18 }}>
          {row.hasChildren ? (
            <button
              type="button"
              aria-label={expanded ? 'Collapse' : 'Expand'}
              onClick={(e) => {
                e.stopPropagation()
                onToggle(node.id)
              }}
              className="inline-flex size-5 shrink-0 items-center justify-center rounded text-muted-foreground hover:bg-accent hover:text-foreground"
            >
              {expanded ? <ChevronDown className="size-4" /> : <ChevronRight className="size-4" />}
            </button>
          ) : (
            <span className="inline-block size-5 shrink-0" />
          )}
          <span className="font-medium text-foreground">{lastSegment(node.name)}</span>
          {node.origin === 'sso' ? (
            <Badge variant="secondary" className="text-[10px]">
              SSO
            </Badge>
          ) : null}
          {isUsersRoot(node) ? (
            <Badge variant="outline" className="text-[10px] text-muted-foreground">
              auto-populated
            </Badge>
          ) : null}
        </div>
      </TableCell>
      <TableCell>
        <UsageBar usage={node.usageUcu} quota={node.quotaUcu} over={over} />
      </TableCell>
      <TableCell className="text-right tabular-nums text-muted-foreground">
        {formatUcu(node.quotaUcu)}
      </TableCell>
      <TableCell
        className={cn(
          'text-right tabular-nums',
          over ? 'font-medium text-destructive' : 'text-muted-foreground',
        )}
      >
        ×{node.penalty.toFixed(2)}
      </TableCell>
      <TableCell className="text-right tabular-nums">{node.queuedCount}</TableCell>
      <TableCell className="text-right tabular-nums">{node.runningCount}</TableCell>
    </TableRow>
  )
}

/** Compact usage-vs-quota bar; destructive tones once usage exceeds quota. */
export function UsageBar({ usage, quota, over }: { usage: number; quota: number; over: boolean }) {
  const fraction = quota > 0 ? Math.min(1, usage / quota) : usage > 0 ? 1 : 0
  return (
    <div className="flex items-center gap-2">
      <div className="relative h-1.5 flex-1 overflow-hidden rounded-full bg-muted">
        <div
          className={cn('h-full rounded-full', over ? 'bg-destructive' : 'bg-primary')}
          style={{ width: `${fraction * 100}%` }}
        />
      </div>
      <span
        className={cn(
          'shrink-0 text-xs tabular-nums',
          over ? 'text-destructive' : 'text-muted-foreground',
        )}
      >
        {formatUcu(usage)} / {formatUcu(quota)}
      </span>
    </div>
  )
}

function TreeSkeleton() {
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
