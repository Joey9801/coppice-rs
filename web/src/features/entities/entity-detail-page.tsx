import { Fragment, useState } from 'react'
import { Link, useNavigate } from '@tanstack/react-router'
import { ArrowLeft, ArrowRight, Network, Plus } from 'lucide-react'
import type {
  JobPhase,
  ListJobsFilter,
  QuotaEntityDetail,
  QuotaEntityNode,
  QuotaEntityView,
} from '@/api/types'
import { derivePhase, JOB_PHASES } from '@/api/types'
import { useJobs, useQuotaEntities, useQuotaEntity } from '@/api/queries'
import { canConfigureEntities, useSession } from '@/auth/session'
import { formatDurationUs, formatPercent, formatUcu } from '@/lib/format'
import { cn } from '@/lib/utils'
import {
  EmptyState,
  IdLink,
  PageHeader,
  SparkLine,
  StatePill,
  StatTile,
  TimeAgo,
} from '@/components'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Select } from '@/components/ui/select'
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
import { UsageBar } from './entities-page'
import { isNotFound, lastSegment } from './lib'

export function EntityDetailPage({ entityId }: { entityId: string }) {
  const { data: detail, isPending, isError, error } = useQuotaEntity(entityId)
  const { data: allEntities } = useQuotaEntities()

  return (
    <div>
      <Link
        to="/entities"
        className="mb-4 inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground"
      >
        <ArrowLeft className="size-4" />
        All entities
      </Link>

      {isPending ? (
        <DetailSkeleton />
      ) : isError ? (
        <Card>
          <EmptyState
            icon={Network}
            title={isNotFound(error) ? 'Entity not found' : "Couldn't load entity"}
            description={
              isNotFound(error) ? (
                <>
                  No entity <span className="font-mono">{entityId}</span> exists in the quota tree.
                </>
              ) : (
                error.message
              )
            }
          />
        </Card>
      ) : (
        <EntityDetailBody detail={detail} allEntities={allEntities ?? []} />
      )}
    </div>
  )
}

function EntityDetailBody({
  detail,
  allEntities,
}: {
  detail: QuotaEntityDetail
  allEntities: QuotaEntityNode[]
}) {
  const { entity, chain, children, stats } = detail
  const { data: session } = useSession()
  const canEdit = canConfigureEntities(session)
  const over = entity.overQuotaRatio > 1

  const usageSeries = stats.usageHistory.map((h) => ({ t: h.tUs, v: h.usageUcu }))

  return (
    <div className="space-y-6">
      <PageHeader
        title={
          <span className="flex flex-wrap items-center gap-2.5">
            <span>{lastSegment(entity.name)}</span>
            {entity.origin === 'sso' ? <Badge variant="secondary">SSO</Badge> : null}
          </span>
        }
        description={
          <span className="flex flex-wrap items-center gap-x-1.5 gap-y-1">
            <Breadcrumb chain={chain} />
            {entity.principal ? (
              <span className="text-muted-foreground">· {entity.principal}</span>
            ) : null}
            <span className="ml-1">
              <IdLink id={entity.id} />
            </span>
          </span>
        }
        actions={
          <Link
            to="/jobs"
            search={{ entity: entity.id }}
            className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline"
          >
            View all jobs
            <ArrowRight className="size-4" />
          </Link>
        }
      />

      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 lg:grid-cols-6">
        <StatTile
          label="Usage"
          value={
            <span className={cn(over && 'text-destructive')}>{formatUcu(entity.usageUcu)}</span>
          }
        >
          {usageSeries.length > 0 ? (
            <SparkLine data={usageSeries} color={over ? 'var(--destructive)' : 'var(--chart-1)'} />
          ) : null}
        </StatTile>
        <StatTile
          label="Quota"
          value={formatUcu(entity.quotaUcu)}
          hint={`${formatPercent(entity.overQuotaRatio)} used`}
        />
        <StatTile
          label="Penalty"
          value={
            <span className={cn(over && 'text-destructive')}>×{entity.penalty.toFixed(2)}</span>
          }
          hint="multiplies queue cost"
        />
        <StatTile
          label="Queued"
          value={entity.queuedCount}
          hint={
            stats.oldestQueuedAgeUs != null
              ? `oldest ${formatDurationUs(stats.oldestQueuedAgeUs)}`
              : 'subtree total'
          }
        />
        <StatTile
          label="Running"
          value={entity.runningCount}
          hint={`${formatUcu(stats.burnRateUcuPerSecond)}/s burn`}
        />
        <StatTile label="Charged 24h" value={formatUcu(stats.chargedUcu24h)} />
      </div>

      {children.length > 0 || canEdit ? (
        <ChildrenCard
          entity={entity}
          childNodes={children}
          canEdit={canEdit}
          allEntities={allEntities}
        />
      ) : null}

      <JobsCard entityId={entity.id} />

      {canEdit ? (
        <Card className="p-6">
          <EntityForm
            mode="edit"
            title="Configure"
            entity={entity}
            allEntities={allEntities}
            onDone={() => {}}
          />
        </Card>
      ) : null}
    </div>
  )
}

function Breadcrumb({ chain }: { chain: QuotaEntityView[] }) {
  if (chain.length === 0) return null
  return (
    <span className="flex flex-wrap items-center gap-x-1 gap-y-1">
      {chain.map((node, i) => {
        const leaf = i === chain.length - 1
        return (
          <Fragment key={node.id}>
            {i > 0 ? <span className="text-muted-foreground">/</span> : null}
            {leaf ? (
              <span className="font-medium text-foreground">{lastSegment(node.name)}</span>
            ) : (
              <Link
                to="/entities/$entityId"
                params={{ entityId: node.id }}
                className="text-muted-foreground hover:text-foreground hover:underline"
              >
                {lastSegment(node.name)}
              </Link>
            )}
          </Fragment>
        )
      })}
    </span>
  )
}

function ChildrenCard({
  entity,
  childNodes,
  canEdit,
  allEntities,
}: {
  entity: QuotaEntityNode
  childNodes: QuotaEntityNode[]
  canEdit: boolean
  allEntities: QuotaEntityNode[]
}) {
  const navigate = useNavigate()
  const [adding, setAdding] = useState(false)

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle>Sub-queues</CardTitle>
        {canEdit ? (
          <Button size="sm" variant="outline" onClick={() => setAdding((v) => !v)}>
            <Plus className="size-4" />
            Add child
          </Button>
        ) : null}
      </CardHeader>
      <CardContent className="space-y-4">
        {adding && canEdit ? (
          <div className="rounded-lg border border-border p-4">
            <EntityForm
              mode="create"
              title="New sub-queue"
              parent={entity}
              allEntities={allEntities}
              onDone={() => setAdding(false)}
            />
          </div>
        ) : null}

        {childNodes.length === 0 ? (
          <EmptyState
            icon={Network}
            title="No sub-queues"
            description="This entity has no children."
          />
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead className="w-56">Usage</TableHead>
                <TableHead className="text-right">Quota</TableHead>
                <TableHead className="text-right">Queued</TableHead>
                <TableHead className="text-right">Running</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {childNodes
                .slice()
                .sort((a, b) => a.name.localeCompare(b.name))
                .map((child) => (
                  <TableRow
                    key={child.id}
                    className="cursor-pointer"
                    onClick={() =>
                      navigate({ to: '/entities/$entityId', params: { entityId: child.id } })
                    }
                  >
                    <TableCell>
                      <span className="flex items-center gap-1.5">
                        <span className="font-medium text-foreground">
                          {lastSegment(child.name)}
                        </span>
                        {child.origin === 'sso' ? (
                          <Badge variant="secondary" className="text-[10px]">
                            SSO
                          </Badge>
                        ) : null}
                      </span>
                    </TableCell>
                    <TableCell>
                      <UsageBar
                        usage={child.usageUcu}
                        quota={child.quotaUcu}
                        over={child.overQuotaRatio > 1}
                      />
                    </TableCell>
                    <TableCell className="text-right tabular-nums text-muted-foreground">
                      {formatUcu(child.quotaUcu)}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{child.queuedCount}</TableCell>
                    <TableCell className="text-right tabular-nums">{child.runningCount}</TableCell>
                  </TableRow>
                ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  )
}

function JobsCard({ entityId }: { entityId: string }) {
  const [state, setState] = useState<JobPhase | ''>('')
  const filter: ListJobsFilter = {
    quotaEntity: entityId,
    states: state ? [state] : undefined,
    limit: 25,
  }
  const { data, isPending } = useJobs(filter)
  const jobs = data?.jobs ?? []

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between gap-3">
        <CardTitle>Jobs in this subtree</CardTitle>
        <Select
          aria-label="Filter by state"
          value={state}
          onChange={(e) => setState((e.target.value || '') as JobPhase | '')}
        >
          <option value="">All states</option>
          {JOB_PHASES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </Select>
      </CardHeader>
      <CardContent>
        {isPending ? (
          <div className="space-y-2">
            {Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-8" />
            ))}
          </div>
        ) : jobs.length === 0 ? (
          <EmptyState title="No jobs" description="No jobs match in this subtree." />
        ) : (
          <>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Job</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead>Image</TableHead>
                  <TableHead className="text-right">Priority</TableHead>
                  <TableHead>Submitted</TableHead>
                  <TableHead className="text-right">Cost</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {jobs.map((job) => (
                  <TableRow key={job.id}>
                    <TableCell>
                      <IdLink id={job.id} />
                    </TableCell>
                    <TableCell>
                      <StatePill state={derivePhase(job.state, job.attemptState)} />
                    </TableCell>
                    <TableCell className="max-w-[16rem]">
                      <span
                        className="block truncate font-mono text-xs text-muted-foreground"
                        title={job.image}
                      >
                        {job.image}
                      </span>
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{job.priority}</TableCell>
                    <TableCell className="whitespace-nowrap text-muted-foreground">
                      <TimeAgo tUs={job.submittedAtUs} />
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatUcu(job.costUcu)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            {data && data.total > jobs.length ? (
              <p className="mt-3 text-xs text-muted-foreground">
                Showing {jobs.length} of {data.total} ·{' '}
                <Link
                  to="/jobs"
                  search={{ entity: entityId }}
                  className="text-primary hover:underline"
                >
                  View all in Jobs
                </Link>
              </p>
            ) : null}
          </>
        )}
      </CardContent>
    </Card>
  )
}

function DetailSkeleton() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-10 w-96" />
      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 lg:grid-cols-6">
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} className="h-24" />
        ))}
      </div>
      <Skeleton className="h-64" />
    </div>
  )
}
