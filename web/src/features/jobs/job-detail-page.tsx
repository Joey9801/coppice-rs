import { Fragment, type ReactNode } from 'react'
import { Link } from '@tanstack/react-router'
import { AlertTriangle, ArrowLeft, SearchX } from 'lucide-react'
import {
  derivePhase,
  isTerminalJobState,
  jobCurrentAttempt,
  type JobDetail,
  type JobId,
  type QuotaEntityView,
} from '@/api/types'
import { useJob } from '@/api/queries'
import { formatTimestampUs, formatUcu } from '@/lib/format'
import { EmptyState, PageHeader, StatePill, StatTile, TimeAgo } from '@/components'
import { Badge } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { JobAccrualTab } from './job-accrual-tab'
import { JobCostTab } from './job-cost-tab'
import { JobLogsTab } from './job-logs-tab'
import { JobQueueTab } from './job-queue-tab'
import { JobSpecTab } from './job-spec-tab'
import { JobTimelineTab } from './job-timeline-tab'

function ratePerHour(rateUcuPerSecond: number): string {
  return `${formatUcu(rateUcuPerSecond * 3600)}/hour`
}

export function JobDetailPage({ jobId }: { jobId: JobId }) {
  const job = useJob(jobId)

  if (job.isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-96" />
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-24" />
          ))}
        </div>
        <Skeleton className="h-64" />
      </div>
    )
  }

  if (job.error) {
    const notFound = (job.error as { code?: string }).code === 'NotFound'
    return (
      <div>
        <BackLink />
        <EmptyState
          icon={SearchX}
          title={notFound ? 'Job not found' : 'Could not load job'}
          description={
            notFound ? (
              <>
                No job with id <span className="font-mono">{jobId}</span> exists.
              </>
            ) : (
              job.error.message
            )
          }
        />
      </div>
    )
  }

  if (!job.data) return null

  return <JobDetailView job={job.data} />
}

function JobDetailView({ job }: { job: JobDetail }) {
  const terminal = isTerminalJobState(job.state)
  const attempt = jobCurrentAttempt(job)
  const phase = derivePhase(job.state, attempt?.state ?? null)

  return (
    <div>
      <BackLink />

      <PageHeader
        title={
          <span className="flex flex-wrap items-center gap-2.5">
            <span className="font-mono text-lg break-all">{job.id}</span>
            <StatePill state={phase} />
          </span>
        }
        description={
          <span className="flex flex-wrap items-center gap-x-1.5 gap-y-1">
            <EntityChain chain={job.entityChain} />
            <span className="text-muted-foreground">· submitted</span>
            <TimeAgo tUs={job.submittedAtUs} />
          </span>
        }
      />

      {job.abortRequested ? (
        <Card className="mb-6 border-destructive/40 bg-destructive/5 p-4">
          <div className="flex gap-3">
            <AlertTriangle className="mt-0.5 size-4 shrink-0 text-destructive" />
            <div className="text-sm">
              <p className="font-medium text-foreground">Abort requested</p>
              <p className="text-muted-foreground">
                {job.abortRequested.reason ?? 'No reason given'} ·{' '}
                {formatTimestampUs(job.abortRequested.requestedAtUs)}
              </p>
            </div>
          </div>
        </Card>
      ) : null}

      <div className="mb-6 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatTile label="Cost so far" value={formatUcu(job.cost.chargedUcu)} />
        <StatTile
          label="Actual"
          value={terminal && job.cost.actualUcu != null ? formatUcu(job.cost.actualUcu) : '—'}
          hint={
            job.cost.trueUp ? (
              <span
                className={
                  job.cost.trueUp.kind === 'Refund'
                    ? 'text-emerald-600 dark:text-emerald-400'
                    : 'text-amber-600 dark:text-amber-400'
                }
              >
                {job.cost.trueUp.kind} {formatUcu(job.cost.trueUp.amountUcu)}
              </span>
            ) : undefined
          }
        />
        <StatTile label="Rate" value={ratePerHour(job.cost.rateUcuPerSecond)} />
        <StatTile
          label="Attempts"
          value={`${job.retriesUsed + 1} of ${job.spec.retry.maxRetries + 1}`}
          hint="used"
        />
      </div>

      <Tabs defaultValue="timeline">
        <TabsList className="flex-wrap">
          <TabsTrigger value="timeline">Timeline</TabsTrigger>
          <TabsTrigger value="spec">Spec</TabsTrigger>
          {job.queue ? <TabsTrigger value="queue">Queue position</TabsTrigger> : null}
          {job.accrual ? <TabsTrigger value="accrual">Accrual</TabsTrigger> : null}
          <TabsTrigger value="cost">Cost &amp; usage</TabsTrigger>
          <TabsTrigger value="logs">Logs</TabsTrigger>
        </TabsList>

        <div className="mt-4">
          <TabsContent value="timeline">
            <JobTimelineTab jobId={job.id} />
          </TabsContent>
          <TabsContent value="spec">
            <JobSpecTab job={job} />
          </TabsContent>
          {job.queue ? (
            <TabsContent value="queue">
              <JobQueueTab queue={job.queue} />
            </TabsContent>
          ) : null}
          {job.accrual ? (
            <TabsContent value="accrual">
              <JobAccrualTab accrual={job.accrual} />
            </TabsContent>
          ) : null}
          <TabsContent value="cost">
            <JobCostTab jobId={job.id} cost={job.cost} />
          </TabsContent>
          <TabsContent value="logs">
            <JobLogsTab jobId={job.id} />
          </TabsContent>
        </div>
      </Tabs>
    </div>
  )
}

function EntityChain({ chain }: { chain: QuotaEntityView[] }): ReactNode {
  if (chain.length === 0) return <span className="text-muted-foreground">no entity</span>
  return (
    <span className="flex flex-wrap items-center gap-x-1 gap-y-1">
      {chain.map((entity, i) => {
        const leaf = i === chain.length - 1
        return (
          <Fragment key={entity.id}>
            {i > 0 ? <span className="text-muted-foreground">›</span> : null}
            <span className={leaf ? 'font-medium text-foreground' : 'text-muted-foreground'}>
              {entity.name}
            </span>
            {entity.penalty > 1 ? (
              <Badge
                variant="outline"
                className="border-amber-500/40 text-amber-600 dark:text-amber-400"
              >
                over quota ×{Number(entity.penalty.toPrecision(3))}
              </Badge>
            ) : null}
          </Fragment>
        )
      })}
    </span>
  )
}

function BackLink() {
  return (
    <Link
      to="/jobs"
      className="mb-3 inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground"
    >
      <ArrowLeft className="size-4" />
      Jobs
    </Link>
  )
}
