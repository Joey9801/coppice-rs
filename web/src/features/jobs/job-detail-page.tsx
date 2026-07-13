import { Fragment, type ReactNode } from 'react'
import { Link } from '@tanstack/react-router'
import { AlertTriangle, ArrowLeft, SearchX } from 'lucide-react'
import {
  type AttemptView,
  type JobDetail,
  type JobId,
  TERMINAL_JOB_STATES,
  type QuotaEntityView,
} from '@/api/types'
import { useJob, useJobLogs } from '@/api/queries'
import {
  formatDurationUs,
  formatPercent,
  formatTimeAgo,
  formatTimestampUs,
  formatUcu,
  formatUcuRatePerHour,
} from '@/lib/format'
import { EmptyState, LogViewer, PageHeader, StatePill, StatTile, TimeAgo } from '@/components'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { JobAccrualPanel } from './job-accrual-panel'
import { JobAttemptsCard } from './job-attempts-card'
import { JobCostCard } from './job-cost-card'
import { JobQueuePanel } from './job-queue-panel'
import { JobSpecCard } from './job-spec-card'
import { JobTimeline } from './job-timeline'
import { JobUsageSection } from './job-usage-section'
import { TrueUpAmount } from './true-up-amount'

export function JobDetailPage({ jobId }: { jobId: JobId }) {
  const job = useJob(jobId)

  if (job.isPending) {
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

/**
 * One stable, scrollable page — no tabs. Every section is always present
 * (so nothing appears or vanishes as the job moves through its lifecycle);
 * only the state-specific panels (queue position, capacity accrual) come
 * and go, and they do so inline where the "why isn't it running yet" story
 * belongs.
 */
function JobDetailView({ job }: { job: JobDetail }) {
  return (
    <div>
      <BackLink />

      <PageHeader
        title={
          <span className="flex flex-wrap items-center gap-2.5">
            <span className="font-mono text-lg break-all">{job.id}</span>
            <StatePill state={job.state} />
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

      <div className="space-y-4">
        {/* Terminal Aborted jobs carry the reason in the state tile; the
            banner is for an abort still in flight (or one that lost the
            race to a natural exit). */}
        {job.abortRequested && job.state !== 'Aborted' ? (
          <Card className="border-destructive/40 bg-destructive/5 p-4">
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

        <HeroTiles job={job} />

        <JobUsageSection job={job} />

        {job.queue ? <JobQueuePanel queue={job.queue} /> : null}
        {job.accrual ? <JobAccrualPanel accrual={job.accrual} /> : null}

        <div className="grid gap-4 lg:grid-cols-5">
          <div className="lg:col-span-3">
            <JobSpecCard job={job} />
          </div>
          <div className="lg:col-span-2">
            <JobCostCard cost={job.cost} />
          </div>
        </div>

        <JobAttemptsCard attempts={job.attempts} currentAttempt={job.currentAttempt} />

        <div className="grid gap-4 xl:grid-cols-2">
          <Card>
            <CardHeader className="p-4 pb-0">
              <CardTitle className="text-sm">Timeline</CardTitle>
            </CardHeader>
            <CardContent className="max-h-96 overflow-auto p-4">
              <JobTimeline jobId={job.id} />
            </CardContent>
          </Card>
          <JobLogsCard jobId={job.id} />
        </div>
      </div>
    </div>
  )
}

function HeroTiles({ job }: { job: JobDetail }) {
  const terminal = TERMINAL_JOB_STATES.includes(job.state)
  const lastAttempt = currentOrLastAttempt(job)
  const nowUs = Date.now() * 1000

  const runtimeUs =
    lastAttempt?.startedAtUs != null
      ? Math.max(0, (lastAttempt.endedAtUs ?? nowUs) - lastAttempt.startedAtUs)
      : null

  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
      <StatTile label="State" value={job.state} hint={stateHint(job, nowUs)} />
      <StatTile
        label="Runtime"
        value={runtimeUs != null ? formatDurationUs(runtimeUs) : '—'}
        hint={
          runtimeUs == null
            ? 'not started'
            : job.spec.maxRuntimeUs != null
              ? `limit ${formatDurationUs(job.spec.maxRuntimeUs)}`
              : 'no runtime limit'
        }
      />
      <StatTile
        label={terminal ? 'Final cost' : 'Cost so far'}
        value={formatUcu(
          terminal && job.cost.actualUcu != null ? job.cost.actualUcu : job.cost.chargedUcu,
        )}
        hint={
          job.cost.trueUp ? (
            <TrueUpAmount trueUp={job.cost.trueUp} />
          ) : (
            formatUcuRatePerHour(job.cost.rateUcuPerSecond)
          )
        }
      />
      <StatTile
        label="Attempts"
        value={`${job.attempts.length} of ${job.spec.retry.maxRetries + 1}`}
        hint={
          job.retriesUsed > 0
            ? `${job.retriesUsed} ${job.retriesUsed === 1 ? 'retry' : 'retries'} used`
            : 'no retries used'
        }
      />
    </div>
  )
}

function currentOrLastAttempt(job: JobDetail): AttemptView | undefined {
  return (
    job.attempts.find((a) => a.id === job.currentAttempt) ?? job.attempts[job.attempts.length - 1]
  )
}

/** State-specific sub-text: how long, and the one detail that matters now. */
function stateHint(job: JobDetail, nowUs: number): ReactNode {
  const inState = formatDurationUs(Math.max(0, nowUs - job.stateSinceUs))
  const outcome = currentOrLastAttempt(job)?.outcome ?? null

  switch (job.state) {
    case 'Submitted':
      return `awaiting admission · ${inState}`
    case 'Accepted':
      return `admitted, entering queue · ${inState}`
    case 'Queued':
      return job.queue
        ? `#${job.queue.rank} of ${job.queue.queueDepth} in queue · waiting ${inState}`
        : `waiting ${inState}`
    case 'Preparing': {
      if (job.accrual) {
        const f = job.accrual.fundedFraction
        const funded = Math.min(f.cpu, f.memory, f.disk)
        return `accruing capacity · ${formatPercent(funded)} funded · ${inState}`
      }
      return `placing on a node · ${inState}`
    }
    case 'Running':
      return `for ${inState}`
    case 'Finalizing':
      return `resolving outcome · ${inState}`
    case 'Succeeded':
      return `exit 0 · finished ${formatTimeAgo(job.stateSinceUs)}`
    case 'Failed': {
      const kind =
        outcome?.kind === 'Exited' ? `exit ${outcome.exitCode ?? '?'}` : (outcome?.kind ?? 'failed')
      return `${kind} · ${formatTimeAgo(job.stateSinceUs)}`
    }
    case 'Aborted':
      return `${job.abortRequested?.reason ?? 'no reason given'} · ${formatTimeAgo(job.stateSinceUs)}`
  }
}

function JobLogsCard({ jobId }: { jobId: JobId }) {
  const logs = useJobLogs(jobId)
  return (
    <Card>
      <CardHeader className="p-4 pb-0">
        <CardTitle className="text-sm">Logs</CardTitle>
      </CardHeader>
      <CardContent className="space-y-2 p-4">
        <LogViewer entries={logs.data?.entries ?? []} loading={logs.isLoading} />
        <p className="text-xs text-muted-foreground">
          Mock data — log storage is not designed in the backend yet.
        </p>
      </CardContent>
    </Card>
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
