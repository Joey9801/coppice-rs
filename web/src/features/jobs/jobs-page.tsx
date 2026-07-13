import { useEffect, useState } from 'react'
import { getRouteApi } from '@tanstack/react-router'
import { ListTodo, Search, X } from 'lucide-react'
import {
  derivePhase,
  JOB_PHASES,
  type JobPhase,
  type JobSummary,
  type ListJobsFilter,
} from '@/api/types'
import { useJobs } from '@/api/queries'
import { formatPercent, formatUcu, shortId } from '@/lib/format'
import { EmptyState, IdLink, outcomePill, PageHeader, StatePill, TimeAgo } from '@/components'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
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
import { useDebouncedValue } from './use-debounced-value'

const route = getRouteApi('/jobs/')

const EMPTY_FILTER: ListJobsFilter = {}

export function JobsPage() {
  const search = route.useSearch()

  const filter: ListJobsFilter = {
    states: search.state ? [search.state] : undefined,
    quotaEntity: search.entity,
    node: search.node,
    search: search.q,
  }

  const jobs = useJobs(filter)
  const all = useJobs(EMPTY_FILTER)

  const isFiltered = Boolean(search.state || search.entity || search.node || search.q)
  const grandTotal = all.data?.total
  const matching = jobs.data?.total

  let description: string | undefined
  if (grandTotal != null) {
    description =
      isFiltered && matching != null
        ? `${grandTotal} jobs · ${matching} matching`
        : `${grandTotal} jobs`
  }

  return (
    <div>
      <PageHeader title="Jobs" description={description} />

      <FilterBar />

      <div className="mt-4 rounded-xl border bg-card">
        {jobs.isLoading ? (
          <TableSkeleton />
        ) : jobs.data && jobs.data.jobs.length > 0 ? (
          <JobsTable jobs={jobs.data.jobs} />
        ) : (
          <EmptyState
            icon={ListTodo}
            title={isFiltered ? 'No jobs match these filters' : 'No jobs yet'}
            description={
              isFiltered
                ? 'Try clearing a filter or widening your search.'
                : 'Submitted jobs will appear here.'
            }
          />
        )}
      </div>
    </div>
  )
}

function FilterBar() {
  const search = route.useSearch()
  const navigate = route.useNavigate()

  const [qInput, setQInput] = useState(search.q ?? '')
  const debouncedQ = useDebouncedValue(qInput, 250)

  useEffect(() => {
    const next = debouncedQ || undefined
    if (next === (search.q ?? undefined)) return
    void navigate({ search: (prev) => ({ ...prev, q: next }), replace: true })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [debouncedQ])

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Select
        aria-label="Filter by state"
        value={search.state ?? ''}
        onChange={(e) =>
          void navigate({
            search: (prev) => ({
              ...prev,
              state: (e.target.value || undefined) as JobPhase | undefined,
            }),
          })
        }
      >
        <option value="">All states</option>
        {JOB_PHASES.map((s) => (
          <option key={s} value={s}>
            {s}
          </option>
        ))}
      </Select>

      <div className="relative w-64 max-w-full">
        <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          className="pl-8"
          placeholder="Search id or image…"
          value={qInput}
          onChange={(e) => setQInput(e.target.value)}
        />
      </div>

      {search.entity ? (
        <FilterChip
          label="entity"
          value={search.entity}
          onClear={() => void navigate({ search: (prev) => ({ ...prev, entity: undefined }) })}
        />
      ) : null}
      {search.node ? (
        <FilterChip
          label="node"
          value={shortId(search.node)}
          onClear={() => void navigate({ search: (prev) => ({ ...prev, node: undefined }) })}
        />
      ) : null}
    </div>
  )
}

function FilterChip({
  label,
  value,
  onClear,
}: {
  label: string
  value: string
  onClear: () => void
}) {
  return (
    <Badge variant="secondary" className="gap-1 py-1 pl-2 pr-1 font-normal">
      <span className="text-muted-foreground">{label}:</span>
      <span className="font-mono">{value}</span>
      <button
        type="button"
        aria-label={`Clear ${label} filter`}
        onClick={onClear}
        className="ml-0.5 inline-flex size-4 items-center justify-center rounded-sm text-muted-foreground hover:bg-background hover:text-foreground"
      >
        <X className="size-3" />
      </button>
    </Badge>
  )
}

function JobsTable({ jobs }: { jobs: JobSummary[] }) {
  const navigate = route.useNavigate()

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Job</TableHead>
          <TableHead>State</TableHead>
          <TableHead>Image</TableHead>
          <TableHead>Entity</TableHead>
          <TableHead className="text-right">Priority</TableHead>
          <TableHead>Submitted</TableHead>
          <TableHead>Where</TableHead>
          <TableHead className="text-right">Cost</TableHead>
          <TableHead>Outcome</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {jobs.map((job) => {
          const phase = derivePhase(job.state, job.attemptState)
          return (
            <TableRow
              key={job.id}
              onClick={() => void navigate({ to: '/jobs/$jobId', params: { jobId: job.id } })}
              className="cursor-pointer"
            >
              <TableCell onClick={(e) => e.stopPropagation()} className="w-px">
                <IdLink id={job.id} />
              </TableCell>
              <TableCell>
                <StatePill state={phase} />
              </TableCell>
              <TableCell className="max-w-[16rem]">
                <span
                  className="block truncate font-mono text-xs text-muted-foreground"
                  title={job.image}
                >
                  {job.image}
                </span>
              </TableCell>
              <TableCell className="whitespace-nowrap">{job.quotaEntityName}</TableCell>
              <TableCell className="text-right tabular-nums">{job.priority}</TableCell>
              <TableCell className="whitespace-nowrap text-muted-foreground">
                <TimeAgo tUs={job.submittedAtUs} />
              </TableCell>
              <TableCell onClick={(e) => e.stopPropagation()} className="whitespace-nowrap">
                <WhereCell job={job} phase={phase} />
              </TableCell>
              <TableCell className="text-right tabular-nums">{formatUcu(job.costUcu)}</TableCell>
              <TableCell>
                {job.outcome && phase !== 'Succeeded' ? (
                  outcomePill(job.outcome)
                ) : (
                  <span className="text-muted-foreground">—</span>
                )}
              </TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}

function WhereCell({ job, phase }: { job: JobSummary; phase: JobPhase }) {
  if (phase === 'Queued' && job.queueRank != null) {
    return <span className="tabular-nums text-muted-foreground">#{job.queueRank}</span>
  }
  if (phase === 'Preparing' && job.fundingFraction != null) {
    return (
      <span className="tabular-nums text-amber-600 dark:text-amber-400">
        {formatPercent(job.fundingFraction)} funded
      </span>
    )
  }
  if (job.node) {
    return <IdLink id={job.node} />
  }
  return <span className="text-muted-foreground">—</span>
}

function TableSkeleton() {
  return (
    <div className="space-y-3 p-4">
      {Array.from({ length: 8 }).map((_, i) => (
        <Skeleton key={i} className="h-8" />
      ))}
    </div>
  )
}
