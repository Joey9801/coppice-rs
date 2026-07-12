import type { AttemptView, JobDetail } from '@/api/types'
import { formatDurationUs, formatResources, formatUcu, shortId } from '@/lib/format'
import { IdLink, KeyValueGrid, outcomePill, StatePill, TimeAgo } from '@/components'
import { cn } from '@/lib/utils'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

export function JobSpecTab({ job }: { job: JobDetail }) {
  const { spec } = job
  const entity = job.entityChain[job.entityChain.length - 1]

  const items = [
    { label: 'Image', value: <span className="font-mono text-xs break-all">{spec.image}</span> },
    { label: 'Resources', value: formatResources(spec.requests) },
    { label: 'Priority class', value: <span className="tabular-nums">{spec.priority}</span> },
    {
      label: 'Max runtime',
      value:
        spec.maxRuntimeUs != null ? (
          formatDurationUs(spec.maxRuntimeUs)
        ) : (
          <span className="text-muted-foreground">none — cost estimate uses policy default</span>
        ),
    },
    {
      label: 'Retry policy',
      value: `up to ${spec.retry.maxRetries} ${
        spec.retry.maxRetries === 1 ? 'retry' : 'retries'
      }, user errors: ${spec.retry.retryUserErrors ? 'yes' : 'no'}`,
    },
    {
      label: 'Quota entity',
      value: (
        <span>
          {entity ? entity.name : '—'}{' '}
          <span className="ml-1 font-mono text-xs text-muted-foreground">
            {shortId(spec.quotaEntity)}
          </span>
        </span>
      ),
    },
  ]

  return (
    <div className="space-y-6">
      <KeyValueGrid items={items} />

      <div>
        <h3 className="mb-2 text-sm font-medium text-foreground">
          Attempts ({job.attempts.length})
        </h3>
        <AttemptsTable attempts={job.attempts} currentAttempt={job.currentAttempt} />
      </div>
    </div>
  )
}

function AttemptsTable({
  attempts,
  currentAttempt,
}: {
  attempts: AttemptView[]
  currentAttempt: string | null
}) {
  return (
    <div className="rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Attempt</TableHead>
            <TableHead>Node</TableHead>
            <TableHead>State</TableHead>
            <TableHead>Outcome</TableHead>
            <TableHead>Started</TableHead>
            <TableHead>Ended</TableHead>
            <TableHead className="text-right">Charged</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {attempts.map((attempt) => {
            const isCurrent = attempt.id === currentAttempt
            return (
              <TableRow key={attempt.id} className={cn(isCurrent && 'bg-muted/40')}>
                <TableCell className="whitespace-nowrap font-mono text-xs">
                  {shortId(attempt.id)}
                  {isCurrent ? (
                    <span className="ml-2 rounded-full bg-primary/10 px-1.5 py-0.5 text-[10px] font-medium text-primary">
                      current
                    </span>
                  ) : null}
                </TableCell>
                <TableCell>
                  <IdLink id={attempt.node} />
                </TableCell>
                <TableCell>
                  <StatePill state={attempt.state} />
                </TableCell>
                <TableCell>
                  {attempt.outcome ? (
                    outcomePill(attempt.outcome)
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </TableCell>
                <TableCell className="whitespace-nowrap text-muted-foreground">
                  {attempt.startedAtUs != null ? <TimeAgo tUs={attempt.startedAtUs} /> : '—'}
                </TableCell>
                <TableCell className="whitespace-nowrap text-muted-foreground">
                  {attempt.endedAtUs != null ? <TimeAgo tUs={attempt.endedAtUs} /> : '—'}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {formatUcu(attempt.chargedUcu)}
                </TableCell>
              </TableRow>
            )
          })}
        </TableBody>
      </Table>
    </div>
  )
}
