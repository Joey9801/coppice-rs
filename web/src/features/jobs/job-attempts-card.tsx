import type { AttemptId, AttemptView } from '@/api/types'
import { formatUcu, shortId } from '@/lib/format'
import { IdLink, outcomePill, StatePill, TimeAgo } from '@/components'
import { cn } from '@/lib/utils'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

export function JobAttemptsCard({
  attempts,
  currentAttempt,
}: {
  attempts: AttemptView[]
  currentAttempt: AttemptId | null
}) {
  return (
    <Card>
      <CardHeader className="p-4 pb-0">
        <CardTitle className="text-sm">Attempts ({attempts.length})</CardTitle>
      </CardHeader>
      <CardContent className="p-4">
        {attempts.length === 0 ? (
          <p className="py-4 text-center text-sm text-muted-foreground">
            No attempts yet — the job hasn't been placed on a node.
          </p>
        ) : (
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
        )}
      </CardContent>
    </Card>
  )
}
