import { Crown } from 'lucide-react'
import type { CoordinatorMember, CoordinatorRole } from '@/api/types'
import { formatPercent } from '@/lib/format'
import { TimeAgo } from '@/components'
import { Badge } from '@/components/ui/badge'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { cn } from '@/lib/utils'

const ROLE_VARIANT: Record<CoordinatorRole, 'default' | 'secondary' | 'outline'> = {
  Leader: 'default',
  Follower: 'secondary',
  Learner: 'outline',
}

function LagCell({ lag }: { lag: number }) {
  if (lag === 0) return <span className="text-muted-foreground">0</span>
  const cls = lag >= 10 ? 'text-red-600 dark:text-red-400' : 'text-amber-600 dark:text-amber-400'
  return <span className={cn('tabular-nums', cls)}>{lag.toLocaleString()} behind</span>
}

function HostBars({ host }: { host: CoordinatorMember['host'] }) {
  const rows = [
    { label: 'CPU', value: host.cpuFraction },
    { label: 'Mem', value: host.memoryFraction },
    { label: 'Disk', value: host.diskFraction },
  ]
  return (
    <div className="flex min-w-[9rem] flex-col gap-1">
      {rows.map((r) => (
        <div key={r.label} className="flex items-center gap-2">
          <span className="w-8 shrink-0 text-xs text-muted-foreground">{r.label}</span>
          <Progress value={r.value} className="h-1.5 flex-1" />
          <span className="w-9 shrink-0 text-right text-xs tabular-nums text-muted-foreground">
            {formatPercent(r.value)}
          </span>
        </div>
      ))}
    </div>
  )
}

export interface MembershipCardProps {
  members: CoordinatorMember[]
  leader: number | null
  className?: string
}

export function MembershipCard({ members, leader, className }: MembershipCardProps) {
  return (
    <Card className={className}>
      <CardHeader>
        <CardTitle>Membership</CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Member</TableHead>
              <TableHead>Address</TableHead>
              <TableHead>Role</TableHead>
              <TableHead>Voter</TableHead>
              <TableHead className="text-right">Last applied</TableHead>
              <TableHead className="text-right">Lag</TableHead>
              <TableHead>Host</TableHead>
              <TableHead>Last seen</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {members.map((m) => (
              <TableRow key={m.id}>
                <TableCell className="font-medium">
                  <span className="flex items-center gap-1.5">
                    {m.id === leader ? (
                      <Crown className="size-3.5 text-amber-500" aria-label="leader" />
                    ) : null}
                    coordinator {m.id}
                  </span>
                </TableCell>
                <TableCell className="font-mono text-xs text-muted-foreground">{m.addr}</TableCell>
                <TableCell>
                  <Badge variant={ROLE_VARIANT[m.role]}>{m.role}</Badge>
                </TableCell>
                <TableCell>
                  {m.voter ? (
                    <span aria-label="voter">✓</span>
                  ) : (
                    <span className="text-muted-foreground" aria-label="non-voter">
                      —
                    </span>
                  )}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {m.lastApplied.toLocaleString()}
                </TableCell>
                <TableCell className="text-right">
                  <LagCell lag={m.replicationLagEntries} />
                </TableCell>
                <TableCell>
                  <HostBars host={m.host} />
                </TableCell>
                <TableCell>
                  <TimeAgo tUs={m.lastSeenUs} className="text-muted-foreground" />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}
