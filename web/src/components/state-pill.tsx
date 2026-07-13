import type { ReactNode } from 'react'
import type {
  AllocationState,
  AttemptOutcome,
  AttemptState,
  JobPhase,
  NodeHealth,
} from '@/api/types'
import { cn } from '@/lib/utils'

export type PillState = JobPhase | AttemptState | AllocationState | NodeHealth | 'Draining'

// State colors intentionally use the Tailwind palette (not semantic tokens):
// each tone is tuned for contrast in both themes via a dark: variant.
type Tone = 'emerald' | 'green' | 'sky' | 'amber' | 'teal' | 'red' | 'orange' | 'violet' | 'slate'

const TONE_CLASS: Record<Tone, string> = {
  emerald: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-500/15 dark:text-emerald-300',
  green: 'bg-green-100 text-green-700 dark:bg-green-500/15 dark:text-green-300',
  sky: 'bg-sky-100 text-sky-700 dark:bg-sky-500/15 dark:text-sky-300',
  amber: 'bg-amber-100 text-amber-800 dark:bg-amber-500/15 dark:text-amber-300',
  teal: 'bg-teal-100 text-teal-700 dark:bg-teal-500/15 dark:text-teal-300',
  red: 'bg-red-100 text-red-700 dark:bg-red-500/15 dark:text-red-300',
  orange: 'bg-orange-100 text-orange-700 dark:bg-orange-500/15 dark:text-orange-300',
  violet: 'bg-violet-100 text-violet-700 dark:bg-violet-500/15 dark:text-violet-300',
  slate: 'bg-muted text-muted-foreground',
}

const STATE_TONE: Record<PillState, Tone> = {
  // greens / blues — the healthy, live, funded states
  Succeeded: 'emerald',
  Healthy: 'emerald',
  Active: 'green',
  Funded: 'green',
  Running: 'sky',
  // amber — waiting / accruing / preparing
  Submitted: 'amber',
  Accepted: 'amber',
  Queued: 'amber',
  Preparing: 'amber',
  Accruing: 'amber',
  Ready: 'amber',
  Dispatching: 'amber',
  // teal — winding down
  Finalizing: 'teal',
  // red — failure / loss
  Failed: 'red',
  Lost: 'red',
  // orange / violet — cancellation / drain
  Aborted: 'orange',
  Draining: 'violet',
  // muted terminal / released
  Terminal: 'slate',
  Released: 'slate',
}

function Pill({
  tone,
  pulse,
  children,
  className,
}: {
  tone: Tone
  pulse?: boolean
  children: ReactNode
  className?: string
}) {
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium whitespace-nowrap',
        TONE_CLASS[tone],
        className,
      )}
    >
      {pulse ? (
        <span className="relative flex size-1.5">
          <span className="absolute inline-flex size-full animate-ping rounded-full bg-current opacity-60" />
          <span className="relative inline-flex size-1.5 rounded-full bg-current" />
        </span>
      ) : null}
      {children}
    </span>
  )
}

export interface StatePillProps {
  state: PillState
  className?: string
}

/** Small colored badge for any job / attempt / allocation / node state. */
export function StatePill({ state, className }: StatePillProps) {
  const tone = STATE_TONE[state]
  return (
    <Pill tone={tone} pulse={state === 'Running'} className={className}>
      {state}
    </Pill>
  )
}

const OUTCOME_LABEL: Record<AttemptOutcome['kind'], string> = {
  Exited: 'Exited',
  OomKilled: 'OOM killed',
  MaxRuntimeExceeded: 'Timed out',
  Aborted: 'Aborted',
  Revoked: 'Revoked',
  PullFailed: 'Pull failed',
  StartFailed: 'Start failed',
  NodeLost: 'Node lost',
  AgentError: 'Agent error',
}

// Color follows who owns the outcome (its class), so the same kind can read
// green or red depending on whether it was expected.
const CLASS_TONE: Record<AttemptOutcome['class'], Tone> = {
  Success: 'emerald',
  UserError: 'red',
  UserRequest: 'orange',
  Platform: 'red',
}

/** A pill describing an attempt's terminal outcome. */
export function outcomePill(outcome: AttemptOutcome): ReactNode {
  const base = OUTCOME_LABEL[outcome.kind]
  const label =
    outcome.kind === 'Exited' && outcome.exitCode != null ? `${base} ${outcome.exitCode}` : base
  const tone =
    outcome.kind === 'Exited' && outcome.class === 'Success' ? 'emerald' : CLASS_TONE[outcome.class]
  return <Pill tone={tone}>{label}</Pill>
}
