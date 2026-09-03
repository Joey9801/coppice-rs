import type { AttemptView } from '@/api/types'

/**
 * A job's current-attempt runtime in seconds, for the detail page's
 * Runtime tile. The span always ends at the attempt's own replicated
 * `endedAt` stamp — never the browser's clock. Only a live attempt (no end
 * recorded yet) measures up to `now`; a terminal attempt without a stamp
 * (one that ended before the field existed) has no honest runtime, and an
 * attempt that terminated before starting has none either — both read as
 * `null` so the tile can say "never started" instead of guessing.
 */
export function attemptRuntimeSeconds(
  attempt: Pick<AttemptView, 'state' | 'startedAt' | 'endedAt'> | null | undefined,
  now: Date,
): number | null {
  if (attempt?.startedAt == null) return null
  const endedAt = attempt.state === 'Terminal' ? attempt.endedAt : (attempt.endedAt ?? now)
  if (endedAt == null) return null
  return Math.max(0, (endedAt.getTime() - attempt.startedAt.getTime()) / 1000)
}
