import { createFileRoute } from '@tanstack/react-router'
import { JOB_PHASES, type JobPhase } from '@/api/types'
import { JobsPage } from '@/features/jobs/jobs-page'

export interface JobsSearch {
  state?: JobPhase
  entity?: string
  node?: string
  q?: string
  /** Metadata key to filter on (ADR 0042); `mval` means nothing without it. */
  mkey?: string
  /** Exact value to match; absent or empty makes the leaf a presence test. */
  mval?: string
}

export const Route = createFileRoute('/jobs/')({
  validateSearch: (search: Record<string, unknown>): JobsSearch => {
    const out: JobsSearch = {}
    const { state, entity, node, q, mkey, mval } = search
    if (typeof state === 'string' && (JOB_PHASES as readonly string[]).includes(state)) {
      out.state = state as JobPhase
    }
    if (typeof entity === 'string' && entity) out.entity = entity
    if (typeof node === 'string' && node) out.node = node
    if (typeof q === 'string' && q) out.q = q
    if (typeof mkey === 'string' && mkey) out.mkey = mkey
    if (typeof mval === 'string' && mval) out.mval = mval
    return out
  },
  component: JobsPage,
})
