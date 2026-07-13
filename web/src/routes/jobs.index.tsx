import { createFileRoute } from '@tanstack/react-router'
import { JOB_PHASES, type JobPhase } from '@/api/types'
import { JobsPage } from '@/features/jobs/jobs-page'

export interface JobsSearch {
  state?: JobPhase
  entity?: string
  node?: string
  q?: string
}

export const Route = createFileRoute('/jobs/')({
  validateSearch: (search: Record<string, unknown>): JobsSearch => {
    const out: JobsSearch = {}
    const { state, entity, node, q } = search
    if (typeof state === 'string' && (JOB_PHASES as readonly string[]).includes(state)) {
      out.state = state as JobPhase
    }
    if (typeof entity === 'string' && entity) out.entity = entity
    if (typeof node === 'string' && node) out.node = node
    if (typeof q === 'string' && q) out.q = q
    return out
  },
  component: JobsPage,
})
