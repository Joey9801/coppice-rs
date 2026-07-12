import { createFileRoute } from '@tanstack/react-router'
import { JOB_STATES, type JobState } from '@/api/types'
import { JobsPage } from '@/features/jobs/jobs-page'

export interface JobsSearch {
  state?: JobState
  entity?: string
  node?: string
  q?: string
}

export const Route = createFileRoute('/jobs/')({
  validateSearch: (search: Record<string, unknown>): JobsSearch => {
    const out: JobsSearch = {}
    const { state, entity, node, q } = search
    if (typeof state === 'string' && (JOB_STATES as readonly string[]).includes(state)) {
      out.state = state as JobState
    }
    if (typeof entity === 'string' && entity) out.entity = entity
    if (typeof node === 'string' && node) out.node = node
    if (typeof q === 'string' && q) out.q = q
    return out
  },
  component: JobsPage,
})
