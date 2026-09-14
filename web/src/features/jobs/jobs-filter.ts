import type { JobFilter } from '@/api/types'
import type { JobsSearch } from '@/routes/jobs.index'

/**
 * Build the `JobFilter` AST from the URL search params: each present param is
 * a leaf, ANDed together with `all`; a single leaf is used bare, and no params
 * means no filter (match everything).
 *
 * The metadata leaf (ADR 0042) needs only `mkey` — that alone is a presence
 * test. `mval` adds exact string equality, and only once it is non-empty, so
 * a half-typed value degrades to presence instead of filtering on `""`.
 */
export function buildFilter(search: JobsSearch): JobFilter | undefined {
  const leaves: JobFilter[] = []
  if (search.state) leaves.push({ phase: { in: [search.state] } })
  if (search.entity) leaves.push({ entity: { id: search.entity } })
  if (search.node) leaves.push({ node: search.node })
  if (search.q) leaves.push({ search: search.q })
  if (search.mkey) {
    const value = search.mval ?? ''
    leaves.push(
      value
        ? { metadata: { key: search.mkey, equals: value } }
        : { metadata: { key: search.mkey } },
    )
  }
  if (leaves.length === 0) return undefined
  if (leaves.length === 1) return leaves[0]
  return { all: leaves }
}
