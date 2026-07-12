import type { QuotaEntityNode } from '@/api/types'
import { MICRO_PER_COST_UNIT } from '@/lib/format'

/** True when a query error is an ApiError with code `NotFound`. */
export function isNotFound(error: unknown): boolean {
  return (
    typeof error === 'object' && error !== null && (error as { code?: unknown }).code === 'NotFound'
  )
}

/** Last segment of a slash path ("Acme/Eng/Platform" → "Platform"). */
export function lastSegment(name: string): string {
  const i = name.lastIndexOf('/')
  return i < 0 ? name : name.slice(i + 1)
}

/** The path up to and including the trailing slash ("Acme/Eng/" for "Acme/Eng/Platform"). */
export function parentPrefix(name: string): string {
  const i = name.lastIndexOf('/')
  return i < 0 ? '' : name.slice(0, i + 1)
}

/** The reserved auto-populated user tree root (`users`, ADR 0022). */
export function isUsersRoot(node: QuotaEntityNode): boolean {
  return node.parent === null && node.name === 'users'
}

export function ucuToCostUnits(ucu: number): number {
  return ucu / MICRO_PER_COST_UNIT
}

export function costUnitsToUcu(costUnits: number): number {
  return Math.round(costUnits * MICRO_PER_COST_UNIT)
}

export interface EntityTreeNode {
  node: QuotaEntityNode
  depth: number
  children: EntityTreeNode[]
}

/**
 * Builds the quota tree from the flat list: children grouped by parent and
 * ordered by name, roots (parent null or a parent not in the set) first by
 * name. Depth is the node's distance from its root.
 */
export function buildEntityTree(nodes: QuotaEntityNode[]): EntityTreeNode[] {
  const ids = new Set(nodes.map((n) => n.id))
  const byParent = new Map<string, QuotaEntityNode[]>()
  for (const node of nodes) {
    if (node.parent === null || !ids.has(node.parent)) continue
    const siblings = byParent.get(node.parent) ?? []
    siblings.push(node)
    byParent.set(node.parent, siblings)
  }

  const build = (node: QuotaEntityNode, depth: number): EntityTreeNode => {
    const kids = (byParent.get(node.id) ?? []).slice().sort((a, b) => a.name.localeCompare(b.name))
    return { node, depth, children: kids.map((k) => build(k, depth + 1)) }
  }

  return nodes
    .filter((n) => n.parent === null || !ids.has(n.parent))
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((root) => build(root, 0))
}

/** Pre-order flattening of the tree, useful for parent `<select>` options. */
export function flattenTree(tree: EntityTreeNode[]): EntityTreeNode[] {
  const out: EntityTreeNode[] = []
  const walk = (n: EntityTreeNode) => {
    out.push(n)
    n.children.forEach(walk)
  }
  tree.forEach(walk)
  return out
}

/**
 * Ids that stay visible under a name/principal filter: every matching node
 * plus all of its ancestors, so the path to each match is preserved.
 */
export function matchingIds(nodes: QuotaEntityNode[], query: string): Set<string> {
  const q = query.trim().toLowerCase()
  const byId = new Map(nodes.map((n) => [n.id, n]))
  const visible = new Set<string>()
  for (const node of nodes) {
    const hit =
      node.name.toLowerCase().includes(q) || (node.principal?.toLowerCase().includes(q) ?? false)
    if (!hit) continue
    let cur: QuotaEntityNode | undefined = node
    while (cur && !visible.has(cur.id)) {
      visible.add(cur.id)
      cur = cur.parent ? byId.get(cur.parent) : undefined
    }
  }
  return visible
}

/** The set of `id` and all of its descendants — parents we must not offer. */
export function descendantIds(nodes: QuotaEntityNode[], id: string): Set<string> {
  const childrenOf = new Map<string, QuotaEntityNode[]>()
  for (const node of nodes) {
    if (node.parent === null) continue
    const kids = childrenOf.get(node.parent) ?? []
    kids.push(node)
    childrenOf.set(node.parent, kids)
  }
  const out = new Set<string>([id])
  const stack = [id]
  while (stack.length > 0) {
    const cur = stack.pop() as string
    for (const child of childrenOf.get(cur) ?? []) {
      if (!out.has(child.id)) {
        out.add(child.id)
        stack.push(child.id)
      }
    }
  }
  return out
}
