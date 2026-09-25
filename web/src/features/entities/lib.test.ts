import { describe, expect, it } from 'vitest'
import type { QuotaEntityNode } from '@/api/types'
import { matchingIds } from './lib'

function node(id: string, name: string, path: string, parent: string | null): QuotaEntityNode {
  return { id, name, path, parent, principal: null } as QuotaEntityNode
}

const NODES = [
  node('quota-1', 'acme', 'acme', null),
  node('quota-2', 'research', 'acme/research', 'quota-1'),
  node('quota-3', 'training', 'acme/research/training', 'quota-2'),
  node('quota-4', 'platform', 'acme/platform', 'quota-1'),
  node('quota-5', 'training', 'acme/platform/training', 'quota-4'),
]

describe('matchingIds', () => {
  it('matches a path substring spanning segments and keeps the ancestors', () => {
    expect([...matchingIds(NODES, 'research/tr')].sort()).toEqual(['quota-1', 'quota-2', 'quota-3'])
  })

  it('matches by id', () => {
    expect(matchingIds(NODES, 'quota-5').has('quota-5')).toBe(true)
  })
})
