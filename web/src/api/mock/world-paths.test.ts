import { describe, expect, it } from 'vitest'
import type { QuotaEntityNode } from '../types'
import { isValidEntitySegment } from '../../lib/quota-entity'
import { isMockInvalid, isMockNotFound, isMockRejected, MockWorld } from './world'

const NOW_US = 1_760_000_000_000_000

function expectThrows(fn: () => unknown, check: (e: unknown) => boolean): void {
  let thrown: unknown = undefined
  try {
    fn()
  } catch (e) {
    thrown = e
  }
  expect(thrown).toBeDefined()
  expect(check(thrown)).toBe(true)
}

function byPath(world: MockWorld, path: string): QuotaEntityNode {
  const found = world.listQuotaEntities().find((e) => e.path === path)
  if (!found) throw new Error(`no entity at ${path}`)
  return found
}

describe('MockWorld entity paths (ADR 0045)', () => {
  it('stores grammar-valid segments and derives each path from the parent chain', () => {
    const world = new MockWorld(NOW_US)
    const entities = world.listQuotaEntities()
    const byId = new Map(entities.map((e) => [e.id, e]))
    for (const e of entities) {
      expect(isValidEntitySegment(e.name)).toBe(true)
      const segments: string[] = []
      let cur: QuotaEntityNode | undefined = e
      while (cur) {
        segments.unshift(cur.name)
        cur = cur.parent ? byId.get(cur.parent) : undefined
      }
      expect(e.path).toBe(segments.join('/'))
    }
    expect(entities.some((e) => e.path === 'acme/research/training')).toBe(true)
  })

  it('keeps sibling names unique across the seeded world, roots included', () => {
    const world = new MockWorld(NOW_US)
    const seen = new Set<string>()
    for (const e of world.listQuotaEntities()) {
      const key = `${e.parent ?? '(root)'}\u0000${e.name}`
      expect(seen.has(key)).toBe(false)
      seen.add(key)
    }
  })

  it('names SSO users by the sub local part and keeps the full sub as principal', () => {
    const world = new MockWorld(NOW_US)
    const user = world.listQuotaEntities().find((e) => e.principal === 'alice.chen@acme.dev')!
    expect(user.name).toBe('alice.chen')
    expect(user.path).toBe('users/alice.chen')
  })

  it('rejects a bad segment as invalid', () => {
    const world = new MockWorld(NOW_US)
    for (const name of ['a/b', '', '-x', ' acme', 'quota-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f']) {
      expectThrows(
        () => world.configureQuotaEntity({ entity: null, parent: null, name, quotaUcu: 1 }),
        isMockInvalid,
      )
    }
  })

  it('rejects sibling clashes on create, rename and reparent; allows case variants and self', () => {
    const world = new MockWorld(NOW_US)
    const research = byPath(world, 'acme/research')
    const training = byPath(world, 'acme/research/training')
    const ingest = byPath(world, 'acme/platform/ingest')

    // Create clash, and a root clash.
    expectThrows(
      () =>
        world.configureQuotaEntity({
          entity: null,
          parent: research.id,
          name: 'evals',
          quotaUcu: 1,
        }),
      isMockRejected,
    )
    expectThrows(
      () => world.configureQuotaEntity({ entity: null, parent: null, name: 'acme', quotaUcu: 1 }),
      isMockRejected,
    )
    // Rename clash.
    expectThrows(
      () =>
        world.configureQuotaEntity({
          entity: training.id,
          parent: research.id,
          name: 'evals',
          quotaUcu: training.quotaUcu,
        }),
      isMockRejected,
    )
    // Reparent into a clash: acme/platform/ingest → under research, no clash;
    // a new research child named "ingest" then blocks moving it there.
    world.configureQuotaEntity({ entity: null, parent: research.id, name: 'ingest', quotaUcu: 1 })
    expectThrows(
      () =>
        world.configureQuotaEntity({
          entity: ingest.id,
          parent: research.id,
          name: 'ingest',
          quotaUcu: ingest.quotaUcu,
        }),
      isMockRejected,
    )

    // Case-sensitive: "Evals" is a different name.
    const upper = world.configureQuotaEntity({
      entity: null,
      parent: research.id,
      name: 'Evals',
      quotaUcu: 1,
    })
    expect(upper.path).toBe('acme/research/Evals')
    // An update keeping its own name is fine.
    const same = world.configureQuotaEntity({
      entity: training.id,
      parent: research.id,
      name: 'training',
      quotaUcu: training.quotaUcu + 1,
    })
    expect(same.path).toBe('acme/research/training')
  })

  it('re-derives descendant paths on a rename while ids stay put', () => {
    const world = new MockWorld(NOW_US)
    const research = byPath(world, 'acme/research')
    const training = byPath(world, 'acme/research/training')
    world.configureQuotaEntity({
      entity: research.id,
      parent: research.parent,
      name: 'science',
      quotaUcu: research.quotaUcu,
    })

    const renamed = world.listQuotaEntities().find((e) => e.id === training.id)!
    expect(renamed.path).toBe('acme/science/training')
    expect(renamed.name).toBe('training')

    const job = world.listJobs({ limit: 1000 }).jobs.find((j) => j.quotaEntity === training.id)
    if (job) {
      expect(job.quotaEntityPath).toBe('acme/science/training')
      const detail = world.buildJobDetail(job.id)
      expect(detail.spec.quotaEntityPath).toBe('acme/science/training')
      expect(detail.entityChain.map((v) => v.path)).toEqual([
        'acme',
        'acme/science',
        'acme/science/training',
      ])
    }
    expect(world.buildQuotaEntityDetail(training.id).chain.map((v) => v.name)).toEqual([
      'acme',
      'science',
      'training',
    ])
  })

  it('resolves the detail read by path or id, and 404s an unknown path', () => {
    const world = new MockWorld(NOW_US)
    const training = byPath(world, 'acme/research/training')
    expect(world.buildQuotaEntityDetail('acme/research/training').entity.id).toBe(training.id)
    expect(world.buildQuotaEntityDetail(training.id).entity.path).toBe('acme/research/training')
    expectThrows(() => world.buildQuotaEntityDetail('acme/nope'), isMockNotFound)
  })

  it('filters jobs by path exactly as by id; unknown id matches nothing; bad path is invalid', () => {
    const world = new MockWorld(NOW_US)
    const research = byPath(world, 'acme/research')
    const ids = (ref: string, scope?: 'subtree' | 'exact') =>
      world.listJobs({ filter: { entity: { ref, scope } }, limit: 1000 }).jobs.map((j) => j.id)
    expect(ids('acme/research')).toEqual(ids(research.id))
    expect(ids('acme/research').length).toBeGreaterThan(0)
    expect(ids('acme/research', 'exact')).toEqual(ids(research.id, 'exact'))
    expect(ids('quota-00000000-0000-0000-0000-000000000000')).toEqual([])
    expectThrows(() => ids('acme/reserch'), isMockInvalid)
    expectThrows(() => ids('acme//research'), isMockInvalid)
  })
})
