/**
 * Seeded PRNG and generation helpers for the mock world.
 *
 * Everything here is pure and deterministic given a seed, so a `MockWorld`
 * built from a fixed seed (and a pinned `nowUs`) reproduces byte-for-byte.
 * Nothing in this file reads the wall clock.
 */

export const GIB = 1024 ** 3
export const TIB = 1024 ** 4
export const MIB = 1024 ** 2

/** mulberry32 — tiny, fast, good-enough PRNG returning [0, 1). */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

/** Stable 32-bit hash of a string, used to seed per-entity generators. */
export function hashSeed(s: string): number {
  let h = 2166136261
  for (let i = 0; i < s.length; i += 1) {
    h ^= s.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  return h >>> 0
}

/**
 * Fixed virtual epoch for the timestamp half of minted uuids. Ids must NOT
 * encode the real wall clock: the world is rebuilt on every page load, and
 * ids that vary per load would break deep links (/jobs/:id survives a
 * reload only because minting is a pure function of the seed + mint order).
 */
const ID_EPOCH_MS = Date.UTC(2026, 0, 1)

export class Rng {
  private next01: () => number
  private idClockMs: number

  constructor(seed: number) {
    this.next01 = mulberry32(seed)
    this.idClockMs = ID_EPOCH_MS
  }

  /** Raw draw in [0, 1). */
  float(): number {
    return this.next01()
  }

  /** Float in [min, max). */
  range(min: number, max: number): number {
    return min + (max - min) * this.next01()
  }

  /** Integer in [min, max] inclusive. */
  int(min: number, max: number): number {
    return Math.floor(this.range(min, max + 1))
  }

  bool(p = 0.5): boolean {
    return this.next01() < p
  }

  /** Uniform pick from a non-empty array. */
  pick<T>(arr: readonly T[]): T {
    const item = arr[Math.floor(this.next01() * arr.length)]
    if (item === undefined) throw new Error('Rng.pick on empty array')
    return item
  }

  /** Weighted pick from [item, weight] pairs (weights > 0). */
  weighted<T>(entries: ReadonlyArray<readonly [T, number]>): T {
    let total = 0
    for (const [, w] of entries) total += w
    let r = this.next01() * total
    for (const [item, w] of entries) {
      r -= w
      if (r <= 0) return item
    }
    const last = entries[entries.length - 1]
    if (last === undefined) throw new Error('Rng.weighted on empty array')
    return last[0]
  }

  /** In-place Fisher–Yates shuffle, returns the same array. */
  shuffle<T>(arr: T[]): T[] {
    for (let i = arr.length - 1; i > 0; i -= 1) {
      const j = Math.floor(this.next01() * (i + 1))
      const a = arr[i]
      const b = arr[j]
      if (a !== undefined && b !== undefined) {
        arr[i] = b
        arr[j] = a
      }
    }
    return arr
  }

  /** uuid-v7-looking string: 48-bit ms timestamp prefix + seeded randomness. */
  uuidV7(tMs: number): string {
    const ts = Math.max(0, Math.floor(tMs))
    const tsHex = ts.toString(16).padStart(12, '0').slice(-12)
    const hex = (n: number) => {
      let s = ''
      for (let i = 0; i < n; i += 1) s += Math.floor(this.next01() * 16).toString(16)
      return s
    }
    const variant = (8 + Math.floor(this.next01() * 4)).toString(16)
    return `${tsHex.slice(0, 8)}-${tsHex.slice(8, 12)}-7${hex(3)}-${variant}${hex(3)}-${hex(12)}`
  }

  /**
   * Typed id `<prefix>-<uuidv7>` per ADR 0024. The timestamp half comes
   * from a seed-driven virtual clock (see ID_EPOCH_MS), never Date.now,
   * so the same seed mints the same ids on every page load.
   */
  mintId(prefix: string): string {
    // Seconds-to-minutes apart so the uuid timestamp prefixes vary the way
    // real entities minted over hours/days would.
    this.idClockMs += this.int(1_000, 600_000)
    return `${prefix}-${this.uuidV7(this.idClockMs)}`
  }
}

/** Inclusive integer range as an array, e.g. range(1, 3) => [1, 2, 3]. */
export function range(min: number, max: number): number[] {
  const out: number[] = []
  for (let i = min; i <= max; i += 1) out.push(i)
  return out
}

// ---------------------------------------------------------------------------
// Name / image pools
// ---------------------------------------------------------------------------

export const REGISTRY = 'registry.acme.dev'

/** Docker-ish image references, filled with a pooled tag at mint time. */
const IMAGE_REPOS = [
  'research/train-encoder',
  'research/embeddings-index',
  'research/eval-harness',
  'platform/feature-store',
  'platform/log-ingest',
  'platform/etl-runner',
  'product/rank-service',
  'product/thumbnailer',
  'product/batch-scorer',
  'infra/backup-agent',
  'infra/gc-sweeper',
]

const IMAGE_TAGS = ['v1.0', 'v1.4', 'v2.1', 'v3.2', 'v3.7', 'latest', 'nightly', 'sha-9f2c1a']

export function mintImage(rng: Rng): string {
  return `${REGISTRY}/${rng.pick(IMAGE_REPOS)}:${rng.pick(IMAGE_TAGS)}`
}

/** Quota tree name pools: org / division / team → `acme/research/embeddings`. */
export const ORG_NAME = 'acme'

export const DIVISIONS = ['research', 'platform', 'product'] as const

export const TEAMS: Record<string, string[]> = {
  research: ['embeddings', 'training', 'evals'],
  platform: ['ingest', 'storage', 'etl'],
  product: ['ranking', 'media'],
}
