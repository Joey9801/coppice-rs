import type { Resources } from '@/api/types'

/**
 * Formatting helpers for the domain's units. Use these everywhere —
 * never hand-roll byte/duration/cost formatting in components.
 *
 * Unit conventions (see src/api/types.ts):
 * - instants: `Date`; durations: whole seconds (`Seconds`)
 * - cpu: millicores (`cpuMillis`, 1000 = one core)
 * - cost: µCU (`Ucu`, 1_000_000 µCU = 1 CU)
 */

export const MICRO_PER_COST_UNIT = 1_000_000

/**
 * "job-…3f2e9a1b" — prefix plus the uuid's last 8 hex chars, for tables.
 * Ids are uuidv7 (time-ordered), so the FRONT of the uuid is a timestamp
 * that's nearly identical for ids minted close together — only the random
 * tail distinguishes them at a glance.
 */
export function shortId(id: string): string {
  const dash = id.indexOf('-')
  if (dash < 0) return id
  const tail = id
    .slice(dash + 1)
    .replaceAll('-', '')
    .slice(-8)
  return `${id.slice(0, dash)}-…${tail}`
}

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes)) return '—'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
  let value = bytes
  let i = 0
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024
    i += 1
  }
  const digits = value >= 100 || i === 0 || Number.isInteger(value) ? 0 : 1
  return `${value.toFixed(digits)} ${units[i]}`
}

/** "500m" below one core, "4 cores" / "4.5 cores" above. */
export function formatCpu(cpuMillis: number): string {
  if (cpuMillis < 1000) return `${cpuMillis}m`
  const cores = cpuMillis / 1000
  const digits = Number.isInteger(cores) ? 0 : 1
  return `${cores.toFixed(digits)} ${cores === 1 ? 'core' : 'cores'}`
}

export function formatResources(r: Resources): string {
  return `${formatCpu(r.cpuMillis)} · ${formatBytes(r.memoryBytes)} · ${formatBytes(r.diskBytes)} disk`
}

/** µCU → CU with sensible precision: "12.4 CU", "0.003 CU". */
export function formatUcu(ucu: number): string {
  const cu = ucu / MICRO_PER_COST_UNIT
  if (cu === 0) return '0 CU'
  if (cu >= 100) return `${cu.toFixed(0)} CU`
  if (cu >= 1) return `${cu.toFixed(1)} CU`
  return `${cu.toFixed(3)} CU`
}

/** µCU/second → "X CU/hour", the human-scale burn rate. */
export function formatUcuRatePerHour(rateUcuPerSecond: number): string {
  return `${formatUcu(rateUcuPerSecond * 3600)}/hour`
}

/** A cost/scheduling multiplier: "×2", "×1.5" (trimmed to a few sig figs). */
export function formatMultiplier(multiplier: number): string {
  return `×${Number(multiplier.toPrecision(3))}`
}

/** Byte units, both binary (…iB) and SI (…B), for reciprocal price detection. */
const PRICE_BYTE_UNITS: ReadonlyArray<{ label: string; size: number }> = [
  { label: 'KiB', size: 2 ** 10 },
  { label: 'MiB', size: 2 ** 20 },
  { label: 'GiB', size: 2 ** 30 },
  { label: 'TiB', size: 2 ** 40 },
  { label: 'PiB', size: 2 ** 50 },
  { label: 'kB', size: 1e3 },
  { label: 'MB', size: 1e6 },
  { label: 'GB', size: 1e9 },
  { label: 'TB', size: 1e12 },
  { label: 'PB', size: 1e15 },
]

/** Binary units for the direct-rate fallback, mirroring `formatBytes`. */
const BINARY_BYTE_UNITS: ReadonlyArray<{ label: string; size: number }> = [
  { label: 'B', size: 1 },
  { label: 'KiB', size: 2 ** 10 },
  { label: 'MiB', size: 2 ** 20 },
  { label: 'GiB', size: 2 ** 30 },
  { label: 'TiB', size: 2 ** 40 },
  { label: 'PiB', size: 2 ** 50 },
]

/** CPU's only human unit; `size` is in millicores (the resource's base unit). */
const CORE_UNIT = { label: 'core', size: 1000 }

/**
 * The largest whole number of resource-unit-hours one CU buys, when the weight
 * reciprocates cleanly — which is how operators usually set it ("8 GiB-hours =
 * 1 CU"). Both SI and binary byte scales are tried, since we can't assume which
 * the operator used; the largest unit (smallest count) wins. Returns null when
 * no unit gives a clean whole count.
 */
function cleanUnitReciprocal(
  weightPerBase: number,
  units: ReadonlyArray<{ label: string; size: number }>,
): { count: number; label: string } | null {
  let best: { count: number; label: string } | null = null
  for (const unit of units) {
    const cuPerHourPerUnit = (weightPerBase * unit.size * 3600) / MICRO_PER_COST_UNIT
    if (cuPerHourPerUnit <= 0) continue
    const count = 1 / cuPerHourPerUnit
    const rounded = Math.round(count)
    // Whole count only, kept in a human range; a tight tolerance so a weight
    // that merely lands near an integer (e.g. 138.9) is not snapped to one.
    if (rounded < 1 || rounded > 1024) continue
    if (Math.abs(count - rounded) <= rounded * 5e-4 && (!best || rounded < best.count)) {
      best = { count: rounded, label: unit.label }
    }
  }
  return best
}

/** Largest binary unit `formatBytes` would display `bytes` in. */
function displayByteUnit(bytes: number): { label: string; size: number } {
  let chosen = BINARY_BYTE_UNITS[0]!
  for (const unit of BINARY_BYTE_UNITS) {
    if (bytes >= unit.size) chosen = unit
  }
  return chosen
}

/**
 * The per-unit price of one resource dimension for the cost breakdown.
 * Prefers the reciprocal "N unit-hours/CU" form (see `cleanUnitReciprocal`),
 * falling back to the direct "X CU/hour / unit" rate — priced per the
 * resource's own display unit — when no clean reciprocal exists.
 *
 * `ratePerSecond` is the total µCU/s for `quantity` of the resource; `quantity`
 * is millicores for `cpu`, bytes otherwise. Returns null when nothing was
 * requested (no price to quote).
 */
export function formatUnitPrice(
  ratePerSecond: number,
  kind: 'cpu' | 'bytes',
  quantity: number,
): string | null {
  if (quantity <= 0 || ratePerSecond <= 0) return null
  const weightPerBase = ratePerSecond / quantity

  const clean = cleanUnitReciprocal(weightPerBase, kind === 'cpu' ? [CORE_UNIT] : PRICE_BYTE_UNITS)
  if (clean) {
    return `${clean.count} ${clean.label}-hour${clean.count === 1 ? '' : 's'}/CU`
  }

  const unit = kind === 'cpu' ? CORE_UNIT : displayByteUnit(quantity)
  return `${formatUcuRatePerHour(weightPerBase * unit.size)} / ${unit.label}`
}

/**
 * Compact duration: "3h 12m", "45s", "850ms".
 *
 * Takes **seconds**, the unit the API uses for durations. Fractional is fine
 * — that is what the sub-second branch renders — so a `Date` difference
 * arrives here as `ms / 1000`, never as raw milliseconds.
 */
export function formatDuration(seconds: number): string {
  if (seconds < 0) return '—'
  const ms = seconds * 1000
  if (ms < 1000) return `${Math.round(ms)}ms`
  const s = Math.floor(seconds)
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return s % 60 ? `${m}m ${s % 60}s` : `${m}m`
  const h = Math.floor(m / 60)
  if (h < 48) return m % 60 ? `${h}h ${m % 60}m` : `${h}h`
  const d = Math.floor(h / 24)
  return h % 24 ? `${d}d ${h % 24}h` : `${d}d`
}

/** "3m ago" for a past instant (uses the current time unless given). */
export function formatTimeAgo(t: Date, now: Date = new Date()): string {
  const ageSeconds = (now.getTime() - t.getTime()) / 1000
  if (ageSeconds < 5) return 'just now'
  return `${formatDuration(ageSeconds)} ago`
}

/** "in 12m" for a future instant. */
export function formatTimeUntil(t: Date, now: Date = new Date()): string {
  const inSeconds = (t.getTime() - now.getTime()) / 1000
  if (inSeconds <= 0) return 'now'
  return `in ${formatDuration(inSeconds)}`
}

/** Absolute local timestamp for detail views / tooltips. */
export function formatTimestamp(t: Date): string {
  return t.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

/** "14:03:27" — time-of-day only, for dense log/timeline rows. */
export function formatTimeOfDay(t: Date): string {
  return t.toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
}

export function formatPercent(fraction: number, digits = 0): string {
  return `${(fraction * 100).toFixed(digits)}%`
}

/** Per-dimension fraction of `part` over `whole` (0 when whole is 0). */
export function resourceFractions(part: Resources, whole: Resources) {
  const frac = (a: number, b: number) => (b > 0 ? a / b : 0)
  return {
    cpu: frac(part.cpuMillis, whole.cpuMillis),
    memory: frac(part.memoryBytes, whole.memoryBytes),
    disk: frac(part.diskBytes, whole.diskBytes),
  }
}
