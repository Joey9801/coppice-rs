import type { Resources } from '@/api/types'

/**
 * Formatting helpers for the domain's units. Use these everywhere —
 * never hand-roll byte/duration/cost formatting in components.
 *
 * Unit conventions (see src/api/types.ts):
 * - timestamps/durations: microseconds (`Us`)
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
  const digits = value >= 100 || i === 0 ? 0 : 1
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

/** Compact duration: "3h 12m", "45s", "850ms". */
export function formatDurationUs(us: number): string {
  if (us < 0) return '—'
  const ms = us / 1000
  if (ms < 1000) return `${Math.round(ms)}ms`
  const s = Math.floor(ms / 1000)
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return s % 60 ? `${m}m ${s % 60}s` : `${m}m`
  const h = Math.floor(m / 60)
  if (h < 48) return m % 60 ? `${h}h ${m % 60}m` : `${h}h`
  const d = Math.floor(h / 24)
  return h % 24 ? `${d}d ${h % 24}h` : `${d}d`
}

/** "3m ago" for a past µs timestamp (uses Date.now() unless given). */
export function formatTimeAgo(tUs: number, nowMs: number = Date.now()): string {
  const ageUs = nowMs * 1000 - tUs
  if (ageUs < 5_000_000) return 'just now'
  return `${formatDurationUs(ageUs)} ago`
}

/** "in 12m" for a future µs timestamp. */
export function formatTimeUntil(tUs: number, nowMs: number = Date.now()): string {
  const inUs = tUs - nowMs * 1000
  if (inUs <= 0) return 'now'
  return `in ${formatDurationUs(inUs)}`
}

/** Absolute local timestamp for detail views / tooltips. */
export function formatTimestampUs(tUs: number): string {
  return new Date(tUs / 1000).toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

/** "14:03:27" — time-of-day only, for dense log/timeline rows. */
export function formatTimeOfDayUs(tUs: number): string {
  return new Date(tUs / 1000).toLocaleTimeString(undefined, {
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
