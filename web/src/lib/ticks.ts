/**
 * "Nice" axis ticks for the usage/utilization charts.
 *
 * Recharts' default tick generation divides the data extent evenly, which
 * produces labels like "3.7 GiB" / "7.4 GiB" — values derived from the data
 * scale rather than values a human would count in. These helpers instead
 * choose a round step (1/2/2.5/5 × a power of ten, in the unit the values
 * will be *formatted* in) and return the full tick array `[0, step, …, top]`
 * with `top ≥ max`, so `<YAxis ticks={…} domain={[0, ticks.at(-1)]} />`
 * lands every label on a neat value.
 */

const NICE_MULTIPLIERS = [1, 2, 2.5, 5, 10]

/** Smallest nice value ≥ `rough` (nice = 1/2/2.5/5 × 10^k). */
function niceStep(rough: number, multipliers: readonly number[] = NICE_MULTIPLIERS): number {
  if (!(rough > 0)) return 1
  const magnitude = 10 ** Math.floor(Math.log10(rough))
  for (const m of multipliers) {
    if (m * magnitude >= rough) return m * magnitude
  }
  return 10 * magnitude
}

function ticksFromStep(max: number, step: number): number[] {
  const count = Math.max(1, Math.ceil(max / step - 1e-9))
  return Array.from({ length: count + 1 }, (_, i) => i * step)
}

/** Ticks at nice decimal values covering [0, max] with ~`target` intervals. */
export function linearTicks(max: number, target = 4): number[] {
  if (!(max > 0)) return [0, 1]
  return ticksFromStep(max, niceStep(max / target))
}

/**
 * Ticks for byte values: nice steps in the binary unit (KiB/MiB/GiB/…) the
 * labels will use, so `formatBytes` renders them as "5 GiB", "10 GiB", ….
 */
export function byteTicks(max: number, target = 4): number[] {
  if (!(max > 0)) return [0, 1]
  const unitPower = Math.min(5, Math.max(0, Math.floor(Math.log(max) / Math.log(1024))))
  const unit = 1024 ** unitPower
  // Within raw bytes (unit 1) fractional steps make no sense.
  const multipliers = unitPower === 0 ? [1, 2, 5, 10] : NICE_MULTIPLIERS
  return ticksFromStep(max, niceStep(max / unit / target, multipliers) * unit)
}

/**
 * Ticks for CPU millicores: nice steps in cores (so labels land on 250m,
 * 500m, 1 core, 2 cores, …) as `formatCpu` renders them.
 */
export function cpuTicks(maxMillis: number, target = 4): number[] {
  if (!(maxMillis > 0)) return [0, 1000]
  return ticksFromStep(maxMillis, niceStep(maxMillis / 1000 / target) * 1000)
}
