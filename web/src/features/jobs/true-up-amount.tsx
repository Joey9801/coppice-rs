import type { CostReport } from '@/api/types'
import { formatUcu } from '@/lib/format'

/** Colored refund/surcharge amount, shared by the hero tile and cost card. */
export function TrueUpAmount({ trueUp }: { trueUp: NonNullable<CostReport['trueUp']> }) {
  return (
    <span
      className={
        trueUp.kind === 'Refund'
          ? 'text-emerald-600 dark:text-emerald-400'
          : 'text-amber-600 dark:text-amber-400'
      }
    >
      {trueUp.kind} {formatUcu(trueUp.amountUcu)}
    </span>
  )
}
