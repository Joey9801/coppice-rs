import type { CostReport } from '@/api/types'
import { formatUcu, formatUcuRatePerHour } from '@/lib/format'
import { KeyValueGrid } from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { TrueUpAmount } from './true-up-amount'

export function JobCostCard({ cost }: { cost: CostReport }) {
  const items = [
    { label: 'Rate', value: formatUcuRatePerHour(cost.rateUcuPerSecond) },
    {
      label: 'Estimated',
      value: (
        <span>
          {formatUcu(cost.estimatedUcu)}
          {cost.estimateUsedDefaultRuntime ? (
            <span className="ml-1.5 text-xs text-muted-foreground">(policy default runtime)</span>
          ) : null}
        </span>
      ),
    },
    { label: 'Charged so far', value: formatUcu(cost.chargedUcu) },
    {
      label: 'Actual',
      value:
        cost.actualUcu != null ? (
          formatUcu(cost.actualUcu)
        ) : (
          <span className="text-muted-foreground">— not final yet</span>
        ),
    },
    {
      label: 'True-up',
      value: cost.trueUp ? (
        <TrueUpAmount trueUp={cost.trueUp} />
      ) : (
        <span className="text-muted-foreground">—</span>
      ),
    },
  ]

  return (
    <Card>
      <CardHeader className="p-4 pb-0">
        <CardTitle className="text-sm">Cost</CardTitle>
      </CardHeader>
      <CardContent className="p-4">
        <KeyValueGrid items={items} />
      </CardContent>
    </Card>
  )
}
