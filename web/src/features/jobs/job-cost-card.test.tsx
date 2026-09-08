import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import type { CostReport, Resources } from '@/api/types'
import { JobCostCard } from './job-cost-card'

const requests: Resources = { cpuMillis: 1000, memoryBytes: 1 << 30, diskBytes: 1 << 30 }

/** A bounded 1-hour job at 1 CU/hour, charged 1 CU upfront. */
function cost(overrides: Partial<CostReport> = {}): CostReport {
  const perSecond = 1_000_000 / 3600
  return {
    rateUcuPerSecond: perSecond,
    rateBreakdown: { cpu: perSecond, memory: 0, disk: 0 },
    priorityMultiplier: 1,
    unboundedMultiplier: 1,
    effectiveRateUcuPerSecond: perSecond,
    chargeWindowSeconds: 3600,
    chargeWindowIsDefault: false,
    estimatedUcu: 1_000_000,
    chargedUcu: 1_000_000,
    refundFraction: 0.75,
    actualUcu: null,
    trueUp: null,
    ...overrides,
  }
}

describe('JobCostCard', () => {
  it('shows the refund and settled cost of a finished job', () => {
    render(
      <JobCostCard
        cost={cost({ actualUcu: 400_000, trueUp: { kind: 'Refund', amountUcu: 600_000 } })}
        requests={requests}
        terminal
      />,
    )
    expect(screen.getByText('Charged at placement')).toBeInTheDocument()
    expect(screen.getByText('Refund 0.600 CU')).toBeInTheDocument()
    expect(screen.getByText('75% of the unused runtime')).toBeInTheDocument()
    expect(screen.getByText('Final cost')).toBeInTheDocument()
    expect(screen.getByText('0.400 CU')).toBeInTheDocument()
    expect(screen.queryByText(/still running/)).not.toBeInTheDocument()
  })

  it('reports no refund for a finished job that ran to its limit', () => {
    render(<JobCostCard cost={cost({ actualUcu: 1_000_000 })} requests={requests} terminal />)
    expect(screen.getByText('none — ran to its limit')).toBeInTheDocument()
    expect(screen.getByText('Final cost')).toBeInTheDocument()
  })

  it('keeps the refund pending while the job is live', () => {
    render(<JobCostCard cost={cost()} requests={requests} terminal={false} />)
    expect(screen.getByText('Refund at finish')).toBeInTheDocument()
    expect(screen.getByText('— still running')).toBeInTheDocument()
    expect(screen.queryByText('Final cost')).not.toBeInTheDocument()
  })

  it('explains that a never-placed terminal job cost nothing', () => {
    render(
      <JobCostCard cost={cost({ chargedUcu: 0, actualUcu: 0 })} requests={requests} terminal />,
    )
    expect(screen.getByText('Never placed on a node, so nothing was charged.')).toBeInTheDocument()
    expect(screen.getByText('Final cost')).toBeInTheDocument()
    expect(screen.queryByText(/ran to its limit/)).not.toBeInTheDocument()
  })

  it('previews the upfront charge for a queued job', () => {
    render(<JobCostCard cost={cost({ chargedUcu: 0 })} requests={requests} terminal={false} />)
    expect(screen.getByText('Will be charged')).toBeInTheDocument()
  })
})
