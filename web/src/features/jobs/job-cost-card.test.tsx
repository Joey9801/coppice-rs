import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import type { AttemptView, CostReport, Resources } from '@/api/types'
import { JobCostCard } from './job-cost-card'

const requests: Resources = { cpuMillis: 1000, memoryBytes: 1 << 30, diskBytes: 1 << 30 }

/** A finished attempt that ran and exited on its own. */
function attempt(n: number, overrides: Partial<AttemptView> = {}): AttemptView {
  return {
    id: `attempt-00000000-0000-0000-0000-00000000000${n}`,
    job: 'job-00000000-0000-0000-0000-000000000001',
    node: 'node-00000000-0000-0000-0000-000000000001',
    allocation: `alloc-00000000-0000-0000-0000-00000000000${n}`,
    state: 'Terminal',
    outcome: { kind: 'Exited', exitCode: 0, class: 'Success' },
    startedAt: new Date('2026-01-01T00:00:00Z'),
    endedAt: new Date('2026-01-01T00:15:00Z'),
    rateUcuPerSecond: 1_000_000 / 3600,
    chargedUcu: 1_000_000,
    ...overrides,
  }
}

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
        attempts={[attempt(1)]}
      />,
    )
    expect(screen.getByText('Charged at placement')).toBeInTheDocument()
    expect(screen.getByText('Refund')).toBeInTheDocument()
    expect(screen.getByText('Refund 0.600 CU')).toBeInTheDocument()
    expect(screen.getByText('75% of the unused runtime')).toBeInTheDocument()
    expect(screen.getByText('Final cost')).toBeInTheDocument()
    expect(screen.getByText('0.400 CU')).toBeInTheDocument()
    expect(screen.queryByText(/still running/)).not.toBeInTheDocument()
  })

  it('reports no refund for a finished job that ran to its limit', () => {
    render(
      <JobCostCard
        cost={cost({ actualUcu: 1_000_000 })}
        requests={requests}
        terminal
        attempts={[attempt(1)]}
      />,
    )
    expect(screen.getByText('none — ran to its limit')).toBeInTheDocument()
    expect(screen.getByText('Final cost')).toBeInTheDocument()
  })

  it('labels a net surcharge as a surcharge', () => {
    render(
      <JobCostCard
        cost={cost({
          chargedUcu: 2_000_000,
          actualUcu: 2_100_000,
          trueUp: { kind: 'Surcharge', amountUcu: 100_000 },
        })}
        requests={requests}
        terminal
        attempts={[attempt(1), attempt(2)]}
      />,
    )
    expect(screen.getByText('Surcharge')).toBeInTheDocument()
    expect(screen.getByText('Surcharge 0.100 CU')).toBeInTheDocument()
    expect(screen.queryByText('Refund')).not.toBeInTheDocument()
    expect(screen.getByText('net across 2 attempts')).toBeInTheDocument()
    expect(screen.getByText('2.1 CU')).toBeInTheDocument()
  })

  it("does not explain a netted refund with the last attempt's fraction", () => {
    render(
      <JobCostCard
        cost={cost({
          chargedUcu: 2_000_000,
          actualUcu: 1_400_000,
          trueUp: { kind: 'Refund', amountUcu: 600_000 },
        })}
        requests={requests}
        terminal
        attempts={[attempt(1), attempt(2)]}
      />,
    )
    expect(screen.getByText('Refund 0.600 CU')).toBeInTheDocument()
    expect(screen.queryByText(/of the unused runtime/)).not.toBeInTheDocument()
    expect(screen.getByText('net across 2 attempts')).toBeInTheDocument()
  })

  it('does not explain a platform-fault refund with the retained fraction', () => {
    // The node was lost: the server refunds the unused charge in full, so
    // the configured 75% would misdescribe the amount shown.
    render(
      <JobCostCard
        cost={cost({ actualUcu: 250_000, trueUp: { kind: 'Refund', amountUcu: 750_000 } })}
        requests={requests}
        terminal
        attempts={[attempt(1, { outcome: { kind: 'NodeLost', class: 'Platform' } })]}
      />,
    )
    expect(screen.getByText('Refund 0.750 CU')).toBeInTheDocument()
    expect(screen.queryByText(/of the unused runtime/)).not.toBeInTheDocument()
  })

  it('does not explain the refund of an attempt that never started', () => {
    render(
      <JobCostCard
        cost={cost({ actualUcu: 0, trueUp: { kind: 'Refund', amountUcu: 1_000_000 } })}
        requests={requests}
        terminal
        attempts={[
          attempt(1, { startedAt: null, outcome: { kind: 'Aborted', class: 'UserRequest' } }),
        ]}
      />,
    )
    expect(screen.queryByText(/of the unused runtime/)).not.toBeInTheDocument()
  })

  it('does not claim a retried job with no net true-up ran to its limit', () => {
    render(
      <JobCostCard
        cost={cost({ chargedUcu: 2_000_000, actualUcu: 2_000_000 })}
        requests={requests}
        terminal
        attempts={[attempt(1), attempt(2)]}
      />,
    )
    expect(screen.getByText('none — no net adjustment across 2 attempts')).toBeInTheDocument()
    expect(screen.queryByText(/ran to its limit/)).not.toBeInTheDocument()
  })

  it('reports the settlement as unknown when the server could not settle it', () => {
    render(<JobCostCard cost={cost()} requests={requests} terminal attempts={[attempt(1)]} />)
    expect(screen.getByText('unknown')).toBeInTheDocument()
    expect(screen.getByText('unavailable')).toBeInTheDocument()
    expect(screen.queryByText(/still running/)).not.toBeInTheDocument()
  })

  it('keeps the refund pending while the job is live', () => {
    render(
      <JobCostCard cost={cost()} requests={requests} terminal={false} attempts={[attempt(1)]} />,
    )
    expect(screen.getByText('Refund at finish')).toBeInTheDocument()
    expect(screen.getByText('— still running')).toBeInTheDocument()
    expect(screen.queryByText('Final cost')).not.toBeInTheDocument()
  })

  it('explains that a never-placed terminal job cost nothing', () => {
    render(
      <JobCostCard
        cost={cost({ chargedUcu: 0, actualUcu: 0 })}
        requests={requests}
        terminal
        attempts={[]}
      />,
    )
    expect(screen.getByText('Never placed on a node, so nothing was charged.')).toBeInTheDocument()
    expect(screen.getByText('Final cost')).toBeInTheDocument()
    expect(screen.queryByText(/ran to its limit/)).not.toBeInTheDocument()
  })

  it('previews the upfront charge for a queued job', () => {
    render(
      <JobCostCard
        cost={cost({ chargedUcu: 0 })}
        requests={requests}
        terminal={false}
        attempts={[]}
      />,
    )
    expect(screen.getByText('Will be charged')).toBeInTheDocument()
  })
})
