import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import type { QueuePositionExplainer } from '@/api/types'
import { JobQueuePanel } from './job-queue-panel'

function explainer(overrides: Partial<QueuePositionExplainer> = {}): QueuePositionExplainer {
  return {
    multiplier: 2,
    penaltyChain: [
      {
        entity: 'quota-00000000-0000-0000-0000-000000000001',
        name: 'team-a',
        usageUcu: 4_000_000_000_000,
        quotaUcu: 1_000_000_000_000,
        overQuotaRatio: 4,
        penalty: 16,
      },
      {
        entity: 'quota-00000000-0000-0000-0000-000000000002',
        name: 'division',
        usageUcu: 500_000_000_000,
        quotaUcu: 1_000_000_000_000,
        overQuotaRatio: 0.5,
        penalty: 1,
      },
    ],
    penaltyProduct: 16,
    ageSeconds: 3 * 3600 + 12 * 60,
    ...overrides,
  }
}

describe('JobQueuePanel', () => {
  it('explains priority, quota pressure and waiting in operator language', () => {
    render(<JobQueuePanel queue={explainer()} />)

    expect(screen.getByText('Queue ranking')).toBeInTheDocument()
    // The three score inputs, each named and shown with its value.
    expect(screen.getByText('Priority')).toBeInTheDocument()
    expect(screen.getByText('Quota pressure')).toBeInTheDocument()
    expect(screen.getByText('Waiting')).toBeInTheDocument()
    expect(screen.getByText('3h 12m')).toBeInTheDocument()
    // No literal queue position is claimed: the server serves ranking terms,
    // not a rank in the queue.
    expect(screen.queryByText(/^#/)).not.toBeInTheDocument()
    expect(screen.queryByText(/of .* queued/)).not.toBeInTheDocument()
  })

  it('keeps the leaf-to-root penalty chain visible as part of the score', () => {
    render(<JobQueuePanel queue={explainer()} />)

    // Both chain links appear, leaf first, with usage vs quota and the
    // per-entity discount they contribute to the ranking.
    const table = screen.getByRole('table')
    expect(table).toHaveTextContent('team-a')
    expect(table).toHaveTextContent('division')
    expect(screen.getByText('Share of quota used')).toBeInTheDocument()
    expect(screen.getByText('Ranking discount')).toBeInTheDocument()
    // Over-quota links are flagged as the discount source; within-quota ones
    // read as no pressure.
    expect(screen.getByText(/over quota, discounting the ranking/)).toBeInTheDocument()
  })

  it('reads as no pressure when every entity is within quota', () => {
    render(
      <JobQueuePanel
        queue={explainer({
          penaltyChain: [
            {
              entity: 'quota-00000000-0000-0000-0000-000000000001',
              name: 'team-b',
              usageUcu: 100,
              quotaUcu: 1_000,
              overQuotaRatio: 0.1,
              penalty: 1,
            },
          ],
          penaltyProduct: 1,
        })}
      />,
    )

    expect(screen.getByText(/within quota/)).toBeInTheDocument()
  })

  it('keeps the raw arithmetic as secondary detail, and honest about the age term', () => {
    render(<JobQueuePanel queue={explainer()} />)

    // The composed figure lives behind the disclosure, not the headline.
    const details = screen.getByText('Score arithmetic').closest('details')!
    expect(details).not.toHaveAttribute('open')
    expect(details).toHaveTextContent('priority term')
    // The age credit is the scheduler's own composition — never presented
    // as a number this panel invented.
    expect(details).toHaveTextContent(/waiting credit \(computed by the scheduler\)/)
  })

  it('renders an infinite penalty product without showing NaN', () => {
    render(
      <JobQueuePanel
        queue={explainer({
          penaltyChain: [
            {
              entity: 'quota-00000000-0000-0000-0000-000000000001',
              name: 'zero-quota-team',
              usageUcu: 10,
              quotaUcu: 0,
              overQuotaRatio: Number.POSITIVE_INFINITY,
              penalty: Number.POSITIVE_INFINITY,
            },
          ],
          penaltyProduct: Number.POSITIVE_INFINITY,
        })}
      />,
    )

    expect(screen.getByText('÷∞')).toBeInTheDocument()
    expect(screen.queryByText(/NaN/)).not.toBeInTheDocument()
  })
})
