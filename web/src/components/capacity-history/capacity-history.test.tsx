import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { ReactElement } from 'react'
import { cloneElement, isValidElement } from 'react'
import type { Resources } from '@/api/types'
import { CapacityHistory } from './capacity-history'
import type { CapacityHistorySample } from './series'

// jsdom gives every element a zero-sized box, so recharts' ResponsiveContainer
// would render nothing at all. Substituting a fixed size lets the SVG render
// so the tests can assert on the marks themselves.
vi.mock('recharts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('recharts')>()
  return {
    ...actual,
    ResponsiveContainer: ({ children }: { children: ReactElement }) =>
      isValidElement(children)
        ? cloneElement(children as ReactElement<{ width?: number; height?: number }>, {
            width: 600,
            height: 180,
          })
        : children,
  }
})

function res(cpuMillis: number, memoryGiB: number, diskGiB: number): Resources {
  return {
    cpuMillis,
    memoryBytes: memoryGiB * 1024 ** 3,
    diskBytes: diskGiB * 1024 ** 3,
  }
}

const CAPACITY = res(8_000, 32, 500)
const ALLOCATED = res(5_000, 20, 200)
const USED = res(2_000, 12, 60)

function history(count = 4, used: Resources | null = USED): CapacityHistorySample[] {
  return Array.from({ length: count }, (_, i) => ({
    t: new Date(1_720_000_000_000 + i * 60_000),
    capacity: CAPACITY,
    allocated: ALLOCATED,
    used,
    reportingNodes: used === null ? 0 : 16,
    totalNodes: 16,
  }))
}

function renderPanel(props: Partial<Parameters<typeof CapacityHistory>[0]> = {}) {
  return render(
    <CapacityHistory
      idPrefix="test"
      history={history()}
      latest={{ capacity: CAPACITY, allocated: ALLOCATED, used: USED }}
      {...props}
    />,
  )
}

describe('CapacityHistory', () => {
  it('charts all three resource dimensions with their latest figures', () => {
    const { container } = renderPanel()

    expect(screen.getByRole('heading', { name: 'CPU' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Memory' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Disk' })).toBeInTheDocument()

    // Each dimension's latest reading, in that dimension's own unit.
    expect(screen.getByText('2 cores used / 5 cores alloc / 8 cores cap')).toBeInTheDocument()
    expect(screen.getByText('12 GiB used / 20 GiB alloc / 32 GiB cap')).toBeInTheDocument()
    expect(screen.getByText('60 GiB used / 200 GiB alloc / 500 GiB cap')).toBeInTheDocument()

    expect(container.querySelectorAll('svg.recharts-surface')).toHaveLength(3)
  })

  it('stacks the charts below the lg breakpoint and lays them out in a row above it', () => {
    const { container } = renderPanel()
    const grid = container.querySelector('.grid')

    expect(grid).toHaveClass('grid-cols-1')
    expect(grid).toHaveClass('lg:grid-cols-3')
  })

  it('draws capacity as a dotted boundary in every dimension', () => {
    // Capacity must stay distinguishable even where it coincides with
    // allocation, so it is a dashed line drawn over the bands.
    const { container } = renderPanel()

    expect(
      container.querySelectorAll('path[stroke-dasharray="4 3"]').length,
    ).toBeGreaterThanOrEqual(3)
  })

  it('labels every mark as measured or derived', () => {
    renderPanel()

    expect(screen.getByText('Used (measured)')).toBeInTheDocument()
    expect(screen.getByText('Allocated, unused (derived)')).toBeInTheDocument()
    expect(screen.getByText('Capacity not allocated (derived)')).toBeInTheDocument()
    expect(screen.getByText('Allocated (funded)')).toBeInTheDocument()
    expect(screen.getByText('Capacity (advertised)')).toBeInTheDocument()
    // Nothing exceeded its allocation here, so that band is not claimed.
    expect(screen.queryByText('Above allocation (measured)')).not.toBeInTheDocument()
  })

  it('names the over-allocation band only when usage actually exceeded allocation', () => {
    const over = history().map((s) => ({ ...s, used: res(6_500, 24, 60) }))
    renderPanel({ history: over })

    expect(screen.getByText('Above allocation (measured)')).toBeInTheDocument()
  })

  it('says usage was not reported instead of showing a zero', () => {
    renderPanel({
      history: history(4, null),
      latest: { capacity: CAPACITY, allocated: ALLOCATED, used: null },
    })

    expect(screen.getByText('not reported / 5 cores alloc / 8 cores cap')).toBeInTheDocument()
    // With nothing measured there are no bands to name — only the two lines.
    expect(screen.queryByText('Used (measured)')).not.toBeInTheDocument()
    expect(screen.queryByText('Allocated, unused (derived)')).not.toBeInTheDocument()
    expect(screen.getByText('Allocated (funded)')).toBeInTheDocument()
    expect(
      screen.getByText(
        'Gaps in the bands are instants with no usage sample; allocated and capacity stay drawn as lines.',
      ),
    ).toBeInTheDocument()
  })

  it('annotates a partial cluster-wide usage figure with its coverage', () => {
    renderPanel({ coverage: { reportingNodes: 3, totalNodes: 16 } })

    expect(screen.getByText('used: 3/16 nodes reporting')).toBeInTheDocument()
  })

  it('says nothing about coverage when every node is reporting', () => {
    renderPanel({ coverage: { reportingNodes: 16, totalNodes: 16 } })

    expect(screen.queryByText(/nodes reporting/)).not.toBeInTheDocument()
  })

  it('keeps the latest figures when there is no history to chart', () => {
    const { container } = renderPanel({ history: [] })

    expect(screen.getAllByText('No history yet')).toHaveLength(3)
    expect(screen.getByText('2 cores used / 5 cores alloc / 8 cores cap')).toBeInTheDocument()
    // Nothing is charted, so the legend claims nothing.
    expect(screen.queryByText('Capacity (advertised)')).not.toBeInTheDocument()
    expect(container.querySelectorAll('svg.recharts-surface')).toHaveLength(0)
  })
})
