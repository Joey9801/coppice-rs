import { fireEvent, render, screen, within } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { CoordinatorMember, CoordinatorStatus } from '@/api/types'
import { CoordinatorsPage } from './coordinators-page'
import { CoordinatorLogsCard } from './coordinator-logs-card'
import { MembershipCard } from './membership-card'
import { shortCoordinatorLabels } from './coordinator-labels'
import { SnapshotCard } from './snapshot-card'

vi.mock('@/api/queries', () => ({
  useCoordinatorStatus: vi.fn(),
  useCoordinatorLogs: () => ({ data: { unsupported: true }, loading: false, polling: false }),
}))

import { useCoordinatorStatus } from '@/api/queries'

// Two random 64-bit raft ids sharing a 4-digit prefix (like the backend
// mints) plus a short one. Both long ids are above 2^53 — as JS numbers
// they would round to the same double; as strings they must survive exactly.
const LONG_A = '7234100239847293847'
const LONG_B = '7234980239847293847'
const SHORT_C = '3'

function member(overrides: Partial<CoordinatorMember>): CoordinatorMember {
  return {
    id: '1',
    addr: 'coord-1.internal:7071',
    role: 'Follower',
    voter: true,
    lastApplied: 42000,
    replicationLagEntries: 0,
    host: { cpuFraction: 0.1, memoryFraction: 0.2, diskFraction: 0.3 },
    lastSeen: new Date(1_720_000_000_000),
    ...overrides,
  }
}

const MEMBERS: CoordinatorMember[] = [
  member({ id: LONG_A, addr: 'coord-a.internal:7071', role: 'Leader' }),
  member({ id: LONG_B, addr: 'coord-b.internal:7071' }),
  member({ id: SHORT_C, addr: 'coord-c.internal:7071', role: 'Learner', voter: false }),
]

const LABELS = shortCoordinatorLabels(MEMBERS.map((m) => m.id))

function status(overrides: Partial<CoordinatorStatus>): CoordinatorStatus {
  return {
    clusterId: 'coppice-prod-1',
    leader: LONG_A,
    term: 7,
    knownCommitted: 42000,
    lastApplied: 41998,
    stateVersion: 61000,
    snapshot: null,
    stateCounts: { jobs: 3, attempts: 5, allocations: 4, nodes: 16, quotaEntities: 2 },
    members: MEMBERS,
    ...overrides,
  }
}

function setStatus(s: CoordinatorStatus) {
  vi.mocked(useCoordinatorStatus).mockReturnValue({
    data: s,
    isLoading: false,
    isError: false,
  } as unknown as ReturnType<typeof useCoordinatorStatus>)
}

describe('shortCoordinatorLabels', () => {
  it('stretches prefixes until every member is unambiguous', () => {
    // Both ids start "7234"; the prefix grows to 5 digits for both.
    expect(LABELS.get(LONG_A)).toBe('72341…')
    expect(LABELS.get(LONG_B)).toBe('72349…')
    expect(new Set(LABELS.values()).size).toBe(LABELS.size)
  })

  it('shows a short id in full, without an ellipsis', () => {
    expect(LABELS.get(SHORT_C)).toBe('3')
  })
})

describe('MembershipCard', () => {
  it('renders unique compact tags for long ids and keeps full identity reachable', () => {
    render(<MembershipCard members={MEMBERS} leader={LONG_A} labels={LABELS} />)

    expect(screen.getByText('c-72341…')).toBeInTheDocument()
    expect(screen.getByText('c-72349…')).toBeInTheDocument()
    expect(screen.getByText('c-3')).toBeInTheDocument()
    // No overflowing "coordinator 7234…" concatenation in the cells.
    expect(screen.queryByText(`coordinator ${LONG_A}`)).not.toBeInTheDocument()
    // The full canonical address stays reachable (tooltip + copy affordance).
    expect(screen.getByTitle('coord-a.internal:7071')).toBeInTheDocument()
    expect(screen.getAllByLabelText('Copy coordinator id')).toHaveLength(3)
    expect(screen.getByLabelText('leader')).toBeInTheDocument()
  })

  it('copies ids above 2^53 exactly, without float rounding', async () => {
    // Number('7234100239847293847') is 7234100239847294000 — the copy
    // affordance must never round-trip the id through a number.
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })
    render(<MembershipCard members={MEMBERS} leader={LONG_A} labels={LABELS} />)

    const row = screen.getByText('c-72341…').closest('tr')!
    fireEvent.click(within(row).getByLabelText('Copy coordinator id'))
    await vi.waitFor(() => expect(writeText).toHaveBeenCalledWith('7234100239847293847'))
  })
})

describe('SnapshotCard', () => {
  it('says no snapshot exists yet, without fabricating zeroed metadata', () => {
    render(<SnapshotCard snapshot={null} lastApplied={41998} />)

    expect(screen.getByText(/No snapshot has been taken yet/)).toBeInTheDocument()
    expect(screen.getByText('Entries since log start')).toBeInTheDocument()
    expect(screen.getByText('41,998')).toBeInTheDocument()
    expect(screen.queryByText('0 B')).not.toBeInTheDocument()
    expect(screen.queryByText(/1970/)).not.toBeInTheDocument()
    expect(screen.queryByText('Last included index')).not.toBeInTheDocument()
  })

  it('renders a present snapshot normally', () => {
    render(
      <SnapshotCard
        snapshot={{
          sizeBytes: 40 * 1024 * 1024,
          lastIncludedIndex: 41000,
          takenAt: new Date(1_760_000_000_000),
          entriesSinceSnapshot: 998,
        }}
        lastApplied={41998}
      />,
    )

    expect(screen.getByText('40 MiB')).toBeInTheDocument()
    expect(screen.getByText('Last included index')).toBeInTheDocument()
    expect(screen.getByText('41,000')).toBeInTheDocument()
    expect(screen.getByText('Entries since snapshot')).toBeInTheDocument()
    expect(screen.getByText('998')).toBeInTheDocument()
  })

  it('marks unreported size and time as not reported, not zero/epoch', () => {
    render(
      <SnapshotCard
        snapshot={{
          sizeBytes: null,
          lastIncludedIndex: 41000,
          takenAt: null,
          entriesSinceSnapshot: 998,
        }}
        lastApplied={41998}
      />,
    )

    expect(screen.getAllByText('not reported')).toHaveLength(2)
    expect(screen.queryByText('0 B')).not.toBeInTheDocument()
    expect(screen.queryByText(/1970/)).not.toBeInTheDocument()
  })
})

describe('CoordinatorLogsCard', () => {
  it('labels a tab per member with compact tags', () => {
    render(<CoordinatorLogsCard members={MEMBERS} labels={LABELS} />)

    expect(screen.getByText('c-72341…')).toBeInTheDocument()
    expect(screen.getByText('c-72349…')).toBeInTheDocument()
    expect(screen.getByText('c-3')).toBeInTheDocument()
  })

  it('keeps Radix arrow-key tab navigation working (no focusable label inside a tab)', () => {
    render(<CoordinatorLogsCard members={MEMBERS} labels={LABELS} />)

    const first = screen.getByRole('tab', { name: 'c-72341…' })
    // The label span must not be an extra focus stop inside the tab button:
    // if it grabs focus, ArrowRight originates outside the tab and Radix
    // ignores it.
    expect(within(first).getByText('c-72341…')).not.toHaveAttribute('tabindex')

    fireEvent.keyDown(first, { key: 'ArrowRight' })
    // jsdom does not fire focus from keydown; Radix moves focus to the next
    // tab and selection follows focus.
    const second = screen.getByRole('tab', { name: 'c-72349…' })
    fireEvent.focus(second)
    expect(second).toHaveAttribute('aria-selected', 'true')
  })
})

describe('CoordinatorsPage', () => {
  it('with no snapshot yet: says so in tile and panel, no zero/epoch artifacts', () => {
    setStatus(status({ snapshot: null }))
    render(<CoordinatorsPage />)

    expect(screen.getByText('no snapshot yet')).toBeInTheDocument()
    expect(screen.getByText('The first snapshot has not been taken.')).toBeInTheDocument()
    expect(screen.getByText(/No snapshot has been taken yet/)).toBeInTheDocument()
    expect(screen.queryByText('0 B')).not.toBeInTheDocument()
    expect(screen.queryByText(/1970/)).not.toBeInTheDocument()
  })

  it('with a present snapshot: keeps the normal display', () => {
    setStatus(
      status({
        snapshot: {
          sizeBytes: 40 * 1024 * 1024,
          lastIncludedIndex: 41000,
          takenAt: new Date(1_760_000_000_000),
          entriesSinceSnapshot: 998,
        },
      }),
    )
    render(<CoordinatorsPage />)

    expect(screen.getByText('998 entries')).toBeInTheDocument()
    expect(screen.getAllByText('40 MiB').length).toBe(2)
    expect(screen.getByText(/^taken$/)).toBeInTheDocument()
    expect(screen.getByText('Last included index')).toBeInTheDocument()
  })

  it('renders compact unique identity tags across leader tile, table and tabs', () => {
    setStatus(status({ snapshot: null }))
    render(<CoordinatorsPage />)

    // Leader tile, membership row and log tab all use the same short tag.
    expect(screen.getAllByText('c-72341…').length).toBeGreaterThanOrEqual(2)
    expect(screen.getAllByText('c-72349…').length).toBeGreaterThanOrEqual(2)
    expect(screen.queryByText(`coordinator ${LONG_A}`)).not.toBeInTheDocument()
    expect(screen.getAllByLabelText('Copy coordinator id').length).toBeGreaterThanOrEqual(4)
  })
})
