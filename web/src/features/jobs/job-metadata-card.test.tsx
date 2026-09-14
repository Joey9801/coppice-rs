import type { ReactNode } from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { JobDetail } from '@/api/types'
import { JobMetadataCard } from './job-metadata-card'

// IdLink (reached via MetadataValue) renders a TanStack Router `Link`,
// which needs a router context we don't set up in unit tests.
vi.mock('@tanstack/react-router', () => ({
  Link: ({ to, children, ...rest }: { to: string; children?: ReactNode }) => (
    <a href={to} {...rest}>
      {children}
    </a>
  ),
}))

const mutate = vi.fn()

vi.mock('@/api/queries', () => ({
  useUpdateJobMetadata: () => ({
    mutate,
    isPending: false,
    isError: false,
    error: null,
  }),
}))

function job(metadata: JobDetail['metadata']): JobDetail {
  return { id: 'job-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f', metadata } as unknown as JobDetail
}

describe('JobMetadataCard', () => {
  it('renders keys sorted ascending', () => {
    render(<JobMetadataCard job={job({ zeta: 'a', alpha: 'b', mid: 'c' })} />)
    const keys = screen.getAllByText(/^(zeta|alpha|mid)$/).map((el) => el.textContent)
    expect(keys).toEqual(['alpha', 'mid', 'zeta'])
  })

  it('shows a quiet empty state with Add still available', () => {
    render(<JobMetadataCard job={job({})} />)
    expect(screen.getByText('No metadata.')).toBeInTheDocument()
    expect(screen.getAllByRole('button', { name: /Add/ }).length).toBeGreaterThan(0)
  })

  it('adds a key: opens the editor, types a value, saves the string verbatim', () => {
    mutate.mockClear()
    render(<JobMetadataCard job={job({})} />)

    fireEvent.click(screen.getByRole('button', { name: 'Add metadata' }))
    fireEvent.change(screen.getByLabelText('Key'), { target: { value: 'ticket' } })
    fireEvent.change(screen.getByLabelText('Value'), { target: { value: 'INC-1234' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    expect(mutate).toHaveBeenCalledWith(
      {
        id: 'job-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f',
        set: { ticket: 'INC-1234' },
      },
      expect.anything(),
    )
  })

  it('stores a JSON-looking value as its own text, unparsed', () => {
    mutate.mockClear()
    render(<JobMetadataCard job={job({})} />)

    fireEvent.click(screen.getByRole('button', { name: 'Add metadata' }))
    fireEvent.change(screen.getByLabelText('Key'), { target: { value: 'note' } })
    fireEvent.change(screen.getByLabelText('Value'), { target: { value: '{"a":1}' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    expect(mutate).toHaveBeenCalledWith(
      {
        id: 'job-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f',
        set: { note: '{"a":1}' },
      },
      expect.anything(),
    )
  })

  it('accepts an empty value', () => {
    mutate.mockClear()
    render(<JobMetadataCard job={job({})} />)

    fireEvent.click(screen.getByRole('button', { name: 'Add metadata' }))
    fireEvent.change(screen.getByLabelText('Key'), { target: { value: 'flag' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    expect(mutate).toHaveBeenCalledWith(
      { id: 'job-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f', set: { flag: '' } },
      expect.anything(),
    )
  })

  it('refuses a value over the 1024-byte limit', () => {
    render(<JobMetadataCard job={job({})} />)

    fireEvent.click(screen.getByRole('button', { name: 'Add metadata' }))
    fireEvent.change(screen.getByLabelText('Key'), { target: { value: 'note' } })
    fireEvent.change(screen.getByLabelText('Value'), { target: { value: 'x'.repeat(1025) } })

    expect(screen.getByText(/the limit is 1024/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled()
  })

  it('shows a validation message for a bad key and disables Save', () => {
    render(<JobMetadataCard job={job({})} />)

    fireEvent.click(screen.getByRole('button', { name: 'Add metadata' }))
    fireEvent.change(screen.getByLabelText('Key'), { target: { value: 'bad key!' } })
    fireEvent.change(screen.getByLabelText('Value'), { target: { value: 'x' } })

    expect(
      screen.getByText('Key may use only letters, digits and . _ - / : characters.'),
    ).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled()
  })

  it('removes a key via the unset patch', () => {
    mutate.mockClear()
    render(<JobMetadataCard job={job({ ticket: 'INC-1234' })} />)

    fireEvent.click(screen.getByRole('button', { name: 'Remove' }))

    expect(mutate).toHaveBeenCalledWith({
      id: 'job-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f',
      unset: ['ticket'],
    })
  })
})
