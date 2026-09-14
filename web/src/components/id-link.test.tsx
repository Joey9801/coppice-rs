import type { ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { IdLink } from './id-link'

// TanStack Router `Link` needs a router context we don't set up in unit
// tests; render it as a plain anchor encoding `to`/`params` in the href.
vi.mock('@tanstack/react-router', () => ({
  Link: ({
    to,
    params,
    children,
    ...rest
  }: {
    to: string
    params?: Record<string, string>
    children?: ReactNode
  }) => (
    <a href={`${to}?${new URLSearchParams(params).toString()}`} {...rest}>
      {children}
    </a>
  ),
}))

describe('IdLink', () => {
  it('links a quota- id to the entity detail route', () => {
    render(<IdLink id="quota-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f" />)
    const link = screen.getByRole('link')
    expect(link).toHaveAttribute(
      'href',
      '/entities/$entityId?entityId=quota-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f',
    )
  })

  it('links a job- id to the job detail route', () => {
    render(<IdLink id="job-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f" />)
    const link = screen.getByRole('link')
    expect(link).toHaveAttribute(
      'href',
      '/jobs/$jobId?jobId=job-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f',
    )
  })

  it('links a node- id to the node detail route', () => {
    render(<IdLink id="node-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f" />)
    const link = screen.getByRole('link')
    expect(link).toHaveAttribute(
      'href',
      '/nodes/$nodeId?nodeId=node-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f',
    )
  })

  it('renders a non-routable prefix as plain mono text, not a link', () => {
    render(<IdLink id="attempt-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f" />)
    expect(screen.queryByRole('link')).not.toBeInTheDocument()
    expect(screen.getByText(/attempt-/)).toBeInTheDocument()
  })
})
