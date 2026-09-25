import type { ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { QuotaEntityView } from '@/api/types'
import { EntityBreadcrumb, EntityLabel } from './entity-label'

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

const ID = 'quota-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f'
const HREF = `/entities/$entityId?entityId=${ID}`

function view(id: string, name: string, path: string): QuotaEntityView {
  return {
    id,
    name,
    path,
    parent: null,
    quotaUcu: 0,
    usageUcu: 0,
    overQuotaRatio: 0,
    penalty: 1,
  }
}

describe('EntityLabel', () => {
  it('inline: renders the path as a link to the id-keyed route, id copyable', () => {
    render(<EntityLabel id={ID} path="acme/eng/platform" />)
    const link = screen.getByRole('link', { name: 'acme/eng/platform' })
    expect(link).toHaveAttribute('href', HREF)
    expect(screen.getByRole('button', { name: 'Copy entity id' })).toBeInTheDocument()
    // Inline keeps the id out of the visible text (it lives in the tooltip).
    expect(screen.queryByText(ID)).not.toBeInTheDocument()
  })

  it('falls back to the id when the path is blank', () => {
    render(<EntityLabel id={ID} path="" />)
    expect(screen.getByRole('link', { name: ID })).toBeInTheDocument()
  })

  it('stacked: shows the full id as visible subtext under the path', () => {
    render(<EntityLabel id={ID} path="acme/eng" variant="stacked" />)
    expect(screen.getByRole('link', { name: 'acme/eng' })).toHaveAttribute('href', HREF)
    expect(screen.getByText(ID)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Copy entity id' })).toBeInTheDocument()
  })

  it('renders plain text with link={false}', () => {
    render(<EntityLabel id={ID} path="acme" link={false} />)
    expect(screen.queryByRole('link')).not.toBeInTheDocument()
    expect(screen.getByText('acme')).toBeInTheDocument()
  })
})

describe('EntityBreadcrumb', () => {
  const chain = [
    view('quota-1', 'acme', 'acme'),
    view('quota-2', 'eng', 'acme/eng'),
    view('quota-3', 'platform', 'acme/eng/platform'),
  ]

  it('links ancestors by id and leaves the leaf unlinked by default', () => {
    render(<EntityBreadcrumb chain={chain} />)
    expect(screen.getByRole('link', { name: 'acme' })).toHaveAttribute(
      'href',
      '/entities/$entityId?entityId=quota-1',
    )
    expect(screen.getByRole('link', { name: 'eng' })).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: 'platform' })).not.toBeInTheDocument()
    expect(screen.getByText('platform')).toBeInTheDocument()
  })

  it('links the leaf with linkLeaf and renders suffixes', () => {
    render(
      <EntityBreadcrumb
        chain={chain}
        linkLeaf
        renderSuffix={(e) => (e.name === 'eng' ? <span>hot</span> : null)}
      />,
    )
    expect(screen.getByRole('link', { name: 'platform' })).toHaveAttribute(
      'href',
      '/entities/$entityId?entityId=quota-3',
    )
    expect(screen.getByText('hot')).toBeInTheDocument()
  })
})
