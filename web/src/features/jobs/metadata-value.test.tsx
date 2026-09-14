import type { ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { MetadataValue } from './metadata-value'
import { isExternalUrl, isTypedCoppiceId } from './metadata-shape'

// IdLink renders a TanStack Router `Link`, which needs a router context we
// don't set up in unit tests; render it as a plain anchor instead.
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

describe('isExternalUrl', () => {
  it('accepts http and https URLs', () => {
    expect(isExternalUrl('https://example.com/path')).toBe(true)
    expect(isExternalUrl('http://example.com')).toBe(true)
  })

  it('rejects other schemes and unparsable strings', () => {
    expect(isExternalUrl('javascript:alert(1)')).toBe(false)
    expect(isExternalUrl('file:///etc/passwd')).toBe(false)
    expect(isExternalUrl('mailto:a@b.com')).toBe(false)
    expect(isExternalUrl('not a url')).toBe(false)
  })
})

describe('isTypedCoppiceId', () => {
  const uuid = '3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f'

  it('matches every routable prefix', () => {
    for (const prefix of [
      'job',
      'node',
      'quota',
      'alloc',
      'attempt',
      'group',
      'cluster',
      'machine',
      'token',
    ]) {
      expect(isTypedCoppiceId(`${prefix}-${uuid}`)).toBe(true)
    }
  })

  it('rejects an unknown prefix or malformed uuid', () => {
    expect(isTypedCoppiceId(`widget-${uuid}`)).toBe(false)
    expect(isTypedCoppiceId('job-not-a-uuid')).toBe(false)
  })
})

describe('MetadataValue', () => {
  it('renders an http(s) string as an external link', () => {
    render(<MetadataValue value="https://example.com/ticket/1" />)
    const link = screen.getByRole('link', { name: 'https://example.com/ticket/1' })
    expect(link).toHaveAttribute('href', 'https://example.com/ticket/1')
    expect(link).toHaveAttribute('target', '_blank')
    expect(link).toHaveAttribute('rel', 'noreferrer')
  })

  it('does not linkify a javascript: URL', () => {
    render(<MetadataValue value="javascript:alert(1)" />)
    expect(screen.queryByRole('link')).not.toBeInTheDocument()
    expect(screen.getByText('javascript:alert(1)')).toBeInTheDocument()
  })

  it('renders a typed Coppice id as an IdLink', () => {
    render(<MetadataValue value="job-3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f" />)
    // IdLink renders a mono short-id link plus a copy affordance.
    expect(screen.getByRole('link')).toBeInTheDocument()
    expect(screen.getByLabelText('Copy id')).toBeInTheDocument()
  })

  it('renders any other string as plain text', () => {
    render(<MetadataValue value="just some text" />)
    expect(screen.queryByRole('link')).not.toBeInTheDocument()
    expect(screen.getByText('just some text')).toBeInTheDocument()
  })

  it('renders a JSON-looking value literally — values are never parsed', () => {
    render(<MetadataValue value='{"a":1}' />)
    expect(screen.getByText('{"a":1}')).toBeInTheDocument()
  })
})
