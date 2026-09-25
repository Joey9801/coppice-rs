import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { QuotaEntityNode } from '@/api/types'
import { EntityForm } from './entity-form'

const mutate = vi.fn()
vi.mock('@/api/queries', () => ({
  useConfigureQuotaEntity: () => ({ mutate, isPending: false, isError: false, error: null }),
}))

function node(id: string, name: string, path: string, parent: string | null): QuotaEntityNode {
  return {
    id,
    name,
    path,
    parent,
    origin: 'configured',
    principal: null,
    quotaUcu: 1_000_000,
    usageUcu: 0,
    overQuotaRatio: 0,
    penalty: 1,
    createdAt: new Date(0),
    updatedAt: new Date(0),
    queuedCount: 0,
    runningCount: 0,
  }
}

const ACME = node('quota-1', 'acme', 'acme', null)
const RESEARCH = node('quota-2', 'research', 'acme/research', 'quota-1')
const EVALS = node('quota-3', 'evals', 'acme/research/evals', 'quota-2')
const ALL = [ACME, RESEARCH, EVALS]

const nameInput = () => screen.getByPlaceholderText('segment')
const submit = () => screen.getByRole('button', { name: /create entity|save changes/i })

describe('EntityForm (segment names, ADR 0045)', () => {
  beforeEach(() => mutate.mockReset())

  it('previews the full path under a fixed parent and submits a bare segment', () => {
    render(<EntityForm mode="create" parent={RESEARCH} allEntities={ALL} onDone={() => {}} />)
    expect(screen.getByText('acme/research/')).toBeInTheDocument()
    fireEvent.change(nameInput(), { target: { value: 'newteam' } })
    expect(screen.getByTestId('entity-path-preview')).toHaveTextContent('acme/research/newteam')
    fireEvent.change(screen.getByLabelText(/quota/i), { target: { value: '5' } })
    expect(submit()).toBeEnabled()
    fireEvent.click(submit())
    expect(mutate).toHaveBeenCalledTimes(1)
    expect(mutate.mock.calls[0]![0]).toEqual({
      entity: null,
      parent: RESEARCH.id,
      name: 'newteam',
      quotaUcu: 5_000_000,
    })
  })

  it('explains a slash and blocks submit', () => {
    render(<EntityForm mode="create" parent={RESEARCH} allEntities={ALL} onDone={() => {}} />)
    fireEvent.change(nameInput(), { target: { value: 'bad/name' } })
    fireEvent.change(screen.getByLabelText(/quota/i), { target: { value: '5' } })
    expect(screen.getByText(/no "\/"/)).toBeInTheDocument()
    expect(submit()).toBeDisabled()
  })

  it('flags a sibling clash before round-tripping', () => {
    render(<EntityForm mode="create" parent={RESEARCH} allEntities={ALL} onDone={() => {}} />)
    fireEvent.change(nameInput(), { target: { value: 'evals' } })
    fireEvent.change(screen.getByLabelText(/quota/i), { target: { value: '5' } })
    expect(screen.getByText(/already has an entity named "evals"/)).toBeInTheDocument()
    expect(submit()).toBeDisabled()
  })

  it('edit shows name and parent read-only and submits them unchanged with the new quota', () => {
    render(<EntityForm mode="edit" entity={EVALS} allEntities={ALL} onDone={() => {}} />)
    expect(screen.queryByPlaceholderText('segment')).not.toBeInTheDocument()
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument()
    expect(screen.queryByTestId('entity-path-preview')).not.toBeInTheDocument()
    // Own path and id, then the parent's path and id.
    expect(screen.getByText('acme/research/evals')).toBeInTheDocument()
    expect(screen.getByText('quota-3')).toBeInTheDocument()
    expect(screen.getByText('acme/research')).toBeInTheDocument()
    expect(screen.getByText('quota-2')).toBeInTheDocument()

    fireEvent.change(screen.getByLabelText(/quota/i), { target: { value: '7' } })
    expect(submit()).toBeEnabled()
    fireEvent.click(submit())
    expect(mutate.mock.calls[0]![0]).toEqual({
      entity: EVALS.id,
      parent: EVALS.parent,
      name: 'evals',
      quotaUcu: 7_000_000,
    })
  })

  it('edit of a root entity shows (root) and submits a null parent', () => {
    render(<EntityForm mode="edit" entity={ACME} allEntities={ALL} onDone={() => {}} />)
    expect(screen.getByText('(root)')).toBeInTheDocument()
    fireEvent.click(submit())
    expect(mutate.mock.calls[0]![0]).toMatchObject({ entity: ACME.id, parent: null, name: 'acme' })
  })
})
