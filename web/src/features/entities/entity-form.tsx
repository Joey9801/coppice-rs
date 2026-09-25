import { useId, useState } from 'react'
import type { ConfigureQuotaEntityInput, QuotaEntityNode } from '@/api/types'
import { useConfigureQuotaEntity } from '@/api/queries'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { entitySegmentError, joinEntityPath } from '@/lib/quota-entity'
import { cn } from '@/lib/utils'
import { buildEntityTree, costUnitsToUcu, descendantIds, flattenTree, ucuToCostUnits } from './lib'

type EntityFormProps = {
  /** Every entity, used to populate the parent picker. */
  allEntities: QuotaEntityNode[]
  onDone: () => void
  title?: string
} & (
  | {
      mode: 'create'
      /** When set, the new entity is created under this fixed parent. */
      parent?: QuotaEntityNode | null
    }
  | { mode: 'edit'; entity: QuotaEntityNode }
)

/** Render `error.message` off the mutation without importing the ApiError class. */
function errorMessage(error: unknown): string {
  if (error && typeof error === 'object' && 'message' in error) {
    const message = (error as { message?: unknown }).message
    if (typeof message === 'string') return message
  }
  return 'Something went wrong.'
}

export function EntityForm(props: EntityFormProps) {
  const { allEntities, onDone } = props
  const mutation = useConfigureQuotaEntity()
  // Two forms can share a page (add-child and configure), so ids are per-form.
  const uid = useId()

  const isEdit = props.mode === 'edit'
  const entity = props.mode === 'edit' ? props.entity : null
  const isSso = entity?.origin === 'sso'

  // A create with a fixed parent locks the parent; otherwise the parent is
  // chosen from the picker (create) or the entity's own parent (edit).
  const fixedParent = props.mode === 'create' ? (props.parent ?? null) : null
  const parentLocked = props.mode === 'create' ? props.parent !== undefined : isSso

  const [parentId, setParentId] = useState<string | null>(
    props.mode === 'create' ? (fixedParent?.id ?? null) : (entity?.parent ?? null),
  )
  // The parent id sent is the chosen/fixed id itself, never re-derived from the
  // (possibly still loading) entity list — that must not silently reparent
  // to the root.
  const effectiveParentId = parentLocked
    ? props.mode === 'create'
      ? (fixedParent?.id ?? null)
      : (entity?.parent ?? null)
    : parentId
  const byId = new Map(allEntities.map((n) => [n.id, n]))
  const effectiveParent = parentLocked
    ? props.mode === 'create'
      ? fixedParent
      : entity && entity.parent
        ? (byId.get(entity.parent) ?? null)
        : null
    : parentId
      ? (byId.get(parentId) ?? null)
      : null

  // Every mode types one segment (ADR 0045): the path is derived from the
  // parent chain, never typed. An SSO identity's name is fixed.
  const [segment, setSegment] = useState(entity?.name ?? '')

  const [quotaInput, setQuotaInput] = useState(
    entity ? String(ucuToCostUnits(entity.quotaUcu)) : '',
  )

  const parentOptions = flattenTree(buildEntityTree(allEntities))
  const excluded = entity ? descendantIds(allEntities, entity.id) : new Set<string>()

  const name = isSso ? (entity?.name ?? '') : segment
  const parentPath = effectiveParent?.path ?? null
  const previewPath = joinEntityPath(parentPath, name)

  // The server's own checks, mirrored so a bad name never round-trips: the
  // segment grammar, then sibling uniqueness under the chosen parent.
  const grammarError = isSso ? null : entitySegmentError(name)
  const clash = isSso
    ? undefined
    : allEntities.find(
        (n) => n.parent === effectiveParentId && n.name === name && n.id !== entity?.id,
      )
  const nameError =
    grammarError ??
    (clash
      ? `${parentPath ? `"${parentPath}"` : 'The root level'} already has an entity named "${name}".`
      : null)
  // Don't shout "Name is required." at an untouched create form.
  const showNameError = nameError !== null && name !== ''

  const quotaCu = Number(quotaInput)
  const quotaValid = quotaInput.trim() !== '' && Number.isFinite(quotaCu) && quotaCu >= 0
  const canSubmit = nameError === null && quotaValid && !mutation.isPending

  const submit = (event: React.FormEvent) => {
    event.preventDefault()
    if (!canSubmit) return
    const input: ConfigureQuotaEntityInput = {
      entity: entity?.id ?? null,
      parent: isSso ? (entity?.parent ?? null) : effectiveParentId,
      name,
      quotaUcu: costUnitsToUcu(quotaCu),
    }
    mutation.mutate(input, { onSuccess: () => onDone() })
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      {props.title ? (
        <h3 className="text-sm font-semibold text-foreground">{props.title}</h3>
      ) : null}

      {isSso ? (
        <p className="rounded-md border border-border bg-muted/40 px-3 py-2 text-xs text-muted-foreground">
          Name and position are owned by the SSO identity, not the admin — only the quota can be
          changed here. Child sub-queues can still be added.
        </p>
      ) : null}

      <div className="space-y-1.5">
        <label className="block text-sm font-medium text-foreground" htmlFor={`${uid}-name`}>
          Name
        </label>
        {isSso ? (
          <p className="font-mono text-sm text-muted-foreground">{entity?.name}</p>
        ) : (
          <div
            className={cn(
              'flex items-center rounded-md border border-input bg-transparent pl-3 shadow-sm focus-within:ring-2 focus-within:ring-ring',
              showNameError && 'border-destructive',
            )}
          >
            {parentPath ? (
              <span className="max-w-[60%] shrink-0 truncate whitespace-nowrap font-mono text-sm text-muted-foreground">
                {parentPath}/
              </span>
            ) : null}
            <input
              id={`${uid}-name`}
              className="h-9 w-full bg-transparent pr-3 font-mono text-sm focus-visible:outline-none"
              placeholder="segment"
              autoComplete="off"
              spellCheck={false}
              aria-invalid={showNameError}
              aria-describedby={`${uid}-name-help`}
              value={segment}
              onChange={(e) => setSegment(e.target.value)}
            />
          </div>
        )}
        <div id={`${uid}-name-help`} className="space-y-0.5 text-xs">
          {showNameError ? <p className="text-destructive">{nameError}</p> : null}
          <p className="text-muted-foreground">
            Full path:{' '}
            <span className="font-mono text-foreground" data-testid="entity-path-preview">
              {name ? previewPath : parentPath ? `${parentPath}/…` : '…'}
            </span>
          </p>
          {!isSso ? (
            <p className="text-muted-foreground">
              One segment: letters, digits, <span className="font-mono">.</span>{' '}
              <span className="font-mono">_</span> <span className="font-mono">-</span>, starting
              with a letter or digit, at most 63 characters.
            </p>
          ) : null}
        </div>
      </div>

      {parentLocked ? (
        <div className="space-y-1.5">
          <span className="block text-sm font-medium text-foreground">Parent</span>
          <p className="font-mono text-sm text-muted-foreground">
            {effectiveParent ? effectiveParent.path : '(root)'}
          </p>
        </div>
      ) : (
        <div className="space-y-1.5">
          <label className="block text-sm font-medium text-foreground" htmlFor={`${uid}-parent`}>
            Parent
          </label>
          <Select
            id={`${uid}-parent`}
            className="w-full"
            value={parentId ?? ''}
            onChange={(e) => setParentId(e.target.value || null)}
          >
            <option value="">(root)</option>
            {parentOptions
              .filter((o) => !excluded.has(o.node.id))
              .map((o) => (
                <option key={o.node.id} value={o.node.id}>
                  {o.node.path}
                </option>
              ))}
          </Select>
        </div>
      )}

      <div className="space-y-1.5">
        <label className="block text-sm font-medium text-foreground" htmlFor={`${uid}-quota`}>
          Quota (CU)
        </label>
        <Input
          id={`${uid}-quota`}
          type="number"
          min={0}
          step="any"
          placeholder="0"
          value={quotaInput}
          onChange={(e) => setQuotaInput(e.target.value)}
        />
        <p className="text-xs text-muted-foreground">
          Soft quota — usage above it drives a scheduling penalty but never blocks jobs.
        </p>
      </div>

      {mutation.isError ? (
        <p className="text-sm text-destructive">{errorMessage(mutation.error)}</p>
      ) : null}

      <div className="flex items-center gap-2">
        <Button type="submit" size="sm" disabled={!canSubmit}>
          {mutation.isPending ? 'Saving…' : isEdit ? 'Save changes' : 'Create entity'}
        </Button>
        <Button
          type="button"
          size="sm"
          variant="ghost"
          onClick={onDone}
          className={cn(mutation.isPending && 'pointer-events-none opacity-50')}
        >
          Cancel
        </Button>
      </div>
    </form>
  )
}
