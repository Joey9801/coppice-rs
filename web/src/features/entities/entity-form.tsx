import { useState } from 'react'
import type { ConfigureQuotaEntityInput, QuotaEntityNode } from '@/api/types'
import { useConfigureQuotaEntity } from '@/api/queries'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { cn } from '@/lib/utils'
import {
  buildEntityTree,
  costUnitsToUcu,
  descendantIds,
  flattenTree,
  lastSegment,
  ucuToCostUnits,
} from './lib'

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

  // Create-under-a-parent types just the leaf segment (parent path is a fixed
  // prefix); root create and edit type the full slash path.
  const usesSegment = props.mode === 'create' && effectiveParent !== null
  const [segment, setSegment] = useState('')
  const [fullName, setFullName] = useState(entity?.name ?? '')

  const [quotaInput, setQuotaInput] = useState(
    entity ? String(ucuToCostUnits(entity.quotaUcu)) : '',
  )

  const parentOptions = flattenTree(buildEntityTree(allEntities))
  const excluded = entity ? descendantIds(allEntities, entity.id) : new Set<string>()

  const composedName = isSso
    ? (entity?.name ?? '')
    : usesSegment
      ? `${effectiveParent?.name ?? ''}/${segment.trim()}`
      : fullName.trim()

  const quotaCu = Number(quotaInput)
  const quotaValid = quotaInput.trim() !== '' && Number.isFinite(quotaCu) && quotaCu >= 0
  const nameValid = usesSegment ? segment.trim() !== '' : composedName !== ''
  const canSubmit = nameValid && quotaValid && !mutation.isPending

  const submit = (event: React.FormEvent) => {
    event.preventDefault()
    if (!canSubmit) return
    const input: ConfigureQuotaEntityInput = {
      entity: entity?.id ?? null,
      parent: isSso ? (entity?.parent ?? null) : effectiveParent ? effectiveParent.id : null,
      name: composedName,
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
        <label className="block text-sm font-medium text-foreground" htmlFor="entity-name">
          Name
        </label>
        {isSso ? (
          <p className="font-mono text-sm text-muted-foreground">{entity?.name}</p>
        ) : usesSegment ? (
          <div className="flex items-center rounded-md border border-input bg-transparent pl-3 shadow-sm focus-within:ring-2 focus-within:ring-ring">
            <span className="whitespace-nowrap font-mono text-sm text-muted-foreground">
              {effectiveParent?.name}/
            </span>
            <input
              id="entity-name"
              className="h-9 w-full bg-transparent pr-3 text-sm focus-visible:outline-none"
              placeholder="segment"
              value={segment}
              onChange={(e) => setSegment(e.target.value)}
            />
          </div>
        ) : (
          <Input
            id="entity-name"
            placeholder="Acme/Eng/Platform"
            value={fullName}
            onChange={(e) => setFullName(e.target.value)}
          />
        )}
        {!isSso && !usesSegment ? (
          <p className="text-xs text-muted-foreground">
            Full slash-separated path, e.g. <span className="font-mono">Acme/Eng/Platform</span>.
          </p>
        ) : null}
      </div>

      {parentLocked ? (
        <div className="space-y-1.5">
          <span className="block text-sm font-medium text-foreground">Parent</span>
          <p className="font-mono text-sm text-muted-foreground">
            {effectiveParent ? effectiveParent.name : '(root)'}
          </p>
        </div>
      ) : (
        <div className="space-y-1.5">
          <label className="block text-sm font-medium text-foreground" htmlFor="entity-parent">
            Parent
          </label>
          <Select
            id="entity-parent"
            className="w-full"
            value={parentId ?? ''}
            onChange={(e) => setParentId(e.target.value || null)}
          >
            <option value="">(root)</option>
            {parentOptions
              .filter((o) => !excluded.has(o.node.id))
              .map((o) => (
                <option key={o.node.id} value={o.node.id}>
                  {'— '.repeat(o.depth)}
                  {lastSegment(o.node.name)}
                </option>
              ))}
          </Select>
        </div>
      )}

      <div className="space-y-1.5">
        <label className="block text-sm font-medium text-foreground" htmlFor="entity-quota">
          Quota (CU)
        </label>
        <Input
          id="entity-quota"
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
