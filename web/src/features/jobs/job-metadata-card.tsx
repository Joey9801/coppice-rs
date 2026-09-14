import { useState } from 'react'
import type { JobDetail } from '@/api/types'
import { useUpdateJobMetadata } from '@/api/queries'
import {
  JOB_METADATA_MAX_KEYS,
  validateMetadataKey,
  validateMetadataValue,
} from '@/lib/job-metadata'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { cn } from '@/lib/utils'
import { MetadataValue } from './metadata-value'

/** Render `error.message` off the mutation without importing the ApiError class. */
function errorMessage(error: unknown): string {
  if (error && typeof error === 'object' && 'message' in error) {
    const message = (error as { message?: unknown }).message
    if (typeof message === 'string') return message
  }
  return 'Something went wrong.'
}

type EditorTarget = { mode: 'add' } | { mode: 'edit'; key: string }

export function JobMetadataCard({ job }: { job: JobDetail }) {
  const [editing, setEditing] = useState<EditorTarget | null>(null)
  const keys = Object.keys(job.metadata).sort()
  const atKeyLimit = keys.length >= JOB_METADATA_MAX_KEYS

  const closeEditor = () => setEditing(null)

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between gap-2 p-4 pb-0">
        <CardTitle className="text-sm">Metadata</CardTitle>
        {editing == null ? (
          <Button
            type="button"
            size="sm"
            variant="ghost"
            disabled={atKeyLimit}
            title={
              atKeyLimit ? `A job may carry at most ${JOB_METADATA_MAX_KEYS} keys.` : undefined
            }
            onClick={() => setEditing({ mode: 'add' })}
          >
            Add
          </Button>
        ) : null}
      </CardHeader>
      <CardContent className="space-y-3 p-4">
        {keys.length === 0 && editing == null ? (
          <div className="space-y-2 text-sm text-muted-foreground">
            <p>No metadata.</p>
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={() => setEditing({ mode: 'add' })}
            >
              Add metadata
            </Button>
          </div>
        ) : (
          <dl className="space-y-2.5 text-sm">
            {keys.map((key) => (
              <div key={key} className="flex items-start justify-between gap-3">
                <div className="min-w-0 flex-1">
                  <dt className="font-mono text-xs text-muted-foreground">{key}</dt>
                  <dd className="min-w-0 text-foreground">
                    <MetadataValue value={job.metadata[key] ?? ''} />
                  </dd>
                </div>
                {editing == null ? (
                  <div className="flex shrink-0 items-center gap-1">
                    <Button
                      type="button"
                      size="sm"
                      variant="ghost"
                      onClick={() => setEditing({ mode: 'edit', key })}
                    >
                      Edit
                    </Button>
                    <RemoveButton jobId={job.id} metadataKey={key} />
                  </div>
                ) : null}
              </div>
            ))}
          </dl>
        )}

        {editing != null ? (
          <MetadataEditor
            job={job}
            target={editing}
            existingKeys={keys}
            onDone={closeEditor}
            onCancel={closeEditor}
          />
        ) : null}
      </CardContent>
    </Card>
  )
}

function RemoveButton({ jobId, metadataKey }: { jobId: JobDetail['id']; metadataKey: string }) {
  const mutation = useUpdateJobMetadata()
  return (
    <Button
      type="button"
      size="sm"
      variant="ghost"
      className="text-destructive hover:text-destructive"
      disabled={mutation.isPending}
      onClick={() => mutation.mutate({ id: jobId, unset: [metadataKey] })}
    >
      Remove
    </Button>
  )
}

function MetadataEditor({
  job,
  target,
  existingKeys,
  onDone,
  onCancel,
}: {
  job: JobDetail
  target: EditorTarget
  existingKeys: string[]
  onDone: () => void
  onCancel: () => void
}) {
  const mutation = useUpdateJobMetadata()
  const isAdd = target.mode === 'add'
  const [key, setKey] = useState(isAdd ? '' : target.key)
  const [value, setValue] = useState(isAdd ? '' : (job.metadata[target.key] ?? ''))

  const keyError = isAdd
    ? (validateMetadataKey(key) ?? (existingKeys.includes(key) ? 'Key already exists.' : null))
    : null
  // An empty value is legal (ADR 0042), so there is nothing to check but
  // the byte limit.
  const valueError = validateMetadataValue(value)
  const canSubmit = !keyError && !valueError && !mutation.isPending

  const submit = (event: React.FormEvent) => {
    event.preventDefault()
    if (!canSubmit) return
    const targetKey = isAdd ? key : target.key
    mutation.mutate({ id: job.id, set: { [targetKey]: value } }, { onSuccess: () => onDone() })
  }

  return (
    <form onSubmit={submit} className="space-y-2 rounded-md border border-border p-3">
      {isAdd ? (
        <div className="space-y-1">
          <label className="block text-xs font-medium text-foreground" htmlFor="metadata-key">
            Key
          </label>
          <Input
            id="metadata-key"
            value={key}
            onChange={(e) => setKey(e.target.value)}
            placeholder="ticket"
            className="font-mono text-sm"
          />
        </div>
      ) : (
        <div className="space-y-1">
          <span className="block text-xs font-medium text-foreground">Key</span>
          <p className="font-mono text-sm text-muted-foreground">{target.key}</p>
        </div>
      )}

      <div className="space-y-1">
        <label className="block text-xs font-medium text-foreground" htmlFor="metadata-value">
          Value
        </label>
        <Input
          id="metadata-value"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          placeholder="INC-1234"
          className="font-mono text-sm"
        />
      </div>

      {keyError ? <p className="text-sm text-destructive">{keyError}</p> : null}
      {!keyError && valueError ? <p className="text-sm text-destructive">{valueError}</p> : null}
      {mutation.isError ? (
        <p className="text-sm text-destructive">{errorMessage(mutation.error)}</p>
      ) : null}

      <div className="flex items-center gap-2">
        <Button type="submit" size="sm" disabled={!canSubmit}>
          {mutation.isPending ? 'Saving…' : 'Save'}
        </Button>
        <Button
          type="button"
          size="sm"
          variant="ghost"
          onClick={onCancel}
          className={cn(mutation.isPending && 'pointer-events-none opacity-50')}
        >
          Cancel
        </Button>
      </div>
    </form>
  )
}
