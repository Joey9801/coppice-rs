import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import type { JobDetail, JobSpec } from '@/api/types'
import { formatDurationUs, formatResources, shortId } from '@/lib/format'
import { KeyValueGrid } from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

/** How much command line to show before hiding the rest behind a toggle. */
const COMMAND_PREVIEW_CHARS = 120

export function JobSpecCard({ job }: { job: JobDetail }) {
  const { spec } = job
  const entity = job.entityChain[job.entityChain.length - 1]

  const items = [
    { label: 'Image', value: <span className="font-mono text-xs break-all">{spec.image}</span> },
    { label: 'Command', value: <CommandLine spec={spec} /> },
    { label: 'Environment', value: <EnvVars env={spec.env} /> },
    { label: 'Resources', value: formatResources(spec.requests) },
    { label: 'Priority class', value: <span className="tabular-nums">{spec.priority}</span> },
    {
      label: 'Max runtime',
      value:
        spec.maxRuntimeUs != null ? (
          formatDurationUs(spec.maxRuntimeUs)
        ) : (
          <span className="text-muted-foreground">none — cost estimate uses policy default</span>
        ),
    },
    {
      label: 'Retry policy',
      value: `up to ${spec.retry.maxRetries} ${
        spec.retry.maxRetries === 1 ? 'retry' : 'retries'
      }, user errors: ${spec.retry.retryUserErrors ? 'yes' : 'no'}`,
    },
    {
      label: 'Quota entity',
      value: (
        <span>
          {entity ? entity.name : '—'}{' '}
          <span className="ml-1 font-mono text-xs text-muted-foreground">
            {shortId(spec.quotaEntity)}
          </span>
        </span>
      ),
    },
  ]

  return (
    <Card>
      <CardHeader className="p-4 pb-0">
        <CardTitle className="text-sm">Specification</CardTitle>
      </CardHeader>
      <CardContent className="p-4">
        <KeyValueGrid items={items} />
      </CardContent>
    </Card>
  )
}

/** Quote a token the way a shell would need it, for display only. */
function shellQuote(token: string): string {
  if (token === '') return "''"
  if (/^[A-Za-z0-9_\-./:=@%+,]+$/.test(token)) return token
  return `'${token.replaceAll("'", String.raw`'\''`)}'`
}

/**
 * The effective command line (entrypoint override + command argv). Specs can be
 * arbitrarily large, so long commands render a one-line preview with the
 * full argv behind a toggle instead of dumping everything unconditionally.
 */
function CommandLine({ spec }: { spec: JobSpec }) {
  const [open, setOpen] = useState(false)
  const tokens = [...(spec.entrypoint ?? []), ...spec.command]

  if (tokens.length === 0) {
    return <span className="text-muted-foreground">image default</span>
  }

  const text = tokens.map(shellQuote).join(' ')
  const prefix = spec.entrypoint === null ? '(image entrypoint) ' : ''

  if (text.length <= COMMAND_PREVIEW_CHARS) {
    return (
      <span className="font-mono text-xs break-all">
        {prefix ? <span className="text-muted-foreground">{prefix}</span> : null}
        {text}
      </span>
    )
  }

  return (
    <div className="min-w-0 space-y-1">
      {open ? (
        <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-all rounded-md border bg-muted/30 p-2 font-mono text-xs">
          {prefix}
          {text}
        </pre>
      ) : (
        <span className="block truncate font-mono text-xs">
          {prefix ? <span className="text-muted-foreground">{prefix}</span> : null}
          {text}
        </span>
      )}
      <DisclosureButton
        open={open}
        onToggle={() => setOpen((v) => !v)}
        showLabel={`Show full command (${tokens.length} tokens)`}
        hideLabel="Hide full command"
      />
    </div>
  )
}

/** Env overlay, collapsed to a count — values can be numerous and noisy. */
function EnvVars({ env }: { env: Record<string, string> }) {
  const [open, setOpen] = useState(false)
  const entries = Object.entries(env)

  if (entries.length === 0) {
    return <span className="text-muted-foreground">none</span>
  }

  return (
    <div className="min-w-0 space-y-1">
      <DisclosureButton
        open={open}
        onToggle={() => setOpen((v) => !v)}
        showLabel={`${entries.length} ${entries.length === 1 ? 'variable' : 'variables'}`}
        hideLabel={`${entries.length} ${entries.length === 1 ? 'variable' : 'variables'}`}
      />
      {open ? (
        <dl className="max-h-56 space-y-0.5 overflow-auto rounded-md border bg-muted/30 p-2 font-mono text-xs">
          {entries.map(([name, value]) => (
            <div key={name} className="break-all">
              <dt className="inline text-muted-foreground after:content-['=']">{name}</dt>
              <dd className="inline">{value}</dd>
            </div>
          ))}
        </dl>
      ) : null}
    </div>
  )
}

function DisclosureButton({
  open,
  onToggle,
  showLabel,
  hideLabel,
}: {
  open: boolean
  onToggle: () => void
  showLabel: string
  hideLabel: string
}) {
  const Icon = open ? ChevronDown : ChevronRight
  return (
    <button
      type="button"
      onClick={onToggle}
      className="inline-flex items-center gap-1 text-xs text-primary hover:underline"
    >
      <Icon className="size-3.5" />
      {open ? hideLabel : showLabel}
    </button>
  )
}
