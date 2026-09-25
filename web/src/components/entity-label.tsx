import { Fragment, type ReactNode } from 'react'
import { Link } from '@tanstack/react-router'
import type { QuotaEntityId, QuotaEntityView } from '@/api/types'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'
import { CopyButton } from './copy-button'

/*
 * One consistent rendering of a quota-entity reference (ADR 0045): the path
 * (`acme/eng/platform`) is what people read; the `quota-<uuid>` id is the
 * durable identity and stays visible — as subtext where there is room, in a
 * tooltip where there is not — and always copyable. Links go to the id-keyed
 * `/entities/$entityId` route so a bookmark survives a rename.
 */

/** Tooltip body: the full path over the full id, both mono. */
function EntityTooltipBody({ id, path }: { id: QuotaEntityId; path: string }) {
  return (
    <div className="space-y-0.5 text-xs">
      <div className="font-mono text-foreground">{path}</div>
      <div className="font-mono text-muted-foreground">{id}</div>
    </div>
  )
}

export interface EntityLabelProps {
  id: QuotaEntityId
  /** The entity's path as the server resolved it on this read. */
  path: string
  /**
   * `inline` (default): the path alone, with the id in a tooltip and a copy
   * button — for table cells and chips. `stacked`: the path over a muted
   * mono id line — for detail headers and cards.
   */
  variant?: 'inline' | 'stacked'
  /** Link the path to the entity's detail page (default true). */
  link?: boolean
  className?: string
}

export function EntityLabel({
  id,
  path,
  variant = 'inline',
  link = true,
  className,
}: EntityLabelProps) {
  // A path is always served alongside the id; fall back to the id rather
  // than render an empty label if one ever arrives blank.
  const label = path || id
  const text = link ? (
    <Link
      to="/entities/$entityId"
      params={{ entityId: id }}
      className="min-w-0 truncate text-sm text-primary hover:underline"
    >
      {label}
    </Link>
  ) : (
    <span className="min-w-0 truncate text-sm text-foreground">{label}</span>
  )

  if (variant === 'stacked') {
    return (
      <span className={cn('inline-flex min-w-0 flex-col', className)}>
        {text}
        <span className="inline-flex items-center gap-1">
          <span className="break-all font-mono text-xs text-muted-foreground">{id}</span>
          <CopyButton value={id} ariaLabel="Copy entity id" />
        </span>
      </span>
    )
  }

  return (
    <span
      className={cn(
        'inline-flex min-w-0 max-w-full items-center gap-1 whitespace-nowrap',
        className,
      )}
    >
      <TooltipProvider delayDuration={300}>
        <Tooltip>
          <TooltipTrigger asChild>{text}</TooltipTrigger>
          <TooltipContent>
            <EntityTooltipBody id={id} path={label} />
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
      <CopyButton value={id} ariaLabel="Copy entity id" />
    </span>
  )
}

export interface EntitySegmentProps {
  id: QuotaEntityId
  /** The entity's own segment, shown as the label. */
  name: string
  /** The full path, carried in the tooltip with the id. */
  path: string
  className?: string
}

/**
 * A tree/child-table cell: just the entity's own segment (its depth already
 * places it), with the full path and id in a tooltip. Not a link — tree rows
 * navigate on click themselves.
 */
export function EntitySegment({ id, name, path, className }: EntitySegmentProps) {
  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className={cn('font-medium text-foreground', className)}>{name}</span>
        </TooltipTrigger>
        <TooltipContent>
          <EntityTooltipBody id={id} path={path} />
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

export interface EntityBreadcrumbProps {
  /** Ancestry, root first, the entity itself last. */
  chain: QuotaEntityView[]
  /** Link the last segment too (default: only ancestors link). */
  linkLeaf?: boolean
  /** Rendered after each segment (e.g. an over-quota badge). */
  renderSuffix?: (entity: QuotaEntityView) => ReactNode
  className?: string
}

/**
 * The path, spelled as its segments with each ancestor linking to its own
 * page: `acme / research / training`. Each segment's tooltip carries that
 * entity's full path and id.
 */
export function EntityBreadcrumb({
  chain,
  linkLeaf = false,
  renderSuffix,
  className,
}: EntityBreadcrumbProps) {
  if (chain.length === 0) return null
  return (
    <span className={cn('flex flex-wrap items-center gap-x-1 gap-y-1', className)}>
      {chain.map((entity, i) => {
        const leaf = i === chain.length - 1
        const label =
          leaf && !linkLeaf ? (
            <span className="font-medium text-foreground">{entity.name}</span>
          ) : (
            <Link
              to="/entities/$entityId"
              params={{ entityId: entity.id }}
              className={cn(
                'hover:underline',
                leaf
                  ? 'font-medium text-foreground'
                  : 'text-muted-foreground hover:text-foreground',
              )}
            >
              {entity.name}
            </Link>
          )
        return (
          <Fragment key={entity.id}>
            {i > 0 ? <span className="text-muted-foreground">/</span> : null}
            <TooltipProvider delayDuration={300}>
              <Tooltip>
                <TooltipTrigger asChild>{label}</TooltipTrigger>
                <TooltipContent>
                  <EntityTooltipBody id={entity.id} path={entity.path} />
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            {renderSuffix?.(entity)}
          </Fragment>
        )
      })}
    </span>
  )
}
