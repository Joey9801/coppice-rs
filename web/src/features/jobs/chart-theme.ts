/** Shared recharts styling tokens (semantic CSS vars) for the job charts. */
export const AXIS_TICK = { fill: 'var(--muted-foreground)', fontSize: 11 } as const

export const TOOLTIP_CONTENT_STYLE = {
  background: 'var(--popover)',
  border: '1px solid var(--border)',
  color: 'var(--popover-foreground)',
  borderRadius: 8,
} as const
