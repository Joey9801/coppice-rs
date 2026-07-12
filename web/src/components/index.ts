// Barrel for the shared app widgets (domain-aware building blocks).
// The vendored primitives under ./ui are imported shadcn-style, e.g.
// `@/components/ui/button`, and are intentionally NOT re-exported here.

export { PageHeader, type PageHeaderProps } from './page-header'
export { StatTile, type StatTileProps } from './stat-tile'
export { StatePill, outcomePill, type StatePillProps, type PillState } from './state-pill'
export { IdLink, type IdLinkProps } from './id-link'
export { TimeAgo, type TimeAgoProps } from './time-ago'
export { ResourceBar, type ResourceBarProps } from './resource-bar'
export { ResourceTriple, type ResourceTripleProps } from './resource-triple'
export { LogViewer, type LogViewerProps } from './log-viewer'
export { EmptyState, type EmptyStateProps } from './empty-state'
export { KeyValueGrid, type KeyValueGridProps } from './key-value-grid'
export { SparkLine, type SparkLineProps } from './spark-line'
