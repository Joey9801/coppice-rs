import { createFileRoute } from '@tanstack/react-router'
import { EntityDetailPage } from '@/features/entities/entity-detail-page'

export const Route = createFileRoute('/entities/$entityId')({
  component: EntityDetailRoute,
})

function EntityDetailRoute() {
  const { entityId } = Route.useParams()
  return <EntityDetailPage entityId={entityId} />
}
