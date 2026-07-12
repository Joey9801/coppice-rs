import { createFileRoute } from '@tanstack/react-router'
import { NodeDetailPage } from '@/features/nodes/node-detail-page'

export const Route = createFileRoute('/nodes/$nodeId')({
  component: NodeDetailRoute,
})

function NodeDetailRoute() {
  const { nodeId } = Route.useParams()
  return <NodeDetailPage nodeId={nodeId} />
}
