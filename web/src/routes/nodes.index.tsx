import { createFileRoute } from '@tanstack/react-router'
import { NodesPage } from '@/features/nodes/nodes-page'

export const Route = createFileRoute('/nodes/')({
  component: NodesPage,
})
