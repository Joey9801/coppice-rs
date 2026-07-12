import { createFileRoute } from '@tanstack/react-router'
import { CoordinatorsPage } from '@/features/coordinators/coordinators-page'

export const Route = createFileRoute('/coordinators')({
  component: CoordinatorsPage,
})
