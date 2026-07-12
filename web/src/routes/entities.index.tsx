import { createFileRoute } from '@tanstack/react-router'
import { EntitiesPage } from '@/features/entities/entities-page'

export const Route = createFileRoute('/entities/')({
  component: EntitiesPage,
})
