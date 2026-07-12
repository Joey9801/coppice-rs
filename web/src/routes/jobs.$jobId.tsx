import { createFileRoute } from '@tanstack/react-router'
import { JobDetailPage } from '@/features/jobs/job-detail-page'

export const Route = createFileRoute('/jobs/$jobId')({
  component: RouteComponent,
})

function RouteComponent() {
  const { jobId } = Route.useParams()
  return <JobDetailPage jobId={jobId} />
}
