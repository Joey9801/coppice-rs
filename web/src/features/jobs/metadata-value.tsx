import { IdLink } from '@/components'
import { isExternalUrl, isTypedCoppiceId } from './metadata-shape'

/**
 * Shape-based rendering of one metadata value (ADR 0042): a value is always
 * a string, and how it is shown follows from its shape — an `http(s)` URL
 * becomes an external link, a typed Coppice id becomes an `IdLink`, and
 * everything else is plain text.
 */
export function MetadataValue({ value }: { value: string }) {
  if (isExternalUrl(value)) {
    return (
      <a
        href={value}
        target="_blank"
        rel="noreferrer"
        className="break-all text-primary hover:underline"
      >
        {value}
      </a>
    )
  }
  if (isTypedCoppiceId(value)) {
    return <IdLink id={value} />
  }
  return <span className="break-all">{value}</span>
}
