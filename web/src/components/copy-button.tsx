import { useState } from 'react'
import { Check, Copy } from 'lucide-react'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

export interface CopyButtonProps {
  value: string
  ariaLabel?: string
}

/** Icon button that copies `value` to the clipboard, flashing a check mark. */
export function CopyButton({ value, ariaLabel = 'Copy id' }: CopyButtonProps) {
  const [copied, setCopied] = useState(false)

  const copy = () => {
    void navigator.clipboard?.writeText(value).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 1200)
    })
  }

  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            onClick={copy}
            aria-label={ariaLabel}
            className="inline-flex size-5 items-center justify-center rounded text-muted-foreground transition-colors hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            {copied ? (
              <Check className="size-3 text-emerald-600 dark:text-emerald-400" />
            ) : (
              <Copy className="size-3" />
            )}
          </button>
        </TooltipTrigger>
        <TooltipContent>
          <span className="font-mono">{value}</span>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
