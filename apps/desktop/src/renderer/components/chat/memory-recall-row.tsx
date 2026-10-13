import type { MemoryRecallSnapshot } from "@devo-ai/sdk/v2/client"
import { BrainIcon } from "lucide-react"
import { memo } from "react"
import {
	TranscriptDisclosure,
	TranscriptDisclosureContent,
	TranscriptDisclosureTrigger,
} from "./transcript-disclosure"

export const MemoryRecallRow = memo(function MemoryRecallRow({
	recall,
	defaultOpen = false,
}: {
	recall: MemoryRecallSnapshot
	defaultOpen?: boolean
}) {
	const count = recall.entries.length
	return (
		<TranscriptDisclosure defaultOpen={defaultOpen}>
			<TranscriptDisclosureTrigger
				label={`Recalled ${count} ${count === 1 ? "memory" : "memories"}`}
				leading={<BrainIcon aria-hidden="true" className="size-3.5 shrink-0" strokeWidth={1.5} />}
				className="focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
			/>
			<TranscriptDisclosureContent rail className="space-y-3 text-[13px] leading-5">
				<p className="text-muted-foreground">Advisory context for this turn.</p>
				{recall.entries.map((entry) => (
					<div key={entry.entryId} className="space-y-1 break-words">
						<p>{entry.summary}</p>
						<p className="text-muted-foreground">{entry.scope} · {entry.kind}</p>
						<p className="text-muted-foreground">{entry.sourceSummary}</p>
						<p className="font-mono text-xs text-muted-foreground">{entry.entryId}</p>
					</div>
				))}
			</TranscriptDisclosureContent>
		</TranscriptDisclosure>
	)
})
