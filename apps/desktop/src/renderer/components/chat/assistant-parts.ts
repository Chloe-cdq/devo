import type { MemoryRecallSnapshot } from "@devo-ai/sdk/v2/client"
import type { SessionCompactionStatus } from "../../atoms/compaction"
import type { ChatMessageEntry } from "../../hooks/use-session-chat"
import type { ReasoningPart, ToolPart } from "../../lib/types"
import {
	COMPACTION_COMPLETED_TEXT,
	compactionStatusFromMetadata,
	isCompactionStatusText,
} from "./compaction-status-divider"

/** A renderable assistant part, including informational transcript records. */
export type RenderablePart =
	| { kind: "tool"; part: ToolPart }
	| { kind: "text"; id: string; text: string; metadata?: Record<string, unknown> }
	| { kind: "reasoning"; part: ReasoningPart }
	| { kind: "compaction"; id: string; status: SessionCompactionStatus }
	| { kind: "memory-recall"; id: string; recall: MemoryRecallSnapshot }

function compactionStatusFromPart(part: {
	text: string
	metadata?: Record<string, unknown>
}): SessionCompactionStatus | null {
	const fromMeta = compactionStatusFromMetadata(part.metadata)
	if (fromMeta) return fromMeta
	if (!isCompactionStatusText(part.text)) return null
	return part.text.trim() === COMPACTION_COMPLETED_TEXT ? "completed" : "started"
}

/**
 * Flattens all assistant parts into an ordered list of renderable items
 * AND extracts the tool-only subset in a single pass.
 * Preserves the natural order: text, reasoning, tool, compaction, text, tool...
 * Filters out synthetic text, todoread without output, and empty text.
 * Strips OpenRouter [REDACTED] chunks from reasoning and skips empty reasoning.
 */
export function getPartsAndTools(assistantMessages: ChatMessageEntry[]): {
	ordered: RenderablePart[]
	tools: ToolPart[]
} {
	const ordered: RenderablePart[] = []
	const tools: ToolPart[] = []
	for (const msg of assistantMessages) {
		for (const part of msg.parts) {
			if (part.type === "tool") {
				tools.push(part)
				if (part.tool === "todoread" && part.state.status !== "completed") continue
				ordered.push({ kind: "tool", part })
			} else if (part.type === "text" && !part.synthetic && part.text.trim()) {
				const metadata = (part as { metadata?: Record<string, unknown> }).metadata
				if (metadata?.["devo/itemKind"] === "memory_recall") {
					ordered.push({ kind: "memory-recall", id: part.id, recall: metadata["devo/memoryRecall"] as MemoryRecallSnapshot })
					continue
				}
				const compactionStatus = compactionStatusFromPart({ text: part.text, metadata })
				if (compactionStatus) {
					ordered.push({ kind: "compaction", id: part.id, status: compactionStatus })
					continue
				}
				ordered.push({ kind: "text", id: part.id, text: part.text, metadata })
			} else if (part.type === "reasoning") {
				// Strip OpenRouter's encrypted [REDACTED] chunks
				const cleaned = part.text.replace("[REDACTED]", "").trim()
				if (cleaned) {
					ordered.push({ kind: "reasoning", part })
				}
			}
		}
	}
	return { ordered, tools }
}
