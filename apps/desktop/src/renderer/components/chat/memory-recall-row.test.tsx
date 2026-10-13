import { describe, expect, test } from "bun:test"
import { Conversation, ConversationContent } from "@devo/ui/components/ai-elements/conversation"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRecallRow } from "./memory-recall-row"

const recall = {
	snapshotRevision: "snapshot-1",
	entries: [{
		entryId: "memory-1", scope: "project" as const, kind: "preference" as const,
		summary: "Prefer <Rust> & TypeScript", sourceSummary: "Explicit user; 1 evidence source",
	}],
}

describe("recall disclosure", () => {
	// Trace: L1-REQ-MEM-001 Rev 2 Acceptance; L2-DES-MEM-001 Rev 4 DD-11.
	// Verifies: the real row exposes a collapsed keyboard-operable disclosure, and expansion shows safe data.
	test("renders collapsed and expanded recall records", () => {
		const render = (defaultOpen: boolean) => renderToStaticMarkup(
			<Conversation><ConversationContent>
				<MemoryRecallRow recall={recall} defaultOpen={defaultOpen} />
			</ConversationContent></Conversation>,
		)
		const collapsed = render(false)
		const expanded = render(true)
		expect({
			collapsedTrigger: collapsed.includes('aria-expanded="false"'),
			collapsedLabel: collapsed.includes("Recalled 1 memory"),
			collapsedDetails: collapsed.includes("memory-1"),
			expandedTrigger: expanded.includes('aria-expanded="true"'),
			entryId: expanded.includes("memory-1"),
			summary: expanded.includes("Prefer &lt;Rust&gt; &amp; TypeScript"),
			source: expanded.includes("Explicit user; 1 evidence source"),
			advisory: expanded.includes("Advisory context for this turn."),
		}).toEqual({
			collapsedTrigger: true, collapsedLabel: true, collapsedDetails: false,
			expandedTrigger: true, entryId: true, summary: true, source: true, advisory: true,
		})
	})
})
