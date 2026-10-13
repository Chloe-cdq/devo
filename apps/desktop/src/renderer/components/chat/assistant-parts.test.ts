import { describe, expect, test } from "bun:test"
import { getPartsAndTools } from "./assistant-parts"

describe("recall display parts", () => {
	// Trace: L1-REQ-MEM-001 Rev 2 Acceptance; L2-DES-MEM-001 Rev 4 DD-11.
	// Verifies: recall is a separate structured row instead of assistant response text, including after the answer.
	test("keeps recall out of assistant answer text", () => {
		const recall = { snapshotRevision: "snapshot-1", entries: [{ entryId: "memory-1", scope: "project", kind: "preference", summary: "Prefer Rust", sourceSummary: "Explicit user; 1 evidence source" }] }
		expect(getPartsAndTools([{
			info: { id: "assistant-1", role: "assistant" },
			parts: [
				{ id: "answer-1", type: "text", text: "Done" },
				{ id: "recall-1", type: "text", text: "Recalled 1 memory", metadata: { "devo/itemKind": "memory_recall", "devo/memoryRecall": recall } },
			],
		} as any])).toEqual({
			ordered: [
				{ kind: "text", id: "answer-1", text: "Done", metadata: undefined },
				{ kind: "memory-recall", id: "recall-1", recall },
			],
			tools: [],
		})
	})
})
