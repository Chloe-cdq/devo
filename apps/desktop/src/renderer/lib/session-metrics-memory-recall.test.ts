import { expect, test } from "bun:test"
import { computeSessionMetricsExtended } from "./session-metrics"
import type { Message, Part } from "./types"

// Trace: L1-REQ-MEM-001 Rev 2 Acceptance; L2-DES-MEM-001 Rev 4 DD-11.
// Verifies: informational recall records do not count as model responses or completed exchanges.
test("memory recall does not change response metrics", () => {
	const user = { id: "user-1", role: "user", time: { created: 1 } } as Message
	const answer = { id: "answer-1", role: "assistant", parentID: "user-1", time: { created: 2, completed: 12 } } as Message
	const recall = { id: "recall-1", role: "assistant", parentID: "user-1", time: { created: 2, completed: 5 } } as Message
	const parts = [{ id: "recall-1-text", messageID: "recall-1", type: "text", text: "Recalled 0 memories", metadata: { "devo/itemKind": "memory_recall" } }] as Part[]
	for (const responses of [[], [answer]]) {
		expect(computeSessionMetricsExtended([user, recall, ...responses], parts, () => "run"))
			.toEqual(computeSessionMetricsExtended([user, ...responses], [], () => "run"))
	}
})
