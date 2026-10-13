import { describe, expect, test } from "bun:test"
import { createDevoClient, type DevoNativeTransport, type DevoNativeTransportEvent } from "./client"

const timestamp = "2026-10-12T00:00:00Z"
const usage = { inputTokens: 0, outputTokens: 0, cacheCreationInputTokens: 0, cacheReadInputTokens: 0, reasoningTokens: 0, totalTokens: 0, callCount: 0, meteredCallCount: 0, failedCallCount: 0, cancelledCallCount: 0 }
const session = {
	id: "session-1", version: 1, cwd: process.cwd(), title: "Memory", source: "interactive",
	createdAt: timestamp, lastActivityAt: timestamp, status: "idle", flags: [], archived: false,
	ephemeral: false, model: { provider: "test", model: "test" },
	settings: { permissionProfile: "default" }, preview: "", queuedCount: 0,
	usage: { total: usage, byPurpose: [], updatedAt: timestamp },
}
const recall = {
	type: "memoryRecall", snapshotRevision: "snapshot-1",
	entries: [{
		entryId: "memory-1", scope: "project", kind: "preference",
		summary: "Prefer Rust", sourceSummary: "Explicit user; 1 evidence source",
	}],
}
const userItem = {
	id: "user-1", sessionId: "session-1", turnId: "turn-1", seq: 1, revision: 1,
	createdAt: timestamp, updatedAt: timestamp, state: "completed",
	item: { type: "userMessage", content: [{ type: "text", text: "Build a tool" }] },
}
const recallItem = {
	...userItem, id: "recall-1", seq: 2, item: recall,
}

class MemoryTransport implements DevoNativeTransport {
	private listener?: (event: DevoNativeTransportEvent) => void
	constructor(private readonly restored: boolean) {}
	async request(method: string): Promise<unknown> {
		switch (method) {
			case "initialize": return { protocolVersion: 1, agentCapabilities: {}, authMethods: [] }
			case "session/list": return { data: [session], nextCursor: null }
			case "session/resume": return { session, lastContextOccupancy: null }
			case "session/items/list": return { data: this.restored ? [userItem, recallItem] : [userItem], nextCursor: null }
			case "session/queue/list": return { entries: [] }
			case "subscription/create": return { subscriptionId: "sub-1", snapshots: [], replay: [], cursors: [] }
			case "subscription/ack": return { serverTimeMs: 0 }
			default: throw new Error(`unexpected method ${method}`)
		}
	}
	async respond(): Promise<void> {}
	subscribe(listener: (event: DevoNativeTransportEvent) => void): () => void {
		this.listener = listener
		return () => { this.listener = undefined }
	}
	connected(): boolean { return true }
	completeRecall() {
		this.listener?.({ type: "notification", method: "item/completed", params: { item: recallItem } })
	}
}

describe("Native memory recall display", () => {
	for (const restored of [false, true]) {
		// Trace: L1-REQ-MEM-001 Rev 2 Acceptance; L2-DES-MEM-001 Rev 4 DD-11.
		// Verifies: live and restored recall becomes one turn-bound, structured display part without duplication.
		test(restored ? "restores the recall display part" : "displays live recall once", async () => {
			const transport = new MemoryTransport(restored)
			const client = createDevoClient({ directory: process.cwd(), transport })
			await client.session.messages({ sessionID: "session-1" })
			transport.completeRecall()
			transport.completeRecall()
			const { data } = await client.session.messages({ sessionID: "session-1" })
			expect(data.filter((message) => message.info.id === "recall-1").map((message) => ({
				id: message.info.id, parentID: message.info.parentID, turnID: message.info.turnID,
				parts: message.parts.map((part) => ({ text: part.text, metadata: part.metadata })),
			}))).toEqual([{
				id: "recall-1", parentID: "user-1", turnID: "turn-1",
				parts: [{
					text: "Recalled 1 memory",
					metadata: { "devo/itemKind": "memory_recall", "devo/memoryRecall": { snapshotRevision: "snapshot-1", entries: recall.entries } },
				}],
			}])
		})
	}
})
