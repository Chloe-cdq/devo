import { describe, expect, test } from "bun:test";
import {
	createDevoClient,
	type DevoNativeTransport,
	type DevoNativeTransportEvent,
} from "./client";

const session = {
	id: "session-automation",
	version: 1,
	cwd: process.cwd(),
	source: "automation",
	createdAt: "2026-09-30T00:00:00Z",
	lastActivityAt: "2026-09-30T00:00:00Z",
	status: "idle",
	flags: [],
	archived: false,
	ephemeral: false,
	queuedCount: 0,
	model: { provider: "test", model: "test-model" },
	settings: { permissionProfile: "default" },
	preview: "",
	usage: {
		total: {
			inputTokens: 0,
			outputTokens: 0,
			cacheCreationInputTokens: 0,
			cacheReadInputTokens: 0,
			reasoningTokens: 0,
			totalTokens: 0,
			callCount: 0,
			meteredCallCount: 0,
			failedCallCount: 0,
			cancelledCallCount: 0,
		},
		byPurpose: [],
		updatedAt: "2026-09-30T00:00:00Z",
	},
};

class Transport implements DevoNativeTransport {
	requests: Array<{ method: string; params: unknown }> = [];
	async request(method: string, params?: unknown): Promise<unknown> {
		this.requests.push({ method, params });
		switch (method) {
			case "initialize":
				return { protocolVersion: 1, agentCapabilities: {}, authMethods: [] };
			case "session/list":
				return { data: [], nextCursor: null };
			case "session/new":
				return { session };
			case "subscription/create":
				return { subscriptionId: "sub-memory", cursors: [], snapshots: [] };
			case "session/metadata/update":
				return { session, appliedToActiveTurn: false };
			default:
				throw new Error(`Unexpected method ${method}`);
		}
	}
	async respond(): Promise<void> {}
	subscribe(_listener: (event: DevoNativeTransportEvent) => void): () => void {
		return () => {};
	}
	connected(): boolean {
		return true;
	}
}

describe("automation memory Native adapter", () => {
	// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-1/DD-2, L2-DES-APP-008 Rev 5
	// Verifies: Native session creation carries automation identity before any turn.
	test("creation sends the automation source before any turn", async () => {
		const transport = new Transport();
		const client = createDevoClient({ directory: process.cwd(), transport });
		await client.session.create({ source: "automation" });
		const request = transport.requests.find((request) => request.method === "session/new")!;
		const params = request.params as { idempotencyKey: string };
		expect(request).toEqual({
			method: "session/new",
			params: {
				cwd: process.cwd(),
				idempotencyKey: params.idempotencyKey,
				source: "automation",
			},
		});
	});

	// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-2, L2-DES-CONV-002 Rev 2 DD-6
	// Verifies: memory preferences use canonical partial session settings patches.
	test("recall and contribution use canonical partial settings patches", async () => {
		const transport = new Transport();
		const client = createDevoClient({ directory: process.cwd(), transport });
		await client.session.create();
		for (const memoryRecall of ["on", "off", "inherit"] as const) {
			await client.session.updateSettings({
				sessionID: session.id,
				memoryRecall,
				memoryContribution: "off",
			});
			expect(transport.requests.at(-1)).toEqual({
				method: "session/metadata/update",
				params: {
					sessionId: session.id,
					expectedVersion: 0,
					settings: { memoryRecall, memoryContribution: "off" },
				},
			});
		}
	});
});
