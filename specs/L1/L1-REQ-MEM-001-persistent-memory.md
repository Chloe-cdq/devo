---
artifact_id: L1-REQ-MEM-001
revision: 1
status: Draft
active_baseline: no
supersedes:
superseded_by:
owner: Human
last_updated: 2026-05-22
---

# L1-REQ-MEM-001 — Persistent Memory

## Purpose

Define persistent memory as agent-maintained core state rather than a client-managed user feature.

## Why This Matters

Persistent memory can help the agent carry useful preferences, project knowledge, and recurring decisions across sessions. However, requiring users to inspect, curate, export, or delete individual memory entries adds unnecessary client complexity and makes memory feel like a user-facing database rather than an internal agent capability.

## Background / Context

Persistent memory is distinct from session transcript history. The client interface operates on sessions, turns, items, configuration, approvals, and user-visible data controls. Persistent memory is generated and maintained by the core agent runtime for future context construction.

The TUI, desktop client, IDE client, and other client surfaces do not need a persistent-memory management protocol. They should not list memory entries, subscribe to memory changes, or expose individual memory deletion/export controls unless a later requirement explicitly promotes memory management to a user-facing feature.

## User / Business Requirement

The program may maintain persistent memory internally, but users are not required to manage persistent memory directly.

## Real User Scenarios

- A user continues work across sessions and benefits from agent-retained context without managing memory records.
- A user deletes or archives a session through normal session controls without needing to resolve individual memory entries.
- A client renders sessions and turns without knowing whether the core created, updated, or used persistent memory internally.

## Functional Requirements

- Persistent memory, where supported, must be generated and maintained by the core agent runtime.
- Persistent memory must not be part of the routine client-server protocol surface.
- Clients must not be required to list, inspect, edit, delete, export, or subscribe to individual persistent memory entries.
- Persistent memory may retain internal source provenance for debugging, safety, privacy, or context-quality purposes.
- Session deletion may cause the core to update, unlink, retain, or remove internal memory according to internal memory policy, but ordinary clients are not required to present per-memory decisions.
- Persistent memory used for model context must pass through the same safety, privacy, and context-construction controls as other model-visible context.
- If persistent memory is disabled or unavailable, normal session, turn, and client behavior must continue to work.

## Non-Functional Requirements

- Persistent memory behavior must remain deterministic enough for debugging and replay where it affects model-visible context.
- Persistent memory must not expose plaintext secrets into model context, logs, telemetry, or routine client projections.
- Persistent memory implementation details must not leak into ordinary session and transcript UI.

## Acceptance Criteria

- Given persistent memory is supported, when the core derives memory from session activity, then no client-side memory management action is required.
- Given a client connects to the server, when it negotiates protocol capabilities, then it does not need persistent-memory list, delete, export, or change-notification methods.
- Given a session is deleted, when the core updates any internal memory linked to that session, then ordinary session deletion can complete without requiring the user to manage individual memory entries.
- Given persistent memory contributes to future model context, when context is assembled, then the memory is treated as core-maintained context rather than as a transcript item or client-managed record.
- Given persistent memory is disabled, when the user uses sessions and turns, then client behavior remains unchanged except for the absence of memory-derived context.

## Out of Scope

- This requirement does not define memory extraction, ranking, retrieval, summarization, storage, compaction, or model-context insertion algorithms.
- This requirement does not define a user-facing memory browser, editor, export flow, deletion flow, or notification stream.
- This requirement does not require persistent memory to be enabled by default.
- This requirement does not guarantee perfect provenance for every internal memory entry.

## Open Questions

- Should a future privacy or diagnostics mode expose internal persistent memory to advanced users?
- Should persistent memory have a global enable/disable setting, or be controlled only by agent mode and core policy?
- How long should internal persistent memory be retained by default?

## Traceability

| Relationship | Target ID | Target Revision | Target Path | Rationale |
|---|---|---:|---|---|
| related-to | L1-REQ-APP-012 | 1 | specs/L1/L1-REQ-APP-012-privacy-data-ownership.md | Persistent memory remains user data when model-visible, but is not a routine client-managed resource. |
| related-to | L2-DES-CONV-001 | 1 | specs/L2/conv/L2-DES-CONV-001-session-jsonl-data-model.md | L2 describes internal memory provenance links in durable session records where needed. |

## Revision Notes

| Revision | Date | Author | Change Type | Notes |
|---:|---|---|---|---|
| 1 | 2026-05-22 | Assistant | Initial | Initial persistent memory ownership requirement. |
| 1 | 2026-05-22 | Human | Refinement | Reframed persistent memory as core-maintained internal state rather than a client-managed protocol feature. |
