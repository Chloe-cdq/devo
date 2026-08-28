---
artifact_id: L1-REQ-MEM-001
revision: 2
status: Approved
active_baseline: yes
supersedes: revision 1 draft
superseded_by:
owner: Human
last_updated: 2026-08-25
---

# L1-REQ-MEM-001 — General Persistent Memory

## Purpose

Define an optional, user-controlled memory mechanism that carries useful knowledge across independent sessions without confusing it with session history or automation run memory.

## Why This Matters

Users repeatedly state preferences, correct the agent, establish project facts, and point to durable references. Repeating that context in every new session is costly and error-prone. At the same time, learned context can become stale, conflict with current instructions, contain sensitive data, or outlive the session from which it was inferred. The product therefore needs useful recall together with clear provenance, bounded authority, inspection, and deletion controls.

## Background / Context

Devo has three distinct continuity mechanisms:

- **Session History** resumes or reviews one session identity.
- **Automation Run Memory** carries one automation's private `memory.md` across its runs.
- **General Persistent Memory** carries validated knowledge into later independent sessions.

General Persistent Memory is helpful context, not an authoritative instruction source. Required rules belong in project instruction files such as `AGENTS.md`; current user instructions, project instructions, safety policy, and system policy always outrank recalled memory.

The design combines Codex-style opt-in background learning and per-session controls with Claude-style inspectability and project isolation. It adds durable provenance, revocation, and deterministic conflict handling appropriate for Devo.

## User / Business Requirement

When explicitly enabled, Devo must remember useful user and project knowledge across independent sessions, recall only relevant bounded context, and let users understand, control, export, forget, reset, and deliberately rebuild that memory.

## Real User Scenarios

- A user asks Devo to remember a stable formatting preference and expects it to apply from the next turn in later projects.
- A user establishes a repository-specific build convention and expects worktrees from that repository to share it without leaking it into unrelated repositories.
- Devo infers a recurring correction from an eligible completed session and recalls it in a later session.
- A user sees which memories were recalled for a turn and inspects their origin.
- A user forgets an incorrect memory and expects old sessions not to recreate it.
- A user deletes a source session and expects inferred memory with no remaining evidence to retire.
- A session uses Web, MCP, or Tool Search; Devo does not passively learn from that session, while an explicit “remember this” request remains possible.

## Functional Requirements

- General Persistent Memory must be disabled by default.
- Once globally enabled, recall and contribution must be independently controllable. Each session must support `Inherit`, `On`, and `Off` for both controls.
- Memory scopes must be `User` and `Project`. Project memory must take precedence when both scopes contain relevant knowledge, and worktrees belonging to the same repository must share a project scope.
- Memory kinds must be `Preference`, `Feedback`, `Fact`, and `Reference`.
- An explicit user request to remember knowledge must be validated, redacted, deduplicated, and committed immediately; the result becomes eligible for recall on the next turn.
- Passive learning must run only in the background against eligible, idle, persistent root sessions. Subagent, ephemeral, and automation sessions must not contribute.
- If a session used Web, MCP, or Tool Search, the whole session must be excluded from passive learning. First release does not require candidate-level taint tracking.
- Automation Run Memory must remain isolated. An automation may consume General Persistent Memory, but its private memory must never contribute back to General Persistent Memory.
- Recalled memory must be advisory, bounded, and visibly distinct from instructions. It must not override current user instructions, project instructions, system policy, or safety policy.
- Recall must be prepared once per root turn and remain stable through that turn's model/tool loop. Subagents must inherit the parent's prepared read-only snapshot and must not independently recall, contribute, remember, or forget.
- Users must be able to inspect status and entries, view provenance, explicitly remember and forget, export, reset a scope, and deliberately rebuild from retained eligible history.
- Forgetting knowledge must create a durable revocation so previously processed source material cannot silently recreate it. A later explicit request may intentionally restore it.
- Deleting a session must remove its candidates and evidence. An inferred entry with no remaining evidence must retire; inferred entries with other evidence and explicit memories must remain unless the user also requests related-memory deletion.
- Resetting a scope must prevent sources older than the reset from automatically repopulating it. New eligible sessions may contribute afterward; rebuilding old history must require an explicit user action.
- Inferred memories must leave automatic recall after 90 days without use or verification while remaining inspectable. Explicit memories must not expire automatically.
- Memory recall must be visible as an expandable per-turn record identifying stable entry IDs and source summaries. Assistant responses are not required to cite recalled memory.
- Secret-bearing material must not be persisted in memory entries, projections, logs, or telemetry.
- Memory failure or unavailability must never prevent normal session or automation operation.

## Non-Functional Requirements

- The first release must use deterministic, explainable lexical retrieval rather than embeddings.
- Automatic recall must select no more than 12 entries and approximately 2,000 tokens per turn.
- Background extraction must be idempotent, quota-aware, retryable, and isolated from foreground response latency.
- Memory state and provenance must remain locally inspectable and exportable without requiring direct database access.
- The authoritative store must support deterministic conflict resolution, deletion, reset, replay protection, and session-evidence removal.
- Plaintext memory content must not appear in routine logs or telemetry.

## Acceptance Criteria

- Given memory is globally disabled, when a session runs, then it neither recalls nor passively contributes memory and all ordinary behavior remains available.
- Given recall is enabled and relevant active entries exist, when a root turn begins, then no more than 12 entries and approximately 2,000 tokens are supplied once as advisory context and a recall record is visible.
- Given the user explicitly asks to remember knowledge, when validation succeeds, then the entry is committed immediately and can be recalled from the next turn.
- Given an eligible session becomes idle, when a later root session starts a background scan, then at most one structured extraction pass processes that source idempotently without delaying the foreground session.
- Given a session used Web, MCP, or Tool Search, when passive extraction scans it, then the entire session is excluded.
- Given two inferred entries conflict for the same scoped subject, when recall runs, then neither conflicting claim is injected until resolved.
- Given a user forgets an entry, when an old source is scanned again, then the revoked knowledge is not recreated.
- Given a project has multiple Git worktrees, when sessions run in them, then they resolve to the same project memory scope.
- Given a source session is deleted and it was the final evidence for an inferred entry, when deletion completes, then the entry is retired while unrelated and explicit entries remain.
- Given memory extraction or storage fails, when the user continues working, then the foreground session remains usable and the failure is available through memory status without leaking content into logs.

## Out of Scope

- Cross-device or cloud synchronization.
- Organization-wide memory scope.
- Semantic embeddings or vector databases in the first release.
- Candidate-level trust propagation for external context.
- Automatic generation of skills or project instruction files from memory.
- Git-backed memory history or manually editable Markdown as a source of truth.
- Perfect secret detection, truth verification, or automatic resolution of inferred conflicts.
- Merging Session History, Automation Run Memory, and General Persistent Memory into one store or lifecycle.

## Open Questions

None for the first-release design.

## Traceability

| Relationship | Target ID | Target Revision | Target Path | Rationale |
|---|---|---:|---|---|
| refined-by | L2-DES-MEM-001 | 2 | specs/L2/memory/L2-DES-MEM-001-persistent-memory-architecture.md | Defines storage, extraction, retrieval, protocol, settings, lifecycle, and module boundaries. |
| related-to | L1-REQ-APP-012 | 1 | specs/L1/L1-REQ-APP-012-privacy-data-ownership.md | General Persistent Memory is locally stored user data with export and deletion controls. |
| related-to | L2-DES-CONV-001 | 1 | specs/L2/conv/L2-DES-CONV-001-session-jsonl-data-model.md | Session records supply provenance and source-eligibility facts without becoming the memory store. |
| related-to | L2-DES-CONV-002 | 1 | specs/L2/conv/L2-DES-CONV-002-two-plane-session-settings.md | Recall and contribution controls use the canonical session-settings path. |
| related-to | L2-DES-APP-008 | 1 | specs/L2/app/L2-DES-APP-008-protocol-unification.md | Memory management is added to Native only; external protocols remain adapters. |

## Revision Notes

| Revision | Date | Author | Change Type | Notes |
|---:|---|---|---|---|
| 1 | 2026-05-22 | Assistant | Initial | Initial internal persistent-memory ownership requirement. |
| 1 | 2026-05-22 | Human | Refinement | Kept memory outside routine client management. |
| 2 | 2026-08-25 | Human + Assistant | Replacement | Human-approved design interview replaced the internal-only requirement with opt-in, inspectable User/Project memory and explicit lifecycle controls. |
