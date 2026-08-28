---
artifact_id: L2-DES-MEM-001
revision: 2
status: Approved
active_baseline: yes
supersedes: revision 1 draft
superseded_by:
owner: Human + Assistant
last_updated: 2026-08-25
---

# L2-DES-MEM-001 — General Persistent Memory Architecture

## Purpose

Define Devo's first-release General Persistent Memory architecture: a server-owned deep module that validates explicit memory requests, learns from eligible sessions in the background, recalls bounded advisory context, and provides transparent Native management without merging memory with session history or automation memory.

## Scope

This document defines:

- User and Project memory scopes and stable project identity
- Entry kinds, provenance, lifecycle, evidence, conflicts, and revocation
- Explicit and passive contribution pipelines
- Deterministic lexical recall and prompt placement
- SQLite authority and generated Markdown projections
- Native protocol methods, built-in tools, and recall visibility
- Session settings and their decision points
- Session deletion, reset, rebuild, retention, failure, and observability behavior
- Server module seam, rollout plan, and verification requirements

It replaces revision 1's Git-backed, two-model extraction/consolidation workspace. It does not implement the design.

## Current-State Audit

- `crates/core/src/memory.rs` contains types, path helpers, and traits, but no production `MemoryStore` adapter, server configuration, job runner, Native methods, tools, or recall integration. It is not a working general memory mechanism.
- Session JSONL persistence, resume, replay, and compaction implement Session History, not General Persistent Memory.
- Desktop automations maintain a separate per-automation `memory.md`; this is Automation Run Memory and remains separate.
- The production model loop is assembled through `crates/core/src/query/mod.rs`. A memory design wired only through unused context-pipeline helpers would not affect real turns.
- Native is the single retained protocol surface per L2-DES-APP-008. Memory behavior must not be implemented independently in legacy or ACP handlers.

## Design Decisions

### DD-1: Three continuity mechanisms remain distinct

Session History, Automation Run Memory, and General Persistent Memory have different identities, authority, lifecycle, and deletion rules. They do not share a store or write path.

Automation may opt into one-way consumption of General Persistent Memory. Its private `memory.md` is never evidence for passive learning and cannot be promoted automatically.

### DD-2: Feature opt-in; recall and contribution are independent

The subsystem is globally disabled by default. When globally enabled, recall and contribution have independent global defaults and independent per-session `Inherit | On | Off` values.

The resolved values are:

```
global enabled == false  -> recall Off, contribution Off
session Inherit          -> corresponding global default
session On / Off         -> explicit session value
```

An automation may consume only when recall resolves to `On`; it never contributes. Subagents inherit a prepared snapshot and have neither independent control.

### DD-3: User and Project scopes; Project wins relevance ties

`User` entries are useful across projects. `Project` entries are isolated to a project identity and may be more specific. Retrieval considers both scopes; Project entries receive a deterministic priority boost, but an irrelevant Project entry must not displace a strongly relevant User entry merely because of scope.

Project identity in the first release is the hash of the canonical Git common directory. All worktrees of one repository therefore share a scope. Outside Git, it is the hash of the canonical workspace root. Moving a repository or workspace creates a new namespace; scope migration is deferred.

### DD-4: SQLite is authoritative; Markdown is a projection

A dedicated local SQLite database is the canonical truth. It provides transactions, FTS lexical retrieval, evidence joins, revocation checks, reset watermarks, and idempotent jobs.

Each User or Project scope has one generated, read-only `MEMORY.md` projection for inspection and export. Projection generation occurs after a successful transaction using atomic replacement. Manual edits are overwritten and never imported. The memory directory is not Git-initialized.

This decision is recorded in `docs/adr/0001-sqlite-authority-for-general-persistent-memory.md`.

### DD-5: Flat typed entries with lightweight provenance

Every entry has one scope, one kind (`Preference | Feedback | Fact | Reference`), one normalized `memory_key`, one body, one origin (`ExplicitUser | InferredSession`), one lifecycle state (`Active | Stale | Conflicted | Retired`), timestamps, and zero or more evidence links.

Provenance records source session and turn IDs when available. The first release has no floating confidence value or trust graph. Explicit user origin has higher authority than inferred origin but remains advisory relative to current instructions and policy.

### DD-6: Explicit writes are immediate; passive learning is background

An explicit request is accepted only from a root agent acting on an identified current user message or from a direct Native command such as `/remember`. The module redacts, validates, normalizes, classifies, deduplicates, and commits it synchronously. It becomes available on the next turn; the active turn's prepared snapshot never changes.

Passive learning is triggered when a new normal root session starts. The background scanner selects prior sessions that are persistent, root, contribution-enabled, idle for at least six hours, within the source window, not already processed at the same source watermark, and free of external-context use. It runs one structured extraction call per source session, then deterministic redaction, validation, deduplication, conflict handling, commit, and FTS update. There is no global consolidation-model pass in the first release.

The extraction input contains persisted user and assistant conversational content needed to understand the outcome; system/developer instructions, raw tool results, attachments, hidden context, approvals, and credentials are excluded. The structured output proposes scope, kind, key, body, and supporting turn references; it cannot write the store directly.

### DD-7: External-context exclusion is session-wide and monotonic

The session record persists `external_context_used` once Web, MCP, or Tool Search is invoked. The value can change only from false to true. Any such session is ineligible for passive extraction. This deliberately avoids candidate-level taint propagation in the first release.

An explicit user request remains allowed because its source and intent are known; it still passes normal secret redaction and validation.

### DD-8: Conflict and duplicate resolution is deterministic

Uniqueness is evaluated by `(scope, memory_key)`:

1. Equivalent content adds evidence and refreshes timestamps without creating another entry.
2. Explicit content replaces inferred content for the same key.
3. Newer explicit content replaces older explicit content and preserves the replacement lineage.
4. Incompatible inferred content marks the canonical entry `Conflicted` and retains the competing claim as a candidate; the key remains inspectable but is excluded from recall.

An extractor never resolves inferred conflicts by itself. A later explicit request may resolve the key.

### DD-9: Revocation and reset prevent resurrection

Forgetting writes a tombstone keyed by scope and normalized memory identity before retiring the entry. Inferred evidence observed at or before the revocation time cannot recreate it. A later explicit user request may intentionally restore the same identity.

Reset clears one scope and advances `ignore_sources_before` for that scope. Automatic scans ignore older evidence after reset. New eligible sessions may contribute. `memory/rebuild` is the only operation allowed to deliberately rescan retained pre-reset history.

### DD-10: Recall is lexical, bounded, stable per turn, and advisory

At the start of each root turn, before the first model call, `prepare_turn` executes one SQLite FTS/lexical query using the current user request plus stable project/session metadata. Deterministic ranking combines lexical relevance, scope priority, entry state, origin, and recency. Only `Active` entries are automatically recalled.

The result is capped at 12 entries and approximately 2,000 tokens. It is rendered as a distinct advisory memory context block, never concatenated into system policy, project instructions, or `AGENTS.md`. The block explicitly states that current instructions and observed repository state take precedence.

The prepared snapshot is reused for every model call and tool loop iteration in that turn. A cwd or setting change is reflected by the next turn. Subagents receive the parent's read-only snapshot; they do not query the store independently. `memory_search` and `memory_read` allow the root agent to fetch additional entries on demand.

### DD-11: Recall is visible without mandatory citations

Each recalled snapshot produces one expandable, session-persisted `MemoryRecall` item/event: “Recalled N memories.” It includes stable entry IDs, scope, kind, concise content/source summaries, and snapshot revision. It does not expose raw transcripts or secret-bearing provenance. The item follows Session History retention: forgetting an entry prevents future recall but does not rewrite historical turns that already displayed it.

The assistant is not required to cite memory in its final response. The event exists for inspectability and diagnosis, not to elevate memory authority.

### DD-12: Native owns management; adapters remain pure

The Native protocol adds:

- `memory/status`
- `memory/list`
- `memory/remember`
- `memory/forget`
- `memory/export`
- `memory/reset`
- `memory/rebuild`

`memory/list` supports scope, kind, state, origin, text, and pagination filters and returns safe provenance summaries. Exact Entry ID deletion is immediate; text-based forgetting first returns matches, and multiple matches require user selection. Clients confirm reset and rebuild before issuing the command.

Recall and contribution toggles are fields of canonical `SessionSettingsPatch` on `session/metadata/update`; no per-concern settings method is introduced. No legacy or ACP memory implementation is added. An external protocol may later project canonical behavior without owning memory logic.

### DD-13: The server owns one deep Memory module

The implementation lives under `crates/server/src/memory/`. The external seam is one `MemoryRuntime` module with three operations:

```rust
impl MemoryRuntime {
    pub async fn prepare_turn(
        &self,
        request: PrepareMemoryRequest,
    ) -> Result<PreparedMemory, MemoryError>;

    pub async fn enqueue_source(
        &self,
        source: SessionMemorySource,
    ) -> Result<EnqueueOutcome, MemoryError>;

    pub async fn execute_command(
        &self,
        command: MemoryCommand,
    ) -> Result<MemoryCommandResult, MemoryError>;
}
```

`prepare_turn` hides identity resolution, eligibility, retrieval, ranking, budgeting, rendering, and recall-event construction. `enqueue_source` hides idempotency, quota checks, extraction, validation, and retry. `execute_command` hides all management transactions and projection refresh.

SQLite repository and model extractor seams are internal because each has a real adapter and a deterministic fake for module tests. Any traits introduced for these seams must document adapter behavior and invariants. Protocol handlers, session actors, query code, and clients must not call repository tables directly.

The shallow skeleton in `crates/core/src/memory.rs` is not extended as the subsystem seam. During implementation, reusable value types may move to an appropriate shared type module; obsolete traits and filesystem helpers are deleted after callers migrate.

## Runtime Flow

```text
Native/session runtime
        |
        +-- prepare_turn(query, settings, project) --------+
        |                                                  |
        |     resolve scopes -> FTS rank -> budget          |
        |                    -> PreparedMemory snapshot     |
        |                    -> MemoryRecall item           |
        |                                                  v
        |                                      core query/model loop
        |                                      (same snapshot all turn)
        |
        +-- enqueue_source(completed session) --------------+
        |                                                   |
        |     durable idempotent job -> eligibility          |
        |     -> one extraction call -> redact/validate      |
        |     -> dedup/conflict/revocation check -> commit   |
        |
        +-- execute_command(remember/forget/...) ------------+
              -> transaction -> FTS -> atomic projection
```

Foreground recall performs no model call. Background failures never fail a user turn.

## Storage Model

The database is stored in Devo's platform-specific application data directory, separate from rollout JSONL. Conceptual tables:

| Table | Purpose | Essential fields |
|---|---|---|
| `memory_entries` | Canonical memory state | `entry_id`, scope type/id, kind, normalized key, body, origin, state, timestamps, replacement ID |
| `memory_candidates` | Short-lived proposed and competing claims | candidate ID, proposed entry fields, source, validation outcome, retention time |
| `memory_evidence` | Source support | `entry_id`, `session_id`, `turn_id`, observed time, source watermark |
| `memory_revocations` | Resurrection prevention | scope, normalized identity, revoked time, optional restored time |
| `memory_jobs` | Idempotent background work | source session/watermark key, state, attempt count, lease, retry time, error class |
| `memory_scope_state` | Scope lifecycle | scope, projection revision, `ignore_sources_before`, last rebuild time |
| FTS virtual table | Lexical retrieval | entry ID, normalized key, body |

Raw candidates and completed job details are retained for 30 days. Session transcripts remain governed by Session History retention, not copied into the memory database.

The Markdown projection lives under the memory data root:

```text
memory/
├── memory.sqlite3
├── user/MEMORY.md
└── projects/<project-id>/MEMORY.md
```

Each projection groups entries by kind and includes safe ID, state, body, origin, timestamps, and source summary. It excludes raw transcript text, raw extraction output, tombstone internals, and secrets.

## Entry Lifecycle and Retention

```text
candidate -> Active -> Stale -> Active
                  \-> Conflicted -> Active
                  \-> Retired

forget: any state -> durable revocation + Retired
```

- Explicit entries do not expire automatically.
- Inferred entries become `Stale` after 90 days without recall or verification. Stale entries stay inspectable but are not injected.
- Deleting a source session removes pending candidates, completed source-job detail, and evidence links for that session. An inferred entry retires when its final evidence disappears. An explicit entry remains unless related-memory deletion was selected.
- A duplicate evidence observation updates the existing entry rather than creating a duplicate.
- Retired and revoked records remain only as long as required to enforce provenance, reset, and resurrection rules; user export distinguishes live entries from lifecycle metadata.

## Session Settings Contract

Canonical protocol values use an explicit enum rather than booleans:

```rust
pub enum MemorySetting {
    Inherit,
    On,
    Off,
}
```

`SessionSettingsPatch` gains `memory_recall` and `memory_contribution`. Both use persist-first field-level `InternalRecordV2::SessionSettings` lines; handler notification is best-effort, and replay prefers field lines.

| Field | Decision point | Mid-turn semantics |
|---|---|---|
| `memory_recall` | once at root-turn preparation, before the first model call | A change during an active turn takes effect next turn; the prepared snapshot does not change. |
| `memory_contribution` | when a source session is evaluated by the background scanner | The next eligibility scan reads the latest persisted value; it never changes the active turn. |

Both fields report `applied_to_active_turn = false`. This promise is also recorded in L2-DES-CONV-002 DD-6 before implementation.

## Built-in Agent Tools

Root agents may receive:

- `memory_search(query, scope?, kind?, state?)` — return bounded summaries and stable IDs.
- `memory_read(entry_id)` — return one safe entry and provenance summary.
- `memory_remember(text, scope?, kind?, source_user_item_id)` — mutate only when tied to explicit current-user intent.
- `memory_forget(entry_id, source_user_item_id)` — mutate only when tied to explicit current-user intent.

Ambiguous natural-language forget requests use search first. Subagents receive none of the mutation tools and do not independently receive read tools; the parent can delegate relevant context in the task message or inherited snapshot.

## Background Scheduling and Failure Policy

- Starting a normal root session schedules a non-blocking scan; it does not wait for extraction.
- Jobs are keyed by source session plus source watermark and claimed transactionally with a lease.
- A configurable fast auxiliary model produces one structured candidate set per source session.
- The scanner processes at most two sources per start by default and does not start below 25% remaining provider quota.
- Transient failures use bounded exponential backoff with at most three attempts. Invalid structured output, unavailable credentials, and permanent provider errors are recorded safely and do not block sessions.
- Status exposes counts, last successful scan, pending/retrying/error jobs, and redacted error classes. Logs and telemetry include IDs, counts, durations, and token usage, never entry bodies or transcript text.

## Configuration

```toml
[memory]
enabled = false
default_recall = "on"
default_contribution = "on"
min_source_idle_hours = 6
source_window_days = 30
inferred_stale_after_days = 90
candidate_and_job_retention_days = 30
max_sources_per_scan = 2
max_entries_per_turn = 12
max_prompt_tokens = 2000
min_rate_limit_remaining_percent = 25
extract_model = "model-slug" # optional; resolves to the configured fast auxiliary model
```

Invalid values fail configuration validation. Global disable dominates all other values. Runtime thresholds may be tuned later without changing the lifecycle contract.

## Privacy and Authority

- Memory is locally stored user data and is sent to a model provider only when used as model context or when an eligible source is explicitly processed by the configured extractor.
- Secret detection and redaction run before candidate commit and before projection. A rejected candidate is never indexed.
- Memory context is labeled as fallible prior knowledge. Current user statements, current repository contents, `AGENTS.md`, system/developer instructions, and enforced safety policy take precedence.
- Required project rules must not be learned only as memory; users and agents should place them in checked-in instruction files.
- External-context session exclusion is a conservative source policy, not a claim that all remaining content is trustworthy.

## Rollout Plan

1. **Foundation** — Native DTOs, `MemorySetting` fields, field-level persistence, SQLite schema/migrations, identity resolver, and disabled `MemoryRuntime` wiring.
2. **Explicit control** — remember/forget/list/status/export/reset, projection, revocation, and lifecycle tests.
3. **Recall** — FTS ranking, bounded advisory snapshot, query-loop integration, inherited subagent snapshot, and `MemoryRecall` item/event.
4. **Passive contribution** — eligibility facts, monotonic external-context marker, job runner, structured extraction, quota guard, and deletion hooks.
5. **Lifecycle closeout** — staleness, rebuild, maintenance, documentation, and removal of obsolete `devo-core` memory skeleton.

Every phase ships with the global feature disabled until its acceptance tests pass. No legacy or ACP parallel slice is created.

## Verification Strategy

Module and integration tests must cover:

- full-object round trips for entries, evidence, revocations, jobs, and scope state
- User/Project identity, including same-repository worktrees and non-Git roots on Windows and Unix
- deterministic ranking, Project tie priority, token/entry caps, and stable per-turn snapshots
- duplicate merging, explicit-over-inferred replacement, inferred conflict withholding, and explicit conflict resolution
- forgetting, old-source replay prevention, reset watermarks, deliberate rebuild, and idempotent job replay
- session deletion with final and non-final evidence and explicit-memory retention
- setting patch partial semantics, persist-first replay, and next-turn/next-scan decision points
- external-context monotonic marking and exclusion for Web, MCP, and Tool Search
- subagent and automation one-way boundaries
- secret fixtures, projection redaction, and absence of content in logs/telemetry
- foreground survival during database, projection, provider, malformed-output, quota, and retry failures
- Native protocol shapes and proof that ACP behavior is unchanged

Tests must not mutate process environment variables. Filesystem tests must use platform-native paths and compare whole objects with `pretty_assertions::assert_eq` where practical.

## Rejected Alternatives

- **Direct editable Markdown, Claude-style** — transparent but cannot reliably enforce transactions, evidence deletion, conflict states, revocation, or reset replay protection.
- **Git-backed memory workspace** — strong history but duplicates privacy-sensitive content, complicates deletion, and makes internal maintenance depend on another state machine.
- **Two-phase extraction plus global consolidation Agent** — potentially richer organization but materially increases model cost, concurrency, sandbox, and failure complexity before retrieval quality is measured.
- **Embeddings/vector database** — unnecessary for the first bounded local corpus and less explainable than FTS.
- **Server natural-language parser for “remember”** — brittle and duplicative; the root agent or explicit client command carries intent through the canonical command.
- **Direct `devo-core::MemoryStore` expansion** — puts server storage concerns at the wrong seam and leaves callers with a shallow implementation-shaped interface.

## Risks and Mitigations

- **Incorrect learned facts** — advisory authority, explicit provenance, conflicts withheld, staleness, and user forget/reset controls.
- **Secret persistence** — narrow extraction input, redaction before commit/projection, content-free logs, safety fixtures.
- **Quota consumption** — default off, background-only extraction, low source cap, quota threshold, bounded retries.
- **Project identity changes after moves** — documented new-namespace behavior; migration deferred rather than hidden heuristics.
- **Prompt inflation** — deterministic 12-entry and 2,000-token caps, plus on-demand read tools.
- **Projection/database divergence** — SQLite remains authoritative and projection is atomically regenerated.

## Traceability

| Relationship | Target ID | Target Revision | Target Path | Rationale |
|---|---|---:|---|---|
| refines | L1-REQ-MEM-001 | 2 | specs/L1/L1-REQ-MEM-001-persistent-memory.md | Implements the approved user-controlled General Persistent Memory requirement. |
| related-to | L1-REQ-APP-012 | 1 | specs/L1/L1-REQ-APP-012-privacy-data-ownership.md | Defines local storage, export, deletion, redaction, and telemetry boundaries. |
| related-to | L2-DES-CONV-001 | 1 | specs/L2/conv/L2-DES-CONV-001-session-jsonl-data-model.md | Session JSONL supplies eligibility facts and evidence references without storing memory entries. |
| related-to | L2-DES-CONV-002 | 1 | specs/L2/conv/L2-DES-CONV-002-two-plane-session-settings.md | Recall and contribution use canonical persist-first session settings and declared decision points. |
| related-to | L2-DES-APP-008 | 1 | specs/L2/app/L2-DES-APP-008-protocol-unification.md | Management methods and recall events land on Native only. |
| related-to | L2-DES-AGENT-003 | 1 | specs/L2/agent/L2-DES-AGENT-003-subagent-architecture.md | Subagents inherit a read-only parent snapshot and cannot mutate memory. |
| related-to | L2-DES-LLM-003 | 1 | specs/L2/llm/L2-DES-LLM-003-model-usage-observability.md | Background extraction usage is metered without logging memory content. |

## References

- [OpenAI Codex memories](https://learn.chatgpt.com/docs/customization/memories) — local host memory, background extraction, source exclusion, and independent controls.
- [Claude Code memory](https://code.claude.com/docs/en/memory) — project-scoped memory, inspectable Markdown, and the distinction between instructions and learned memory.

## Revision Notes

| Revision | Date | Author | Change Type | Notes |
|---:|---|---|---|---|
| 1 | 2026-05-27 | Assistant | Initial | Draft Git-backed two-phase extraction/consolidation architecture. |
| 2 | 2026-08-25 | Human + Assistant | Replacement | Human-approved design interview replaced revision 1 with a SQLite-authoritative, lightweight, Native-manageable User/Project architecture. |
