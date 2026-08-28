---
status: accepted
---

# Use SQLite as the authority for General Persistent Memory

General Persistent Memory needs atomic deduplication, provenance, conflict states, durable revocation, reset replay protection, and deterministic retrieval. Devo will therefore keep canonical memory state in a dedicated local SQLite database and generate read-only per-scope `MEMORY.md` projections for inspection; it will not use editable Markdown or a Git-backed memory workspace as the source of truth.

## Considered Options

- Editable Markdown is easy to inspect but makes lifecycle invariants and concurrent updates difficult to enforce.
- A Git-backed Markdown workspace adds audit history but duplicates privacy-sensitive content and makes deletion depend on Git history.
- SQLite plus generated projections preserves human visibility while keeping one transactional authority.

## Consequences

Memory writes, forgetting, reset, rebuild, and recall must go through the server-owned Memory module. Manual projection edits are overwritten, and export remains portable Markdown rather than a database-only workflow.
