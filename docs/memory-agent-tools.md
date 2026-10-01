# Agent memory tools

Root turns receive `memory_search`, `memory_read`, `memory_remember`, and
`memory_forget`. Automatic recall settings do not disable these explicit tools;
the global memory feature gate still applies.

`memory_search(query, scope?, kind?, state?)` defaults to User scope and
Active/Restored entries. Queries must contain 1–1024 characters after trimming.
Results contain at most 20 summaries, each limited to 240 characters plus a
truncation marker, with stable IDs, scopes, kinds, and states.

`memory_read(entry_id)` returns one User or current-Project entry with its ID,
scope, kind, state, bounded body (4000 characters plus a truncation marker), and
a provenance summary. Other projects and unknown IDs share the same unavailable
result. Neither read surface exposes raw evidence, transcript text, storage
paths, or secret-bearing stored content.
While source deletion or external-context cleanup is pending, reads hide
inferred entries and omit source counts for explicit entries.

Remember and forget use the existing canonical Memory commands. A root mutation
tool invocation asserts explicit current-user intent; the server verifies the
active turn, current user-item binding, root session, scope, and provenance.
This is structural authorization, not natural-language classification. Claims,
delegated task messages, and stale user items cannot supply independent write
authority. Forget retains the stable-ID/current-user binding and later-turn
candidate confirmation workflow described in L2-DES-MEM-001 revision 4.

Subagents inherit the parent's prepared advisory snapshot independently of
`fork_turns`. Store changes and recall settings updates after preparation do not
alter that snapshot. Follow-up child turns retain it; a child never prepares a
fresh recall. Message edits, rollback, and manual compaction preserve the loaded
child's delegation identity and inherited snapshot when rebuilding history.
Delegation during root recall preparation waits for the snapshot.
Concurrent Native task starts and later turn publication share one preparation
lane, and aborted admission releases waiting delegations with an error.
Prepared-empty snapshots, including manual compaction, do not wait.
Restarted approval continuations restore and publish that turn's persisted
snapshot before replaying tools, including when the original snapshot was empty.
Memory tools and aliases are hidden and rejected at execution,
and the server coordinator rejects independent child read/search requests.
Delegated sessions, including ephemeral sessions, cannot update recall or
contribution settings. Passive source enqueue remains a server-owned seam with
no agent tool or Native command; source admission follows the eligibility rules
in [Passive memory contribution](memory-passive-extraction.md).
