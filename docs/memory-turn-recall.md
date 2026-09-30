# Automatic memory recall

Before the first model request of a root turn, the server prepares one lexical snapshot using the current request and the workspace name. Session `memory_recall` is resolved against the global memory gate and default. Mid-turn setting and cwd changes affect the next turn.

Only Active or explicitly Restored entries from User Memory and the resolved Project Memory scope are eligible. An outstanding revocation excludes an entry even if its stored lifecycle state is inconsistent. Retrieval searches all distinct lexical terms through bounded SQLite FTS batches, deduplicates matching entries, and scores relevance against the complete term set. Common English function words are ignored. Relevance ranks first, Project scope breaks relevance ties, explicit origin and greater evidence count break remaining ties, then newer updates and ascending stable entry IDs provide deterministic ordering.

The hard limits are 12 entries and approximately 2,000 tokens, including the advisory framing. Configuration can lower either limit. The token estimate uses UTF-8 bytes divided by four, rounded up. Each entry contributes a bounded content summary; entries that do not fit are skipped so smaller relevant entries can still fit.

The model receives a separate user-role `<advisory_memory>` block. The block labels every entry as quoted data and says current user instructions, project instructions, system and safety policy, and current repository evidence take precedence. Memory content is JSON-escaped so it cannot terminate the block or forge its structure. The system policy and repository instruction prefix are unaffected.

Native history exposes one `memoryRecall` item per prepared root turn with `snapshotRevision` and ordered `entries`. Each entry contains `entryId`, `scope`, `kind`, `summary`, and `sourceSummary`. Clients can collapse it to “Recalled N memories” and expand its entries. Source summaries expose the origin and evidence count, never source transcript text, paths, or opaque source identifiers. A snapshot revision hashes the bounded summaries and ordering. Recall lookup faults yield a persisted empty snapshot and do not fail the foreground turn. If the snapshot item cannot be persisted, the turn omits recall context and does not announce a completed recall item.

The same immutable block is included in every model/tool iteration, including automatic and context-limit compaction requests, compaction retries, and task continuations. Compaction receives the advisory block separately from the conversation history it summarizes. Approval and failure recovery reuse the persisted snapshot of the original turn rather than querying updated memory. Recall items are display history and do not enter subsequent turns as ordinary model history. Forgetting affects future snapshots and preserves historical recall records.

Subagents do not independently prepare memory. Parent-snapshot inheritance and the remaining memory management UI are separate implementation slices.

## Failure diagnostics

Provider failures may quote recalled memory or conversation text. Error message,
request-detail, model-name, and finish-reason fields therefore use
`SensitiveErrorText`: `Display`, `Debug`, and serialization redact their values.
SDK completion errors, stream creation errors, and stream items are normalized
before leaving the provider boundary. Known HTTP failures use the status code
for classification (with context-limit handling for invalid-context responses),
so quoted body text cannot impersonate authentication or server status codes. The router and core query boundary also
normalize third-party SDK failures. Normalization discards unsafe source/context
formatting, retains a safe structured cause, and captures diagnostic category and
recovery guidance before isolating private text. Repeated normalization preserves
the category; typed HTTP, I/O, and JSON/transport decode failures take precedence
before the legacy SDK compatibility fallback. SSE numeric error codes and known
error types also outrank echoed message text. Retry and compaction decisions do
not parse redacted formatting. Wrapping preserves structured recovery flags and
retry-delay metadata.

Native error and retry notifications explicitly use `user_message()` or
`user_message_for_error()`. Compaction failures use the same private-text type
and expose details only in user notifications. These projections must never be
used in diagnostic logs. Native RPC error responses log only their code,
request ID, and message byte count; they never log the projected user message.
CLI failures print details explicitly and retain the safe typed error on return.
Safe serialization is deliberately lossy and is not a
persistence format for private provider details; Native user-visible history
retains its existing error payload. No additional provider-response copies are
written to diagnostic storage.

Regression tests cover all typed error representations, unsafe SDK sources and
contexts, retry classification, Native root-turn failures, lazy HTTP/SSE errors,
compaction, and recovery. The existing workspace-test CI job includes these tests.
