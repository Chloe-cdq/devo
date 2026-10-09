# General Persistent Memory acceptance

General Persistent Memory is owned by the server Memory module. SQLite is
canonical; generated Markdown is an inspectable projection. Native exposes
`memory/status`, `memory/remember`, `memory/forget`, `memory/list`,
`memory/export`, `memory/reset`, and `memory/rebuild`. Root-agent search/read
tools use the same module. ACP remains a transport/projection adapter under the
unchanged `protocol-lock.json`.

The global gate defaults to Off. Deliberately enable it in the Devo configuration:

```toml
[memory]
enabled = true
```

Recall and contribution inherit independent On defaults after opt-in. Either can
be disabled globally or with `memoryRecall` / `memoryContribution` in canonical
`session/metadata/update`. Recall changes apply next turn; contribution changes
apply at the next source scan. See [configuration.md](configuration.md),
[memory-turn-recall.md](memory-turn-recall.md), and
[memory-passive-extraction.md](memory-passive-extraction.md).

Raw candidate detail and completed job detail expire inclusively at 30 days by
default. Maintenance runs on startup and background scans. Completed jobs become
minimal receipts before their detail is deleted, in the same transaction.
Receipts retain replay protection and scan timestamps. Canonical entries,
evidence, revocations, durable claim relations and unfinished jobs are preserved.
Inferred ageing is a separate lifecycle rule; an ageing projection failure does
not prevent expired detail from being pruned. See [memory-lifecycle.md](memory-lifecycle.md).

Routine memory diagnostics carry fixed error classes and safe IDs/counts.
Database messages, invalid stored values, filesystem paths, provider error
bodies, extraction output and conversation content are omitted. Status also
whitelists stored error classes. Background failures remain isolated from
interactive and automation turns; foreground recall falls back to an empty
snapshot when memory storage is unavailable or held by background work.
Synchronous recall work runs on the blocking pool so filesystem work does not
block Tokio workers. Filesystem and external SQLite access retain their existing
individual-operation latency; there is no new recall deadline. Management commands still report
safe errors rather than claiming a failed write succeeded.

| Acceptance area | Verification |
|---|---|
| Retention boundary, rollback and preserved authority | `memory::lifecycle_tests::scan_receipt_tests` |
| Initialization, status and background storage log privacy; root/automation survival | `memory_hardening::storage_failures_are_content_free_and_isolated_from_foreground` |
| Provider failures, credentials, quota and asynchronous scanning | `runtime::memory_scan_tests`, `memory_passive_extraction` |
| Malformed output and exhausted retries | `memory::extraction::tests`, `memory::jobs::tests` |
| Background mutex contention and continued recall | `busy_storage_does_not_queue_foreground_recall`, `memory_contention_keeps_turns_and_ping_available` |
| Recall storage failure and automation isolation | `memory_turn_recall`, `memory_automation` |
| Recovered explicit intent and approval source binding | `memory_explicit_recovery`, `memory_turn_recovery` |
| Native method schemas and retry/capability metadata | `memory_contract::every_native_memory_method_matches_pinned_contract` |
| Recall item and replayable completion event | `memory_recall` in `devo-protocol` |
| Platform-native identity and atomic projection replacement | `memory::identity::tests`, `memory_identity_compatibility`, `memory_foundation` |
| ACP compatibility | Existing ACP protocol/server tests; unchanged adapter sources and protocol lock |

The `Memory acceptance` workflow runs the Memory module, every memory integration
test, Native/ACP protocol tests, and core/config memory tests on Windows and Unix.
Windows runs independent test cases serially to isolate filesystem load;
concurrency scenarios still run their own tasks, threads and barriers.
The regular workflow continues to run the full workspace suite, format and lint
checks. The obsolete core extraction/consolidation skeleton has no remaining
callers and is removed; the core keeps only query context and tool contracts.

Approved specifications must stay Approved until the corresponding acceptance
verification succeeds on both platforms. Record outstanding environmental or
test failures instead of treating a partial run as closeout.
