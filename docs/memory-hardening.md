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
whitelists stored error classes. Foreground recall storage failures, errors
returned by background scanning, and Memory source-intent reconciliation
failures retain a fixed `storage_error` class and `degraded` health for the
runtime's lifetime, even when no background job exists. This includes retention
and maintenance projection failures before job creation, deleted-source cleanup,
and deferred projection repair. Later successful recall, scanning or repair does
not clear the observed failure. Ordinary foreground storage-mutex contention
does not record a health failure. Background failures remain isolated from
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
| Background maintenance and deleted-source repair failure visibility with usable recall | `runtime::memory_scan_tests::maintenance_tests` |
| Recall storage failure visibility, foreground survival and automation isolation | `memory_turn_recall`, `memory_automation` |
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

The preceding acceptance baseline passed on Windows and Unix on 2026-10-09 at
source commit `3a7bc4675b7ac13e4b7142b966ec2d2ffa0dda27`.
[Memory acceptance](https://github.com/Chloe-cdq/devo/actions/runs/37910872389)
passed on both platforms, and
[regular CI](https://github.com/Chloe-cdq/devo/actions/runs/37910872306)
passed full-workspace tests, all-target compilation, test traceability, Rustfmt
and documentation checks. The active L1 requirement (revision 2) and L2 design
(revision 4) are Implemented; historical revision 3 remains unchanged.

The latest maintenance correction was verified locally on Windows on 2026-10-10:
all 300 server Memory module/runtime tests, 9 recall integration tests and the
storage-failure privacy/isolation integration test passed. All three new
maintenance-status regressions first failed on healthy status and empty error
classes, then passed after the correction. They cover SQLite retention,
maintenance projection and deleted-source projection repair failures without
error jobs, successful foreground recall, and retention of safe failure status
after recovery. Rustfmt, test traceability and independent Standards/Spec
reviews passed. Windows/Unix acceptance and regular CI will be rerun on this
correction after it is pushed.

Two verification limitations remain under the user's accepted exclusions.
CI Clippy on Rust 1.99 reports `double_must_use` on the unchanged `async_trait`
surface at `crates/safety/src/lib.rs:646`. Compared with main
`045d17af7caffb5c50742a6b14119519c65ef0a1`, the safety source file and manifest,
the async-trait lockfile entry, and the Clippy toolchain/check command are
identical; no separate main Clippy run was launched. An earlier local
full-workspace run hung in a proxy-dependent fixture, and a rerun failed with
LNK1104 because the old process then held its test executable. Local proxy
issues are temporarily outside scope, and repository instructions prohibit
interrupting Rust commands. No complete local Windows workspace pass is claimed.
