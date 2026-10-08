# Memory test concurrency investigation (#50)

This investigation concerns [issue #50](https://github.com/Chloe-cdq/devo/issues/50).
The measured revision is `9859bf020f43bc6ce7a13fc5fd916520574b1474`, rather than
historical baseline `458467c8`. Results below do not identify an introducing
commit or prove that every historical failure had the same cause.

## Reproduction environment

- Date: 2026-10-08; recorded timestamps use Asia/Shanghai (+08:00).
- Windows, `x86_64-pc-windows-msvc`; 22 logical processors, approximately 32 GiB RAM.
- `rustc 1.97.1 (8bab26f4f 2026-07-14)` and Cargo 1.97.1.
- `RUST_MIN_STACK=16777216`, matching `.cargo/config.toml`. Direct executable
  invocations explicitly preserved this setting in the launching shell.
- Other Cargo builds were active. Build-lock waits are excluded from the test
  durations below; ambient compiler and filesystem load was not controlled.

Absolute paths below describe this measured checkout and cache; replace them
with your local workspace and cache paths when repeating the commands.

The first command was:

```powershell
cargo test --target-dir C:\Users\58253\code\devo\target -p devo-server --lib memory -- --test-threads=1
```

Its test executable was copied before any source edits. All baseline comparisons
used that same executable, with SHA-256:

```text
6B861075E2B578F4D61061E7AF2616BE0CF1C66F77946427CD8F6B797BE82237
```

Each invocation selected `memory` and passed the thread count explicitly.

| Threads | Repetitions | Harness durations (seconds) | Failed runs |
| --- | --- | --- | --- |
| 1 | 2 | 107.83, 101.48 | 0 |
| 22 | 5 | 15.48, 14.41, 35.08, 20.59, 19.23 | 1 |
| 64 | 3 | 16.39, 19.82, 21.74 | 1 |

Each run selected 252 tests. The failing 22-thread run passed 250 and failed:

- `runtime::memory_scan_tests::deleting_processed_source_removes_its_memory`:
  expected one entry, observed zero.
- `runtime::memory_scan_tests::scan_requires_known_quota_at_threshold`:
  expected one extractor call, observed zero.

The failing 64-thread run passed 250 and failed:

- `runtime::memory_scan_tests::deleting_source_during_extraction_prevents_late_commit`:
  `Error: deadline has elapsed`.
- `runtime::memory_scan_tests::source_delete_retries_after_projection_write_failure`:
  `Error: 系统找不到指定的路径。 (os error 3)`.

These comparisons establish intermittent failures at a fixed revision. Passing
serially, or passing some runs with more threads, does not establish concurrency
safety or a resource-exhaustion threshold.

## Scan fixture startup recovery

A diagnostic-only assertion added `MemoryCommand::Status` to the pre-delete
entry-count failure. Its executable SHA-256 was:

```text
08EE2BD85A6419216602CB2DCC1C087ECDC44AEB123CA0A79AA29AA51F363D47
```

The third focused `runtime::memory_scan_tests` run with 22 threads failed in
3.48 seconds: 19 passed, 3 failed. It reproduced the zero-entry failure together
with `missing_source_does_not_abort_other_sources` (zero extractor calls) and
`source_delete_survives_memory_storage_error` (indexing an empty entry list).
The pre-delete assertion captured:

```text
storage_health: "healthy"
entry_count: 0
pending_job_count: 0
error_job_count: 0
last_successful_scan_at: None
error_classes: []
source_exclusion_reasons: [SourceFenced]
```

`ServerRuntime::with_protocols` attaches the rollout store, setting
`source_recovery_pending`, then launches recovery on a background thread.
`source_has_intent` deliberately refuses sources until recovery finishes.
The production `enqueue_source(Scan)` path awaits reconciliation before scanning.
The test fixture instead returned after indexing, and its `scan` helper called
`run_background_scan` directly. Consequently it could scan before startup
recovery completed. Removing a fixture journal before recovery finished could
also make recovery's file-open step fail and leave the initial fence pending.

The fixture now explicitly reconciles source intents after indexing and before
returning. This preserves production's fail-closed recovery behavior and makes
fixture journal mutations happen after initial recovery. The existing
`startup_source_recovery_gates_inference_until_canonical_facts_are_replayed`
test deterministically checks the pending-recovery gate. The affected scan tests
exercise extraction and deletion through the corrected fixture. No production
code or timeout values change.

## Direct forget completion deadline

Both historical forget cases passed the ten baseline invocations. A subsequent
22-thread diagnostic run reproduced `direct_first_blocks_confirmation`:
251 passed, 1 failed, in 21.98 seconds, with:

```text
Error: timed out waiting for turn/completed for 01a1190b-5ecc-7e82-9cac-d55ca52e4131
Caused by:
    deadline has elapsed
```

The unannotated failure does not identify which completion wait timed out.
Separately, the direct test starts `run_turn`, including its five-second
completion wait, before deliberately blocking the forget mutation. That deadline
therefore includes the blocked interval and the competing confirmation turn.
A controlled regression arms the old completion wait using a oneshot barrier,
holds the mutation, and advances Tokio time by six seconds without a real-time
sleep. The old ordering failed in 2.45 seconds, after the competing confirmation
assertion succeeded:

```text
Error: direct forget completion
Caused by:
    0: timed out waiting for turn/completed for 01a11925-b2a0-74b2-99dd-78bc8f1b082b
    1: deadline has elapsed
```

This RED executable had SHA-256
`F6F975CF41186C7018657C92B1BBF501C08EA4557E7B5F48671E16048EB94B79`.
The test selected `memory_forget_concurrency::direct_first_blocks_confirmation`
with `--exact --nocapture`, and failed 0 passed / 1 failed / 724 filtered out.

The correction starts the direct turn, checks the competing
confirmation while the mutation is blocked, releases the mutation, and only then
waits for direct-turn completion. Buffered notifications allow that wait to begin
after release. The completion deadline remains five seconds.

## Verification and remaining scope

The controlled GREEN used the same exact test selector and passed 1/1 in
2.37 seconds. Formatting checks for both changed Rust files and `git diff
--check` passed. The fixed server-library executable has SHA-256:

```text
E6D11BD32FBD065B6704143C7D4B18A921B7DAB2435C3BCF28866C0FCB53032C
```

All post-fix repetitions used this immutable executable, with the launching
shell preserving `RUST_MIN_STACK`. Commands were:

```powershell
$env:RUST_MIN_STACK = '16777216'
& $binary memory --test-threads=1
& $binary memory --test-threads=22
& $binary memory --test-threads=64
& $binary runtime::memory_scan_tests --test-threads=22
```

Here `$binary` was the saved fixed executable. The baseline repetitions used
the corresponding saved baseline executable with the same `memory` selector
and explicit thread counts. Workspace compilation remained active during the
post-fix runs; these are bounded correctness checks, not performance benchmarks.

| Selector | Threads | Repetitions | Harness durations (seconds) | Failed runs |
| --- | --- | --- | --- | --- |
| `memory` | 1 | 1 | 140.40 | 0 |
| `memory` | 22 | 3 | 24.11, 26.96, 20.98 | 0 |
| `memory` | 64 | 3 | 22.35, 22.67, 24.36 | 1 |
| `runtime::memory_scan_tests` | 22 | 5 | 4.96, 3.71, 3.69, 3.83, 3.66 | 0 |

Each memory run selected 252 tests; each focused scan run selected 22. All seven
historical issue tests passed in every post-fix memory run. The first 64-thread
run nevertheless failed a different, unchanged test, passing 251/252:

```text
memory::source_intent_tests::deletion_intent_does_not_wait_for_blocked_memory_commit
panicked at crates\server\src\memory\source_intent_tests.rs:501:5:
commit did not reach the reader lock
```

That test probes for a blocked SQLite commit with a two-second real-time setup
deadline before checking deletion intent. Three exact, single-thread runs against
each baseline and fixed executable all passed (baseline: 0.52, 0.54, 0.47 seconds;
fixed: 0.45, 0.41, 0.43 seconds). These limited comparisons do not establish its
failure cause or justify a claim that the entire memory suite is free of flakes.
No timeout or implementation change was made for this additional failure.

The complete server library executable from the workspace build also passed
724 tests, with 0 failures and 1 ignored, using 22 threads in 26.95 seconds.
That launch set `NO_PROXY=127.0.0.1,localhost,::1` to bypass proxies for loopback.

The initial full workspace command built in 24 minutes 36 seconds, then
encountered local HTTP failures and stalled loopback fixtures:

- `tools::handlers::websearch::tests::tavily_search_request_matches_api_shape`:
  `ExecutionFailed("Tavily search error (502 Bad Gateway): ")`; the core library
  finished 643 passed, 1 failed, 21 ignored in 8.36 seconds.
- `provider_stream_logs_omit_recall_and_response_bodies`: marked `FAILED` in
  `memory_compaction`; its final failure dump was not emitted while other tests
  remained running.
- `provider_completion_error_logs_omit_recall_and_response_bodies` and
  `provider_stream_error_logs_omit_recall_and_response_bodies`: reported running
  for over 60 seconds and remained pending.

Relaunching the identical binaries with launch-only
`NO_PROXY=127.0.0.1,localhost,::1` made the Tavily case pass in 0.02 seconds and
all five compaction tests pass in 0.05 seconds. This supports loopback proxy
interference in the validation environment; the responsible proxy configuration
was not identified. The network helper preserves the default reqwest builder
when neither a configured nor an environment proxy URL is supplied, while the
privacy error tests await their local
server after receiving an expected provider error. A misrouted request can
therefore leave them waiting for a connection that never arrives.

These HTTP observations are separate from the memory synchronization findings.
Proxy code remains unchanged; environment flags were supplied by the launching shell.
The original Rust commands were left running, following the repository's
instruction not to interrupt them.

The loopback-bypass workspace run completed with exit 101: 3930 passed,
4 failed, 78 ignored across 146 reported harness results, including doctests.
Its four failures were stdio startup timeouts:

| Target | Test | Error |
| --- | --- | --- |
| `acp_initialization_e2e` | `stdio_acp_auth_gates_acp_methods` | timed out waiting for server auth initialize response |
| `acp_initialization_e2e` | `stdio_acp_initialize_negotiates_capabilities_and_allows_session_setup` | timed out waiting for ACP initialize response |
| `acp_session_contract_e2e` | `stdio_acp_session_config_options_select_model_binding` | timed out waiting for ACP initialize response |
| `acp_session_contract_e2e` | `stdio_proxy_acp_prompt_streams_each_agent_chunk_once` | timed out waiting for real server initialize response |

The initialization target failed 0/2 in 203.29 seconds; the contract target
passed 1/3 in 248.92 seconds. The launching command supplied `--target-dir`
but omitted the runtime binary-path variables used by those fixtures, so they
fell back to a nested Cargo build in the empty default worktree target.
This was an investigation-launch configuration omission, separate from the
memory fixes. After that CLI build finished, setting `CARGO_BIN_EXE_devo`
and `CARGO_TARGET_DIR` made the identical initialization and contract binaries
pass 2/2 in 0.54 seconds and 3/3 in 2.65 seconds respectively.

The Windows sandbox library completed 146 passed, 0 failed, 3 ignored in
309.41 seconds. Its legacy process tests share a serial lock and can spend
substantial time in restricted-token/private-desktop setup; they were allowed
to finish normally.

Full workspace verification with all launching settings supplied completed
successfully: exit 0, 3934 passed, 0 failed, 78 ignored across 146 reported
harness results, including doctests. It started at 10:35:48 +08:00 and took
627.54 seconds including Cargo startup. The server library passed 724 tests,
with 1 ignored, in 19.50 seconds; the Windows sandbox library passed 146 tests,
with 3 ignored, in 245.94 seconds. The command was:

```powershell
$env:NO_PROXY = '127.0.0.1,localhost,::1'
$env:CARGO_TARGET_DIR = 'C:\Users\58253\code\devo\target\issue-50-verified'
$env:CARGO_BIN_EXE_devo = 'C:\Users\58253\.codex\worktrees\issue-50\devo\target\debug\devo.exe'
cargo test --offline --workspace --no-fail-fast --jobs 2 -- --test-threads=22
```

The CLI path points to the binary built from this checkout by the preceding
nested build. These are launch-shell settings, not changes to test process
environment handling or production networking code.

Initial workspace command:

```powershell
cargo test --offline --workspace --no-fail-fast --jobs 2 --target-dir C:\Users\58253\code\devo\target\issue-50-isolated -- --test-threads=22
```

This cache reused dependency artifacts; local workspace packages were cleaned
before building so the measured executable came from the isolated checkout.

The historical durability timeout
`memory_forget_durability::durable_commit_with_projection_failure_invalidates_search_and_confirmation`
has not been reproduced in this investigation. Its completion wait already starts
after the intentionally blocked search is released. No production race, lost
notification, or exclusively environmental explanation has been established for
that case; it should not be described as fixed by the fixture changes.

Recorded command outputs, per-run exit codes, timestamps, diagnostic patch, and
summary CSVs are preserved in `.issue-50-evidence/` in the investigation worktree.
The report includes the relevant failure output so its conclusions remain
reviewable without those local logs.
