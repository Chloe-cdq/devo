# Scoped memory export and reset

Native clients can inspect or clear one explicitly selected scope with
`memory/export` and `memory/reset`. Both requests require
`{ "scope": "user" }` or `{ "scope": "project" }`; an omitted, null, or
unknown scope is invalid. Project identity is resolved from the connection's
Native Session selectors using the same rules as `memory/list`. Conflicting
Project selectors fail before any state changes. The global memory feature
must be enabled for either command.

## Export

`memory/export` returns `scope`, `markdown`, and `lifecycle`. The Markdown is a
portable UTF-8 document containing all safe entries in the selected scope,
including active, restored, stale, conflicted, and retired entries. It includes
kind, origin, timestamps, replacement lineage, and safe provenance identifiers.
Conflicting inferred claims are shown separately. Export is not paginated.

`lifecycle` contains `ignoreSourcesBefore`, `lastRebuildAt`, and
`revocationCount`. Lifecycle values are also included in the Markdown when
present. Export reads canonical SQLite state rather than importing or trusting
manual edits to `MEMORY.md`. It omits credential-bearing entries and claims,
raw extraction responses, candidate details, transcripts, journal paths, and
provider internals. While source cleanup is pending, export returns a redacted
storage-unavailable error until reconciliation completes.

## Reset

Clients should display a confirmation identifying the scope before dispatching
`memory/reset`. Dispatch is the explicit reset command; the server does not
introduce an additional interactive confirmation or an implicit default scope.
Reset requires a session-bound interactive connection. User reset rejects
ambiguous Native Session selectors; Project reset resolves one canonical
Project identity. Automation sessions cannot authorize a reset. The global
feature gate is evaluated before caller validation. The response contains
`scope`, `clearedEntryCount`, `clearedCandidateCount`, and `ignoreSourcesBefore`.

One SQLite transaction removes all entries in the selected scope, their lexical
index and evidence, short-lived candidates, proposal claims and source bindings,
and revocations. It advances that scope's `ignore_sources_before` watermark.
Other scopes and shared source jobs/receipts are preserved. Resetting an empty
scope still writes a watermark; repeated resets cannot lower it if the clock
moves backward. Explicit future remember commands can add entries normally.

Every passive extraction commit checks the timestamp of its cited evidence
against the scope watermark inside its transaction. Evidence at or before the
watermark cannot recreate entries, candidates, or proposal claims, including
when the extraction task started before reset or the server restarted. A later
journal timestamp does not make old cited evidence eligible. New eligible
sources after reset contribute normally. Deliberate rescan of older history is
reserved for the future `memory/rebuild` operation.

`MEMORY.md` is atomically regenerated after the reset transaction commits. If
projection replacement fails, the reset remains committed and Native returns a
redacted storage error. Startup regenerates projections from SQLite, including
empty scopes with reset watermarks, without restoring cleared knowledge.
