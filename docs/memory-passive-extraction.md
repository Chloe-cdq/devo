# Passive memory contribution

When General Persistent Memory is enabled, creating a persistent normal root
session schedules a background scan. Startup and foreground turns do not wait
for discovery, extraction, retries, or projection repair.

A scan admits at most `memory.max_sources_per_scan` sources (default 2).
Sources must be persistent root sessions, idle for at least
`memory.min_source_idle_hours` (default 6), and no older than
`memory.source_window_days` (default 30). The latest persisted
`memoryContribution` field controls admission. Subagents, automation sessions,
borrowed fork history, unfinished turns, damaged journals, and sessions that
used Web, MCP, or Tool Search are excluded. Journals larger than 1 MiB are
skipped to bound background input work.

Only persisted user text (including mid-turn steering corrections) and assistant
conversational text are sent to the extractor. Attachments, tool results, reasoning, approvals, hidden context, and
system/developer instructions are excluded. Credential-bearing sources and
candidates are
rejected before sending or committing, including explicitly assigned short
passwords and tokens; safe status never contains transcript or provider response
bodies.

The extractor selects `memory.extract_model`, then the configured small model,
then the catalog's fast auxiliary model. It performs one completion per job
attempt, with tools disabled, and accepts only bounded JSON candidates with
source turn references. It never writes memory directly. Each attempt is metered
in the triggering
session with Native usage purpose `memoryExtraction`, without transcript content.
Provider, model, and selected variant request/options and headers follow the
normal request precedence. Overlays cannot change the fixed extraction input,
output limit, disabled tools/thinking, or background privacy marker.

Known provider quota must meet
`memory.min_rate_limit_remaining_percent` (default 25). Missing or stale quota
telemetry skips extraction. Known local provider initialization failures are
recorded as terminal errors for eligible source jobs without a model call, even
when quota is unavailable. Missing credentials use `credentials_unavailable`;
other initialization failures use `permanent_provider_error`. Error details are
never exposed in memory status. Supported HTTP providers observe request and token
rate-limit headers; the lowest available percentage controls admission.

SQLite claims a session/watermark pair under an immediate transaction. Claims
have a two-minute lease and a unique owner. Concurrent processes cannot claim
the same live lease. Transient failures retry after 30 seconds and then 60
seconds, with three attempts total. Permanent and exhausted failures remain
visible through Native `memory/status` using content-free error classes.

Candidate validation, scoped identity, evidence, revocation/reset checks, FTS,
and job completion commit together. Equivalent claims add evidence; inferred
claims preserve explicit authority, and conflicting inferred claims stay out of
recall. Competing claims retain their source with the candidate; their evidence
is never added as support for the incumbent entry. Proposal groups persist
scoped conservative claim identities and stable entry-ID bindings independently
of candidate history. Equivalent explicit display changes keep those bindings;
an explicit resolution can select either retained claim. Entry merges redirect
bindings in the same transaction before removing duplicates. Candidate-history
pruning cannot remove live conflict authority. Model proposal keys group
competitors and never replace the approved textual entry identity.

Schema version 6 backfills uniquely provable proposal bindings from existing
candidates, including equivalent display changes. It leaves ambiguous identities
unbound, preserves their history, and withholds already-active inferred
competitors without merging their bodies or evidence. The migration is atomic
and idempotent; upgrading a v5 database does not rerun the v5 identity migration.

Markdown projections are atomically replaced after the authoritative
commit and repaired from SQLite on restart. Projection failure never repeats
the extraction call. Persisted source eligibility is rechecked before every
attempt and before commit. Missing or unreadable journals are isolated from other
sources in the scan.

Example opt-in configuration:

```toml
[memory]
enabled = true
extract_model = "provider/fast-model"
max_sources_per_scan = 2
min_rate_limit_remaining_percent = 25
```

Native `session/metadata/update` remains the only session settings write path.
Changes to contribution apply at the next source eligibility check; they do not
alter an active foreground turn.
