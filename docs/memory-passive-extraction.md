# Passive memory contribution

When General Persistent Memory is enabled, creating a persistent normal root
session schedules a background scan. Startup and foreground turns do not wait
for discovery, extraction, retries, or projection repair.
Creating an automation session does not schedule a scan.
The session runtime submits scan and cleanup work through the memory module's
source entry point; the memory module owns extraction and source reconciliation.

A scan admits at most `memory.max_sources_per_scan` sources (default 2).
Sources must be persistent root sessions, idle for at least
`memory.min_source_idle_hours` (default 6), and no older than
`memory.source_window_days` (default 30). The latest persisted
`memoryContribution` field controls admission. Subagents, automation sessions,
borrowed fork history, unfinished turns, damaged journals, and sessions that
used Web, MCP, or Tool Search are excluded. Journals larger than 1 MiB are
skipped to bound background input work.
Automation identity is checked in both session metadata and persisted
`SessionSource` field records. Malformed source values also exclude the session.

Actual external-tool use first resolves the complete ancestor chain and closes
memory admission for every source before publishing external content. Loaded
actor snapshots retain ephemeral ancestors; the index retains unloaded durable
ancestors. Traversal passes through ephemeral sessions to reach durable
ancestors, and missing identities or cycles remain errors. Wholly ephemeral
chains need no persistent exclusion.

The foreground records fsynced `ExternalContextUsed` facts in ordinary canonical
session history. Optional source-ledger, memory-database and projection writes
run in background reconciliation; failure of both exclusion databases does not
reject local Web, MCP, Tool Search or hosted Web. While exclusion is pending,
new extraction claims and commits are refused, inferred memory is withheld from
recall and on-demand results, and explicit memory remains available.
Reconciliation transfers the pending fence to the session ledger or the dedicated
memory database. A committed exclusion survives projection refresh failure.

Canonical marker retry ownership belongs to session persistence even when memory
cannot initialize. A failed marker is retried before the next ordinary append;
remaining ancestor markers are still attempted. When optional memory is absent,
a failed marker also schedules the available session-ledger fallback. Ordinary
session history remains the durable recovery authority when both optional
exclusion databases reject writes.

Startup withholds inferred memory and extraction until background recovery has
read canonical source facts and restored their exclusions. Recovery captures a
stable file prefix under the append lock and streams provenance outside that
lock, skipping large message and tool payloads. A known source with a crash tail
or unsupported history version is quarantined independently; if source identity
cannot be established, inference remains closed until recovery succeeds. The
next scan or reconciliation retries pending storage repair. Memory status reports
`degraded` and the content-free `source_provenance_storage` class for observed
storage failures during the runtime's lifetime. This isolation concerns optional
memory storage; canonical conversation persistence retains its ordinary durability
contract.
Subagent use also excludes its durable parent chain. Failed and interrupted turns do not
themselves taint a session; only completed turns contribute text. Merely
offering hosted Web capability does not exclude a text-only session. The source
reader also recognizes older tool records without this fact. For legacy
`functions.exec` wrappers, it admits a sequence of direct calls to known local
tools with literal arguments, immutable result bindings, and `text(...)` output
of those results (including property reads and literal `??` fallbacks). Tool
names inside a local command string do not count as tool use. Dynamic calls,
aliases, malformed or absent code, and other unrecognized
statements are excluded because their behavior cannot be established from the
journal.

Only persisted user text (including mid-turn steering corrections) and assistant
conversational text are sent to the extractor. Attachments, tool results, reasoning, approvals, hidden context, and
system/developer instructions are excluded. Credential-bearing sources and
candidates are
rejected before sending or committing, including explicitly assigned short
passwords and tokens. One private policy checks source admission, returned
candidates, explicit and inferred writes, and projections. Supported credential
labels include spaces, underscores, hyphens and case variations (for example,
`API key: ab`); label normalization never changes stored claim text or identity.
Safe status never contains transcript or provider response
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
An unavailable selected model records `provider_unavailable` on eligible source
jobs without sending source text to a fallback model. Model resolution uses the
runtime catalog, which includes built-in presets as well as user providers.

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

Deleting a source session first records durable deletion intent in the session
index, then removes its rollout and session metadata even if memory storage or
projection is unavailable. Cleanup is queued without waiting for the memory
database. Extraction checks that same durable intent before claiming a source
and before starting a commit. A transaction already in progress can finish
after intent is recorded; search, list, recall, and forget selectors withhold
inferred memory until idempotent cleanup fences the source and removes its
evidence. Public entry results also omit provenance while any source intent is
pending; explicit memory remains available. Markdown projections may lag this
fence until cleanup repairs them. The idempotent
memory transaction permanently fences the source, removes its candidates, job
details, evidence, and proposal-claim support, and recomputes conflicts from
surviving support. An inferred entry is retired when its last evidence disappears.
Explicit entries remain. Fresh, uncontested evidence from a different source
can reactivate an entry retired only by source deletion; forget revocations
still block inference. A stale scan or in-flight extraction cannot recreate
memory from a fenced source. Affected projection scopes are durably recorded
and rebuilt from SQLite, including on startup or the next scan after a
projection write failure. Source exclusion after external-context use retains
its evidence and claim rows for audit while removing their inference authority.

Expired candidates are pruned on startup and before a scan. Completed job
details older than `memory.candidate_and_job_retention_days` (default 30) are
replaced by a minimal source/watermark/completion-time receipt, so expiry does
not cause a processed source to be extracted again or erase the last successful
scan timestamp. Active and retryable jobs remain.

Candidate validation, scoped identity, evidence, revocation/reset checks, FTS,
and job completion commit together. Equivalent claims add evidence; inferred
claims preserve explicit authority, and conflicting inferred claims stay out of
recall. Competing claims retain their source with the candidate; their evidence
is never added as support for the incumbent entry. Proposal groups persist
scoped conservative claim identities and stable entry-ID bindings independently
of candidate history. Equivalent explicit display changes keep those bindings;
an explicit resolution can select either retained claim. Entry merges redirect
bindings in the same transaction before removing duplicates. Candidate-history
pruning cannot remove live conflict authority. One live-claim view defines
which retained source support has authority for admission, withholding, and
source-change reconciliation. Admission checks all retained
memberships of the scoped conservative claim before creating an inferred entry,
including opposing claims that have no entry binding. Renaming a model proposal
key cannot discard a known conflict or explicit authority. Explicit writes,
entry merges and migration reconcile those same memberships. Inference can add
supporting evidence but cannot reactivate an existing conflict. Model proposal
keys group competitors and never replace the approved textual entry identity.

Entry resolution verifies the actual body's conservative identity before merging
entries or attaching evidence; a matching storage key alone is insufficient.
When a preserved historical inferred key occupies a different claim's canonical
key, inference retains the new proposal as an unbound `identity_collision`
candidate without overwriting the historical entry or attaching evidence to it.
Known proposal-group conflicts still apply their shared withholding policy. An
explicit write of the new text fails without overwriting history. An authorized explicit write of the
original historical text can canonicalize its own identity, after which fresh
inference can admit the distinct claim independently. This does not automatically
rekey inferred history.

Schema version 6 backfills uniquely provable proposal bindings from existing
candidates, including equivalent display changes. It leaves ambiguous identities
unbound, preserves their history, and withholds all provably matching inferred
competitors without merging their bodies or evidence. Withholding compares actual
scoped body identities for bound and unbound memberships, so an ambiguous
duplicate cannot regain recall after a later membership is bound. The migration is atomic
and idempotent; upgrading a v5 database does not rerun the v5 identity migration.

Schema version 7 repairs databases already marked v6. It rebinds only uniquely
provable scoped identities from durable proposal claims, even when candidates
have been pruned, and withholds inferred entries that escaped a known conflict
under another label. Non-sensitive bodies, identities, timestamps and evidence
are retained; ambiguous identities remain unbound. A derived historical key that
also identifies a surviving safe entry does not erase its authority or tombstone. The repair also removes
credential-bearing memory content and its candidate, claim, evidence, revocation
and FTS copies, including content stored through explicit commands. Generated
Markdown is rebuilt without those values. Original conversation journals are
unchanged. Both repairs and the final version marker commit in one transaction;
a failed migration rolls back and can be retried. Reopening v7 does not rerun
historical identity migrations. Schema version 8 records per-source proposal
support so later source deletion or exclusion can remove only that source's
conflict authority. Existing claims whose complete supporter set cannot be
reconstructed after candidate pruning are marked legacy-unattributed and kept
conservative until an explicit resolution; migration does not guess a source.

These rules enforce already-known relationships and a finite credential grammar.
They do not infer semantic conflicts between previously unrelated wordings or
promise to recognize every possible secret described in natural language.

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
