# Explicit scoped memory rebuild

Native `memory/rebuild` accepts `{ "scope": "user" }` or
`{ "scope": "project" }`. Omitted, null, and unknown scopes are invalid.
Clients should confirm the selected scope and explain that retained eligible
conversation history, including history before the most recent reset, will be
sent through the configured memory extractor before dispatch. Dispatch is the
explicit authorization; the server does not add a confirmation round trip.

Authorization requires enabled memory and a bound interactive session. User
scope requires an unambiguous Native Session selection. Project scope resolves
one canonical Project identity from the current connection context. Automation
cannot authorize either scope. Acceptance returns `scope`, `rebuildId`, and
`requestedAt` after committing the authorization to SQLite; transcript reads
and provider requests run in the background.

Each scope has one durable authorization per reset epoch. Concurrent calls and
retries return the same acceptance, including its original timestamp. Another
reset cancels outstanding rebuild jobs and advances the epoch even when the
clock has not advanced. A cancelled worker cannot dispatch or commit against
that old authorization. Rebuild leaves `ignoreSourcesBefore` unchanged and
records `lastRebuildAt` at acceptance.

Only retained sessions created by acceptance time are considered. Current
journal and index facts are checked again: sources must be persistent,
interactive, root, unforked, idle, contribution-enabled, and free of external
context or pending source cleanup. Project sources must resolve to the selected
Project identity, including a fresh check immediately before dispatch. The
usual recent-history window is relaxed for this explicit
operation; the idle threshold and every other eligibility rule still apply.
Source fences, redaction, output validation, conservative identity,
revocations, explicit authority, and inferred conflicts use the existing
extraction and admission path. Reset preserves revocation tombstones so rebuild
cannot revive forgotten identities. Proven legacy inferred aliases are
canonicalized before reset removes their entry bodies; only explicit restoration permits later
eligible evidence under the existing restoration rules.

Jobs use the authorization ID and current source watermark as their durable
identity. A source watermark is committed once; minimal receipts continue to
prevent replay after raw job details expire. Source changes cancel superseded
queued jobs. Leases prevent concurrent workers from admitting the same job.
Queueing and completion update request state atomically with pending jobs, so
a late worker cannot hide recoverable work behind a completed request.
Validated v10 full-journal scan receipts are copied to the stable accounting-free
identity before admission, preventing upgrade replay. Expired leases allow
restart recovery without allowing the former owner to
commit. Queueing due to unknown or insufficient quota spends no attempts.
Provider retries retain the existing bounded backoff and three-attempt limit.
One pass obeys `max_sources_per_scan`; background passes continue while eligible
work can progress. Foreground session work does not wait for extraction.
Persisted, verified MemoryExtraction accounting records without a turn do not change extraction
watermarks or idle age, including when the retained source is the trigger.
Foreground usage and all semantic journal changes continue to affect eligibility.

Reset, startup, migration, and ordinary scans never create rebuild authorization.
A later scheduled scan may resume an already authorized request after restart,
quota recovery, or an idle source becoming available. Restart alone does not start an extractor. Reissuing the scoped command can also resume the same
request. An explicit rebuild invocation does not append an ordinary scan.

`memory/status.rebuild` appears after the first accepted rebuild and contains
`pendingRequestCount`, `pendingJobCount`, `runningJobCount`, `retryingJobCount`,
`completedJobCount`, and `errorJobCount`. Existing `errorClasses` reports safe
job failure classes. These aggregates contain no transcript, candidate content,
source paths, or raw provider errors. `memory/export` includes lifecycle metadata.
Schema version 11 introduces durable rebuild requests and job authorization links.
