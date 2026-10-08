# Memory conflicts and staleness

General Persistent Memory retains conservative textual claim identity within
User or Project scope. Equivalent inferred observations add evidence to the
existing canonical entry. Equivalent explicit writes retain its ID and earliest
creation time. Inferred evidence does not revise explicit content, state, or
revision timestamps.

Extraction proposal keys establish durable relationships between competing
claims; they do not replace canonical textual identity. Selecting a known claim
explicitly retires the other live entries in its established proposal groups.
Retired entries retain their body and provenance, and `replacement_entry_id`
points to the selected entry. Later explicit selections extend this lineage;
selecting a former entry clears its outgoing link to avoid a cycle. Different
scopes and unrelated claims remain independent. Explicit group authority survives
exclusion or deletion of inference sources. Established competitors retain their
group memberships so a later explicit selection still replaces the previous one;
unsupported memberships do not independently become live inference.

Incompatible inference leaves the accepted entry `Conflicted` and retains the
opposing candidate. `MEMORY.md` displays competing bodies during their retention
window and durable normalized claims afterwards. Pruning refreshes projections
to remove expired raw candidate detail. Automatic recall and default
`memory_search` exclude conflicts. Native listing, state-filtered search, and
reading an entry by ID remain inspection surfaces. Extraction cannot resolve an
outstanding conflict.

Inference becomes `Stale` after the configured interval (90 days by default)
since the later of last recall and accepted verification. Equivalent extraction
can verify stale inference or inference retired by source removal and reactivate it
after checking existing competition. Fresh evidence for a retired claim withholds
both claims when a competing inference is still live.
It cannot reactivate a retired replacement. Explicit entries never automatically
become stale. Conflict and retirement take precedence over ageing.

Maintenance runs on startup, background scans, and before memory reads or recall.
It removes stale entries from the lexical index and refreshes affected projections.
Automatic recall and successful on-demand `memory_search` or `memory_read`
record use of still-valid inferred entries with the same clock. Inspection of
Stale or Conflicted entries and Native listing do not extend their lifetime.
Recall checks age before recording its timestamp, so overdue entries cannot
renew themselves. A turn's already prepared snapshot remains immutable.

Schema version 9 transactionally refreshes proposal-authority views while retaining
entry IDs, keys, evidence, and replacement links. Lifecycle tests inject a clock
and cover the exact 90-day boundary, renewal, restart, and conflict resolution.

Schema version 10 retains the job kind in minimal receipts after completed job
details expire. `lastSuccessfulScanAt` counts completed source scans only, both
before and after pruning and restart; maintenance jobs never advance it.
Existing receipts without recoverable job kinds keep an unknown (`NULL`) kind.
They continue to prevent replay of their source watermarks but are excluded from
scan-time reporting. An upgraded database can therefore report no successful
scan time until a known source scan completes. Raw candidates and completed job
details still expire after the configured retention period (30 days by default).

Schema version 11 adds explicit scoped rebuild authorization and job links.
See [memory-rebuild.md](memory-rebuild.md) for replay, cancellation, and recovery.
