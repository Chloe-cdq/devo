# Session deletion and persistent memory

Native `session/delete` accepts an optional `relatedMemory` policy:

```json
{"sessionId":"<session-id>","relatedMemory":"forget"}
```

Omitting the policy, or selecting `preserve`, preserves explicit memory. Deleting
any source removes its candidates, extraction job details and receipts, proposal
support, and evidence in one Memory-owned SQLite transaction. Inferred entries
retire only when their final evidence is removed and leave lexical recall.
Evidence from other sessions remains intact.

Selecting `forget` also revokes every identity with evidence from the deleted
session tree, including identities supported by other sessions. It uses the
same durable revocation mutation and global deletion lease as `memory/forget`.
Unrelated memory remains intact. Passive extraction cannot resurrect a revoked
identity; a later explicit remember request from another source follows the existing
restore rules. Deleted sources and pending deletion intents reject explicit
writes, including requests admitted before session removal.
The policy is available on Native only; ACP deletion retains the default policy.

The session runtime submits source cleanup through Memory's `enqueue_source`
Interface and waits for its transaction result. Blocking storage work runs outside the session actor; projection repair is
scheduled separately after canonical completion. Ordinary deletion retains a durable cleanup
intent if Memory is unavailable or busy and continues deleting the session.
Related-memory deletion fails before removing the session if canonical cleanup
cannot commit, so callers can retry their explicitly selected policy.
If rollout or session-index removal fails after canonical cleanup, Memory keeps
only the affected entry IDs associated with that source. This association survives
restart, so retrying with `forget` still uses the normal revocation path even after
evidence removal. Identity merging transfers these associations to the retained
entry. Reconciliation releases these retry IDs once the session index confirms
deletion; it does not retain the deleted evidence or conversation text.

Projection refresh is derived work. Its failure does not undo canonical cleanup
or fail session deletion. Memory retains the affected projection scopes for a
later reconciliation or startup rebuild. Completed canonical cleanup releases
the source-deletion fence even when projection repair remains pending, allowing
inferred memory with surviving evidence to remain available.

Deletion retains ownership if its initiating request is cancelled. The owned
operation publishes the session-deleted event to other connected clients.
