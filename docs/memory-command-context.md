# Memory command context

Direct Native User `memory/remember`, `memory/forget`, and `memory/reset`
commands resolve the same session context. One Native Session selector supplies the source session
and workspace. Multiple distinct Native Session selectors are ambiguous. When
there are no Native Session selectors, the existing delivery subscription is
the fallback. A validated current-user-item binding takes precedence for a
root-agent action; direct selection does not replace that authorization check.
User selection and candidate workspaces derive from the same selector snapshot.
User reset requires an unambiguous interactive session after the global memory
gate; Project reset resolves one canonical Project identity and verifies its
interactive source. Export uses inspection context and requires no User session
binding. Both export and reset require an explicit scope. See
[memory-export-reset.md](memory-export-reset.md) for their wire and lifecycle
contracts.

The server passes `MemoryUserSessionSelection` (selected, unbound, or ambiguous)
to a forget request. Memory resolves the stored entry's actual scope before
using that selection: exact Entry IDs can identify a User entry even when the
wire request omits scope, and Project deletion resolves its Project from the
entry and candidate workspaces. An ambiguous User selection therefore does not
prevent an independently resolvable Project exact-ID deletion.

This is context data for `execute_command`, not an additional MemoryRuntime
operation. The three caller-facing operations remain `prepare_turn`,
`enqueue_source`, and `execute_command`; identity, storage, and revocation
remain owned by Memory.

Memory search queries and list text filters match literal substrings in entry
bodies and normalized keys. Percent signs, underscores, and exclamation marks
are ordinary text, not query syntax. Matching retains the existing ASCII
case-insensitive behavior. Search queries retain the existing trimming and
1-to-1024-character validation; absent or empty list text applies no text
restriction.

Mixed session settings patches compare permission targets with canonical
persisted settings before appending a permission field. Repairing a rejected
actor notification preserves the existing explicit sandbox without appending
a duplicate permission field. Permission and resolved sandbox reach the actor
in one mailbox command. A real persisted permission change clears the prior
sandbox override in both canonical history and runtime replay; an explicitly
supplied sandbox field wins, including when its value equals the prior value.
The same re-derivation applies when a fork already has the target live preset
but its canonical persisted preset changes.
