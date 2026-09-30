# Automation memory

An automation run has two separate memory contexts:

- **General Persistent Memory** is the server's bounded advisory User and Project
  snapshot. Automations consume the same root-turn preparation, entry/token caps,
  immutable tool-loop snapshot, Native recall item, and failure fallback as
  interactive sessions. It is read-only for automation runs.
- **Automation Run Memory** is the automation's private
  `automations/<id>/memory.md` under the desktop config directory. The desktop
  includes its contents as quoted advisory input, separately labeled from General
  Persistent Memory. The automation can continue updating its private file.

Both contexts are subordinate to current user and project instructions, system
and safety policy, and current repository evidence. Private content is JSON quoted
and delimiter escaped before inclusion in a user input part; it is not sent as a
system instruction.

## Recall configuration

Automation `execution.memoryRecall` accepts `on`, `off`, or `inherit` (the default
for existing configurations). `inherit` resolves the server's memory recall
setting. Globally disabled memory always wins, including when the automation asks
for `on`. No extra recall query is made by the desktop executor.

The executor persists recall and contribution through Native
`session/metadata/update` with a partial settings patch before sending the first
prompt. It sets contribution to `off`, but server source eligibility also rejects
an automation if a later caller requests contribution `on`. Recall preparation
failures omit General Persistent Memory context without failing the run or
removing its private context.

## Durable identity and one-way access

Native `session/new` accepts optional `source: "automation"`. Omitted source
means `interactive`, preserving existing clients' behavior. Native session
snapshots expose the automation source. Source is creation identity; it is not a
mutable `SessionSettingsPatch` field. The server records it as an internal
`SessionSettings` field line named `sessionSource` before registering the actor.
Replay, whole-record metadata refreshes, and forks preserve automation identity.
The existing ACP wire surface is unchanged.

`SessionMemorySource.source` carries that durable source to the memory module's
`enqueue_source` admission seam. Automation transcripts are rejected regardless
of the contribution preference. Background extraction itself remains a later
implementation step.

The server rejects automation-bound `memory/remember` and `memory/forget` Native
commands and agent mutation tools. Read/search and bounded recall remain available.
The executor never imports the private file as a memory entry, evidence,
candidate, or extraction job. The private file and General Persistent Memory
SQLite database retain independent namespaces and lifecycles.

Connections may select both interactive and automation sessions. Project mutations
use the existing workspace identity and active-session priority, then check the
chosen source. An idle automation selector does not block an active interactive
session. Equally preferred mixed sources fail closed unless a verified current
user item binds the command to one session. Exact-ID forget checks the entry's
actual scope before applying this source gate.
