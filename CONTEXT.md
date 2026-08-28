# Devo Domain Language

This glossary defines the canonical language for Devo's agent and conversation domain. It keeps concepts with different scopes and lifecycles from being conflated.

## Memory and Continuity

**Memory Mechanism（记忆机制）**:
An umbrella term for capabilities that preserve context across conversation or execution boundaries. Always qualify it with one of the canonical terms below.
_Avoid_: Memory（记忆）when used without a scope

**Session History（会话历史）**:
The record associated with one session identity, used to continue or review that same session. A compacted representation remains session history rather than becoming long-term memory.
_Avoid_: Session memory, long-term memory

**Automation Run Memory（自动化运行记忆）**:
Information retained by one automation identity across its separate runs and isolated from other automations.
_Avoid_: Session history, global memory

**General Persistent Memory（通用长期记忆）**:
Agent-maintained knowledge that may be recalled in later independent sessions, such as stable preferences, project knowledge, and recurring decisions. It is knowledge derived from experience, not a transcript of one session.
_Avoid_: Session history, automation memory, transcript

**Memory Candidate（记忆候选）**:
Proposed knowledge awaiting validation before it becomes general persistent memory. A candidate may come from an explicit user request or from learning inferred after a session becomes eligible.
_Avoid_: Memory entry, saved memory

**Memory Entry（记忆条目）**:
Validated knowledge represented within general persistent memory. Every entry has one memory scope, one memory kind, provenance explaining why it exists, and a lifecycle state controlling whether it may be recalled.
_Avoid_: Memory candidate, transcript item

**Memory Scope（记忆作用域）**:
The audience within which a memory entry may be recalled. The canonical scopes are User and Project.
_Avoid_: Memory kind, session scope

**Memory Kind（记忆类型）**:
The semantic category of a memory entry, independent from who or which project may recall it. The canonical kinds are Preference, Feedback, Fact, and Reference.
_Avoid_: Memory scope

**Memory Provenance（记忆来源）**:
The durable explanation of how a memory candidate or entry came to exist. The canonical origins are Explicit User and Inferred Session.
_Avoid_: Confidence score, transcript

**Explicit Memory Request（显式记忆请求）**:
A user's direct request to remember or forget specific knowledge. It has higher authority than inferred session learning but remains subordinate to current instructions and enforced policy.
_Avoid_: Feedback, project instruction

**Memory Revocation（记忆撤销）**:
A durable decision that identified knowledge must no longer be recalled or regenerated from previously processed sources.
_Avoid_: Temporary disable, stale memory

**Stale Memory（陈旧记忆）**:
An inferred memory entry that has aged out of automatic recall without being deleted. It remains inspectable and may become active again after verification.
_Avoid_: Forgotten memory, deleted memory

**User Memory（用户记忆）**:
General persistent memory scoped to one user across projects, limited to preferences and facts that remain meaningful outside a particular repository.
_Avoid_: Global rule, organization policy

**Project Memory（项目记忆）**:
General persistent memory scoped to one project and shared by sessions and worktrees belonging to that project.
_Avoid_: Session history, checked-in project instruction

**Memory Contribution（记忆贡献）**:
Permission for a session to serve as source material for future memory candidates. It is independent from permission to recall existing memories in that session.
_Avoid_: Memory enabled

**Memory Recall（记忆召回）**:
Selection of relevant user and project memories for a task. Recalled memory is advisory context and cannot override current instructions, project instructions, or enforced policy.
_Avoid_: Instruction loading, policy enforcement

**Memory Projection（记忆投影）**:
A human-readable representation of memory state for inspection or export. It is derived from the authoritative memory store and is not edited as the source of truth.
_Avoid_: Memory store, memory database

**Memory Evidence（记忆证据）**:
The source session and turn references that support an inferred memory entry. Removing the final surviving evidence retires the inferred entry; explicit memory does not depend on retained session evidence.
_Avoid_: Transcript copy, confidence score

**Memory Conflict（记忆冲突）**:
Two incompatible inferred claims about the same scoped subject. Conflicted knowledge is retained for inspection but withheld from recall until it is resolved.
_Avoid_: Newer memory, duplicate memory

**Memory Reset（记忆重置）**:
A deliberate clearing of one memory scope that also prevents older source material from silently repopulating it. New eligible experience may contribute after the reset.
_Avoid_: Temporary disable, rebuild

**Memory Rebuild（记忆重建）**:
A deliberate reprocessing of retained eligible history to reconstruct memory after reset, repair, or migration.
_Avoid_: Automatic recall, reset

**Memory Recall Event（记忆召回事件）**:
A user-visible record that identifies which memory entries were supplied to a turn and where they came from, without requiring the assistant's final answer to cite them.
_Avoid_: Model citation, debug log

**Memory Source Eligibility（记忆来源资格）**:
Whether a completed session may contribute inferred knowledge. Eligibility excludes sessions whose origin or context makes passive learning unsafe or misleading, while leaving explicit memory requests independent.
_Avoid_: Memory enabled, trust score

**Memory Entry State（记忆条目状态）**:
The lifecycle condition of a memory entry. The canonical states are Active, Stale, Conflicted, and Retired; revocation is a separate durable decision rather than an entry state.
_Avoid_: Job status, confidence level
