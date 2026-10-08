use std::path::PathBuf;

#[cfg(test)]
use chrono::{DateTime, Utc};
use devo_protocol::native::ids::{ItemId, MemoryEntryId};
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryExportResult, MemoryForgetParams, MemoryForgetResult, MemoryKind,
    MemoryListResult, MemoryOrigin, MemoryReadEntry, MemoryRebuildResult, MemoryResetResult,
    MemoryScope, MemorySearchResult, MemoryState, MemoryStatus,
};
use devo_protocol::native::session::MemorySetting;
use devo_protocol::{SessionId, TurnId};

/// Commands supported by the memory runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryCommand {
    /// Return effective feature state and safe aggregate health counts.
    Status,
    /// Validate, commit, and project an explicit user memory request.
    Remember(MemoryRememberRequest),
    /// Resolve and validate every fallible prerequisite for a forget mutation.
    PrepareForget(MemoryForgetRequest),
    /// Retire one prepared identity or return candidates for an ambiguous text match.
    Forget(PreparedMemoryForgetRequest),
    /// Return a filtered, paginated view of canonical memory entries.
    List(ListMemoryRequest),
    /// Return bounded recall-eligible candidates for the root-agent search tool.
    Search(SearchMemoryRequest),
    /// Read bounded entry content and provenance within the caller's workspace.
    Read(ReadMemoryRequest),
    /// Export all safe canonical entries and lifecycle metadata in one scope.
    Export(ScopedMemoryRequest),
    /// Clear one scope and atomically advance its source exclusion watermark.
    Reset(ScopedMemoryRequest),
    /// Authorize a deliberate, scoped background rebuild from retained history.
    Rebuild {
        scope: MemoryScope,
        user_session: MemoryUserSessionSelection,
        sessions: Vec<ProjectMemorySession>,
    },
    /// Verify a direct Native User reset caller before clearing the User scope.
    ResetUser {
        user_session: MemoryUserSessionSelection,
        sessions: Vec<ProjectMemorySession>,
    },
    /// Resolve Native Session candidates to one canonical Project scope and
    /// execute the requested management operation within that scope.
    Project {
        candidates: Vec<ProjectMemorySession>,
        operation: ProjectMemoryOperation,
    },
}

/// Project-scoped management operations accepted by [`MemoryCommand::Project`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectMemoryOperation {
    /// Export the selected Project memory scope.
    Export,
    /// Reset the selected Project memory scope.
    Reset,
    /// Validate, commit, and project an explicit Project memory request.
    Remember {
        text: String,
        kind: Option<MemoryKind>,
        source: MemorySourceBinding,
    },
    /// Return a filtered, paginated Project memory view.
    List {
        kind: Option<MemoryKind>,
        state: Option<MemoryState>,
        origin: Option<MemoryOrigin>,
        text: Option<String>,
        cursor: Option<String>,
        limit: Option<u32>,
    },
}

/// Runtime-owned Native Session facts needed to resolve a Project scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMemorySession {
    pub session_id: SessionId,
    /// The Session summary workspace, or `None` when that summary is
    /// unavailable. Command execution applies the global gate before rejecting it.
    pub workspace_root: Option<PathBuf>,
    pub activity: ProjectMemorySessionActivity,
    /// Durable creation source; unavailable facts cannot authorize mutations.
    pub source: Option<devo_protocol::native::session::SessionSource>,
}

/// Whether a Project candidate owns an active turn or is only selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectMemorySessionActivity {
    Active,
    Inactive,
}

/// Result returned by [`super::MemoryRuntime::execute_command`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryCommandResult {
    /// Result of [`MemoryCommand::Status`].
    Status(MemoryStatus),
    /// Result of [`MemoryCommand::Remember`].
    Remember(MemoryEntry),
    /// Opaque authorization input produced before a forget lease is acquired.
    PreparedForget(PreparedMemoryForgetRequest),
    /// Result of [`MemoryCommand::Forget`].
    Forget(MemoryForgetResult),
    /// Result of [`MemoryCommand::List`].
    List(MemoryListResult),
    /// Result of [`MemoryCommand::Search`].
    Search(MemorySearchResult),
    /// Result of [`MemoryCommand::Read`].
    Read(MemoryReadEntry),
    /// Result of a scoped export command.
    Export(MemoryExportResult),
    /// Result of a committed scoped reset command.
    Reset(MemoryResetResult),
    /// Durable acceptance of a scoped background rebuild.
    Rebuild(MemoryRebuildResult),
}

/// Input passed through the server-owned memory command seam for an explicit
/// user request. The source identifiers are retained as provenance only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRememberRequest {
    pub text: String,
    pub scope: MemoryScope,
    pub kind: Option<MemoryKind>,
    pub source: MemorySourceContext,
}

/// Internal input for one server-owned passive extraction observation.
///
/// A revoked identity cannot be recreated by this path. The type is crate
/// private because inferred writes are not part of the public command seam.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemoryInferredRememberRequest {
    pub(crate) text: String,
    pub(crate) scope: MemoryScope,
    pub(crate) kind: Option<MemoryKind>,
    pub(crate) source: MemorySourceContext,
    pub(crate) source_observed_at: DateTime<Utc>,
    pub(crate) source_watermark: String,
}

/// Typed provenance and workspace context shared by memory mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySourceContext {
    pub user_item_id: Option<ItemId>,
    pub session_id: SessionId,
    pub turn_id: Option<TurnId>,
    pub workspace_root: PathBuf,
}

/// Session-bound provenance supplied before a Project workspace is resolved.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemorySourceBinding {
    pub user_item_id: Option<ItemId>,
    /// Active-turn source Session; direct commands fall back to the selected
    /// Project Session when absent.
    pub session_id: Option<SessionId>,
    pub turn_id: Option<TurnId>,
}

/// Selector accepted by a server-owned forget request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryForgetSelector {
    /// Select one entry by its stable identity.
    EntryId(MemoryEntryId),
    /// Select entries whose body or normalized identity contains this text.
    Text(String),
}

impl MemoryForgetSelector {
    pub(crate) fn from_params(params: &MemoryForgetParams) -> Result<Self, &'static str> {
        match (&params.entry_id, &params.text) {
            (Some(entry_id), None) => Ok(Self::EntryId(entry_id.clone())),
            (None, Some(text)) => Ok(Self::Text(text.clone())),
            (Some(_), Some(_)) => Err("memory forget accepts exactly one of entryId or text"),
            (None, None) => Err("memory forget requires entryId or text"),
        }
    }
}

/// Input for resolving every fallible prerequisite before a forget lease is acquired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryForgetRequest {
    pub selector: MemoryForgetSelector,
    pub scope: MemoryScope,
    pub source: MemoryForgetSource,
}

/// Server-resolved User context. Ambiguity is deferred until the target's
/// actual scope is known, so Project exact-ID deletion can resolve independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryUserSessionSelection {
    Selected(SessionId),
    Unbound,
    Ambiguous,
}

/// Server-verified Session facts used to bind a forget request to one scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryForgetSource {
    /// Session already bound to the current user item, when Agent provenance exists.
    pub bound_session_id: Option<SessionId>,
    /// Native Session used for User scope when no Agent binding exists.
    pub user_session: MemoryUserSessionSelection,
    /// Runtime-owned Session/workspace facts available for Project resolution.
    pub sessions: Vec<ProjectMemorySession>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreparedMemoryForgetScope {
    pub(super) scope: MemoryScope,
    pub(super) scope_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PreparedMemoryForgetTarget {
    Exact(MemoryEntryId),
    Text(String),
}

/// Opaque forget command whose scope identity has passed fallible validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedMemoryForgetRequest {
    pub(super) target: PreparedMemoryForgetTarget,
    pub(super) scope: PreparedMemoryForgetScope,
    pub(super) source_session_id: SessionId,
}

impl PreparedMemoryForgetRequest {
    pub(crate) fn scope(&self) -> MemoryScope {
        self.scope.scope
    }

    pub(crate) fn source_session_id(&self) -> SessionId {
        self.source_session_id
    }
}

/// Filter and paging input for a memory inspection command.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListMemoryRequest {
    pub scope: Option<MemoryScope>,
    pub kind: Option<MemoryKind>,
    pub state: Option<MemoryState>,
    pub origin: Option<MemoryOrigin>,
    pub text: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub workspace_root: PathBuf,
}

/// Runtime-owned search input. Eligibility, ordering, limits, and safe
/// projection are applied inside the memory deep module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchMemoryRequest {
    pub query: String,
    pub scope: MemoryScope,
    pub kind: Option<MemoryKind>,
    pub state: Option<MemoryState>,
    pub workspace_root: PathBuf,
}

/// Server-bound stable ID and current workspace for an on-demand read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadMemoryRequest {
    pub entry_id: MemoryEntryId,
    pub workspace_root: PathBuf,
}

/// Input for turn preparation.
#[derive(Debug, Clone)]
pub struct PrepareMemoryRequest {
    /// Current root-turn request used for lexical relevance.
    pub query: String,
    pub workspace_root: PathBuf,
    /// Raw per-session recall preference. The runtime resolves `inherit` using
    /// its configured global default before preparing a snapshot.
    pub session_recall: MemorySetting,
}

/// Prepared memory context for a turn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreparedMemory {
    pub project_scope_id: Option<String>,
    pub entries: Vec<devo_protocol::native::rpc_memory::MemoryRecallEntry>,
    pub snapshot_revision: String,
}

/// Explicit scope and server-resolved workspace for export and reset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedMemoryRequest {
    pub scope: MemoryScope,
    pub workspace_root: PathBuf,
}
