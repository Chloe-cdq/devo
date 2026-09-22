use std::path::PathBuf;

use chrono::{DateTime, Utc};
use devo_protocol::native::ids::{ItemId, MemoryEntryId};
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryForgetParams, MemoryForgetResult, MemoryKind, MemoryListResult,
    MemoryOrigin, MemoryScope, MemoryState, MemoryStatus,
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
    /// Retire one exact identity or return candidates for an ambiguous text match.
    Forget(MemoryForgetRequest),
    /// Return a filtered, paginated view of canonical memory entries.
    List(ListMemoryRequest),
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
    /// Validate, commit, and project an explicit Project memory request.
    Remember {
        text: String,
        kind: Option<MemoryKind>,
        source_user_item_id: Option<ItemId>,
        /// Active-turn source Session; direct commands fall back to the
        /// selected Project Session when absent.
        source_session_id: Option<SessionId>,
        source_turn_id: Option<TurnId>,
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
    /// Result of [`MemoryCommand::Forget`].
    Forget(MemoryForgetResult),
    /// Result of [`MemoryCommand::List`].
    List(MemoryListResult),
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
#[allow(dead_code)]
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

/// Input passed through the server-owned memory command seam for a forget
/// request. Exactly one selector is required: a stable entry ID or text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryForgetRequest {
    pub selector: MemoryForgetSelector,
    pub scope: MemoryScope,
    pub source: MemorySourceContext,
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

/// Input for turn preparation.
#[derive(Debug, Clone)]
pub struct PrepareMemoryRequest {
    pub workspace_root: PathBuf,
    /// Raw per-session recall preference. The runtime resolves `inherit` using
    /// its configured global default before preparing a snapshot.
    pub session_recall: MemorySetting,
}

/// Prepared memory context for a turn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreparedMemory {
    pub project_scope_id: Option<String>,
    pub user_entries: Vec<MemoryEntry>,
}

/// A completed session source eligible for future memory extraction.
#[derive(Debug, Clone, Default)]
pub struct SessionMemorySource {
    /// Raw per-session contribution preference read when a background scan
    /// evaluates this source session.
    pub session_contribution: MemorySetting,
}

/// Outcome of attempting to enqueue a session source.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnqueueOutcome {
    /// Whether this source was accepted for processing.
    pub accepted: bool,
}
