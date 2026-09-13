//! Server-owned General Persistent Memory runtime.
//!
//! This module deliberately exposes one high-level command surface. SQLite
//! tables are an implementation detail and are never returned to callers.

mod entries;
mod forget;
mod forget_execution;
mod identity;
mod projection;
mod queries;
#[cfg(test)]
mod runtime_test_support;
mod schema;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use chrono::DateTime;
use chrono::Utc;
use devo_core::MemoryConfig;
use devo_protocol::SessionId;
use devo_protocol::TurnId;
use devo_protocol::native::ids::ItemId;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_protocol::native::rpc_memory::MemoryForgetParams;
use devo_protocol::native::rpc_memory::MemoryForgetResult;
use devo_protocol::native::rpc_memory::MemoryKind;
use devo_protocol::native::rpc_memory::MemoryListResult;
use devo_protocol::native::rpc_memory::MemoryOrigin;
use devo_protocol::native::rpc_memory::MemoryScope;
use devo_protocol::native::rpc_memory::MemoryState;
use devo_protocol::native::rpc_memory::MemoryStatus;
use devo_protocol::native::session::MemorySetting;
use rusqlite::Connection;
use thiserror::Error;

pub use forget_execution::MemoryForgetExecutor;
pub(crate) use forget_execution::RuntimeMemoryForgetExecutor;

const MEMORY_DATABASE_FILENAME: &str = "memory.sqlite3";
const MEMORY_SCHEMA_VERSION: &str = "3";
const USER_SCOPE_ID: &str = "user";
const DEFAULT_LIST_LIMIT: u32 = 50;
const MAX_LIST_LIMIT: u32 = 100;

/// Per-session memory behavior kept by the server runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionMemorySettings {
    pub(crate) recall: MemorySetting,
    pub(crate) contribution: MemorySetting,
}

impl Default for SessionMemorySettings {
    fn default() -> Self {
        Self {
            recall: MemorySetting::Inherit,
            contribution: MemorySetting::Inherit,
        }
    }
}

/// A session memory setting snapshot returned by the actor for metadata updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionMemorySettingsSnapshot {
    pub(crate) settings: SessionMemorySettings,
    pub(crate) version: u64,
}

/// Errors raised by memory initialization or command execution.
#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("failed to prepare memory directory: {0}")]
    Directory(#[from] std::io::Error),
    #[error("memory database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("memory database lock was poisoned")]
    LockPoisoned,
    #[error("memory database returned an invalid count: {0}")]
    InvalidCount(i64),
    #[error("memory database returned an invalid timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("failed to resolve memory project identity: {0}")]
    ProjectIdentity(String),
    #[error("memory is disabled")]
    Disabled,
    #[error("invalid memory request: {0}")]
    InvalidRequest(String),
    #[error("memory content was rejected because it may contain a secret")]
    SecretContentRejected,
    #[error("memory database contains an invalid value: {0}")]
    InvalidStoredValue(String),
}

/// Server-owned runtime for General Persistent Memory.
pub struct MemoryRuntime {
    config: MemoryConfig,
    memory_root: PathBuf,
    connection: Mutex<Connection>,
}

pub(super) fn scope_name(scope: MemoryScope) -> &'static str {
    match scope {
        MemoryScope::User => "user",
        MemoryScope::Project => "project",
    }
}

pub(super) fn kind_name(kind: MemoryKind) -> &'static str {
    match kind {
        MemoryKind::Preference => "preference",
        MemoryKind::Feedback => "feedback",
        MemoryKind::Fact => "fact",
        MemoryKind::Reference => "reference",
    }
}

pub(super) fn state_name(state: MemoryState) -> &'static str {
    match state {
        MemoryState::Active => "active",
        MemoryState::Stale => "stale",
        MemoryState::Conflicted => "conflicted",
        MemoryState::Retired => "retired",
        MemoryState::Restored => "restored",
    }
}

pub(super) fn origin_name(origin: MemoryOrigin) -> &'static str {
    match origin {
        MemoryOrigin::ExplicitUser => "explicit_user",
        MemoryOrigin::InferredSession => "inferred_session",
    }
}

impl MemoryRuntime {
    /// Opens or creates the dedicated memory database and applies all
    /// idempotent schema migrations.
    pub fn open(memory_root: PathBuf, config: MemoryConfig) -> Result<Self, MemoryError> {
        fs::create_dir_all(&memory_root)?;
        let connection = Connection::open(memory_root.join(MEMORY_DATABASE_FILENAME))?;
        schema::create_schema(&connection)?;
        Ok(Self {
            config,
            memory_root,
            connection: Mutex::new(connection),
        })
    }

    /// Prepares an immutable memory snapshot for a turn.
    pub async fn prepare_turn(
        &self,
        request: PrepareMemoryRequest,
    ) -> Result<PreparedMemory, MemoryError> {
        if self.config.resolve_recall(request.session_recall) != MemorySetting::On {
            return Ok(PreparedMemory::default());
        }
        let identity = identity::resolve_project_memory_identity(&request.workspace_root)
            .map_err(|error| MemoryError::ProjectIdentity(error.to_string()))?;
        let user_entries = self
            .list_recallable(ListMemoryRequest {
                scope: Some(MemoryScope::User),
                state: Some(MemoryState::Active),
                limit: Some(self.config.max_entries_per_turn),
                workspace_root: request.workspace_root.clone(),
                ..ListMemoryRequest::default()
            })?
            .data;
        Ok(PreparedMemory {
            project_scope_id: Some(identity.scope_id),
            user_entries,
        })
    }

    /// Accepts a session source for later extraction work. Disabled memory
    /// never queues a source.
    pub async fn enqueue_source(
        &self,
        source: SessionMemorySource,
    ) -> Result<EnqueueOutcome, MemoryError> {
        Ok(EnqueueOutcome {
            accepted: self
                .config
                .resolve_contribution(source.session_contribution)
                == MemorySetting::On,
        })
    }

    /// Resolves the canonical Project scope ID used to bind runtime session
    /// selectors to the memory projection.
    pub(crate) fn project_scope_id(
        &self,
        workspace_root: &std::path::Path,
    ) -> Result<String, MemoryError> {
        self.scope_id(MemoryScope::Project, workspace_root)
    }

    /// Executes one memory command through the public runtime seam.
    pub async fn execute_command(
        &self,
        command: MemoryCommand,
    ) -> Result<MemoryCommandResult, MemoryError> {
        match command {
            MemoryCommand::Status => Ok(MemoryCommandResult::Status(self.status()?)),
            MemoryCommand::Remember(request) => {
                if !self.config.enabled {
                    return Err(MemoryError::Disabled);
                }
                Ok(MemoryCommandResult::Remember(self.remember(request)?))
            }
            MemoryCommand::Forget(request) => {
                if !self.config.enabled {
                    return Err(MemoryError::Disabled);
                }
                Ok(MemoryCommandResult::Forget(self.forget(request)?))
            }
            MemoryCommand::List(request) => {
                if !self.config.enabled {
                    return Ok(MemoryCommandResult::List(Page {
                        data: Vec::new(),
                        next_cursor: None,
                    }));
                }
                Ok(MemoryCommandResult::List(self.list(request)?))
            }
        }
    }

    /// Records one observation from the server-owned passive extraction path.
    ///
    /// This remains crate-private so callers must first pass through the
    /// server's source admission and scheduling boundary rather than invoking
    /// inferred persistence as a public memory command.
    #[allow(dead_code)]
    pub(crate) fn record_inferred(
        &self,
        request: MemoryInferredRememberRequest,
    ) -> Result<Option<MemoryEntry>, MemoryError> {
        if !self.config.enabled {
            return Err(MemoryError::Disabled);
        }
        self.remember_inferred(request)
    }

    fn status(&self) -> Result<MemoryStatus, MemoryError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        Ok(MemoryStatus {
            enabled: self.config.enabled,
            storage_health: "healthy".into(),
            entry_count: count_rows(&connection, "SELECT COUNT(*) FROM memory_entries")?,
            candidate_count: count_rows(&connection, "SELECT COUNT(*) FROM memory_candidates")?,
            pending_job_count: count_rows(
                &connection,
                "SELECT COUNT(*) FROM memory_jobs WHERE state = 'pending'",
            )?,
            retrying_job_count: count_rows(
                &connection,
                "SELECT COUNT(*) FROM memory_jobs WHERE state = 'retrying'",
            )?,
            error_job_count: count_rows(
                &connection,
                "SELECT COUNT(*) FROM memory_jobs WHERE state = 'error'",
            )?,
            last_successful_scan_at: last_successful_scan_at(&connection)?,
            error_classes: error_classes(&connection)?,
        })
    }
}

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
}

/// Result returned by [`MemoryRuntime::execute_command`].
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

/// Input passed through the server-owned memory command seam for a forget
/// request. Exactly one selector is required: a stable entry ID or text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryForgetSelector {
    /// Select one entry by its stable identity.
    EntryId(devo_protocol::native::ids::MemoryEntryId),
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

fn count_rows(connection: &Connection, sql: &str) -> Result<u64, MemoryError> {
    let count: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    u64::try_from(count).map_err(|_| MemoryError::InvalidCount(count))
}

fn last_successful_scan_at(connection: &Connection) -> Result<Option<DateTime<Utc>>, MemoryError> {
    let timestamp = connection.query_row(
        "SELECT MAX(updated_at)
         FROM memory_jobs
         WHERE state = 'completed' AND job_kind = 'source_scan'",
        [],
        |row| row.get::<_, Option<String>>(0),
    )?;
    timestamp
        .map(|timestamp| {
            DateTime::parse_from_rfc3339(&timestamp)
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .map_err(|_| MemoryError::InvalidTimestamp(timestamp))
        })
        .transpose()
}

fn error_classes(connection: &Connection) -> Result<Vec<String>, MemoryError> {
    let mut statement = connection.prepare(
        "SELECT DISTINCT error_class
         FROM memory_jobs
         WHERE state = 'error' AND error_class IS NOT NULL
         ORDER BY error_class",
    )?;
    let classes = statement
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(MemoryError::from)?;
    Ok(classes
        .into_iter()
        .map(redact_error_class)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

fn redact_error_class(error_class: String) -> String {
    match error_class.as_str() {
        "credentials_unavailable"
        | "invalid_structured_output"
        | "permanent_provider_error"
        | "provider_unavailable"
        | "quota_unavailable"
        | "transient_provider_error" => error_class,
        _ => "unknown".to_string(),
    }
}
