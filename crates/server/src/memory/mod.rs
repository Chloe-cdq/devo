//! Server-owned General Persistent Memory runtime.
//!
//! This module deliberately exposes one high-level command surface. SQLite
//! tables are an implementation detail and are never returned to callers.

mod command_execution;
mod command_types;
mod entries;
mod equivalence;
mod forget;
mod identity;
mod migration;
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
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_protocol::native::rpc_memory::MemoryForgetResult;
use devo_protocol::native::rpc_memory::MemoryKind;
use devo_protocol::native::rpc_memory::MemoryOrigin;
use devo_protocol::native::rpc_memory::MemoryScope;
use devo_protocol::native::rpc_memory::MemoryState;
use devo_protocol::native::rpc_memory::MemoryStatus;
use devo_protocol::native::session::MemorySetting;
use rusqlite::Connection;
use thiserror::Error;

pub use command_execution::MemoryCommandExecutor;
pub(crate) use command_execution::RuntimeMemoryCommandExecutor;
pub(crate) use command_types::MemoryInferredRememberRequest;
pub use command_types::{
    EnqueueOutcome, ListMemoryRequest, MemoryCommand, MemoryCommandResult, MemoryForgetRequest,
    MemoryForgetSelector, MemoryRememberRequest, MemorySourceContext, PrepareMemoryRequest,
    PreparedMemory, ProjectMemoryOperation, ProjectMemorySession, ProjectMemorySessionActivity,
    SessionMemorySource,
};

const MEMORY_DATABASE_FILENAME: &str = "memory.sqlite3";
const MEMORY_SCHEMA_VERSION: &str = "4";
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
    #[error("Project scope has ambiguous Native Session selectors")]
    AmbiguousProjectScope,
    #[error("Project scope requires a Native Session selector")]
    ProjectSessionRequired,
    #[error("Project scope requires a Session with a workspace root")]
    ProjectSessionUnavailable,
    #[error("memory is disabled")]
    Disabled,
    #[error("invalid memory request: {0}")]
    InvalidRequest(String),
    #[error("memory content was rejected because it may contain a secret")]
    SecretContentRejected,
    #[error("memory database contains an invalid value: {0}")]
    InvalidStoredValue(String),
    #[error("memory forget committed but projection refresh failed: {projection_error}")]
    ForgetCommitted {
        result: Box<MemoryForgetResult>,
        #[source]
        projection_error: Box<MemoryError>,
    },
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
        let runtime = Self {
            config,
            memory_root,
            connection: Mutex::new(connection),
        };
        runtime.rebuild_projections()?;
        Ok(runtime)
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
            MemoryCommand::Project {
                candidates,
                operation,
            } => {
                if !self.config.enabled {
                    return match operation {
                        ProjectMemoryOperation::Remember { .. } => Err(MemoryError::Disabled),
                        ProjectMemoryOperation::List { .. } => {
                            Ok(MemoryCommandResult::List(Page {
                                data: Vec::new(),
                                next_cursor: None,
                            }))
                        }
                    };
                }
                let (selected_session_id, workspace_root) =
                    self.resolve_project_memory_source(candidates)?;
                match operation {
                    ProjectMemoryOperation::Remember {
                        text,
                        kind,
                        source_user_item_id,
                        source_session_id,
                        source_turn_id,
                    } => Ok(MemoryCommandResult::Remember(self.remember(
                        MemoryRememberRequest {
                            text,
                            scope: MemoryScope::Project,
                            kind,
                            source: MemorySourceContext {
                                user_item_id: source_user_item_id,
                                session_id: source_session_id.unwrap_or(selected_session_id),
                                turn_id: source_turn_id,
                                workspace_root,
                            },
                        },
                    )?)),
                    ProjectMemoryOperation::List {
                        kind,
                        state,
                        origin,
                        text,
                        cursor,
                        limit,
                    } => Ok(MemoryCommandResult::List(self.list(ListMemoryRequest {
                        scope: Some(MemoryScope::Project),
                        kind,
                        state,
                        origin,
                        text,
                        cursor,
                        limit,
                        workspace_root,
                    })?)),
                }
            }
        }
    }

    /// Resolves Native Session candidates to the one canonical Project source.
    pub(crate) fn resolve_project_memory_source(
        &self,
        candidates: Vec<ProjectMemorySession>,
    ) -> Result<(SessionId, PathBuf), MemoryError> {
        let mut selected: Option<(SessionId, PathBuf, ProjectMemorySessionActivity, String)> = None;
        for candidate in candidates {
            let workspace_root = candidate
                .workspace_root
                .ok_or(MemoryError::ProjectSessionUnavailable)?;
            let identity = identity::resolve_project_memory_identity(&workspace_root)
                .map_err(|error| MemoryError::ProjectIdentity(error.to_string()))?;
            if let Some((_, _, current_activity, current_scope_id)) = selected.as_ref() {
                if *current_scope_id != identity.scope_id {
                    return Err(MemoryError::AmbiguousProjectScope);
                }
                if candidate.activity == ProjectMemorySessionActivity::Active
                    && *current_activity == ProjectMemorySessionActivity::Inactive
                {
                    selected = Some((
                        candidate.session_id,
                        workspace_root,
                        candidate.activity,
                        identity.scope_id,
                    ));
                }
            } else {
                selected = Some((
                    candidate.session_id,
                    workspace_root,
                    candidate.activity,
                    identity.scope_id,
                ));
            }
        }
        selected
            .map(|(session_id, workspace_root, _, _)| (session_id, workspace_root))
            .ok_or(MemoryError::ProjectSessionRequired)
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
