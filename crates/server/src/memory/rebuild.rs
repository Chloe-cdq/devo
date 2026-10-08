//! Durable, explicit authorization for scoped history reprocessing.
use chrono::{DateTime, Utc};
use devo_protocol::native::rpc_memory::{MemoryRebuildResult, MemoryRebuildStatus, MemoryScope};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use super::stored_values::{parse_scope, parse_timestamp};
use super::{
    MemoryError, MemoryRuntime, MemoryUserSessionSelection, ProjectMemorySession,
    ScopedMemoryRequest, ensure_interactive_memory_source, scope_name,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RebuildRequest {
    pub(super) id: String,
    pub(super) scope: MemoryScope,
    pub(super) scope_id: String,
    pub(super) reset_fence: String,
    pub(super) requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ScanTarget {
    Automatic,
    Rebuild(RebuildRequest),
}

pub(super) fn rebuild_authorized(
    connection: &Connection,
    request: &RebuildRequest,
) -> Result<bool, MemoryError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM memory_rebuild_requests r
         WHERE rebuild_id = ?1 AND reset_fence = ?2 AND state != 'cancelled' AND reset_fence = COALESCE((
             SELECT COALESCE(ignore_sources_before, '') || ':' || projection_revision
             FROM memory_scope_state s WHERE s.scope_type = r.scope_type AND s.scope_id = r.scope_id), ':0'))",
        rusqlite::params![request.id, request.reset_fence], |row| row.get(/*idx*/ 0),
    )?)
}

impl MemoryRuntime {
    pub(super) fn authorize_rebuild(
        &self,
        scope: MemoryScope,
        user_session: MemoryUserSessionSelection,
        sessions: Vec<ProjectMemorySession>,
    ) -> Result<MemoryRebuildResult, MemoryError> {
        if !self.config.enabled {
            return Err(MemoryError::Disabled);
        }
        let workspace_root = match scope {
            MemoryScope::User => {
                let source = match user_session {
                    MemoryUserSessionSelection::Selected(id) => sessions
                        .iter()
                        .find(|session| session.session_id == id)
                        .and_then(|session| session.source),
                    MemoryUserSessionSelection::Unbound | MemoryUserSessionSelection::Ambiguous => {
                        None
                    }
                };
                ensure_interactive_memory_source(source)?;
                Default::default()
            }
            MemoryScope::Project => {
                let selected = self.resolve_project_memory_source(sessions)?;
                ensure_interactive_memory_source(selected.source)?;
                selected.workspace_root
            }
        };
        self.begin_rebuild(ScopedMemoryRequest {
            scope,
            workspace_root,
        })
    }

    pub(super) fn begin_rebuild(
        &self,
        request: ScopedMemoryRequest,
    ) -> Result<MemoryRebuildResult, MemoryError> {
        let scope_id = self.scope_id(request.scope, &request.workspace_root)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // The revision distinguishes consecutive resets even if their timestamps coincide.
        let fence: String = transaction.query_row(
            "SELECT COALESCE(ignore_sources_before, '') || ':' || projection_revision FROM memory_scope_state WHERE scope_type = ?1 AND scope_id = ?2",
            rusqlite::params![scope_name(request.scope), scope_id], |row| row.get(/*idx*/ 0),
        ).optional()?.unwrap_or_else(|| ":0".into());
        let now = (self.clock)().to_rfc3339();
        transaction.execute(
            "INSERT INTO memory_rebuild_requests(rebuild_id, scope_type, scope_id, reset_fence, requested_at)
             VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(scope_type, scope_id, reset_fence)
             DO UPDATE SET state = 'pending'",
            rusqlite::params![uuid::Uuid::now_v7().simple().to_string(), scope_name(request.scope), scope_id, fence, now],
        )?;
        let (rebuild_id, requested_at): (String, String) = transaction.query_row(
            "SELECT rebuild_id, requested_at FROM memory_rebuild_requests WHERE scope_type = ?1 AND scope_id = ?2 AND reset_fence = ?3",
            rusqlite::params![scope_name(request.scope), scope_id, fence],
            |row| Ok((row.get(/*idx*/ 0)?, row.get(/*idx*/ 1)?)),
        )?;
        transaction.execute(
            "INSERT INTO memory_scope_state(scope_type, scope_id, last_rebuild_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_type, scope_id) DO UPDATE SET last_rebuild_at = excluded.last_rebuild_at",
            rusqlite::params![scope_name(request.scope), scope_id, requested_at],
        )?;
        transaction.commit()?;
        Ok(MemoryRebuildResult {
            scope: request.scope,
            rebuild_id,
            requested_at: parse_timestamp(&requested_at)?,
        })
    }

    pub(super) fn pending_rebuilds(&self) -> Result<Vec<RebuildRequest>, MemoryError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let mut statement = connection.prepare(
            "SELECT rebuild_id, scope_type, scope_id, reset_fence, requested_at FROM memory_rebuild_requests WHERE state = 'pending' ORDER BY requested_at",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(/*idx*/ 0)?,
                    row.get::<_, String>(/*idx*/ 1)?,
                    row.get::<_, String>(/*idx*/ 2)?,
                    row.get::<_, String>(/*idx*/ 3)?,
                    row.get::<_, String>(/*idx*/ 4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(id, scope, scope_id, reset_fence, requested_at)| {
                Ok(RebuildRequest {
                    id,
                    scope: parse_scope(&scope)?,
                    scope_id,
                    reset_fence,
                    requested_at: parse_timestamp(&requested_at)?,
                })
            })
            .collect()
    }

    pub(super) fn rebuild_status(
        &self,
        connection: &Connection,
    ) -> Result<Option<MemoryRebuildStatus>, MemoryError> {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM memory_rebuild_requests)",
            [],
            |row| row.get(/*idx*/ 0),
        )?;
        if !exists {
            return Ok(None);
        }
        let count = |state: &str| -> Result<u64, MemoryError> {
            Ok(connection.query_row(
                "SELECT COUNT(*) FROM memory_jobs WHERE job_kind = 'source_rebuild' AND state = ?1",
                [state],
                |row| row.get(/*idx*/ 0),
            )?)
        };
        let receipts: u64 = connection.query_row(
            "SELECT COUNT(*) FROM memory_job_receipts WHERE job_kind = 'source_rebuild'",
            [],
            |row| row.get(/*idx*/ 0),
        )?;
        Ok(Some(MemoryRebuildStatus {
            pending_request_count: connection.query_row(
                "SELECT COUNT(*) FROM memory_rebuild_requests WHERE state = 'pending'",
                [],
                |row| row.get(/*idx*/ 0),
            )?,
            pending_job_count: count("pending")?,
            running_job_count: count("running")?,
            retrying_job_count: count("retrying")?,
            completed_job_count: count("completed")? + receipts,
            error_job_count: count("error")?,
        }))
    }

    /// Retire deferred work when a source no longer satisfies current eligibility.
    pub(super) async fn cancel_rebuild_source(
        self: &std::sync::Arc<Self>,
        rebuild_id: &str,
        source_id: &str,
    ) -> anyhow::Result<()> {
        let memory = std::sync::Arc::clone(self);
        let rebuild_id = rebuild_id.to_owned();
        let source_id = source_id.to_owned();
        tokio::task::spawn_blocking(move || -> Result<(), MemoryError> {
            let connection = memory.connection.lock().map_err(|_| MemoryError::LockPoisoned)?;
            connection.execute("UPDATE memory_jobs SET state = 'cancelled', lease_owner = NULL, lease_until = NULL
                WHERE rebuild_id = ?1 AND source_session_id = ?2 AND state IN ('pending', 'running', 'retrying')",
                rusqlite::params![rebuild_id, source_id])?;
            Ok(())
        }).await??;
        Ok(())
    }

    /// Scans resume durable authorizations without creating new ones.
    pub(super) fn finish_rebuild_pass(
        &self,
        request: &RebuildRequest,
        deferred: bool,
    ) -> Result<bool, MemoryError> {
        if deferred {
            return Ok(false);
        }
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        Ok(connection.execute(
            "UPDATE memory_rebuild_requests SET state = 'completed'
             WHERE rebuild_id = ?1 AND state = 'pending'
                AND NOT EXISTS(SELECT 1 FROM memory_jobs WHERE rebuild_id = ?1
                    AND state IN ('pending', 'running', 'retrying'))",
            [&request.id],
        )? > 0)
    }
}
