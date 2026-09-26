use super::command_types::{PreparedMemoryForgetScope, PreparedMemoryForgetTarget};
use super::entries::{load_entry, normalize_body};
use super::{
    MemoryError, MemoryForgetRequest, MemoryForgetSelector, MemoryForgetSource, MemoryRuntime,
    PreparedMemoryForgetRequest, ResolvedProjectMemorySession, scope_name,
    select_project_memory_session, state_name,
};
use chrono::Utc;
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryForgetResult, MemoryScope, MemoryState,
};

impl MemoryRuntime {
    pub(super) fn prepare_forget(
        &self,
        request: MemoryForgetRequest,
    ) -> Result<PreparedMemoryForgetRequest, MemoryError> {
        let (target, scope) = match request.selector {
            MemoryForgetSelector::EntryId(entry_id) => {
                let entry = self
                    .entry_by_id(&entry_id)?
                    .ok_or_else(|| MemoryError::InvalidRequest("memory entry not found".into()))?;
                let scope = entry.scope;
                (PreparedMemoryForgetTarget::Exact(entry), scope)
            }
            MemoryForgetSelector::Text(text) => (
                PreparedMemoryForgetTarget::Text(normalize_body(&text)?),
                request.scope,
            ),
        };
        let expected_project_scope_id = match &target {
            PreparedMemoryForgetTarget::Exact(entry) if scope == MemoryScope::Project => {
                Some(entry.scope_id.as_str())
            }
            PreparedMemoryForgetTarget::Exact(_) | PreparedMemoryForgetTarget::Text(_) => None,
        };
        let (source_session_id, scope_id) =
            self.resolve_forget_source(scope, expected_project_scope_id, request.source)?;
        if matches!(
            &target,
            PreparedMemoryForgetTarget::Exact(entry) if entry.scope_id != scope_id
        ) {
            return Err(MemoryError::InvalidRequest(
                "memory entry not found".to_string(),
            ));
        }
        Ok(PreparedMemoryForgetRequest {
            target,
            scope: PreparedMemoryForgetScope { scope, scope_id },
            source_session_id,
        })
    }

    fn resolve_forget_source(
        &self,
        scope: MemoryScope,
        expected_project_scope_id: Option<&str>,
        source: MemoryForgetSource,
    ) -> Result<(devo_protocol::SessionId, String), MemoryError> {
        match scope {
            MemoryScope::User => {
                let session_id = source
                    .bound_session_id
                    .or(source.user_session_id)
                    .ok_or_else(|| {
                        MemoryError::InvalidRequest(
                            "memory/forget requires a session-bound connection".to_string(),
                        )
                    })?;
                let session = source
                    .sessions
                    .into_iter()
                    .find(|candidate| candidate.session_id == session_id)
                    .ok_or_else(|| {
                        MemoryError::InvalidRequest(
                            "memory/forget requires a session with a workspace root".to_string(),
                        )
                    })?;
                session.workspace_root.ok_or_else(|| {
                    MemoryError::InvalidRequest(
                        "memory/forget requires a session with a workspace root".to_string(),
                    )
                })?;
                Ok((session_id, super::USER_SCOPE_ID.to_string()))
            }
            MemoryScope::Project => {
                let mut resolved_candidates = Vec::with_capacity(source.sessions.len());
                let mut deferred_error = None;
                for candidate in source.sessions {
                    if source.bound_session_id.is_some()
                        && source.bound_session_id != Some(candidate.session_id)
                    {
                        continue;
                    }
                    let Some(workspace_root) = candidate.workspace_root else {
                        if expected_project_scope_id.is_some() {
                            deferred_error.get_or_insert(MemoryError::ProjectSessionUnavailable);
                            continue;
                        }
                        return Err(MemoryError::ProjectSessionUnavailable);
                    };
                    let scope_id = match self.scope_id(MemoryScope::Project, &workspace_root) {
                        Ok(scope_id) => scope_id,
                        Err(error) => {
                            if expected_project_scope_id.is_some() {
                                deferred_error.get_or_insert(error);
                                continue;
                            }
                            return Err(error);
                        }
                    };
                    if expected_project_scope_id.is_some_and(|expected| expected != scope_id) {
                        continue;
                    }
                    resolved_candidates.push(ResolvedProjectMemorySession {
                        session_id: candidate.session_id,
                        workspace_root,
                        activity: candidate.activity,
                        scope_id,
                    });
                }
                if resolved_candidates.is_empty() && expected_project_scope_id.is_some() {
                    return Err(deferred_error.unwrap_or_else(|| {
                        MemoryError::InvalidRequest("memory entry not found".to_string())
                    }));
                }
                let selected = select_project_memory_session(resolved_candidates)?;
                Ok((selected.session_id, selected.scope_id))
            }
        }
    }

    pub(super) fn forget(
        &self,
        request: PreparedMemoryForgetRequest,
    ) -> Result<MemoryForgetResult, MemoryError> {
        let scope = request.scope.scope;
        let prepared_scope_id = request.scope.scope_id;
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.unchecked_transaction()?;
        let (entry_id, normalized_key, scope_id, prepared_entry) = match request.target {
            PreparedMemoryForgetTarget::Exact(entry) => (
                entry.entry_id.to_string(),
                entry.normalized_key.clone(),
                prepared_scope_id,
                Some(entry),
            ),
            PreparedMemoryForgetTarget::Text(text) => {
                let scope_id = prepared_scope_id.clone();
                let targets = {
                    let mut statement = transaction.prepare(
                        "SELECT entry_id, normalized_key
                         FROM memory_entries
                         WHERE scope_type = ?1
                           AND scope_id = ?2
                           AND (instr(lower(body), lower(?3)) > 0
                                OR instr(lower(normalized_key), lower(?3)) > 0)
                         ORDER BY updated_at DESC, entry_id ASC",
                    )?;
                    statement
                        .query_map(
                            rusqlite::params![scope_name(scope), scope_id, text],
                            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                        )?
                        .collect::<Result<Vec<_>, _>>()?
                };
                if targets.len() != 1 {
                    if targets.is_empty() {
                        return Err(MemoryError::InvalidRequest("memory entry not found".into()));
                    }
                    let candidates = targets
                        .iter()
                        .map(|(entry_id, _)| {
                            load_entry(&transaction, &MemoryEntryId::from_string(entry_id.clone()))?
                                .ok_or_else(|| {
                                    MemoryError::InvalidStoredValue(
                                        "forget candidate is missing".into(),
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    return Ok(MemoryForgetResult {
                        forgotten: None,
                        candidates,
                    });
                }
                let (entry_id, normalized_key) = targets.into_iter().next().ok_or_else(|| {
                    MemoryError::InvalidStoredValue("forget target is missing".into())
                })?;
                (entry_id, normalized_key, scope_id, None)
            }
        };

        let updated_at = Utc::now();
        let now = updated_at.to_rfc3339();
        transaction.execute(
            "INSERT INTO memory_revocations (
                 revocation_id, scope_type, scope_id, normalized_key, revoked_at, restored_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, NULL)
             ON CONFLICT(scope_type, scope_id, normalized_key) DO UPDATE SET
                 revoked_at = excluded.revoked_at,
                 restored_at = NULL",
            rusqlite::params![
                uuid::Uuid::now_v7().simple().to_string(),
                scope_name(scope),
                scope_id,
                normalized_key,
                now,
            ],
        )?;
        let updated = transaction.execute(
            "UPDATE memory_entries
             SET state = ?1, updated_at = ?2
             WHERE entry_id = ?3 AND scope_type = ?4 AND scope_id = ?5",
            rusqlite::params![
                state_name(MemoryState::Retired),
                now,
                entry_id,
                scope_name(scope),
                scope_id,
            ],
        )?;
        if updated != 1 {
            return Err(MemoryError::InvalidRequest(
                "memory entry not found".to_string(),
            ));
        }
        transaction.execute(
            "DELETE FROM memory_entries_fts WHERE entry_id = ?1",
            [entry_id.as_str()],
        )?;
        let entry_id = MemoryEntryId::from_string(entry_id);
        let entry = match prepared_entry {
            Some(entry) => MemoryEntry {
                state: MemoryState::Retired,
                updated_at,
                ..entry
            },
            None => load_entry(&transaction, &entry_id)?.ok_or_else(|| {
                MemoryError::InvalidStoredValue("forgotten entry is missing".into())
            })?,
        };
        let result = MemoryForgetResult {
            forgotten: Some(entry),
            candidates: Vec::new(),
        };
        transaction.commit()?;

        if let Err(projection_error) = self.refresh_projection(&connection, scope, &scope_id) {
            return Err(MemoryError::ForgetCommitted {
                result: Box::new(result),
                projection_error: Box::new(projection_error),
            });
        }
        Ok(result)
    }
}
