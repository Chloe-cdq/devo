use super::command_types::{PreparedMemoryForgetScope, PreparedMemoryForgetTarget};
use super::entries::{load_entry, normalize_body};
use super::{
    MemoryError, MemoryForgetRequest, MemoryForgetSelector, MemoryForgetSource, MemoryRuntime,
    PreparedMemoryForgetRequest, ResolvedProjectMemorySession, scope_name,
    select_project_memory_session,
};
use chrono::Utc;
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::{MemoryForgetResult, MemoryOrigin, MemoryScope};
use rusqlite::OptionalExtension;

impl MemoryRuntime {
    pub(super) fn prepare_forget(
        &self,
        request: MemoryForgetRequest,
    ) -> Result<PreparedMemoryForgetRequest, MemoryError> {
        let (target, scope, expected_project_scope_id) = match request.selector {
            MemoryForgetSelector::EntryId(entry_id) => {
                let entry = self
                    .entry_by_id(&entry_id)?
                    .ok_or_else(|| MemoryError::InvalidRequest("memory entry not found".into()))?;
                if entry.origin == MemoryOrigin::InferredSession
                    && self.has_pending_source_deletions()
                {
                    return Err(MemoryError::InvalidRequest("memory entry not found".into()));
                }
                let scope = entry.scope;
                let expected_project_scope_id = match scope {
                    MemoryScope::User if entry.scope_id != super::USER_SCOPE_ID => {
                        return Err(MemoryError::InvalidRequest(
                            "memory entry not found".to_string(),
                        ));
                    }
                    MemoryScope::User => None,
                    MemoryScope::Project => Some(entry.scope_id),
                };
                (
                    PreparedMemoryForgetTarget::Exact(entry.entry_id),
                    scope,
                    expected_project_scope_id,
                )
            }
            MemoryForgetSelector::Text(text) => (
                PreparedMemoryForgetTarget::Text(normalize_body(&text)?),
                request.scope,
                None,
            ),
        };
        let (source_session_id, scope_id) = self.resolve_forget_source(
            scope,
            expected_project_scope_id.as_deref(),
            request.source,
        )?;
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
                let session_id = match source.bound_session_id {
                    Some(session_id) => session_id,
                    None => match source.user_session {
                        super::MemoryUserSessionSelection::Selected(session_id) => session_id,
                        super::MemoryUserSessionSelection::Unbound => {
                            return Err(MemoryError::InvalidRequest(
                                "memory/forget requires a session-bound connection".to_string(),
                            ));
                        }
                        super::MemoryUserSessionSelection::Ambiguous => {
                            return Err(MemoryError::InvalidRequest(
                                "memory/forget User scope has ambiguous Native Session selectors"
                                    .to_string(),
                            ));
                        }
                    },
                };
                let session = source
                    .sessions
                    .into_iter()
                    .find(|candidate| candidate.session_id == session_id)
                    .ok_or_else(|| {
                        MemoryError::InvalidRequest(
                            "memory/forget requires a session with a workspace root".to_string(),
                        )
                    })?;
                super::ensure_interactive_memory_source(session.source)?;
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
                        source: candidate.source,
                        scope_id,
                    });
                }
                if resolved_candidates.is_empty() && expected_project_scope_id.is_some() {
                    return Err(deferred_error.unwrap_or_else(|| {
                        MemoryError::InvalidRequest("memory entry not found".to_string())
                    }));
                }
                let selected = select_project_memory_session(resolved_candidates)?;
                super::ensure_interactive_memory_source(selected.source)?;
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
        let hide_inferred = self.has_pending_source_deletions();
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.unchecked_transaction()?;
        let (entry_id, normalized_key, scope_id) = match request.target {
            PreparedMemoryForgetTarget::Exact(prepared_entry_id) => {
                let normalized_key = transaction
                    .query_row(
                        "SELECT normalized_key FROM memory_entries
                         WHERE entry_id = ?1 AND scope_type = ?2 AND scope_id = ?3
                           AND (?4 = 0 OR origin = 'explicit_user')",
                        rusqlite::params![
                            prepared_entry_id.as_str(),
                            scope_name(scope),
                            prepared_scope_id,
                            hide_inferred,
                        ],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .ok_or_else(|| MemoryError::InvalidRequest("memory entry not found".into()))?;
                (
                    prepared_entry_id.to_string(),
                    normalized_key,
                    prepared_scope_id,
                )
            }
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
                           AND (?4 = 0 OR origin = 'explicit_user')
                         ORDER BY updated_at DESC, entry_id ASC",
                    )?;
                    statement
                        .query_map(
                            rusqlite::params![scope_name(scope), scope_id, text, hide_inferred],
                            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                        )?
                        .collect::<Result<Vec<_>, _>>()?
                };
                if targets.len() != 1 {
                    if targets.is_empty() {
                        return Err(MemoryError::InvalidRequest("memory entry not found".into()));
                    }
                    let mut candidates = targets
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
                    if !hide_inferred && self.has_pending_source_deletions() {
                        candidates.retain(|entry| entry.origin == MemoryOrigin::ExplicitUser);
                    }
                    if candidates.is_empty() {
                        return Err(MemoryError::InvalidRequest("memory entry not found".into()));
                    }
                    if candidates.len() == 1 {
                        return Err(MemoryError::InvalidRequest(
                            "memory selection changed; retry".into(),
                        ));
                    }
                    return Ok(MemoryForgetResult {
                        forgotten: None,
                        candidates,
                    });
                }
                let (entry_id, normalized_key) = targets.into_iter().next().ok_or_else(|| {
                    MemoryError::InvalidStoredValue("forget target is missing".into())
                })?;
                (entry_id, normalized_key, scope_id)
            }
        };

        if !hide_inferred && self.has_pending_source_deletions() {
            let origin: String = transaction.query_row(
                "SELECT origin FROM memory_entries WHERE entry_id = ?1",
                [entry_id.as_str()],
                |row| row.get(0),
            )?;
            if origin == "inferred_session" {
                return Err(MemoryError::InvalidRequest("memory entry not found".into()));
            }
        }

        let now = Utc::now().to_rfc3339();
        super::revocation_lifecycle::revoke_entry(
            &transaction,
            &entry_id,
            scope_name(scope),
            &scope_id,
            &normalized_key,
            &now,
        )?;
        let entry_id = MemoryEntryId::from_string(entry_id);
        let entry = load_entry(&transaction, &entry_id)?
            .ok_or_else(|| MemoryError::InvalidStoredValue("forgotten entry is missing".into()))?;
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
