use chrono::Utc;
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::MemoryForgetResult;
use rusqlite::OptionalExtension;

use super::entries::{load_entry, normalize_body};
use super::{
    MemoryError, MemoryForgetRequest, MemoryForgetSelector, MemoryRuntime, MemoryScope, scope_name,
    state_name,
};

impl MemoryRuntime {
    pub(super) fn forget(
        &self,
        request: MemoryForgetRequest,
    ) -> Result<MemoryForgetResult, MemoryError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.unchecked_transaction()?;
        let (entry_id, normalized_key, scope, scope_id) = match request.selector {
            MemoryForgetSelector::EntryId(entry_id) => {
                let target = transaction
                    .query_row(
                        "SELECT entry_id, normalized_key, scope_type, scope_id
                         FROM memory_entries
                         WHERE entry_id = ?1",
                        [entry_id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ))
                        },
                    )
                    .optional()?;
                let (entry_id, normalized_key, scope, scope_id) = target
                    .ok_or_else(|| MemoryError::InvalidRequest("memory entry not found".into()))?;
                let scope = parse_scope(&scope)?;
                (entry_id, normalized_key, scope, scope_id)
            }
            MemoryForgetSelector::Text(text) => {
                let text = normalize_body(&text)?;
                let scope = request.scope;
                let scope_id = self.scope_id(scope, &request.source.workspace_root)?;
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
                            rusqlite::params![scope_name(request.scope), scope_id, text],
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
                (entry_id, normalized_key, scope, scope_id)
            }
        };

        let now = Utc::now().to_rfc3339();
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
        transaction.execute(
            "UPDATE memory_entries
             SET state = ?1, updated_at = ?2
             WHERE entry_id = ?3 AND scope_type = ?4 AND scope_id = ?5",
            rusqlite::params![
                state_name(devo_protocol::native::rpc_memory::MemoryState::Retired),
                now,
                entry_id,
                scope_name(scope),
                scope_id,
            ],
        )?;
        transaction.execute(
            "DELETE FROM memory_entries_fts WHERE entry_id = ?1",
            [entry_id.as_str()],
        )?;
        transaction.commit()?;

        let entry_id = MemoryEntryId::from_string(entry_id);
        let entry = load_entry(&connection, &entry_id)?
            .ok_or_else(|| MemoryError::InvalidStoredValue("forgotten entry is missing".into()))?;
        self.refresh_projection(&connection, scope, &scope_id)?;
        Ok(MemoryForgetResult {
            forgotten: Some(entry),
            candidates: Vec::new(),
        })
    }
}

fn parse_scope(value: &str) -> Result<MemoryScope, MemoryError> {
    match value {
        "user" => Ok(MemoryScope::User),
        "project" => Ok(MemoryScope::Project),
        _ => Err(MemoryError::InvalidStoredValue(value.into())),
    }
}
