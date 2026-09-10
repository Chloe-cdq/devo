use chrono::Utc;
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::MemoryForgetResult;
use rusqlite::OptionalExtension;

use super::entries::{load_entry, normalize_body};
use super::{MemoryError, MemoryForgetRequest, MemoryRuntime, scope_name, state_name};

impl MemoryRuntime {
    pub(super) fn forget(
        &self,
        request: MemoryForgetRequest,
    ) -> Result<MemoryForgetResult, MemoryError> {
        let scope_id = self.scope_id(request.scope, &request.workspace_root)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.unchecked_transaction()?;
        let (entry_id, normalized_key) = match (request.entry_id, request.text) {
            (Some(_), Some(_)) => {
                return Err(MemoryError::InvalidRequest(
                    "memory forget accepts exactly one of entryId or text".into(),
                ));
            }
            (None, None) => {
                return Err(MemoryError::InvalidRequest(
                    "memory forget requires entryId or text".into(),
                ));
            }
            (Some(entry_id), None) => {
                let target = transaction
                    .query_row(
                        "SELECT entry_id, normalized_key
                         FROM memory_entries
                         WHERE entry_id = ?1 AND scope_type = ?2 AND scope_id = ?3",
                        rusqlite::params![entry_id.as_str(), scope_name(request.scope), scope_id,],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()?;
                target
                    .ok_or_else(|| MemoryError::InvalidRequest("memory entry not found".into()))?
            }
            (None, Some(text)) => {
                let text = normalize_body(&text)?;
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
                targets.into_iter().next().ok_or_else(|| {
                    MemoryError::InvalidStoredValue("forget target is missing".into())
                })?
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
                scope_name(request.scope),
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
                scope_name(request.scope),
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
        self.refresh_projection(&connection, request.scope, &scope_id)?;
        Ok(MemoryForgetResult {
            forgotten: Some(entry),
            candidates: Vec::new(),
        })
    }
}
