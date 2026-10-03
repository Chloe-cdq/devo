use chrono::{DateTime, Utc};
use rusqlite::Transaction;

use super::MemoryError;
use super::stored_values::parse_timestamp;

pub(super) fn canonicalize_revocation_identity(
    transaction: &Transaction<'_>,
    scope_type: &str,
    scope_id: &str,
    canonical_key: &str,
    legacy_key: &str,
) -> Result<(), MemoryError> {
    if canonical_key == legacy_key {
        return Ok(());
    }

    let mut statement = transaction.prepare(
        "SELECT revocation_id, revoked_at, restored_at
         FROM memory_revocations
         WHERE scope_type = ?1 AND scope_id = ?2
           AND (normalized_key = ?3 OR normalized_key = ?4)",
    )?;
    let revocations = statement
        .query_map(
            rusqlite::params![scope_type, scope_id, canonical_key, legacy_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    if revocations.is_empty() {
        return Ok(());
    }

    let mut revocation_id = String::new();
    let mut latest_revoked_at = None::<(DateTime<Utc>, String)>;
    let mut latest_restored_at = None::<(DateTime<Utc>, String)>;
    for (stored_revocation_id, revoked_at, restored_at) in revocations {
        revocation_id = revocation_id.max(stored_revocation_id);
        let parsed_revoked_at = parse_timestamp(&revoked_at)?;
        if latest_revoked_at
            .as_ref()
            .is_none_or(|(latest, _)| parsed_revoked_at > *latest)
        {
            latest_revoked_at = Some((parsed_revoked_at, revoked_at));
        }
        if let Some(restored_at) = restored_at {
            let parsed_restored_at = parse_timestamp(&restored_at)?;
            if latest_restored_at
                .as_ref()
                .is_none_or(|(latest, _)| parsed_restored_at > *latest)
            {
                latest_restored_at = Some((parsed_restored_at, restored_at));
            }
        }
    }
    let (revoked_at, revoked_at_value) = latest_revoked_at.ok_or_else(|| {
        MemoryError::InvalidStoredValue("revocation identity is missing revoked_at".into())
    })?;
    let restored_at = latest_restored_at
        .filter(|(restored_at, _)| *restored_at >= revoked_at)
        .map(|(_, value)| value);

    transaction.execute(
        "DELETE FROM memory_revocations
         WHERE scope_type = ?1 AND scope_id = ?2
           AND (normalized_key = ?3 OR normalized_key = ?4)",
        rusqlite::params![scope_type, scope_id, canonical_key, legacy_key],
    )?;
    transaction.execute(
        "INSERT INTO memory_revocations (
             revocation_id, scope_type, scope_id, normalized_key, revoked_at, restored_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            revocation_id,
            scope_type,
            scope_id,
            canonical_key,
            revoked_at_value,
            restored_at,
        ],
    )?;
    Ok(())
}

/// Apply durable revocation, retirement and lexical removal in the caller's transaction.
pub(super) fn revoke_entry(
    transaction: &Transaction<'_>,
    entry_id: &str,
    scope_type: &str,
    scope_id: &str,
    normalized_key: &str,
    now: &str,
) -> Result<(), MemoryError> {
    transaction.execute(
        "INSERT INTO memory_revocations (
                 revocation_id, scope_type, scope_id, normalized_key, revoked_at, restored_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, NULL)
             ON CONFLICT(scope_type, scope_id, normalized_key) DO UPDATE SET
                 revoked_at = excluded.revoked_at,
                 restored_at = NULL",
        rusqlite::params![
            uuid::Uuid::now_v7().simple().to_string(),
            scope_type,
            scope_id,
            normalized_key,
            now,
        ],
    )?;
    let updated = transaction.execute(
        "UPDATE memory_entries
             SET state = ?1, updated_at = ?2
             WHERE entry_id = ?3 AND scope_type = ?4 AND scope_id = ?5",
        rusqlite::params!["retired", now, entry_id, scope_type, scope_id,],
    )?;
    if updated != 1 {
        return Err(MemoryError::InvalidRequest(
            "memory entry not found".to_string(),
        ));
    }
    transaction.execute(
        "DELETE FROM memory_entries_fts WHERE entry_id = ?1",
        [entry_id],
    )?;
    Ok(())
}
