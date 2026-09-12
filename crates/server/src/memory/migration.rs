use std::collections::BTreeMap;

use rusqlite::Transaction;

use super::MemoryError;
use super::equivalence;

struct StoredEntry {
    entry_id: String,
    scope_type: String,
    scope_id: String,
    kind: String,
    normalized_key: String,
    body: String,
    origin: String,
    state: String,
    created_at: String,
    updated_at: String,
    last_recalled_at: Option<String>,
    replacement_entry_id: Option<String>,
    expires_at: Option<String>,
}

pub(super) fn migrate_explicit_equivalence(
    transaction: &Transaction<'_>,
) -> Result<(), MemoryError> {
    let entries = load_entries(transaction)?;
    let mut groups = BTreeMap::<(String, String, String), Vec<StoredEntry>>::new();
    let mut explicit_key_migrations = BTreeMap::<(String, String, String), String>::new();
    for entry in entries {
        let normalized_key = if entry.origin == "explicit_user" {
            equivalence::explicit_memory_key(&entry.body)
        } else {
            entry.normalized_key.clone()
        };
        if entry.origin == "explicit_user" && entry.normalized_key != normalized_key {
            explicit_key_migrations.insert(
                (
                    entry.scope_type.clone(),
                    entry.scope_id.clone(),
                    entry.normalized_key.clone(),
                ),
                normalized_key.clone(),
            );
        }
        groups
            .entry((
                entry.scope_type.clone(),
                entry.scope_id.clone(),
                normalized_key,
            ))
            .or_default()
            .push(entry);
    }

    transaction.execute_batch(
        "DROP INDEX IF EXISTS memory_entries_scope_key;
         DROP INDEX IF EXISTS memory_revocations_scope_identity;",
    )?;
    for ((scope_type, scope_id, old_key), new_key) in explicit_key_migrations {
        transaction.execute(
            "UPDATE memory_revocations
             SET normalized_key = ?1
             WHERE scope_type = ?2 AND scope_id = ?3 AND normalized_key = ?4",
            rusqlite::params![new_key, scope_type, scope_id, old_key],
        )?;
    }
    transaction.execute_batch(
        "UPDATE memory_revocations AS kept
         SET revoked_at = (
                 SELECT MAX(all_rows.revoked_at)
                 FROM memory_revocations AS all_rows
                 WHERE all_rows.scope_type = kept.scope_type
                   AND all_rows.scope_id = kept.scope_id
                   AND all_rows.normalized_key = kept.normalized_key
             ),
             restored_at = (
                 SELECT CASE
                     WHEN MAX(all_rows.restored_at) >= MAX(all_rows.revoked_at)
                     THEN MAX(all_rows.restored_at)
                     ELSE NULL
                 END
                 FROM memory_revocations AS all_rows
                 WHERE all_rows.scope_type = kept.scope_type
                   AND all_rows.scope_id = kept.scope_id
                   AND all_rows.normalized_key = kept.normalized_key
             )
         WHERE kept.revocation_id = (
             SELECT MAX(candidate.revocation_id)
             FROM memory_revocations AS candidate
             WHERE candidate.scope_type = kept.scope_type
               AND candidate.scope_id = kept.scope_id
               AND candidate.normalized_key = kept.normalized_key
         );

         DELETE FROM memory_revocations
         WHERE revocation_id != (
             SELECT MAX(candidate.revocation_id)
             FROM memory_revocations AS candidate
             WHERE candidate.scope_type = memory_revocations.scope_type
               AND candidate.scope_id = memory_revocations.scope_id
               AND candidate.normalized_key = memory_revocations.normalized_key
         );",
    )?;
    let mut replacement_redirects = Vec::new();
    for ((_, _, normalized_key), entries) in groups {
        replacement_redirects.extend(merge_group(transaction, &normalized_key, &entries)?);
    }
    for (duplicate_id, keeper_id) in replacement_redirects {
        transaction.execute(
            "UPDATE memory_entries SET replacement_entry_id = ?1
             WHERE replacement_entry_id = ?2",
            rusqlite::params![keeper_id, duplicate_id],
        )?;
    }
    transaction.execute(
        "UPDATE memory_entries SET replacement_entry_id = NULL
         WHERE replacement_entry_id = entry_id",
        [],
    )?;
    deduplicate_evidence(transaction)?;
    transaction.execute_batch(
        "DELETE FROM memory_entries_fts;
         INSERT INTO memory_entries_fts (entry_id, normalized_key, body)
         SELECT entry_id, normalized_key, body FROM memory_entries;
         CREATE UNIQUE INDEX memory_entries_scope_key
             ON memory_entries (scope_type, scope_id, normalized_key);
         CREATE UNIQUE INDEX memory_revocations_scope_identity
             ON memory_revocations (scope_type, scope_id, normalized_key);",
    )?;
    Ok(())
}

fn load_entries(transaction: &Transaction<'_>) -> Result<Vec<StoredEntry>, MemoryError> {
    let mut statement = transaction.prepare(
        "SELECT entry_id, scope_type, scope_id, kind, normalized_key, body, origin, state,
                created_at, updated_at, last_recalled_at, replacement_entry_id, expires_at
         FROM memory_entries",
    )?;
    let entries = statement
        .query_map([], |row| {
            Ok(StoredEntry {
                entry_id: row.get(0)?,
                scope_type: row.get(1)?,
                scope_id: row.get(2)?,
                kind: row.get(3)?,
                normalized_key: row.get(4)?,
                body: row.get(5)?,
                origin: row.get(6)?,
                state: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
                last_recalled_at: row.get(10)?,
                replacement_entry_id: row.get(11)?,
                expires_at: row.get(12)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(entries)
}

fn merge_group(
    transaction: &Transaction<'_>,
    normalized_key: &str,
    entries: &[StoredEntry],
) -> Result<Vec<(String, String)>, MemoryError> {
    let keeper = entries
        .iter()
        .min_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.entry_id.cmp(&right.entry_id))
        })
        .ok_or_else(|| MemoryError::InvalidStoredValue("empty migration group".into()))?;
    let current = entries
        .iter()
        .filter(|entry| entry.origin == "explicit_user")
        .max_by(|left, right| {
            left.updated_at
                .cmp(&right.updated_at)
                .then_with(|| left.entry_id.cmp(&right.entry_id))
        })
        .or_else(|| {
            entries.iter().max_by(|left, right| {
                left.updated_at
                    .cmp(&right.updated_at)
                    .then_with(|| left.entry_id.cmp(&right.entry_id))
            })
        })
        .ok_or_else(|| MemoryError::InvalidStoredValue("empty migration group".into()))?;

    let mut replacement_redirects = Vec::new();
    for duplicate in entries
        .iter()
        .filter(|entry| entry.entry_id != keeper.entry_id)
    {
        replacement_redirects.push((duplicate.entry_id.clone(), keeper.entry_id.clone()));
        transaction.execute(
            "DELETE FROM memory_evidence AS duplicate
             WHERE duplicate.entry_id = ?1
               AND EXISTS (
                   SELECT 1 FROM memory_evidence AS kept
                   WHERE kept.entry_id = ?2
                     AND kept.session_id = duplicate.session_id
                     AND kept.turn_id IS duplicate.turn_id
                     AND kept.source_user_item_id IS duplicate.source_user_item_id
               )",
            rusqlite::params![duplicate.entry_id, keeper.entry_id],
        )?;
        transaction.execute(
            "UPDATE memory_evidence SET entry_id = ?1 WHERE entry_id = ?2",
            rusqlite::params![keeper.entry_id, duplicate.entry_id],
        )?;
        transaction.execute(
            "DELETE FROM memory_entries WHERE entry_id = ?1",
            [&duplicate.entry_id],
        )?;
    }

    transaction.execute(
        "UPDATE memory_entries
         SET kind = ?1, normalized_key = ?2, body = ?3, origin = ?4, state = ?5,
             updated_at = ?6, last_recalled_at = ?7, replacement_entry_id = ?8,
             expires_at = ?9
         WHERE entry_id = ?10",
        rusqlite::params![
            current.kind,
            normalized_key,
            current.body,
            current.origin,
            current.state,
            current.updated_at,
            current.last_recalled_at,
            current.replacement_entry_id,
            current.expires_at,
            keeper.entry_id,
        ],
    )?;
    Ok(replacement_redirects)
}

fn deduplicate_evidence(transaction: &Transaction<'_>) -> Result<(), MemoryError> {
    transaction.execute_batch(
        "DELETE FROM memory_evidence AS duplicate
         WHERE EXISTS (
             SELECT 1 FROM memory_evidence AS kept
             WHERE kept.entry_id = duplicate.entry_id
               AND kept.session_id = duplicate.session_id
               AND kept.turn_id IS duplicate.turn_id
               AND kept.source_user_item_id IS duplicate.source_user_item_id
               AND (
                   kept.observed_at < duplicate.observed_at
                   OR (
                       kept.observed_at = duplicate.observed_at
                       AND kept.evidence_id < duplicate.evidence_id
                   )
               )
         );",
    )?;
    Ok(())
}
