//! Selective historical memory cleanup, inside the schema migration transaction.

use std::collections::BTreeSet;

use rusqlite::Transaction;

use crate::memory::MemoryError;
use crate::memory::entry_identity::MemoryEntryIdentity;

use super::contains_secret;

pub(in crate::memory) fn purge_unsafe_memory(
    transaction: &Transaction<'_>,
) -> Result<(), MemoryError> {
    let mut entry_ids = BTreeSet::new();
    let mut candidate_ids = BTreeSet::new();
    let mut unsafe_identities = BTreeSet::new();
    let mut safe_entry_keys = BTreeSet::new();
    let mut affected_scopes = BTreeSet::new();
    for (table, id_column) in [
        ("memory_entries", "entry_id"),
        ("memory_candidates", "candidate_id"),
    ] {
        let mut statement = transaction.prepare(&format!(
            "SELECT {id_column}, scope_type, scope_id, normalized_key, body FROM {table}"
        ))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for (id, scope_type, scope_id, normalized_key, body) in rows {
            let unsafe_body = contains_secret(&body);
            if !unsafe_body && !contains_secret(&normalized_key) {
                if table == "memory_entries" {
                    safe_entry_keys.insert((scope_type, scope_id, normalized_key));
                }
                continue;
            }
            affected_scopes.insert((scope_type.clone(), scope_id.clone()));
            if table == "memory_entries" {
                entry_ids.insert(id);
                unsafe_identities.insert((scope_type.clone(), scope_id.clone(), normalized_key));
            } else {
                candidate_ids.insert(id);
            }
            if unsafe_body {
                let identity = MemoryEntryIdentity::from_body(&body);
                unsafe_identities.insert((
                    scope_type.clone(),
                    scope_id.clone(),
                    identity.canonical_key,
                ));
                unsafe_identities.insert((scope_type, scope_id, identity.legacy_inferred_key));
            }
        }
    }

    // A lossy identity derived from an unsafe body is not proof against an
    // existing safe entry with that scoped key. Preserve its authority and
    // tombstone; direct sensitive fields and bindings are still removed.
    unsafe_identities.retain(|identity| !safe_entry_keys.contains(identity));

    let mut statement = transaction.prepare(
        "SELECT rowid, scope_type, scope_id, proposal_key, canonical_key, entry_id FROM memory_proposal_claims"
    )?;
    let claims = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for (rowid, scope_type, scope_id, proposal_key, canonical_key, entry_id) in claims {
        if contains_secret(&proposal_key)
            || contains_secret(&canonical_key)
            || unsafe_identities.contains(&(scope_type.clone(), scope_id.clone(), proposal_key))
            || unsafe_identities.contains(&(scope_type.clone(), scope_id.clone(), canonical_key))
            || entry_id.as_ref().is_some_and(|id| entry_ids.contains(id))
        {
            affected_scopes.insert((scope_type, scope_id));
            transaction.execute(
                "DELETE FROM memory_proposal_claims WHERE rowid = ?1",
                [rowid],
            )?;
        }
    }

    let mut statement = transaction.prepare(
        "SELECT revocation_id, scope_type, scope_id, normalized_key FROM memory_revocations",
    )?;
    let revocations = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for (id, scope_type, scope_id, normalized_key) in revocations {
        if contains_secret(&normalized_key)
            || unsafe_identities.contains(&(scope_type.clone(), scope_id.clone(), normalized_key))
        {
            affected_scopes.insert((scope_type, scope_id));
            transaction.execute(
                "DELETE FROM memory_revocations WHERE revocation_id = ?1",
                [id],
            )?;
        }
    }

    // A scope marker survives removal of its final row so startup also replaces
    // a historical Markdown projection for an emptied scope.
    for (scope_type, scope_id) in affected_scopes {
        transaction.execute(
            "INSERT INTO memory_scope_state(scope_type, scope_id) VALUES (?1, ?2)
             ON CONFLICT(scope_type, scope_id) DO NOTHING",
            rusqlite::params![scope_type, scope_id],
        )?;
    }
    for id in &entry_ids {
        transaction.execute("DELETE FROM memory_evidence WHERE entry_id = ?1", [id])?;
        transaction.execute(
            "UPDATE memory_entries SET replacement_entry_id = NULL WHERE replacement_entry_id = ?1",
            [id],
        )?;
        transaction.execute("DELETE FROM memory_entries_fts WHERE entry_id = ?1", [id])?;
        transaction.execute("DELETE FROM memory_entries WHERE entry_id = ?1", [id])?;
    }
    for id in candidate_ids {
        transaction.execute(
            "DELETE FROM memory_candidates WHERE candidate_id = ?1",
            [id],
        )?;
    }

    // FTS can contain orphan copies or outdated bytes belonging to an otherwise
    // safe entry. Read surviving canonical bytes when restoring its recall row.
    let mut statement = transaction
        .prepare("SELECT rowid, entry_id, normalized_key, body FROM memory_entries_fts")?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    let mut affected_fts_entries = BTreeSet::new();
    for (rowid, entry_id, normalized_key, body) in rows {
        if contains_secret(&normalized_key) || contains_secret(&body) {
            transaction.execute("DELETE FROM memory_entries_fts WHERE rowid = ?1", [rowid])?;
            affected_fts_entries.insert(entry_id);
        }
    }
    for id in affected_fts_entries {
        transaction.execute(
            "INSERT INTO memory_entries_fts(entry_id, normalized_key, body)
             SELECT entry_id, normalized_key, body FROM memory_entries
             WHERE entry_id = ?1 AND state IN ('active', 'restored')
               AND NOT EXISTS (SELECT 1 FROM memory_entries_fts WHERE entry_id = ?1)",
            [id],
        )?;
    }
    Ok(())
}
