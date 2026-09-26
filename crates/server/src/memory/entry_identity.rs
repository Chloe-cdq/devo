use devo_protocol::native::rpc_memory::{MemoryOrigin, MemoryScope};
use rusqlite::Transaction;

use super::equivalence;
use super::stored_values::parse_origin;
use super::{MemoryError, scope_name};

pub(super) struct MemoryEntryIdentity {
    pub(super) canonical_key: String,
    pub(super) legacy_inferred_key: String,
}

pub(super) struct ExistingMemoryEntry {
    pub(super) entry_id: String,
    pub(super) origin: MemoryOrigin,
}

impl MemoryEntryIdentity {
    pub(super) fn from_body(body: &str) -> Self {
        Self {
            canonical_key: equivalence::explicit_memory_key(body),
            legacy_inferred_key: body
                .chars()
                .filter(|character| character.is_alphanumeric() || character.is_whitespace())
                .collect::<String>()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_lowercase(),
        }
    }

    pub(super) fn resolve_and_merge_existing(
        &self,
        transaction: &Transaction<'_>,
        scope: MemoryScope,
        scope_id: &str,
    ) -> Result<Option<ExistingMemoryEntry>, MemoryError> {
        let mut statement = transaction.prepare(
            "SELECT entry_id, origin, normalized_key
             FROM memory_entries
             WHERE scope_type = ?1 AND scope_id = ?2
               AND (
                   normalized_key = ?3
                   OR (normalized_key = ?4 AND origin = 'inferred_session')
               )
             ORDER BY CASE WHEN normalized_key = ?3 THEN 0 ELSE 1 END, entry_id ASC",
        )?;
        let matches = statement
            .query_map(
                rusqlite::params![
                    scope_name(scope),
                    scope_id,
                    self.canonical_key,
                    self.legacy_inferred_key,
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let Some((keeper_id, keeper_origin)) = matches.first() else {
            return Ok(None);
        };

        for (duplicate_id, _) in matches.iter().skip(1) {
            transaction.execute(
                "UPDATE memory_entries SET replacement_entry_id = ?1
                 WHERE replacement_entry_id = ?2",
                rusqlite::params![keeper_id, duplicate_id],
            )?;
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
                rusqlite::params![duplicate_id, keeper_id],
            )?;
            transaction.execute(
                "UPDATE memory_evidence SET entry_id = ?1 WHERE entry_id = ?2",
                rusqlite::params![keeper_id, duplicate_id],
            )?;
            transaction.execute(
                "DELETE FROM memory_entries_fts WHERE entry_id = ?1",
                [duplicate_id],
            )?;
            transaction.execute(
                "DELETE FROM memory_entries WHERE entry_id = ?1",
                [duplicate_id],
            )?;
        }
        transaction.execute(
            "UPDATE memory_entries SET replacement_entry_id = NULL
             WHERE entry_id = replacement_entry_id",
            [],
        )?;

        Ok(Some(ExistingMemoryEntry {
            entry_id: keeper_id.clone(),
            origin: parse_origin(keeper_origin)?,
        }))
    }
}
