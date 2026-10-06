use devo_protocol::native::rpc_memory::{MemoryOrigin, MemoryScope};
use rusqlite::Transaction;

use super::equivalence;
use super::revocation_lifecycle::canonicalize_revocation_identity;
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

/// Storage-key occupancy is separate from proven conservative claim identity.
pub(super) enum MemoryIdentityResolution {
    Vacant,
    Existing(ExistingMemoryEntry),
    Occupied,
}

pub(super) enum IdentityResolutionMode {
    Explicit,
    Inferred,
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
        body: &str,
        mode: IdentityResolutionMode,
    ) -> Result<MemoryIdentityResolution, MemoryError> {
        let mut statement = transaction.prepare(
            "SELECT entry_id, origin, normalized_key, body
             FROM memory_entries
             WHERE scope_type = ?1 AND scope_id = ?2
               AND (
                   normalized_key = ?3
                   OR (
                       normalized_key = ?4
                       AND origin = 'inferred_session'
                       AND body = ?5
                   )
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
                    body,
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        // Historical inferred keys can be lossy. Validate every match before
        // any merge, binding redirect, evidence write or tombstone change.
        if matches.iter().any(|(_, _, _, stored_body)| {
            equivalence::explicit_memory_key(stored_body) != self.canonical_key
        }) {
            return Ok(MemoryIdentityResolution::Occupied);
        }
        let Some((keeper_id, keeper_origin, _, _)) = matches.first() else {
            return Ok(MemoryIdentityResolution::Vacant);
        };
        let proven_legacy_key = (self.canonical_key != self.legacy_inferred_key
            && matches
                .iter()
                .any(|(_, _, normalized_key, _)| normalized_key == &self.legacy_inferred_key))
        .then(|| self.legacy_inferred_key.clone());
        let redirects = merge_entry_records(
            transaction,
            keeper_id,
            matches
                .iter()
                .skip(1)
                .map(|(entry_id, _, _, _)| entry_id.as_str()),
        )?;
        apply_replacement_redirects(transaction, &redirects)?;
        if matches!(mode, IdentityResolutionMode::Explicit)
            && let Some(proven_legacy_key) = proven_legacy_key
        {
            canonicalize_revocation_identity(
                transaction,
                scope_name(scope),
                scope_id,
                &self.canonical_key,
                &proven_legacy_key,
            )?;
        }

        Ok(MemoryIdentityResolution::Existing(ExistingMemoryEntry {
            entry_id: keeper_id.clone(),
            origin: parse_origin(keeper_origin)?,
        }))
    }
}

pub(super) fn merge_entry_records<'a>(
    transaction: &Transaction<'_>,
    keeper_id: &str,
    duplicate_ids: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<(String, String)>, MemoryError> {
    let mut redirects = Vec::new();
    for duplicate_id in duplicate_ids {
        redirects.push((duplicate_id.to_owned(), keeper_id.to_owned()));
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
            "UPDATE memory_proposal_claims SET entry_id = ?1 WHERE entry_id = ?2",
            rusqlite::params![keeper_id, duplicate_id],
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO memory_deleted_source_entries(source_session_id, entry_id)
             SELECT source_session_id, ?1 FROM memory_deleted_source_entries WHERE entry_id = ?2",
            rusqlite::params![keeper_id, duplicate_id],
        )?;
        transaction.execute(
            "DELETE FROM memory_entries WHERE entry_id = ?1",
            [duplicate_id],
        )?;
    }
    if !redirects.is_empty() {
        super::proposal_relations::reconcile_entry(transaction, keeper_id)?;
    }
    Ok(redirects)
}

pub(super) fn apply_replacement_redirects(
    transaction: &Transaction<'_>,
    redirects: &[(String, String)],
) -> Result<(), MemoryError> {
    for (duplicate_id, keeper_id) in redirects {
        transaction.execute(
            "UPDATE memory_entries SET replacement_entry_id = ?1
             WHERE replacement_entry_id = ?2",
            rusqlite::params![keeper_id, duplicate_id],
        )?;
    }
    transaction.execute(
        "UPDATE memory_entries SET replacement_entry_id = NULL
         WHERE entry_id = replacement_entry_id",
        [],
    )?;
    Ok(())
}
