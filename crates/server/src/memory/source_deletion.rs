//! Atomic source cleanup and optional related-memory revocation.
use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use devo_protocol::SessionId;
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_protocol::native::rpc_session::RelatedMemoryDeletion;
use rusqlite::TransactionBehavior;

use super::entries::load_entry;
use super::proposal_reconciliation;
use super::stored_values::parse_scope;
use super::{MemoryError, MemoryRuntime};

pub(super) fn delete_source_records(
    connection: &mut rusqlite::Connection,
    sources: &[SessionId],
    now: DateTime<Utc>,
    expiry_cutoff: DateTime<Utc>,
    related_memory: RelatedMemoryDeletion,
) -> Result<Vec<MemoryEntry>, MemoryError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut forgotten_ids = BTreeSet::new();
    for source in sources {
        let source_id = source.to_string();
        transaction.execute(
            "INSERT INTO memory_deleted_sources(source_session_id, deleted_at)
         VALUES (?1, ?2) ON CONFLICT(source_session_id) DO NOTHING",
            rusqlite::params![source_id, now.to_rfc3339()],
        )?;
        let affected_entries = {
            let mut statement = transaction.prepare(
            "SELECT DISTINCT entry.entry_id, entry.scope_type, entry.scope_id, entry.normalized_key
             FROM memory_entries AS entry
             JOIN memory_evidence AS evidence ON evidence.entry_id = entry.entry_id
             WHERE evidence.session_id = ?1
             UNION
             SELECT entry.entry_id, entry.scope_type, entry.scope_id, entry.normalized_key
             FROM memory_entries AS entry
             JOIN memory_deleted_source_entries AS retry ON retry.entry_id = entry.entry_id
             WHERE retry.source_session_id = ?1",
        )?;
            statement
                .query_map([&source_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        // Keep identities until session removal succeeds, so retries can still
        // select related revocation after canonical evidence has been erased.
        for (entry_id, _, _, _) in &affected_entries {
            transaction.execute(
                "INSERT OR IGNORE INTO memory_deleted_source_entries
                 (source_session_id, entry_id) VALUES (?1, ?2)",
                rusqlite::params![source_id, entry_id],
            )?;
        }
        let affected_groups = {
            let mut statement = transaction.prepare(
                "SELECT DISTINCT scope_type, scope_id, proposal_key
             FROM memory_proposal_claim_sources WHERE source_session_id = ?1",
            )?;
            statement
                .query_map([&source_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut recheck_entries = affected_entries
            .iter()
            .map(|(entry_id, _, _, _)| entry_id.clone())
            .collect::<BTreeSet<_>>();
        for (scope_type, scope_id, proposal_key) in &affected_groups {
            recheck_entries.extend(proposal_reconciliation::bound_inferred_entries_for_group(
                &transaction,
                scope_type,
                scope_id,
                proposal_key,
            )?);
        }
        if related_memory == RelatedMemoryDeletion::Forget {
            for (entry_id, scope_type, scope_id, normalized_key) in &affected_entries {
                super::revocation_lifecycle::revoke_entry(
                    &transaction,
                    entry_id,
                    scope_type,
                    scope_id,
                    normalized_key,
                    &now.to_rfc3339(),
                )?;
                forgotten_ids.insert(entry_id.clone());
            }
        }
        transaction.execute(
            "DELETE FROM memory_proposal_claim_sources WHERE source_session_id = ?1",
            [&source_id],
        )?;
        transaction.execute(
            "DELETE FROM memory_proposal_claims
         WHERE legacy_unattributed = 0
           AND NOT EXISTS (
             SELECT 1 FROM memory_proposal_claims AS anchored
             JOIN memory_entries AS authority ON authority.entry_id = anchored.entry_id
             WHERE anchored.scope_type = memory_proposal_claims.scope_type
               AND anchored.scope_id = memory_proposal_claims.scope_id
               AND anchored.proposal_key = memory_proposal_claims.proposal_key
               AND authority.scope_type = anchored.scope_type AND authority.scope_id = anchored.scope_id
               AND authority.origin = 'explicit_user' AND authority.state IN ('active', 'restored'))
           AND NOT EXISTS (
             SELECT 1 FROM memory_proposal_claim_sources AS support
             WHERE support.scope_type = memory_proposal_claims.scope_type
               AND support.scope_id = memory_proposal_claims.scope_id
               AND support.proposal_key = memory_proposal_claims.proposal_key
               AND support.canonical_key = memory_proposal_claims.canonical_key)",
            [],
        )?;
        transaction.execute(
            "DELETE FROM memory_candidates WHERE source_session_id = ?1",
            [&source_id],
        )?;
        transaction.execute(
            "DELETE FROM memory_evidence WHERE session_id = ?1",
            [&source_id],
        )?;
        transaction.execute(
            "DELETE FROM memory_jobs WHERE source_session_id = ?1",
            [&source_id],
        )?;
        transaction.execute(
            "DELETE FROM memory_job_receipts WHERE source_session_id = ?1",
            [&source_id],
        )?;
        for (entry_id, scope_type, scope_id, _normalized_key) in affected_entries {
            transaction.execute(
                "INSERT OR IGNORE INTO memory_deleted_source_scopes
             (source_session_id, scope_type, scope_id) VALUES (?1, ?2, ?3)",
                rusqlite::params![source_id, scope_type, scope_id],
            )?;
            transaction.execute(
                "UPDATE memory_entries SET state = 'retired', updated_at = ?1
             WHERE entry_id = ?2 AND origin = 'inferred_session'
               AND NOT EXISTS (
                   SELECT 1 FROM memory_evidence WHERE entry_id = ?2
               )",
                rusqlite::params![now.to_rfc3339(), entry_id],
            )?;
            transaction.execute(
                "DELETE FROM memory_entries_fts WHERE entry_id = ?1 AND EXISTS (
                SELECT 1 FROM memory_entries
                WHERE entry_id = ?1 AND state = 'retired'
            )",
                [&entry_id],
            )?;
        }
        for (scope_type, scope_id, _proposal_key) in affected_groups {
            transaction.execute(
                "INSERT OR IGNORE INTO memory_deleted_source_scopes
             (source_session_id, scope_type, scope_id) VALUES (?1, ?2, ?3)",
                rusqlite::params![source_id, scope_type, scope_id],
            )?;
        }
        for entry_id in recheck_entries {
            proposal_reconciliation::reconcile_entry_after_source_change(
                &transaction,
                &entry_id,
                expiry_cutoff,
            )?;
        }
    }
    let forgotten = forgotten_ids
        .into_iter()
        .map(|entry_id| {
            load_entry(&transaction, &MemoryEntryId::from_string(entry_id))?.ok_or_else(|| {
                MemoryError::InvalidStoredValue("forgotten source entry is missing".into())
            })
        })
        .collect::<Result<Vec<_>, MemoryError>>()?;
    transaction.commit()?;
    Ok(forgotten)
}

impl MemoryRuntime {
    /// Reconcile source records and their derived projection on a background worker.
    pub(crate) fn delete_sources(
        &self,
        sources: &[SessionId],
        now: DateTime<Utc>,
        related_memory: RelatedMemoryDeletion,
    ) -> Result<Vec<MemoryEntry>, MemoryError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let forgotten = delete_source_records(
            &mut connection,
            sources,
            now,
            self.inferred_expiry_cutoff(now),
            related_memory,
        )?;
        if let Err(projection_error) = self.refresh_deleted_source_projections(&connection, sources)
        {
            return Err(MemoryError::SourceDeletionCommitted {
                forgotten,
                projection_error: Box::new(projection_error),
            });
        }
        Ok(forgotten)
    }

    pub(super) fn refresh_deleted_source_projections(
        &self,
        connection: &rusqlite::Connection,
        sources: &[SessionId],
    ) -> Result<(), MemoryError> {
        let mut scopes = BTreeSet::new();
        for source in sources {
            let pending = {
                let mut statement = connection.prepare(
                    "SELECT scope_type, scope_id FROM memory_deleted_source_scopes
                     WHERE source_session_id = ?1",
                )?;
                statement
                    .query_map([source.to_string()], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            };
            scopes.extend(pending);
        }
        for (scope_type, scope_id) in scopes {
            self.refresh_projection(connection, parse_scope(&scope_type)?, &scope_id)?;
        }
        for source in sources {
            connection.execute(
                "DELETE FROM memory_deleted_source_scopes WHERE source_session_id = ?1",
                [source.to_string()],
            )?;
        }
        Ok(())
    }
}
