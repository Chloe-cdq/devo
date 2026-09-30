use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use devo_protocol::SessionId;
use rusqlite::TransactionBehavior;

use super::proposal_reconciliation;
use super::stored_values::parse_scope;
use super::{MemoryError, MemoryRuntime};

impl MemoryRuntime {
    /// Excludes externally informed sessions without erasing their audit evidence.
    pub(crate) fn exclude_sources(
        &self,
        sources: &[SessionId],
        now: DateTime<Utc>,
    ) -> Result<(), MemoryError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut scopes = BTreeSet::new();
        let mut groups = BTreeSet::new();
        let mut recheck_entries = BTreeSet::new();
        for source in sources {
            let source_id = source.to_string();
            transaction.execute(
                "INSERT OR IGNORE INTO memory_excluded_sources(source_session_id, excluded_at)
                 VALUES (?1, ?2)",
                rusqlite::params![source_id, now.to_rfc3339()],
            )?;
            let mut statement = transaction.prepare(
                "SELECT DISTINCT entry.entry_id, entry.scope_type, entry.scope_id
                 FROM memory_entries AS entry
                 JOIN memory_evidence AS evidence ON evidence.entry_id = entry.entry_id
                 WHERE evidence.session_id = ?1",
            )?;
            let affected_entries = statement
                .query_map([&source_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            drop(statement);
            for (entry_id, scope_type, scope_id) in affected_entries {
                recheck_entries.insert(entry_id.clone());
                scopes.insert((scope_type, scope_id));
                transaction.execute(
                    "UPDATE memory_entries SET state = 'retired'
                     WHERE entry_id = ?1 AND origin = 'inferred_session'
                       AND NOT EXISTS (
                         SELECT 1 FROM memory_evidence AS evidence
                         WHERE evidence.entry_id = ?1
                           AND NOT EXISTS (SELECT 1 FROM memory_excluded_sources AS excluded
                             WHERE excluded.source_session_id = evidence.session_id))",
                    [&entry_id],
                )?;
                transaction.execute(
                    "DELETE FROM memory_entries_fts WHERE entry_id = ?1
                     AND EXISTS (SELECT 1 FROM memory_entries
                       WHERE entry_id = ?1 AND state = 'retired')",
                    [&entry_id],
                )?;
            }
            let mut statement = transaction.prepare(
                "SELECT DISTINCT scope_type, scope_id, proposal_key
                 FROM memory_proposal_claim_sources WHERE source_session_id = ?1",
            )?;
            let affected_groups = statement
                .query_map([&source_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            drop(statement);
            for (scope_type, scope_id, proposal_key) in affected_groups {
                scopes.insert((scope_type.clone(), scope_id.clone()));
                groups.insert((scope_type, scope_id, proposal_key));
            }
        }
        for (scope_type, scope_id, proposal_key) in groups {
            recheck_entries.extend(proposal_reconciliation::bound_inferred_entries_for_group(
                &transaction,
                &scope_type,
                &scope_id,
                &proposal_key,
            )?);
        }
        for entry_id in recheck_entries {
            proposal_reconciliation::reconcile_entry_after_source_change(&transaction, &entry_id)?;
        }
        transaction.commit()?;
        for (scope_type, scope_id) in scopes {
            self.refresh_projection(&connection, parse_scope(&scope_type)?, &scope_id)?;
        }
        Ok(())
    }

    /// Permanently fences deleted sources and removes their contribution.
    pub(crate) fn delete_sources(
        &self,
        sources: &[SessionId],
        now: DateTime<Utc>,
    ) -> Result<(), MemoryError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for source in sources {
            let source_id = source.to_string();
            transaction.execute(
                "INSERT INTO memory_deleted_sources(source_session_id, deleted_at)
                 VALUES (?1, ?2) ON CONFLICT(source_session_id) DO NOTHING",
                rusqlite::params![source_id, now.to_rfc3339()],
            )?;
            let affected_entries = {
                let mut statement = transaction.prepare(
                    "SELECT DISTINCT entry.entry_id, entry.scope_type, entry.scope_id
                     FROM memory_entries AS entry
                     JOIN memory_evidence AS evidence ON evidence.entry_id = entry.entry_id
                     WHERE evidence.session_id = ?1",
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
                .map(|(entry_id, _, _)| entry_id.clone())
                .collect::<BTreeSet<_>>();
            for (scope_type, scope_id, proposal_key) in &affected_groups {
                recheck_entries.extend(proposal_reconciliation::bound_inferred_entries_for_group(
                    &transaction,
                    scope_type,
                    scope_id,
                    proposal_key,
                )?);
            }
            transaction.execute(
                "DELETE FROM memory_proposal_claim_sources WHERE source_session_id = ?1",
                [&source_id],
            )?;
            transaction.execute(
                "DELETE FROM memory_proposal_claims
                 WHERE legacy_unattributed = 0
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
            for (entry_id, scope_type, scope_id) in affected_entries {
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
                )?;
            }
        }
        transaction.commit()?;
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
            self.refresh_projection(&connection, parse_scope(&scope_type)?, &scope_id)?;
        }
        for source in sources {
            connection.execute(
                "DELETE FROM memory_deleted_source_scopes WHERE source_session_id = ?1",
                [source.to_string()],
            )?;
        }
        Ok(())
    }

    /// Prunes short-lived detail while retaining a minimal idempotency receipt.
    pub(crate) fn prune_expired(&self, now: DateTime<Utc>) -> Result<(), MemoryError> {
        let retention = Duration::try_days(
            self.config
                .candidate_and_job_retention_days
                .try_into()
                .unwrap_or(i64::MAX),
        )
        .unwrap_or(Duration::MAX);
        let cutoff = now
            .checked_sub_signed(retention)
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT OR IGNORE INTO memory_job_receipts(source_session_id, source_watermark, completed_at)
             SELECT source_session_id, source_watermark, updated_at FROM memory_jobs
             WHERE state = 'completed' AND julianday(updated_at) <= julianday(?1)",
            [cutoff.to_rfc3339()],
        )?;
        transaction.execute(
            "DELETE FROM memory_jobs
             WHERE state = 'completed' AND julianday(updated_at) <= julianday(?1)",
            [cutoff.to_rfc3339()],
        )?;
        transaction.execute(
            "DELETE FROM memory_candidates
             WHERE julianday(retention_until) <= julianday(?1)",
            [now.to_rfc3339()],
        )?;
        transaction.commit()?;
        Ok(())
    }
}
