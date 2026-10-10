use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

use chrono::{DateTime, Duration, Utc};
use devo_protocol::SessionId;
use rusqlite::TransactionBehavior;

use super::proposal_reconciliation;
use super::stored_values::parse_scope;
use super::{MemoryError, MemoryRuntime};

impl MemoryRuntime {
    /// A post-commit projection failure cannot revoke the durable exclusion.
    pub(crate) fn fence_external_context_sources(
        &self,
        sources: &[SessionId],
    ) -> Result<(), MemoryError> {
        let Err(error) = self.exclude_sources(sources, Utc::now()) else {
            return Ok(());
        };
        self.note_source_provenance_storage_failure();
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        for source in sources {
            let excluded: bool = connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM memory_excluded_sources WHERE source_session_id = ?1)",
                [source.to_string()],
                |row| row.get(0),
            )?;
            if !excluded {
                return Err(error);
            }
        }
        Ok(())
    }

    /// Applies durable session-index intents to memory storage. Failed intents
    /// remain in the index for the next startup or scan.
    pub(crate) fn reconcile_source_intents(&self) {
        self.reconcile_external_context_sources();
        if let Some(db) = self.deletion_ledger.as_ref() {
            match db.pending_memory_source_deletions() {
                Ok(pending) if !pending.is_empty() => {
                    let committed = match self.delete_sources(
                        &pending,
                        Utc::now(),
                        devo_protocol::native::rpc_session::RelatedMemoryDeletion::Preserve,
                    ) {
                        Ok(_) => true,
                        Err(MemoryError::SourceDeletionCommitted { .. }) => {
                            self.storage_failed.store(true, Ordering::Relaxed);
                            tracing::warn!("memory source projection refresh remains pending");
                            true
                        }
                        Err(_) => {
                            self.storage_failed.store(true, Ordering::Relaxed);
                            tracing::warn!(
                                error_class = "storage_error",
                                "memory source deletion remains pending"
                            );
                            false
                        }
                    };
                    if committed {
                        let mut completed = Vec::new();
                        for source in pending {
                            match db.get_session(&source) {
                                Ok(None) => completed.push(source),
                                Ok(Some(_)) => {}
                                Err(_) => {
                                    self.storage_failed.store(true, Ordering::Relaxed);
                                    tracing::warn!(error_class = "storage_error", %source, "failed to inspect deleted session")
                                }
                            }
                        }
                        if db.finish_memory_source_deletions(&completed).is_err() {
                            self.storage_failed.store(true, Ordering::Relaxed);
                            tracing::warn!(
                                error_class = "storage_error",
                                "failed to finish memory source deletion ledger"
                            );
                        }
                    }
                }
                Ok(_) => {}
                Err(_) => {
                    self.storage_failed.store(true, Ordering::Relaxed);
                    tracing::warn!(
                        error_class = "storage_error",
                        "failed to read memory source deletion ledger"
                    )
                }
            }
        }
        if let Some(db) = self.deletion_ledger.as_ref() {
            let release_retry_entries = (|| -> Result<(), MemoryError> {
                let sources = {
                    let connection = self
                        .connection
                        .lock()
                        .map_err(|_| MemoryError::LockPoisoned)?;
                    let mut statement = connection.prepare(
                        "SELECT DISTINCT source_session_id FROM memory_deleted_source_entries",
                    )?;
                    statement
                        .query_map([], |row| row.get::<_, String>(0))?
                        .collect::<Result<Vec<_>, _>>()?
                };
                let mut completed = Vec::new();
                for source in sources {
                    let source_id = SessionId::try_from(source.as_str())
                        .map_err(|error| MemoryError::InvalidStoredValue(error.to_string()))?;
                    match db.get_session(&source_id) {
                        Ok(None) => completed.push(source),
                        Ok(Some(_)) => {}
                        Err(_) => {
                            self.storage_failed.store(true, Ordering::Relaxed);
                            tracing::warn!(error_class = "storage_error", %source, "failed to inspect deleted session")
                        }
                    }
                }
                let mut connection = self
                    .connection
                    .lock()
                    .map_err(|_| MemoryError::LockPoisoned)?;
                let transaction = connection.transaction()?;
                for source in completed {
                    transaction.execute(
                        "DELETE FROM memory_deleted_source_entries WHERE source_session_id = ?1",
                        [source],
                    )?;
                }
                transaction.commit()?;
                Ok(())
            })();
            if release_retry_entries.is_err() {
                self.storage_failed.store(true, Ordering::Relaxed);
                tracing::warn!(
                    error_class = "storage_error",
                    "memory source retry identity cleanup remains pending"
                );
            }
        }
        // Projection repair owns its durable scopes after canonical cleanup has
        // released the source fence. It must not hide surviving inferred entries.
        let Ok(connection) = self.connection.lock() else {
            return;
        };
        let repair = (|| -> Result<(), MemoryError> {
            let sources = {
                let mut statement = connection.prepare(
                    "SELECT DISTINCT source_session_id FROM memory_deleted_source_scopes",
                )?;
                statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            let sources = sources
                .into_iter()
                .map(|source| {
                    SessionId::try_from(source.as_str())
                        .map_err(|error| MemoryError::InvalidStoredValue(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.refresh_deleted_source_projections(&connection, &sources)
        })();
        if repair.is_err() {
            self.storage_failed.store(true, Ordering::Relaxed);
            tracing::warn!(
                error_class = "projection_error",
                "memory source projection repair remains pending"
            );
        }
    }

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
            proposal_reconciliation::reconcile_entry_after_source_change(
                &transaction,
                &entry_id,
                self.inferred_expiry_cutoff(now),
            )?;
        }
        transaction.commit()?;
        self.excluded_external_sources
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?
            .extend(sources.iter().map(ToString::to_string));
        for (scope_type, scope_id) in scopes {
            self.refresh_projection(&connection, parse_scope(&scope_type)?, &scope_id)?;
        }
        Ok(())
    }

    /// Prunes short-lived detail while retaining a minimal idempotency receipt.
    pub(crate) fn prune_expired(&self, now: DateTime<Utc>) -> Result<(), MemoryError> {
        // Projection failure during ageing must not retain expired raw detail.
        let lifecycle_result = self.expire_inferred(now);
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
            "INSERT OR IGNORE INTO memory_job_receipts(source_session_id, source_watermark, completed_at, job_kind)
             SELECT source_session_id, source_watermark, updated_at, job_kind FROM memory_jobs
             WHERE state = 'completed' AND julianday(updated_at) <= julianday(?1)",
            [cutoff.to_rfc3339()],
        )?;
        transaction.execute(
            "DELETE FROM memory_jobs
             WHERE state = 'completed' AND julianday(updated_at) <= julianday(?1)",
            [cutoff.to_rfc3339()],
        )?;
        let scopes = {
            let mut statement = transaction.prepare(
                "SELECT DISTINCT scope_type, scope_id FROM memory_candidates
                 WHERE julianday(retention_until) <= julianday(?1)",
            )?;
            statement
                .query_map([now.to_rfc3339()], |row| {
                    Ok((
                        row.get::<_, String>(/*idx*/ 0)?,
                        row.get::<_, String>(/*idx*/ 1)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        transaction.execute(
            "DELETE FROM memory_candidates
             WHERE julianday(retention_until) <= julianday(?1)",
            [now.to_rfc3339()],
        )?;
        transaction.commit()?;
        for (scope, scope_id) in scopes {
            self.refresh_projection(&connection, parse_scope(&scope)?, &scope_id)?;
        }
        lifecycle_result
    }
}
