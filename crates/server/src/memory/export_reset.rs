//! Scoped portable inspection and transactional reset of canonical memory.

use devo_protocol::native::rpc_memory::{
    MemoryExportResult, MemoryResetResult, MemoryScope, MemoryScopeLifecycle,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use super::entries::load_scope_entries;
use super::projection::render_projection;
use super::stored_values::parse_timestamp;
use super::{MemoryError, MemoryRuntime, ScopedMemoryRequest, scope_name};

impl MemoryRuntime {
    pub(super) fn export(
        &self,
        request: ScopedMemoryRequest,
    ) -> Result<MemoryExportResult, MemoryError> {
        let scope_id = self.scope_id(request.scope, &request.workspace_root)?;
        self.expire_inferred((self.clock)())?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        // Check under the storage lock so cleanup cannot commit and clear its
        // intent between this check and reading canonical source provenance.
        if self.has_pending_source_deletions() {
            return Err(MemoryError::StorageBusy);
        }
        let transaction = connection.unchecked_transaction()?;
        let result = self.render_scope_export(&transaction, request.scope, &scope_id)?;
        transaction.commit()?;
        Ok(result)
    }

    /// Also used to regenerate the read-only projection from canonical records.
    pub(super) fn render_scope_export(
        &self,
        connection: &Connection,
        scope: MemoryScope,
        scope_id: &str,
    ) -> Result<MemoryExportResult, MemoryError> {
        let entries = load_scope_entries(connection, scope, scope_id)?;
        let mut markdown = render_projection(scope, &entries);
        super::competing_projection::append_claims(connection, scope, scope_id, &mut markdown)?;
        let state = connection
            .query_row(
                "SELECT ignore_sources_before, last_rebuild_at FROM memory_scope_state
             WHERE scope_type = ?1 AND scope_id = ?2",
                rusqlite::params![scope_name(scope), scope_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(/*idx*/ 0)?,
                        row.get::<_, Option<String>>(/*idx*/ 1)?,
                    ))
                },
            )
            .optional()?
            .unwrap_or_default();
        let lifecycle = MemoryScopeLifecycle {
            ignore_sources_before: state.0.map(|value| parse_timestamp(&value)).transpose()?,
            last_rebuild_at: state.1.map(|value| parse_timestamp(&value)).transpose()?,
            revocation_count: connection.query_row(
                "SELECT COUNT(*) FROM memory_revocations WHERE scope_type = ?1 AND scope_id = ?2",
                rusqlite::params![scope_name(scope), scope_id],
                |row| row.get(/*idx*/ 0),
            )?,
        };
        if lifecycle.ignore_sources_before.is_some()
            || lifecycle.last_rebuild_at.is_some()
            || lifecycle.revocation_count > 0
        {
            markdown.push_str("\n## Lifecycle\n");
            if let Some(cutoff) = lifecycle.ignore_sources_before {
                markdown.push_str(&format!(
                    "\n- ignore_sources_before: {}\n",
                    cutoff.to_rfc3339()
                ));
            }
            if let Some(rebuild) = lifecycle.last_rebuild_at {
                markdown.push_str(&format!("\n- last_rebuild_at: {}\n", rebuild.to_rfc3339()));
            }
            markdown.push_str(&format!(
                "\n- revocation_count: {}\n",
                lifecycle.revocation_count
            ));
        }
        Ok(MemoryExportResult {
            scope,
            markdown,
            lifecycle,
        })
    }

    pub(super) fn reset(
        &self,
        request: ScopedMemoryRequest,
    ) -> Result<MemoryResetResult, MemoryError> {
        let scope_id = self.scope_id(request.scope, &request.workspace_root)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous = transaction.query_row(
            "SELECT ignore_sources_before FROM memory_scope_state WHERE scope_type = ?1 AND scope_id = ?2",
            rusqlite::params![scope_name(request.scope), scope_id],
            |row| row.get::<_, Option<String>>(/*idx*/ 0),
        ).optional()?.flatten().map(|value| parse_timestamp(&value)).transpose()?;
        // Compute the fence after acquiring the write transaction, and never lower it.
        let now = (self.clock)();
        let cutoff = previous.map_or(now, |previous| previous.max(now));
        transaction.execute(
            "INSERT INTO memory_scope_state(scope_type, scope_id, projection_revision, ignore_sources_before)
             VALUES (?1, ?2, 1, ?3) ON CONFLICT(scope_type, scope_id) DO UPDATE SET
                projection_revision = projection_revision + 1, ignore_sources_before = excluded.ignore_sources_before",
            rusqlite::params![scope_name(request.scope), scope_id, cutoff.to_rfc3339()],
        )?;
        let revoked_inference = {
            let mut statement = transaction.prepare(
                "SELECT e.normalized_key, e.body FROM memory_entries e
                 JOIN memory_revocations r ON r.scope_type = e.scope_type AND r.scope_id = e.scope_id
                    AND r.normalized_key = e.normalized_key
                 WHERE e.scope_type = ?1 AND e.scope_id = ?2 AND e.origin = 'inferred_session'",
            )?;
            statement
                .query_map(
                    rusqlite::params![scope_name(request.scope), scope_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )?
                .collect::<Result<Vec<_>, _>>()?
        };
        for (stored_key, body) in revoked_inference {
            let identity = super::entry_identity::MemoryEntryIdentity::from_body(&body);
            if stored_key == identity.legacy_inferred_key && stored_key != identity.canonical_key {
                super::revocation_lifecycle::canonicalize_revocation_identity(
                    &transaction,
                    scope_name(request.scope),
                    &scope_id,
                    &identity.canonical_key,
                    &stored_key,
                )?;
            }
        }
        transaction.execute(
            "DELETE FROM memory_entries_fts WHERE entry_id IN (
                SELECT entry_id FROM memory_entries WHERE scope_type = ?1 AND scope_id = ?2)",
            rusqlite::params![scope_name(request.scope), scope_id],
        )?;
        transaction.execute(
            "DELETE FROM memory_evidence WHERE entry_id IN (
                SELECT entry_id FROM memory_entries WHERE scope_type = ?1 AND scope_id = ?2)",
            rusqlite::params![scope_name(request.scope), scope_id],
        )?;
        let cleared_candidate_count = transaction.execute(
            "DELETE FROM memory_candidates WHERE scope_type = ?1 AND scope_id = ?2",
            rusqlite::params![scope_name(request.scope), scope_id],
        )? as u64;
        for table in [
            "memory_proposal_claim_sources",
            "memory_proposal_claims",
            "memory_deleted_source_scopes",
        ] {
            transaction.execute(
                &format!("DELETE FROM {table} WHERE scope_type = ?1 AND scope_id = ?2"),
                rusqlite::params![scope_name(request.scope), scope_id],
            )?;
        }
        let cleared_entry_count = transaction.execute(
            "DELETE FROM memory_entries WHERE scope_type = ?1 AND scope_id = ?2",
            rusqlite::params![scope_name(request.scope), scope_id],
        )? as u64;
        transaction.execute(
            "UPDATE memory_jobs SET state = 'cancelled', lease_owner = NULL, lease_until = NULL
             WHERE rebuild_id IN (SELECT rebuild_id FROM memory_rebuild_requests
                 WHERE scope_type = ?1 AND scope_id = ?2) AND state IN ('pending', 'running', 'retrying')",
            rusqlite::params![scope_name(request.scope), scope_id],
        )?;
        transaction.execute(
            "UPDATE memory_rebuild_requests SET state = 'cancelled' WHERE scope_type = ?1 AND scope_id = ?2",
            rusqlite::params![scope_name(request.scope), scope_id],
        )?;
        transaction.commit()?;
        let result = MemoryResetResult {
            scope: request.scope,
            cleared_entry_count,
            cleared_candidate_count,
            ignore_sources_before: cutoff,
        };
        if let Err(projection_error) =
            self.refresh_projection(&connection, request.scope, &scope_id)
        {
            return Err(MemoryError::ResetCommitted {
                result: Box::new(result),
                projection_error: Box::new(projection_error),
            });
        }
        Ok(result)
    }
}
