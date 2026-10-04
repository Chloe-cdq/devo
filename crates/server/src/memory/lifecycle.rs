//! Deterministic memory ageing and explicit replacement transitions.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use devo_protocol::native::rpc_memory::MemoryScope;
use rusqlite::{Connection, Transaction};

use super::stored_values::parse_scope;
use super::{MemoryError, MemoryRuntime, scope_name};

impl MemoryRuntime {
    pub(super) fn inferred_expiry_cutoff(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let age = Duration::try_days(
            self.config
                .inferred_stale_after_days
                .try_into()
                .unwrap_or(i64::MAX),
        )
        .unwrap_or(Duration::MAX);
        now.checked_sub_signed(age)
            .unwrap_or(DateTime::<Utc>::MIN_UTC)
    }

    /// Successful on-demand use renews only inference that is still recallable.
    /// Inspecting inactive entries must not extend their lifetime or revive them.
    pub(super) fn record_on_demand_use(
        &self,
        connection: &Connection,
        entry_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), MemoryError> {
        connection.execute(
            "UPDATE memory_entries SET last_recalled_at = ?1
             WHERE entry_id = ?2 AND origin = 'inferred_session' AND state IN ('active', 'restored')
               AND MAX(julianday(updated_at), COALESCE(julianday(last_recalled_at), julianday(updated_at)))
                   > julianday(?3)",
            rusqlite::params![now.to_rfc3339(), entry_id, self.inferred_expiry_cutoff(now).to_rfc3339()],
        )?;
        Ok(())
    }

    /// Expire only recallable inference. Its accepted verification timestamp is
    /// `updated_at`; recalls extend its lifetime without revising that timestamp.
    pub(super) fn expire_inferred(&self, now: DateTime<Utc>) -> Result<(), MemoryError> {
        let cutoff = self.inferred_expiry_cutoff(now).to_rfc3339();
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.unchecked_transaction()?;
        let expired = {
            let mut statement = transaction.prepare(
                "SELECT entry_id, scope_type, scope_id FROM memory_entries
                 WHERE origin = 'inferred_session' AND state IN ('active', 'restored')
                   AND MAX(julianday(updated_at),
                       COALESCE(julianday(last_recalled_at), julianday(updated_at))) <= julianday(?1)",
            )?;
            statement
                .query_map([cutoff], |row| {
                    Ok((
                        row.get::<_, String>(/*idx*/ 0)?,
                        row.get::<_, String>(/*idx*/ 1)?,
                        row.get::<_, String>(/*idx*/ 2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut scopes = BTreeSet::new();
        for (entry_id, scope, scope_id) in expired {
            transaction.execute(
                "UPDATE memory_entries SET state = 'stale' WHERE entry_id = ?1",
                [&entry_id],
            )?;
            transaction.execute(
                "DELETE FROM memory_entries_fts WHERE entry_id = ?1",
                [&entry_id],
            )?;
            scopes.insert((scope, scope_id));
        }
        transaction.commit()?;
        for (scope, scope_id) in scopes {
            self.refresh_projection(&connection, parse_scope(&scope)?, &scope_id)?;
        }
        Ok(())
    }
}

/// Replace only established scoped competitors. Equivalent writes retain the
/// canonical ID; different claims retain their own evidence and replacement links.
pub(super) fn replace_competing_entries(
    transaction: &Transaction<'_>,
    scope: MemoryScope,
    scope_id: &str,
    canonical_key: &str,
    entry_id: &str,
) -> Result<(), MemoryError> {
    transaction.execute(
        "UPDATE memory_entries SET state = 'retired', replacement_entry_id = ?1
         WHERE scope_type = ?2 AND scope_id = ?3 AND entry_id != ?1
           AND state IN ('active', 'restored', 'stale', 'conflicted')
           AND entry_id IN (
             SELECT competing.entry_id FROM memory_proposal_claims AS chosen
             JOIN memory_proposal_claims AS competing
               ON competing.scope_type = chosen.scope_type AND competing.scope_id = chosen.scope_id
              AND competing.proposal_key = chosen.proposal_key
             WHERE chosen.scope_type = ?2 AND chosen.scope_id = ?3
               AND chosen.canonical_key = ?4 AND competing.canonical_key != ?4)",
        rusqlite::params![entry_id, scope_name(scope), scope_id, canonical_key],
    )?;
    transaction.execute(
        "DELETE FROM memory_entries_fts WHERE entry_id IN (
           SELECT entry_id FROM memory_entries WHERE replacement_entry_id = ?1 AND state = 'retired')",
        [entry_id],
    )?;
    Ok(())
}
