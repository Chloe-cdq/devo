use std::collections::BTreeSet;

use rusqlite::Transaction;

use super::MemoryError;

pub(super) fn bound_inferred_entries_for_group(
    transaction: &Transaction<'_>,
    scope_type: &str,
    scope_id: &str,
    proposal_key: &str,
) -> Result<BTreeSet<String>, MemoryError> {
    let mut statement = transaction.prepare(
        "SELECT DISTINCT entry.entry_id
         FROM memory_proposal_claims AS claim
         JOIN memory_entries AS entry ON entry.entry_id = claim.entry_id
         WHERE claim.scope_type = ?1 AND claim.scope_id = ?2
           AND claim.proposal_key = ?3 AND entry.origin = 'inferred_session'
           AND entry.state = 'conflicted'",
    )?;
    Ok(statement
        .query_map(
            rusqlite::params![scope_type, scope_id, proposal_key],
            |row| row.get::<_, String>(0),
        )?
        .collect::<Result<BTreeSet<_>, _>>()?)
}

pub(super) fn reconcile_entry_after_source_change(
    transaction: &Transaction<'_>,
    entry_id: &str,
) -> Result<(), MemoryError> {
    let has_live_claim: bool = transaction.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM memory_live_proposal_claims AS claim
            WHERE claim.entry_id = ?1
            )",
        [entry_id],
        |row| row.get(0),
    )?;
    if !has_live_claim {
        return Ok(());
    }

    let contested: bool = transaction.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM memory_contested_proposal_claims WHERE entry_id = ?1
        )",
        [entry_id],
        |row| row.get(0),
    )?;
    if !contested {
        transaction.execute(
            "UPDATE memory_entries SET state = 'active'
             WHERE entry_id = ?1 AND origin = 'inferred_session' AND state = 'conflicted'
               AND EXISTS(SELECT 1 FROM memory_evidence WHERE entry_id = ?1)",
            [entry_id],
        )?;
        transaction.execute(
            "INSERT INTO memory_entries_fts(entry_id, normalized_key, body)
             SELECT entry_id, normalized_key, body FROM memory_entries
             WHERE entry_id = ?1 AND origin = 'inferred_session' AND state = 'active'
               AND NOT EXISTS(SELECT 1 FROM memory_entries_fts WHERE entry_id = ?1)",
            [entry_id],
        )?;
    }
    Ok(())
}
