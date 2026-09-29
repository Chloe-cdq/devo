//! Durable proposal-group membership, independent of display text and candidate retention.
//!
//! A claim uses the approved conservative textual identity. Its optional entry
//! binding follows canonical-entry merges; an opposing claim can remain unbound
//! until an authorized explicit write selects it. Membership is not evidence.

use std::collections::BTreeMap;

use devo_protocol::native::rpc_memory::MemoryScope;
use rusqlite::{OptionalExtension, Transaction};

use super::entries::contains_secret;
use super::entry_identity::ExistingMemoryEntry;
use super::equivalence;
use super::stored_values::{parse_origin, parse_scope};
use super::{MemoryError, scope_name};

pub(super) struct ProposalClaim<'a> {
    pub(super) scope: MemoryScope,
    pub(super) scope_id: &'a str,
    pub(super) proposal_key: &'a str,
    pub(super) canonical_key: &'a str,
    pub(super) entry_id: Option<&'a str>,
}

pub(super) fn create_schema(transaction: &Transaction<'_>) -> Result<(), MemoryError> {
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS memory_proposal_claims (
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            proposal_key TEXT NOT NULL,
            canonical_key TEXT NOT NULL,
            entry_id TEXT,
            PRIMARY KEY(scope_type, scope_id, proposal_key, canonical_key),
            FOREIGN KEY(entry_id) REFERENCES memory_entries(entry_id) ON DELETE SET NULL
         );
         CREATE INDEX IF NOT EXISTS memory_proposal_claims_identity
            ON memory_proposal_claims(scope_type, scope_id, canonical_key);
         CREATE INDEX IF NOT EXISTS memory_proposal_claims_entry ON memory_proposal_claims(entry_id);",
    )?;
    Ok(())
}

pub(super) fn record_claim(
    transaction: &Transaction<'_>,
    claim: ProposalClaim<'_>,
) -> Result<(), MemoryError> {
    transaction.execute(
        "INSERT INTO memory_proposal_claims
            (scope_type, scope_id, proposal_key, canonical_key, entry_id)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(scope_type, scope_id, proposal_key, canonical_key)
         DO UPDATE SET entry_id = COALESCE(excluded.entry_id, memory_proposal_claims.entry_id)",
        rusqlite::params![
            scope_name(claim.scope),
            claim.scope_id,
            claim.proposal_key,
            claim.canonical_key,
            claim.entry_id
        ],
    )?;
    withhold_competing_inferred(transaction, claim.scope, claim.scope_id, claim.proposal_key)
}

pub(super) fn competitor(
    transaction: &Transaction<'_>,
    scope: MemoryScope,
    scope_id: &str,
    proposal_key: &str,
) -> Result<Option<ExistingMemoryEntry>, MemoryError> {
    let entry = transaction
        .query_row(
            "SELECT DISTINCT entry.entry_id, entry.origin FROM memory_proposal_claims AS claim
         JOIN memory_entries AS entry ON entry.entry_id = claim.entry_id
         WHERE claim.scope_type = ?1 AND claim.scope_id = ?2 AND claim.proposal_key = ?3
            AND entry.scope_type = ?1 AND entry.scope_id = ?2
            AND entry.state IN ('active', 'restored', 'conflicted')
         ORDER BY CASE entry.origin WHEN 'explicit_user' THEN 0 ELSE 1 END, entry.entry_id
         LIMIT 1",
            rusqlite::params![scope_name(scope), scope_id, proposal_key],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    entry
        .map(|(entry_id, origin)| {
            Ok(ExistingMemoryEntry {
                entry_id,
                origin: parse_origin(&origin)?,
            })
        })
        .transpose()
}

pub(super) fn bind_explicit_entry(
    transaction: &Transaction<'_>,
    scope: MemoryScope,
    scope_id: &str,
    canonical_key: &str,
    entry_id: &str,
) -> Result<(), MemoryError> {
    transaction.execute(
        "UPDATE memory_proposal_claims SET entry_id = ?1
         WHERE scope_type = ?2 AND scope_id = ?3 AND canonical_key = ?4",
        rusqlite::params![entry_id, scope_name(scope), scope_id, canonical_key],
    )?;
    let keys = {
        let mut statement = transaction.prepare(
            "SELECT proposal_key FROM memory_proposal_claims
             WHERE scope_type = ?1 AND scope_id = ?2 AND canonical_key = ?3",
        )?;
        statement
            .query_map(
                rusqlite::params![scope_name(scope), scope_id, canonical_key],
                |row| row.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?
    };
    for key in keys {
        withhold_competing_inferred(transaction, scope, scope_id, &key)?;
    }
    Ok(())
}

fn withhold_competing_inferred(
    transaction: &Transaction<'_>,
    scope: MemoryScope,
    scope_id: &str,
    proposal_key: &str,
) -> Result<(), MemoryError> {
    // Withholding repairs authority/recall state without changing claim bodies,
    // evidence, or their accepted-content timestamps.
    transaction.execute(
        "UPDATE memory_entries SET state = 'conflicted'
         WHERE scope_type = ?1 AND scope_id = ?2 AND origin = 'inferred_session'
            AND state IN ('active', 'restored')
            AND entry_id IN (SELECT entry_id FROM memory_proposal_claims
                WHERE scope_type = ?1 AND scope_id = ?2 AND proposal_key = ?3)
            AND (SELECT COUNT(DISTINCT entry.entry_id)
                FROM memory_proposal_claims AS claim
                JOIN memory_entries AS entry ON entry.entry_id = claim.entry_id
                WHERE claim.scope_type = ?1 AND claim.scope_id = ?2 AND claim.proposal_key = ?3
                    AND entry.scope_type = ?1 AND entry.scope_id = ?2
                    AND entry.state IN ('active', 'restored', 'conflicted')) > 1",
        rusqlite::params![scope_name(scope), scope_id, proposal_key],
    )?;
    transaction.execute(
        "DELETE FROM memory_entries_fts WHERE entry_id IN (
            SELECT entry.entry_id FROM memory_proposal_claims AS claim
            JOIN memory_entries AS entry ON entry.entry_id = claim.entry_id
            WHERE claim.scope_type = ?1 AND claim.scope_id = ?2 AND claim.proposal_key = ?3
                AND entry.scope_type = ?1 AND entry.scope_id = ?2 AND entry.state = 'conflicted')",
        rusqlite::params![scope_name(scope), scope_id, proposal_key],
    )?;
    Ok(())
}

pub(super) fn backfill_claims(transaction: &Transaction<'_>) -> Result<(), MemoryError> {
    let entries = {
        let mut statement = transaction
            .prepare("SELECT scope_type, scope_id, body, entry_id FROM memory_entries")?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut identities = BTreeMap::<(String, String, String), Vec<String>>::new();
    for (scope, scope_id, body, entry_id) in entries {
        identities
            .entry((scope, scope_id, equivalence::explicit_memory_key(&body)))
            .or_default()
            .push(entry_id);
    }
    let candidates = {
        let mut statement = transaction.prepare(
            "SELECT scope_type, scope_id, normalized_key, body FROM memory_candidates
             WHERE validation_outcome IN ('accepted', 'conflicted', 'explicit_authority')",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut ambiguous_claims = 0;
    for (scope, scope_id, proposal_key, body) in candidates {
        if contains_secret(&body) || contains_secret(&proposal_key) {
            continue;
        }
        let canonical_key = equivalence::explicit_memory_key(&body);
        let matches = identities.get(&(scope.clone(), scope_id.clone(), canonical_key.clone()));
        if matches.is_some_and(|entries| entries.len() > 1) {
            ambiguous_claims += 1;
        }
        // An ambiguous historical identity remains unbound; never guess or merge
        // incompatible entries to reconstruct a proposal relation.
        let entry_id = matches
            .filter(|entries| entries.len() == 1)
            .map(|entries| entries[0].as_str());
        record_claim(
            transaction,
            ProposalClaim {
                scope: parse_scope(&scope)?,
                scope_id: &scope_id,
                proposal_key: &proposal_key,
                canonical_key: &canonical_key,
                entry_id,
            },
        )?;
    }
    if ambiguous_claims > 0 {
        tracing::warn!(
            ambiguous_claims,
            "memory proposal migration retained ambiguous claim bindings for explicit resolution"
        );
    }
    Ok(())
}
