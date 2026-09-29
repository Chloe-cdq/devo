//! Durable proposal-group membership, independent of display text and candidate retention.
//!
//! A claim uses the approved conservative textual identity. Its optional entry
//! binding follows canonical-entry merges; an opposing claim can remain unbound
//! until an authorized explicit write selects it. Membership is not evidence.

use std::collections::BTreeMap;

use devo_protocol::native::rpc_memory::{MemoryOrigin, MemoryScope};
use rusqlite::{OptionalExtension, Transaction};

use super::entries::contains_secret;
use super::entry_identity::{ExistingMemoryEntry, MemoryIdentityResolution};
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
    if let Some(entry_id) = claim.entry_id {
        bind_entry(
            transaction,
            claim.scope,
            claim.scope_id,
            claim.canonical_key,
            entry_id,
        )
    } else {
        reconcile_identity(
            transaction,
            claim.scope,
            claim.scope_id,
            claim.canonical_key,
        )
    }
}

pub(super) enum InferredAdmission {
    New,
    Existing(ExistingMemoryEntry),
    ExplicitAuthority,
    Conflict,
    IdentityCollision,
}

/// Records all known memberships before deciding whether inference may create
/// an entry. A model label is never allowed to reset an existing relationship.
pub(super) fn admit_inferred(
    transaction: &Transaction<'_>,
    scope: MemoryScope,
    scope_id: &str,
    proposal_key: &str,
    canonical_key: &str,
    resolution: MemoryIdentityResolution,
) -> Result<InferredAdmission, MemoryError> {
    let existing = match resolution {
        MemoryIdentityResolution::Vacant => None,
        MemoryIdentityResolution::Existing(entry) => Some(entry),
        MemoryIdentityResolution::Occupied => {
            record_claim(
                transaction,
                ProposalClaim {
                    scope,
                    scope_id,
                    proposal_key,
                    canonical_key,
                    entry_id: None,
                },
            )?;
            return Ok(InferredAdmission::IdentityCollision);
        }
    };
    record_claim(
        transaction,
        ProposalClaim {
            scope,
            scope_id,
            proposal_key,
            canonical_key,
            entry_id: existing.as_ref().map(|entry| entry.entry_id.as_str()),
        },
    )?;
    if let Some(entry) = &existing
        && entry.origin == MemoryOrigin::ExplicitUser
    {
        return Ok(InferredAdmission::Existing(
            existing.expect("checked explicit entry"),
        ));
    }
    let competitor = transaction.query_row(
        "SELECT COALESCE(entry.origin, 'inferred_session') FROM memory_proposal_claims AS competing
         LEFT JOIN memory_entries AS entry ON entry.entry_id = competing.entry_id
         WHERE competing.scope_type = ?1 AND competing.scope_id = ?2
            AND competing.canonical_key != ?3
            AND competing.proposal_key IN (
                SELECT proposal_key FROM memory_proposal_claims
                WHERE scope_type = ?1 AND scope_id = ?2 AND canonical_key = ?3)
            AND (competing.entry_id IS NULL OR (entry.scope_type = ?1 AND entry.scope_id = ?2
                AND entry.state IN ('active', 'restored', 'conflicted')))
         ORDER BY CASE entry.origin WHEN 'explicit_user' THEN 0 ELSE 1 END
         LIMIT 1",
        rusqlite::params![scope_name(scope), scope_id, canonical_key],
        |row| row.get::<_, String>(0),
    ).optional()?.map(|origin| parse_origin(&origin)).transpose()?;
    if competitor == Some(MemoryOrigin::ExplicitUser) {
        return Ok(InferredAdmission::ExplicitAuthority);
    }
    if let Some(existing) = existing {
        // Inference can add supporting evidence, but cannot reactivate a
        // conflicted or retired entry. Its lifecycle state is already reconciled.
        return Ok(InferredAdmission::Existing(existing));
    }
    if competitor.is_some() {
        Ok(InferredAdmission::Conflict)
    } else {
        Ok(InferredAdmission::New)
    }
}

pub(super) fn bind_entry(
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
    reconcile_identity(transaction, scope, scope_id, canonical_key)
}

fn reconcile_identity(
    transaction: &Transaction<'_>,
    scope: MemoryScope,
    scope_id: &str,
    canonical_key: &str,
) -> Result<(), MemoryError> {
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

pub(super) fn reconcile_entry(
    transaction: &Transaction<'_>,
    entry_id: &str,
) -> Result<(), MemoryError> {
    let groups = {
        let mut statement = transaction.prepare(
            "SELECT DISTINCT scope_type, scope_id, proposal_key FROM memory_proposal_claims WHERE entry_id = ?1",
        )?;
        statement
            .query_map([entry_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (scope, scope_id, key) in groups {
        withhold_competing_inferred(transaction, parse_scope(&scope)?, &scope_id, &key)?;
    }
    Ok(())
}

fn withhold_competing_inferred(
    transaction: &Transaction<'_>,
    scope: MemoryScope,
    scope_id: &str,
    proposal_key: &str,
) -> Result<(), MemoryError> {
    // An ambiguous binding stays unbound, but every scoped body proven to be
    // the contested canonical claim must be withheld. Accepted content,
    // evidence, keys, and timestamps are unchanged.
    let contested_keys = {
        let mut statement = transaction.prepare(
            "SELECT DISTINCT owned.canonical_key FROM memory_proposal_claims AS owned
             JOIN memory_proposal_claims AS competing
                ON competing.scope_type = owned.scope_type
                AND competing.scope_id = owned.scope_id
                AND competing.proposal_key = owned.proposal_key
             LEFT JOIN memory_entries AS competitor ON competitor.entry_id = competing.entry_id
             WHERE owned.scope_type = ?1 AND owned.scope_id = ?2 AND owned.proposal_key = ?3
                AND competing.canonical_key != owned.canonical_key
                AND (competing.entry_id IS NULL OR (
                    competitor.scope_type = ?1 AND competitor.scope_id = ?2
                    AND competitor.state IN ('active', 'restored', 'conflicted')))",
        )?;
        statement
            .query_map(
                rusqlite::params![scope_name(scope), scope_id, proposal_key],
                |row| row.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?
    };
    let entries = {
        let mut statement = transaction.prepare(
            "SELECT entry_id, body FROM memory_entries
             WHERE scope_type = ?1 AND scope_id = ?2 AND origin = 'inferred_session'
                AND state IN ('active', 'restored', 'conflicted')",
        )?;
        statement
            .query_map(rusqlite::params![scope_name(scope), scope_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (entry_id, body) in entries {
        if contested_keys.contains(&equivalence::explicit_memory_key(&body)) {
            transaction.execute(
                "UPDATE memory_entries SET state = 'conflicted'
                 WHERE entry_id = ?1 AND state IN ('active', 'restored')",
                [&entry_id],
            )?;
            transaction.execute(
                "DELETE FROM memory_entries_fts WHERE entry_id = ?1",
                [&entry_id],
            )?;
        }
    }
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

type StoredIdentities = BTreeMap<(String, String, String), Vec<String>>;

fn stored_identities(transaction: &Transaction<'_>) -> Result<StoredIdentities, MemoryError> {
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
    let mut identities = StoredIdentities::new();
    for (scope, scope_id, body, entry_id) in entries {
        identities
            .entry((scope, scope_id, equivalence::explicit_memory_key(&body)))
            .or_default()
            .push(entry_id);
    }
    Ok(identities)
}

/// Repairs v6 key drift from durable claims, even after candidate history was pruned.
/// Only unique scoped conservative identities are rebound; bodies and evidence are unchanged.
pub(super) fn repair_claims(transaction: &Transaction<'_>) -> Result<(), MemoryError> {
    let identities = stored_identities(transaction)?;
    let claims = {
        let mut statement = transaction.prepare(
            "SELECT DISTINCT scope_type, scope_id, canonical_key FROM memory_proposal_claims",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut ambiguous_claims = 0;
    for (scope, scope_id, canonical_key) in claims {
        let matches = identities.get(&(scope.clone(), scope_id.clone(), canonical_key.clone()));
        if matches.is_some_and(|entries| entries.len() > 1) {
            ambiguous_claims += 1;
        }
        let entry_id = matches
            .filter(|entries| entries.len() == 1)
            .map(|entries| entries[0].as_str());
        transaction.execute(
            "UPDATE memory_proposal_claims SET entry_id = ?1
             WHERE scope_type = ?2 AND scope_id = ?3 AND canonical_key = ?4",
            rusqlite::params![entry_id, scope, scope_id, canonical_key],
        )?;
    }
    let groups = {
        let mut statement = transaction.prepare(
            "SELECT DISTINCT scope_type, scope_id, proposal_key FROM memory_proposal_claims",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (scope, scope_id, key) in groups {
        withhold_competing_inferred(transaction, parse_scope(&scope)?, &scope_id, &key)?;
    }
    if ambiguous_claims > 0 {
        tracing::warn!(
            ambiguous_claims,
            "memory proposal repair left ambiguous identities unbound"
        );
    }
    Ok(())
}

pub(super) fn backfill_claims(transaction: &Transaction<'_>) -> Result<(), MemoryError> {
    let identities = stored_identities(transaction)?;
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
