//! Human-readable competing claims, independent of short-lived candidate detail.

use std::collections::BTreeMap;

use devo_protocol::native::rpc_memory::MemoryScope;
use rusqlite::Connection;

use super::entries::contains_secret;
use super::{MemoryError, equivalence, scope_name};

pub(super) fn append_claims(
    connection: &Connection,
    scope: MemoryScope,
    scope_id: &str,
    projection: &mut String,
) -> Result<(), MemoryError> {
    let claims = {
        let mut statement = connection.prepare(
            "SELECT DISTINCT contested.proposal_key, contested.canonical_key
             FROM memory_contested_proposal_claims AS contested
             WHERE contested.scope_type = ?1 AND contested.scope_id = ?2 AND EXISTS (
               SELECT 1 FROM memory_live_proposal_claims AS accepted
               JOIN memory_entries AS entry ON entry.entry_id = accepted.entry_id
               WHERE accepted.scope_type = contested.scope_type AND accepted.scope_id = contested.scope_id
                 AND accepted.proposal_key = contested.proposal_key AND entry.state = 'conflicted'
                 AND accepted.canonical_key != contested.canonical_key)
             ORDER BY contested.proposal_key, contested.canonical_key",
        )?;
        statement
            .query_map(rusqlite::params![scope_name(scope), scope_id], |row| {
                Ok((
                    row.get::<_, String>(/*idx*/ 0)?,
                    row.get::<_, String>(/*idx*/ 1)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    if claims.is_empty() {
        return Ok(());
    }
    let mut bodies = BTreeMap::new();
    let mut statement = connection.prepare(
        "SELECT normalized_key, body FROM memory_candidates WHERE scope_type = ?1 AND scope_id = ?2
         ORDER BY created_at ASC, candidate_id ASC",
    )?;
    for row in statement.query_map(rusqlite::params![scope_name(scope), scope_id], |row| {
        Ok((
            row.get::<_, String>(/*idx*/ 0)?,
            row.get::<_, String>(/*idx*/ 1)?,
        ))
    })? {
        let (proposal_key, body) = row?;
        if !contains_secret(&body) && !contains_secret(&proposal_key) {
            bodies.insert(
                (proposal_key, equivalence::explicit_memory_key(&body)),
                body,
            );
        }
    }
    let mut rendered = String::new();
    for (proposal_key, canonical_key) in claims {
        if contains_secret(&proposal_key) || contains_secret(&canonical_key) {
            continue;
        }
        let body = bodies
            .get(&(proposal_key.clone(), canonical_key.clone()))
            .unwrap_or(&canonical_key);
        rendered.push_str(&format!(
            "\n- {body}\n  - canonical_claim: {canonical_key}\n  - proposal_key: {proposal_key}\n",
        ));
    }
    if !rendered.is_empty() {
        projection.push_str("\n## Competing inferred claims\n");
        projection.push_str(&rendered);
    }
    Ok(())
}
