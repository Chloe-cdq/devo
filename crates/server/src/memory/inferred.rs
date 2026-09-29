use super::entries::{contains_secret, normalize_body};
use super::entry_identity::{IdentityResolutionMode, MemoryEntryIdentity};
use super::extraction::ExtractionCandidate;
use super::jobs::JobClaim;
use super::source::ExtractableSource;
use super::stored_values::parse_timestamp;
use super::{MemoryError, MemoryRuntime, kind_name, scope_name};
use chrono::{DateTime, SecondsFormat, Utc};
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::session::MemorySetting;
use rusqlite::{OptionalExtension, TransactionBehavior};

impl MemoryRuntime {
    pub(super) fn commit_extraction(
        &self,
        claim: &JobClaim,
        source: &ExtractableSource,
        candidates: &[ExtractionCandidate],
        now: DateTime<Utc>,
    ) -> Result<(), MemoryError> {
        if self
            .config
            .resolve_contribution(source.session_contribution)
            != MemorySetting::On
        {
            return Ok(());
        }
        let timestamp = now.to_rfc3339_opts(SecondsFormat::Millis, /*use_z*/ true);
        let mut prepared = Vec::new();
        for candidate in candidates {
            let body = normalize_body(&candidate.body)?;
            if contains_secret(&body) || contains_secret(&candidate.key) {
                continue;
            }
            if body.chars().count() > 1_000
                || candidate.key.chars().count() > 200
                || candidate.key.trim().is_empty()
                || candidate.evidence.is_empty()
                || candidate.evidence.iter().any(|turn| {
                    !source
                        .messages
                        .iter()
                        .any(|message| &message.turn_id == turn)
                })
            {
                return Err(MemoryError::InvalidRequest(
                    "invalid extraction candidate".into(),
                ));
            }
            let scope_id = self.scope_id(candidate.scope, &source.workspace_root)?;
            let observed_at = source
                .messages
                .iter()
                .filter(|message| candidate.evidence.contains(&message.turn_id))
                .map(|message| message.observed_at)
                .min()
                .expect("validated evidence");
            prepared.push((candidate, body, scope_id, observed_at));
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let owned: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM memory_jobs
             WHERE job_id = ?1 AND lease_owner = ?2 AND state = 'running' AND lease_until > ?3
                AND source_session_id = ?4 AND source_watermark = ?5)",
            rusqlite::params![
                claim.id,
                claim.owner,
                timestamp,
                source.session_id.as_str(),
                source.watermark
            ],
            |row| row.get(0),
        )?;
        if !owned {
            return Ok(());
        }
        let mut scopes = Vec::new();
        let retention = chrono::Duration::try_days(
            self.config
                .candidate_and_job_retention_days
                .try_into()
                .unwrap_or(i64::MAX),
        )
        .unwrap_or(chrono::Duration::MAX);
        let retention_until = now
            .checked_add_signed(retention)
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
            .to_rfc3339();
        for (candidate, body, scope_id, observed_at) in prepared {
            let scope = scope_name(candidate.scope);
            let identity = MemoryEntryIdentity::from_body(&body);
            let reset = transaction.query_row(
                "SELECT ignore_sources_before FROM memory_scope_state WHERE scope_type = ?1 AND scope_id = ?2",
                rusqlite::params![scope, scope_id], |row| row.get::<_, Option<String>>(0),
            ).optional()?.flatten();
            if reset
                .map(|value| parse_timestamp(&value))
                .transpose()?
                .is_some_and(|cutoff| observed_at <= cutoff)
            {
                continue;
            }
            let mut statement = transaction.prepare(
                "SELECT revoked_at, restored_at FROM memory_revocations
                 WHERE scope_type = ?1 AND scope_id = ?2 AND (normalized_key = ?3
                    OR (normalized_key = ?4 AND EXISTS (
                        SELECT 1 FROM memory_entries WHERE scope_type = ?1 AND scope_id = ?2
                            AND normalized_key = ?4 AND origin = 'inferred_session' AND body = ?5
                    )))",
            )?;
            let revocations = statement
                .query_map(
                    rusqlite::params![
                        scope,
                        scope_id,
                        identity.canonical_key,
                        identity.legacy_inferred_key,
                        body
                    ],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            drop(statement);
            let mut blocked = false;
            for (revoked, restored) in revocations {
                let revoked = parse_timestamp(&revoked)?;
                let restored = restored.map(|value| parse_timestamp(&value)).transpose()?;
                if observed_at <= revoked || restored.is_none_or(|value| value < revoked) {
                    blocked = true;
                    break;
                }
            }
            if blocked {
                continue;
            }
            let existing = identity.resolve_and_merge_existing(
                &transaction,
                candidate.scope,
                &scope_id,
                &body,
                IdentityResolutionMode::Inferred,
            )?;
            // Model keys group competing inferred proposals only. Canonical identity
            // remains the approved conservative textual identity of the actual body.
            let proposal_key = super::equivalence::explicit_memory_key(&candidate.key);
            let competitor = if existing.is_none() {
                transaction.query_row(
                    "SELECT entry.entry_id, entry.origin FROM memory_entries AS entry
                     JOIN memory_candidates AS candidate
                        ON candidate.scope_type = entry.scope_type AND candidate.scope_id = entry.scope_id
                        AND candidate.body = entry.body
                     WHERE entry.scope_type = ?1 AND entry.scope_id = ?2
                        AND candidate.normalized_key = ?3 AND candidate.validation_outcome = 'accepted'
                        AND entry.state IN ('active', 'restored', 'conflicted')
                     ORDER BY CASE entry.origin WHEN 'explicit_user' THEN 0 ELSE 1 END, entry.entry_id LIMIT 1",
                    rusqlite::params![scope, scope_id, proposal_key],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                ).optional()?
            } else {
                None
            };
            let (entry_id, outcome) = if let Some(existing) = existing {
                transaction.execute(
                    "UPDATE memory_entries SET updated_at = ?1 WHERE entry_id = ?2",
                    rusqlite::params![timestamp, existing.entry_id],
                )?;
                (Some(existing.entry_id), "accepted")
            } else if let Some((entry_id, origin)) = competitor {
                if origin == "explicit_user" {
                    (None, "explicit_authority")
                } else {
                    transaction.execute(
                        "UPDATE memory_entries SET state = 'conflicted', updated_at = ?1 WHERE entry_id = ?2",
                        rusqlite::params![timestamp, entry_id],
                    )?;
                    transaction.execute(
                        "DELETE FROM memory_entries_fts WHERE entry_id = ?1",
                        [&entry_id],
                    )?;
                    // The opposing source supports the retained candidate, not
                    // the incumbent entry's body.
                    (None, "conflicted")
                }
            } else {
                let entry_id = MemoryEntryId::new().to_string();
                transaction.execute(
                    "INSERT INTO memory_entries (
                        entry_id, scope_type, scope_id, kind, normalized_key, body,
                        origin, state, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'inferred_session', 'active', ?7, ?7)",
                    rusqlite::params![
                        entry_id,
                        scope,
                        scope_id,
                        kind_name(candidate.kind),
                        identity.canonical_key,
                        body,
                        timestamp
                    ],
                )?;
                transaction.execute(
                    "INSERT INTO memory_entries_fts (entry_id, normalized_key, body) VALUES (?1, ?2, ?3)",
                    rusqlite::params![entry_id, identity.canonical_key, body],
                )?;
                (Some(entry_id), "accepted")
            };
            transaction.execute(
                "INSERT INTO memory_candidates (
                    candidate_id, scope_type, scope_id, kind, normalized_key, body, origin,
                    source_session_id, validation_outcome, retention_until, created_at
                 ) SELECT ?1, ?2, ?3, ?4, ?5, ?6, 'inferred_session', ?7, ?8, ?9, ?10
                 WHERE NOT EXISTS (SELECT 1 FROM memory_candidates
                    WHERE scope_type = ?2 AND scope_id = ?3 AND normalized_key = ?5
                        AND body = ?6 AND source_session_id = ?7)",
                rusqlite::params![
                    uuid::Uuid::now_v7().simple().to_string(),
                    scope,
                    scope_id,
                    kind_name(candidate.kind),
                    proposal_key,
                    body,
                    source.session_id.as_str(),
                    outcome,
                    retention_until,
                    timestamp
                ],
            )?;
            if let Some(entry_id) = entry_id {
                for turn in &candidate.evidence {
                    let user_item = source
                        .messages
                        .iter()
                        .find(|message| &message.turn_id == turn && message.role == "user")
                        .map(|message| message.item_id.as_str());
                    transaction.execute(
                        "INSERT INTO memory_evidence (
                            evidence_id, entry_id, session_id, turn_id, source_user_item_id, observed_at, source_watermark
                         ) SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7
                         WHERE NOT EXISTS (SELECT 1 FROM memory_evidence
                            WHERE entry_id = ?2 AND session_id = ?3 AND turn_id IS ?4 AND source_user_item_id IS ?5)",
                        rusqlite::params![uuid::Uuid::now_v7().simple().to_string(), entry_id,
                            source.session_id.as_str(), turn.as_str(), user_item, observed_at.to_rfc3339(), source.watermark],
                    )?;
                }
            }
            if !scopes.contains(&(candidate.scope, scope_id.clone())) {
                scopes.push((candidate.scope, scope_id));
            }
        }
        transaction.execute(
            "UPDATE memory_jobs SET state = 'completed', error_class = NULL, lease_owner = NULL,
                lease_until = NULL, retry_at = NULL, updated_at = ?1 WHERE job_id = ?2 AND lease_owner = ?3",
            rusqlite::params![timestamp, claim.id, claim.owner],
        )?;
        transaction.commit()?;
        for (scope, scope_id) in scopes {
            if let Err(error) = self.refresh_projection(&connection, scope, &scope_id) {
                connection.execute(
                    "UPDATE memory_jobs SET state = 'error', error_class = 'projection_error' WHERE job_id = ?1",
                    [&claim.id],
                )?;
                return Err(error);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "inferred_tests.rs"]
mod tests;
