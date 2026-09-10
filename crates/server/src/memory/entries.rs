use chrono::{DateTime, Utc};
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_protocol::native::rpc_memory::MemoryKind;
use devo_protocol::native::rpc_memory::MemoryOrigin;
use devo_protocol::native::rpc_memory::MemoryScope;
use devo_protocol::native::rpc_memory::MemoryState;
use devo_safety::{InMemorySecretDetectorRegistry, SecretDetectorRegistry};
use rusqlite::{Connection, OptionalExtension};

use super::identity;
use super::projection::{render_projection, write_atomic_projection};
use super::{
    MemoryError, MemoryInferredRememberRequest, MemoryRememberRequest, MemoryRuntime,
    USER_SCOPE_ID, kind_name, origin_name, scope_name, state_name,
};

#[allow(dead_code)]
enum MemoryWriteMode {
    Explicit,
    Inferred {
        source_observed_at: DateTime<Utc>,
        source_watermark: String,
    },
}

impl MemoryRuntime {
    pub(super) fn remember(
        &self,
        request: MemoryRememberRequest,
    ) -> Result<MemoryEntry, MemoryError> {
        self.remember_entry(request, MemoryWriteMode::Explicit)?
            .ok_or_else(|| {
                MemoryError::InvalidStoredValue("explicit memory write was skipped".into())
            })
    }

    #[allow(dead_code)]
    pub(super) fn remember_inferred(
        &self,
        request: MemoryInferredRememberRequest,
    ) -> Result<Option<MemoryEntry>, MemoryError> {
        self.remember_entry(
            MemoryRememberRequest {
                text: request.text,
                scope: request.scope,
                kind: request.kind,
                source: request.source,
            },
            MemoryWriteMode::Inferred {
                source_observed_at: request.source_observed_at,
                source_watermark: request.source_watermark,
            },
        )
    }

    fn remember_entry(
        &self,
        request: MemoryRememberRequest,
        mode: MemoryWriteMode,
    ) -> Result<Option<MemoryEntry>, MemoryError> {
        let body = normalize_body(&request.text)?;
        if contains_secret(&body) {
            return Err(MemoryError::SecretContentRejected);
        }
        let kind = request.kind.unwrap_or_else(|| classify_kind(&body));
        let normalized_key = normalize_key(&body);
        let scope_id = self.scope_id(request.scope, &request.source.workspace_root)?;
        let now = Utc::now().to_rfc3339();
        let (origin, observed_at, source_watermark, source_observed_at, allow_restore) = match mode
        {
            MemoryWriteMode::Explicit => (
                MemoryOrigin::ExplicitUser,
                now.clone(),
                now.clone(),
                None,
                true,
            ),
            MemoryWriteMode::Inferred {
                source_observed_at,
                source_watermark,
            } => (
                MemoryOrigin::InferredSession,
                source_observed_at.to_rfc3339(),
                source_watermark,
                Some(source_observed_at),
                false,
            ),
        };
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.unchecked_transaction()?;
        let existing: Option<(String, String)> = transaction
            .query_row(
                "SELECT entry_id, origin
                 FROM memory_entries
                 WHERE scope_type = ?1 AND scope_id = ?2 AND normalized_key = ?3",
                rusqlite::params![scope_name(request.scope), scope_id, normalized_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let existing_origin = existing
            .as_ref()
            .map(|(_, origin)| parse_origin(origin))
            .transpose()?;
        let revocation = transaction
            .query_row(
                "SELECT revoked_at, restored_at
                 FROM memory_revocations
                 WHERE scope_type = ?1 AND scope_id = ?2 AND normalized_key = ?3",
                rusqlite::params![scope_name(request.scope), scope_id, normalized_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?;
        let revocation = revocation
            .map(|(revoked_at, restored_at)| -> Result<_, MemoryError> {
                Ok((
                    parse_timestamp(&revoked_at)?,
                    restored_at.as_deref().map(parse_timestamp).transpose()?,
                ))
            })
            .transpose()?;
        let revocation_active = revocation
            .as_ref()
            .is_some_and(|(revoked_at, restored_at)| {
                restored_at.is_none_or(|restored_at| restored_at < *revoked_at)
            });
        if let Some(source_observed_at) = source_observed_at
            && (revocation
                .as_ref()
                .is_some_and(|(revoked_at, _)| source_observed_at <= *revoked_at)
                || revocation_active)
        {
            return Ok(None);
        }
        let preserve_existing =
            source_observed_at.is_some() && existing_origin == Some(MemoryOrigin::ExplicitUser);
        let existing_id = existing.as_ref().map(|(entry_id, _)| entry_id);
        let state = if revocation.is_some() {
            MemoryState::Restored
        } else {
            MemoryState::Active
        };
        let entry_id = if let Some(existing_id) = existing_id {
            if preserve_existing {
                transaction.execute(
                    "UPDATE memory_entries
                     SET updated_at = ?1
                     WHERE entry_id = ?2",
                    rusqlite::params![now, existing_id],
                )?;
            } else {
                transaction.execute(
                    "UPDATE memory_entries
                     SET kind = ?1, body = ?2, origin = ?3, state = ?4,
                         updated_at = ?5, replacement_entry_id = NULL
                     WHERE entry_id = ?6",
                    rusqlite::params![
                        kind_name(kind),
                        body,
                        origin_name(origin),
                        state_name(state),
                        now,
                        existing_id,
                    ],
                )?;
            }
            if allow_restore {
                transaction.execute(
                    "UPDATE memory_revocations
                     SET restored_at = ?1
                     WHERE scope_type = ?2 AND scope_id = ?3 AND normalized_key = ?4
                       AND (restored_at IS NULL OR restored_at < revoked_at)",
                    rusqlite::params![now, scope_name(request.scope), scope_id, normalized_key,],
                )?;
            }
            MemoryEntryId::from_string(existing_id.clone())
        } else {
            let entry_id = MemoryEntryId::new();
            transaction.execute(
                "INSERT INTO memory_entries (
                     entry_id, scope_type, scope_id, kind, normalized_key, body,
                     origin, state, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)",
                rusqlite::params![
                    entry_id.as_str(),
                    scope_name(request.scope),
                    scope_id,
                    kind_name(kind),
                    normalized_key,
                    body,
                    origin_name(origin),
                    state_name(state),
                    now,
                ],
            )?;
            entry_id
        };
        if !preserve_existing {
            transaction.execute(
                "DELETE FROM memory_entries_fts WHERE entry_id = ?1",
                [entry_id.as_str()],
            )?;
            transaction.execute(
                "INSERT INTO memory_entries_fts (entry_id, normalized_key, body)
                 SELECT entry_id, normalized_key, body
                 FROM memory_entries
                 WHERE entry_id = ?1",
                [entry_id.as_str()],
            )?;
        }
        transaction.execute(
            "INSERT INTO memory_evidence (
                 evidence_id, entry_id, session_id, turn_id, source_user_item_id,
                 observed_at, source_watermark
             )
             SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM memory_evidence
                 WHERE entry_id = ?2
                   AND session_id = ?3
                   AND turn_id IS ?4
                   AND source_user_item_id IS ?5
             )",
            rusqlite::params![
                uuid::Uuid::now_v7().simple().to_string(),
                entry_id.as_str(),
                request.source.session_id.to_string(),
                request.source.turn_id.map(|turn_id| turn_id.to_string()),
                request
                    .source
                    .user_item_id
                    .map(|item_id| item_id.to_string()),
                observed_at,
                source_watermark,
            ],
        )?;
        transaction.commit()?;
        let entry = load_entry(&connection, &entry_id)?
            .ok_or_else(|| MemoryError::InvalidStoredValue("committed entry is missing".into()))?;
        self.refresh_projection(&connection, request.scope, &scope_id)?;
        Ok(Some(entry))
    }

    pub(super) fn scope_id(
        &self,
        scope: MemoryScope,
        workspace_root: &std::path::Path,
    ) -> Result<String, MemoryError> {
        match scope {
            MemoryScope::User => Ok(USER_SCOPE_ID.to_string()),
            MemoryScope::Project => identity::resolve_project_memory_identity(workspace_root)
                .map(|identity| identity.scope_id)
                .map_err(|error| MemoryError::ProjectIdentity(error.to_string())),
        }
    }

    pub(super) fn refresh_projection(
        &self,
        connection: &Connection,
        scope: MemoryScope,
        scope_id: &str,
    ) -> Result<(), MemoryError> {
        let entries = load_scope_entries(connection, scope, scope_id)?;
        let projection = render_projection(scope, &entries);
        let directory = match scope {
            MemoryScope::User => self.memory_root.join("user"),
            MemoryScope::Project => self.memory_root.join("projects").join(scope_id),
        };
        write_atomic_projection(&directory.join("MEMORY.md"), projection.as_bytes())
    }
}

pub(super) fn normalize_body(text: &str) -> Result<String, MemoryError> {
    let body = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if body.is_empty() {
        return Err(MemoryError::InvalidRequest(
            "memory text must not be empty".into(),
        ));
    }
    Ok(body)
}

fn normalize_key(body: &str) -> String {
    body.chars()
        .filter(|character| character.is_alphanumeric() || character.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn classify_kind(body: &str) -> MemoryKind {
    let body = body.to_ascii_lowercase();
    if body.starts_with("i prefer ") || body.starts_with("i like ") {
        MemoryKind::Preference
    } else if body.starts_with("feedback:") {
        MemoryKind::Feedback
    } else if body.starts_with("http://") || body.starts_with("https://") {
        MemoryKind::Reference
    } else {
        MemoryKind::Fact
    }
}

fn contains_secret(body: &str) -> bool {
    let lower_body = body.to_ascii_lowercase();
    let marker_match = [
        "sk-",
        "ghp_",
        "github_pat_",
        "xoxb-",
        "xoxp-",
        "bearer ",
        "api_key=",
        "apikey=",
        "aws_secret_access_key",
        "-----begin ",
    ]
    .iter()
    .any(|marker| lower_body.contains(marker));
    marker_match
        || InMemorySecretDetectorRegistry::with_default_detectors()
            .all()
            .into_iter()
            .any(|detector| !detector.detect(body).is_empty())
}

pub(super) fn load_entry(
    connection: &Connection,
    entry_id: &MemoryEntryId,
) -> Result<Option<MemoryEntry>, MemoryError> {
    let entry = connection
        .query_row(
            "SELECT entry_id, scope_type, scope_id, kind, normalized_key, body,
                    origin, state, created_at, updated_at, replacement_entry_id
             FROM memory_entries
             WHERE entry_id = ?1",
            [entry_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, Option<String>>(10)?,
                ))
            },
        )
        .optional()?;
    let Some((
        entry_id,
        scope_type,
        scope_id,
        kind,
        normalized_key,
        body,
        origin,
        state,
        created_at,
        updated_at,
        replacement_entry_id,
    )) = entry
    else {
        return Ok(None);
    };
    let provenance = load_provenance(connection, &entry_id)?;
    Ok(Some(MemoryEntry {
        entry_id: MemoryEntryId::from_string(entry_id.to_string()),
        scope: parse_scope(&scope_type)?,
        scope_id,
        kind: parse_kind(&kind)?,
        normalized_key,
        body,
        origin: parse_origin(&origin)?,
        state: parse_state(&state)?,
        created_at: parse_timestamp(&created_at)?,
        updated_at: parse_timestamp(&updated_at)?,
        replacement_entry_id: replacement_entry_id.map(MemoryEntryId::from_string),
        provenance,
    }))
}

fn load_scope_entries(
    connection: &Connection,
    scope: MemoryScope,
    scope_id: &str,
) -> Result<Vec<MemoryEntry>, MemoryError> {
    let mut statement = connection.prepare(
        "SELECT entry_id
         FROM memory_entries
         WHERE scope_type = ?1 AND scope_id = ?2
         ORDER BY updated_at DESC, entry_id ASC",
    )?;
    let ids = statement
        .query_map(rusqlite::params![scope_name(scope), scope_id], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    ids.iter()
        .map(|entry_id| {
            load_entry(connection, &MemoryEntryId::from_string(entry_id.clone()))?.ok_or_else(
                || MemoryError::InvalidStoredValue("projection entry is missing".into()),
            )
        })
        .collect()
}

fn load_provenance(
    connection: &Connection,
    entry_id: &str,
) -> Result<Vec<devo_protocol::native::rpc_memory::MemoryProvenance>, MemoryError> {
    let mut statement = connection.prepare(
        "SELECT session_id, turn_id, source_user_item_id
         FROM memory_evidence
         WHERE entry_id = ?1
         ORDER BY observed_at ASC, evidence_id ASC",
    )?;
    let rows = statement
        .query_map([entry_id], |row| {
            Ok(devo_protocol::native::rpc_memory::MemoryProvenance {
                source_session_id: row.get(0)?,
                source_turn_id: row.get(1)?,
                source_user_item_id: row
                    .get::<_, Option<String>>(2)?
                    .map(devo_protocol::native::ids::ItemId::from_string),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn parse_scope(value: &str) -> Result<MemoryScope, MemoryError> {
    match value {
        "user" => Ok(MemoryScope::User),
        "project" => Ok(MemoryScope::Project),
        _ => Err(MemoryError::InvalidStoredValue(value.into())),
    }
}

fn parse_kind(value: &str) -> Result<MemoryKind, MemoryError> {
    match value {
        "preference" => Ok(MemoryKind::Preference),
        "feedback" => Ok(MemoryKind::Feedback),
        "fact" => Ok(MemoryKind::Fact),
        "reference" => Ok(MemoryKind::Reference),
        _ => Err(MemoryError::InvalidStoredValue(value.into())),
    }
}

fn parse_state(value: &str) -> Result<MemoryState, MemoryError> {
    match value {
        "active" => Ok(MemoryState::Active),
        "stale" => Ok(MemoryState::Stale),
        "conflicted" => Ok(MemoryState::Conflicted),
        "retired" => Ok(MemoryState::Retired),
        "restored" => Ok(MemoryState::Restored),
        _ => Err(MemoryError::InvalidStoredValue(value.into())),
    }
}

fn parse_origin(value: &str) -> Result<MemoryOrigin, MemoryError> {
    match value {
        "explicit_user" => Ok(MemoryOrigin::ExplicitUser),
        "inferred_session" => Ok(MemoryOrigin::InferredSession),
        _ => Err(MemoryError::InvalidStoredValue(value.into())),
    }
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, MemoryError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| MemoryError::InvalidTimestamp(value.into()))
}
