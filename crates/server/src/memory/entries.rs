use chrono::{DateTime, Utc};
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_protocol::native::rpc_memory::MemoryKind;
use devo_protocol::native::rpc_memory::MemoryListResult;
use devo_protocol::native::rpc_memory::MemoryOrigin;
use devo_protocol::native::rpc_memory::MemoryScope;
use devo_protocol::native::rpc_memory::MemoryState;
use devo_safety::{InMemorySecretDetectorRegistry, SecretDetectorRegistry};
use rusqlite::{Connection, OptionalExtension};

use super::identity;
use super::projection::{render_projection, write_atomic_projection};
use super::{
    DEFAULT_LIST_LIMIT, ListMemoryRequest, MAX_LIST_LIMIT, MemoryError, MemoryRememberRequest,
    MemoryRuntime, USER_SCOPE_ID,
};

impl MemoryRuntime {
    pub(super) fn remember(
        &self,
        request: MemoryRememberRequest,
    ) -> Result<MemoryEntry, MemoryError> {
        let body = normalize_body(&request.text)?;
        if contains_secret(&body) {
            return Err(MemoryError::SecretContentRejected);
        }
        if request.source_user_item_id.trim().is_empty() {
            return Err(MemoryError::InvalidRequest(
                "source_user_item_id must not be empty".into(),
            ));
        }
        let kind = request.kind.unwrap_or_else(|| classify_kind(&body));
        let normalized_key = normalize_key(&body);
        let scope_id = self.scope_id(request.scope, &request.workspace_root)?;
        let now = Utc::now().to_rfc3339();
        let entry_id = MemoryEntryId::new();
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.unchecked_transaction()?;
        let existing_id: Option<String> = transaction
            .query_row(
                "SELECT entry_id
                 FROM memory_entries
                 WHERE scope_type = ?1 AND scope_id = ?2 AND normalized_key = ?3",
                rusqlite::params![scope_name(request.scope), scope_id, normalized_key],
                |row| row.get(0),
            )
            .optional()?;
        let entry_id = if let Some(existing_id) = existing_id {
            transaction.execute(
                "UPDATE memory_entries
                 SET kind = ?1, body = ?2, origin = 'explicit_user', state = 'active',
                     updated_at = ?3, replacement_entry_id = NULL
                 WHERE entry_id = ?4",
                rusqlite::params![kind_name(kind), body, now, existing_id],
            )?;
            MemoryEntryId::from_string(existing_id)
        } else {
            transaction.execute(
                "INSERT INTO memory_entries (
                     entry_id, scope_type, scope_id, kind, normalized_key, body,
                     origin, state, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'explicit_user', 'active', ?7, ?7)",
                rusqlite::params![
                    entry_id.as_str(),
                    scope_name(request.scope),
                    scope_id,
                    kind_name(kind),
                    normalized_key,
                    body,
                    now,
                ],
            )?;
            entry_id
        };
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
        transaction.execute(
            "INSERT INTO memory_evidence (
                 evidence_id, entry_id, session_id, turn_id, source_user_item_id,
                 observed_at, source_watermark
             )
             SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?6
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
                request.source_session_id,
                request.source_turn_id,
                request.source_user_item_id,
                now,
            ],
        )?;
        transaction.commit()?;
        let entry = load_entry(&connection, &entry_id)?
            .ok_or_else(|| MemoryError::InvalidStoredValue("committed entry is missing".into()))?;
        self.refresh_projection(&connection, request.scope, &scope_id)?;
        Ok(entry)
    }

    pub(super) fn list(&self, request: ListMemoryRequest) -> Result<MemoryListResult, MemoryError> {
        let scope = request.scope.unwrap_or(MemoryScope::User);
        let scope_id = self.scope_id(scope, &request.workspace_root)?;
        let limit = request
            .limit
            .unwrap_or(DEFAULT_LIST_LIMIT)
            .clamp(1, MAX_LIST_LIMIT);
        let offset = parse_cursor(request.cursor.as_deref())?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let mut statement = connection.prepare(
            "SELECT entry_id
             FROM memory_entries
             WHERE scope_type = ?1
               AND scope_id = ?2
               AND (?3 IS NULL OR kind = ?3)
               AND (?4 IS NULL OR state = ?4)
               AND (?5 IS NULL OR origin = ?5)
               AND (?6 IS NULL OR body LIKE '%' || ?6 || '%' OR normalized_key LIKE '%' || ?6 || '%')
             ORDER BY updated_at DESC, entry_id ASC
             LIMIT ?7 OFFSET ?8",
        )?;
        let kind = request.kind.map(kind_name);
        let state = request.state.map(state_name);
        let origin = request.origin.map(origin_name);
        let ids = statement
            .query_map(
                rusqlite::params![
                    scope_name(scope),
                    scope_id,
                    kind,
                    state,
                    origin,
                    request.text,
                    i64::from(limit) + 1,
                    i64::try_from(offset).map_err(|_| {
                        MemoryError::InvalidRequest("memory cursor is too large".into())
                    })?,
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        let has_next = ids.len() > usize::try_from(limit).unwrap_or(usize::MAX);
        let ids = ids.into_iter().take(limit as usize).collect::<Vec<_>>();
        drop(statement);
        let entries = ids
            .iter()
            .map(|id| {
                load_entry(&connection, &MemoryEntryId::from_string(id.clone()))?.ok_or_else(|| {
                    MemoryError::InvalidStoredValue("listed entry is missing".into())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            data: entries,
            next_cursor: has_next.then(|| (offset + usize::try_from(limit).unwrap()).to_string()),
        })
    }

    fn scope_id(
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

    fn refresh_projection(
        &self,
        connection: &Connection,
        scope: MemoryScope,
        scope_id: &str,
    ) -> Result<(), MemoryError> {
        let entries = load_scope_entries(connection, scope, scope_id)?;
        let projection = render_projection(scope, &entries);
        let directory = match scope {
            MemoryScope::User => self.memory_root.join("user"),
            MemoryScope::Project => self.memory_root.join("project").join(scope_id),
        };
        write_atomic_projection(&directory.join("MEMORY.md"), projection.as_bytes())
    }
}

fn normalize_body(text: &str) -> Result<String, MemoryError> {
    let body = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if body.is_empty() {
        return Err(MemoryError::InvalidRequest(
            "memory text must not be empty".into(),
        ));
    }
    Ok(body)
}

fn normalize_key(body: &str) -> String {
    body.to_ascii_lowercase()
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

fn scope_name(scope: MemoryScope) -> &'static str {
    match scope {
        MemoryScope::User => "user",
        MemoryScope::Project => "project",
    }
}

fn kind_name(kind: MemoryKind) -> &'static str {
    match kind {
        MemoryKind::Preference => "preference",
        MemoryKind::Feedback => "feedback",
        MemoryKind::Fact => "fact",
        MemoryKind::Reference => "reference",
    }
}

fn state_name(state: MemoryState) -> &'static str {
    match state {
        MemoryState::Active => "active",
        MemoryState::Stale => "stale",
        MemoryState::Conflicted => "conflicted",
        MemoryState::Retired => "retired",
    }
}

fn origin_name(origin: MemoryOrigin) -> &'static str {
    match origin {
        MemoryOrigin::ExplicitUser => "explicit_user",
        MemoryOrigin::InferredSession => "inferred_session",
    }
}

fn parse_cursor(cursor: Option<&str>) -> Result<usize, MemoryError> {
    cursor
        .unwrap_or("0")
        .parse::<usize>()
        .map_err(|_| MemoryError::InvalidRequest("memory cursor must be a number".into()))
}

fn load_entry(
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
