use devo_protocol::native::rpc_memory::{MemoryOrigin, MemoryReadEntry, MemoryScope};
use rusqlite::OptionalExtension;

use super::entries::contains_secret;
use super::stored_values::{parse_kind, parse_origin, parse_scope, parse_state};
use super::{MemoryError, MemoryRuntime, ReadMemoryRequest, USER_SCOPE_ID};

impl MemoryRuntime {
    pub(super) fn read(&self, request: ReadMemoryRequest) -> Result<MemoryReadEntry, MemoryError> {
        if request.entry_id.as_str().trim().is_empty() || request.entry_id.as_str().len() > 256 {
            return Err(MemoryError::InvalidRequest(
                "memory read requires a stable entry ID".into(),
            ));
        }
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let row = connection
            .query_row(
                "SELECT scope_type, scope_id, kind, state, body, origin,
                    (SELECT COUNT(*) FROM memory_evidence WHERE entry_id = e.entry_id)
             FROM memory_entries e WHERE entry_id = ?1",
                [request.entry_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(/*idx*/ 0)?,
                        row.get::<_, String>(/*idx*/ 1)?,
                        row.get::<_, String>(/*idx*/ 2)?,
                        row.get::<_, String>(/*idx*/ 3)?,
                        row.get::<_, String>(/*idx*/ 4)?,
                        row.get::<_, String>(/*idx*/ 5)?,
                        row.get::<_, i64>(/*idx*/ 6)?,
                    ))
                },
            )
            .optional()?;
        let unavailable = || MemoryError::InvalidRequest("memory entry is unavailable".into());
        let (scope, scope_id, kind, state, body, origin, evidence_count) =
            row.ok_or_else(unavailable)?;
        let scope = parse_scope(&scope)?;
        let expected_scope = match scope {
            MemoryScope::User => USER_SCOPE_ID.to_string(),
            MemoryScope::Project => self.scope_id(scope, &request.workspace_root)?,
        };
        if scope_id != expected_scope {
            return Err(unavailable());
        }
        if contains_secret(&body) {
            return Err(MemoryError::SecretContentRejected);
        }
        let origin = match parse_origin(&origin)? {
            MemoryOrigin::ExplicitUser => "Explicit user memory",
            MemoryOrigin::InferredSession => "Inferred session memory",
        };
        let source = if evidence_count == 1 {
            "source"
        } else {
            "sources"
        };
        let mut bounded_body = body.chars().take(/*n*/ 4000).collect::<String>();
        if body.chars().count() > 4000 {
            bounded_body.push('…');
        }
        Ok(MemoryReadEntry {
            entry_id: request.entry_id,
            scope,
            kind: parse_kind(&kind)?,
            state: parse_state(&state)?,
            body: bounded_body,
            source_summary: format!("{origin} ({evidence_count} {source})"),
        })
    }
}
