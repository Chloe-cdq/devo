use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::MemoryListResult;
use devo_protocol::native::rpc_memory::MemoryScope;

use super::entries::load_entry;
use super::{
    DEFAULT_LIST_LIMIT, ListMemoryRequest, MAX_LIST_LIMIT, MemoryError, MemoryRuntime, kind_name,
    origin_name, scope_name, state_name,
};

enum MemoryListMode {
    Management,
    Recallable,
}

impl MemoryRuntime {
    pub(super) fn list(&self, request: ListMemoryRequest) -> Result<MemoryListResult, MemoryError> {
        self.list_with_mode(request, MemoryListMode::Management)
    }

    pub(super) fn list_recallable(
        &self,
        request: ListMemoryRequest,
    ) -> Result<MemoryListResult, MemoryError> {
        self.list_with_mode(request, MemoryListMode::Recallable)
    }

    fn list_with_mode(
        &self,
        request: ListMemoryRequest,
        mode: MemoryListMode,
    ) -> Result<MemoryListResult, MemoryError> {
        let scope = request.scope.unwrap_or(MemoryScope::User);
        let scope_id = self.scope_id(scope, &request.workspace_root)?;
        let limit = request
            .limit
            .unwrap_or(DEFAULT_LIST_LIMIT)
            .clamp(1, MAX_LIST_LIMIT);
        let offset = parse_cursor(request.cursor.as_deref())?;
        let state_filter = match mode {
            MemoryListMode::Management => "AND (?4 IS NULL OR state = ?4)",
            MemoryListMode::Recallable => {
                "AND (?4 IS NULL OR state = ?4 OR (?4 = 'active' AND state = 'restored'))"
            }
        };
        let query = format!(
            "SELECT entry_id
             FROM memory_entries
             WHERE scope_type = ?1
               AND scope_id = ?2
               AND (?3 IS NULL OR kind = ?3)
               {state_filter}
               AND (?5 IS NULL OR origin = ?5)
               AND (?6 IS NULL OR body LIKE '%' || ?6 || '%' OR normalized_key LIKE '%' || ?6 || '%')
             ORDER BY updated_at DESC, entry_id ASC
             LIMIT ?7 OFFSET ?8"
        );
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let mut statement = connection.prepare(&query)?;
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
}

fn parse_cursor(cursor: Option<&str>) -> Result<usize, MemoryError> {
    cursor
        .unwrap_or("0")
        .parse::<usize>()
        .map_err(|_| MemoryError::InvalidRequest("memory cursor must be a number".into()))
}
