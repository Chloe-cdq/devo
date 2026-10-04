use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::MemoryListResult;
use devo_protocol::native::rpc_memory::MemoryScope;
use devo_protocol::native::rpc_memory::{MemorySearchEntry, MemorySearchResult};

use super::entries::load_entry;
use super::{
    DEFAULT_LIST_LIMIT, ListMemoryRequest, MAX_LIST_LIMIT, MemoryError, MemoryRuntime,
    SearchMemoryRequest, kind_name, origin_name, scope_name, state_name,
};

impl MemoryRuntime {
    #[cfg(test)]
    pub(super) fn list_recallable(
        &self,
        mut request: ListMemoryRequest,
    ) -> Result<MemoryListResult, MemoryError> {
        request.state = Some(devo_protocol::native::rpc_memory::MemoryState::Active);
        let mut active = self.list(request.clone())?;
        request.state = Some(devo_protocol::native::rpc_memory::MemoryState::Restored);
        active.data.extend(self.list(request)?.data);
        Ok(active)
    }

    pub(super) fn search(
        &self,
        mut request: SearchMemoryRequest,
    ) -> Result<MemorySearchResult, MemoryError> {
        const SEARCH_LIMIT: u32 = 20;
        const MAX_SUMMARY_CHARS: usize = 240;

        request.query = request.query.trim().to_string();
        if request.query.is_empty() || request.query.chars().count() > 1024 {
            return Err(MemoryError::InvalidRequest(
                "memory search query must contain 1 to 1024 characters".into(),
            ));
        }

        let scope_id = self.scope_id(request.scope, &request.workspace_root)?;
        let now = (self.clock)();
        self.expire_inferred(now)?;
        let mut pending_source_deletion = self.has_pending_source_deletions();
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        loop {
            let mut statement = connection.prepare(
                "SELECT entry_id
             FROM memory_entries
             WHERE scope_type = ?1
               AND scope_id = ?2
               AND (?3 IS NULL OR kind = ?3)
               AND ((?4 IS NULL AND state IN ('active', 'restored')) OR state = ?4)
               AND (body LIKE '%' || ?5 || '%' OR normalized_key LIKE '%' || ?5 || '%')
               AND (?7 = 0 OR origin = 'explicit_user')
             ORDER BY updated_at DESC, entry_id ASC
             LIMIT ?6",
            )?;
            let ids = statement
                .query_map(
                    rusqlite::params![
                        scope_name(request.scope),
                        scope_id,
                        request.kind.map(kind_name),
                        request.state.map(state_name),
                        request.query.as_str(),
                        SEARCH_LIMIT,
                        pending_source_deletion,
                    ],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            drop(statement);
            let entries = ids
                .iter()
                .map(|entry_id| {
                    load_entry(&connection, &MemoryEntryId::from_string(entry_id.clone()))?
                        .ok_or_else(|| {
                            MemoryError::InvalidStoredValue("searched entry is missing".into())
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            // If intent committed while this read waited, rerun the limit with explicit-only SQL.
            if !pending_source_deletion && self.has_pending_source_deletions() {
                pending_source_deletion = true;
                continue;
            }
            let result = Page {
                data: entries
                    .into_iter()
                    .filter(|entry| !super::entries::contains_secret(&entry.body))
                    .map(|entry| {
                        let mut summary = entry
                            .body
                            .chars()
                            .take(MAX_SUMMARY_CHARS)
                            .collect::<String>();
                        if entry.body.chars().count() > MAX_SUMMARY_CHARS {
                            summary.push('…');
                        }
                        MemorySearchEntry {
                            entry_id: entry.entry_id,
                            scope: entry.scope,
                            kind: entry.kind,
                            state: entry.state,
                            summary,
                        }
                    })
                    .collect(),
                next_cursor: None,
            };
            for entry in &result.data {
                self.record_on_demand_use(&connection, entry.entry_id.as_str(), now)?;
            }
            return Ok(result);
        }
    }

    pub(super) fn list(&self, request: ListMemoryRequest) -> Result<MemoryListResult, MemoryError> {
        let scope = request.scope.unwrap_or(MemoryScope::User);
        let scope_id = self.scope_id(scope, &request.workspace_root)?;
        self.expire_inferred((self.clock)())?;
        let mut pending_source_deletion = self.has_pending_source_deletions();
        let limit = request
            .limit
            .unwrap_or(DEFAULT_LIST_LIMIT)
            .clamp(1, MAX_LIST_LIMIT);
        let offset = request
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<usize>()
            .map_err(|_| MemoryError::InvalidRequest("memory cursor must be a number".into()))?;
        let query = "SELECT entry_id
             FROM memory_entries
             WHERE scope_type = ?1
               AND scope_id = ?2
               AND (?3 IS NULL OR kind = ?3)
               AND (?4 IS NULL OR state = ?4)
               AND (?5 IS NULL OR origin = ?5)
               AND (?6 IS NULL OR body LIKE '%' || ?6 || '%' OR normalized_key LIKE '%' || ?6 || '%')
               AND (?9 = 0 OR origin = 'explicit_user')
             ORDER BY updated_at DESC, entry_id ASC
             LIMIT ?7 OFFSET ?8";
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let kind = request.kind.map(kind_name);
        let state = request.state.map(state_name);
        let origin = request.origin.map(origin_name);
        loop {
            let mut statement = connection.prepare(query)?;
            let ids = statement
                .query_map(
                    rusqlite::params![
                        scope_name(scope),
                        scope_id,
                        kind,
                        state,
                        origin,
                        request.text.as_deref(),
                        i64::from(limit) + 1,
                        i64::try_from(offset).map_err(|_| {
                            MemoryError::InvalidRequest("memory cursor is too large".into())
                        })?,
                        pending_source_deletion,
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
                    load_entry(&connection, &MemoryEntryId::from_string(id.clone()))?.ok_or_else(
                        || MemoryError::InvalidStoredValue("listed entry is missing".into()),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            if !pending_source_deletion && self.has_pending_source_deletions() {
                pending_source_deletion = true;
                continue;
            }
            return Ok(Page {
                data: entries,
                next_cursor: has_next
                    .then(|| (offset + usize::try_from(limit).unwrap()).to_string()),
            });
        }
    }
}
