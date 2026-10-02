use std::collections::HashSet;
use std::path::PathBuf;

use devo_core::SessionId;

use super::super::ServerRuntime;

impl ServerRuntime {
    /// Loaded actors retain ephemeral ancestry; the index retains unloaded
    /// durable ancestry. Neither source alone describes every valid chain.
    async fn resolve_external_context_ancestor(
        &self,
        session_id: SessionId,
    ) -> Result<crate::db::SessionIndexRecord, String> {
        if let Some(handle) = self.session(session_id).await
            && let Some(snapshot) = handle.hook_context_snapshot().await
        {
            return Ok(crate::db::SessionIndexRecord {
                metadata: snapshot.summary,
                rollout_path: snapshot.record.map(|record| record.rollout_path),
            });
        }
        self.deps
            .db
            .get_session_index(&session_id)
            .map_err(|error| format!("failed to read ancestor session {session_id}: {error}"))?
            .ok_or_else(|| format!("ancestor session {session_id} unavailable"))
    }

    pub(in crate::runtime) async fn mark_external_context_used(
        &self,
        rollout_path: Option<PathBuf>,
        session_id: SessionId,
        mut parent_session_id: Option<SessionId>,
    ) -> Result<(), String> {
        let mut contains_durable_session = rollout_path.is_some();
        let mut sources = vec![session_id];
        let mut records = vec![(session_id, rollout_path)];
        let mut visited = HashSet::from([session_id]);
        while let Some(parent_id) = parent_session_id {
            if !visited.insert(parent_id) {
                return Err("external-context parent chain contains a cycle".into());
            }
            let index = self.resolve_external_context_ancestor(parent_id).await?;
            contains_durable_session |= !index.metadata.ephemeral;
            parent_session_id = index.metadata.parent_session_id;
            sources.push(parent_id);
            records.push((parent_id, index.rollout_path));
        }
        // Wholly ephemeral chains cannot be admitted as passive sources and
        // therefore must not depend on memory storage being available.
        if !contains_durable_session
            && let Some(handle) = self.session(session_id).await
            && let Some(snapshot) = handle.hook_context_snapshot().await
            && snapshot.summary.ephemeral
        {
            return Ok(());
        }
        // Close memory admission immediately. Optional database writes and
        // projection repair run in the background, never on the tool path.
        if let Some(memory) = &self.memory {
            memory.begin_external_context_sources(&sources);
        }
        let mut marker_failed = false;
        for (source_id, path) in records {
            if let Some(path) = path {
                let store = self.rollout_store.clone();
                let marker = tokio::task::spawn_blocking(move || {
                    store.mark_external_context_used_at(&path, source_id)
                })
                .await;
                if !matches!(marker, Ok(Ok(()))) {
                    marker_failed = true;
                    if let Some(memory) = &self.memory {
                        memory.note_source_provenance_storage_failure();
                    }
                    tracing::warn!(
                        "external-context rollout marker unavailable; memory reconciliation required"
                    );
                }
            }
        }
        if let Some(memory) = &self.memory {
            memory.enqueue_source(crate::memory::scan::MemorySourceWork::Reconcile);
        } else if marker_failed {
            // Preserve the available ledger fallback even when the optional
            // memory module failed initialization. It cannot block this tool.
            let db = std::sync::Arc::clone(&self.deps.db);
            let _ = std::thread::spawn(move || {
                if db.record_external_context_sources(&sources).is_err() {
                    tracing::warn!("external-context source ledger reconciliation required");
                }
            });
        }
        Ok(())
    }
}
