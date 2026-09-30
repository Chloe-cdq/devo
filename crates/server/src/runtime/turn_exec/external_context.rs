use std::collections::HashSet;
use std::path::PathBuf;

use devo_core::SessionId;

use super::super::ServerRuntime;

impl ServerRuntime {
    pub(in crate::runtime) async fn mark_external_context_used(
        &self,
        rollout_path: Option<PathBuf>,
        session_id: SessionId,
        mut parent_session_id: Option<SessionId>,
    ) -> Result<(), String> {
        self.deps
            .db
            .record_external_context_sources(&[session_id])
            .map_err(|error| format!("failed to record external-context source: {error}"))?;
        let mut ancestors = Vec::new();
        let mut visited = HashSet::from([session_id]);
        while let Some(parent_id) = parent_session_id {
            if !visited.insert(parent_id) {
                return Err("external-context parent chain contains a cycle".into());
            }
            let index = self
                .deps
                .db
                .get_session_index(&parent_id)
                .map_err(|error| format!("failed to read parent session {parent_id}: {error}"))?
                .ok_or_else(|| format!("parent session {parent_id} unavailable"))?;
            parent_session_id = index.metadata.parent_session_id;
            ancestors.push((parent_id, index.rollout_path));
        }
        let ancestor_ids = ancestors.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        self.deps
            .db
            .record_external_context_sources(&ancestor_ids)
            .map_err(|error| {
                format!("failed to record ancestor external-context sources: {error}")
            })?;
        for (parent_id, rollout_path) in ancestors {
            if let Some(path) = rollout_path {
                let store = self.rollout_store.clone();
                tokio::task::spawn_blocking(move || {
                    store.mark_external_context_used_at(&path, parent_id)
                })
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            }
        }
        if let Some(path) = rollout_path {
            let store = self.rollout_store.clone();
            tokio::task::spawn_blocking(move || {
                store.mark_external_context_used_at(&path, session_id)
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        }
        if let Some(memory) = &self.memory {
            let memory = std::sync::Arc::clone(memory);
            let db = std::sync::Arc::clone(&self.deps.db);
            tokio::task::spawn_blocking(move || {
                super::super::session_deletion::reconcile_external_context_sources(&memory, &db);
            })
            .await
            .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}
