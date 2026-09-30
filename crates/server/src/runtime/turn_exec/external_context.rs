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
        while let Some(parent_id) = parent_session_id {
            let parent = self
                .session(parent_id)
                .await
                .ok_or_else(|| format!("parent session {parent_id} unavailable"))?;
            let summary = parent
                .summary()
                .await
                .ok_or_else(|| format!("parent session {parent_id} summary unavailable"))?;
            let record = parent
                .record()
                .await
                .ok_or_else(|| format!("parent session {parent_id} record unavailable"))?;
            if let Some(record) = record {
                let store = self.rollout_store.clone();
                tokio::task::spawn_blocking(move || {
                    store.mark_external_context_used_at(&record.rollout_path, parent_id)
                })
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            }
            parent_session_id = summary.parent_session_id;
        }
        let Some(path) = rollout_path else {
            return Ok(());
        };
        let store = self.rollout_store.clone();
        tokio::task::spawn_blocking(move || store.mark_external_context_used_at(&path, session_id))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())
    }
}
