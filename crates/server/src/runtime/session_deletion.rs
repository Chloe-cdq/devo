use std::sync::Arc;

use super::{ServerRuntime, SessionId};

impl ServerRuntime {
    pub(crate) async fn delete_session_tree(
        self: &Arc<Self>,
        root_session_id: SessionId,
    ) -> Result<Vec<SessionId>, String> {
        let session_ids = self.collect_session_delete_tree(root_session_id).await;
        for session_id in &session_ids {
            self.await_session_turn_interrupt_before_delete(*session_id)
                .await;
        }

        if let Some(memory) = self.memory.as_ref()
            && !session_ids.is_empty()
        {
            let memory = Arc::clone(memory);
            let sources = session_ids.clone();
            tokio::task::spawn_blocking(move || {
                memory.delete_sources(&sources, chrono::Utc::now())
            })
            .await
            .map_err(|error| format!("failed to join memory source deletion: {error}"))?
            .map_err(|error| format!("failed to delete memory sources: {error}"))?;
        }

        let mut deleted_session_ids = Vec::new();
        for session_id in session_ids {
            let removed = self.sessions.lock().await.remove(&session_id);
            self.clear_deleted_session_runtime_state(session_id).await;
            let persisted = self
                .deps
                .db
                .get_session(&session_id)
                .map_err(|error| format!("failed to inspect session before delete: {error}"))?;
            let deleted_rollout = self
                .rollout_store
                .delete_session_rollouts(&session_id)
                .map_err(|error| format!("failed to delete session rollout: {error}"))?;
            if persisted.is_some() {
                self.deps
                    .db
                    .clear_pending(&session_id, crate::db::QueueType::Turn)
                    .map_err(|error| format!("failed to clear pending turn queue: {error}"))?;
                self.deps
                    .db
                    .clear_pending(&session_id, crate::db::QueueType::Steer)
                    .map_err(|error| format!("failed to clear pending steer queue: {error}"))?;
                self.deps
                    .db
                    .delete_session(&session_id)
                    .map_err(|error| format!("failed to delete session metadata: {error}"))?;
            }
            if removed.is_some() || persisted.is_some() || deleted_rollout {
                deleted_session_ids.push(session_id);
            }
        }
        Ok(deleted_session_ids)
    }

    async fn collect_session_delete_tree(&self, root_session_id: SessionId) -> Vec<SessionId> {
        // Cascade only sub-agent children (`parent_session_id` + agent markers).
        // User forks use `fork_from_id` and must remain after the source is deleted.
        let session_ids_in_runtime: Vec<SessionId> = {
            let sessions = self.sessions.lock().await;
            sessions.keys().copied().collect()
        };
        let mut agent_children_by_parent = Vec::new();
        for session_id in session_ids_in_runtime {
            let Some(handle) = self.session(session_id).await else {
                continue;
            };
            let Some(summary) = handle.summary().await else {
                continue;
            };
            let is_agent_child = summary.agent_path.is_some()
                || summary.agent_role.is_some()
                || summary.agent_nickname.is_some();
            if is_agent_child && let Some(parent_id) = summary.parent_session_id {
                agent_children_by_parent.push((session_id, parent_id));
            }
        }
        agent_children_by_parent.sort_by_key(|(session_id, _parent_id)| session_id.to_string());

        let mut seen = std::collections::HashSet::new();
        let mut session_ids = Vec::new();
        seen.insert(root_session_id);
        session_ids.push(root_session_id);
        let mut index = 0;
        while index < session_ids.len() {
            let parent_session_id = session_ids[index];
            for (session_id, parent_id) in &agent_children_by_parent {
                if *parent_id == parent_session_id && seen.insert(*session_id) {
                    session_ids.push(*session_id);
                }
            }
            index += 1;
        }
        session_ids
    }

    pub(crate) async fn await_session_turn_interrupt_before_delete(
        self: &Arc<Self>,
        session_id: SessionId,
    ) {
        let Some(turn_id) = self.runtime_active_turn_id(session_id).await else {
            return;
        };
        let receiver = self.subscribe_terminal_turn_status(turn_id).await;
        if self.recent_terminal_turn_status(turn_id).await.is_some() {
            return;
        }
        self.signal_active_turn_interrupt(session_id).await;
        if tokio::time::timeout(std::time::Duration::from_secs(5), receiver)
            .await
            .is_err()
            && self.runtime_active_turn_id(session_id).await.is_some()
        {
            tracing::warn!(
                session_id = %session_id,
                turn_id = %turn_id,
                "turn interrupt timed out before session delete"
            );
        }
    }
}
