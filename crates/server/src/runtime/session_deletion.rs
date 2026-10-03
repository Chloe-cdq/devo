use std::sync::Arc;

use super::{ServerEvent, ServerRuntime, SessionDeletedPayload, SessionId};

impl ServerRuntime {
    pub(crate) async fn delete_session_tree(
        self: &Arc<Self>,
        root_session_id: SessionId,
        related_memory: devo_protocol::native::rpc_session::RelatedMemoryDeletion,
    ) -> Result<Vec<SessionId>, String> {
        use devo_protocol::native::rpc_session::RelatedMemoryDeletion;
        // The task retains the deletion lease through storage completion even
        // when its caller disconnects or cancels while awaiting the response.
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let session_ids = runtime.collect_session_delete_tree(root_session_id).await;
            for session_id in &session_ids {
                runtime
                    .await_session_turn_interrupt_before_delete(*session_id)
                    .await;
            }
            if related_memory == RelatedMemoryDeletion::Preserve {
                runtime
                    .deps
                    .db
                    .record_memory_source_deletions(&session_ids)
                    .map_err(|error| {
                        format!("failed to record session deletion intent: {error}")
                    })?;
            }
            let cleanup_committed = if let Some(memory) = runtime.memory.as_ref() {
                let reservation = match related_memory {
                    RelatedMemoryDeletion::Preserve => None,
                    RelatedMemoryDeletion::Forget => Some(
                        runtime
                            .memory_forget_coordinator
                            .authorize_native(root_session_id, /*entry_id*/ None)
                            .map_err(|error| error.to_string())?,
                    ),
                };
                let (reply, completion) = tokio::sync::oneshot::channel();
                memory.enqueue_source(crate::memory::scan::MemorySourceWork::DeleteSources {
                    sources: session_ids.clone(),
                    related_memory,
                    reply,
                });
                let forgotten = match completion
                    .await
                    .map_err(|error| format!("memory source cleanup worker failed: {error}"))?
                {
                    Ok(forgotten) => Some(forgotten),
                    Err(error) if related_memory == RelatedMemoryDeletion::Preserve => {
                        tracing::warn!(%error, "memory source deletion remains pending");
                        None
                    }
                    Err(error) => return Err(format!("failed to delete related memory: {error}")),
                };
                if let Some(forgotten) = forgotten {
                    if let Some(reservation) = reservation {
                        reservation
                            .commit_sources(&forgotten)
                            .map_err(|error| error.to_string())?;
                    }
                    true
                } else {
                    false
                }
            } else {
                if related_memory == RelatedMemoryDeletion::Forget {
                    return Err("memory runtime is unavailable for related-memory deletion".into());
                }
                false
            };
            if related_memory == RelatedMemoryDeletion::Forget {
                runtime
                    .deps
                    .db
                    .record_memory_source_deletions(&session_ids)
                    .map_err(|error| {
                        format!("failed to record session deletion intent: {error}")
                    })?;
            }
            let mut deleted_session_ids = Vec::new();
            for session_id in session_ids.iter().copied() {
                let removed = runtime.sessions.lock().await.remove(&session_id);
                runtime
                    .clear_deleted_session_runtime_state(session_id)
                    .await;
                let persisted =
                    runtime.deps.db.get_session(&session_id).map_err(|error| {
                        format!("failed to inspect session before delete: {error}")
                    })?;
                let deleted_rollout = runtime
                    .rollout_store
                    .delete_session_rollouts(&session_id)
                    .map_err(|error| format!("failed to delete session rollout: {error}"))?;
                if persisted.is_some() {
                    runtime
                        .deps
                        .db
                        .clear_pending(&session_id, crate::db::QueueType::Turn)
                        .map_err(|error| format!("failed to clear pending turn queue: {error}"))?;
                    runtime
                        .deps
                        .db
                        .clear_pending(&session_id, crate::db::QueueType::Steer)
                        .map_err(|error| format!("failed to clear pending steer queue: {error}"))?;
                    runtime
                        .deps
                        .db
                        .delete_session(&session_id)
                        .map_err(|error| format!("failed to delete session metadata: {error}"))?;
                }
                if removed.is_some() || persisted.is_some() || deleted_rollout {
                    deleted_session_ids.push(session_id);
                }
            }
            if cleanup_committed
                && let Err(error) = runtime.deps.db.finish_memory_source_deletions(&session_ids)
            {
                tracing::warn!(%error, "failed to finish memory source deletion ledger");
            }
            if let Some(memory) = runtime.memory.as_ref() {
                memory.enqueue_source(crate::memory::scan::MemorySourceWork::Reconcile);
            }
            if !deleted_session_ids.is_empty() {
                runtime
                    .broadcast_event(ServerEvent::SessionDeleted(SessionDeletedPayload {
                        session_id: root_session_id,
                        deleted_session_ids: deleted_session_ids.clone(),
                    }))
                    .await;
            }
            Ok(deleted_session_ids)
        })
        .await
        .map_err(|error| format!("session deletion task failed: {error}"))?
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
