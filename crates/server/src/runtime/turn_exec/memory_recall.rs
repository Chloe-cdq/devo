//! Root-turn recall admission and Native persistence, outside the session actor.

use std::sync::Arc;

use devo_core::TurnId;
use devo_protocol::native::event::ServerNotification;
use devo_protocol::native::ids;
use devo_protocol::native::item::{Item, ItemEnvelope, ItemState};

use crate::memory::{PrepareMemoryRequest, PreparedMemory};
use crate::runtime::session_actor::SessionActorState;
use crate::runtime::{ServerRuntime, TurnInputMode};

impl ServerRuntime {
    pub(super) async fn prepare_turn_memory(
        &self,
        state: &SessionActorState,
        turn_id: TurnId,
        query: &str,
        input_mode: &TurnInputMode,
    ) -> Option<Arc<str>> {
        if state.summary.is_subagent() {
            return None;
        }
        let session_id = state.session_id();
        if matches!(
            input_mode,
            TurnInputMode::Recovery | TurnInputMode::ApprovalResume
        ) {
            // The original turn may have had recall Off or a failed preparation.
            // Neither absence nor read failure authorizes a fresh recall on resume.
            let path = state.record.as_ref()?.rollout_path.clone();
            let native_turn = ids::TurnId::from_legacy_uuid(turn_id.into());
            let history =
                tokio::task::spawn_blocking(move || devo_core::read_canonical_history(&path)).await;
            let Ok(Ok(history)) = history else {
                tracing::warn!(%session_id, "memory recall snapshot could not be recovered");
                return None;
            };
            let entries = history.items.into_iter().find_map(|envelope| {
                if envelope.turn_id == native_turn
                    && let Item::MemoryRecall { entries, .. } = envelope.item
                {
                    return Some(entries);
                }
                None
            })?;
            let prepared = PreparedMemory::from_entries(/*project_scope_id*/ None, entries);
            let context = prepared.advisory_context();
            return (!context.is_empty()).then(|| Arc::from(context));
        }
        let memory = self.memory.as_ref()?;
        let prepared = match memory
            .prepare_turn(PrepareMemoryRequest {
                query: query.to_string(),
                workspace_root: state.core.cwd.clone(),
                session_recall: state.memory_settings.recall,
            })
            .await
        {
            Ok(prepared) => prepared,
            Err(_) => {
                // Error values can contain corrupt stored strings; never log them.
                tracing::warn!(%session_id, "memory recall preparation failed");
                PreparedMemory::from_entries(/*project_scope_id*/ None, Vec::new())
            }
        };
        if prepared.snapshot_revision.is_empty() {
            return None;
        }
        let context = prepared.advisory_context();
        let now = chrono::Utc::now();
        let envelope = ItemEnvelope {
            id: ids::ItemId::new(),
            session_id: ids::SessionId::from_legacy_uuid(session_id.into()),
            turn_id: ids::TurnId::from_legacy_uuid(turn_id.into()),
            seq: self.allocate_item_sequence(session_id).await,
            revision: 1,
            created_at: now,
            updated_at: now,
            state: ItemState::Completed,
            item: Item::MemoryRecall {
                snapshot_revision: prepared.snapshot_revision,
                entries: prepared.entries,
            },
        };
        if let Some(record) = state.record.clone() {
            let store = self.rollout_store.clone();
            let item = envelope.clone();
            if !matches!(
                tokio::task::spawn_blocking(move || store.append_canonical_item(&record, item))
                    .await,
                Ok(Ok(()))
            ) {
                tracing::warn!(%session_id, "memory recall item could not be persisted");
                return None;
            }
        }
        self.broadcast_native_notification(
            session_id,
            ServerNotification::ItemCompleted {
                item: Box::new(envelope),
            },
        )
        .await;
        (!context.is_empty()).then(|| Arc::from(context))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_forget_runtime_support::{
        configured_data_root, remember, start_subscribed_session,
    };
    use crate::support::{ScriptedProvider, build_runtime_with_workspace_config};
    use anyhow::{Context, Result};
    use pretty_assertions::assert_eq;

    /// Trace: L2-DES-MEM-001 Rev 4 DD-6
    /// Verifies: a failed recall item append omits context and completion notification.
    #[tokio::test]
    async fn persistence_failure_omits_context_and_completed_notification() -> Result<()> {
        let data = configured_data_root()?;
        let runtime =
            build_runtime_with_workspace_config(data.path(), Arc::new(ScriptedProvider::new([])))?;
        let (connection, mut notifications, session_id) =
            start_subscribed_session(&runtime, data.path(), /*request_id*/ 160).await?;
        remember(&runtime, connection, /*request_id*/ 161, "Use tabs").await?;
        let record = runtime
            .session(session_id)
            .await
            .context("actor")?
            .record()
            .await
            .flatten()
            .context("record")?;
        let restored = runtime
            .hydrate_runtime_session(session_id, &record.rollout_path)
            .await?;
        let mut state = SessionActorState::from_runtime_session(restored);
        let unwriteable = data.path().join("unwriteable");
        std::fs::create_dir(&unwriteable)?;
        state
            .record
            .as_mut()
            .context("detached record")?
            .rollout_path = unwriteable;
        while notifications.try_recv().is_ok() {}
        assert_eq!(
            runtime
                .prepare_turn_memory(
                    &state,
                    TurnId::new(),
                    "Use tabs",
                    &TurnInputMode::VisibleUserMessage
                )
                .await,
            None
        );
        assert!(
            notifications.try_recv().is_err(),
            "failed persistence must not announce a completed recall"
        );
        runtime.shutdown().await;
        Ok(())
    }

    /// Trace: L2-DES-MEM-001 Rev 4 DD-6
    /// Verifies: replay sequence advances past a Native-only memory recall item.
    #[tokio::test]
    async fn restored_sequence_advances_past_native_only_recall() -> Result<()> {
        let data = configured_data_root()?;
        let runtime =
            build_runtime_with_workspace_config(data.path(), Arc::new(ScriptedProvider::new([])))?;
        let (connection, _notifications, session_id) =
            start_subscribed_session(&runtime, data.path(), /*request_id*/ 170).await?;
        remember(&runtime, connection, /*request_id*/ 171, "Use tabs").await?;
        let record = runtime
            .session(session_id)
            .await
            .context("actor")?
            .record()
            .await
            .flatten()
            .context("record")?;
        let restored = runtime
            .hydrate_runtime_session(session_id, &record.rollout_path)
            .await?;
        let state = SessionActorState::from_runtime_session(restored);
        assert!(
            runtime
                .prepare_turn_memory(
                    &state,
                    TurnId::new(),
                    "Use tabs",
                    &TurnInputMode::VisibleUserMessage
                )
                .await
                .is_some()
        );
        let history = devo_core::read_canonical_history(&record.rollout_path)?;
        let recall = history.items.last().context("recall item")?;
        assert!(matches!(recall.item, Item::MemoryRecall { .. }));
        let restored = runtime
            .hydrate_runtime_session(session_id, &record.rollout_path)
            .await?;
        assert_eq!(restored.next_item_seq, recall.seq + 1);
        runtime.shutdown().await;
        Ok(())
    }
}
