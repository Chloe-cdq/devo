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
            return state.inherited_memory.clone();
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
#[path = "memory_admission_tests.rs"]
mod admission_tests;

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

    /// Trace: L2-DES-MEM-001 Rev 4 DD-6
    /// Verifies: Native delegation cannot freeze a root snapshot before recall preparation completes.
    #[tokio::test]
    async fn delegation_waits_for_parent_memory_preparation() -> Result<()> {
        use crate::runtime::session_actor::state::TurnMemoryPreparation;
        use crate::support::{
            StreamScript, start_turn_with_approval_policy, wait_for_stream_calls,
        };
        use devo_core::tools::AgentToolCoordinator;
        let data = configured_data_root()?;
        let provider = Arc::new(ScriptedProvider::new([
            StreamScript::Pending,
            ScriptedProvider::completed("child"),
        ]));
        let runtime = build_runtime_with_workspace_config(data.path(), provider.clone())?;
        let (connection, _notifications, parent) =
            start_subscribed_session(&runtime, data.path(), /*request_id*/ 180).await?;
        remember(&runtime, connection, /*request_id*/ 181, "Use tabs").await?;
        start_turn_with_approval_policy(&runtime, connection, parent, "Use tabs", Some("never"))
            .await?;
        wait_for_stream_calls(&provider, /*expected*/ 1).await?;
        let mut pending = runtime
            .active_spawn_snapshot_for_session(parent)
            .await
            .context("active snapshot")?;
        let turn_id = pending.parent_active_turn_id.context("parent turn")?;
        let original = match &*pending.prepared_memory.borrow() {
            TurnMemoryPreparation::Ready(context) => context.clone(),
            TurnMemoryPreparation::Pending => panic!("provider request requires prepared memory"),
        };
        let readiness = pending.prepared_memory.clone();
        readiness.send_replace(TurnMemoryPreparation::Pending);
        pending.prepared_memory = tokio::sync::watch::channel(TurnMemoryPreparation::Pending).0;
        runtime
            .register_turn_spawn_snapshot(parent, turn_id, Arc::new(pending))
            .await;
        let spawn = runtime
            .clone()
            .spawn_agent(devo_protocol::SpawnAgentParams {
                session_id: parent,
                message: "Use tabs".into(),
                fork_turns: Some("none".into()),
                max_turns: None,
                tool_policy: devo_protocol::AgentToolPolicy::Inherit,
                ephemeral: true,
            });
        tokio::pin!(spawn);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(/*millis*/ 50), &mut spawn)
                .await
                .is_err(),
            "delegation must wait for the pending parent snapshot"
        );
        readiness.send_replace(TurnMemoryPreparation::Ready(original.clone()));
        tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 5), spawn).await??;
        wait_for_stream_calls(&provider, /*expected*/ 2).await?;
        let request = provider.requests().last().context("child request")?.clone();
        let inherited = crate::support::message_texts(&request)
            .into_iter()
            .find(|text| text.contains("<advisory_memory>"));
        assert_eq!(inherited.as_deref(), original.as_deref());
        runtime.shutdown().await;
        Ok(())
    }

    /// Trace: L2-DES-MEM-001 Rev 4 DD-6
    /// Verifies: manual compaction, which never prepares recall, permits prepared-empty delegation.
    #[tokio::test]
    async fn delegation_during_manual_compaction_has_no_recall_wait() -> Result<()> {
        use crate::support::{
            StreamScript, start_turn_with_approval_policy, wait_for_stream_calls,
        };
        use devo_core::tools::AgentToolCoordinator;
        let data = configured_data_root()?;
        let provider = Arc::new(ScriptedProvider::new([
            StreamScript::Pending,
            ScriptedProvider::completed("child"),
        ]));
        let runtime = build_runtime_with_workspace_config(data.path(), provider.clone())?;
        let (connection, _notifications, parent) =
            start_subscribed_session(&runtime, data.path(), /*request_id*/ 190).await?;
        start_turn_with_approval_policy(&runtime, connection, parent, "parent", Some("never"))
            .await?;
        wait_for_stream_calls(&provider, /*expected*/ 1).await?;
        let handle = runtime.session(parent).await.context("parent actor")?;
        let record = handle.record().await.flatten().context("parent record")?;
        let restored = runtime
            .hydrate_runtime_session(parent, &record.rollout_path)
            .await?;
        let mut state = SessionActorState::from_runtime_session(restored);
        let mut compaction = runtime
            .active_turns
            .active_turn_metadata(parent)
            .await
            .context("active turn")?;
        compaction.kind = devo_core::TurnKind::ManualCompaction;
        let turn_id = compaction.turn_id;
        state.active_turn = Some(compaction);
        runtime.clear_turn_spawn_snapshot(parent, turn_id).await;
        runtime
            .register_turn_spawn_snapshot(parent, turn_id, Arc::new(state.spawn_snapshot()))
            .await;
        let spawn = runtime
            .clone()
            .spawn_agent(devo_protocol::SpawnAgentParams {
                session_id: parent,
                message: "child".into(),
                fork_turns: Some("none".into()),
                max_turns: None,
                tool_policy: devo_protocol::AgentToolPolicy::Inherit,
                ephemeral: true,
            });
        let result = tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 5), spawn).await;
        assert!(
            result.is_ok(),
            "manual compaction has no pending recall preparation"
        );
        result??;
        wait_for_stream_calls(&provider, /*expected*/ 2).await?;
        assert!(
            crate::support::message_texts(provider.requests().last().context("child request")?)
                .iter()
                .all(|text| !text.contains("<advisory_memory>"))
        );
        runtime.shutdown().await;
        Ok(())
    }
}
