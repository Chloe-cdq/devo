use std::path::Path;

use devo_core::SessionId;
use devo_core::SessionSettingsField;
use devo_protocol::native::rpc_session::SessionSettingsPatch;
use devo_protocol::native::session::MemorySetting;
use devo_protocol::native::session::SessionSettings;

use crate::memory::SessionMemorySettingsSnapshot;
use crate::persistence::RolloutStore;
use crate::runtime::session_actor::SessionHandle;

pub(super) struct MemorySettingsPatchPlan {
    recall: Option<MemorySetting>,
    contribution: Option<MemorySetting>,
    updates: Vec<(SessionSettingsField, serde_json::Value)>,
}

pub(super) enum PersistMemorySettingsError {
    Persistence(anyhow::Error),
    SessionUnavailable,
}

impl MemorySettingsPatchPlan {
    pub(super) fn new(current: &SessionSettings, patch: Option<&SessionSettingsPatch>) -> Self {
        let recall = patch
            .and_then(|patch| patch.memory_recall)
            .filter(|recall| *recall != current.memory_recall);
        let contribution = patch
            .and_then(|patch| patch.memory_contribution)
            .filter(|contribution| *contribution != current.memory_contribution);
        let mut updates = Vec::with_capacity(2);
        if let Some(recall) = recall {
            updates.push((
                SessionSettingsField::MemoryRecall,
                serde_json::to_value(recall).expect("serialize memory recall setting"),
            ));
        }
        if let Some(contribution) = contribution {
            updates.push((
                SessionSettingsField::MemoryContribution,
                serde_json::to_value(contribution).expect("serialize memory contribution setting"),
            ));
        }
        Self {
            recall,
            contribution,
            updates,
        }
    }

    pub(super) async fn persist(
        &self,
        rollout_store: &RolloutStore,
        session_handle: &SessionHandle,
        rollout_path: Option<&Path>,
        session_id: SessionId,
    ) -> Result<Option<SessionMemorySettingsSnapshot>, PersistMemorySettingsError> {
        if self.updates.is_empty() {
            return Ok(None);
        }
        if let Some(path) = rollout_path {
            rollout_store
                .append_session_settings_batch_at(path, session_id, &self.updates)
                .map_err(PersistMemorySettingsError::Persistence)?;
            if !session_handle.notify_memory_settings(self.recall, self.contribution) {
                tracing::warn!(
                    %session_id,
                    "failed to notify session actor of persisted memory settings"
                );
            }
            return Ok(None);
        }
        session_handle
            .update_memory_settings(self.recall, self.contribution)
            .await
            .map(Some)
            .ok_or(PersistMemorySettingsError::SessionUnavailable)
    }

    pub(super) fn apply_to(&self, settings: &mut SessionSettings) {
        if let Some(recall) = self.recall {
            settings.memory_recall = recall;
        }
        if let Some(contribution) = self.contribution {
            settings.memory_contribution = contribution;
        }
    }
}
