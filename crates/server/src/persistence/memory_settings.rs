use std::path::Path;

use anyhow::Result;
use chrono::Utc;
use devo_core::RolloutLine;
use devo_core::SessionId;
use devo_core::SessionSettingsField;
use devo_core::SessionSettingsLine;
use devo_protocol::native::session::MemorySetting;

use super::ReplayState;
use super::RolloutStore;

impl RolloutStore {
    /// Appends one field-level session settings line without requiring the
    /// actor-owned session record (L2-DES-CONV-002 Phase 2 persist-first
    /// path: the handler must not wait on the actor mailbox to persist).
    pub(crate) fn append_session_settings_at(
        &self,
        rollout_path: &Path,
        session_id: SessionId,
        field: SessionSettingsField,
        value: serde_json::Value,
    ) -> Result<()> {
        self.append_session_settings_batch_at(rollout_path, session_id, &[(field, value)])
    }

    /// Appends several field-level settings lines under one file lock and one
    /// fsync so concurrent patches cannot interleave. A crash may retain a
    /// complete prefix because rollout records remain independently replayable.
    pub(crate) fn append_session_settings_batch_at(
        &self,
        rollout_path: &Path,
        session_id: SessionId,
        settings: &[(SessionSettingsField, serde_json::Value)],
    ) -> Result<()> {
        let lines = settings
            .iter()
            .map(|(field, value)| {
                RolloutLine::SessionSettings(SessionSettingsLine {
                    timestamp: Utc::now(),
                    session_id,
                    field: *field,
                    value: value.clone(),
                    // Placeholder: the per-file projector assigns the
                    // authoritative epoch at write time.
                    epoch: 0,
                })
            })
            .collect::<Vec<_>>();
        self.append_lines(rollout_path, &lines)
    }

    /// Persists non-default memory settings when a new session inherits an
    /// existing session snapshot, such as a fork.
    pub(crate) fn append_inherited_memory_settings_at(
        &self,
        rollout_path: &Path,
        session_id: SessionId,
        settings: crate::memory::SessionMemorySettings,
    ) -> Result<()> {
        let mut updates = Vec::with_capacity(2);
        if settings.recall != MemorySetting::Inherit {
            updates.push((
                SessionSettingsField::MemoryRecall,
                serde_json::to_value(settings.recall).expect("serialize memory recall setting"),
            ));
        }
        if settings.contribution != MemorySetting::Inherit {
            updates.push((
                SessionSettingsField::MemoryContribution,
                serde_json::to_value(settings.contribution)
                    .expect("serialize memory contribution setting"),
            ));
        }
        self.append_session_settings_batch_at(rollout_path, session_id, &updates)
    }
}

impl ReplayState {
    pub(super) fn memory_settings(&self) -> crate::memory::SessionMemorySettings {
        let setting = |field: SessionSettingsField| {
            self.session_settings
                .get(&field)
                .and_then(|value| serde_json::from_value(value.clone()).ok())
                .unwrap_or_default()
        };
        crate::memory::SessionMemorySettings {
            recall: setting(SessionSettingsField::MemoryRecall),
            contribution: setting(SessionSettingsField::MemoryContribution),
        }
    }
}
