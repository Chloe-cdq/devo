use std::path::Path;

use anyhow::Result;
use chrono::Utc;
use devo_core::{InternalRecordV2, RolloutLineV2, SessionId};
use devo_protocol::native::ids::SessionId as NativeSessionId;

use super::RolloutStore;

impl RolloutStore {
    /// Persist the session-wide external-context fact once, before a local
    /// external tool is dispatched. The per-file write lock serializes calls.
    pub(crate) fn mark_external_context_used_at(
        &self,
        rollout_path: &Path,
        session_id: SessionId,
    ) -> Result<()> {
        self.with_locked_write_state(rollout_path, |state| {
            if state.external_context_used {
                return Ok(());
            }
            let line = RolloutLineV2::Internal {
                v: 2,
                timestamp: Utc::now(),
                session_id: NativeSessionId::from_legacy_uuid(uuid::Uuid::from(session_id)),
                turn_id: None,
                seq: state.next_line_index,
                entry: InternalRecordV2::ExternalContextUsed,
            };
            self.write_v2_lines(rollout_path, state, std::slice::from_ref(&line))?;
            state.projector.observe_v2_line(&line);
            state.external_context_used = true;
            Ok(())
        })
    }
}
