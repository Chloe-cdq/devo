use super::ServerRuntime;
use crate::memory::ProjectMemorySession;
use crate::memory::ProjectMemorySessionActivity;
use devo_core::SessionId;

impl ServerRuntime {
    /// Collects runtime-owned Session facts for Project command execution.
    /// Canonical repository identity and ambiguity stay inside MemoryRuntime.
    pub(super) async fn project_memory_sessions(
        &self,
        connection_id: u64,
        active_session_ids: &[SessionId],
    ) -> Vec<ProjectMemorySession> {
        let mut session_ids = active_session_ids.to_vec();
        for session_id in self.native_session_ids_for_connection(connection_id).await {
            if !session_ids.contains(&session_id) {
                session_ids.push(session_id);
            }
        }
        let mut sessions = Vec::with_capacity(session_ids.len());
        for session_id in session_ids {
            sessions.push(ProjectMemorySession {
                session_id,
                workspace_root: self
                    .session_summary_snapshot(session_id)
                    .await
                    .map(|summary| summary.cwd),
                activity: if active_session_ids.contains(&session_id) {
                    ProjectMemorySessionActivity::Active
                } else {
                    ProjectMemorySessionActivity::Inactive
                },
            });
        }
        sessions
    }
}
