use super::ServerRuntime;
use crate::memory::MemoryUserSessionSelection;
use crate::memory::ProjectMemorySession;
use crate::memory::ProjectMemorySessionActivity;
use devo_core::SessionId;

pub(super) struct MemoryCommandSessions {
    pub(super) user_session: MemoryUserSessionSelection,
    pub(super) sessions: Vec<ProjectMemorySession>,
}

impl ServerRuntime {
    /// Derives User selection and candidate workspace facts from one Native
    /// selector snapshot. Project identity and resolution stay in MemoryRuntime.
    pub(super) async fn memory_command_sessions(
        &self,
        connection_id: u64,
        active_session_ids: &[SessionId],
    ) -> MemoryCommandSessions {
        let mut session_ids = active_session_ids.to_vec();
        let native_session_ids = self.native_session_ids_for_connection(connection_id).await;
        let user_session = match native_session_ids.as_slice() {
            [] => self
                .subscribed_session_for_connection(connection_id)
                .await
                .map(MemoryUserSessionSelection::Selected)
                .unwrap_or(MemoryUserSessionSelection::Unbound),
            [session_id] => MemoryUserSessionSelection::Selected(*session_id),
            [_, _, ..] => MemoryUserSessionSelection::Ambiguous,
        };
        // Native selectors are authoritative; the legacy delivery subscription
        // is only a fallback for connections without a Native Session selector.
        if native_session_ids.is_empty()
            && let MemoryUserSessionSelection::Selected(session_id) = user_session
            && !session_ids.contains(&session_id)
        {
            session_ids.push(session_id);
        }
        for session_id in native_session_ids {
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
        MemoryCommandSessions {
            user_session,
            sessions,
        }
    }
}
