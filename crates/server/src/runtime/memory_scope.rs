use std::path::PathBuf;

use devo_core::SessionId;
use thiserror::Error;

use super::ServerRuntime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProjectMemoryContext {
    pub(super) session_id: SessionId,
    pub(super) workspace_root: PathBuf,
    pub(super) scope_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectMemoryCandidate {
    context: ProjectMemoryContext,
    activity: ProjectMemoryActivity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectMemoryActivity {
    Active,
    Inactive,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(super) enum ProjectMemoryContextError {
    #[error("requires a Native Session selector or active turn")]
    NoSession,
    #[error("has ambiguous Native Session selectors")]
    Ambiguous,
    #[error("session {0} has no workspace summary")]
    SessionUnavailable(SessionId),
    #[error("failed to resolve Project identity: {0}")]
    ProjectIdentity(String),
}

fn choose_project_memory_context(
    candidates: Vec<ProjectMemoryCandidate>,
) -> Result<ProjectMemoryContext, ProjectMemoryContextError> {
    let mut selected: Option<ProjectMemoryCandidate> = None;
    for candidate in candidates {
        if let Some(current) = selected.as_ref() {
            if current.context.scope_id != candidate.context.scope_id {
                return Err(ProjectMemoryContextError::Ambiguous);
            }
            if candidate.activity == ProjectMemoryActivity::Active
                && current.activity == ProjectMemoryActivity::Inactive
            {
                selected = Some(candidate);
            }
        } else {
            selected = Some(candidate);
        }
    }
    selected
        .map(|candidate| candidate.context)
        .ok_or(ProjectMemoryContextError::NoSession)
}

impl ServerRuntime {
    pub(super) async fn project_memory_context(
        &self,
        connection_id: u64,
        active_session_ids: &[SessionId],
    ) -> Result<ProjectMemoryContext, ProjectMemoryContextError> {
        let Some(memory) = self.memory.as_ref() else {
            return Err(ProjectMemoryContextError::NoSession);
        };
        let mut session_ids = self.native_session_ids_for_connection(connection_id).await;
        for active_session_id in active_session_ids {
            if !session_ids.contains(active_session_id) {
                session_ids.push(*active_session_id);
            }
        }

        let mut candidates = Vec::with_capacity(session_ids.len());
        for session_id in session_ids {
            let Some(summary) = self.session_summary_snapshot(session_id).await else {
                return Err(ProjectMemoryContextError::SessionUnavailable(session_id));
            };
            let scope_id = memory
                .project_scope_id(&summary.cwd)
                .map_err(|error| ProjectMemoryContextError::ProjectIdentity(error.to_string()))?;
            candidates.push(ProjectMemoryCandidate {
                context: ProjectMemoryContext {
                    session_id,
                    workspace_root: summary.cwd,
                    scope_id,
                },
                activity: if active_session_ids.contains(&session_id) {
                    ProjectMemoryActivity::Active
                } else {
                    ProjectMemoryActivity::Inactive
                },
            });
        }
        choose_project_memory_context(candidates)
    }
}

#[cfg(test)]
mod tests {
    use devo_core::SessionId;
    use pretty_assertions::assert_eq;

    use super::choose_project_memory_context;
    use super::{
        ProjectMemoryActivity, ProjectMemoryCandidate, ProjectMemoryContext,
        ProjectMemoryContextError,
    };

    fn candidate(
        session_id: SessionId,
        scope_id: &str,
        activity: ProjectMemoryActivity,
    ) -> ProjectMemoryCandidate {
        ProjectMemoryCandidate {
            context: ProjectMemoryContext {
                session_id,
                workspace_root: scope_id.into(),
                scope_id: scope_id.into(),
            },
            activity,
        }
    }

    /// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3
    /// Verifies: same-project active sessions select the active context.
    #[test]
    fn same_project_sessions_share_one_context() {
        let active_session = SessionId::new();
        let selected_session = SessionId::new();
        let expected =
            candidate(active_session, "project-a", ProjectMemoryActivity::Active).context;

        let actual = choose_project_memory_context(vec![
            candidate(
                selected_session,
                "project-a",
                ProjectMemoryActivity::Inactive,
            ),
            candidate(active_session, "project-a", ProjectMemoryActivity::Active),
        ])
        .expect("same project sessions should be accepted");

        assert_eq!(actual, expected);
    }

    /// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3
    /// Verifies: active sessions from different projects are ambiguous.
    #[test]
    fn different_projects_are_ambiguous() {
        let result = choose_project_memory_context(vec![
            candidate(SessionId::new(), "project-a", ProjectMemoryActivity::Active),
            candidate(
                SessionId::new(),
                "project-b",
                ProjectMemoryActivity::Inactive,
            ),
        ]);

        assert_eq!(result, Err(ProjectMemoryContextError::Ambiguous));
    }
}
