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
    active: bool,
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
            if candidate.active && !current.active {
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
        active_session_id: Option<SessionId>,
    ) -> Result<ProjectMemoryContext, ProjectMemoryContextError> {
        let Some(memory) = self.memory.as_ref() else {
            return Err(ProjectMemoryContextError::NoSession);
        };
        let mut session_ids = self.native_session_ids_for_connection(connection_id).await;
        if let Some(active_session_id) = active_session_id
            && !session_ids.contains(&active_session_id)
        {
            session_ids.push(active_session_id);
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
                active: Some(session_id) == active_session_id,
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
    use super::{ProjectMemoryCandidate, ProjectMemoryContext, ProjectMemoryContextError};

    fn candidate(session_id: SessionId, scope_id: &str, active: bool) -> ProjectMemoryCandidate {
        ProjectMemoryCandidate {
            context: ProjectMemoryContext {
                session_id,
                workspace_root: scope_id.into(),
                scope_id: scope_id.into(),
            },
            active,
        }
    }

    #[test]
    fn same_project_sessions_share_one_context() {
        let active_session = SessionId::new();
        let selected_session = SessionId::new();
        let expected = candidate(active_session, "project-a", true).context;

        let actual = choose_project_memory_context(vec![
            candidate(selected_session, "project-a", false),
            candidate(active_session, "project-a", true),
        ])
        .expect("same project sessions should be accepted");

        assert_eq!(actual, expected);
    }

    #[test]
    fn different_projects_are_ambiguous() {
        let result = choose_project_memory_context(vec![
            candidate(SessionId::new(), "project-a", true),
            candidate(SessionId::new(), "project-b", false),
        ]);

        assert_eq!(result, Err(ProjectMemoryContextError::Ambiguous));
    }
}
