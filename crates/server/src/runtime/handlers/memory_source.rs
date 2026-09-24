use super::super::*;

use devo_protocol::native::rpc_memory::MemoryScope;

use crate::memory::{MemorySourceBinding, MemorySourceContext, ProjectMemorySession};

pub(super) struct ActiveMemoryMutationSource {
    pub(super) active_session_ids: Vec<devo_protocol::SessionId>,
    pub(super) source: Option<MemorySourceBinding>,
}

pub(super) enum MemoryMutationSource {
    User(MemorySourceContext),
    Project {
        candidates: Vec<ProjectMemorySession>,
        source: MemorySourceBinding,
    },
}

impl ServerRuntime {
    pub(super) async fn resolve_active_memory_mutation_source(
        &self,
        connection_id: u64,
        source_user_item_id: Option<&devo_protocol::native::ids::ItemId>,
        operation: &str,
        request_id: &serde_json::Value,
    ) -> Result<ActiveMemoryMutationSource, serde_json::Value> {
        let active_turns = self.active_turns.turns_for_connection(connection_id).await;
        let active_session_ids = active_turns
            .iter()
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();
        if active_turns.is_empty() && source_user_item_id.is_some() {
            return Err(self.error_response(
                request_id.clone(),
                ProtocolErrorCode::InvalidParams,
                format!("direct {operation} commands must omit sourceUserItemId"),
            ));
        }
        let source = if let Some(source_user_item_id) = source_user_item_id {
            let mut source = None;
            for (session_id, turn) in &active_turns {
                if self
                    .current_user_item_text(*session_id, turn.turn_id, source_user_item_id)
                    .await
                    .is_ok()
                {
                    source = Some(MemorySourceBinding {
                        session_id: Some(*session_id),
                        turn_id: Some(turn.turn_id),
                        user_item_id: Some(source_user_item_id.clone()),
                    });
                    break;
                }
            }
            let Some(source) = source else {
                return Err(self.error_response(
                    request_id.clone(),
                    ProtocolErrorCode::InvalidParams,
                    format!("{operation} source item is not the current user message"),
                ));
            };
            Some(source)
        } else {
            None
        };
        Ok(ActiveMemoryMutationSource {
            active_session_ids,
            source,
        })
    }

    pub(super) async fn resolve_memory_mutation_source(
        &self,
        connection_id: u64,
        scope: MemoryScope,
        source_user_item_id: Option<&devo_protocol::native::ids::ItemId>,
        operation: &str,
        request_id: &serde_json::Value,
    ) -> Result<MemoryMutationSource, serde_json::Value> {
        let active = self
            .resolve_active_memory_mutation_source(
                connection_id,
                source_user_item_id,
                operation,
                request_id,
            )
            .await?;
        let active_source = active.source;

        match scope {
            MemoryScope::Project => {
                if active_source.is_none() && source_user_item_id.is_some() {
                    return Err(self.error_response(
                        request_id.clone(),
                        ProtocolErrorCode::InvalidParams,
                        format!("direct {operation} commands must omit sourceUserItemId"),
                    ));
                }
                let candidates = self
                    .project_memory_sessions(connection_id, &active.active_session_ids)
                    .await;
                Ok(MemoryMutationSource::Project {
                    candidates,
                    source: active_source.unwrap_or_default(),
                })
            }
            MemoryScope::User => {
                let source = if let Some(source) = active_source {
                    source
                } else if let Some(session_id) =
                    self.subscribed_session_for_connection(connection_id).await
                {
                    if source_user_item_id.is_some() {
                        return Err(self.error_response(
                            request_id.clone(),
                            ProtocolErrorCode::InvalidParams,
                            format!("direct {operation} commands must omit sourceUserItemId"),
                        ));
                    }
                    MemorySourceBinding {
                        session_id: Some(session_id),
                        ..MemorySourceBinding::default()
                    }
                } else {
                    return Err(self.error_response(
                        request_id.clone(),
                        ProtocolErrorCode::InvalidParams,
                        format!("{operation} requires a session-bound connection"),
                    ));
                };
                let session_id = source.session_id.ok_or_else(|| {
                    self.error_response(
                        request_id.clone(),
                        ProtocolErrorCode::InvalidParams,
                        format!("{operation} requires a session-bound connection"),
                    )
                })?;
                let Some(workspace_root) = self
                    .session_summary_snapshot(session_id)
                    .await
                    .map(|summary| summary.cwd)
                else {
                    return Err(self.error_response(
                        request_id.clone(),
                        ProtocolErrorCode::InvalidParams,
                        format!("{operation} requires a session with a workspace root"),
                    ));
                };
                Ok(MemoryMutationSource::User(MemorySourceContext {
                    session_id,
                    turn_id: source.turn_id,
                    user_item_id: source.user_item_id,
                    workspace_root,
                }))
            }
        }
    }
}
