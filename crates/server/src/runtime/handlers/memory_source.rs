use super::super::*;

use devo_core::SessionId;
use devo_protocol::native::rpc_memory::MemoryScope;

pub(super) struct MemoryMutationSource {
    pub(super) session_id: SessionId,
    pub(super) turn_id: Option<String>,
    pub(super) user_item_id: Option<String>,
    pub(super) workspace_root: std::path::PathBuf,
}

impl ServerRuntime {
    pub(super) async fn resolve_memory_mutation_source(
        &self,
        connection_id: u64,
        scope: MemoryScope,
        source_user_item_id: Option<&devo_protocol::native::ids::ItemId>,
        operation: &str,
        request_id: &serde_json::Value,
    ) -> Result<MemoryMutationSource, serde_json::Value> {
        let active_turns = self.active_turns.turns_for_connection(connection_id).await;
        let active_session_ids = active_turns
            .iter()
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();
        let active_source = if active_turns.is_empty() {
            None
        } else {
            let Some(source_user_item_id) = source_user_item_id else {
                return Err(self.error_response(
                    request_id.clone(),
                    ProtocolErrorCode::InvalidParams,
                    format!("{operation} in an active turn requires sourceUserItemId"),
                ));
            };
            let source_user_item_id = source_user_item_id.to_string();
            let mut active_source = None;
            for (session_id, turn) in &active_turns {
                let item_matches = if let Some(stream) = self.active_stream_state(*session_id).await
                {
                    let stream = stream.lock().await;
                    stream.turn_inline.as_ref().is_some_and(|inline| {
                        inline.turn_id == turn.turn_id
                            && inline.persisted_turn_items.iter().any(|item| {
                                item.turn_id == turn.turn_id
                                    && item.item_id.to_string() == source_user_item_id
                                    && matches!(
                                        &item.turn_item,
                                        devo_core::TurnItem::UserMessage(_)
                                    )
                            })
                    })
                } else {
                    false
                };
                if item_matches {
                    active_source = Some((
                        *session_id,
                        Some(turn.turn_id.to_string()),
                        Some(source_user_item_id.clone()),
                    ));
                    break;
                }
            }
            let Some(active_source) = active_source else {
                return Err(self.error_response(
                    request_id.clone(),
                    ProtocolErrorCode::InvalidParams,
                    format!("{operation} source item is not the current user message"),
                ));
            };
            Some(active_source)
        };

        let (session_id, turn_id, user_item_id, workspace_root) = match scope {
            MemoryScope::Project => {
                if active_source.is_none() && source_user_item_id.is_some() {
                    return Err(self.error_response(
                        request_id.clone(),
                        ProtocolErrorCode::InvalidParams,
                        format!("direct {operation} commands must omit sourceUserItemId"),
                    ));
                }
                let context = self
                    .project_memory_context(connection_id, &active_session_ids)
                    .await
                    .map_err(|error| {
                        self.project_memory_context_error_response(
                            request_id.clone(),
                            operation,
                            error,
                        )
                    })?;
                let (turn_id, user_item_id) = active_source
                    .as_ref()
                    .map(|source| (source.1.clone(), source.2.clone()))
                    .unwrap_or((None, None));
                let session_id = active_source
                    .as_ref()
                    .map(|source| source.0)
                    .unwrap_or(context.session_id);
                (session_id, turn_id, user_item_id, context.workspace_root)
            }
            MemoryScope::User => {
                let (session_id, turn_id, user_item_id) = if let Some(source) = active_source {
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
                    (session_id, None, None)
                } else {
                    return Err(self.error_response(
                        request_id.clone(),
                        ProtocolErrorCode::InvalidParams,
                        format!("{operation} requires a session-bound connection"),
                    ));
                };
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
                (session_id, turn_id, user_item_id, workspace_root)
            }
        };

        Ok(MemoryMutationSource {
            session_id,
            turn_id,
            user_item_id,
            workspace_root,
        })
    }
}
