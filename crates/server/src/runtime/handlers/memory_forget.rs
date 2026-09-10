use super::super::*;

use crate::memory::MemoryCommand;
use crate::memory::MemoryCommandResult;
use crate::memory::MemoryForgetRequest;

impl ServerRuntime {
    /// Native `memory/forget`: retires an exact entry or returns candidates
    /// for an ambiguous text selector without mutating memory.
    pub(crate) async fn handle_native_memory_forget(
        self: &Arc<Self>,
        connection_id: u64,
        request_id: serde_json::Value,
        params: serde_json::Value,
    ) -> serde_json::Value {
        let params: devo_protocol::native::rpc_memory::MemoryForgetParams =
            match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return self.error_response(
                        request_id,
                        ProtocolErrorCode::InvalidParams,
                        format!("invalid memory/forget params: {error}"),
                    );
                }
            };
        let Some(memory) = self.memory.as_ref() else {
            return self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                "memory runtime is unavailable",
            );
        };
        let active_turns = self.active_turns.turns_for_connection(connection_id).await;
        let active_session_ids = active_turns
            .iter()
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();
        let active_source = if active_turns.is_empty() {
            None
        } else {
            let Some(source_user_item_id) = params.source_user_item_id.as_ref() else {
                return self.error_response(
                    request_id,
                    ProtocolErrorCode::InvalidParams,
                    "memory/forget in an active turn requires sourceUserItemId",
                );
            };
            let mut active_source = None;
            for (session_id, turn) in &active_turns {
                let item_matches = if let Some(stream) = self.active_stream_state(*session_id).await
                {
                    let stream = stream.lock().await;
                    stream.turn_inline.as_ref().is_some_and(|inline| {
                        inline.turn_id == turn.turn_id
                            && inline.persisted_turn_items.iter().any(|item| {
                                item.turn_id == turn.turn_id
                                    && item.item_id.to_string() == source_user_item_id.to_string()
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
                        Some(source_user_item_id.to_string()),
                    ));
                    break;
                }
            }
            let Some(active_source) = active_source else {
                return self.error_response(
                    request_id,
                    ProtocolErrorCode::InvalidParams,
                    "memory/forget source item is not the current user message",
                );
            };
            Some(active_source)
        };
        let (source_session_id, source_turn_id, source_user_item_id, workspace_root) =
            match params.scope {
                devo_protocol::native::rpc_memory::MemoryScope::Project => {
                    if active_source.is_none() && params.source_user_item_id.is_some() {
                        return self.error_response(
                            request_id,
                            ProtocolErrorCode::InvalidParams,
                            "direct memory/forget commands must omit sourceUserItemId",
                        );
                    }
                    let context = match self
                        .project_memory_context(connection_id, &active_session_ids)
                        .await
                    {
                        Ok(context) => context,
                        Err(error) => {
                            return self.project_memory_context_error_response(
                                request_id,
                                "memory/forget",
                                error,
                            );
                        }
                    };
                    let (source_turn_id, source_user_item_id) = active_source
                        .as_ref()
                        .map(|source| (source.1.clone(), source.2.clone()))
                        .unwrap_or((None, None));
                    let source_session_id = active_source
                        .as_ref()
                        .map(|source| source.0)
                        .unwrap_or(context.session_id);
                    (
                        source_session_id,
                        source_turn_id,
                        source_user_item_id,
                        context.workspace_root,
                    )
                }
                devo_protocol::native::rpc_memory::MemoryScope::User => {
                    let (source_session_id, source_turn_id, source_user_item_id) =
                        if let Some(source) = active_source {
                            source
                        } else if let Some(session_id) =
                            self.subscribed_session_for_connection(connection_id).await
                        {
                            if params.source_user_item_id.is_some() {
                                return self.error_response(
                                    request_id,
                                    ProtocolErrorCode::InvalidParams,
                                    "direct memory/forget commands must omit sourceUserItemId",
                                );
                            }
                            (session_id, None, None)
                        } else {
                            return self.error_response(
                                request_id,
                                ProtocolErrorCode::InvalidParams,
                                "memory/forget requires a session-bound connection",
                            );
                        };
                    let Some(workspace_root) = self
                        .session_summary_snapshot(source_session_id)
                        .await
                        .map(|summary| summary.cwd)
                    else {
                        return self.error_response(
                            request_id,
                            ProtocolErrorCode::InvalidParams,
                            "memory/forget requires a session with a workspace root",
                        );
                    };
                    (
                        source_session_id,
                        source_turn_id,
                        source_user_item_id,
                        workspace_root,
                    )
                }
            };
        let result = memory
            .execute_command(MemoryCommand::Forget(MemoryForgetRequest {
                entry_id: params.entry_id,
                text: params.text,
                scope: params.scope,
                source_user_item_id,
                source_session_id: source_session_id.to_string(),
                source_turn_id,
                workspace_root,
            }))
            .await;
        match result {
            Ok(MemoryCommandResult::Forget(result)) => serde_json::to_value(SuccessResponse {
                id: request_id,
                result,
            })
            .expect("serialize memory/forget response"),
            Ok(MemoryCommandResult::Status(_))
            | Ok(MemoryCommandResult::Remember(_))
            | Ok(MemoryCommandResult::RememberInferred(_))
            | Ok(MemoryCommandResult::List(_)) => self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                "memory/forget returned an unexpected result",
            ),
            Err(error) => self.memory_error_response(request_id, error),
        }
    }
}
