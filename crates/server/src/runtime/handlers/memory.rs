use super::super::*;

use crate::memory::ListMemoryRequest;
use crate::memory::MemoryCommand;
use crate::memory::MemoryCommandResult;
use crate::memory::MemoryError;
use crate::memory::MemoryRememberRequest;

impl ServerRuntime {
    /// Native `memory/status`: reports safe aggregate state without exposing
    /// database internals. Memory failures degrade to an unavailable status so
    /// ordinary session operations remain usable.
    pub(crate) async fn handle_native_memory_status(
        self: &Arc<Self>,
        request_id: serde_json::Value,
        params: serde_json::Value,
    ) -> serde_json::Value {
        if let Err(error) =
            serde_json::from_value::<devo_protocol::native::rpc_memory::MemoryStatusParams>(params)
        {
            return self.error_response(
                request_id,
                ProtocolErrorCode::InvalidParams,
                format!("invalid memory/status params: {error}"),
            );
        }

        let configured_enabled = self
            .deps
            .config_store
            .lock()
            .map(|store| store.effective_config().memory.enabled)
            .unwrap_or(false);
        let status = match self.memory.as_ref() {
            Some(memory) => match memory.execute_command(MemoryCommand::Status).await {
                Ok(MemoryCommandResult::Status(status)) => status,
                Ok(MemoryCommandResult::Remember(_)) | Ok(MemoryCommandResult::List(_)) => {
                    tracing::error!("memory status command returned an unexpected result");
                    unavailable_memory_status(configured_enabled)
                }
                Err(error) => {
                    tracing::warn!(%error, "memory status unavailable");
                    unavailable_memory_status(configured_enabled)
                }
            },
            None => unavailable_memory_status(configured_enabled),
        };
        serde_json::to_value(SuccessResponse {
            id: request_id,
            result: status,
        })
        .expect("serialize memory/status response")
    }

    /// Native `memory/remember`: commits an explicit User-scope memory and
    /// returns the canonical entry projection.
    pub(crate) async fn handle_native_memory_remember(
        self: &Arc<Self>,
        connection_id: u64,
        request_id: serde_json::Value,
        params: serde_json::Value,
    ) -> serde_json::Value {
        let params: devo_protocol::native::rpc_memory::MemoryRememberParams =
            match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return self.error_response(
                        request_id,
                        ProtocolErrorCode::InvalidParams,
                        format!("invalid memory/remember params: {error}"),
                    );
                }
            };
        if params.scope != devo_protocol::native::rpc_memory::MemoryScope::User {
            return self.error_response(
                request_id,
                ProtocolErrorCode::InvalidParams,
                "memory/remember currently accepts only User scope",
            );
        }
        let Some(memory) = self.memory.as_ref() else {
            return self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                "memory runtime is unavailable",
            );
        };
        let (source_session_id, source_turn_id) = if let Some((session_id, turn)) = self
            .active_turns
            .session_for_connection(connection_id)
            .await
        {
            let item_matches = if let Some(stream) = self.active_stream_state(session_id).await {
                let stream = stream.lock().await;
                stream.turn_inline.as_ref().is_some_and(|inline| {
                    inline.turn_id == turn.turn_id
                        && inline.persisted_turn_items.iter().any(|item| {
                            item.turn_id == turn.turn_id
                                && item.item_id.to_string()
                                    == params.source_user_item_id.to_string()
                                && matches!(&item.turn_item, devo_core::TurnItem::UserMessage(_))
                        })
                })
            } else {
                false
            };
            if !item_matches {
                return self.error_response(
                    request_id,
                    ProtocolErrorCode::InvalidParams,
                    "memory/remember source item is not the current user message",
                );
            }
            (session_id.to_string(), Some(turn.turn_id.to_string()))
        } else if let Some(session_id) = self.subscribed_session_for_connection(connection_id).await
        {
            (session_id.to_string(), None)
        } else {
            return self.error_response(
                request_id,
                ProtocolErrorCode::InvalidParams,
                "memory/remember requires a session-bound connection",
            );
        };
        let result = memory
            .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
                text: params.text,
                scope: params.scope,
                kind: params.kind,
                source_user_item_id: params.source_user_item_id.to_string(),
                source_session_id,
                source_turn_id,
                workspace_root: std::path::PathBuf::new(),
            }))
            .await;
        match result {
            Ok(MemoryCommandResult::Remember(entry)) => serde_json::to_value(SuccessResponse {
                id: request_id,
                result: entry,
            })
            .expect("serialize memory/remember response"),
            Ok(_) => self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                "memory/remember returned an unexpected result",
            ),
            Err(error) => self.memory_error_response(request_id, error),
        }
    }

    /// Native `memory/list`: exposes only canonical User-scope entries with
    /// bounded offset pagination and safe provenance fields.
    pub(crate) async fn handle_native_memory_list(
        self: &Arc<Self>,
        request_id: serde_json::Value,
        params: serde_json::Value,
    ) -> serde_json::Value {
        let params: devo_protocol::native::rpc_memory::MemoryListParams =
            match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return self.error_response(
                        request_id,
                        ProtocolErrorCode::InvalidParams,
                        format!("invalid memory/list params: {error}"),
                    );
                }
            };
        if let Some(scope) = params.scope
            && scope != devo_protocol::native::rpc_memory::MemoryScope::User
        {
            return self.error_response(
                request_id,
                ProtocolErrorCode::InvalidParams,
                "memory/list currently accepts only User scope",
            );
        }
        let Some(memory) = self.memory.as_ref() else {
            return self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                "memory runtime is unavailable",
            );
        };
        let result = memory
            .execute_command(MemoryCommand::List(ListMemoryRequest {
                scope: Some(devo_protocol::native::rpc_memory::MemoryScope::User),
                kind: params.kind,
                state: params.state,
                origin: params.origin,
                text: params.text,
                cursor: params.cursor,
                limit: params.limit,
                workspace_root: std::path::PathBuf::new(),
            }))
            .await;
        match result {
            Ok(MemoryCommandResult::List(page)) => serde_json::to_value(SuccessResponse {
                id: request_id,
                result: page,
            })
            .expect("serialize memory/list response"),
            Ok(_) => self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                "memory/list returned an unexpected result",
            ),
            Err(error) => self.memory_error_response(request_id, error),
        }
    }

    fn memory_error_response(
        &self,
        request_id: serde_json::Value,
        error: MemoryError,
    ) -> serde_json::Value {
        let (code, message) = match error {
            MemoryError::InvalidRequest(message) => (ProtocolErrorCode::InvalidParams, message),
            MemoryError::SecretContentRejected => (
                ProtocolErrorCode::InvalidParams,
                "memory content was rejected for safety".to_string(),
            ),
            MemoryError::Disabled => (
                ProtocolErrorCode::InternalError,
                "memory is disabled".to_string(),
            ),
            _ => (
                ProtocolErrorCode::InternalError,
                "memory operation is unavailable".to_string(),
            ),
        };
        self.error_response(request_id, code, message)
    }
}

fn unavailable_memory_status(enabled: bool) -> devo_protocol::native::rpc_memory::MemoryStatus {
    devo_protocol::native::rpc_memory::MemoryStatus {
        enabled,
        storage_health: "unavailable".into(),
        entry_count: 0,
        candidate_count: 0,
        pending_job_count: 0,
        retrying_job_count: 0,
        error_job_count: 0,
        last_successful_scan_at: None,
        error_classes: Vec::new(),
    }
}
