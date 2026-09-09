use super::super::*;

use super::super::memory_scope::ProjectMemoryContextError;
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

    /// Native `memory/remember`: commits an explicit User- or Project-scope memory and
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
                    "memory/remember in an active turn requires sourceUserItemId",
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
                    "memory/remember source item is not the current user message",
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
                            "direct memory/remember commands must omit sourceUserItemId",
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
                                "memory/remember",
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
                                    "direct memory/remember commands must omit sourceUserItemId",
                                );
                            }
                            (session_id, None, None)
                        } else {
                            return self.error_response(
                                request_id,
                                ProtocolErrorCode::InvalidParams,
                                "memory/remember requires a session-bound connection",
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
                            "memory/remember requires a session with a workspace root",
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
            .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
                text: params.text,
                scope: params.scope,
                kind: params.kind,
                source_user_item_id,
                source_session_id: source_session_id.to_string(),
                source_turn_id,
                workspace_root,
            }))
            .await;
        match result {
            Ok(MemoryCommandResult::Remember(entry)) => serde_json::to_value(SuccessResponse {
                id: request_id,
                result: entry,
            })
            .expect("serialize memory/remember response"),
            Ok(MemoryCommandResult::Status(_)) | Ok(MemoryCommandResult::List(_)) => self
                .error_response(
                    request_id,
                    ProtocolErrorCode::InternalError,
                    "memory/remember returned an unexpected result",
                ),
            Err(error) => self.memory_error_response(request_id, error),
        }
    }

    /// Native `memory/list`: exposes canonical User- or Project-scope entries with
    /// bounded offset pagination and safe provenance fields.
    pub(crate) async fn handle_native_memory_list(
        self: &Arc<Self>,
        connection_id: u64,
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
        let Some(memory) = self.memory.as_ref() else {
            return self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                "memory runtime is unavailable",
            );
        };
        let scope = params.scope.unwrap_or_default();
        let workspace_root = if scope == devo_protocol::native::rpc_memory::MemoryScope::Project {
            let active_session_ids = self
                .active_turns
                .turns_for_connection(connection_id)
                .await
                .into_iter()
                .map(|(session_id, _)| session_id)
                .collect::<Vec<_>>();
            let context = match self
                .project_memory_context(connection_id, &active_session_ids)
                .await
            {
                Ok(context) => context,
                Err(error) => {
                    return self.project_memory_context_error_response(
                        request_id,
                        "memory/list",
                        error,
                    );
                }
            };
            context.workspace_root
        } else {
            std::path::PathBuf::new()
        };
        let result = memory
            .execute_command(MemoryCommand::List(ListMemoryRequest {
                scope: Some(scope),
                kind: params.kind,
                state: params.state,
                origin: params.origin,
                text: params.text,
                cursor: params.cursor,
                limit: params.limit,
                workspace_root,
            }))
            .await;
        match result {
            Ok(MemoryCommandResult::List(page)) => serde_json::to_value(SuccessResponse {
                id: request_id,
                result: page,
            })
            .expect("serialize memory/list response"),
            Ok(MemoryCommandResult::Status(_)) | Ok(MemoryCommandResult::Remember(_)) => self
                .error_response(
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
            MemoryError::Directory(_)
            | MemoryError::Database(_)
            | MemoryError::LockPoisoned
            | MemoryError::InvalidCount(_)
            | MemoryError::InvalidTimestamp(_)
            | MemoryError::ProjectIdentity(_)
            | MemoryError::InvalidStoredValue(_) => (
                ProtocolErrorCode::InternalError,
                "memory operation is unavailable".to_string(),
            ),
        };
        self.error_response(request_id, code, message)
    }

    fn project_memory_context_error_response(
        &self,
        request_id: serde_json::Value,
        method: &str,
        error: ProjectMemoryContextError,
    ) -> serde_json::Value {
        let message = match error {
            ProjectMemoryContextError::NoSession => {
                format!("{method} Project scope requires a session-bound connection")
            }
            ProjectMemoryContextError::Ambiguous => {
                format!("{method} Project scope has ambiguous Native Session selectors")
            }
            ProjectMemoryContextError::SessionUnavailable(_) => {
                format!("{method} Project scope requires a session with a workspace root")
            }
            ProjectMemoryContextError::ProjectIdentity(_) => {
                format!("{method} Project scope identity is unavailable")
            }
        };
        self.error_response(request_id, ProtocolErrorCode::InvalidParams, message)
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
