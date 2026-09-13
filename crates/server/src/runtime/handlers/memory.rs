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
                Ok(MemoryCommandResult::Remember(_))
                | Ok(MemoryCommandResult::Forget(_))
                | Ok(MemoryCommandResult::List(_)) => {
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
        let source = match self
            .resolve_memory_mutation_source(
                connection_id,
                params.scope,
                params.source_user_item_id.as_ref(),
                "memory/remember",
                &request_id,
            )
            .await
        {
            Ok(source) => source,
            Err(response) => return response,
        };
        let result = memory
            .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
                text: params.text,
                scope: params.scope,
                kind: params.kind,
                source,
            }))
            .await;
        match result {
            Ok(MemoryCommandResult::Remember(entry)) => serde_json::to_value(SuccessResponse {
                id: request_id,
                result: entry,
            })
            .expect("serialize memory/remember response"),
            Ok(MemoryCommandResult::Status(_))
            | Ok(MemoryCommandResult::Forget(_))
            | Ok(MemoryCommandResult::List(_)) => self.error_response(
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
            Ok(MemoryCommandResult::Status(_))
            | Ok(MemoryCommandResult::Remember(_))
            | Ok(MemoryCommandResult::Forget(_)) => self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                "memory/list returned an unexpected result",
            ),
            Err(error) => self.memory_error_response(request_id, error),
        }
    }

    pub(super) fn memory_error_response(
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
            | MemoryError::InvalidStoredValue(_)
            | MemoryError::ForgetCommitted { .. } => (
                ProtocolErrorCode::InternalError,
                "memory operation is unavailable".to_string(),
            ),
        };
        self.error_response(request_id, code, message)
    }

    pub(super) fn project_memory_context_error_response(
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
