use super::super::*;

use crate::memory::{MemoryForgetRequest, MemoryForgetSelector, MemoryForgetSource};

use crate::runtime::memory_forget_authorization::{
    MemoryForgetExecutionError, complete_forget_execution,
};
use crate::runtime::memory_forget_preparation::prepare_forget;

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
        let selector = match MemoryForgetSelector::from_params(&params) {
            Ok(selector) => selector,
            Err(message) => {
                return self.error_response(request_id, ProtocolErrorCode::InvalidParams, message);
            }
        };
        let active_source = match self
            .resolve_active_memory_mutation_source(
                connection_id,
                params.source_user_item_id.as_ref(),
                "memory/forget",
                &request_id,
            )
            .await
        {
            Ok(source) => source,
            Err(response) => return response,
        };
        let user_session_id = match active_source
            .source
            .as_ref()
            .and_then(|source| source.session_id)
        {
            Some(session_id) => Some(session_id),
            None => self.subscribed_session_for_connection(connection_id).await,
        };
        let sessions = self
            .project_memory_sessions(connection_id, &active_source.active_session_ids)
            .await;
        let entry_id = match &selector {
            MemoryForgetSelector::EntryId(entry_id) => Some(entry_id.clone()),
            MemoryForgetSelector::Text(_) => None,
        };
        let prepared = match prepare_forget(
            self,
            memory,
            MemoryForgetRequest {
                selector,
                scope: params.scope,
                source: MemoryForgetSource {
                    bound_session_id: active_source.source.and_then(|source| source.session_id),
                    user_session_id,
                    sessions,
                },
            },
        )
        .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                return self.memory_error_response(request_id, "memory/forget", error);
            }
        };
        let source_session_id = prepared.source_session_id();
        let reservation = match self
            .memory_forget_coordinator
            .authorize_native(source_session_id, entry_id.as_ref())
        {
            Ok(reservation) => reservation,
            Err(error) => {
                return self.error_response(
                    request_id,
                    ProtocolErrorCode::InvalidParams,
                    error.to_string(),
                );
            }
        };
        let command = crate::memory::MemoryCommand::Forget(prepared);
        let result = self
            .deps
            .memory_command_executor
            .execute(memory, command)
            .await;
        match complete_forget_execution(reservation, result) {
            Ok(result) => serde_json::to_value(SuccessResponse {
                id: request_id,
                result,
            })
            .expect("serialize memory/forget response"),
            Err(MemoryForgetExecutionError::Projection(projection_error)) => self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                format!(
                    "memory forget committed but projection refresh failed: {projection_error}"
                ),
            ),
            Err(MemoryForgetExecutionError::UnexpectedResult) => self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                "memory/forget returned an unexpected result",
            ),
            Err(MemoryForgetExecutionError::Storage(error)) => {
                self.memory_error_response(request_id, "memory/forget", error)
            }
            Err(MemoryForgetExecutionError::Coordinator(error)) => self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                error.to_string(),
            ),
        }
    }
}
