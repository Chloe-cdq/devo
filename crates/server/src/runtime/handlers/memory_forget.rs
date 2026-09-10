use super::super::*;

use crate::memory::MemoryCommand;
use crate::memory::MemoryCommandResult;
use crate::memory::MemoryForgetRequest;
use crate::memory::MemoryForgetSelector;

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
        let source = match self
            .resolve_memory_mutation_source(
                connection_id,
                params.scope,
                params.source_user_item_id.as_ref(),
                "memory/forget",
                &request_id,
            )
            .await
        {
            Ok(source) => source,
            Err(response) => return response,
        };
        let result = memory
            .execute_command(MemoryCommand::Forget(MemoryForgetRequest {
                selector,
                scope: params.scope,
                source,
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
            | Ok(MemoryCommandResult::List(_)) => self.error_response(
                request_id,
                ProtocolErrorCode::InternalError,
                "memory/forget returned an unexpected result",
            ),
            Err(error) => self.memory_error_response(request_id, error),
        }
    }
}
