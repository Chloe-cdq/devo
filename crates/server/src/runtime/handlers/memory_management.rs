use super::super::*;
use crate::memory::{
    MemoryCommand, MemoryCommandResult, ProjectMemoryOperation, ScopedMemoryRequest,
};
use devo_protocol::native::rpc_memory::{MemoryExportParams, MemoryScope};

impl ServerRuntime {
    /// Native scoped management. Dispatch is the client's confirmed reset command.
    pub(crate) async fn handle_native_memory_management(
        self: &Arc<Self>,
        connection_id: u64,
        request_id: serde_json::Value,
        method: &'static str,
        params: serde_json::Value,
    ) -> serde_json::Value {
        // Export and reset share the same required-scope wire shape.
        let params: MemoryExportParams = match serde_json::from_value(params) {
            Ok(params) => params,
            Err(error) => {
                return self.error_response(
                    request_id,
                    ProtocolErrorCode::InvalidParams,
                    format!("invalid {method} params: {error}"),
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
        let command = match params.scope {
            MemoryScope::User => {
                let request = ScopedMemoryRequest {
                    scope: MemoryScope::User,
                    workspace_root: Default::default(),
                };
                if method == "memory/export" {
                    MemoryCommand::Export(request)
                } else {
                    let context = self.memory_command_sessions(connection_id, &[]).await;
                    MemoryCommand::ResetUser {
                        user_session: context.user_session,
                        sessions: context.sessions,
                    }
                }
            }
            MemoryScope::Project => {
                let active_session_ids = self
                    .active_turns
                    .turns_for_connection(connection_id)
                    .await
                    .into_iter()
                    .map(|(session_id, _)| session_id)
                    .collect::<Vec<_>>();
                let candidates = self
                    .memory_command_sessions(connection_id, &active_session_ids)
                    .await
                    .sessions;
                MemoryCommand::Project {
                    candidates,
                    operation: if method == "memory/export" {
                        ProjectMemoryOperation::Export
                    } else {
                        ProjectMemoryOperation::Reset
                    },
                }
            }
        };
        let result = match memory.execute_command(command).await {
            Ok(MemoryCommandResult::Export(result)) => serde_json::to_value(result),
            Ok(MemoryCommandResult::Reset(result)) => serde_json::to_value(result),
            Ok(MemoryCommandResult::Status(_))
            | Ok(MemoryCommandResult::Remember(_))
            | Ok(MemoryCommandResult::PreparedForget(_))
            | Ok(MemoryCommandResult::Forget(_))
            | Ok(MemoryCommandResult::List(_))
            | Ok(MemoryCommandResult::Read(_))
            | Ok(MemoryCommandResult::Search(_)) => {
                return self.error_response(
                    request_id,
                    ProtocolErrorCode::InternalError,
                    "memory management returned an unexpected result",
                );
            }
            Err(error) => return self.memory_error_response(request_id, method, error),
        }
        .expect("serialize memory management result");
        serde_json::to_value(SuccessResponse {
            id: request_id,
            result,
        })
        .expect("serialize memory management response")
    }
}
