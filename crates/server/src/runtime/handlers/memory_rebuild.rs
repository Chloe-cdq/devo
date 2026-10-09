use super::super::*;
use crate::memory::scan::ScanTrigger;
use crate::memory::{MemoryCommand, MemoryCommandResult, MemoryUserSessionSelection};
use devo_protocol::native::rpc_memory::MemoryRebuildParams;

impl ServerRuntime {
    /// Dispatch asserts the client's confirmed intent. Acceptance commits before
    /// background journal reads or model requests, keeping foreground lanes free.
    pub(crate) async fn handle_native_memory_rebuild(
        self: &Arc<Self>,
        connection_id: u64,
        request_id: serde_json::Value,
        params: serde_json::Value,
    ) -> serde_json::Value {
        let params: MemoryRebuildParams = match serde_json::from_value(params) {
            Ok(params) => params,
            Err(error) => {
                return self.error_response(
                    request_id,
                    ProtocolErrorCode::InvalidParams,
                    format!("invalid memory/rebuild params: {error}"),
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
        let active_ids = self
            .active_turns
            .turns_for_connection(connection_id)
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        let context = self
            .memory_command_sessions(connection_id, &active_ids)
            .await;
        let triggering = match context.user_session {
            MemoryUserSessionSelection::Selected(id) => Some(id),
            MemoryUserSessionSelection::Unbound | MemoryUserSessionSelection::Ambiguous => {
                context.sessions.first().map(|session| session.session_id)
            }
        };
        let result = match memory
            .execute_command(MemoryCommand::Rebuild {
                scope: params.scope,
                user_session: context.user_session,
                sessions: context.sessions,
            })
            .await
        {
            Ok(MemoryCommandResult::Rebuild(result)) => result,
            Ok(MemoryCommandResult::Status(_))
            | Ok(MemoryCommandResult::Remember(_))
            | Ok(MemoryCommandResult::PreparedForget(_))
            | Ok(MemoryCommandResult::Forget(_))
            | Ok(MemoryCommandResult::List(_))
            | Ok(MemoryCommandResult::Search(_))
            | Ok(MemoryCommandResult::Read(_))
            | Ok(MemoryCommandResult::Export(_))
            | Ok(MemoryCommandResult::Reset(_)) => {
                return self.error_response(
                    request_id,
                    ProtocolErrorCode::InternalError,
                    "memory/rebuild returned an unexpected result",
                );
            }
            Err(error) => return self.memory_error_response(request_id, "memory/rebuild", error),
        };
        if let Some(session_id) = triggering {
            let runtime = Arc::clone(self);
            tokio::spawn(async move {
                if let Some(summary) = runtime.session_summary_snapshot(session_id).await {
                    match runtime.deps.context_for_workspace(&summary.cwd).await {
                        Ok(context) => runtime.schedule_memory_scan(
                            session_id,
                            context,
                            ScanTrigger::ExplicitRebuild,
                        ),
                        Err(_) => tracing::warn!(
                            error_class = "provider_unavailable",
                            "memory rebuild context unavailable"
                        ),
                    }
                }
            });
        }
        serde_json::to_value(SuccessResponse {
            id: request_id,
            result,
        })
        .expect("serialize memory/rebuild response")
    }
}
