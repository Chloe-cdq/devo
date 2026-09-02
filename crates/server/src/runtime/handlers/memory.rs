use super::super::*;

use crate::memory::MemoryCommand;
use crate::memory::MemoryCommandResult;

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
