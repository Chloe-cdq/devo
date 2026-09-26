use std::sync::Arc;

use super::ServerRuntime;
use crate::memory::{
    MemoryCommand, MemoryCommandResult, MemoryError, MemoryForgetRequest, MemoryRuntime,
    PreparedMemoryForgetRequest,
};

pub(super) async fn prepare_forget(
    runtime: &ServerRuntime,
    memory: &Arc<MemoryRuntime>,
    request: MemoryForgetRequest,
) -> Result<PreparedMemoryForgetRequest, MemoryError> {
    let result = runtime
        .deps
        .memory_command_executor
        .execute(memory, MemoryCommand::PrepareForget(request))
        .await?;
    match result {
        MemoryCommandResult::PreparedForget(prepared) => Ok(prepared),
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Search(_) => Err(MemoryError::InvalidStoredValue(
            "forget preparation returned an unexpected result".to_string(),
        )),
    }
}
