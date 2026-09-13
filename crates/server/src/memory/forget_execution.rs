use async_trait::async_trait;

use super::{MemoryCommand, MemoryCommandResult, MemoryError, MemoryForgetRequest, MemoryRuntime};

/// Executes one authorized memory-forget storage command.
///
/// Implementations must preserve the command result and storage errors. They
/// may delay execution for deterministic concurrency control, but must invoke
/// the supplied [`MemoryRuntime`] exactly once when execution proceeds.
#[async_trait]
pub trait MemoryForgetExecutor: Send + Sync {
    async fn execute(
        &self,
        memory: &MemoryRuntime,
        request: MemoryForgetRequest,
    ) -> Result<MemoryCommandResult, MemoryError>;
}

pub(crate) struct RuntimeMemoryForgetExecutor;

#[async_trait]
impl MemoryForgetExecutor for RuntimeMemoryForgetExecutor {
    async fn execute(
        &self,
        memory: &MemoryRuntime,
        request: MemoryForgetRequest,
    ) -> Result<MemoryCommandResult, MemoryError> {
        memory.execute_command(MemoryCommand::Forget(request)).await
    }
}
