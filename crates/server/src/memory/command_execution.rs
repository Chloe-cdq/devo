use async_trait::async_trait;

use super::{MemoryCommand, MemoryCommandResult, MemoryError, MemoryRuntime};

/// Executes one memory command on the supplied runtime.
///
/// Implementations must preserve command results and storage errors. They may
/// delay execution for deterministic concurrency control, but must invoke the
/// supplied [`MemoryRuntime`] exactly once when execution proceeds.
#[async_trait]
pub trait MemoryCommandExecutor: Send + Sync {
    async fn execute(
        &self,
        memory: &MemoryRuntime,
        command: MemoryCommand,
    ) -> Result<MemoryCommandResult, MemoryError>;
}

pub(crate) struct RuntimeMemoryCommandExecutor;

#[async_trait]
impl MemoryCommandExecutor for RuntimeMemoryCommandExecutor {
    async fn execute(
        &self,
        memory: &MemoryRuntime,
        command: MemoryCommand,
    ) -> Result<MemoryCommandResult, MemoryError> {
        memory.execute_command(command).await
    }
}
