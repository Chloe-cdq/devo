use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use anyhow::{Context, Result};
use async_trait::async_trait;
use devo_provider::ModelProviderSDK;
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

use crate::memory::command_execution::MemoryCommandExecutor;
use crate::memory::{MemoryCommand, MemoryCommandResult, MemoryError, MemoryRuntime};
use crate::{ServerRuntime, ServerRuntimeDependencies};

pub(crate) struct BlockingFirstMemoryCommandExecutor {
    calls: AtomicUsize,
    mutation_started: Notify,
    release_mutation: Notify,
}

pub(crate) struct BlockingMemorySearchExecutor {
    armed: AtomicBool,
    snapshot_ready: Notify,
    release_snapshot: Notify,
}

impl BlockingMemorySearchExecutor {
    pub(crate) fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            snapshot_ready: Notify::new(),
            release_snapshot: Notify::new(),
        }
    }

    pub(crate) fn block_next_search(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    pub(crate) async fn wait_until_snapshot_ready(&self) -> Result<()> {
        timeout(Duration::from_secs(5), self.snapshot_ready.notified())
            .await
            .context("memory search snapshot did not become ready")?;
        Ok(())
    }

    pub(crate) fn release(&self) {
        self.release_snapshot.notify_one();
    }
}

impl BlockingFirstMemoryCommandExecutor {
    pub(crate) fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            mutation_started: Notify::new(),
            release_mutation: Notify::new(),
        }
    }

    pub(crate) async fn wait_until_started(&self) -> Result<()> {
        timeout(Duration::from_secs(5), self.mutation_started.notified())
            .await
            .context("first memory forget mutation did not start")?;
        Ok(())
    }

    pub(crate) fn release(&self) {
        self.release_mutation.notify_one();
    }
}

#[async_trait]
impl MemoryCommandExecutor for BlockingFirstMemoryCommandExecutor {
    async fn execute(
        &self,
        memory: &MemoryRuntime,
        command: MemoryCommand,
    ) -> Result<MemoryCommandResult, MemoryError> {
        if matches!(command, MemoryCommand::Forget(_))
            && self.calls.fetch_add(1, Ordering::SeqCst) == 0
        {
            self.mutation_started.notify_one();
            self.release_mutation.notified().await;
        }
        memory.execute_command(command).await
    }
}

#[async_trait]
impl MemoryCommandExecutor for BlockingMemorySearchExecutor {
    async fn execute(
        &self,
        memory: &MemoryRuntime,
        command: MemoryCommand,
    ) -> Result<MemoryCommandResult, MemoryError> {
        let is_search = matches!(command, MemoryCommand::Search(_));
        let result = memory.execute_command(command).await?;
        if is_search && self.armed.swap(false, Ordering::SeqCst) {
            self.snapshot_ready.notify_one();
            self.release_snapshot.notified().await;
        }
        Ok(result)
    }
}

pub(crate) fn build_runtime_with_overrides(
    data_root: &std::path::Path,
    provider: Arc<dyn ModelProviderSDK>,
    workspace_root: Option<&std::path::Path>,
    memory_command_executor: Option<Arc<dyn MemoryCommandExecutor>>,
) -> Result<Arc<ServerRuntime>> {
    crate::support::build_runtime_with_dependencies(
        data_root,
        provider,
        workspace_root,
        move |dependencies: ServerRuntimeDependencies| {
            if let Some(executor) = memory_command_executor {
                dependencies.with_test_memory_command_executor(executor)
            } else {
                dependencies
            }
        },
    )
}
