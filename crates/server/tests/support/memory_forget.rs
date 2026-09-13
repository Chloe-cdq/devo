use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use anyhow::{Context, Result};
use async_trait::async_trait;
use devo_server::memory::{
    MemoryCommand, MemoryCommandExecutor, MemoryCommandResult, MemoryError, MemoryRuntime,
};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

pub struct BlockingFirstMemoryCommandExecutor {
    calls: AtomicUsize,
    mutation_started: Notify,
    release_mutation: Notify,
}

pub struct BlockingSecondMemoryListExecutor {
    armed: AtomicBool,
    list_calls: AtomicUsize,
    snapshot_ready: Notify,
    release_snapshot: Notify,
}

impl BlockingSecondMemoryListExecutor {
    pub fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            list_calls: AtomicUsize::new(0),
            snapshot_ready: Notify::new(),
            release_snapshot: Notify::new(),
        }
    }

    pub fn block_next_search(&self) {
        self.list_calls.store(0, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    pub async fn wait_until_snapshot_ready(&self) -> Result<()> {
        timeout(Duration::from_secs(5), self.snapshot_ready.notified())
            .await
            .context("memory search snapshot did not become ready")?;
        Ok(())
    }

    pub fn release(&self) {
        self.release_snapshot.notify_one();
    }
}

impl BlockingFirstMemoryCommandExecutor {
    pub fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            mutation_started: Notify::new(),
            release_mutation: Notify::new(),
        }
    }

    pub async fn wait_until_started(&self) -> Result<()> {
        timeout(Duration::from_secs(5), self.mutation_started.notified())
            .await
            .context("first memory forget mutation did not start")?;
        Ok(())
    }

    pub fn release(&self) {
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
impl MemoryCommandExecutor for BlockingSecondMemoryListExecutor {
    async fn execute(
        &self,
        memory: &MemoryRuntime,
        command: MemoryCommand,
    ) -> Result<MemoryCommandResult, MemoryError> {
        let is_list = matches!(command, MemoryCommand::List(_));
        let result = memory.execute_command(command).await?;
        if is_list
            && self.armed.load(Ordering::SeqCst)
            && self.list_calls.fetch_add(1, Ordering::SeqCst) == 1
        {
            self.snapshot_ready.notify_one();
            self.release_snapshot.notified().await;
            self.armed.store(false, Ordering::SeqCst);
        }
        Ok(result)
    }
}
