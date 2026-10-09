//! Background source work outside foreground session and turn tasks.
use super::MemoryRuntime;
use super::scan::MemorySourceWork;
use std::sync::Arc;
use std::time::Duration;

impl MemoryRuntime {
    /// Owns passive source scheduling and durable source-intent reconciliation.
    pub(crate) fn enqueue_source(self: &Arc<Self>, work: MemorySourceWork) {
        match work {
            MemorySourceWork::DeleteSources {
                sources,
                related_memory,
                reply,
            } => {
                let memory = Arc::clone(self);
                let _ = std::thread::spawn(move || {
                    // Foreground deletion commits canonical records only. Durable
                    // scope markers leave projection I/O to reconciliation.
                    let result = (|| {
                        let mut connection =
                            memory.connection.try_lock().map_err(|error| match error {
                                std::sync::TryLockError::WouldBlock => {
                                    super::MemoryError::StorageBusy
                                }
                                std::sync::TryLockError::Poisoned(_) => {
                                    super::MemoryError::LockPoisoned
                                }
                            })?;
                        let previous_timeout: u64 =
                            connection.query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
                        connection.busy_timeout(Duration::ZERO)?;
                        let now = (memory.clock)();
                        let result = super::source_deletion::delete_source_records(
                            &mut connection,
                            &sources,
                            now,
                            memory.inferred_expiry_cutoff(now),
                            related_memory,
                        );
                        if connection
                            .busy_timeout(Duration::from_millis(previous_timeout))
                            .is_err()
                        {
                            tracing::warn!(
                                error_class = "storage_error",
                                "failed to restore memory storage timeout"
                            );
                        }
                        result
                    })();
                    let _ = reply.send(result);
                });
            }
            MemorySourceWork::Scan(context) => {
                let memory = Arc::clone(self);
                tokio::spawn(async move {
                    let repair = Arc::clone(&memory);
                    if tokio::task::spawn_blocking(move || {
                        repair.reconcile_source_intents();
                    })
                    .await
                    .is_err()
                    {
                        tracing::warn!(
                            error_class = "worker_error",
                            "memory source reconciliation task failed"
                        );
                    }
                    if memory.run_background_scan(context).await.is_err() {
                        tracing::warn!(
                            error_class = "storage_error",
                            "background memory scan failed"
                        );
                    }
                });
            }
            MemorySourceWork::Reconcile => {
                let start = {
                    let mut state = self
                        .reconcile_state
                        .lock()
                        .expect("reconcile state poisoned");
                    state.pending = true;
                    if state.running {
                        false
                    } else {
                        state.running = true;
                        true
                    }
                };
                if !start {
                    return;
                }
                let memory = Arc::clone(self);
                let _ = std::thread::spawn(move || {
                    loop {
                        let pending = {
                            let mut state = memory
                                .reconcile_state
                                .lock()
                                .expect("reconcile state poisoned");
                            if state.pending {
                                state.pending = false;
                                true
                            } else {
                                state.running = false;
                                false
                            }
                        };
                        if !pending {
                            break;
                        }
                        memory.reconcile_source_intents();
                    }
                });
            }
        }
    }
}
