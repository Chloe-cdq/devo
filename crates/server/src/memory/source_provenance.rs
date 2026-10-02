//! Recoverable source fencing: foreground work never waits for a memory database.

use std::sync::atomic::Ordering;

use devo_protocol::SessionId;

use super::MemoryRuntime;

impl MemoryRuntime {
    /// Keep inferred reads and extraction closed until canonical history has
    /// restored any exclusion that optional databases failed to persist.
    pub(crate) fn attach_source_rollout_store(&mut self, store: crate::persistence::RolloutStore) {
        self.source_rollout_store = Some(store);
        self.source_recovery_pending.store(true, Ordering::Release);
    }

    /// Publish intent before external content can reach any session in the chain.
    /// This lock never waits on SQLite, projection writes, or journal reads.
    pub(crate) fn begin_external_context_sources(&self, sources: &[SessionId]) {
        self.pending_external_sources
            .lock()
            .expect("source provenance state poisoned")
            .extend(sources.iter().copied());
    }

    pub(super) fn reconcile_external_context_sources(&self) {
        if self.source_recovery_pending.load(Ordering::Acquire) {
            let recovered = self.source_rollout_store.as_ref().map_or_else(
                || Ok(Default::default()),
                crate::persistence::RolloutStore::external_context_sources,
            );
            match recovered {
                Ok(sources) => {
                    self.begin_external_context_sources(&sources.into_iter().collect::<Vec<_>>());
                    self.source_recovery_pending.store(false, Ordering::Release);
                }
                Err(_) => {
                    self.note_source_provenance_storage_failure();
                    tracing::warn!("memory source provenance recovery remains pending");
                }
            }
        }
        let sources: Vec<_> = self
            .pending_external_sources
            .lock()
            .expect("source provenance state poisoned")
            .iter()
            .copied()
            .collect();
        if sources.is_empty() {
            return;
        }
        let ledger_recorded = self
            .deletion_ledger
            .as_ref()
            .is_some_and(|db| db.record_external_context_sources(&sources).is_ok());
        if !ledger_recorded {
            self.note_source_provenance_storage_failure();
        }
        // A durable ledger is enough to transfer ownership of the pending
        // fence; existing source-intent reads stay closed until reconciliation.
        let fenced = ledger_recorded || self.fence_external_context_sources(&sources).is_ok();
        if fenced {
            let mut pending = self
                .pending_external_sources
                .lock()
                .expect("source provenance state poisoned");
            for source in sources {
                pending.remove(&source);
            }
        } else {
            self.note_source_provenance_storage_failure();
        }
    }
}
