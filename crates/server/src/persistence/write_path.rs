use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use anyhow::Context;
use anyhow::Result;

use super::RolloutStore;
use super::WritePathState;
use super::hydrate_write_state;

impl RolloutStore {
    /// Runs one rollout append operation while holding the path's write lock
    /// and mutable projector state for the operation's full duration.
    pub(super) fn with_locked_write_state<T>(
        &self,
        rollout_path: &Path,
        operation: impl FnOnce(&mut WritePathState) -> Result<T>,
    ) -> Result<T> {
        if let Some(parent) = rollout_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create rollout directory {}", parent.display()))?;
        }
        let file_lock = {
            let mut locks = self
                .file_locks
                .lock()
                .expect("rollout file-locks table poisoned");
            locks
                .entry(rollout_path.to_path_buf())
                .or_insert_with(|| Arc::new(StdMutex::new(())))
                .clone()
        };
        let _guard = file_lock.lock().expect("rollout per-file lock poisoned");
        let mut write_states = self
            .write_states
            .lock()
            .expect("rollout write-state table poisoned");
        let state = match write_states.get_mut(rollout_path) {
            Some(state) => state,
            None => {
                let state = hydrate_write_state(rollout_path)?;
                write_states
                    .entry(rollout_path.to_path_buf())
                    .or_insert(state)
            }
        };
        operation(state)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::TryLockError;
    use std::sync::mpsc;

    use pretty_assertions::assert_eq;

    use super::super::RolloutStore;

    #[test]
    fn locked_write_workflow_holds_the_file_lock_for_the_entire_callback() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let store = Arc::new(RolloutStore::new(dir.path().to_path_buf(), None));
        let rollout_path = dir.path().join("nested").join("rollout.jsonl");
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);

        let workflow = {
            let store = Arc::clone(&store);
            let rollout_path = rollout_path.clone();
            std::thread::spawn(move || {
                store
                    .with_locked_write_state(&rollout_path, |_state| {
                        entered_tx.send(()).expect("signal callback entered");
                        release_rx.recv().expect("release callback");
                        Ok(())
                    })
                    .expect("run write workflow");
            })
        };
        entered_rx.recv().expect("callback entered");

        let file_lock = {
            let locks = store
                .file_locks
                .lock()
                .expect("rollout file-locks table poisoned");
            Arc::clone(
                locks
                    .get(&rollout_path)
                    .expect("workflow registered rollout lock"),
            )
        };
        let lock_state = match file_lock.try_lock() {
            Ok(_guard) => "available",
            Err(TryLockError::WouldBlock) => "held",
            Err(TryLockError::Poisoned(_)) => "poisoned",
        };
        assert_eq!(lock_state, "held");

        release_tx.send(()).expect("release callback");
        workflow.join().expect("join write workflow");
    }
}
