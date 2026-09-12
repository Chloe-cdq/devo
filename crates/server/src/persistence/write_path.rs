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
    use std::sync::Barrier;
    use std::sync::TryLockError;
    use std::sync::mpsc;

    use devo_core::InternalRecordV2;
    use devo_core::SessionId;
    use devo_core::SessionSettingsField;
    use devo_core::rollout_v2::RolloutLineV2;
    use devo_protocol::native::session::MemorySetting;
    use pretty_assertions::assert_eq;

    use super::super::ParsedRolloutLine;
    use super::super::RolloutStore;
    use super::super::parse_rollout_line;

    #[test]
    fn locked_write_workflow_holds_the_file_lock_for_the_entire_callback() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let store = Arc::new(RolloutStore::new(
            dir.path().to_path_buf(),
            /*event_log*/ None,
        ));
        let rollout_path = dir.path().join("nested").join("rollout.jsonl");
        let (entered_tx, entered_rx) = mpsc::sync_channel(/*bound*/ 0);
        let (release_tx, release_rx) = mpsc::sync_channel(/*bound*/ 0);

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

    #[test]
    fn concurrent_single_and_batch_settings_appends_preserve_batch_and_epochs() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let store = Arc::new(RolloutStore::new(
            dir.path().to_path_buf(),
            /*event_log*/ None,
        ));
        let rollout_path = dir.path().join("rollout.jsonl");
        let session_id = SessionId::new();
        let start = Arc::new(Barrier::new(/*n*/ 3));
        let on = serde_json::to_value(MemorySetting::On).expect("serialize on");
        let off = serde_json::to_value(MemorySetting::Off).expect("serialize off");

        let single = {
            let store = Arc::clone(&store);
            let rollout_path = rollout_path.clone();
            let start = Arc::clone(&start);
            let on = on.clone();
            std::thread::spawn(move || {
                start.wait();
                store
                    .append_session_settings_at(
                        &rollout_path,
                        session_id,
                        SessionSettingsField::MemoryRecall,
                        on,
                    )
                    .expect("append single setting");
            })
        };
        let batch = {
            let store = Arc::clone(&store);
            let rollout_path = rollout_path.clone();
            let start = Arc::clone(&start);
            let on = on.clone();
            let off = off.clone();
            std::thread::spawn(move || {
                start.wait();
                store
                    .append_session_settings_batch_at(
                        &rollout_path,
                        session_id,
                        &[
                            (SessionSettingsField::MemoryRecall, off),
                            (SessionSettingsField::MemoryContribution, on),
                        ],
                    )
                    .expect("append settings batch");
            })
        };

        start.wait();
        single.join().expect("join single append");
        batch.join().expect("join batch append");

        let entries = std::fs::read_to_string(&rollout_path)
            .expect("read rollout")
            .lines()
            .map(|raw| {
                let ParsedRolloutLine::V2(v2) = parse_rollout_line(raw).expect("parse line") else {
                    panic!("settings line must parse as v2");
                };
                let RolloutLineV2::Internal { entry, .. } = *v2 else {
                    panic!("settings line must be a v2 Internal record");
                };
                entry
            })
            .collect::<Vec<_>>();
        let setting = |field, value, epoch| InternalRecordV2::SessionSettings {
            schema_version: 1,
            field,
            value,
            epoch,
        };
        let expected = if entries.first()
            == Some(&setting(
                SessionSettingsField::MemoryRecall,
                on.clone(),
                /*epoch*/ 1,
            )) {
            vec![
                setting(
                    SessionSettingsField::MemoryRecall,
                    on.clone(),
                    /*epoch*/ 1,
                ),
                setting(
                    SessionSettingsField::MemoryRecall,
                    off.clone(),
                    /*epoch*/ 2,
                ),
                setting(
                    SessionSettingsField::MemoryContribution,
                    on,
                    /*epoch*/ 3,
                ),
            ]
        } else {
            vec![
                setting(SessionSettingsField::MemoryRecall, off, /*epoch*/ 1),
                setting(
                    SessionSettingsField::MemoryContribution,
                    on.clone(),
                    /*epoch*/ 2,
                ),
                setting(SessionSettingsField::MemoryRecall, on, /*epoch*/ 3),
            ]
        };
        assert_eq!(entries, expected);
    }
}
