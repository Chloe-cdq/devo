use std::sync::Arc;

use devo_protocol::SessionId;
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use super::SessionHandle;
use crate::runtime::session_actor::commands::SessionCommand;

/// Verifies: durable settings notification is retained even when the ordinary actor mailbox is full.
#[test]
fn memory_settings_notification_does_not_depend_on_mailbox_capacity() {
    let (tx, _rx) = mpsc::channel(1);
    let (reply, _reply_rx) = oneshot::channel();
    tx.try_send(SessionCommand::GetSummary { reply })
        .expect("fill actor mailbox");
    let initial_memory_settings = crate::memory::SessionMemorySettingsSnapshot {
        settings: Default::default(),
        version: 1,
    };
    let (memory_settings_tx, _memory_settings_rx) = watch::channel(initial_memory_settings);
    let handle = SessionHandle {
        session_id: SessionId::new(),
        tx,
        max_turns: None,
        state_change_gate: Arc::new(tokio::sync::Mutex::new(())),
        metadata_update_gate: Arc::new(tokio::sync::Mutex::new(())),
        memory_settings_tx,
    };

    assert!(handle.notify_memory_settings(Some(MemorySetting::Off), Some(MemorySetting::On)));
    assert_eq!(
        *handle.memory_settings_tx.borrow(),
        crate::memory::SessionMemorySettingsSnapshot {
            settings: crate::memory::SessionMemorySettings {
                recall: MemorySetting::Off,
                contribution: MemorySetting::On,
            },
            version: 2,
        }
    );
}
