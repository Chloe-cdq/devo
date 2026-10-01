use std::sync::Arc;

use devo_protocol::SessionId;
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use super::SessionHandle;
use crate::runtime::session_actor::commands::SessionCommand;

/// Trace: L2-DES-CONV-002 Rev 2 DD-3, L2-DES-MEM-001 Rev 3 Session Controls
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
        memory_settings_tx,
        permission_preset: Arc::new(std::sync::Mutex::new(None)),
    };

    assert!(handle.notify_memory_settings(Some(MemorySetting::Off), Some(MemorySetting::On)));
    assert_eq!(
        *handle.memory_settings_tx.borrow(),
        crate::memory::SessionMemorySettingsSnapshot {
            settings: crate::memory::SessionMemorySettings {
                recall: MemorySetting::Off,
                contribution: MemorySetting::On,
                source: Default::default(),
            },
            version: 2,
        }
    );
}

/// Trace: L2-DES-CONV-002 Rev 2 DD-3, L2-DES-SERVER-002.
/// Verifies: capacity waits leave notification/read paths usable, and rejected
/// best-effort sends cannot replace the last accepted permission target.
#[tokio::test]
async fn permission_snapshot_survives_mailbox_backpressure() {
    use devo_protocol::PermissionPreset;
    let (tx, mut rx) = mpsc::channel(1);
    let (memory_settings_tx, _memory_settings_rx) =
        watch::channel(crate::memory::SessionMemorySettingsSnapshot {
            settings: Default::default(),
            version: 1,
        });
    let handle = SessionHandle {
        session_id: SessionId::new(),
        tx,
        max_turns: None,
        state_change_gate: Arc::new(tokio::sync::Mutex::new(())),
        memory_settings_tx,
        permission_preset: Arc::new(std::sync::Mutex::new(None)),
    };
    let cwd = std::env::current_dir().expect("native test cwd");
    let default_profile = crate::runtime::safety_profile_from_protocol(
        PermissionPreset::Default,
        cwd.clone(),
        Vec::new(),
    );
    let full_profile = crate::runtime::safety_profile_from_protocol(
        PermissionPreset::FullAccess,
        cwd.clone(),
        Vec::new(),
    );
    let auto_profile =
        crate::runtime::safety_profile_from_protocol(PermissionPreset::AutoReview, cwd, Vec::new());
    handle.notify_permission_profile(default_profile, "workspace".to_string());
    let waiting_writer = handle.clone();
    let applying = waiting_writer.apply_permission_profile(full_profile);
    tokio::pin!(applying);
    assert!(futures::poll!(&mut applying).is_pending());
    handle.notify_permission_profile(auto_profile, "workspace".to_string());
    assert_eq!(handle.permission_preset(), Some(PermissionPreset::Default));

    let Some(SessionCommand::ApplyPermissionProfile {
        profile,
        sandbox_profile,
        reply,
    }) = rx.recv().await
    else {
        panic!("expected first accepted permission command");
    };
    assert_eq!(
        (profile.preset, sandbox_profile),
        (
            devo_safety::PermissionPreset::Default,
            "workspace".to_string()
        )
    );
    let _ = reply.send(());
    assert!(futures::poll!(&mut applying).is_pending());
    assert_eq!(
        handle.permission_preset(),
        Some(PermissionPreset::FullAccess)
    );
    let Some(SessionCommand::ApplyPermissionProfile {
        profile,
        sandbox_profile,
        reply,
    }) = rx.recv().await
    else {
        panic!("expected waiting permission command after capacity became available");
    };
    assert_eq!(
        (profile.preset, sandbox_profile),
        (devo_safety::PermissionPreset::FullAccess, "off".to_string())
    );
    reply.send(()).expect("waiting caller");
    assert!(applying.await);
    assert!(rx.try_recv().is_err());
}
