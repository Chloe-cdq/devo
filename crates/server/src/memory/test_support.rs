use std::path::PathBuf;

use devo_protocol::SessionId;
use devo_protocol::TurnId;
use devo_protocol::native::ids::ItemId;
use uuid::Uuid;

use super::MemorySourceContext;

pub fn deterministic_uuid(seed: &str) -> Uuid {
    let value = seed.bytes().fold(0_u128, |value, byte| {
        value.rotate_left(5) ^ u128::from(byte)
    });
    Uuid::from_u128(value)
}

pub fn test_source(
    user_item_id: Option<&str>,
    session_id: &str,
    turn_id: Option<&str>,
    workspace_root: PathBuf,
) -> MemorySourceContext {
    MemorySourceContext {
        user_item_id: user_item_id.map(|seed| {
            ItemId::from_string(format!("item_{:032x}", deterministic_uuid(seed).as_u128()))
        }),
        session_id: SessionId::from(deterministic_uuid(session_id)),
        turn_id: turn_id.map(|seed| TurnId::from(deterministic_uuid(seed))),
        workspace_root,
    }
}

/// Hold the real storage mutex on an OS thread until the test releases it.
/// The deadline bounds a broken implementation without sleeping in the test.
pub(crate) fn hold_storage(
    memory: std::sync::Arc<super::MemoryRuntime>,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    std::sync::mpsc::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _storage = memory.connection.lock().unwrap();
        held_tx.send(()).unwrap();
        let _ = release_rx.recv_timeout(std::time::Duration::from_secs(/*secs*/ 30));
    });
    (held_rx, release_tx, worker)
}
