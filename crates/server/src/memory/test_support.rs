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
