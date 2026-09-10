use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use devo_core::MemoryConfig;
use devo_protocol::SessionId;
use devo_protocol::TurnId;
use devo_protocol::native::ids::ItemId;
use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_protocol::native::rpc_memory::MemoryKind;
use devo_protocol::native::rpc_memory::MemoryProvenance;
use devo_protocol::native::rpc_memory::MemoryScope;
use devo_protocol::native::rpc_memory::MemoryState;
use pretty_assertions::assert_eq;
use uuid::Uuid;

use super::{
    MemoryCommand, MemoryCommandResult, MemoryForgetRequest, MemoryForgetSelector,
    MemoryInferredRememberRequest, MemoryRememberRequest, MemoryRuntime, MemorySourceContext,
};

fn test_uuid(seed: &str) -> Uuid {
    let value = seed.bytes().fold(0_u128, |value, byte| {
        value.rotate_left(5) ^ u128::from(byte)
    });
    Uuid::from_u128(value)
}

fn test_source(
    user_item_id: Option<&str>,
    session_id: &str,
    turn_id: Option<&str>,
) -> MemorySourceContext {
    MemorySourceContext {
        user_item_id: user_item_id
            .map(|seed| ItemId::from_string(format!("item_{:032x}", test_uuid(seed).as_u128()))),
        session_id: SessionId::from(test_uuid(session_id)),
        turn_id: turn_id.map(|seed| TurnId::from(test_uuid(seed))),
        workspace_root: PathBuf::new(),
    }
}

fn remember_request(text: &str) -> MemoryRememberRequest {
    MemoryRememberRequest {
        text: text.to_owned(),
        scope: MemoryScope::User,
        kind: Some(MemoryKind::Preference),
        source: test_source(Some("user-item-1"), "session-1", Some("turn-1")),
    }
}

fn forget_request(entry_id: devo_protocol::native::ids::MemoryEntryId) -> MemoryForgetRequest {
    MemoryForgetRequest {
        selector: MemoryForgetSelector::EntryId(entry_id),
        scope: MemoryScope::User,
        source: test_source(None, "session-1", None),
    }
}

fn inferred_request(text: &str, observed_at: &str) -> MemoryInferredRememberRequest {
    MemoryInferredRememberRequest {
        text: text.to_owned(),
        scope: MemoryScope::User,
        kind: Some(MemoryKind::Preference),
        source: test_source(None, "session-2", Some("turn-2")),
        source_observed_at: DateTime::parse_from_rfc3339(observed_at)
            .expect("observed timestamp")
            .with_timezone(&Utc),
        source_watermark: observed_at.to_owned(),
    }
}

fn open_runtime(root: &std::path::Path) -> MemoryRuntime {
    MemoryRuntime::open(
        root.to_path_buf(),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("memory runtime")
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-9
/// Verifies: inferred source replay cannot reactivate a revoked identity.
#[tokio::test]
async fn old_inferred_evidence_cannot_reactivate_a_revoked_identity() {
    let database_root = tempfile::tempdir().expect("temporary memory root");
    let runtime = open_runtime(database_root.path());
    let remembered = match runtime
        .execute_command(MemoryCommand::Remember(remember_request("Use tabs")))
        .await
        .expect("remember entry")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Status(_) => panic!("expected remembered entry"),
    };
    runtime
        .execute_command(MemoryCommand::Forget(forget_request(
            remembered.entry_id.clone(),
        )))
        .await
        .expect("forget entry");

    let result = runtime
        .record_inferred(inferred_request("Use tabs", "2026-09-09T00:00:00Z"))
        .expect("replay old evidence");
    assert_eq!(result, None);

    let listed = match runtime
        .execute_command(MemoryCommand::List(super::ListMemoryRequest {
            scope: Some(MemoryScope::User),
            state: Some(MemoryState::Retired),
            workspace_root: PathBuf::new(),
            ..Default::default()
        }))
        .await
        .expect("list retired entry")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::Status(_) => panic!("expected retired list"),
    };
    assert_eq!(listed.data.len(), 1);
    assert_eq!(listed.data[0].entry_id, remembered.entry_id);
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-8, DD-9
/// Verifies: an old inferred observation cannot overwrite an explicitly restored identity.
#[tokio::test]
async fn restored_explicit_memory_rejects_old_inferred_replay() {
    let database_root = tempfile::tempdir().expect("temporary memory root");
    let runtime = open_runtime(database_root.path());
    let remembered = match runtime
        .execute_command(MemoryCommand::Remember(remember_request("Use tabs")))
        .await
        .expect("remember entry")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Status(_) => panic!("expected remembered entry"),
    };
    runtime
        .execute_command(MemoryCommand::Forget(forget_request(
            remembered.entry_id.clone(),
        )))
        .await
        .expect("forget entry");
    let restored = match runtime
        .execute_command(MemoryCommand::Remember(remember_request("Use tabs")))
        .await
        .expect("restore entry")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Status(_) => panic!("expected restored entry"),
    };

    let replay = runtime
        .record_inferred(inferred_request("Use tabs!", "2026-09-09T00:00:00Z"))
        .expect("replay old evidence");

    assert_eq!(replay, None);
    assert_eq!(restored.entry_id, remembered.entry_id);
    assert_eq!(restored.body, "Use tabs");
    assert_eq!(restored.state, MemoryState::Restored);
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-8
/// Verifies: inferred content preserves an explicit identity while adding evidence.
#[tokio::test]
async fn inferred_memory_does_not_replace_explicit_content() {
    let database_root = tempfile::tempdir().expect("temporary memory root");
    let runtime = open_runtime(database_root.path());
    let remembered = match runtime
        .execute_command(MemoryCommand::Remember(remember_request("Use tabs")))
        .await
        .expect("remember entry")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Status(_) => panic!("expected remembered entry"),
    };

    let inferred = runtime
        .record_inferred(inferred_request("Use tabs!", "2026-09-11T00:00:00Z"))
        .expect("inferred duplicate")
        .expect("evidence-preserving inference");
    let remembered_updated_at = remembered.updated_at;
    let expected = MemoryEntry {
        updated_at: inferred.updated_at,
        provenance: vec![
            remembered.provenance[0].clone(),
            MemoryProvenance {
                source_session_id: Some(SessionId::from(test_uuid("session-2")).to_string()),
                source_turn_id: Some(TurnId::from(test_uuid("turn-2")).to_string()),
                source_user_item_id: None,
            },
        ],
        ..remembered
    };
    assert_eq!(inferred, expected);
    assert!(inferred.updated_at > remembered_updated_at);

    let listed = match runtime
        .execute_command(MemoryCommand::List(super::ListMemoryRequest {
            scope: Some(MemoryScope::User),
            workspace_root: PathBuf::new(),
            ..Default::default()
        }))
        .await
        .expect("list explicit entry")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::Status(_) => panic!("expected memory list"),
    };
    assert_eq!(listed.data, vec![inferred]);
}
