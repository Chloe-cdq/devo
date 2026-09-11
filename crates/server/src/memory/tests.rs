use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use devo_protocol::SessionId;
use devo_protocol::TurnId;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_protocol::native::rpc_memory::MemoryKind;
use devo_protocol::native::rpc_memory::MemoryOrigin;
use devo_protocol::native::rpc_memory::MemoryProvenance;
use devo_protocol::native::rpc_memory::MemoryScope;
use devo_protocol::native::rpc_memory::MemoryState;
use pretty_assertions::assert_eq;

use super::runtime_test_support::{forget_request, open_runtime, remember_request};
use super::test_support::{deterministic_uuid, test_source};
use super::{
    MemoryCommand, MemoryCommandResult, MemoryForgetSelector, MemoryInferredRememberRequest,
};

fn inferred_request(text: &str, observed_at: DateTime<Utc>) -> MemoryInferredRememberRequest {
    MemoryInferredRememberRequest {
        text: text.to_owned(),
        scope: MemoryScope::User,
        kind: Some(MemoryKind::Preference),
        source: test_source(
            /*user_item_id*/ None,
            "session-2",
            Some("turn-2"),
            Default::default(),
        ),
        source_observed_at: observed_at,
        source_watermark: observed_at.to_rfc3339(),
    }
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
    let forgotten = match runtime
        .execute_command(MemoryCommand::Forget(forget_request(
            MemoryForgetSelector::EntryId(remembered.entry_id.clone()),
        )))
        .await
        .expect("forget entry")
    {
        MemoryCommandResult::Forget(result) => result.forgotten.expect("forgotten entry"),
        MemoryCommandResult::Remember(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Status(_) => panic!("expected forget result"),
    };

    let old_observed_at = remembered.updated_at;
    let result = runtime
        .record_inferred(inferred_request("Use tabs", old_observed_at))
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
    assert_eq!(
        listed,
        Page {
            data: vec![forgotten],
            next_cursor: None,
        }
    );
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
            MemoryForgetSelector::EntryId(remembered.entry_id.clone()),
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

    let old_observed_at = remembered.updated_at;
    let replay = runtime
        .record_inferred(inferred_request("Use tabs!", old_observed_at))
        .expect("replay old evidence");

    assert_eq!(replay, None);
    let expected_restored = MemoryEntry {
        entry_id: remembered.entry_id,
        scope: MemoryScope::User,
        scope_id: "user".to_owned(),
        kind: MemoryKind::Preference,
        normalized_key: "use tabs".to_owned(),
        body: "Use tabs".to_owned(),
        origin: MemoryOrigin::ExplicitUser,
        state: MemoryState::Restored,
        created_at: restored.created_at,
        updated_at: restored.updated_at,
        replacement_entry_id: None,
        provenance: restored.provenance.clone(),
    };
    assert_eq!(restored, expected_restored);
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

    let inferred_observed_at = remembered.updated_at + chrono::Duration::seconds(1);
    let inferred = runtime
        .record_inferred(inferred_request("Use tabs!", inferred_observed_at))
        .expect("inferred duplicate")
        .expect("evidence-preserving inference");
    let remembered_updated_at = remembered.updated_at;
    let expected = MemoryEntry {
        updated_at: inferred.updated_at,
        provenance: vec![
            remembered.provenance[0].clone(),
            MemoryProvenance {
                source_session_id: Some(
                    SessionId::from(deterministic_uuid("session-2")).to_string(),
                ),
                source_turn_id: Some(TurnId::from(deterministic_uuid("turn-2")).to_string()),
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
    assert_eq!(
        listed,
        Page {
            data: vec![inferred],
            next_cursor: None,
        }
    );
}
