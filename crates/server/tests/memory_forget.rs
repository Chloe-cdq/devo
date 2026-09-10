use std::fs;
use std::path::PathBuf;

use devo_core::MemoryConfig;
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryScope, MemoryState};
use devo_server::memory::{
    MemoryCommand, MemoryCommandResult, MemoryForgetRequest, MemoryForgetSelector,
    MemoryInferredRememberRequest, MemoryRememberRequest, MemoryRuntime, PrepareMemoryRequest,
};
use pretty_assertions::assert_eq;
use rusqlite::Connection;

fn remember_request(text: &str) -> MemoryRememberRequest {
    MemoryRememberRequest {
        text: text.to_owned(),
        scope: MemoryScope::User,
        kind: Some(MemoryKind::Preference),
        source_user_item_id: Some("user-item-1".to_owned()),
        source_session_id: "session-1".to_owned(),
        source_turn_id: Some("turn-1".to_owned()),
        workspace_root: PathBuf::new(),
    }
}

fn forget_request(selector: MemoryForgetSelector) -> MemoryForgetRequest {
    MemoryForgetRequest {
        selector,
        scope: MemoryScope::User,
        source_user_item_id: None,
        source_session_id: "session-1".to_owned(),
        source_turn_id: None,
        workspace_root: PathBuf::new(),
    }
}

fn inferred_request(text: &str, observed_at: &str) -> MemoryInferredRememberRequest {
    MemoryInferredRememberRequest {
        text: text.to_owned(),
        scope: MemoryScope::User,
        kind: Some(MemoryKind::Preference),
        source_user_item_id: None,
        source_session_id: "session-2".to_owned(),
        source_turn_id: Some("turn-2".to_owned()),
        source_observed_at: chrono::DateTime::parse_from_rfc3339(observed_at)
            .expect("observed timestamp")
            .with_timezone(&chrono::Utc),
        source_watermark: observed_at.to_owned(),
        workspace_root: PathBuf::new(),
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

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-9, DD-12
/// Verifies: explicit remember restores a revoked identity and records its lineage.
#[tokio::test]
async fn explicit_remember_restores_revoked_identity_and_records_lineage() {
    let database_root = tempfile::tempdir().expect("temporary memory root");
    let runtime = open_runtime(database_root.path());

    let first = match runtime
        .execute_command(MemoryCommand::Remember(remember_request("Use tabs")))
        .await
        .expect("initial remember")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_) => {
            panic!("expected remembered entry")
        }
    };

    let connection =
        Connection::open(database_root.path().join("memory.sqlite3")).expect("memory database");
    connection
        .execute(
            "UPDATE memory_entries SET state = 'retired' WHERE entry_id = ?1",
            [&first.entry_id.to_string()],
        )
        .expect("retire entry");
    connection
        .execute(
            "INSERT INTO memory_revocations
                (revocation_id, scope_type, scope_id, normalized_key, revoked_at, restored_at)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
            rusqlite::params![
                "revocation-1",
                "user",
                first.scope_id,
                first.normalized_key,
                "2026-09-10T00:00:00Z",
            ],
        )
        .expect("write revocation");

    let restored = match runtime
        .execute_command(MemoryCommand::Remember(remember_request("Use tabs")))
        .await
        .expect("explicit restore")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_) => {
            panic!("expected restored entry")
        }
    };

    assert_eq!(restored.entry_id, first.entry_id);
    assert_eq!(restored.state, MemoryState::Restored);

    let restored_at: Option<String> = connection
        .query_row(
            "SELECT restored_at FROM memory_revocations
             WHERE scope_type = 'user' AND scope_id = ?1 AND normalized_key = ?2",
            rusqlite::params![first.scope_id, first.normalized_key],
            |row| row.get(0),
        )
        .expect("load restoration timestamp");
    assert!(
        restored_at.is_some(),
        "restoration lineage must be recorded"
    );

    let listed = match runtime
        .execute_command(MemoryCommand::List(
            devo_server::memory::ListMemoryRequest {
                scope: Some(MemoryScope::User),
                state: Some(MemoryState::Active),
                workspace_root: PathBuf::new(),
                ..Default::default()
            },
        ))
        .await
        .expect("list restored entry")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Status(_) => panic!("expected active list"),
    };
    assert!(listed.data.is_empty());

    let restored_list = match runtime
        .execute_command(MemoryCommand::List(
            devo_server::memory::ListMemoryRequest {
                scope: Some(MemoryScope::User),
                state: Some(MemoryState::Restored),
                workspace_root: PathBuf::new(),
                ..Default::default()
            },
        ))
        .await
        .expect("list restored entry")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Status(_) => panic!("expected restored list"),
    };
    assert_eq!(restored_list.data, vec![restored.clone()]);

    let prepared = runtime
        .prepare_turn(PrepareMemoryRequest {
            workspace_root: database_root.path().to_path_buf(),
            session_recall: devo_protocol::native::session::MemorySetting::On,
        })
        .await
        .expect("prepare restored memory recall");
    assert_eq!(prepared.user_entries, vec![restored.clone()]);
    let projection = fs::read_to_string(database_root.path().join("user").join("MEMORY.md"))
        .expect("read restored user projection");
    assert!(projection.contains("state: restored"));
    assert!(!projection.contains("revoked_at"));
    assert!(!projection.contains("restored_at"));
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
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Status(_) => panic!("expected remembered entry"),
    };

    runtime
        .execute_command(MemoryCommand::Forget(forget_request(
            MemoryForgetSelector::EntryId(remembered.entry_id.clone()),
        )))
        .await
        .expect("forget entry");

    let result = runtime
        .execute_command(MemoryCommand::RememberInferred(inferred_request(
            "Use tabs",
            "2026-09-09T00:00:00Z",
        )))
        .await
        .expect("replay old evidence");
    assert_eq!(result, MemoryCommandResult::RememberInferred(None));

    let listed = match runtime
        .execute_command(MemoryCommand::List(
            devo_server::memory::ListMemoryRequest {
                scope: Some(MemoryScope::User),
                state: Some(MemoryState::Retired),
                workspace_root: PathBuf::new(),
                ..Default::default()
            },
        ))
        .await
        .expect("list retired entry")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::RememberInferred(_)
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
        | MemoryCommandResult::RememberInferred(_)
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
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Status(_) => panic!("expected restored entry"),
    };

    let replay = runtime
        .execute_command(MemoryCommand::RememberInferred(inferred_request(
            "Use tabs!",
            "2026-09-09T00:00:00Z",
        )))
        .await
        .expect("replay old evidence");

    assert_eq!(replay, MemoryCommandResult::RememberInferred(None));
    assert_eq!(restored.entry_id, remembered.entry_id);
    assert_eq!(restored.body, "Use tabs");
    assert_eq!(restored.state, MemoryState::Restored);
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-8
/// Verifies: inferred content cannot replace an existing explicit identity.
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
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Status(_) => panic!("expected remembered entry"),
    };

    let inferred = runtime
        .execute_command(MemoryCommand::RememberInferred(inferred_request(
            "Use tabs!",
            "2026-09-11T00:00:00Z",
        )))
        .await
        .expect("inferred duplicate");

    assert_eq!(inferred, MemoryCommandResult::RememberInferred(None));
    let listed = match runtime
        .execute_command(MemoryCommand::List(
            devo_server::memory::ListMemoryRequest {
                scope: Some(MemoryScope::User),
                workspace_root: PathBuf::new(),
                ..Default::default()
            },
        ))
        .await
        .expect("list explicit entry")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Status(_) => panic!("expected memory list"),
    };
    assert_eq!(listed.data, vec![remembered]);
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-9, DD-12
/// Verifies: exact forget commits revocation before returning the retired entry.
#[tokio::test]
async fn exact_forget_commits_revocation_before_returning_retired_entry() {
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
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Status(_) => panic!("expected remembered entry"),
    };

    let result = match runtime
        .execute_command(MemoryCommand::Forget(forget_request(
            MemoryForgetSelector::EntryId(remembered.entry_id.clone()),
        )))
        .await
        .expect("forget entry")
    {
        MemoryCommandResult::Forget(result) => result,
        MemoryCommandResult::List(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Status(_) => panic!("expected forget result"),
    };
    let forgotten = result.forgotten.expect("exact forget returns entry");
    assert_eq!(forgotten.entry_id, remembered.entry_id);
    assert_eq!(forgotten.state, MemoryState::Retired);
    assert!(result.candidates.is_empty());

    let listed = match runtime
        .execute_command(MemoryCommand::List(
            devo_server::memory::ListMemoryRequest {
                scope: Some(MemoryScope::User),
                state: Some(MemoryState::Retired),
                workspace_root: PathBuf::new(),
                ..Default::default()
            },
        ))
        .await
        .expect("list retired entry")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Status(_) => panic!("expected retired list"),
    };
    assert_eq!(listed.data, vec![forgotten.clone()]);
    let projection = fs::read_to_string(database_root.path().join("user").join("MEMORY.md"))
        .expect("read user projection");
    assert!(projection.contains("state: retired"));
    assert!(!projection.contains("revoked_at"));
    assert!(!projection.contains("restored_at"));

    let connection =
        Connection::open(database_root.path().join("memory.sqlite3")).expect("memory database");
    let (revocation_count, retired_count): (i64, i64) = connection
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM memory_revocations
                  WHERE scope_type = 'user' AND scope_id = 'user' AND normalized_key = ?1),
                 (SELECT COUNT(*) FROM memory_entries
                  WHERE entry_id = ?2 AND state = 'retired')",
            rusqlite::params![remembered.normalized_key, remembered.entry_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("load forget lifecycle");
    assert_eq!((revocation_count, retired_count), (1, 1));
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: ambiguous text forget returns candidates without mutation.
#[tokio::test]
async fn ambiguous_text_forget_returns_candidates_without_mutation() {
    let database_root = tempfile::tempdir().expect("temporary memory root");
    let runtime = open_runtime(database_root.path());
    for text in ["I prefer tabs", "I prefer spaces"] {
        runtime
            .execute_command(MemoryCommand::Remember(remember_request(text)))
            .await
            .expect("remember candidate");
    }

    let result = match runtime
        .execute_command(MemoryCommand::Forget(forget_request(
            MemoryForgetSelector::Text("I prefer".to_owned()),
        )))
        .await
        .expect("ambiguous forget")
    {
        MemoryCommandResult::Forget(result) => result,
        MemoryCommandResult::List(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::RememberInferred(_)
        | MemoryCommandResult::Status(_) => panic!("expected forget result"),
    };
    assert!(result.forgotten.is_none());
    assert_eq!(result.candidates.len(), 2);

    let connection =
        Connection::open(database_root.path().join("memory.sqlite3")).expect("memory database");
    let (revocation_count, active_count): (i64, i64) = connection
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM memory_revocations),
                 (SELECT COUNT(*) FROM memory_entries WHERE state = 'active')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("load ambiguous lifecycle");
    assert_eq!((revocation_count, active_count), (0, 2));
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: text forget treats SQL wildcard characters as literal text.
#[tokio::test]
async fn text_forget_selector_treats_sql_wildcards_as_literal_text() {
    let database_root = tempfile::tempdir().expect("temporary memory root");
    let runtime = open_runtime(database_root.path());
    runtime
        .execute_command(MemoryCommand::Remember(remember_request("Use tabs")))
        .await
        .expect("remember entry");

    let error = runtime
        .execute_command(MemoryCommand::Forget(forget_request(
            MemoryForgetSelector::Text("%".to_owned()),
        )))
        .await
        .expect_err("a wildcard must not select the only entry");
    assert_eq!(
        error.to_string(),
        "invalid memory request: memory entry not found"
    );
}
