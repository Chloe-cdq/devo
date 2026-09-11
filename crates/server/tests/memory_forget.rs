use std::fs;
use std::path::PathBuf;

use devo_server::memory::MemorySourceContext;

#[path = "../src/memory/runtime_test_support.rs"]
mod runtime_support;
#[path = "../src/memory/test_support.rs"]
mod test_support;

use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryForgetResult, MemoryScope, MemoryState,
};
use devo_server::memory::{
    MemoryCommand, MemoryCommandResult, MemoryForgetRequest, MemoryForgetSelector,
    MemoryRememberRequest, MemoryRuntime, PrepareMemoryRequest,
};
use pretty_assertions::assert_eq;
use runtime_support::{forget_request, open_runtime, remember_request};
use rusqlite::Connection;

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
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_) => {
            panic!("expected restored entry")
        }
    };

    let expected_restored = MemoryEntry {
        state: MemoryState::Restored,
        updated_at: restored.updated_at,
        ..first.clone()
    };
    assert_eq!(restored, expected_restored);

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
        | MemoryCommandResult::Status(_) => panic!("expected active list"),
    };
    assert_eq!(
        listed,
        Page {
            data: Vec::new(),
            next_cursor: None,
        }
    );

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
        | MemoryCommandResult::Status(_) => panic!("expected restored list"),
    };
    assert_eq!(
        restored_list,
        Page {
            data: vec![restored.clone()],
            next_cursor: None,
        }
    );

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
        | MemoryCommandResult::Status(_) => panic!("expected forget result"),
    };
    let forgotten = result
        .forgotten
        .clone()
        .expect("exact forget returns entry");
    let expected_forgotten = MemoryEntry {
        state: MemoryState::Retired,
        updated_at: forgotten.updated_at,
        ..remembered.clone()
    };
    assert_eq!(
        result,
        MemoryForgetResult {
            forgotten: Some(expected_forgotten),
            candidates: Vec::new(),
        }
    );

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
        | MemoryCommandResult::Status(_) => panic!("expected retired list"),
    };
    assert_eq!(
        listed,
        Page {
            data: vec![forgotten.clone()],
            next_cursor: None,
        }
    );
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

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-9, DD-12
/// Verifies: exact forget resolves the entry's persisted scope instead of the request default.
#[tokio::test]
async fn exact_forget_uses_persisted_scope_for_project_entry() {
    let database_root = tempfile::tempdir().expect("temporary memory root");
    let project_root = database_root.path().join("project");
    std::fs::create_dir_all(&project_root).expect("project root");
    let runtime = open_runtime(database_root.path());
    let mut project_request = remember_request("Use tabs");
    project_request.scope = MemoryScope::Project;
    project_request.source.workspace_root = project_root.clone();
    let remembered = match runtime
        .execute_command(MemoryCommand::Remember(project_request))
        .await
        .expect("remember project entry")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Status(_) => panic!("expected remembered entry"),
    };

    let result = runtime
        .execute_command(MemoryCommand::Forget(forget_request(
            MemoryForgetSelector::EntryId(remembered.entry_id.clone()),
        )))
        .await
        .expect("forget project entry by stable ID");

    let result = match result {
        MemoryCommandResult::Forget(result) => result,
        MemoryCommandResult::List(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::Status(_) => panic!("expected forget result"),
    };
    let forgotten = result.forgotten.clone().expect("forgotten project entry");
    let expected_forgotten = MemoryEntry {
        state: MemoryState::Retired,
        updated_at: forgotten.updated_at,
        ..remembered
    };
    assert_eq!(
        result,
        MemoryForgetResult {
            forgotten: Some(expected_forgotten),
            candidates: Vec::new(),
        }
    );
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: ambiguous text forget returns candidates without mutation.
#[tokio::test]
async fn ambiguous_text_forget_returns_candidates_without_mutation() {
    let database_root = tempfile::tempdir().expect("temporary memory root");
    let runtime = open_runtime(database_root.path());
    let mut expected_candidates = Vec::new();
    for text in ["I prefer tabs", "I prefer spaces"] {
        let remembered = match runtime
            .execute_command(MemoryCommand::Remember(remember_request(text)))
            .await
            .expect("remember candidate")
        {
            MemoryCommandResult::Remember(entry) => entry,
            MemoryCommandResult::Forget(_)
            | MemoryCommandResult::List(_)
            | MemoryCommandResult::Status(_) => panic!("expected remembered candidate"),
        };
        expected_candidates.push(remembered);
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
        | MemoryCommandResult::Status(_) => panic!("expected forget result"),
    };
    expected_candidates.sort_by_key(|entry| entry.entry_id.to_string());
    let mut actual_candidates = result.candidates;
    actual_candidates.sort_by_key(|entry| entry.entry_id.to_string());
    assert_eq!(
        MemoryForgetResult {
            forgotten: result.forgotten,
            candidates: actual_candidates,
        },
        MemoryForgetResult {
            forgotten: None,
            candidates: expected_candidates,
        }
    );

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
