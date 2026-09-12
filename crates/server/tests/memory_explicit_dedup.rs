use std::fs;
use std::path::Path;

use devo_core::MemoryConfig;
use devo_protocol::native::ids::ItemId;
use devo_protocol::native::rpc_memory::{MemoryEntry, MemoryKind, MemoryProvenance, MemoryScope};
use devo_server::memory::{
    ListMemoryRequest, MemoryCommand, MemoryCommandResult, MemoryRememberRequest, MemoryRuntime,
};
use pretty_assertions::assert_eq;
use rusqlite::Connection;
use tempfile::TempDir;

async fn remember(runtime: &MemoryRuntime, request: MemoryRememberRequest) -> MemoryEntry {
    match runtime
        .execute_command(MemoryCommand::Remember(request))
        .await
        .expect("remember explicit memory")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_) | MemoryCommandResult::List(_) => {
            panic!("unexpected memory command result")
        }
    }
}

async fn list(
    runtime: &MemoryRuntime,
    scope: MemoryScope,
    workspace_root: &Path,
    text: Option<&str>,
) -> Vec<MemoryEntry> {
    match runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(scope),
            text: text.map(str::to_string),
            workspace_root: workspace_root.to_path_buf(),
            ..ListMemoryRequest::default()
        }))
        .await
        .expect("list explicit memory")
    {
        MemoryCommandResult::List(page) => page.data,
        MemoryCommandResult::Status(_) | MemoryCommandResult::Remember(_) => {
            panic!("unexpected memory command result")
        }
    }
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-8
/// Verifies: harmless explicit-request wording updates one stable canonical entry.
#[tokio::test]
async fn equivalent_wording_updates_one_canonical_entry_and_deduplicates_evidence() {
    let data_root = TempDir::new().expect("memory data root");
    let runtime = MemoryRuntime::open(
        data_root.path().join("memory"),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("open enabled memory runtime");

    let first = remember(
        &runtime,
        MemoryRememberRequest {
            text: "I prefer dark mode".to_string(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Fact),
            source_user_item_id: Some("item-1".to_string()),
            source_session_id: "session-1".to_string(),
            source_turn_id: Some("turn-1".to_string()),
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let equivalent_request = MemoryRememberRequest {
        text: "  Please remember that MY PREFERENCE IS dark mode!  ".to_string(),
        scope: MemoryScope::User,
        kind: None,
        source_user_item_id: Some("item-2".to_string()),
        source_session_id: "session-2".to_string(),
        source_turn_id: Some("turn-2".to_string()),
        workspace_root: data_root.path().to_path_buf(),
    };
    let updated = remember(&runtime, equivalent_request.clone()).await;
    let replayed = remember(&runtime, equivalent_request).await;

    assert_eq!(updated.entry_id, first.entry_id);
    assert_eq!(updated.created_at, first.created_at);
    assert!(updated.updated_at > first.updated_at);
    assert_eq!(updated.normalized_key, "i prefer dark mode");
    assert_eq!(
        updated.body,
        "Please remember that MY PREFERENCE IS dark mode!"
    );
    assert_eq!(updated.kind, MemoryKind::Preference);
    assert_eq!(
        replayed.provenance,
        vec![
            MemoryProvenance {
                source_session_id: Some("session-1".to_string()),
                source_turn_id: Some("turn-1".to_string()),
                source_user_item_id: Some(ItemId::from_string("item-1".to_string())),
            },
            MemoryProvenance {
                source_session_id: Some("session-2".to_string()),
                source_turn_id: Some("turn-2".to_string()),
                source_user_item_id: Some(ItemId::from_string("item-2".to_string())),
            },
        ]
    );
    assert_eq!(
        list(
            &runtime,
            MemoryScope::User,
            data_root.path(),
            /*text*/ None,
        )
        .await,
        vec![replayed]
    );
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3, DD-8
/// Verifies: equivalence is scope-local and preserves identity-bearing claim tokens.
#[tokio::test]
async fn equivalence_is_scope_local_and_does_not_merge_different_claims() {
    let data_root = TempDir::new().expect("memory data root");
    let runtime = MemoryRuntime::open(
        data_root.path().join("memory"),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("open enabled memory runtime");
    let user_rust = remember(
        &runtime,
        MemoryRememberRequest {
            text: "The project uses Rust".to_string(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Fact),
            source_user_item_id: Some("user-rust".to_string()),
            source_session_id: "session-user".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let project_rust = remember(
        &runtime,
        MemoryRememberRequest {
            text: "The project uses Rust".to_string(),
            scope: MemoryScope::Project,
            kind: Some(MemoryKind::Fact),
            source_user_item_id: Some("project-rust-1".to_string()),
            source_session_id: "session-project".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let project_rust_updated = remember(
        &runtime,
        MemoryRememberRequest {
            text: "Remember that the project uses Rust.".to_string(),
            scope: MemoryScope::Project,
            kind: None,
            source_user_item_id: Some("project-rust-2".to_string()),
            source_session_id: "session-project".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let project_python = remember(
        &runtime,
        MemoryRememberRequest {
            text: "The project uses Python".to_string(),
            scope: MemoryScope::Project,
            kind: Some(MemoryKind::Fact),
            source_user_item_id: Some("project-python".to_string()),
            source_session_id: "session-project".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let project_not_rust = remember(
        &runtime,
        MemoryRememberRequest {
            text: "The project does not use Rust".to_string(),
            scope: MemoryScope::Project,
            kind: Some(MemoryKind::Fact),
            source_user_item_id: Some("project-not-rust".to_string()),
            source_session_id: "session-project".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let project_dot_env = remember(
        &runtime,
        MemoryRememberRequest {
            text: "Use .env for configuration".to_string(),
            scope: MemoryScope::Project,
            kind: Some(MemoryKind::Fact),
            source_user_item_id: Some("project-dot-env".to_string()),
            source_session_id: "session-project".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let project_env = remember(
        &runtime,
        MemoryRememberRequest {
            text: "Use env for configuration".to_string(),
            scope: MemoryScope::Project,
            kind: Some(MemoryKind::Fact),
            source_user_item_id: Some("project-env".to_string()),
            source_session_id: "session-project".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let project_parent_config = remember(
        &runtime,
        MemoryRememberRequest {
            text: "Use ../config".to_string(),
            scope: MemoryScope::Project,
            kind: Some(MemoryKind::Fact),
            source_user_item_id: Some("project-parent-config".to_string()),
            source_session_id: "session-project".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let project_root_config = remember(
        &runtime,
        MemoryRememberRequest {
            text: "Use /config".to_string(),
            scope: MemoryScope::Project,
            kind: Some(MemoryKind::Fact),
            source_user_item_id: Some("project-root-config".to_string()),
            source_session_id: "session-project".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;

    assert_ne!(user_rust.entry_id, project_rust.entry_id);
    assert_eq!(project_rust_updated.entry_id, project_rust.entry_id);
    assert_ne!(project_dot_env.entry_id, project_env.entry_id);
    assert_ne!(project_parent_config.entry_id, project_root_config.entry_id);
    assert_eq!(
        list(
            &runtime,
            MemoryScope::User,
            data_root.path(),
            /*text*/ None,
        )
        .await,
        vec![user_rust]
    );
    let mut project_entries = list(
        &runtime,
        MemoryScope::Project,
        data_root.path(),
        /*text*/ None,
    )
    .await;
    project_entries.sort_by(|left, right| left.entry_id.as_str().cmp(right.entry_id.as_str()));
    let mut expected = vec![
        project_root_config,
        project_parent_config,
        project_env,
        project_dot_env,
        project_not_rust,
        project_python,
        project_rust_updated,
    ];
    expected.sort_by(|left, right| left.entry_id.as_str().cmp(right.entry_id.as_str()));
    assert_eq!(project_entries, expected);
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-4, DD-8
/// Verifies: deduplication updates SQLite, FTS, API listing, and Markdown durably.
#[tokio::test]
async fn deduplication_stays_consistent_across_storage_search_projection_and_restart() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime = MemoryRuntime::open(
        memory_root.clone(),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("open enabled memory runtime");
    let first = remember(
        &runtime,
        MemoryRememberRequest {
            text: "Remember that I would prefer compact responses.".to_string(),
            scope: MemoryScope::User,
            kind: None,
            source_user_item_id: Some("compact-1".to_string()),
            source_session_id: "session-1".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let updated = remember(
        &runtime,
        MemoryRememberRequest {
            text: "My preference is compact responses!".to_string(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Preference),
            source_user_item_id: Some("compact-2".to_string()),
            source_session_id: "session-2".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;

    assert_eq!(updated.entry_id, first.entry_id);
    assert_eq!(
        list(
            &runtime,
            MemoryScope::User,
            data_root.path(),
            Some("compact responses")
        )
        .await,
        vec![updated.clone()]
    );
    assert_eq!(
        list(
            &runtime,
            MemoryScope::User,
            data_root.path(),
            Some("would prefer")
        )
        .await,
        Vec::<MemoryEntry>::new()
    );

    let connection =
        Connection::open(memory_root.join("memory.sqlite3")).expect("open memory database");
    let stored: (i64, i64, String, String) = connection
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM memory_entries),
                 (SELECT COUNT(*) FROM memory_entries_fts),
                 normalized_key,
                 body
             FROM memory_entries_fts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read canonical and FTS state");
    assert_eq!(
        stored,
        (
            1,
            1,
            "i prefer compact responses".to_string(),
            "My preference is compact responses!".to_string(),
        )
    );
    let fts_matches: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM memory_entries_fts WHERE memory_entries_fts MATCH 'compact'",
            [],
            |row| row.get(0),
        )
        .expect("query memory FTS index");
    assert_eq!(fts_matches, 1);
    drop(connection);

    let projection = fs::read_to_string(memory_root.join("user").join("MEMORY.md"))
        .expect("read user memory projection");
    assert!(projection.contains("My preference is compact responses!"));
    assert!(!projection.contains("Remember that I would prefer compact responses."));

    drop(runtime);
    let reopened = MemoryRuntime::open(
        memory_root,
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("reopen memory runtime");
    assert_eq!(
        list(
            &reopened,
            MemoryScope::User,
            data_root.path(),
            /*text*/ None,
        )
        .await,
        vec![updated]
    );
}
