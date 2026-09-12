use std::fs;
use std::path::Path;

use chrono::{DateTime, Utc};
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
) -> Vec<MemoryEntry> {
    match runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(scope),
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

fn enabled_config() -> MemoryConfig {
    MemoryConfig {
        enabled: true,
        ..MemoryConfig::default()
    }
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-4, DD-8
/// Verifies: opening a current database repairs a stale Markdown projection.
#[tokio::test]
async fn restart_repairs_projection_after_database_commit() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime = MemoryRuntime::open(memory_root.clone(), enabled_config())
        .expect("open enabled memory runtime");
    let remembered = remember(
        &runtime,
        MemoryRememberRequest {
            text: "I prefer repaired projections".to_string(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Preference),
            source_user_item_id: Some("projection-repair".to_string()),
            source_session_id: "session-repair".to_string(),
            source_turn_id: None,
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    let projection_path = memory_root.join("user").join("MEMORY.md");
    fs::write(&projection_path, "stale projection").expect("simulate stale projection");
    drop(runtime);

    let reopened = MemoryRuntime::open(memory_root, enabled_config())
        .expect("reopen memory runtime and repair projections");
    assert_eq!(
        list(&reopened, MemoryScope::User, data_root.path()).await,
        vec![remembered.clone()]
    );
    let repaired = fs::read_to_string(projection_path).expect("read repaired projection");
    assert!(repaired.contains(&remembered.body));
    assert!(!repaired.contains("stale projection"));
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-4, DD-8
/// Verifies: schema migration merges legacy keys without changing canonical identity.
#[tokio::test]
async fn schema_upgrade_rekeys_and_merges_legacy_equivalent_entries() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("open memory runtime");
    let oldest = remember(
        &runtime,
        MemoryRememberRequest {
            text: "Please remember that I prefer compact responses".to_string(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Preference),
            source_user_item_id: Some("legacy-item".to_string()),
            source_session_id: "legacy-session".to_string(),
            source_turn_id: Some("legacy-turn".to_string()),
            workspace_root: data_root.path().to_path_buf(),
        },
    )
    .await;
    drop(runtime);

    let connection = Connection::open(memory_root.join("memory.sqlite3"))
        .expect("open memory database for legacy fixture");
    connection
        .execute(
            "UPDATE memory_entries SET normalized_key = ?1 WHERE entry_id = ?2",
            rusqlite::params![
                "please remember that i prefer compact responses",
                oldest.entry_id.as_str(),
            ],
        )
        .expect("restore legacy normalized key");
    connection
        .execute(
            "INSERT INTO memory_entries (
                 entry_id, scope_type, scope_id, kind, normalized_key, body,
                 origin, state, created_at, updated_at
             ) VALUES (?1, 'user', ?2, 'preference', ?3, ?4,
                       'explicit_user', 'active', ?5, ?5)",
            rusqlite::params![
                "legacy-duplicate",
                oldest.scope_id,
                "remember that i prefer compact responses",
                "Remember that I prefer compact responses",
                "2030-01-01T00:00:00Z",
            ],
        )
        .expect("insert equivalent legacy duplicate");
    connection
        .execute(
            "INSERT INTO memory_evidence (
                 evidence_id, entry_id, session_id, turn_id, source_user_item_id,
                 observed_at, source_watermark
             ) VALUES ('legacy-evidence-2', 'legacy-duplicate', 'new-session',
                       'new-turn', 'new-item', ?1, ?1)",
            ["2030-01-01T00:00:00Z"],
        )
        .expect("insert duplicate provenance");
    connection
        .execute(
            "INSERT INTO memory_evidence (
                 evidence_id, entry_id, session_id, turn_id, source_user_item_id,
                 observed_at, source_watermark
             ) VALUES ('legacy-evidence-replay', 'legacy-duplicate', 'legacy-session',
                       'legacy-turn', 'legacy-item', ?1, ?1)",
            ["2030-01-01T00:00:00Z"],
        )
        .expect("insert replayed duplicate provenance");
    connection
        .execute(
            "INSERT INTO memory_entries (
                 entry_id, scope_type, scope_id, kind, normalized_key, body,
                 origin, state, created_at, updated_at
             ) VALUES (?1, 'user', ?2, 'fact', ?3, ?4,
                       'inferred_session', 'conflicted', ?5, ?5)",
            rusqlite::params![
                "legacy-inferred",
                oldest.scope_id,
                "i prefer compact responses",
                "Model inferred a compact-response preference",
                "2040-01-01T00:00:00Z",
            ],
        )
        .expect("insert colliding inferred memory");
    connection
        .execute(
            "INSERT INTO memory_evidence (
                 evidence_id, entry_id, session_id, turn_id, source_user_item_id,
                 observed_at, source_watermark
             ) VALUES ('legacy-evidence-inferred', 'legacy-inferred', 'inferred-session',
                       'inferred-turn', NULL, ?1, ?1)",
            ["2040-01-01T00:00:00Z"],
        )
        .expect("insert inferred provenance");
    connection
        .execute_batch(
            "DELETE FROM memory_entries_fts;
             INSERT INTO memory_entries_fts (entry_id, normalized_key, body)
             SELECT entry_id, normalized_key, body FROM memory_entries;
             UPDATE memory_schema_meta SET value = '3' WHERE key = 'schema_version';",
        )
        .expect("downgrade fixture schema marker");
    drop(connection);
    fs::write(
        memory_root.join("user").join("MEMORY.md"),
        "stale legacy projection",
    )
    .expect("write stale legacy projection");

    let reopened =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("upgrade memory runtime");
    let mut expected = oldest;
    expected.normalized_key = "i prefer compact responses".to_string();
    expected.body = "Remember that I prefer compact responses".to_string();
    expected.updated_at = DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .expect("parse fixture timestamp")
        .with_timezone(&Utc);
    expected.provenance = vec![
        MemoryProvenance {
            source_session_id: Some("legacy-session".to_string()),
            source_turn_id: Some("legacy-turn".to_string()),
            source_user_item_id: Some(ItemId::from_string("legacy-item".to_string())),
        },
        MemoryProvenance {
            source_session_id: Some("new-session".to_string()),
            source_turn_id: Some("new-turn".to_string()),
            source_user_item_id: Some(ItemId::from_string("new-item".to_string())),
        },
        MemoryProvenance {
            source_session_id: Some("inferred-session".to_string()),
            source_turn_id: Some("inferred-turn".to_string()),
            source_user_item_id: None,
        },
    ];
    let expected_entries = vec![expected];
    assert_eq!(
        list(&reopened, MemoryScope::User, data_root.path()).await,
        expected_entries
    );
    drop(reopened);
    let reopened = MemoryRuntime::open(memory_root.clone(), enabled_config())
        .expect("reopen migrated memory runtime idempotently");
    assert_eq!(
        list(&reopened, MemoryScope::User, data_root.path()).await,
        expected_entries
    );

    let connection = Connection::open(memory_root.join("memory.sqlite3"))
        .expect("open upgraded memory database");
    let stored: (String, i64, i64) = connection
        .query_row(
            "SELECT
                 (SELECT value FROM memory_schema_meta WHERE key = 'schema_version'),
                 (SELECT COUNT(*) FROM memory_entries),
                 (SELECT COUNT(*) FROM memory_entries_fts
                  WHERE memory_entries_fts MATCH 'compact')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read migrated storage state");
    assert_eq!(stored, ("4".to_string(), 1, 1));
    let projection = fs::read_to_string(memory_root.join("user").join("MEMORY.md"))
        .expect("read migrated projection");
    assert!(projection.contains("Remember that I prefer compact responses"));
    assert!(!projection.contains("Model inferred a compact-response preference"));
    assert!(!projection.contains("stale legacy projection"));
}
