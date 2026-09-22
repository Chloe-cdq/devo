use std::fs;
use std::path::Path;

use devo_server::memory::MemorySourceContext;
#[path = "../src/memory/test_support.rs"]
mod memory_test_support;

use chrono::{DateTime, Utc};
use devo_core::MemoryConfig;
use devo_protocol::native::ids::{ItemId, MemoryEntryId};
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryKind, MemoryOrigin, MemoryProvenance, MemoryScope, MemoryState,
};
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
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_) => {
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
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::Forget(_) => {
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
            source: memory_test_support::test_source(
                Some("projection-repair"),
                "session-repair",
                /*turn_id*/ None,
                data_root.path().to_path_buf(),
            ),
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

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-4, DD-8, DD-9
/// Verifies: schema migration merges legacy keys without changing canonical identity or reviving a revoked identity.
#[tokio::test]
async fn schema_upgrade_rekeys_and_merges_legacy_equivalent_entries() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("open memory runtime");
    let legacy_source = memory_test_support::test_source(
        Some("legacy-item"),
        "legacy-session",
        Some("legacy-turn"),
        data_root.path().to_path_buf(),
    );
    let oldest = remember(
        &runtime,
        MemoryRememberRequest {
            text: "Please remember that I prefer compact responses".to_string(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Preference),
            source: legacy_source.clone(),
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
            "INSERT INTO memory_entries (
                 entry_id, scope_type, scope_id, kind, normalized_key, body,
                 origin, state, created_at, updated_at
             ) VALUES (?1, 'user', ?2, 'preference', ?3, ?4,
                       'explicit_user', 'retired', ?5, ?5)",
            rusqlite::params![
                "legacy-retired-duplicate",
                oldest.scope_id,
                "please note that i prefer compact responses",
                "Please note that I prefer compact responses",
                "2029-01-01T00:00:00Z",
            ],
        )
        .expect("insert retired equivalent legacy entry");
    connection
        .execute(
            "INSERT INTO memory_evidence (
                 evidence_id, entry_id, session_id, turn_id, source_user_item_id,
                 observed_at, source_watermark
             ) VALUES ('legacy-evidence-replay', 'legacy-duplicate', ?1, ?2, ?3, ?4, ?4)",
            rusqlite::params![
                legacy_source.session_id.to_string(),
                legacy_source.turn_id.map(|turn_id| turn_id.to_string()),
                legacy_source.user_item_id.as_ref().map(ToString::to_string),
                "2030-01-01T00:00:00Z",
            ],
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
        .execute(
            "INSERT INTO memory_entries (
                 entry_id, scope_type, scope_id, kind, normalized_key, body,
                 origin, state, created_at, updated_at, replacement_entry_id
             ) VALUES (?1, 'user', ?2, 'fact', ?3, ?4,
                       'inferred_session', 'retired', ?5, ?5, 'legacy-duplicate')",
            rusqlite::params![
                "legacy-lineage",
                oldest.scope_id,
                "lineage-anchor",
                "Retired lineage anchor",
                "2029-01-01T00:00:00Z",
            ],
        )
        .expect("insert external replacement lineage");
    connection
        .execute_batch(
            "INSERT INTO memory_revocations (
                 revocation_id, scope_type, scope_id, normalized_key, revoked_at, restored_at
             ) VALUES
                 ('legacy-revocation-1', 'user', 'user',
                  'please remember that i prefer compact responses',
                  '2027-01-01T00:00:00Z', '2028-01-01T00:00:00Z'),
                 ('legacy-revocation-2', 'user', 'user',
                  'remember that i prefer compact responses',
                  '2035-01-01T00:00:00Z', NULL);",
        )
        .expect("insert converging legacy revocations");
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
    expected.state = MemoryState::Retired;
    expected.updated_at = DateTime::parse_from_rfc3339("2035-01-01T00:00:00Z")
        .expect("parse fixture timestamp")
        .with_timezone(&Utc);
    expected.provenance = vec![
        MemoryProvenance {
            source_session_id: Some(legacy_source.session_id.to_string()),
            source_turn_id: legacy_source.turn_id.map(|turn_id| turn_id.to_string()),
            source_user_item_id: legacy_source.user_item_id,
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
    let lineage = MemoryEntry {
        entry_id: MemoryEntryId::from_string("legacy-lineage".to_string()),
        scope: MemoryScope::User,
        scope_id: expected.scope_id.clone(),
        kind: MemoryKind::Fact,
        normalized_key: "lineage-anchor".to_string(),
        body: "Retired lineage anchor".to_string(),
        origin: MemoryOrigin::InferredSession,
        state: MemoryState::Retired,
        created_at: DateTime::parse_from_rfc3339("2029-01-01T00:00:00Z")
            .expect("parse lineage fixture timestamp")
            .with_timezone(&Utc),
        updated_at: DateTime::parse_from_rfc3339("2029-01-01T00:00:00Z")
            .expect("parse lineage fixture timestamp")
            .with_timezone(&Utc),
        replacement_entry_id: Some(expected.entry_id.clone()),
        provenance: Vec::new(),
    };
    let expected_entries = vec![expected, lineage];
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
    assert_eq!(stored, ("4".to_string(), 2, 0));
    let revocations = connection
        .prepare(
            "SELECT revocation_id, normalized_key, revoked_at, restored_at
             FROM memory_revocations
             ORDER BY revocation_id",
        )
        .expect("prepare migrated revocation query")
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .expect("query migrated revocations")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect migrated revocations");
    assert_eq!(
        revocations,
        vec![(
            "legacy-revocation-2".to_string(),
            "i prefer compact responses".to_string(),
            "2035-01-01T00:00:00Z".to_string(),
            None,
        )]
    );
    let projection = fs::read_to_string(memory_root.join("user").join("MEMORY.md"))
        .expect("read migrated projection");
    assert!(projection.contains("Remember that I prefer compact responses"));
    assert!(projection.contains("state: retired"));
    assert!(!projection.contains("Model inferred a compact-response preference"));
    assert!(!projection.contains("stale legacy projection"));
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-8, DD-9
/// Verifies: schema migration preserves the public Restored state when restoration is the latest lifecycle event.
#[tokio::test]
async fn schema_upgrade_restores_identity_when_restore_is_latest() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("open memory runtime");
    let remembered = remember(
        &runtime,
        MemoryRememberRequest {
            text: "Please remember that I prefer restored migrations".to_string(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Preference),
            source: memory_test_support::test_source(
                Some("restored-item"),
                "restored-session",
                /*turn_id*/ None,
                data_root.path().to_path_buf(),
            ),
        },
    )
    .await;
    drop(runtime);

    let connection = Connection::open(memory_root.join("memory.sqlite3"))
        .expect("open memory database for restored fixture");
    connection
        .execute(
            "UPDATE memory_entries SET normalized_key = ?1 WHERE entry_id = ?2",
            rusqlite::params![
                "please remember that i prefer restored migrations",
                remembered.entry_id.as_str(),
            ],
        )
        .expect("restore legacy normalized key");
    connection
        .execute(
            "INSERT INTO memory_revocations (
                 revocation_id, scope_type, scope_id, normalized_key, revoked_at, restored_at
             ) VALUES (?1, 'user', 'user', ?2, ?3, ?4)",
            rusqlite::params![
                "legacy-restored-revocation",
                "please remember that i prefer restored migrations",
                "2027-01-01T00:00:00Z",
                "2030-01-01T00:00:00Z",
            ],
        )
        .expect("insert restored legacy revocation");
    connection
        .execute(
            "UPDATE memory_schema_meta SET value = '3' WHERE key = 'schema_version'",
            [],
        )
        .expect("downgrade fixture schema marker");
    drop(connection);

    let reopened = MemoryRuntime::open(memory_root.clone(), enabled_config())
        .expect("upgrade restored memory runtime");
    let mut expected = remembered;
    expected.normalized_key = "i prefer restored migrations".to_string();
    expected.state = MemoryState::Restored;
    expected.updated_at = DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .expect("parse restored fixture timestamp")
        .with_timezone(&Utc);
    assert_eq!(
        list(&reopened, MemoryScope::User, data_root.path()).await,
        vec![expected]
    );
    drop(reopened);

    let connection = Connection::open(memory_root.join("memory.sqlite3"))
        .expect("open upgraded restored memory database");
    let fts_matches: i64 = connection
        .query_row(
            "SELECT COUNT(*) AS fts_matches FROM memory_entries_fts
             WHERE memory_entries_fts MATCH 'restored'",
            [],
            |row| row.get("fts_matches"),
        )
        .expect("query restored memory FTS index");
    assert_eq!(fts_matches, 1);
}
